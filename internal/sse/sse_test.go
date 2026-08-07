package sse

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 10 * time.Second

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// getRequest returns a Request builder for u that sets the standard SSE headers,
// bound to the per-connection ctx.
func getRequest(u string) func(context.Context) (*http.Request, error) {
	return func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "text/event-stream")
		return req, nil
	}
}

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// --- pure parser tests -------------------------------------------------------

func TestSplitField(t *testing.T) {
	cases := []struct {
		line        string
		field, want string
	}{
		{"data: hello", "data", "hello"},
		{"data:hello", "data", "hello"},    // no optional space
		{"data:  hello", "data", " hello"}, // only ONE leading space stripped
		{"id: 42", "id", "42"},
		{"event: status", "event", "status"},
		{"bare", "bare", ""},         // no colon: field with empty value
		{":comment", "", "comment"},  // leading colon: empty field name
		{"data: a:b", "data", "a:b"}, // only first colon splits
	}
	for _, tc := range cases {
		field, value := splitField(tc.line)
		if field != tc.field || value != tc.want {
			t.Errorf("splitField(%q) = (%q,%q), want (%q,%q)", tc.line, field, value, tc.field, tc.want)
		}
	}
}

func TestAccumulate_MultiLineDataAndFields(t *testing.T) {
	var f Frame
	accumulate("id: 7", &f)
	accumulate("event: status", &f)
	accumulate("data: line1", &f)
	accumulate("data: line2", &f)
	accumulate("unknown: ignored", &f)

	if f.ID != "7" || f.Event != "status" {
		t.Fatalf("id/event = (%q,%q), want (7,status)", f.ID, f.Event)
	}
	if f.Data != "line1\nline2" {
		t.Fatalf("data = %q, want %q", f.Data, "line1\nline2")
	}
}

// --- integration tests over an httptest SSE server ---------------------------

// TestRun_ParsesFramesAndKeepalivesReconnectsAndCancels exercises the frame
// parser (id/event/data, keepalive comment, blank-line boundary), the reconnect
// loop, and clean return on ctx cancel in one flow.
func TestRun_ParsesFramesAndKeepalivesReconnectsAndCancels(t *testing.T) {
	var conns atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if conns.Add(1) == 1 {
			_, _ = io.WriteString(w, ": keepalive\n\n")
			_, _ = io.WriteString(w, "id: 1\nevent: status\ndata: {\"a\":1}\n\n")
			flusher.Flush()
			return // close -> force reconnect
		}
		_, _ = io.WriteString(w, "id: 2\ndata: two\n\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	frames := make(chan Frame, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := New(Config{
		Name:    "test",
		Logger:  discardLogger(),
		Client:  server.Client(),
		Request: getRequest(server.URL),
		Handler: func(_ context.Context, f Frame) bool {
			if f.Data == "" {
				return false
			}
			frames <- f
			return true
		},
		ResetBackoff:    func(r ConnResult) bool { return r.ReadAny },
		BackoffBase:     2 * time.Millisecond,
		BackoffMax:      20 * time.Millisecond,
		WatchdogTimeout: 5 * time.Second,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- reader.Run(ctx) }()

	first := waitFor(t, frames, "first frame")
	if first.ID != "1" || first.Event != "status" || first.Data != `{"a":1}` {
		t.Fatalf("first frame = %+v", first)
	}
	second := waitFor(t, frames, "frame after reconnect")
	if second.ID != "2" || second.Data != "two" {
		t.Fatalf("second frame = %+v", second)
	}

	cancel()
	if err := waitFor(t, errCh, "Run return after cancel"); err != context.Canceled {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

// TestRun_ResetBackoffFalseGrowsGaps pins the backoff-reset seam: when
// ResetBackoff always returns false the reconnect gaps must grow (no hot-loop),
// modeling arcade's oversized-frame rule.
func TestRun_ResetBackoffFalseGrowsGaps(t *testing.T) {
	conns := make(chan time.Time, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case conns <- time.Now():
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// Close immediately: no lines read, so ReadAny stays false either way; the
		// point is ResetBackoff==false keeps the backoff growing.
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := New(Config{
		Name:            "test",
		Logger:          discardLogger(),
		Client:          server.Client(),
		Request:         getRequest(server.URL),
		Handler:         func(context.Context, Frame) bool { return false },
		ResetBackoff:    func(ConnResult) bool { return false },
		BackoffBase:     30 * time.Millisecond,
		BackoffMax:      5 * time.Second,
		WatchdogTimeout: 5 * time.Second,
	})
	go func() { _ = reader.Run(ctx) }()

	t0 := waitFor(t, conns, "connect #1")
	t1 := waitFor(t, conns, "connect #2")
	t2 := waitFor(t, conns, "connect #3")
	t3 := waitFor(t, conns, "connect #4")

	g1, g2, g3 := t1.Sub(t0), t2.Sub(t1), t3.Sub(t2)
	if g2 <= g1 || g3 <= g2 {
		t.Fatalf("reconnect gaps did not grow: g1=%s g2=%s g3=%s", g1, g2, g3)
	}
}

// TestRun_WatchdogReconnectsSilentStream pins the read-liveness watchdog: a
// connection that goes silent (no close) is dropped and redialed without Run
// returning.
func TestRun_WatchdogReconnectsSilentStream(t *testing.T) {
	var conns atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		conns.Add(1)
		_, _ = io.WriteString(w, "id: 1\ndata: hi\n\n")
		flusher.Flush()
		<-r.Context().Done() // go silent without closing (both connects)
	}))
	defer server.Close()

	frames := make(chan Frame, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := New(Config{
		Name:            "test",
		Logger:          discardLogger(),
		Client:          server.Client(),
		Request:         getRequest(server.URL),
		Handler:         func(_ context.Context, f Frame) bool { frames <- f; return f.Data != "" },
		ResetBackoff:    func(r ConnResult) bool { return r.Delivered > 0 && r.Err == nil },
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      50 * time.Millisecond,
		WatchdogTimeout: 150 * time.Millisecond,
	})

	errCh := make(chan error, 1)
	go func() { errCh <- reader.Run(ctx) }()

	waitFor(t, frames, "first frame")
	// The watchdog must drop the silent connection and the stream must reconnect.
	waitFor(t, frames, "frame after watchdog reconnect")
	if conns.Load() < 2 {
		t.Fatalf("expected reconnect, conns=%d", conns.Load())
	}
	select {
	case err := <-errCh:
		t.Fatalf("Run returned during watchdog reconnect: %v", err)
	default:
	}

	cancel()
	if err := waitFor(t, errCh, "Run return after cancel"); err != context.Canceled {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

// TestRun_OnConnectRunsEachConnect confirms OnConnect fires once per successful
// connect, before frames flow.
func TestRun_OnConnectRunsEachConnect(t *testing.T) {
	var conns atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if conns.Add(1) == 1 {
			return // close -> reconnect
		}
		flusher.Flush() // flush headers so client.Do returns before we block
		<-r.Context().Done()
	}))
	defer server.Close()

	connects := make(chan struct{}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := New(Config{
		Name:            "test",
		Logger:          discardLogger(),
		Client:          server.Client(),
		Request:         getRequest(server.URL),
		OnConnect:       func(context.Context) { connects <- struct{}{} },
		Handler:         func(context.Context, Frame) bool { return false },
		ResetBackoff:    func(r ConnResult) bool { return r.ReadAny },
		BackoffBase:     2 * time.Millisecond,
		BackoffMax:      20 * time.Millisecond,
		WatchdogTimeout: 5 * time.Second,
	})
	go func() { _ = reader.Run(ctx) }()

	waitFor(t, connects, "OnConnect #1")
	waitFor(t, connects, "OnConnect #2 after reconnect")
}

// TestRun_Non200Reconnects confirms a non-200 status ends the connection with an
// error and the reader reconnects rather than returning.
func TestRun_Non200Reconnects(t *testing.T) {
	var conns atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conns.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: ok\n\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	frames := make(chan Frame, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sawErr atomic.Bool
	reader := New(Config{
		Name:    "test",
		Logger:  discardLogger(),
		Client:  server.Client(),
		Request: getRequest(server.URL),
		Handler: func(_ context.Context, f Frame) bool { frames <- f; return true },
		ResetBackoff: func(r ConnResult) bool {
			if r.Err != nil {
				sawErr.Store(true)
			}
			return r.ReadAny
		},
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      50 * time.Millisecond,
		WatchdogTimeout: 5 * time.Second,
	})
	go func() { _ = reader.Run(ctx) }()

	f := waitFor(t, frames, "frame after non-200 reconnect")
	if f.Data != "ok" {
		t.Fatalf("frame = %+v", f)
	}
	if !sawErr.Load() {
		t.Fatal("expected a non-nil ConnResult.Err for the non-200 connection")
	}
}
