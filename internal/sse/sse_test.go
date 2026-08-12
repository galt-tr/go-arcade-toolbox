package sse

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// --- jitter / retry / watchdog ----------------------------------------------

// TestSleepFor_JittersBeneathTheCeiling pins the reconnect delay distribution.
// Without jitter every client that shared a server wakes at exactly the same
// offsets and re-dials in lockstep, so a restart lands as synchronized spikes
// rather than a ramp.
func TestSleepFor_JittersBeneathTheCeiling(t *testing.T) {
	r := New(Config{BackoffBase: time.Second, BackoffMax: time.Minute})

	for _, tc := range []struct {
		name    string
		draw    float64
		ceiling time.Duration
		want    time.Duration
	}{
		{"full draw takes the ceiling", 1, 8 * time.Second, 8 * time.Second},
		{"mid draw lands beneath it", 0.5, 8 * time.Second, 4 * time.Second},
		{"zero draw is floored, never a hot loop", 0, 8 * time.Second, 500 * time.Millisecond},
		{"ceiling at or under the floor is used verbatim", 0, 200 * time.Millisecond, 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.cfg.Rand = func() float64 { return tc.draw }
			if got := r.sleepFor(tc.ceiling); got != tc.want {
				t.Errorf("sleepFor(%s) with draw %v = %s, want %s", tc.ceiling, tc.draw, got, tc.want)
			}
		})
	}
}

// TestRun_AdoptsServerRetryDirective verifies the `retry:` field is parsed and
// honored. A server that knows it is about to sever every stream can hand out
// its own spread; the client cannot infer that on its own.
func TestRun_AdoptsServerRetryDirective(t *testing.T) {
	conns := make(chan time.Time, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case conns <- time.Now():
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// A complete frame carrying only a retry directive, then close.
		_, _ = io.WriteString(w, "retry: 400\nevent: shutdown\ndata: {}\n\n")
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := New(Config{
		Name:            "test",
		Logger:          discardLogger(),
		Client:          server.Client(),
		Request:         getRequest(server.URL),
		Handler:         func(context.Context, Frame) bool { return true },
		ResetBackoff:    func(ConnResult) bool { return false },
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      5 * time.Second,
		WatchdogTimeout: 5 * time.Second,
		Rand:            func() float64 { return 1 }, // take the whole ceiling
	})
	go func() { _ = reader.Run(ctx) }()

	t0 := waitFor(t, conns, "connect #1")
	t1 := waitFor(t, conns, "connect #2")

	// Without the directive the ceiling would still be BackoffBase (10ms); the
	// server asked for 400ms and it must win.
	if gap := t1.Sub(t0); gap < 300*time.Millisecond {
		t.Errorf("reconnect gap = %s, want >= 300ms (server retry directive ignored)", gap)
	}
}

// TestRun_SlowHandlerDoesNotTripItsOwnWatchdog covers the read-liveness
// watchdog's scope: it measures the SOCKET, not the consumer. A handler that
// blocks longer than WatchdogTimeout used to cancel its own healthy connection,
// and the reconnect made the backlog it was working through worse.
func TestRun_SlowHandlerDoesNotTripItsOwnWatchdog(t *testing.T) {
	var conns atomic.Int32
	const watchdog = 100 * time.Millisecond

	// slowDone is closed by the handler once it has blocked for longer than the
	// watchdog. The server waits for it before writing the second frame, so
	// that frame can only be read over a connection that SURVIVED the slow
	// handler — reading it from a buffer or over a redial is not possible.
	slowDone := make(chan struct{})
	var slowOnce sync.Once
	hold := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conns.Add(1)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("response writer is not a flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		_, _ = io.WriteString(w, "event: status\ndata: {\"n\":1}\n\n")
		flusher.Flush()
		select {
		case <-slowDone:
		case <-hold:
			return
		}
		_, _ = io.WriteString(w, "event: status\ndata: {\"n\":2}\n\n")
		flusher.Flush()
		<-hold
	}))
	defer server.Close()
	defer close(hold)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotSecond := make(chan struct{}, 1)
	reader := New(Config{
		Name:    "test",
		Logger:  discardLogger(),
		Client:  server.Client(),
		Request: getRequest(server.URL),
		Handler: func(_ context.Context, f Frame) bool {
			if strings.Contains(f.Data, `"n":2`) {
				select {
				case gotSecond <- struct{}{}:
				default:
				}
				return true
			}
			// Block for well over the watchdog while the socket sits silent.
			time.Sleep(3 * watchdog)
			slowOnce.Do(func() { close(slowDone) })
			return true
		},
		ResetBackoff:    func(ConnResult) bool { return true },
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      time.Second,
		WatchdogTimeout: watchdog,
	})
	go func() { _ = reader.Run(ctx) }()

	waitFor(t, gotSecond, "second frame over the surviving connection")
	if got := conns.Load(); got != 1 {
		t.Errorf("connections = %d, want 1: the slow handler canceled its own connection", got)
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
		// Draw the full ceiling every time: this test is about the ceiling
		// doubling, and an unpinned jitter draw would make "gaps grow" a coin
		// flip rather than an assertion.
		Rand: func() float64 { return 1 },
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
