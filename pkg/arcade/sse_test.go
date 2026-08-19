package arcade

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sseTestTimeout = 10 * time.Second

type recordedSSERequest struct {
	lastEventID   string
	callbackToken string
	accept        string
}

// waitFor reads one value from ch or fails the test after sseTestTimeout.
func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(sseTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func TestStreamStatus_DeliversReconnectsResumesAndSkips(t *testing.T) {
	// given: an SSE server that closes the stream after the first batch of events
	var requestCount atomic.Int32
	requests := make(chan recordedSSERequest, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok, "response writer must support flushing") {
			return
		}

		requests <- recordedSSERequest{
			lastEventID:   r.Header.Get("Last-Event-ID"),
			callbackToken: r.URL.Query().Get("callbackToken"),
			accept:        r.Header.Get("Accept"),
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		switch requestCount.Add(1) {
		case 1:
			// keepalive comment must be ignored
			_, _ = io.WriteString(w, ": keepalive\n\n")
			_, _ = io.WriteString(w, "id: 1\nevent: status\ndata: {\"txid\":\"tx-aaa\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n")
			// malformed data frame must be skipped without killing the stream
			_, _ = io.WriteString(w, "id: 1-bad\nevent: status\ndata: {malformed\n\n")
			_, _ = io.WriteString(w, "id: 2\nevent: status\ndata: {\"txid\":\"tx-bbb\",\"txStatus\":\"MINED\"}\n\n")
			flusher.Flush()
			// returning closes the stream and forces the client to reconnect
		default:
			_, _ = io.WriteString(w, "id: 3\ndata: {\"txid\":\"tx-ccc\",\"txStatus\":\"IMMUTABLE\"}\n\n")
			flusher.Flush()
			<-r.Context().Done()
		}
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// when:
	events := make(chan StatusEvent, 10)
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			if ev.Record.TxID == "tx-aaa" {
				// onEvent error must be logged only - the event still counts as
				// delivered, so the cursor still advances past it.
				return errors.New("handler failed on purpose")
			}
			return nil
		})
	}()

	// then: events delivered in order, malformed frame skipped.
	first := waitFor(t, events, "first event")
	assert.Equal(t, "1", first.ID)
	assert.Equal(t, "tx-aaa", first.Record.TxID)
	assert.Equal(t, StatusSeenOnNetwork, first.Record.Status)

	second := waitFor(t, events, "second event")
	assert.Equal(t, "2", second.ID)
	assert.Equal(t, "tx-bbb", second.Record.TxID)
	assert.Equal(t, StatusMined, second.Record.Status)

	// and: the client reconnects and keeps receiving events.
	third := waitFor(t, events, "third event after reconnect")
	assert.Equal(t, "3", third.ID)
	assert.Equal(t, "tx-ccc", third.Record.TxID)
	assert.Equal(t, StatusImmutable, third.Record.Status)

	// and: the first request carries no Last-Event-ID but the callback token.
	firstReq := waitFor(t, requests, "first request record")
	assert.Empty(t, firstReq.lastEventID)
	assert.Equal(t, testCallbackToken, firstReq.callbackToken)
	assert.Equal(t, "text/event-stream", firstReq.accept)

	// and: the reconnect carries the id of the last DELIVERED event (2, NOT the
	// malformed 1-bad frame that was read but never delivered - at-least-once).
	secondReq := waitFor(t, requests, "second request record")
	assert.Equal(t, "2", secondReq.lastEventID)
	assert.Equal(t, testCallbackToken, secondReq.callbackToken)

	// when: outer ctx canceled.
	cancel()

	// then: StreamStatus returns the context error.
	err := waitFor(t, errCh, "StreamStatus return after cancel")
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamStatus_SendsInitialLastEventID(t *testing.T) {
	lastEventIDs := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case lastEventIDs <- r.Header.Get("Last-Event-ID"):
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "42", func(StatusEvent) error { return nil })
	}()

	// then: before any event is delivered, the lastEventID argument is sent.
	assert.Equal(t, "42", waitFor(t, lastEventIDs, "initial Last-Event-ID"))

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

func TestStreamStatus_WatchdogReconnectsStalledStream(t *testing.T) {
	// given: a server whose first connection delivers one event and then goes
	// silent (no keepalives, no close) - simulating a dead TCP peer.
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok) {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		switch requestCount.Add(1) {
		case 1:
			_, _ = io.WriteString(w, "id: 1\nevent: status\ndata: {\"txid\":\"tx-stalled\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n")
			flusher.Flush()
			<-r.Context().Done() // go silent without closing
		default:
			_, _ = io.WriteString(w, "id: 2\nevent: status\ndata: {\"txid\":\"tx-after-reconnect\",\"txStatus\":\"MINED\"}\n\n")
			flusher.Flush()
			<-r.Context().Done()
		}
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))
	client.sseReadWatchdogTimeout = 150 * time.Millisecond
	client.sseBackoffBase = 10 * time.Millisecond
	client.sseBackoffMax = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan StatusEvent, 10)
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			return nil
		})
	}()

	// then: the first event arrives on the stalled connection.
	assert.Equal(t, "tx-stalled", waitFor(t, events, "first event").Record.TxID)

	// and: the watchdog drops the dead connection and the stream reconnects
	// (StreamStatus must NOT return - the outer ctx is still alive).
	assert.Equal(t, "tx-after-reconnect", waitFor(t, events, "event after watchdog reconnect").Record.TxID)

	select {
	case err := <-errCh:
		t.Fatalf("StreamStatus returned after watchdog reconnect: %v", err)
	default:
	}

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

func TestStreamStatus_ContextCancelWhileConnected(t *testing.T) {
	// given: a server that holds the stream open without sending events.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok) {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(StatusEvent) error { return nil })
	}()

	// Give the client time to connect, then cancel.
	connected := make(chan struct{})
	go func() { time.Sleep(100 * time.Millisecond); close(connected) }()
	<-connected
	cancel()

	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

// TestStreamStatus_MinedFrameParsesProof proves the CLIENT's SSE parse path:
// a MINED frame carrying blockHash/blockHeight and a hex merklePath decodes
// into a fully-populated TxRecord (merklePath as HexBytes, timestamp as time).
func TestStreamStatus_MinedFrameParsesProof(t *testing.T) {
	const (
		eventID     = "1745870512987654321"
		blockHash   = "000000000000000001885e0c6c302cbbacf927e1b5cf7884588973e72f8b1234"
		blockHeight = uint64(870123)
		// Opaque BUMP hex; the client decodes it into HexBytes.
		merklePathHex = "0100cafe"
	)

	minedFrame := "id: " + eventID + "\n" +
		"event: status\n" +
		`data: {"txid":"` + testTxID + `","txStatus":"MINED","timestamp":"2026-04-28T18:21:52Z",` +
		`"blockHash":"` + blockHash + `","blockHeight":870123,"merklePath":"` + merklePathHex + `"}` +
		"\n\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok) {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, minedFrame)
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan StatusEvent, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			return nil
		})
	}()

	ev := waitFor(t, events, "MINED frame")
	assert.Equal(t, eventID, ev.ID)
	assert.Equal(t, testTxID, ev.Record.TxID)
	assert.Equal(t, StatusMined, ev.Record.Status)
	assert.Equal(t, blockHash, ev.Record.BlockHash)
	assert.Equal(t, blockHeight, ev.Record.BlockHeight)
	assert.Equal(t, HexBytes{0x01, 0x00, 0xca, 0xfe}, ev.Record.MerklePath)
	assert.Equal(t, time.Date(2026, 4, 28, 18, 21, 52, 0, time.UTC), ev.Record.Timestamp.UTC())

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

// TestStreamStatus_NonStatusEventSkipped confirms frames whose event type is
// not "status" are ignored without killing the stream.
func TestStreamStatus_NonStatusEventSkipped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok) {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// A non-status event that must be skipped, then a real status event.
		_, _ = io.WriteString(w, "id: 9\nevent: ping\ndata: {\"txid\":\"tx-ignored\",\"txStatus\":\"RECEIVED\"}\n\n")
		_, _ = io.WriteString(w, "id: 10\nevent: status\ndata: {\"txid\":\"tx-real\",\"txStatus\":\"RECEIVED\"}\n\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan StatusEvent, 4)
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			return nil
		})
	}()

	ev := waitFor(t, events, "first delivered event")
	assert.Equal(t, "tx-real", ev.Record.TxID, "the non-status 'ping' frame must have been skipped")

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

// TestStreamStatus_OversizedFrameNoHotLoop pins the backoff-reset-only-after-
// clean-delivery rule: a connection that delivers an event and then dies with a
// read error (an oversized frame tripping bufio.ErrTooLong on every reconnect)
// must keep backing off. If backoff were reset on any delivery, reconnects would
// hot-loop at a fixed cadence; here we assert each reconnect gap reaches the
// doubling ceiling its sleep should have used.
func TestStreamStatus_OversizedFrameNoHotLoop(t *testing.T) {
	conns := make(chan time.Time, 8)
	// A single line larger than the 1 MiB scanner cap -> bufio.ErrTooLong.
	oversized := "data: " + strings.Repeat("x", sseScannerMaxBufferSize+1024) + "\n\n"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case conns <- time.Now():
		default:
		}
		flusher, ok := w.(http.Flusher)
		if !assert.True(t, ok) {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// One valid event (delivered=1), then an oversized frame that trips
		// ErrTooLong so the connection ends with err != nil.
		_, _ = io.WriteString(w, "id: 1\nevent: status\ndata: {\"txid\":\"tx-1\",\"txStatus\":\"SEEN_ON_NETWORK\"}\n\n")
		flusher.Flush()
		_, _ = io.WriteString(w, oversized)
		flusher.Flush()
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))
	client.sseBackoffBase = 40 * time.Millisecond
	client.sseBackoffMax = 5 * time.Second
	// Pin the jitter to take the whole ceiling, so the sleeps are exactly the
	// doubling ceiling (40ms, 80ms, 160ms) instead of a random draw beneath it.
	// Without this the gaps are samples of full jitter and "each gap exceeds the
	// last" is simply not true of them — the assertion below would be testing the
	// RNG, not the backoff. Same pin as the internal/sse test of this rule.
	client.sseRand = func() float64 { return 1 }

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(StatusEvent) error { return nil })
	}()

	// Collect four connection timestamps.
	t0 := waitFor(t, conns, "connect #1")
	t1 := waitFor(t, conns, "connect #2")
	t2 := waitFor(t, conns, "connect #3")
	t3 := waitFor(t, conns, "connect #4")

	g1, g2, g3 := t1.Sub(t0), t2.Sub(t1), t3.Sub(t2)
	t.Logf("reconnect gaps: g1=%s g2=%s g3=%s", g1, g2, g3)

	// Backoff was NOT reset (delivered>0 but err!=nil), so each sleep is the next
	// doubling of the ceiling: 40ms, 80ms, 160ms.
	//
	// Assert each gap against its OWN expected sleep rather than against the
	// previous gap. Comparing gaps to each other looks like the more direct
	// statement of "the backoff grows", but it is not measurable: a gap is
	// sleep + scheduling delay, and on a loaded runner the delay can dwarf the
	// sleep. That is exactly how this failed — g1=63ms g2=298ms g3=174ms, where
	// a ~218ms stall landed inside the g2 window and made g3 < g2 even though
	// every sleep was correct. 0847861 pinned the jitter to remove the RNG from
	// this comparison and judged the 2x margins wide enough to absorb the rest;
	// they are not, and no fixed margin is, because the stall is unbounded.
	//
	// Lower bounds are immune to it. Scheduling delay can only ever make a gap
	// LONGER than its sleep, never shorter, so `gap >= expected sleep` holds
	// under arbitrary load while still failing loudly on the bug this test
	// exists for: a hot loop, or a backoff reset, produces gaps at a flat ~40ms
	// cadence, which misses the 80ms and 160ms bounds immediately.
	base := 40 * time.Millisecond
	assert.GreaterOrEqual(t, g1, base,
		"first reconnect gap must be at least one backoff ceiling")
	assert.GreaterOrEqual(t, g2, 2*base,
		"second reconnect gap must be at least the doubled ceiling — a shorter one "+
			"means backoff was reset on delivery and reconnects are hot-looping")
	assert.GreaterOrEqual(t, g3, 4*base,
		"third reconnect gap must be at least the twice-doubled ceiling")

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}

// TestStreamStatus_ReconnectsOnNon200 confirms a non-200 status ends the
// connection with an error and the client reconnects (no return).
func TestStreamStatus_ReconnectsOnNon200(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		flusher, _ := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "id: 1\nevent: status\ndata: {\"txid\":\"tx-ok\",\"txStatus\":\"RECEIVED\"}\n\n")
		flusher.Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))
	client.sseBackoffBase = 10 * time.Millisecond
	client.sseBackoffMax = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan StatusEvent, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			return nil
		})
	}()

	assert.Equal(t, "tx-ok", waitFor(t, events, "event after non-200 reconnect").Record.TxID)

	cancel()
	require.ErrorIs(t, waitFor(t, errCh, "return after cancel"), context.Canceled)
}
