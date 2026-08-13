package arcade

// Tests for the exported SSE options. Until these existed the fields they write
// were reachable only from inside this package, so a deployment could tune the
// chaintracks stream (pkg/headers has exported the same three knobs all along)
// and not the arcade one.
//
// The field assertions alone would be satisfied by an option that writes a field
// nobody reads — which is exactly how WithApplyConcurrency was dead for a while
// over in pkg/monitor. So the watchdog, the one knob with observable behavior at
// this level, is also proven end-to-end against a server that goes silent.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
)

func TestOptions_DefaultsWhenUnset(t *testing.T) {
	c := New(logging.NewTestLogger(t), resty.New(), defaultConfig("http://example.invalid"))

	require.Equal(t, readWatchdogTimeout, c.sseReadWatchdogTimeout)
	require.Equal(t, sseBackoffBase, c.sseBackoffBase)
	require.Equal(t, sseBackoffMax, c.sseBackoffMax)
	require.NotNil(t, c.sseClient, "a dedicated streaming client is built by default")
}

func TestOptions_OverrideTheDefaults(t *testing.T) {
	custom := &http.Client{}
	c := New(logging.NewTestLogger(t), resty.New(), defaultConfig("http://example.invalid"),
		WithSSEHTTPClient(custom),
		WithSSEBackoff(5*time.Millisecond, 25*time.Millisecond),
		WithReadWatchdogTimeout(90*time.Millisecond),
	)

	require.Same(t, custom, c.sseClient)
	require.Equal(t, 5*time.Millisecond, c.sseBackoffBase)
	require.Equal(t, 25*time.Millisecond, c.sseBackoffMax)
	require.Equal(t, 90*time.Millisecond, c.sseReadWatchdogTimeout)
}

// Non-positive and nil values are ignored rather than installed. A zero
// watchdog would disable read-liveness entirely and a nil SSE client would
// panic on the first dial, so "ignore" is the only safe reading of an unset
// field in a variadic option.
func TestOptions_IgnoreZeroValues(t *testing.T) {
	c := New(logging.NewTestLogger(t), resty.New(), defaultConfig("http://example.invalid"),
		WithSSEHTTPClient(nil),
		WithSSEBackoff(0, 0),
		WithReadWatchdogTimeout(0),
	)

	require.NotNil(t, c.sseClient)
	require.Equal(t, sseBackoffBase, c.sseBackoffBase)
	require.Equal(t, sseBackoffMax, c.sseBackoffMax)
	require.Equal(t, readWatchdogTimeout, c.sseReadWatchdogTimeout)
}

// TestOptions_WatchdogReachesTheReader is the liveness half: an option-set
// watchdog must actually drop and redial a connection that has gone silent. The
// server answers the first request and then stops writing without closing —
// indistinguishable from a dead TCP peer at the application layer, which is the
// case the watchdog exists for.
func TestOptions_WatchdogReachesTheReader(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)
		w.Header().Set("Content-Type", "text/event-stream")

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

	// Every knob below comes from an option, not a field poke.
	client := New(logging.NewTestLogger(t), resty.New(), defaultConfig(server.URL),
		WithReadWatchdogTimeout(150*time.Millisecond),
		WithSSEBackoff(10*time.Millisecond, 50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan StatusEvent, 10)
	go func() {
		_ = client.StreamStatus(ctx, "", func(ev StatusEvent) error {
			events <- ev
			return nil
		})
	}()

	select {
	case ev := <-events:
		require.Equal(t, "tx-stalled", ev.Record.TxID)
	case <-time.After(5 * time.Second):
		t.Fatal("no first event")
	}

	// The silent connection must be dropped by the watchdog and redialed, which
	// is the only way the second event can arrive.
	select {
	case ev := <-events:
		require.Equal(t, "tx-after-reconnect", ev.Record.TxID)
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not drop the stalled connection; the option never reached the reader")
	}
	require.GreaterOrEqual(t, requestCount.Load(), int32(2))
}

// The options are additive: an existing caller passing none is unchanged.
func TestNew_RemainsCallableWithoutOptions(t *testing.T) {
	var _ TxOracle = New(logging.NewTestLogger(t), resty.New(), defs.Arcade{Enabled: true, URL: "http://example.invalid"})
}
