package arcade

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
)

const (
	testTxID = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"

	// testEFHex is an opaque (but valid) hex fixture; the client treats the EF
	// bytes opaquely (the arg is already binary Extended Format).
	testEFHex = "0100000001abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678900000000025100ffffffff0100e1f505000000001976a914eb0bd5edba389198e73f8efabddfc61666969ff788ac00000000"

	testCallbackToken = "test-callback-token"
)

func defaultConfig(url string) defs.Arcade {
	return defs.Arcade{
		Enabled:           true,
		URL:               url,
		EventsURL:         url,
		CallbackToken:     testCallbackToken,
		FullStatusUpdates: true,
	}
}

func newClient(t testing.TB, config defs.Arcade) *Client {
	t.Helper()
	return New(logging.NewTestLogger(t), resty.New(), config)
}

func mustDecodeEF(t testing.TB) []byte {
	t.Helper()
	ef, err := hex.DecodeString(testEFHex)
	require.NoError(t, err)
	return ef
}

func writeJSON(t testing.TB, w http.ResponseWriter, statusCode int, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	// assert (not require): this runs in the HTTP handler goroutine where FailNow is illegal.
	assert.NoError(t, json.NewEncoder(w).Encode(payload))
}

// --- Broadcast ---

func TestBroadcast_202Accepted(t *testing.T) {
	// given: arcade accepts the tx with an early RECEIVED status
	ef := mustDecodeEF(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/tx", r.URL.Path)
		assert.Equal(t, "application/octet-stream", r.Header.Get("Content-Type"))
		assert.Equal(t, testCallbackToken, r.Header.Get("X-CallbackToken"))
		assert.Equal(t, "true", r.Header.Get("X-FullStatusUpdates"))

		body, readErr := io.ReadAll(r.Body)
		assert.NoError(t, readErr)
		assert.Equal(t, ef, body, "EF bytes must be posted verbatim (binary, not hex)")

		// arcade's ARC-compatible submit response: {txid, status:202, txStatus}
		writeJSON(t, w, http.StatusAccepted, map[string]any{
			"txid":     testTxID,
			"status":   202,
			"txStatus": string(StatusReceived),
		})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	// when:
	res, err := client.Broadcast(t.Context(), testTxID, ef)

	// then:
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, testTxID, res.TxID)
	assert.Equal(t, StatusReceived, res.Status)
	assert.False(t, res.Rejected)
	assert.Empty(t, res.ExtraInfo)
}

func TestBroadcast_202IdempotentResubmitEchoesExistingStatus(t *testing.T) {
	// given: an idempotent re-submit; arcade echoes the existing lifecycle state
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusAccepted, map[string]any{
			"txid":     testTxID,
			"status":   202,
			"txStatus": string(StatusSeenOnNetwork),
		})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, StatusSeenOnNetwork, res.Status)
	assert.False(t, res.Rejected)
}

func TestBroadcast_4xxRejectionIsFinal(t *testing.T) {
	// given: a tx-level validation rejection (400). arcade puts the human reason
	// in "reason" while "error" is generic.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusBadRequest, map[string]any{
			"error":  "transaction failed validation",
			"txid":   testTxID,
			"reason": "arc error 465: transaction fee is too low",
		})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	// when:
	res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))

	// then: FINAL rejection surfaced as a result with err == nil.
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, testTxID, res.TxID)
	assert.True(t, res.Rejected)
	assert.Equal(t, StatusRejected, res.Status)
	assert.Equal(t, "arc error 465: transaction fee is too low", res.ExtraInfo,
		"ExtraInfo must carry the specific reason, not the generic error line")
}

func TestBroadcast_503Backpressure(t *testing.T) {
	cases := []struct {
		name       string
		setHeader  func(w http.ResponseWriter)
		wantWithin func(t *testing.T, d time.Duration)
	}{
		{
			name:      "explicit seconds",
			setHeader: func(w http.ResponseWriter) { w.Header().Set("Retry-After", "7") },
			wantWithin: func(t *testing.T, d time.Duration) {
				assert.Equal(t, 7*time.Second, d)
			},
		},
		{
			name:      "absent header defaults to 1s",
			setHeader: func(_ http.ResponseWriter) {},
			wantWithin: func(t *testing.T, d time.Duration) {
				assert.Equal(t, defaultRetryAfter, d)
				assert.Equal(t, 1*time.Second, d)
			},
		},
		{
			name: "http-date in the past defaults to 1s (clock skew)",
			setHeader: func(w http.ResponseWriter) {
				w.Header().Set("Retry-After", time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat))
			},
			wantWithin: func(t *testing.T, d time.Duration) {
				assert.Equal(t, defaultRetryAfter, d)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				tc.setHeader(w)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()

			client := newClient(t, defaultConfig(server.URL))

			res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))
			require.Error(t, err)
			assert.Nil(t, res)

			var bp *BackpressureError
			require.ErrorAs(t, err, &bp)
			tc.wantWithin(t, bp.RetryAfter)
		})
	}
}

func TestBroadcast_5xxIsOpaqueTransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusInternalServerError, map[string]any{"error": "failed to submit"})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))
	require.Error(t, err)
	assert.Nil(t, res)

	// Not a backpressure error - the fate is unknown.
	var bp *BackpressureError
	assert.False(t, errors.As(err, &bp))
}

func TestBroadcast_TransportErrorWhenUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := server.URL
	server.Close() // nothing listening now

	client := newClient(t, defaultConfig(url))

	res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))
	require.Error(t, err)
	assert.Nil(t, res)
}

func TestBroadcast_SuccessWithoutTxIDIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusAccepted, map[string]any{"status": 202})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	res, err := client.Broadcast(t.Context(), testTxID, mustDecodeEF(t))
	require.Error(t, err)
	assert.Nil(t, res)
}

// --- GetTx ---

func TestGetTx_ParsesFullRecord(t *testing.T) {
	// given: a mined record with all the proof fields our wire.go parses
	// (blockHash/Height, merklePath+rawTx as hex, nextRetryAt/merkleRegisteredAt).
	mined := time.Date(2026, 4, 28, 18, 21, 52, 0, time.UTC)
	body := map[string]any{
		"txid":               testTxID,
		"txStatus":           string(StatusMined),
		"status":             200,
		"timestamp":          mined.Format(time.RFC3339),
		"blockHash":          "00000000000000000002a7c4c1e48d76c5a37902165a9e5f1e0f8f9c9d4c7b3a",
		"blockHeight":        800123,
		"merklePath":         "fec70c0d00",
		"rawTx":              "0100000001",
		"competingTxs":       []string{},
		"nextRetryAt":        "0001-01-01T00:00:00Z",
		"merkleRegisteredAt": "0001-01-01T00:00:00Z",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/tx/"+testTxID, r.URL.Path)
		writeJSON(t, w, http.StatusOK, body)
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	// when:
	rec, err := client.GetTx(t.Context(), testTxID)

	// then:
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, testTxID, rec.TxID)
	assert.Equal(t, StatusMined, rec.Status)
	assert.Equal(t, uint64(800123), rec.BlockHeight)
	assert.Equal(t, "00000000000000000002a7c4c1e48d76c5a37902165a9e5f1e0f8f9c9d4c7b3a", rec.BlockHash)
	assert.Equal(t, HexBytes{0xfe, 0xc7, 0x0c, 0x0d, 0x00}, rec.MerklePath)
	assert.Equal(t, HexBytes{0x01, 0x00, 0x00, 0x00, 0x01}, rec.RawTx)
	assert.True(t, rec.NextRetryAt.IsZero())
	assert.True(t, rec.MerkleRegisteredAt.IsZero())
}

func TestGetTx_404IsErrTxNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusNotFound, map[string]any{"error": "transaction not found"})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	rec, err := client.GetTx(t.Context(), testTxID)
	require.Error(t, err)
	assert.Nil(t, rec)
	assert.ErrorIs(t, err, ErrTxNotFound)
}

func TestGetTx_5xxIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	rec, err := client.GetTx(t.Context(), testTxID)
	require.Error(t, err)
	assert.Nil(t, rec)
	assert.NotErrorIs(t, err, ErrTxNotFound)
}

// --- Health ---

func TestHealth_ParsesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/health", r.URL.Path)
		writeJSON(t, w, http.StatusOK, map[string]any{
			"healthy":     true,
			"version":     "v0.8.0",
			"status":      "ok",
			"blockHeight": 958779,
			"datahub_urls": []map[string]any{
				{"url": "https://example", "healthy": true},
			},
		})
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	health, err := client.Health(t.Context())
	require.NoError(t, err)
	require.NotNil(t, health)
	assert.True(t, health.Healthy)
	assert.Equal(t, "v0.8.0", health.Version)
	assert.Equal(t, uint64(958779), health.BlockHeight)
}

func TestHealth_UnreachableIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	url := server.URL
	server.Close()

	client := newClient(t, defaultConfig(url))

	health, err := client.Health(t.Context())
	require.Error(t, err)
	assert.Nil(t, health)
}

func TestHealth_Non2xxIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newClient(t, defaultConfig(server.URL))

	health, err := client.Health(t.Context())
	require.Error(t, err)
	assert.Nil(t, health)
}

// --- parseRetryAfter unit coverage ---

func TestParseRetryAfter(t *testing.T) {
	assert.Equal(t, defaultRetryAfter, parseRetryAfter(""))
	assert.Equal(t, defaultRetryAfter, parseRetryAfter("   "))
	assert.Equal(t, 3*time.Second, parseRetryAfter("3"))
	assert.Equal(t, defaultRetryAfter, parseRetryAfter("-5"), "negative seconds fall back to default")
	assert.Equal(t, defaultRetryAfter, parseRetryAfter("not-a-number"))

	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	got := parseRetryAfter(future)
	assert.Greater(t, got, time.Duration(0))
	assert.LessOrEqual(t, got, 30*time.Second)

	past := time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat)
	assert.Equal(t, defaultRetryAfter, parseRetryAfter(past))
}

// --- Circuit breaker ---

// cbServer serves /tx and /health with atomically-togglable outcomes and
// per-endpoint hit counters, so a test can drive the breaker deterministically.
type cbServer struct {
	server  *httptest.Server
	txFail  atomic.Bool
	healthy atomic.Bool
	txHits  atomic.Int32
	hpHits  atomic.Int32
}

func newCBServer(t testing.TB) *cbServer {
	t.Helper()
	cb := &cbServer{}
	cb.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tx":
			cb.txHits.Add(1)
			if cb.txFail.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			writeJSON(t, w, http.StatusAccepted, map[string]any{
				"txid": testTxID, "status": 202, "txStatus": string(StatusReceived),
			})
		case "/health":
			cb.hpHits.Add(1)
			writeJSON(t, w, http.StatusOK, map[string]any{"healthy": cb.healthy.Load()})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(cb.server.Close)
	return cb
}

func TestCircuitBreaker_OpensAfterThresholdThenProbesAndCloses(t *testing.T) {
	srv := newCBServer(t)
	srv.txFail.Store(true) // arcade is broken

	cfg := defaultConfig(srv.server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 3, HealthProbeIntervalSeconds: 1}
	client := newClient(t, cfg)

	// Use a controllable clock so the probe-throttle window is deterministic.
	fake := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	client.breaker.now = fake.now

	ef := mustDecodeEF(t)

	// 3 consecutive 5xx failures trip the breaker.
	for i := 0; i < 3; i++ {
		_, err := client.Broadcast(t.Context(), testTxID, ef)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrCircuitOpen)
	}
	require.Equal(t, int32(3), srv.txHits.Load())

	// Next broadcast: breaker open, within the probe interval -> fail fast,
	// no /tx dial and no /health probe yet.
	_, err := client.Broadcast(t.Context(), testTxID, ef)
	require.ErrorIs(t, err, ErrCircuitOpen)
	require.Equal(t, int32(3), srv.txHits.Load(), "open breaker must not dial /tx")
	require.Equal(t, int32(0), srv.hpHits.Load(), "probe throttled within interval")

	// Advance past the probe interval; arcade still unhealthy -> probe fires,
	// breaker stays open.
	fake.advance(2 * time.Second)
	_, err = client.Broadcast(t.Context(), testTxID, ef)
	require.ErrorIs(t, err, ErrCircuitOpen)
	require.Equal(t, int32(1), srv.hpHits.Load(), "unhealthy probe fired once")
	require.Equal(t, int32(3), srv.txHits.Load())

	// Arcade recovers; advance again -> probe succeeds, breaker closes, /tx dials.
	srv.txFail.Store(false)
	srv.healthy.Store(true)
	fake.advance(2 * time.Second)
	res, err := client.Broadcast(t.Context(), testTxID, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int32(2), srv.hpHits.Load(), "healthy probe fired to close")
	require.Equal(t, int32(4), srv.txHits.Load(), "closed breaker dials /tx again")

	// And it stays closed for subsequent successes.
	_, err = client.Broadcast(t.Context(), testTxID, ef)
	require.NoError(t, err)
	require.Equal(t, int32(5), srv.txHits.Load())
	require.Equal(t, int32(2), srv.hpHits.Load(), "no further probes while closed")
}

func TestCircuitBreaker_DisabledWhenThresholdZero(t *testing.T) {
	srv := newCBServer(t)
	srv.txFail.Store(true)

	cfg := defaultConfig(srv.server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 0}
	client := newClient(t, cfg)

	ef := mustDecodeEF(t)
	for i := 0; i < 5; i++ {
		_, err := client.Broadcast(t.Context(), testTxID, ef)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrCircuitOpen, "disabled breaker never short-circuits")
	}
	require.Equal(t, int32(5), srv.txHits.Load())
}

// 503 backpressure and 4xx rejections are arcade-is-serving signals; they must
// NOT trip the breaker.
func TestCircuitBreaker_BackpressureDoesNotTrip(t *testing.T) {
	var mode atomic.Int32 // 0 => 503, 1 => 202
	var txHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		txHits.Add(1)
		if mode.Load() == 0 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(t, w, http.StatusAccepted, map[string]any{
			"txid": testTxID, "status": 202, "txStatus": string(StatusReceived),
		})
	}))
	defer server.Close()

	cfg := defaultConfig(server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 2, HealthProbeIntervalSeconds: 1}
	client := newClient(t, cfg)

	ef := mustDecodeEF(t)
	for i := 0; i < 5; i++ {
		_, err := client.Broadcast(t.Context(), testTxID, ef)
		var bp *BackpressureError
		require.ErrorAs(t, err, &bp)
	}
	// Breaker never opened: a subsequent success dials /tx normally.
	mode.Store(1)
	res, err := client.Broadcast(t.Context(), testTxID, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int32(6), txHits.Load(), "breaker stayed closed across all 503s")
}

// A canceled caller context says nothing about arcade's health, so it must NOT
// trip the breaker. This is the blast-stop / errgroup-cancel case: many workers
// abort at once and the breaker used to count every one of them as an outage,
// then refuse healthy broadcasts long afterwards.
func TestCircuitBreaker_CallerCancellationDoesNotTrip(t *testing.T) {
	srv := newCBServer(t)
	srv.healthy.Store(true)

	cfg := defaultConfig(srv.server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 2, HealthProbeIntervalSeconds: 1}
	client := newClient(t, cfg)
	client.breaker.now = (&fakeClock{t: time.Unix(1_700_000_000, 0)}).now

	ef := mustDecodeEF(t)

	// Far more cancellations than the threshold, as a blast stop delivers.
	for range 10 {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := client.Broadcast(ctx, testTxID, ef)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, ErrCircuitOpen)
	}

	// An expired deadline is the same signal and must behave the same.
	for range 10 {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		_, err := client.Broadcast(ctx, testTxID, ef)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrCircuitOpen)
	}

	// The breaker never opened: healthy traffic still reaches /tx, on the very
	// next call and without waiting for a /health probe to close anything.
	before := srv.txHits.Load()
	res, err := client.Broadcast(t.Context(), testTxID, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, before+1, srv.txHits.Load(), "breaker stayed closed across every cancellation")
	require.Equal(t, int32(0), srv.hpHits.Load(), "a closed breaker never probes /health")
}

// A cancellation mixed into a genuine outage must not *reset* the failure count
// either: it is neither success nor failure, so the real 5xx failures either
// side of it still add up to an open breaker.
func TestCircuitBreaker_CancellationDoesNotResetFailureCount(t *testing.T) {
	srv := newCBServer(t)
	srv.txFail.Store(true) // arcade is genuinely broken

	cfg := defaultConfig(srv.server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 2, HealthProbeIntervalSeconds: 1}
	client := newClient(t, cfg)
	client.breaker.now = (&fakeClock{t: time.Unix(1_700_000_000, 0)}).now

	ef := mustDecodeEF(t)

	_, err := client.Broadcast(t.Context(), testTxID, ef)
	require.Error(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = client.Broadcast(ctx, testTxID, ef)
	require.ErrorIs(t, err, context.Canceled)

	_, err = client.Broadcast(t.Context(), testTxID, ef)
	require.Error(t, err)

	// Two real failures reached the threshold despite the cancellation between them.
	_, err = client.Broadcast(t.Context(), testTxID, ef)
	require.ErrorIs(t, err, ErrCircuitOpen)
}

// An open breaker must not spend its throttled /health probe on a caller whose
// context is already dead: that probe cannot succeed, and burning it delays the
// recovery of every live caller by a full probe interval.
func TestCircuitBreaker_DeadContextDoesNotBurnTheProbeSlot(t *testing.T) {
	srv := newCBServer(t)
	srv.txFail.Store(true)

	cfg := defaultConfig(srv.server.URL)
	cfg.CircuitBreaker = defs.ArcadeCircuitBreaker{FailureThreshold: 2, HealthProbeIntervalSeconds: 1}
	client := newClient(t, cfg)
	fake := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	client.breaker.now = fake.now

	ef := mustDecodeEF(t)
	for range 2 {
		_, err := client.Broadcast(t.Context(), testTxID, ef)
		require.Error(t, err)
	}

	// Arcade recovers, and the probe window opens.
	srv.txFail.Store(false)
	srv.healthy.Store(true)
	fake.advance(2 * time.Second)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := client.Broadcast(ctx, testTxID, ef)
	require.ErrorIs(t, err, ErrCircuitOpen)
	require.Equal(t, int32(0), srv.hpHits.Load(), "a dead context must not consume the probe")

	// The live caller behind it still gets its probe in the same interval.
	res, err := client.Broadcast(t.Context(), testTxID, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int32(1), srv.hpHits.Load())
}

// fakeClock is a monotonic test clock guarded for use across the breaker's
// probe goroutine-free path (Broadcast is synchronous in these tests).
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
