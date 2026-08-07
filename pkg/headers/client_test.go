package headers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
)

const testTimeout = 10 * time.Second

// hashN builds a distinct, non-zero chainhash.Hash from a seed byte.
func hashN(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b
	h[31] = b
	return h
}

// testWireHeader builds a wire header with distinct merkle root and block hash.
func testWireHeader(height uint32, merkle, hash byte) wireHeader {
	return wireHeader{
		Version:    536870912,
		PrevHash:   hashN(hash - 1),
		MerkleRoot: hashN(merkle),
		Timestamp:  1700000000 + height,
		Bits:       0x1d00ffff,
		Nonce:      42,
		Height:     height,
		Hash:       hashN(hash),
	}
}

// newTestClient builds a Client pointed at baseURL + the real /chaintracks/v2
// path prefix Arcade serves under, exercising the direct-append URL scheme.
func newTestClient(t *testing.T, baseURL string, opts ...Option) *Client {
	t.Helper()
	cfg := defs.ChainTracks{Enabled: true, URL: baseURL + "/chaintracks/v2"}
	c, err := New(logging.NewTestLogger(t), cfg, opts...)
	require.NoError(t, err)
	return c
}

func writeJSON(t *testing.T, w http.ResponseWriter, code int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	require.NoError(t, json.NewEncoder(w).Encode(v))
}

// writeSSEFrame marshals v (a pointer, so chainhash fields hex-encode) into one
// SSE data frame and flushes it.
func writeSSEFrame(t *testing.T, w http.ResponseWriter, flusher http.Flusher, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	require.NoError(t, err)
	flusher.Flush()
}

func startSSE(t *testing.T, w http.ResponseWriter) http.Flusher {
	t.Helper()
	flusher, ok := w.(http.Flusher)
	require.True(t, ok, "response writer must support flushing")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	return flusher
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

// fastSSE tunes the SSE reconnect for quick, deterministic tests.
func fastSSE() []Option {
	return []Option{WithSSEBackoff(2*time.Millisecond, 20*time.Millisecond)}
}

func TestNew_EmptyURLErrors(t *testing.T) {
	_, err := New(logging.NewTestLogger(t), defs.ChainTracks{Enabled: true, URL: ""})
	require.Error(t, err)
}

func TestCurrentHeight(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/height", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, heightResponse{Height: 800100})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	h, err := c.CurrentHeight(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint32(800100), h)
}

func TestHeaderByHeight_ParseAndCache(t *testing.T) {
	var headerReqs atomic.Int32
	hdr := testWireHeader(700000, 0x11, 0x22)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/height", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, heightResponse{Height: 800100})
	})
	mux.HandleFunc("GET /chaintracks/v2/header/height/{height}", func(w http.ResponseWriter, r *http.Request) {
		headerReqs.Add(1)
		if r.PathValue("height") != "700000" {
			writeJSON(t, w, http.StatusNotFound, map[string]string{"error": "Header not found"})
			return
		}
		writeJSON(t, w, http.StatusOK, &hdr)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	ctx := t.Context()

	// Prime the tip so the immutable-header cache is enabled (700000 < 800100-100).
	_, err := c.CurrentHeight(ctx)
	require.NoError(t, err)

	got, err := c.HeaderByHeight(ctx, 700000)
	require.NoError(t, err)
	assert.Equal(t, uint32(700000), got.Height)
	assert.Equal(t, int32(536870912), got.Version)
	assert.Equal(t, uint32(0x1d00ffff), got.Bits)
	assert.True(t, got.MerkleRoot.IsEqual(&hdr.MerkleRoot), "merkle root round-trips as hex")
	assert.True(t, got.PrevBlock.IsEqual(&hdr.PrevHash), "prev hash maps to PrevBlock")
	assert.True(t, got.Hash.IsEqual(&hdr.Hash), "block hash round-trips")

	// Second call is served from cache: no second HTTP request.
	got2, err := c.HeaderByHeight(ctx, 700000)
	require.NoError(t, err)
	assert.Same(t, got, got2, "cache returns the same header instance")
	assert.Equal(t, int32(1), headerReqs.Load(), "second HeaderByHeight is a cache hit")
}

func TestHeaderByHeight_RecentNotCached(t *testing.T) {
	var headerReqs atomic.Int32
	hdr := testWireHeader(800050, 0x11, 0x22) // within tip-100 of 800100: mutable

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/height", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, heightResponse{Height: 800100})
	})
	mux.HandleFunc("GET /chaintracks/v2/header/height/{height}", func(w http.ResponseWriter, _ *http.Request) {
		headerReqs.Add(1)
		writeJSON(t, w, http.StatusOK, &hdr)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	ctx := t.Context()
	_, err := c.CurrentHeight(ctx)
	require.NoError(t, err)

	_, err = c.HeaderByHeight(ctx, 800050)
	require.NoError(t, err)
	_, err = c.HeaderByHeight(ctx, 800050)
	require.NoError(t, err)
	assert.Equal(t, int32(2), headerReqs.Load(), "recent (mutable) headers are re-fetched, never cached")
}

func TestHeaderByHeight_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/header/height/{height}", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusNotFound, map[string]string{"error": "Header not found"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	_, err := c.HeaderByHeight(t.Context(), 999999)
	require.ErrorIs(t, err, ErrHeaderNotFound)
}

func TestVerifyMerkleRoot(t *testing.T) {
	hdr := testWireHeader(700000, 0x11, 0x22)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/height", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, heightResponse{Height: 800100})
	})
	mux.HandleFunc("GET /chaintracks/v2/header/height/{height}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("height") == "700000" {
			writeJSON(t, w, http.StatusOK, &hdr)
			return
		}
		writeJSON(t, w, http.StatusNotFound, map[string]string{"error": "Header not found"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	ctx := t.Context()

	t.Run("matching root verifies", func(t *testing.T) {
		root := hdr.MerkleRoot
		ok, err := c.VerifyMerkleRoot(ctx, &root, 700000)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("mismatching root fails without error", func(t *testing.T) {
		other := hashN(0x99)
		ok, err := c.VerifyMerkleRoot(ctx, &other, 700000)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("beyond-tip height is false with no error", func(t *testing.T) {
		root := hdr.MerkleRoot
		ok, err := c.VerifyMerkleRoot(ctx, &root, 900000) // > tip 800100
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("miss at or below tip is an error", func(t *testing.T) {
		root := hdr.MerkleRoot
		_, err := c.VerifyMerkleRoot(ctx, &root, 500) // <= tip but 404
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrHeaderNotFound)
	})
}

func TestSubscribeTip_FreshOnConnectAndReconnect(t *testing.T) {
	var conns atomic.Int32
	tip1 := testWireHeader(800100, 0x01, 0x0A) // served by GET /tip
	tip2 := testWireHeader(800101, 0x02, 0x0B) // pushed on the 2nd stream connect

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/tip", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, &tip1)
	})
	mux.HandleFunc("GET /chaintracks/v2/tip/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher := startSSE(t, w)
		if conns.Add(1) == 1 {
			// Close immediately to force a transparent reconnect.
			return
		}
		writeSSEFrame(t, w, flusher, &tip2)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL, fastSSE()...)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	tipCh := c.SubscribeTip(ctx)

	// After the initial connect, a fresh tip (fetched via GET /tip) is emitted.
	first := waitFor(t, tipCh, "initial tip")
	assert.Equal(t, uint32(800100), first.Height)
	assert.True(t, first.Hash.IsEqual(&tip1.Hash))

	// After the transparent reconnect, the new tip frame flows through (a
	// duplicate fresh-on-reconnect tip1 may precede it and is skipped here).
	for {
		ev := waitFor(t, tipCh, "post-reconnect tip")
		if ev.Height == 800101 {
			assert.True(t, ev.Hash.IsEqual(&tip2.Hash))
			break
		}
	}
	assert.GreaterOrEqual(t, conns.Load(), int32(2), "the stream reconnected")

	// Canceling the context closes the channel.
	cancel()
	assertClosed(t, tipCh)
}

func TestSubscribeTip_ClosesOnCancel(t *testing.T) {
	tip1 := testWireHeader(800100, 0x01, 0x0A)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/tip", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, &tip1)
	})
	mux.HandleFunc("GET /chaintracks/v2/tip/stream", func(w http.ResponseWriter, r *http.Request) {
		startSSE(t, w)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL, fastSSE()...)
	ctx, cancel := context.WithCancel(t.Context())
	tipCh := c.SubscribeTip(ctx)

	first := waitFor(t, tipCh, "initial tip")
	assert.Equal(t, uint32(800100), first.Height)

	cancel()
	assertClosed(t, tipCh)
}

func TestSubscribeReorg_MapsFieldsAndEvictsCache(t *testing.T) {
	var headerReqs atomic.Int32
	hdr700 := testWireHeader(700000, 0x30, 0x31)
	common := testWireHeader(699999, 0x40, 0x41) // fork height
	newTip := testWireHeader(800100, 0x50, 0x51)
	orphanA := hashN(0x61)
	orphanB := hashN(0x62)
	reorg := wireReorg{
		OrphanedHashes: []chainhash.Hash{orphanA, orphanB},
		CommonAncestor: &common,
		NewTip:         &newTip,
		Depth:          2,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/height", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, heightResponse{Height: 800100})
	})
	mux.HandleFunc("GET /chaintracks/v2/header/height/{height}", func(w http.ResponseWriter, r *http.Request) {
		headerReqs.Add(1)
		if r.PathValue("height") == "700000" {
			writeJSON(t, w, http.StatusOK, &hdr700)
			return
		}
		writeJSON(t, w, http.StatusNotFound, map[string]string{"error": "Header not found"})
	})
	mux.HandleFunc("GET /chaintracks/v2/reorg/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher := startSSE(t, w)
		writeSSEFrame(t, w, flusher, &reorg)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL, fastSSE()...)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Prime the tip and cache the deep header at 700000.
	_, err := c.CurrentHeight(ctx)
	require.NoError(t, err)
	_, err = c.HeaderByHeight(ctx, 700000)
	require.NoError(t, err)
	require.Equal(t, int32(1), headerReqs.Load())
	require.NotNil(t, c.cacheGet(700000), "deep header is cached")

	reorgCh := c.SubscribeReorg(ctx)
	ev := waitFor(t, reorgCh, "reorg event")

	assert.Equal(t, uint32(699999), ev.ForkHeight, "fork height = common ancestor height")
	assert.True(t, ev.NewTip.IsEqual(&newTip.Hash), "new tip = reorg new-tip hash")
	require.Len(t, ev.OrphanedHashes, 2)
	assert.True(t, ev.OrphanedHashes[0].IsEqual(&orphanA))
	assert.True(t, ev.OrphanedHashes[1].IsEqual(&orphanB))
	assert.Equal(t, chainhash.Hash{}, ev.OldTip, "OldTip is zero (best-effort, no tip stream active)")

	// Eviction happens before the event is forwarded, so the cache entry at/above
	// the fork height is already gone: the next fetch hits the server again.
	assert.Nil(t, c.cacheGet(700000), "reorg evicted the cached header at/above fork height")
	_, err = c.HeaderByHeight(ctx, 700000)
	require.NoError(t, err)
	assert.Equal(t, int32(2), headerReqs.Load(), "header re-fetched after reorg eviction")
}

func TestSubscribeReorg_ReconnectsAndClosesOnCancel(t *testing.T) {
	var conns atomic.Int32
	common := testWireHeader(699999, 0x40, 0x41)
	newTip := testWireHeader(800100, 0x50, 0x51)
	reorg := wireReorg{
		OrphanedHashes: []chainhash.Hash{hashN(0x61)},
		CommonAncestor: &common,
		NewTip:         &newTip,
		Depth:          1,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /chaintracks/v2/reorg/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher := startSSE(t, w)
		if conns.Add(1) == 1 {
			// Drop the first connection: a best-effort stream must not wedge.
			return
		}
		writeSSEFrame(t, w, flusher, &reorg)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestClient(t, srv.URL, fastSSE()...)
	ctx, cancel := context.WithCancel(t.Context())

	reorgCh := c.SubscribeReorg(ctx)
	ev := waitFor(t, reorgCh, "reorg event after reconnect")
	assert.Equal(t, uint32(699999), ev.ForkHeight)
	assert.GreaterOrEqual(t, conns.Load(), int32(2), "the reorg stream reconnected")

	cancel()
	assertClosed(t, reorgCh)
}

func TestToReorgEvent_OldTipBestEffort(t *testing.T) {
	c := newTestClient(t, "http://127.0.0.1:1") // no IO in this test
	common := testWireHeader(699999, 0x40, 0x41)
	newTip := testWireHeader(800100, 0x50, 0x51)
	orphanA := hashN(0x71)
	orphanB := hashN(0x72)
	w := &wireReorg{
		OrphanedHashes: []chainhash.Hash{orphanA, orphanB},
		CommonAncestor: &common,
		NewTip:         &newTip,
		Depth:          2,
	}

	// No tip observed yet: OldTip is zero.
	ev := c.toReorgEvent(w)
	assert.Equal(t, uint32(699999), ev.ForkHeight)
	assert.True(t, ev.NewTip.IsEqual(&newTip.Hash))
	assert.Equal(t, chainhash.Hash{}, ev.OldTip)

	// Last tip seen IS one of the orphans: OldTip is derived.
	c.observeTip(700001, orphanB)
	ev = c.toReorgEvent(w)
	assert.True(t, ev.OldTip.IsEqual(&orphanB), "OldTip derived from last-seen tip among orphans")

	// Last tip seen is NOT among the orphans: OldTip falls back to zero.
	notOrphan := hashN(0x7F)
	c.observeTip(700002, notOrphan)
	ev = c.toReorgEvent(w)
	assert.Equal(t, chainhash.Hash{}, ev.OldTip)
}

func TestCacheGuardAndEviction(t *testing.T) {
	c := newTestClient(t, "http://127.0.0.1:1")

	// Tip unknown: nothing is cached.
	c.maybeCache(1, &Header{Height: 1})
	assert.Nil(t, c.cacheGet(1))

	c.observeTipHeight(800100)
	c.maybeCache(700000, &Header{Height: 700000}) // deep: cached
	c.maybeCache(800050, &Header{Height: 800050}) // recent: not cached
	assert.NotNil(t, c.cacheGet(700000))
	assert.Nil(t, c.cacheGet(800050))

	c.evictFrom(700000)
	assert.Nil(t, c.cacheGet(700000), "evictFrom drops entries at/above the fork height")
}

// assertClosed drains ch and asserts it closes within the timeout.
func assertClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel did not close after context cancellation")
		}
	}
}
