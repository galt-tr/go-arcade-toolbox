package mockarcade

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// wireHeader is the exact JSON block-header shape pkg/headers.Client decodes
// from ChainTracks (GET /header/height/{n}, /tip, and the tip SSE stream). The
// hash fields go on the wire as byte-reversed hex strings — the form the
// client's chainhash.Hash JSON decoder expects — via the MarshalJSON below.
type wireHeader struct {
	Version    int32
	PrevHash   chainhash.Hash
	MerkleRoot chainhash.Hash
	Timestamp  uint32
	Bits       uint32
	Nonce      uint32
	Height     uint32
	Hash       chainhash.Hash
}

// MarshalJSON emits the ChainTracks wire shape with hash fields as byte-reversed
// hex strings. A value receiver ensures it is used even for non-addressable
// struct fields (a pointer-receiver MarshalJSON on chainhash.Hash is not).
func (h wireHeader) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"version":      h.Version,
		"previousHash": h.PrevHash.String(),
		"merkleRoot":   h.MerkleRoot.String(),
		"time":         h.Timestamp,
		"bits":         h.Bits,
		"nonce":        h.Nonce,
		"height":       h.Height,
		"hash":         h.Hash.String(),
	})
}

// ChainTracks is a mock of the ChainTracks HTTP/SSE service. It serves
// synthetic-but-internally-consistent headers so headers.VerifyMerkleRoot can
// validate a synthetic BUMP: register a merkle root at a height with
// RegisterHeader, and GET /header/height/{n} returns a header carrying that
// root. It implements GET /height, /header/height/{n}, /tip and the SSE
// /tip/stream and /reorg/stream. Safe for concurrent use.
type ChainTracks struct {
	server *httptest.Server

	mu      sync.Mutex
	headers map[uint32]wireHeader // registered synthetic headers by height
	tip     uint32

	subsMu   sync.Mutex
	tipSubs  map[chan sseFrame]struct{}
	reorgSub map[chan sseFrame]struct{}
}

// NewChainTracks starts a mock ChainTracks server and registers cleanup on tb.
func NewChainTracks(tb testing.TB) *ChainTracks {
	tb.Helper()
	c := newChainTracks()
	tb.Cleanup(c.server.Close)
	return c
}

// NewChainTracksServer starts a mock ChainTracks server without a [testing.TB],
// for use from non-test binaries (notably cmd/perfrunner). The caller MUST
// invoke the returned close function to shut the server down.
func NewChainTracksServer() (*ChainTracks, func()) {
	c := newChainTracks()
	return c, c.server.Close
}

// newChainTracks builds and starts the mock ChainTracks server. Shared by the
// test and non-test constructors.
func newChainTracks() *ChainTracks {
	c := &ChainTracks{
		headers:  make(map[uint32]wireHeader),
		tipSubs:  make(map[chan sseFrame]struct{}),
		reorgSub: make(map[chan sseFrame]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /height", c.handleHeight)
	mux.HandleFunc("GET /header/height/{n}", c.handleHeaderByHeight)
	mux.HandleFunc("GET /tip", c.handleTip)
	mux.HandleFunc("GET /tip/stream", c.handleTipStream)
	mux.HandleFunc("GET /reorg/stream", c.handleReorgStream)
	c.server = httptest.NewServer(mux)
	return c
}

// URL is the base URL to hand to defs.ChainTracks.URL.
func (c *ChainTracks) URL() string { return c.server.URL }

// RegisterHeader registers a synthetic header at the given height carrying the
// given merkle root. This is the hook that makes a synthetic BUMP validate:
// VerifyMerkleRoot(root, height) fetches this header and byte-compares its
// merkleRoot. The mock advances its reported tip to at least this height.
func (c *ChainTracks) RegisterHeader(height uint32, merkleRoot chainhash.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headers[height] = c.synthHeaderLocked(height, merkleRoot)
	if height > c.tip {
		c.tip = height
	}
}

// SetTip overrides the height reported by GET /height (defaults to the highest
// registered header height).
func (c *ChainTracks) SetTip(height uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tip = height
}

// synthHeaderLocked builds an internally consistent synthetic header: the block
// hash is the double-SHA256 of the serialized 80-byte header, and prevHash
// chains from the header one below (zero if none). Caller holds c.mu.
func (c *ChainTracks) synthHeaderLocked(height uint32, merkleRoot chainhash.Hash) wireHeader {
	var prev chainhash.Hash
	if h, ok := c.headers[height-1]; ok {
		prev = h.Hash
	}
	h := wireHeader{
		Version:    1,
		PrevHash:   prev,
		MerkleRoot: merkleRoot,
		Timestamp:  uint32(1231006505 + int64(height)*600), //nolint:gosec // synthetic
		Bits:       0x1d00ffff,
		Nonce:      height,
		Height:     height,
	}
	h.Hash = headerHash(h)
	return h
}

// headerHash computes the double-SHA256 of the serialized 80-byte header.
func headerHash(h wireHeader) chainhash.Hash {
	var buf [80]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(h.Version)) //nolint:gosec // synthetic
	copy(buf[4:36], h.PrevHash[:])
	copy(buf[36:68], h.MerkleRoot[:])
	binary.LittleEndian.PutUint32(buf[68:72], h.Timestamp)
	binary.LittleEndian.PutUint32(buf[72:76], h.Bits)
	binary.LittleEndian.PutUint32(buf[76:80], h.Nonce)
	first := sha256.Sum256(buf[:])
	second := sha256.Sum256(first[:])
	return chainhash.Hash(second)
}

func (c *ChainTracks) handleHeight(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	tip := c.tip
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"height": tip})
}

func (c *ChainTracks) handleHeaderByHeight(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.ParseUint(r.PathValue("n"), 10, 32)
	if err != nil {
		http.Error(w, "bad height", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	h, ok := c.headers[uint32(n)]
	c.mu.Unlock()
	if !ok {
		http.Error(w, "header not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (c *ChainTracks) handleTip(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	h, ok := c.headers[c.tip]
	c.mu.Unlock()
	if !ok {
		// No registered headers yet: synthesize a genesis-ish tip.
		writeJSON(w, http.StatusOK, wireHeader{Version: 1, Height: 0, Hash: headerHash(wireHeader{Version: 1})})
		return
	}
	writeJSON(w, http.StatusOK, h)
}

// EmitTip pushes a tip SSE event carrying a synthetic header for height with the
// given merkle root (also registered).
func (c *ChainTracks) EmitTip(height uint32, merkleRoot chainhash.Hash) {
	c.RegisterHeader(height, merkleRoot)
	c.mu.Lock()
	h := c.headers[height]
	c.mu.Unlock()
	data, _ := json.Marshal(h)
	frame := sseFrame{id: strconv.FormatInt(time.Now().UnixNano(), 10), data: string(data)}
	c.subsMu.Lock()
	for ch := range c.tipSubs {
		select {
		case ch <- frame:
		default:
		}
	}
	c.subsMu.Unlock()
}

func (c *ChainTracks) handleTipStream(w http.ResponseWriter, r *http.Request) {
	c.serveSSE(w, r, c.tipSubs)
}

func (c *ChainTracks) handleReorgStream(w http.ResponseWriter, r *http.Request) {
	c.serveSSE(w, r, c.reorgSub)
}

func (c *ChainTracks) serveSSE(w http.ResponseWriter, r *http.Request, set map[chan sseFrame]struct{}) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan sseFrame, 16)
	c.subsMu.Lock()
	set[ch] = struct{}{}
	c.subsMu.Unlock()
	defer func() {
		c.subsMu.Lock()
		delete(set, ch)
		c.subsMu.Unlock()
	}()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			_, _ = w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		case f := <-ch:
			_, _ = w.Write([]byte("id: " + f.id + "\ndata: " + f.data + "\n\n"))
			flusher.Flush()
		}
	}
}
