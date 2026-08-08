package headers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sse"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
)

// userAgent identifies this client on every request.
const userAgent = "go-arcade-toolbox"

// defaultCacheDepth is the tip-distance guard for the immutable-header cache: a
// header at height h is cached only once h is at least this many blocks below
// the last-observed tip. It mirrors the ChainTracks server's own cache boundary
// (it marks heights below tip-100 as immutable), so a cached header is one the
// server itself considers settled and no longer subject to reorg.
const defaultCacheDepth uint32 = 100

// ErrHeaderNotFound is returned by [Client.HeaderByHeight] when ChainTracks has
// no header for the requested height (a 404) — either the height is above the
// current tip or otherwise unknown to the service. It is a definitive negative
// answer, not a transport failure; consumers can branch on it via errors.Is.
var ErrHeaderNotFound = errors.New("headers: header not found")

// Compile-time proof that the concrete client satisfies both contracts.
var (
	_ Headers         = (*Client)(nil)
	_ ChainSubscriber = (*Client)(nil)
)

// wireHeader is the JSON block-header shape ChainTracks emits on its header
// endpoints and tip stream. It matches go-chaintracks' chaintracks.BlockHeader
// (an embedded go-sdk block.Header — version/previousHash/merkleRoot/time/bits/
// nonce — plus height and hash); chainhash.Hash (un)marshals as byte-reversed
// hex on the wire.
type wireHeader struct {
	Version    int32          `json:"version"`
	PrevHash   chainhash.Hash `json:"previousHash"`
	MerkleRoot chainhash.Hash `json:"merkleRoot"`
	Timestamp  uint32         `json:"time"`
	Bits       uint32         `json:"bits"`
	Nonce      uint32         `json:"nonce"`
	Height     uint32         `json:"height"`
	Hash       chainhash.Hash `json:"hash"`
}

// toHeader projects the wire shape onto the toolbox's clean [Header].
func (w *wireHeader) toHeader() *Header {
	return &Header{
		Version:    w.Version,
		PrevBlock:  w.PrevHash,
		MerkleRoot: w.MerkleRoot,
		Timestamp:  w.Timestamp,
		Bits:       w.Bits,
		Nonce:      w.Nonce,
		Height:     w.Height,
		Hash:       w.Hash,
	}
}

// wireReorg is the JSON reorg shape ChainTracks emits on its reorg stream. It
// matches go-chaintracks' chaintracks.ReorgEvent.
type wireReorg struct {
	OrphanedHashes []chainhash.Hash `json:"orphanedHashes"`
	CommonAncestor *wireHeader      `json:"commonAncestor"`
	NewTip         *wireHeader      `json:"newTip"`
	Depth          uint32           `json:"depth"`
}

// heightResponse is the JSON body of ChainTracks' GET /height.
type heightResponse struct {
	Height uint32 `json:"height"`
}

// Client is the concrete ChainTracks-backed [Headers] + [ChainSubscriber].
//
// # Transport
//
// It talks HTTP/SSE directly to the ChainTracks route surface Arcade serves
// under the configured base URL (…/chaintracks/v2): GET /height, GET /tip, GET
// /header/height/{n}, and the SSE streams /tip/stream and /reorg/stream — the
// client appends those paths beneath the base verbatim, matching the config's
// URL contract. REST calls use a resty client; the long-lived SSE streams use a
// dedicated net/http client with no overall timeout (cancellation is
// context-driven).
//
// # Local verification
//
// VerifyMerkleRoot is performed LOCALLY (fetch the header for the height, then
// byte-compare merkle roots); ChainTracks exposes no isValidRootForHeight route,
// so the SPV decision stays on this side of the wire. See [Client.VerifyMerkleRoot].
//
// # Header cache
//
// Headers at heights at or below tip-N (N = cacheDepth, default
// [defaultCacheDepth]) are cached by height, so a repeated HeaderByHeight —
// including the ones VerifyMerkleRoot drives — costs no HTTP round-trip. A reorg
// event evicts every cached entry at or above its fork height (see
// [Client.SubscribeReorg]). With the default depth only immutable heights are
// cached; a throughput deployment may lower cacheDepth (see [WithCacheDepth]) to
// cache recent heights too — the reorg eviction keeps that SPV-safe (see
// [Client.maybeCache]).
type Client struct {
	logger    *slog.Logger
	rest      *resty.Client
	sseClient *http.Client

	baseURL        string
	heightURL      string
	tipURL         string
	tipStreamURL   string
	reorgStreamURL string

	cacheDepth uint32

	// tipHeight is the last-observed tip height (from CurrentHeight, a tip fetch
	// or the tip stream). It guards the immutable-header cache. Zero means the
	// tip is not yet known, in which case nothing is cached.
	tipHeight atomic.Uint32

	// lastTip tracks the most recent tip hash seen on the tip stream, used only
	// as a best-effort source for ReorgEvent.OldTip (see toReorgEvent).
	lastTipMu   sync.RWMutex
	lastTipHash chainhash.Hash
	lastTipSet  bool

	cacheMu sync.RWMutex
	cache   map[uint32]*Header

	// SSE tuning (overridable in tests via options).
	sseBackoffBase  time.Duration
	sseBackoffMax   time.Duration
	sseReadWatchdog time.Duration
}

// New constructs a ChainTracks headers client for the base URL in cfg. The URL
// is used as-is: request paths (/height, /tip, /header/height/{n},
// /tip/stream, /reorg/stream) are appended beneath it. It errors if cfg.URL is
// empty.
func New(logger *slog.Logger, cfg defs.ChainTracks, opts ...Option) (*Client, error) {
	logger = logging.Child(logger, "chaintracks")

	base := strings.TrimSuffix(strings.TrimSpace(cfg.URL), "/")
	if base == "" {
		return nil, fmt.Errorf("chaintracks url is empty")
	}

	c := &Client{
		logger:          logger,
		baseURL:         base,
		heightURL:       base + "/height",
		tipURL:          base + "/tip",
		tipStreamURL:    base + "/tip/stream",
		reorgStreamURL:  base + "/reorg/stream",
		cacheDepth:      defaultCacheDepth,
		cache:           make(map[uint32]*Header),
		sseBackoffBase:  sseBackoffBase,
		sseBackoffMax:   sseBackoffMax,
		sseReadWatchdog: readWatchdogTimeout,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.rest == nil {
		c.rest = resty.New()
	}
	c.rest = c.rest.
		SetHeader("Accept", "application/json").
		SetHeader("User-Agent", userAgent).
		SetLogger(logging.RestyAdapter(logger)).
		SetDebug(logging.IsDebug(logger))

	if c.sseClient == nil {
		c.sseClient = sse.NewHTTPClient()
	}

	return c, nil
}

// CurrentHeight returns the height of the current chain tip via GET /height.
func (c *Client) CurrentHeight(ctx context.Context) (uint32, error) {
	body := &heightResponse{}
	response, err := c.rest.R().
		SetContext(ctx).
		SetResult(body).
		Get(c.heightURL)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch chaintracks height: %w", err)
	}
	if !response.IsSuccess() {
		return 0, fmt.Errorf("chaintracks height returned http status [%d %s]", response.StatusCode(), response.Status())
	}

	c.observeTipHeight(body.Height)
	return body.Height, nil
}

// HeaderByHeight returns the block header at height. Immutable (deep) headers
// are served from the cache; otherwise the header is fetched from GET
// /header/height/{height}. A 404 is mapped to [ErrHeaderNotFound]; any other
// non-2xx or a transport failure is returned as an error.
func (c *Client) HeaderByHeight(ctx context.Context, height uint32) (*Header, error) {
	if h := c.cacheGet(height); h != nil {
		return h, nil
	}

	body := &wireHeader{}
	url := fmt.Sprintf("%s/header/height/%d", c.baseURL, height)
	response, err := c.rest.R().
		SetContext(ctx).
		SetResult(body).
		Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch chaintracks header at height %d: %w", height, err)
	}

	switch {
	case response.StatusCode() == http.StatusNotFound:
		return nil, fmt.Errorf("chaintracks has no header at height %d: %w", height, ErrHeaderNotFound)
	case !response.IsSuccess():
		return nil, fmt.Errorf("chaintracks header at height %d returned http status [%d %s]", height, response.StatusCode(), response.Status())
	}

	header := body.toHeader()
	c.maybeCache(height, header)
	return header, nil
}

// VerifyMerkleRoot reports whether root is the merkle root of the block at
// height, verified LOCALLY: it fetches the header for height (see
// [Client.HeaderByHeight]) and byte-compares root against that header's merkle
// root. ChainTracks exposes no isValidRootForHeight route, so the comparison is
// deliberately performed here — this is the toolbox's SPV trust anchor.
//
// Beyond-tip decision (see the type doc): when the header is missing AND the
// height is above the current tip, the block simply has not been seen yet, so a
// proof against it is not-yet-valid — this returns (false, nil), not an error.
// A miss at or below the tip is a genuine retrieval failure and returns
// (false, err), as does any transport/5xx error. A found header whose root
// differs returns (false, nil).
func (c *Client) VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	header, err := c.HeaderByHeight(ctx, height)
	if err != nil {
		if !errors.Is(err, ErrHeaderNotFound) {
			return false, fmt.Errorf("verify merkle root at height %d: %w", height, err)
		}

		// The header is missing. Distinguish "above the tip" (not-yet-valid,
		// false+nil) from "at/below the tip but unknown" (a real miss, error).
		tip, tipErr := c.CurrentHeight(ctx)
		if tipErr != nil {
			return false, fmt.Errorf("verify merkle root at height %d: cannot determine tip after header miss: %w", height, tipErr)
		}
		if height > tip {
			return false, nil
		}
		return false, fmt.Errorf("verify merkle root at height %d: %w", height, err)
	}

	return header.MerkleRoot.IsEqual(root), nil
}

// observeTipHeight records the last-known tip height (guards the cache).
func (c *Client) observeTipHeight(height uint32) {
	c.tipHeight.Store(height)
}

// observeTip records the last-known tip height and hash. The hash feeds the
// best-effort ReorgEvent.OldTip derivation.
func (c *Client) observeTip(height uint32, hash chainhash.Hash) {
	c.observeTipHeight(height)
	c.lastTipMu.Lock()
	c.lastTipHash = hash
	c.lastTipSet = true
	c.lastTipMu.Unlock()
}

// cacheGet returns the cached header for height, or nil on a miss.
func (c *Client) cacheGet(height uint32) *Header {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	return c.cache[height]
}

// maybeCache caches header for any block at height <= tip-cacheDepth against the
// last-observed tip; heights above the tip, and the case where no tip is yet
// known, are never cached. With the default cacheDepth every cached header is
// immutable (deep enough to never reorg). A high-throughput deployment MAY lower
// cacheDepth (even to 0) to also cache recent, still-reorgable headers — the
// win being that the ~thousand proofs sharing one block do a single header fetch
// instead of one each. This stays SPV-safe: a reorg evicts every cached header
// at or above its fork BEFORE any consumer acts on it (see SubscribeReorg), and
// DemoteReorgedProofs re-verifies affected proofs, so a cached header can only be
// an orphan for the brief, self-healing window until the reorg event lands.
func (c *Client) maybeCache(height uint32, header *Header) {
	tip := c.tipHeight.Load()
	if tip == 0 || height+c.cacheDepth > tip {
		return
	}
	c.cacheMu.Lock()
	c.cache[height] = header
	c.cacheMu.Unlock()
}

// evictFrom drops every cached header at or above forkHeight. Called on a reorg
// so no orphaned block's header survives in the cache.
func (c *Client) evictFrom(forkHeight uint32) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	for h := range c.cache {
		if h >= forkHeight {
			delete(c.cache, h)
		}
	}
}
