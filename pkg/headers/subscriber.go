package headers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/internal/sse"
)

// SSE tuning shared by the tip and reorg streams. The transport plumbing itself
// (reconnect loop, backoff, watchdog, SSE parser) lives in internal/sse; these
// bounds are handed to it per stream and stay overridable in tests.
const (
	// sseBackoffBase and sseBackoffMax bound the reconnect exponential backoff.
	sseBackoffBase = 1 * time.Second
	sseBackoffMax  = 60 * time.Second

	// readWatchdogTimeout drops and redials a connection on which no line has
	// been read for this long. ChainTracks sends `: keepalive` comments every
	// 15s, so 60s of silence means a dead TCP peer.
	readWatchdogTimeout = 60 * time.Second

	// tipChanBuffer / reorgChanBuffer are the small out-channel buffers so a
	// briefly-busy consumer does not immediately stall the reader.
	tipChanBuffer   = 8
	reorgChanBuffer = 8
)

// SubscribeTip implements [ChainSubscriber.SubscribeTip]. It opens the
// /tip/stream SSE, emitting a TipEvent for each tip the stream carries, and
// auto-reconnects with backoff on any transport error — the channel stays open
// across the gap and closes only when ctx is canceled.
//
// After every (re)connect it emits a fresh TipEvent carrying the current tip
// (fetched via GET /tip), so a consumer converges on real chain state after a
// gap without waiting for the next block. Consecutive identical tips are
// de-duplicated so the on-connect fetch and the server's on-connect frame do
// not double-emit.
func (c *Client) SubscribeTip(ctx context.Context) <-chan TipEvent {
	out := make(chan TipEvent, tipChanBuffer)

	go func() {
		defer close(out)

		// last/have are touched only by this goroutine (the SSE reader calls the
		// callbacks synchronously from here), so no lock is needed.
		var last chainhash.Hash
		var have bool

		send := func(ev TipEvent) {
			select {
			case out <- ev:
			case <-ctx.Done():
			}
			last, have = ev.Hash, true
		}

		reader := sse.New(sse.Config{
			Name:    "chaintracks-tip",
			Logger:  c.logger,
			Client:  c.sseClient,
			Request: c.sseRequest(c.tipStreamURL),
			OnConnect: func(connCtx context.Context) {
				// Force a fresh emission on this (re)connect: reset dedup, then
				// emit the current tip. If the fetch fails, the reset ensures the
				// stream's own on-connect frame still emits a fresh tip.
				have = false
				ev, err := c.fetchTip(connCtx)
				if err != nil {
					c.logger.DebugContext(connCtx, "tip stream: current-tip fetch after connect failed",
						slog.String("error", err.Error()))
					return
				}
				send(ev)
			},
			Handler: func(_ context.Context, f sse.Frame) bool {
				if f.Data == "" {
					return false
				}
				var w wireHeader
				if err := json.Unmarshal([]byte(f.Data), &w); err != nil {
					c.logger.WarnContext(ctx, "tip stream: skipping malformed frame",
						slog.String("error", err.Error()))
					return false
				}
				c.observeTip(w.Height, w.Hash)
				ev := TipEvent{Height: w.Height, Hash: w.Hash}
				if have && last.IsEqual(&ev.Hash) {
					return false
				}
				send(ev)
				return true
			},
			// A connection that read at least one line was healthy; reset backoff.
			// One that never got that far keeps backing off to avoid a hot-loop.
			ResetBackoff:    func(r sse.ConnResult) bool { return r.ReadAny },
			BackoffBase:     c.sseBackoffBase,
			BackoffMax:      c.sseBackoffMax,
			WatchdogTimeout: c.sseReadWatchdog,
		})

		_ = reader.Run(ctx)
	}()

	return out
}

// SubscribeReorg implements [ChainSubscriber.SubscribeReorg]. It opens the
// /reorg/stream SSE and emits a ReorgEvent for each reorg, auto-reconnecting
// with backoff on transport errors; the channel closes only when ctx is
// canceled. The stream is BEST-EFFORT: reorgs occurring during a reconnect gap
// are missed and never replayed (there is no on-connect snapshot).
//
// Every delivered reorg evicts the header cache at or above its fork height
// before the event is forwarded, so a consumer that acts on the event never
// races an orphaned header out of the cache.
func (c *Client) SubscribeReorg(ctx context.Context) <-chan ReorgEvent {
	out := make(chan ReorgEvent, reorgChanBuffer)

	go func() {
		defer close(out)

		reader := sse.New(sse.Config{
			Name:    "chaintracks-reorg",
			Logger:  c.logger,
			Client:  c.sseClient,
			Request: c.sseRequest(c.reorgStreamURL),
			Handler: func(_ context.Context, f sse.Frame) bool {
				if f.Data == "" {
					return false
				}
				var w wireReorg
				if err := json.Unmarshal([]byte(f.Data), &w); err != nil {
					c.logger.WarnContext(ctx, "reorg stream: skipping malformed frame",
						slog.String("error", err.Error()))
					return false
				}
				if w.NewTip == nil {
					c.logger.WarnContext(ctx, "reorg stream: skipping event without new tip",
						slog.Int("orphanedCount", len(w.OrphanedHashes)))
					return false
				}

				ev := c.toReorgEvent(&w)
				// Evict before forwarding so a consumer observing the event never
				// finds a stale orphaned header still cached.
				c.evictFrom(ev.ForkHeight)

				select {
				case out <- ev:
				case <-ctx.Done():
				}
				return true
			},
			ResetBackoff:    func(r sse.ConnResult) bool { return r.ReadAny },
			BackoffBase:     c.sseBackoffBase,
			BackoffMax:      c.sseBackoffMax,
			WatchdogTimeout: c.sseReadWatchdog,
		})

		_ = reader.Run(ctx)
	}()

	return out
}

// sseRequest returns a request builder for the given SSE stream URL: a GET with
// the standard event-stream headers, bound to the per-connection context the
// reader passes it. The chaintracks streams carry no Last-Event-ID resume.
func (c *Client) sseRequest(url string) func(context.Context) (*http.Request, error) {
	return func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("User-Agent", userAgent)
		return req, nil
	}
}

// toReorgEvent maps the wire reorg shape onto the toolbox [ReorgEvent].
// OldTip is best-effort: the payload provides no single labeled old tip and the
// orphaned hashes carry no heights, so OldTip is set only when the last tip seen
// on the tip stream is itself among the orphaned blocks; otherwise it is zero.
func (c *Client) toReorgEvent(w *wireReorg) ReorgEvent {
	ev := ReorgEvent{
		NewTip:         w.NewTip.Hash,
		OrphanedHashes: w.OrphanedHashes,
	}
	if w.CommonAncestor != nil {
		ev.ForkHeight = w.CommonAncestor.Height
	}

	c.lastTipMu.RLock()
	lastHash, haveLast := c.lastTipHash, c.lastTipSet
	c.lastTipMu.RUnlock()
	if haveLast {
		for i := range w.OrphanedHashes {
			if w.OrphanedHashes[i].IsEqual(&lastHash) {
				ev.OldTip = lastHash
				break
			}
		}
	}

	return ev
}

// fetchTip fetches the current tip via GET /tip and records it (height+hash).
func (c *Client) fetchTip(ctx context.Context) (TipEvent, error) {
	body := &wireHeader{}
	response, err := c.rest.R().
		SetContext(ctx).
		SetResult(body).
		Get(c.tipURL)
	if err != nil {
		return TipEvent{}, fmt.Errorf("failed to fetch chaintracks tip: %w", err)
	}
	if !response.IsSuccess() {
		return TipEvent{}, fmt.Errorf("chaintracks tip returned http status [%d %s]", response.StatusCode(), response.Status())
	}

	c.observeTip(body.Height, body.Hash)
	return TipEvent{Height: body.Height, Hash: body.Hash}, nil
}
