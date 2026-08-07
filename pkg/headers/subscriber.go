package headers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// SSE tuning shared by the tip and reorg streams. These mirror pkg/arcade/sse.go
// (the toolbox's proven status-stream reader); the pattern is duplicated here
// rather than extracted into a shared package because arcade's implementation is
// tightly bound to its own Client type, and a focused ~single-file reader keeps
// the headers package self-contained. See the report note.
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

	sseScannerInitialBufferSize = 64 * 1024
	sseScannerMaxBufferSize     = 1 << 20
)

// newSSEClient builds an HTTP client for the long-lived SSE streams: no overall
// timeout (cancellation is context-driven) with sane dial/TLS/header timeouts.
func newSSEClient() *http.Client {
	return &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}
}

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

		// last/have are touched only by this goroutine (streamSSE calls the
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

		c.streamSSE(ctx, "tip", c.tipStreamURL,
			func(connCtx context.Context) {
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
			func(data string) {
				var w wireHeader
				if err := json.Unmarshal([]byte(data), &w); err != nil {
					c.logger.WarnContext(ctx, "tip stream: skipping malformed frame",
						slog.String("error", err.Error()))
					return
				}
				c.observeTip(w.Height, w.Hash)
				ev := TipEvent{Height: w.Height, Hash: w.Hash}
				if have && last.IsEqual(&ev.Hash) {
					return
				}
				send(ev)
			},
		)
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

		c.streamSSE(ctx, "reorg", c.reorgStreamURL, nil, func(data string) {
			var w wireReorg
			if err := json.Unmarshal([]byte(data), &w); err != nil {
				c.logger.WarnContext(ctx, "reorg stream: skipping malformed frame",
					slog.String("error", err.Error()))
				return
			}
			if w.NewTip == nil {
				c.logger.WarnContext(ctx, "reorg stream: skipping event without new tip",
					slog.Int("orphanedCount", len(w.OrphanedHashes)))
				return
			}

			ev := c.toReorgEvent(&w)
			// Evict before forwarding so a consumer observing the event never
			// finds a stale orphaned header still cached.
			c.evictFrom(ev.ForkHeight)

			select {
			case out <- ev:
			case <-ctx.Done():
			}
		})
	}()

	return out
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

// streamSSE runs the reconnect loop for one SSE endpoint until ctx is canceled.
// onConnect (may be nil) runs once after each successful connect; onData runs
// for each SSE data frame. name labels the stream in logs.
func (c *Client) streamSSE(ctx context.Context, name, url string, onConnect func(context.Context), onData func(string)) {
	backoff := c.sseBackoffBase

	for {
		if ctx.Err() != nil {
			return
		}

		progressed := c.streamOnce(ctx, url, onConnect, onData)
		if ctx.Err() != nil {
			return
		}

		// A connection that read at least one line was healthy; reset backoff.
		// One that never got that far keeps backing off to avoid a hot-loop.
		if progressed {
			backoff = c.sseBackoffBase
		}

		c.logger.DebugContext(ctx, "chaintracks sse stream interrupted, reconnecting",
			slog.String("stream", name),
			slog.Duration("backoff", backoff))

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.sseBackoffMax)
	}
}

// streamOnce performs a single SSE connection and dispatches data frames until
// the stream ends. It reports whether any line was read (used to reset backoff).
// A per-connection context backs a read-liveness watchdog: when no line arrives
// within the watchdog timeout the connection (not the outer ctx) is canceled so
// the stream reconnects instead of hanging on a dead peer.
func (c *Client) streamOnce(ctx context.Context, url string, onConnect func(context.Context), onData func(string)) (progressed bool) {
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.WarnContext(ctx, "chaintracks sse: failed to build request",
			slog.String("url", url), slog.String("error", err.Error()))
		return false
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", userAgent)

	response, err := c.sseClient.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.WarnContext(ctx, "chaintracks sse: failed to connect",
				slog.String("url", url), slog.String("error", err.Error()))
		}
		return false
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, sseScannerInitialBufferSize))
		c.logger.WarnContext(ctx, "chaintracks sse: unexpected http status",
			slog.String("url", url), slog.Int("status", response.StatusCode))
		return false
	}

	if onConnect != nil {
		onConnect(ctx)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, sseScannerInitialBufferSize), sseScannerMaxBufferSize)

	watchdog := time.AfterFunc(c.sseReadWatchdog, cancelConn)
	defer watchdog.Stop()

	var data string
	for scanner.Scan() {
		watchdog.Reset(c.sseReadWatchdog)
		progressed = true
		line := scanner.Text()

		// A blank line ends a frame: dispatch accumulated data and reset.
		if line == "" {
			if data != "" {
				onData(data)
			}
			data = ""
			continue
		}
		// Comments (": keepalive") are ignored; only "data:" lines accumulate.
		if strings.HasPrefix(line, ":") {
			continue
		}
		if value, ok := sseDataValue(line); ok {
			if data != "" {
				data += "\n"
			}
			data += value
		}
	}
	// An incomplete trailing frame is discarded per the SSE spec.

	return progressed
}

// sseDataValue returns the value of a "data:" SSE field line (stripping the one
// optional leading space), and whether the line was a data field. Non-data
// fields (id/event/…) are irrelevant to these streams and ignored.
func sseDataValue(line string) (string, bool) {
	field, value, found := strings.Cut(line, ":")
	if !found || field != "data" {
		return "", false
	}
	return strings.TrimPrefix(value, " "), true
}
