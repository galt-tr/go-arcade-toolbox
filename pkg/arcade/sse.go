package arcade

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sse"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/txtrace"
)

const (
	sseBackoffBase = 1 * time.Second
	sseBackoffMax  = 60 * time.Second

	// readWatchdogTimeout is the default read-liveness watchdog of the SSE
	// stream: arcade sends `: keepalive` comments every 15s, so 60s without a
	// single line means a dead TCP peer - the connection is dropped and
	// redialed instead of hanging.
	readWatchdogTimeout = 60 * time.Second

	// sseScannerMaxBufferSize is the maximum size of a single SSE line (1 MiB),
	// mirroring the shared reader's cap. Retained here so the oversized-frame
	// test can build a line that trips it.
	sseScannerMaxBufferSize = sse.MaxLineSize
)

// eventsURL builds the full SSE endpoint URL, falling back to the base URL when
// EventsURL is not configured and scoping the stream with the callback token.
func eventsURL(config defs.Arcade) string {
	base := config.EventsURL
	if base == "" {
		base = config.URL
	}
	endpoint := base + "/events"
	if config.CallbackToken != "" {
		endpoint += "?callbackToken=" + url.QueryEscape(config.CallbackToken)
	}
	return endpoint
}

// StreamStatus connects to {EventsURL}/events?callbackToken=... and invokes
// onEvent sequentially per status event, honoring the [TxOracle.StreamStatus]
// contract: it auto-reconnects with exponential backoff (1s base, 2x, 60s cap;
// the backoff is reset only after a cleanly-ended connection that delivered at
// least one event - a connection killed by a read error keeps backing off, so a
// permanently oversized frame cannot cause a reconnect hot-loop), resumes with
// Last-Event-ID from the most recently delivered event (or lastEventID before
// any event arrives), and drops+redials a connection on which nothing is read
// for the watchdog timeout.
//
// The transport plumbing (reconnect loop, backoff, watchdog, SSE parser) lives
// in internal/sse; this method supplies only the arcade-specific policy: the
// request (callback token + Last-Event-ID resume header), per-frame dispatch
// (the StatusEvent shape, the at-least-once ns cursor), and the reset-only-after
// -clean-delivery rule.
//
// It blocks until ctx is canceled and then returns ctx.Err(). An error returned
// by onEvent is only logged - the event still counts as delivered (at-least-once).
//
// ONE CONNECTION, DELIBERATELY. A mined block's MINED wave shares this stream
// with live ACCEPTED/SEEN, and measurably head-of-line-blocks it: ACCEPTED
// delivery p50 86ms but p95 21.6s, and the p95 splits entirely by block bucket
// (26,777ms in the bucket containing a 122,640-tx block vs 2,269ms in one
// without). The obvious fix - a second subscription carrying only MINED, the way
// the merkle SEEN/STUMP topic split fixed the same shape upstream - is NOT
// available to a client today and would make things worse if attempted:
// arcade's GET /events parses exactly one query parameter (callbackToken) and
// one header (Last-Event-ID), with no status filter anywhere, so a second
// connection on the same token receives a full duplicate of every event AND
// doubles arcade's per-event cost, because the token->txid membership probe runs
// once per client inside the single fan-out goroutine that is already the
// bottleneck (~1,600 events/s against ~4,000/s of demand at 1000 TPS).
//
// Splitting this stream therefore needs an arcade-side change first: a status
// filter on /events (its store layer already accepts an onlyStatuses argument
// for catchup) plus a per-client predicate in the fan-out. Verified against
// arcade v0.11.6.
func (c *Client) StreamStatus(ctx context.Context, lastEventID string, onEvent func(StatusEvent) error) error {
	reader := sse.New(sse.Config{
		Name:   "arcade-events",
		Logger: c.logger,
		Client: c.sseClient,
		Request: func(connCtx context.Context) (*http.Request, error) {
			req, err := http.NewRequestWithContext(connCtx, http.MethodGet, c.eventsURL, nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")
			req.Header.Set("User-Agent", userAgent)
			// Last-Event-ID resumes from the most recently delivered event; the
			// cursor is advanced by dispatchFrame on delivery (at-least-once).
			if lastEventID != "" {
				req.Header.Set("Last-Event-ID", lastEventID)
			}
			return req, nil
		},
		Handler: func(hctx context.Context, f sse.Frame) bool {
			return c.dispatchFrame(hctx, f, &lastEventID, onEvent)
		},
		// Reset backoff only after a cleanly-ended connection that delivered
		// events; a connection that died with a read error (e.g. an oversized
		// frame hitting bufio.ErrTooLong on every reconnect) must keep backing
		// off to avoid a hot-loop.
		ResetBackoff: func(r sse.ConnResult) bool {
			return r.Delivered > 0 && r.Err == nil
		},
		BackoffBase:     c.sseBackoffBase,
		BackoffMax:      c.sseBackoffMax,
		WatchdogTimeout: c.sseReadWatchdogTimeout,
	})

	return reader.Run(ctx)
}

// dispatchFrame parses and delivers one accumulated SSE frame. It reports
// whether the event was delivered to onEvent. Frames with an event type other
// than "status" and frames whose data does not parse as a TxRecord (or carries
// no txid) are skipped without killing the stream. On delivery it advances
// lastEventID (the resume cursor).
func (c *Client) dispatchFrame(ctx context.Context, f sse.Frame, lastEventID *string, onEvent func(StatusEvent) error) bool {
	// Stamp wire-arrival time before any parsing so it reflects when the frame
	// landed, not when apply got to it.
	recvAt := time.Now()
	if f.Data == "" {
		return false
	}
	if f.Event != "" && f.Event != "status" {
		return false
	}

	var record TxRecord
	if err := json.Unmarshal([]byte(f.Data), &record); err != nil {
		c.logger.WarnContext(
			ctx, "skipping malformed arcade status event",
			slog.String("eventID", f.ID),
			slog.String("error", err.Error()),
		)
		return false
	}
	if record.TxID == "" {
		c.logger.WarnContext(ctx, "skipping arcade status event without txid", slog.String("eventID", f.ID))
		return false
	}

	if txtrace.Marked(record.TxID) {
		txtrace.Emit(c.logger, "sse_recv", record.TxID,
			"status", string(record.Status),
			"recv_at", recvAt.Format(time.RFC3339Nano),
			"arcade_event_id", f.ID,
			"arcade_ts", record.Timestamp.Format(time.RFC3339Nano))
	}

	if err := onEvent(StatusEvent{ID: f.ID, Record: record, RecvAt: recvAt}); err != nil {
		c.logger.ErrorContext(
			ctx, "arcade status event handler failed",
			slog.String("eventID", f.ID),
			slog.String("txID", record.TxID),
			slog.String("error", err.Error()),
		)
	}

	// Intentionally NOT SSE-spec behavior (the spec advances Last-Event-ID on
	// every frame carrying an id): Last-Event-ID is advanced only for DELIVERED
	// frames, so frames that were read but not delivered are redelivered by the
	// server after a reconnect (at-least-once delivery).
	if f.ID != "" {
		*lastEventID = f.ID
	}
	return true
}
