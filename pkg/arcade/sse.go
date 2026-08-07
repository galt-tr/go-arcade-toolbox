package arcade

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
)

const (
	sseBackoffBase = 1 * time.Second
	sseBackoffMax  = 60 * time.Second

	// readWatchdogTimeout is the default read-liveness watchdog of the SSE
	// stream: arcade sends `: keepalive` comments every 15s, so 60s without a
	// single line means a dead TCP peer - the connection is dropped and
	// redialed instead of hanging.
	readWatchdogTimeout = 60 * time.Second

	// sseScannerInitialBufferSize is the initial buffer of the SSE line scanner.
	sseScannerInitialBufferSize = 64 * 1024
	// sseScannerMaxBufferSize is the maximum size of a single SSE line (1 MiB).
	sseScannerMaxBufferSize = 1 << 20
)

// newSSEClient creates an HTTP client for the long-lived SSE stream: no overall
// timeout (cancellation is driven by context), but sane dial/TLS/response-header
// timeouts on the transport.
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
// It blocks until ctx is canceled and then returns ctx.Err(). An error returned
// by onEvent is only logged - the event still counts as delivered (at-least-once).
func (c *Client) StreamStatus(ctx context.Context, lastEventID string, onEvent func(StatusEvent) error) error {
	backoff := c.sseBackoffBase

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		delivered, err := c.streamOnce(ctx, &lastEventID, onEvent)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			c.logger.WarnContext(
				ctx, "arcade events stream interrupted, reconnecting",
				slog.String("error", err.Error()),
				slog.Int("deliveredEvents", delivered),
				slog.Duration("backoff", backoff),
			)
		} else {
			c.logger.DebugContext(
				ctx, "arcade events stream closed by server, reconnecting",
				slog.Int("deliveredEvents", delivered),
				slog.Duration("backoff", backoff),
			)
		}

		// Reset backoff only after a cleanly-ended connection that delivered
		// events; a connection that died with a read error (e.g. an oversized
		// frame hitting bufio.ErrTooLong on every reconnect) must keep backing
		// off to avoid a hot-loop.
		if delivered > 0 && err == nil {
			backoff = c.sseBackoffBase
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, c.sseBackoffMax)
	}
}

// streamOnce performs a single connection to the events endpoint and dispatches
// status events until the stream ends. It returns the number of delivered events.
//
// A per-connection context backs a read-liveness watchdog: when no line is read
// for c.sseReadWatchdogTimeout, the connection (not the outer ctx) is canceled
// so the stream reconnects instead of hanging forever on a dead TCP peer.
func (c *Client) streamOnce(ctx context.Context, lastEventID *string, onEvent func(StatusEvent) error) (delivered int, err error) {
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, c.eventsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create arcade events request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("User-Agent", userAgent)
	if *lastEventID != "" {
		req.Header.Set("Last-Event-ID", *lastEventID)
	}

	response, err := c.sseClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to arcade events stream: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, sseScannerInitialBufferSize))
		return 0, fmt.Errorf("arcade events stream returned unexpected http status [%d %s]", response.StatusCode, response.Status)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, sseScannerInitialBufferSize), sseScannerMaxBufferSize)

	// Read-liveness watchdog: cancel this connection when no line arrives in time.
	watchdog := time.AfterFunc(c.sseReadWatchdogTimeout, cancelConn)
	defer watchdog.Stop()

	var frame sseFrame
	for scanner.Scan() {
		watchdog.Reset(c.sseReadWatchdogTimeout)
		line := scanner.Text()

		// A blank line marks the end of a frame.
		if line == "" {
			if c.dispatchFrame(ctx, frame, lastEventID, onEvent) {
				delivered++
			}
			frame = sseFrame{}
			continue
		}

		// Lines starting with ":" are keepalive comments; all others accumulate.
		if !strings.HasPrefix(line, ":") {
			processSSELine(line, &frame)
		}
	}
	// An incomplete frame at the end of the stream is discarded per the SSE spec.

	return delivered, c.mapScanError(connCtx, ctx, scanner.Err())
}

// mapScanError wraps a scanner read error of the events stream, distinguishing a
// watchdog-triggered stall (connCtx canceled while the outer ctx is still alive)
// from a plain read failure. A nil scanErr is returned as nil.
func (c *Client) mapScanError(connCtx, ctx context.Context, scanErr error) error {
	if scanErr == nil {
		return nil
	}
	// A watchdog-triggered cancellation only kills this connection: the outer
	// ctx is still alive, so StreamStatus reconnects instead of returning.
	if connCtx.Err() != nil && ctx.Err() == nil {
		return fmt.Errorf("arcade events stream stalled (no data for %s): %w", c.sseReadWatchdogTimeout, scanErr)
	}
	return fmt.Errorf("arcade events stream read failed: %w", scanErr)
}

// sseFrame accumulates the fields of one SSE frame until a blank line dispatches it.
type sseFrame struct {
	id    string
	event string
	data  string
}

// dispatchFrame parses and delivers one accumulated SSE frame. It reports
// whether the event was delivered to onEvent. Frames with an event type other
// than "status" and frames whose data does not parse as a TxRecord (or carries
// no txid) are skipped without killing the stream.
func (c *Client) dispatchFrame(ctx context.Context, frame sseFrame, lastEventID *string, onEvent func(StatusEvent) error) bool {
	if frame.data == "" {
		return false
	}
	if frame.event != "" && frame.event != "status" {
		return false
	}

	var record TxRecord
	if err := json.Unmarshal([]byte(frame.data), &record); err != nil {
		c.logger.WarnContext(
			ctx, "skipping malformed arcade status event",
			slog.String("eventID", frame.id),
			slog.String("error", err.Error()),
		)
		return false
	}
	if record.TxID == "" {
		c.logger.WarnContext(ctx, "skipping arcade status event without txid", slog.String("eventID", frame.id))
		return false
	}

	if err := onEvent(StatusEvent{ID: frame.id, Record: record}); err != nil {
		c.logger.ErrorContext(
			ctx, "arcade status event handler failed",
			slog.String("eventID", frame.id),
			slog.String("txID", record.TxID),
			slog.String("error", err.Error()),
		)
	}

	// Intentionally NOT SSE-spec behavior (the spec advances Last-Event-ID on
	// every frame carrying an id): Last-Event-ID is advanced only for DELIVERED
	// frames, so frames that were read but not delivered are redelivered by the
	// server after a reconnect (at-least-once delivery).
	if frame.id != "" {
		*lastEventID = frame.id
	}
	return true
}

// processSSELine accumulates one SSE line into the current frame. Unknown field
// names are silently ignored per the SSE spec.
func processSSELine(line string, frame *sseFrame) {
	field, value := splitSSELine(line)
	switch field {
	case "id":
		frame.id = value
	case "event":
		frame.event = value
	case "data":
		if frame.data != "" {
			frame.data += "\n"
		}
		frame.data += value
	}
}

// splitSSELine splits one SSE line into its field name and value, stripping the
// single optional space after the colon per the SSE spec.
func splitSSELine(line string) (field, value string) {
	field, value, found := strings.Cut(line, ":")
	if !found {
		// A line with no colon is a field with an empty value per the SSE spec.
		return line, ""
	}
	return field, strings.TrimPrefix(value, " ")
}
