// Package sse is the toolbox's shared Server-Sent-Events reader: the reconnect
// loop, exponential backoff, per-connection read-liveness watchdog, growable
// line scanner and SSE frame parser that pkg/arcade (the arcade status stream)
// and pkg/headers (the chaintracks tip/reorg streams) both need.
//
// It owns ONLY the transport plumbing that is genuinely identical between those
// two consumers. Everything package-specific is supplied through [Config]:
//
//   - Request builds the HTTP GET per (re)connect, so each consumer sets its own
//     URL and headers (arcade adds a Last-Event-ID resume header; headers does
//     not).
//   - OnConnect runs once after each successful connect, so headers can emit a
//     fresh tip on every (re)connect. arcade leaves it nil.
//   - Handler receives each parsed [Frame] and decides what to do with it
//     (parse, dispatch, advance a cursor) and whether it counted as delivered.
//   - ResetBackoff decides, from a finished connection's [ConnResult], whether
//     to reset the reconnect backoff. arcade resets only after a clean close
//     that delivered at least one frame (so a permanently-oversized frame cannot
//     hot-loop); headers resets whenever a connection read any line at all.
//
// The reader knows nothing about arcade's StatusEvent / ns-cursor or headers'
// TipEvent / ReorgEvent - those stay in their packages behind the callbacks.
package sse

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// InitialBufferSize is the initial capacity of the SSE line scanner.
	InitialBufferSize = 64 * 1024
	// MaxLineSize is the maximum size of a single SSE line (1 MiB); a longer
	// line trips bufio.ErrTooLong and ends the connection with an error.
	MaxLineSize = 1 << 20
)

// NewHTTPClient builds an http.Client for a long-lived SSE stream: no overall
// timeout (cancellation is context-driven) with sane dial/TLS/response-header
// timeouts on the transport.
func NewHTTPClient() *http.Client {
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

// Frame is one parsed SSE frame: the accumulated id/event/data fields between
// two frame boundaries (blank lines). Consumers that only care about data
// ignore ID and Event.
type Frame struct {
	ID    string
	Event string
	Data  string
	// RetryMS is the server's requested reconnect delay in milliseconds from a
	// `retry:` line, or 0 when the frame carried none. Per the SSE spec this is
	// a stream-level directive rather than per-frame state, so the reader hoists
	// the most recent value onto [ConnResult.ServerRetry].
	RetryMS int
}

// ConnResult summarizes a single finished connection, handed to
// [Config.ResetBackoff] to decide whether the reconnect backoff resets.
type ConnResult struct {
	// Delivered is the number of frames [Config.Handler] reported as delivered.
	Delivered int
	// ReadAny reports whether at least one line was scanned on the connection
	// (i.e. the connection was healthy enough to carry data or a keepalive).
	ReadAny bool
	// Err is the connection's terminal error: nil for a clean server-side close,
	// otherwise a connect/status/read failure (including a watchdog-triggered
	// stall). It does NOT include an outer-context cancellation - that is handled
	// by [Reader.Run] returning before ResetBackoff is consulted.
	Err error
	// ServerRetry is the last `retry:` directive the server sent on this
	// connection, or 0 if it sent none. [Reader.Run] adopts it as the next
	// backoff ceiling, clamped to [BackoffBase, BackoffMax], which lets a server
	// that knows it is restarting spread the reconnect herd it is about to
	// create - something the client cannot infer on its own.
	ServerRetry time.Duration
}

// Config parameterizes a [Reader]. Client, Request, Handler and ResetBackoff are
// required; Logger defaults to slog.Default() and OnConnect may be nil.
type Config struct {
	// Name labels the stream in the reader's reconnect logs.
	Name string
	// Logger records reconnect/backoff/stall events. Defaults to slog.Default().
	Logger *slog.Logger
	// Client drives the long-lived requests (see [NewHTTPClient]).
	Client *http.Client
	// Request builds the SSE GET request, bound to the per-connection ctx passed
	// to it, once per (re)connect. Binding to that ctx lets the watchdog abort an
	// in-flight body read. Consumers set the URL and all headers here.
	Request func(ctx context.Context) (*http.Request, error)
	// OnConnect, if non-nil, runs once after each successful (HTTP 200) connect
	// and before the read loop starts. It is passed the OUTER ctx (so it is not
	// aborted by the read watchdog). Used to emit fresh state on every reconnect.
	OnConnect func(ctx context.Context)
	// Handler processes one parsed [Frame] at each frame boundary (blank line)
	// and reports whether it counted as delivered. It is called synchronously
	// from Run's goroutine, in order, and may block (e.g. on a bounded channel);
	// it is passed the OUTER ctx.
	Handler func(ctx context.Context, f Frame) (delivered bool)
	// ResetBackoff decides, from a finished connection's result, whether to reset
	// the reconnect backoff to BackoffBase.
	ResetBackoff func(ConnResult) bool
	// BackoffBase and BackoffMax bound the reconnect exponential backoff
	// ceiling (BackoffBase, x2 each reconnect, capped at BackoffMax). The
	// actual delay is drawn randomly beneath the ceiling - see [Reader.sleepFor].
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// Rand returns a value in [0,1) for the reconnect jitter. Defaults to
	// math/rand/v2; tests pin it to make backoff deterministic.
	Rand func() float64
	// WatchdogTimeout drops and redials a connection on which no line has been
	// read for this long (a dead TCP peer); the drop cancels only the connection,
	// not the outer ctx, so the stream reconnects instead of returning.
	WatchdogTimeout time.Duration
}

// Reader runs the reconnect loop for one SSE endpoint. Construct it with [New]
// and drive it with [Reader.Run].
type Reader struct {
	cfg Config
}

// New builds a [Reader] from cfg. A nil Logger falls back to slog.Default().
func New(cfg Config) *Reader {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reader{cfg: cfg}
}

// Run connects to the endpoint and dispatches frames until ctx is canceled,
// auto-reconnecting with exponential backoff across gaps. It blocks until ctx is
// canceled and then returns ctx.Err(). All callbacks run in this goroutine.
//
// The backoff resets to BackoffBase only when [Config.ResetBackoff] approves the
// finished connection's result; otherwise it keeps growing, so a connection that
// dies the same way on every attempt (e.g. an oversized frame) backs off instead
// of hot-looping. A watchdog-triggered stall drops just the connection and loops;
// only an outer-ctx cancellation returns.
func (r *Reader) Run(ctx context.Context) error {
	ceiling := r.cfg.BackoffBase

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		res := r.runOnce(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}

		if res.Err != nil {
			r.cfg.Logger.WarnContext(ctx, "sse stream interrupted, reconnecting",
				slog.String("stream", r.cfg.Name),
				slog.String("error", res.Err.Error()),
				slog.Int("delivered", res.Delivered),
				slog.Duration("backoff_ceiling", ceiling))
		} else {
			r.cfg.Logger.DebugContext(ctx, "sse stream closed, reconnecting",
				slog.String("stream", r.cfg.Name),
				slog.Int("delivered", res.Delivered),
				slog.Duration("backoff_ceiling", ceiling))
		}

		if r.cfg.ResetBackoff(res) {
			ceiling = r.cfg.BackoffBase
		}
		// A server that knows it is about to sever every stream (a rolling
		// restart) can hand out its own spread; honor it, but never let it pin
		// us at 0 or park us for an hour.
		if res.ServerRetry > 0 {
			ceiling = min(max(res.ServerRetry, r.cfg.BackoffBase), r.cfg.BackoffMax)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.sleepFor(ceiling)):
		}

		ceiling = min(ceiling*2, r.cfg.BackoffMax)
	}
}

// sleepFor draws the actual reconnect delay for a backoff ceiling.
//
// The ceiling advances deterministically (doubling) but the sleep is randomized
// beneath it - AWS "full jitter". Without this, every client that was connected
// to the same server wakes at exactly the same offsets (1s, 2s, 4s...) and
// re-dials in lockstep, so the reconnect storm that follows a restart lands as
// a series of synchronized spikes on a server that is still cold. Spreading the
// draws turns those spikes into a ramp.
//
// The draw is floored at half the base delay so a reset ceiling can never
// degenerate into a hot reconnect loop against a server that accepts and
// immediately closes.
func (r *Reader) sleepFor(ceiling time.Duration) time.Duration {
	floor := r.cfg.BackoffBase / 2
	if ceiling <= floor {
		return ceiling
	}
	d := time.Duration(r.random() * float64(ceiling))
	return max(d, floor)
}

// random returns a value in [0,1), from Config.Rand when set (tests pin it for
// determinism) and math/rand/v2 otherwise.
func (r *Reader) random() float64 {
	if r.cfg.Rand != nil {
		return r.cfg.Rand()
	}
	// Reconnect jitter is a load-spreading device, not a security primitive:
	// predicting it buys an attacker nothing, and crypto/rand on every reconnect
	// would be pure cost.
	return rand.Float64() //nolint:gosec // non-cryptographic by design
}

// runOnce performs a single connection and dispatches frames until the stream
// ends. A per-connection context backs the read-liveness watchdog: when no line
// arrives within WatchdogTimeout the connection (not the outer ctx) is canceled,
// so the stream reconnects instead of hanging on a dead peer.
func (r *Reader) runOnce(ctx context.Context) ConnResult {
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	req, err := r.cfg.Request(connCtx)
	if err != nil {
		return ConnResult{Err: fmt.Errorf("%s stream: failed to build request: %w", r.cfg.Name, err)}
	}

	response, err := r.cfg.Client.Do(req)
	if err != nil {
		return ConnResult{Err: fmt.Errorf("%s stream: failed to connect: %w", r.cfg.Name, err)}
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, InitialBufferSize))
		return ConnResult{Err: fmt.Errorf("%s stream: unexpected http status [%d %s]", r.cfg.Name, response.StatusCode, response.Status)}
	}

	if r.cfg.OnConnect != nil {
		r.cfg.OnConnect(ctx)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, InitialBufferSize), MaxLineSize)

	// Read-liveness watchdog: cancel this connection when no line arrives in time.
	watchdog := time.AfterFunc(r.cfg.WatchdogTimeout, cancelConn)
	defer watchdog.Stop()

	res := ConnResult{}
	var frame Frame
	for scanner.Scan() {
		watchdog.Reset(r.cfg.WatchdogTimeout)
		res.ReadAny = true
		line := scanner.Text()

		// A blank line marks the end of a frame.
		if line == "" {
			if frame.RetryMS > 0 {
				res.ServerRetry = time.Duration(frame.RetryMS) * time.Millisecond
			}
			// Suspend the watchdog across the hand-off. It measures SOCKET
			// liveness, not pipeline liveness: while we are parked handing a
			// frame to a slow consumer the peer is by definition fine - we are
			// the slow one. Without this, a Handler that blocks longer than
			// WatchdogTimeout cancels its own healthy connection, and the
			// resulting reconnect makes the backlog it was working through
			// worse. Note that moving Handler onto its own goroutine behind a
			// bounded queue does NOT fix this on its own: once that queue fills,
			// the reader blocks on the enqueue instead and the watchdog fires
			// anyway.
			watchdog.Stop()
			delivered := r.cfg.Handler(ctx, frame)
			watchdog.Reset(r.cfg.WatchdogTimeout)
			if delivered {
				res.Delivered++
			}
			frame = Frame{}
			continue
		}

		// Lines starting with ":" are keepalive comments; all others accumulate.
		if !strings.HasPrefix(line, ":") {
			accumulate(line, &frame)
		}
	}
	// An incomplete frame at the end of the stream is discarded per the SSE spec.

	res.Err = r.mapScanErr(connCtx, ctx, scanner.Err())
	return res
}

// mapScanErr wraps a scanner read error, distinguishing a watchdog-triggered
// stall (connCtx canceled while the outer ctx is still alive) from a plain read
// failure. A nil scanErr is returned as nil.
func (r *Reader) mapScanErr(connCtx, ctx context.Context, scanErr error) error {
	if scanErr == nil {
		return nil
	}
	// A watchdog-triggered cancellation only kills this connection: the outer ctx
	// is still alive, so Run reconnects instead of returning.
	if connCtx.Err() != nil && ctx.Err() == nil {
		return fmt.Errorf("%s stream: stalled (no data for %s): %w", r.cfg.Name, r.cfg.WatchdogTimeout, scanErr)
	}
	return fmt.Errorf("%s stream: read failed: %w", r.cfg.Name, scanErr)
}

// accumulate folds one SSE field line into the current frame. Unknown field
// names are ignored per the SSE spec.
func accumulate(line string, frame *Frame) {
	field, value := splitField(line)
	switch field {
	case "id":
		frame.ID = value
	case "event":
		frame.Event = value
	case "data":
		if frame.Data != "" {
			frame.Data += "\n"
		}
		frame.Data += value
	case "retry":
		// Spec: ignore the field unless the value is all ASCII digits.
		if ms, err := strconv.Atoi(value); err == nil && ms >= 0 {
			frame.RetryMS = ms
		}
	}
}

// splitField splits one SSE line into its field name and value, stripping the
// single optional space after the colon per the SSE spec. A line with no colon
// is a field with an empty value.
func splitField(line string) (field, value string) {
	field, value, found := strings.Cut(line, ":")
	if !found {
		return line, ""
	}
	return field, strings.TrimPrefix(value, " ")
}
