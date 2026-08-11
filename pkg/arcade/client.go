package arcade

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/go-softwarelab/common/pkg/must"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sse"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/txtrace"
)

// defaultRetryAfter is used when Arcade replies 503 without a parsable
// Retry-After header. Arcade sends "Retry-After: 1" on backpressure (see the
// live server, handlers.go), so 1s mirrors production; the old wallet-toolbox
// client defaulted to 5s, but the new [BackpressureError] contract pins the
// arcade-accurate 1s default.
const defaultRetryAfter = 1 * time.Second

// userAgent identifies this client on every request.
const userAgent = "go-arcade-toolbox"

// Compile-time proof that the concrete client satisfies the oracle contract.
var _ TxOracle = (*Client)(nil)

// apiError is arcade's JSON error body. On a submit rejection arcade returns
// {"error":"transaction failed validation","txid":...,"reason":"<why>", ...}
// (plus the ARC taxonomy "detail"/"title"/"status" when classified), so the
// human-readable cause lives in "reason"/"detail" while "error" is generic.
type apiError struct {
	Err    string `json:"error"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
}

// message returns the most specific human-readable reason available, preferring
// arcade's "reason"/"detail" over the generic "error" line.
func (a *apiError) message() string {
	if a == nil {
		return ""
	}
	switch {
	case a.Reason != "":
		return a.Reason
	case a.Detail != "":
		return a.Detail
	default:
		return a.Err
	}
}

// healthResponse is the subset of arcade's GET /health body this client needs.
// Arcade's response carries more (status string, per-datahub health) that the
// [Health] contract deliberately drops.
type healthResponse struct {
	Healthy     bool   `json:"healthy"`
	Version     string `json:"version"`
	BlockHeight uint64 `json:"blockHeight"`
}

// Client is the concrete HTTP/SSE [TxOracle]: the toolbox's connection to a
// single Arcade instance. REST calls (Broadcast, GetTx, Health) go through a
// resty client; the long-lived SSE stream (StreamStatus) uses a dedicated
// net/http client with no overall timeout. A single-target circuit breaker
// guards Broadcast.
type Client struct {
	logger     *slog.Logger
	httpClient *resty.Client
	sseClient  *http.Client
	config     defs.Arcade

	broadcastURL string
	queryTxURL   string
	healthURL    string
	eventsURL    string

	broadcastHeaders map[string]string

	breaker *circuitBreaker

	// sseReadWatchdogTimeout is the SSE read-liveness watchdog: when no line is
	// read for this long the connection is dropped and redialed. Defaults to
	// readWatchdogTimeout; overridable in tests.
	sseReadWatchdogTimeout time.Duration
	// sseBackoffBase and sseBackoffMax bound the SSE reconnect exponential
	// backoff. Default to the package consts; overridable in tests to keep the
	// reconnect assertions fast and deterministic.
	sseBackoffBase time.Duration
	sseBackoffMax  time.Duration
}

// New constructs a Client for a single Arcade instance. httpClient is the resty
// client used for REST calls (a fresh one is created when nil); it is configured
// in place with JSON Accept, a User-Agent, and the toolbox logger.
func New(logger *slog.Logger, httpClient *resty.Client, config defs.Arcade) *Client {
	logger = logging.Child(logger, "arcade")

	if httpClient == nil {
		httpClient = resty.New()
		// The stock http.Transport keeps only 2 idle connections per host, so a
		// high-concurrency background broadcaster (many parallel POST /tx to one
		// arcade) would churn a fresh TCP+TLS handshake per request. Give it a
		// generous keep-alive pool so concurrent broadcasts reuse connections.
		httpClient.SetTransport(&http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          512,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		})
	}
	httpClient = httpClient.
		SetHeader("Accept", "application/json").
		SetHeader("User-Agent", userAgent).
		SetLogger(logging.RestyAdapter(logger)).
		SetDebug(logging.IsDebug(logger))

	broadcastHeaders := map[string]string{
		"Content-Type": "application/octet-stream",
	}
	if config.CallbackToken != "" {
		broadcastHeaders["X-CallbackToken"] = config.CallbackToken
	}
	if config.CallbackURL != "" {
		broadcastHeaders["X-CallbackUrl"] = config.CallbackURL
	}
	if config.FullStatusUpdates {
		broadcastHeaders["X-FullStatusUpdates"] = "true"
	}

	c := &Client{
		logger:                 logger,
		httpClient:             httpClient,
		sseClient:              sse.NewHTTPClient(),
		config:                 config,
		broadcastURL:           config.URL + "/tx",
		queryTxURL:             config.URL + "/tx/{txid}",
		healthURL:              config.URL + "/health",
		eventsURL:              eventsURL(config),
		broadcastHeaders:       broadcastHeaders,
		sseReadWatchdogTimeout: readWatchdogTimeout,
		sseBackoffBase:         sseBackoffBase,
		sseBackoffMax:          sseBackoffMax,
	}

	probeInterval := time.Duration(must.ConvertToInt64FromUnsigned(config.CircuitBreaker.HealthProbeIntervalSeconds)) * time.Second
	c.breaker = &circuitBreaker{
		failureThreshold: config.CircuitBreaker.FailureThreshold,
		probeInterval:    probeInterval,
		now:              time.Now,
		probe: func(ctx context.Context) bool {
			health, err := c.Health(ctx)
			return err == nil && health.Healthy
		},
	}

	return c
}

// Broadcast submits a binary Extended Format transaction to POST /tx and maps
// the reply to the [BroadcastResult] contract. It is guarded by the circuit
// breaker: when the breaker is open it fails fast with [ErrCircuitOpen] without
// dialing arcade.
func (c *Client) Broadcast(ctx context.Context, txid string, ef []byte) (*BroadcastResult, error) {
	if err := c.breaker.allow(ctx); err != nil {
		c.logger.WarnContext(ctx, "arcade broadcast short-circuited by open circuit breaker",
			slog.String("txID", txid))
		return nil, err
	}

	result, err := c.broadcast(ctx, txid, ef)

	// Feed the breaker: a 202 accept, a 4xx rejection and a 503 backpressure
	// reply are all evidence arcade is serving; only an opaque failure counts.
	// A caller-side cancellation is neither — see [isCallerCancellation].
	var bp *BackpressureError
	switch {
	case isCallerCancellation(err):
	case err == nil, errors.As(err, &bp):
		c.breaker.recordSuccess()
	default:
		c.breaker.recordFailure()
	}

	return result, err
}

// isCallerCancellation reports whether err is our own context being canceled or
// expiring, rather than anything arcade did.
//
// Such an error carries NO information about arcade's health, so it must count
// as neither success nor failure for the circuit breaker. Counting it as a
// failure is how a *client* shutdown took arcade down for everyone: stopping a
// 200-worker load generator cancels 200 in-flight broadcasts at once, which blew
// straight past failure_threshold: 10 and opened the breaker — 538 "short-
// circuited by open circuit breaker" warnings against zero transport errors and
// zero arcade 5xx, 102 of them refusing healthy broadcasts in the *next* run.
// The fuel keeper's errgroup does the same thing on every sibling failure, and
// it shares this client with the payment path.
//
// The client sets no overall HTTP timeout of its own, so a deadline in the chain
// is always the caller's; arcade going slow or dead still surfaces as a
// transport error and still opens the breaker.
func isCallerCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// broadcast performs the POST /tx request and maps the outcome per the
// [BroadcastResult] contract (202 -> early result; 4xx -> final rejection with
// err nil; 503 -> *BackpressureError; >=500/transport -> plain error).
func (c *Client) broadcast(ctx context.Context, txid string, ef []byte) (*BroadcastResult, error) {
	// Trace the sampled tx's POST /tx boundary (create-stack ctx flag) or a
	// marked txid (covers a deferred/monitor-driven broadcast). Cheap guard.
	traced := txtrace.Sampled(ctx) || txtrace.Marked(txid)
	if traced {
		txtrace.Emit(c.logger, "broadcast_post", txid)
	}

	rec := &TxRecord{}
	apiErr := &apiError{}

	response, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(c.broadcastHeaders).
		SetBody(ef).
		SetResult(rec).
		SetError(apiErr).
		Post(c.broadcastURL)
	if err != nil {
		if traced {
			txtrace.Emit(c.logger, "broadcast_error", txid, "error", err.Error())
		}
		return nil, wrapTransportError(err)
	}
	if traced {
		txtrace.Emit(c.logger, "broadcast_accepted", txid,
			"http_status", response.StatusCode(),
			"arcade_status", string(rec.Status))
	}

	switch {
	case response.IsSuccess():
		if rec.TxID == "" {
			return nil, fmt.Errorf("arcade returned success [%d %s] without transaction status", response.StatusCode(), response.Status())
		}
		return &BroadcastResult{TxID: rec.TxID, Status: rec.Status}, nil

	case response.StatusCode() == http.StatusServiceUnavailable:
		// Backpressure: the tx was NOT queued, a retry is safe.
		return nil, &BackpressureError{RetryAfter: parseRetryAfter(response.Header().Get("Retry-After"))}

	case response.StatusCode() >= http.StatusInternalServerError:
		// Opaque failure: the tx fate is unknown, reconcile via GetTx.
		return nil, fmt.Errorf("arcade returned http status [%d %s]: %s", response.StatusCode(), response.Status(), c.errBody(apiErr, response))

	default:
		// 4xx tx-level rejection: FINAL and durable. Rejected + reason, err nil.
		return &BroadcastResult{
			TxID:      txid,
			Status:    StatusRejected,
			Rejected:  true,
			ExtraInfo: c.errBody(apiErr, response),
		}, nil
	}
}

// GetTx polls GET /tx/{txid} for the authoritative record. A 404 is mapped to
// an error matching [ErrTxNotFound] so consumers can branch with errors.Is.
func (c *Client) GetTx(ctx context.Context, txid string) (*TxRecord, error) {
	rec := &TxRecord{}
	apiErr := &apiError{}

	response, err := c.httpClient.R().
		SetContext(ctx).
		SetResult(rec).
		SetError(apiErr).
		SetPathParam("txid", txid).
		Get(c.queryTxURL)
	if err != nil {
		return nil, wrapTransportError(err)
	}

	switch response.StatusCode() {
	case http.StatusOK:
		return rec, nil
	case http.StatusNotFound:
		return nil, fmt.Errorf("arcade has no record of tx %s: %w", txid, ErrTxNotFound)
	default:
		return nil, fmt.Errorf("arcade returned unexpected http status [%d %s] for tx %s: %s", response.StatusCode(), response.Status(), txid, c.errBody(apiErr, response))
	}
}

// Health probes GET /health and maps it to the [Health] contract. A transport
// failure or a non-2xx status is returned as an error.
func (c *Client) Health(ctx context.Context) (*Health, error) {
	body := &healthResponse{}

	response, err := c.httpClient.R().
		SetContext(ctx).
		SetResult(body).
		Get(c.healthURL)
	if err != nil {
		return nil, wrapTransportError(err)
	}
	if !response.IsSuccess() {
		return nil, fmt.Errorf("arcade health returned http status [%d %s]", response.StatusCode(), response.Status())
	}

	return &Health{
		Healthy:     body.Healthy,
		Version:     body.Version,
		BlockHeight: body.BlockHeight,
	}, nil
}

// errBody returns arcade's human-readable error reason, falling back to the
// HTTP status line when the body carried nothing usable.
func (c *Client) errBody(apiErr *apiError, response *resty.Response) string {
	if msg := apiErr.message(); msg != "" {
		return msg
	}
	return fmt.Sprintf("http status %d %s", response.StatusCode(), response.Status())
}

// wrapTransportError distinguishes an unreachable arcade (a net.Error) from
// other request-send failures; both are opaque failures with an unknown tx fate.
//
// A canceled/expired caller context is called out first and by name: net/http
// reports it as a *url.Error, which satisfies net.Error, so it would otherwise
// be mislabelled "arcade is unreachable" when arcade was never at fault. The
// wrap keeps errors.Is(err, context.Canceled) working for [isCallerCancellation].
func wrapTransportError(err error) error {
	if isCallerCancellation(err) {
		return fmt.Errorf("arcade request canceled before completion: %w", err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("arcade is unreachable: %w", netErr)
	}
	return fmt.Errorf("failed to send request to arcade: %w", err)
}

// parseRetryAfter parses a Retry-After header value: either an integer number
// of seconds or an HTTP date. It defaults to [defaultRetryAfter] when the header
// is absent or unparsable, and treats a past HTTP date (clock skew) as the
// default rather than retrying immediately.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return defaultRetryAfter
	}
	if seconds, err := strconv.Atoi(header); err == nil {
		if seconds < 0 {
			return defaultRetryAfter
		}
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(header); err == nil {
		if until := time.Until(date); until > 0 {
			return until
		}
		return defaultRetryAfter
	}
	return defaultRetryAfter
}
