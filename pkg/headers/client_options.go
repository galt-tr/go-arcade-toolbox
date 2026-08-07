package headers

import (
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
)

// Option customizes a [Client] built by [New].
type Option func(*Client)

// WithRESTClient injects the resty client used for REST calls (height, tip,
// header-by-height). New configures it in place with JSON Accept, a User-Agent
// and the toolbox logger. A fresh resty client is created when this is unset.
func WithRESTClient(client *resty.Client) Option {
	return func(c *Client) {
		c.rest = client
	}
}

// WithSSEHTTPClient injects the http.Client used for the long-lived SSE streams
// (tip/reorg). A dedicated no-overall-timeout client is created when unset.
func WithSSEHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		c.sseClient = client
	}
}

// WithCacheDepth overrides the tip-distance guard for the immutable-header
// cache (default [defaultCacheDepth]): a header is cached only once its height
// is more than depth blocks below the last-observed tip.
func WithCacheDepth(depth uint32) Option {
	return func(c *Client) {
		c.cacheDepth = depth
	}
}

// WithSSEBackoff overrides the SSE reconnect exponential-backoff bounds
// (defaults: 1s base, 60s cap). Primarily for tests that need fast, deterministic
// reconnects.
func WithSSEBackoff(base, maximum time.Duration) Option {
	return func(c *Client) {
		c.sseBackoffBase = base
		c.sseBackoffMax = maximum
	}
}

// WithReadWatchdogTimeout overrides the SSE read-liveness watchdog (default
// [readWatchdogTimeout]): a connection on which no line is read for this long is
// dropped and redialed.
func WithReadWatchdogTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.sseReadWatchdog = timeout
	}
}
