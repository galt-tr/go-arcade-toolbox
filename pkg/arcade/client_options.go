package arcade

import (
	"net/http"
	"time"
)

// Option customizes a [Client] built by [New].
//
// These mirror the equivalents on [headers.Client], which has exported the same
// three SSE knobs since it was written. The asymmetry was not a decision: the
// fields existed here from the start but only in-package tests could reach them,
// so a deployment could tune the chaintracks stream and not the arcade one.
type Option func(*Client)

// WithSSEHTTPClient injects the http.Client used for the long-lived status
// stream (StreamStatus). The default is a dedicated client with NO overall
// timeout — a Timeout on this client is a hard cap on how long the stream may
// stay connected, which turns into a reconnect storm rather than an error, so
// override it only with a client built for streaming.
func WithSSEHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.sseClient = client
		}
	}
}

// WithSSEBackoff overrides the SSE reconnect exponential-backoff bounds
// (defaults: 1s base, 60s cap). The actual sleep is full jitter drawn beneath
// the current ceiling, floored at base/2. Primarily for tests that need fast,
// deterministic reconnects. Non-positive values are ignored.
func WithSSEBackoff(base, maximum time.Duration) Option {
	return func(c *Client) {
		if base > 0 {
			c.sseBackoffBase = base
		}
		if maximum > 0 {
			c.sseBackoffMax = maximum
		}
	}
}

// WithReadWatchdogTimeout overrides the SSE read-liveness watchdog (default
// [readWatchdogTimeout]): a connection on which no line is read for this long is
// dropped and redialed.
//
// Arcade sends `: keepalive` comments every 15s, so this must stay comfortably
// above that cadence — set it below and every healthy connection kills itself
// between keepalives. Non-positive values are ignored.
func WithReadWatchdogTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if timeout > 0 {
			c.sseReadWatchdogTimeout = timeout
		}
	}
}
