package arcade

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned by [Client.Broadcast] when the circuit breaker is
// open: consecutive opaque failures (transport errors or HTTP >=500) to the
// single Arcade target have tripped the breaker, so Broadcast is
// short-circuited to fail fast until a GET /health probe succeeds.
//
// It is a transient, retry-safe condition — distinct from a tx-level rejection
// (which is a durable [BroadcastResult] with Rejected=true) and from
// [BackpressureError] (arcade responding under load). The transaction was NOT
// submitted, so the caller may retry once the breaker recovers.
var ErrCircuitOpen = errors.New("arcade: circuit breaker open")

// circuitBreaker is a single-target breaker around Broadcast. It counts
// consecutive opaque failures and, once failureThreshold is reached, opens:
// Broadcast then fails fast with [ErrCircuitOpen] instead of dialing arcade.
// While open it probes GET /health at most once per probeInterval; a healthy
// probe closes the breaker. There is NO failover chain — multi-instance HA is
// a future router that will implement [TxOracle] itself.
//
// Only opaque failures count against the breaker. A 4xx tx-level rejection, a
// 503 backpressure reply and a 202 accept are all evidence arcade is serving,
// so each resets the failure counter (recordSuccess).
type circuitBreaker struct {
	// failureThreshold is the number of consecutive opaque failures that opens
	// the breaker. Zero disables the breaker entirely (allow always permits).
	failureThreshold uint
	// probeInterval is the minimum wait between GET /health probes while open.
	probeInterval time.Duration
	// probe reports whether arcade is healthy; wired to the client's Health call.
	probe func(context.Context) bool
	// now is the clock, overridable in tests.
	now func() time.Time

	mu                  sync.Mutex
	consecutiveFailures uint
	open                bool
	lastProbe           time.Time
}

// allow reports whether a broadcast may proceed. When closed it always permits.
// When open it permits only after a successful GET /health probe, throttled to
// one probe per probeInterval; otherwise it returns [ErrCircuitOpen] without
// touching the broadcast endpoint.
func (cb *circuitBreaker) allow(ctx context.Context) error {
	if cb.failureThreshold == 0 {
		return nil
	}

	cb.mu.Lock()
	if !cb.open {
		cb.mu.Unlock()
		return nil
	}
	// Open: throttle probes so a storm of broadcasts can't hammer /health.
	if cb.now().Sub(cb.lastProbe) < cb.probeInterval {
		cb.mu.Unlock()
		return ErrCircuitOpen
	}
	cb.lastProbe = cb.now()
	probe := cb.probe
	cb.mu.Unlock()

	if probe != nil && probe(ctx) {
		cb.mu.Lock()
		cb.open = false
		cb.consecutiveFailures = 0
		cb.mu.Unlock()
		return nil
	}
	return ErrCircuitOpen
}

// recordFailure counts one opaque failure and opens the breaker once the
// threshold is reached.
func (cb *circuitBreaker) recordFailure() {
	if cb.failureThreshold == 0 {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFailures++
	if cb.consecutiveFailures >= cb.failureThreshold && !cb.open {
		cb.open = true
		// Start the probe clock so the first probe waits a full interval,
		// giving arcade a chance to recover before we retry it.
		cb.lastProbe = cb.now()
	}
}

// recordSuccess resets the failure counter and closes the breaker: arcade
// responded, so it is serving.
func (cb *circuitBreaker) recordSuccess() {
	if cb.failureThreshold == 0 {
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.consecutiveFailures = 0
	cb.open = false
}
