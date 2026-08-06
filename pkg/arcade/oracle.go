package arcade

import (
	"context"
	"errors"
	"time"
)

// ErrTxNotFound is returned by [TxOracle.GetTx] when Arcade has no verdict for
// the requested txid. Implementations MUST return an error matching this
// sentinel via errors.Is so consumers — and the HA router, which must
// distinguish "this instance does not know the tx" from "this instance is
// broken" — can branch on not-found portably across implementations.
var ErrTxNotFound = errors.New("arcade: transaction not found")

// TxOracle is the transaction-truth oracle contract: everything the toolbox
// needs from Arcade to broadcast a transaction and learn its authoritative
// fate. It is implemented by the concrete HTTP/SSE Client (task M3.1); a
// multi-instance HA router is expected to implement the same interface in a
// follow-up, so the contract described on each method is load-bearing and
// implementations must honor it exactly.
type TxOracle interface {
	// Broadcast submits a transaction (ef is the binary Extended Format blob;
	// txid is its canonical id, used for logging/correlation) and returns the
	// EARLY outcome. The returned status is not final — see [BroadcastResult]
	// for the full outcome taxonomy and the error contract (tx-level rejection
	// vs backpressure vs opaque failure).
	Broadcast(ctx context.Context, txid string, ef []byte) (*BroadcastResult, error)

	// GetTx polls the authoritative current record for a txid: status plus
	// blockHash/blockHeight, merklePath (BUMP), rawTx and competingTxs when
	// present. This is the source of terminal history that the SSE stream does
	// not replay to fresh connections. For a txid Arcade has no verdict for,
	// implementations MUST return an error matching [ErrTxNotFound] (via
	// errors.Is).
	GetTx(ctx context.Context, txid string) (*TxRecord, error)

	// StreamStatus opens the status SSE stream and invokes onEvent for every
	// event until ctx is canceled or a terminal transport error occurs.
	//
	// lastEventID resumes a previously-dropped stream: pass the ID of the last
	// [StatusEvent] successfully processed (see [StatusEvent.ID]). Pass "" for a
	// cold start — which, per the arcade contract, replays only NON-terminal
	// statuses, so a cold-starting consumer must poll [TxOracle.GetTx] for any
	// transaction that may have gone terminal while disconnected.
	//
	// Delivery is at-least-once: onEvent may see the same event twice (e.g.
	// across a reconnect), so handlers must be idempotent. If onEvent returns a
	// non-nil error the stream stops and that error is returned.
	StreamStatus(ctx context.Context, lastEventID string, onEvent func(StatusEvent) error) error

	// Health reports whether this Arcade instance is serving and how fresh its
	// chain view is. See [Health].
	Health(ctx context.Context) (*Health, error)
}

// BroadcastResult is the outcome of a [TxOracle.Broadcast] call. It captures
// arcade's asynchronous, three-way broadcast contract.
//
// Outcome mapping (the implementation MUST follow this):
//
//   - HTTP 202: the transaction entered the pipeline. Status holds the early
//     lifecycle state (typically RECEIVED, or the existing state on an
//     idempotent re-submit), Rejected is false, and err is nil. 202 is NOT a
//     final verdict — the authoritative outcome arrives over the SSE stream or
//     via GetTx.
//   - HTTP 4xx: a tx-level rejection. This is FINAL: arcade persists a terminal
//     REJECTED row, so the verdict is durable and observable to subscribers.
//     Rejected is true, ExtraInfo carries arcade's human-readable reason, and
//     err is nil. Callers MUST NOT retry — the transaction bytes are the
//     problem, not a transient condition.
//   - HTTP 503: backpressure, returned as a [*BackpressureError] with result
//     nil (see that type). The transaction was NOT queued; a retry is safe.
//   - HTTP >=500 (other) or a transport error: an opaque failure returned as a
//     plain error with result nil. The transaction's fate is unknown; reconcile
//     via GetTx before retrying.
type BroadcastResult struct {
	// TxID echoes the transaction id arcade acknowledged.
	TxID string
	// Status is the early lifecycle status from a 202, or REJECTED on a 4xx.
	Status Status
	// Rejected is true only for a tx-level (4xx) rejection, which is final.
	Rejected bool
	// ExtraInfo carries arcade's rejection reason when Rejected is true;
	// otherwise empty.
	ExtraInfo string
}

// BackpressureError is returned by [TxOracle.Broadcast] when Arcade replies 503
// under broker backpressure. It signals a transient, retry-safe condition: the
// transaction was NOT queued, so resubmitting the same bytes is safe and is the
// contract the 503 expresses. Arcade sends Retry-After: 1, surfaced here as
// RetryAfter, which callers should honor as a minimum backoff before retry.
type BackpressureError struct {
	// RetryAfter is arcade's advised minimum wait before retrying, parsed from
	// the Retry-After response header (arcade currently sends 1 second).
	RetryAfter time.Duration
}

// Error implements the error interface.
func (e *BackpressureError) Error() string {
	return "arcade: backpressure, retry after " + e.RetryAfter.String()
}

// StatusEvent is one event delivered by [TxOracle.StreamStatus].
type StatusEvent struct {
	// ID is the SSE event id: a nanosecond-resolution timestamp string. Pass
	// the ID of the last successfully-processed event back to StreamStatus as
	// lastEventID (Last-Event-ID) to resume the stream without gaps. Because
	// delivery is at-least-once, the same ID may be observed more than once.
	ID string
	// Record is the transaction record carried by this event. On the stream,
	// intermediate frames may carry only the identifying/status fields; the
	// full proof fields (merklePath, rawTx) are guaranteed via GetTx.
	Record TxRecord
}

// Health is the minimal liveness/freshness view from Arcade's GET /health. It
// carries the fields a client needs to gate submission and detect a stale chain
// view; arcade's response has more (status string, per-endpoint datahub health)
// that this contract deliberately drops.
type Health struct {
	// Healthy is arcade's liveness bool. Clients gate submission on this.
	Healthy bool
	// Version is arcade's build version string.
	Version string
	// BlockHeight is arcade's own processed active-tip height — a freshness
	// signal, not a liveness gate. A reachable instance can still report a
	// stale height; comparing it against the network tip detects that. Zero
	// when arcade has no readable height. Note the type mirrors arcade's wire
	// (uint64) while headers.CurrentHeight mirrors ChainTracks (uint32) —
	// comparing the two requires an explicit cast.
	BlockHeight uint64
}
