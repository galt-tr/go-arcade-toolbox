package arcade

import (
	"encoding/hex"
	"time"
)

// HexBytes is a byte slice that marshals to / from a JSON hex string.
//
// Behavior is a faithful copy of arcade's models.HexBytes
// (github.com/bsv-blockchain/arcade, models/transaction.go) so the two encode
// and decode identically:
//
//   - Marshal: a nil slice becomes JSON null; any non-nil slice — INCLUDING a
//     non-nil empty slice — becomes a quoted lowercase hex string (the empty
//     slice becomes "").
//   - Unmarshal: JSON null becomes a nil slice; a quoted hex string is decoded
//     (so "" decodes to a non-nil, zero-length slice).
//
// The nil-vs-empty distinction is intentional and pinned by tests: null and ""
// are not interchangeable on the wire.
type HexBytes []byte

// MarshalJSON converts HexBytes to a JSON hex string (nil -> null).
func (h HexBytes) MarshalJSON() ([]byte, error) {
	if h == nil {
		return []byte("null"), nil
	}
	return []byte(`"` + hex.EncodeToString(h) + `"`), nil
}

// UnmarshalJSON decodes a JSON hex string into HexBytes (null -> nil).
func (h *HexBytes) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*h = nil
		return nil
	}
	// Strip surrounding quotes.
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		data = data[1 : len(data)-1]
	}
	decoded, err := hex.DecodeString(string(data))
	if err != nil {
		return err
	}
	*h = decoded
	return nil
}

// Status is the lifecycle state of a transaction as reported by Arcade.
//
// The constant wire values are copied verbatim from arcade's models.Status
// (github.com/bsv-blockchain/arcade, models/transaction.go) so status strings
// round-trip byte-for-byte between the two.
//
// The enum is OPEN for forward compatibility: a future arcade may emit status
// values this package does not know. Unknown values unmarshal verbatim (Status
// is a plain string type), are non-terminal (IsTerminal returns false), and —
// mirroring arcade's own default-open lattice — CanSupersede returns true for
// them against any prev. Consumers must tolerate statuses outside the constant
// set below rather than erroring on them.
type Status string

// Transaction lifecycle statuses. The string values are the exact wire values
// arcade emits — do not "correct" them. In particular the multi-node status is
// SEEN_MULTIPLE_NODES (arcade prose sometimes writes SEEN_ON_MULTIPLE_NODES;
// the CODE constant, and therefore the wire, uses SEEN_MULTIPLE_NODES).
const (
	// StatusUnknown indicates the transaction status is unknown.
	StatusUnknown Status = "UNKNOWN"
	// StatusReceived indicates the transaction was received by Arcade.
	StatusReceived Status = "RECEIVED"
	// StatusSentToNetwork indicates the transaction was sent to the network.
	StatusSentToNetwork Status = "SENT_TO_NETWORK"
	// StatusAcceptedByNetwork indicates the transaction was accepted by the network.
	StatusAcceptedByNetwork Status = "ACCEPTED_BY_NETWORK"
	// StatusSeenOnNetwork indicates the transaction was seen on the network.
	StatusSeenOnNetwork Status = "SEEN_ON_NETWORK"
	// StatusSeenMultipleNodes indicates the transaction was seen by multiple
	// independent nodes. NOTE the wire value is SEEN_MULTIPLE_NODES.
	StatusSeenMultipleNodes Status = "SEEN_MULTIPLE_NODES"
	// StatusDoubleSpendAttempted indicates a double spend was attempted.
	StatusDoubleSpendAttempted Status = "DOUBLE_SPEND_ATTEMPTED"
	// StatusRejected indicates the transaction was rejected.
	StatusRejected Status = "REJECTED"
	// StatusPendingRetry indicates broadcast failed with a retryable error and
	// will be retried.
	StatusPendingRetry Status = "PENDING_RETRY"
	// StatusStumpProcessing indicates the transaction has a STUMP and is
	// building the BUMP.
	StatusStumpProcessing Status = "STUMP_PROCESSING"
	// StatusMined indicates the transaction was mined into a block.
	StatusMined Status = "MINED"
	// StatusImmutable indicates the transaction is buried deep enough to be final.
	StatusImmutable Status = "IMMUTABLE"
)

// AllStatuses returns every defined Status in lifecycle order. Mirrors arcade's
// models.AllStatuses; useful for exhaustive enumeration (metric registration,
// lattice checks, tests).
func AllStatuses() []Status {
	return []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusDoubleSpendAttempted, StatusRejected, StatusPendingRetry,
		StatusStumpProcessing, StatusMined, StatusImmutable,
	}
}

// terminalStatuses is the set of statuses that represent a final outcome. A
// record already in one of these must not be overwritten by a later,
// lower-priority update. Copied from arcade's terminalStatuses; the sole
// allowed departure from a terminal state is MINED -> IMMUTABLE, encoded in
// the CanSupersede lattice below.
var terminalStatuses = map[Status]struct{}{
	StatusRejected:             {},
	StatusDoubleSpendAttempted: {},
	StatusMined:                {},
	StatusImmutable:            {},
}

// IsTerminal reports whether this status is a final outcome that must not be
// overwritten by later, lower-priority updates. Mirrors arcade's
// models.Status.IsTerminal.
func (s Status) IsTerminal() bool {
	_, ok := terminalStatuses[s]
	return ok
}

// disallowedPreviousStatuses returns the set of previous statuses from which a
// transition INTO s is forbidden.
//
// This is a faithful copy of the transition lattice in arcade's
// models.Status.DisallowedPreviousStatuses; CanSupersede consults it. Keeping
// the lattice unexported keeps the public surface to IsTerminal + CanSupersede
// while preserving arcade's exact semantics. Notable edges:
//
//   - PENDING_RETRY -> SENT_TO_NETWORK is allowed (the retry reaper republishing).
//   - SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES / ACCEPTED_BY_NETWORK may recover a
//     REJECTED transaction (a peer accepted after another rejected).
//   - Only MINED -> IMMUTABLE may leave a terminal state; every other terminal
//     is a sink for lower-priority updates.
func disallowedPreviousStatuses(s Status) []Status {
	switch s {
	case StatusUnknown, StatusReceived:
		// Regressing to UNKNOWN/RECEIVED is only valid on the initial insert.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry, StatusStumpProcessing,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSentToNetwork:
		// PENDING_RETRY -> SENT_TO_NETWORK is the reaper republishing; allowed.
		return []Status{
			StatusSentToNetwork, StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusAcceptedByNetwork:
		return []Status{
			StatusAcceptedByNetwork,
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenOnNetwork:
		// May follow earlier states or recover a previously-REJECTED tx.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusSeenMultipleNodes:
		return []Status{
			StatusSeenMultipleNodes,
			StatusPendingRetry,
			StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusPendingRetry:
		// Only valid for in-flight txs; never from a terminal or confirmed state.
		return []Status{
			StatusSeenOnNetwork, StatusSeenMultipleNodes,
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusStumpProcessing:
		// Meaningful only for in-flight txs; never push a terminal tx back here.
		return []Status{
			StatusRejected, StatusDoubleSpendAttempted,
			StatusMined, StatusImmutable,
		}
	case StatusRejected, StatusDoubleSpendAttempted:
		// Rejection can override any non-terminal in-flight state but must not
		// clobber an already-confirmed (MINED/IMMUTABLE) transaction.
		return []Status{StatusMined, StatusImmutable}
	case StatusMined:
		// MINED can be set from any in-flight state, but a transient
		// reorg-style regression must not pull a tx out of IMMUTABLE.
		return []Status{StatusImmutable}
	case StatusImmutable:
		// IMMUTABLE is the highest-priority sink — reachable from anything.
		return []Status{}
	default:
		return []Status{}
	}
}

// CanSupersede reports whether a record currently in status prev may be updated
// to s — i.e. whether s is allowed to overwrite prev under arcade's status
// lattice.
//
// This is arcade's models.Status.CanTransitionFrom under a clearer verb:
// s.CanSupersede(prev) == arcade's s.CanTransitionFrom(prev), same receiver,
// same argument, same lattice (disallowedPreviousStatuses) — only the method
// name changed.
//
// Two edges bypass the lattice, matching arcade:
//
//   - An empty prev ("") is always allowed: the first write for a txid has no
//     predecessor to regress from.
//   - prev == s is always allowed: re-asserting the same status is an
//     idempotent no-op for callers that may receive duplicate events.
//
// A status outside the known constant set bans nothing (the lattice's default
// case, mirroring arcade), so an unknown s can supersede any prev — including
// IMMUTABLE. See the open-enum note on [Status].
func (s Status) CanSupersede(prev Status) bool {
	if prev == "" || prev == s {
		return true
	}
	for _, banned := range disallowedPreviousStatuses(s) {
		if banned == prev {
			return false
		}
	}
	return true
}

// TxRecord is the transaction status DTO Arcade returns from GET /tx and
// carries on each SSE status event.
//
// It is arcade's models.TransactionStatus with byte-identical JSON tags (only
// the Go type name differs). Arcade's internal CreatedAt field (json:"-", never
// serialized) is intentionally omitted — it is not part of the wire surface.
//
// Marshaling notes, verified against arcade's encoder and pinned by the golden
// tests:
//
//   - MerklePath and RawTx are [HexBytes]: hex string on the wire, omitted when
//     empty (their omitempty is effective because they are slices).
//   - Timestamp, NextRetryAt and MerkleRegisteredAt are time.Time. encoding/json
//     omitempty does NOT apply to struct types, so despite the omitempty tags
//     NextRetryAt and MerkleRegisteredAt ALWAYS appear (as
//     "0001-01-01T00:00:00Z" when zero), exactly as arcade emits them.
//   - TxIDs marks a bulk-transition event in arcade internals; it is omitempty
//     and effectively never set on records read from the store / API.
type TxRecord struct {
	TxID   string `json:"txid"`
	Status Status `json:"txStatus"`
	// StatusCode is the numeric ARC status code (JSON key "status") — e.g. 202
	// echoed on a submit response — distinct from the string lifecycle status
	// carried in txStatus.
	StatusCode         int       `json:"status,omitempty"`
	Timestamp          time.Time `json:"timestamp"`
	TxIDs              []string  `json:"txids,omitempty"`
	BlockHash          string    `json:"blockHash,omitempty"`
	BlockHeight        uint64    `json:"blockHeight,omitempty"`
	MerklePath         HexBytes  `json:"merklePath,omitempty"`
	ExtraInfo          string    `json:"extraInfo,omitempty"`
	CompetingTxs       []string  `json:"competingTxs,omitempty"`
	RawTx              HexBytes  `json:"rawTx,omitempty"`
	RetryCount         int       `json:"retryCount,omitempty"`
	NextRetryAt        time.Time `json:"nextRetryAt,omitempty"`
	MerkleRegisteredAt time.Time `json:"merkleRegisteredAt,omitempty"`
}
