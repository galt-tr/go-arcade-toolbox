package metastore

import "errors"

// ErrNotFound is wrapped by every mutation that addresses a row by identity and
// finds none (SetActiveStorage, SetTxID, sync-state Update, certificate
// SoftDelete, known-tx MarkSuspectFailed/SetProof/UpdateStatus, RelinquishOutput
// for an unknown outpoint). Callers match it with errors.Is. The read path keeps
// the (value, found bool, err) convention instead — a missing row there is not an
// error.
var ErrNotFound = errors.New("metastore: not found")

// ErrStatusUpdateSkipped is returned by a guarded status transition whose CAS
// precondition (expectedCurrent / skipForStatuses) did not hold while the row
// itself exists, so nothing was written. It is an explicit skip signal, never a
// silent no-op — the caller decides whether that is benign (idempotent replay)
// or an error. A guarded transition against a row that does not exist reports
// [ErrNotFound] instead, so the two cases are always distinguishable.
var ErrStatusUpdateSkipped = errors.New("metastore: status update skipped (precondition not met)")

// ErrOutputBasketMismatch is returned by RelinquishOutput when the outpoint
// exists for the user but is not in the requested basket — distinct from
// [ErrNotFound] (no such outpoint at all).
var ErrOutputBasketMismatch = errors.New("metastore: output exists in a different basket")
