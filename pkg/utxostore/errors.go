package utxostore

import (
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Sentinel errors. Match with errors.Is.
var (
	// ErrBatch is the top-level sentinel returned by batch methods when one
	// or more items failed — inspect the per-item Err (Mint, SpendOp) or use
	// errors.As on the returned error for methods whose ops carry no Err
	// slot (Remove, Freeze, Unfreeze, ReserveOutpoints).
	ErrBatch = errors.New("utxostore: one or more batch items failed — inspect per-item Err")

	// ErrContention is the transient "could not settle under concurrency"
	// sentinel: a bounded budget for pinning down one row's state ran out with
	// NEITHER outcome true of it, so the store reports the ambiguity instead of
	// guessing. It names a CAUSE, not a backend and not an operation — read it
	// as "ask me again", never as a verdict on the coin.
	//
	// Do not infer the producer set from a store's concurrency style. Optimistic
	// and lock-based backends alike raise it, because both can lose a write to a
	// peer and then fail to classify the loss. Two causes exist today:
	//
	//   - A candidate set consumed by peers. Coins existed, but racing claimers
	//     took every one, so an empty answer would understate the pool. Who can
	//     tell that apart from a genuinely empty pool varies: a store may see it
	//     within one claim statement, or a caller may only establish it after
	//     its own bounded retries. Where the funder is the retrier it absorbs
	//     the error inside its budget; where it is the reporter it hands
	//     ErrContention on, and nothing above it retries.
	//
	//     A caller can also establish it WITHOUT the store ever raising it. A
	//     SQL claim answers (nil, nil) on an empty result and says no more,
	//     because within one statement it cannot tell exhaustion from rows its
	//     FOR UPDATE SKIP LOCKED had to skip — and in Mode A those rows stay
	//     skipped for as long as a peer's whole CreateAction runs. So the funder
	//     asks a second question when a full pass allocated nothing: the store's
	//     non-locking ClaimableExists probe. Coins claimable but unclaimed means
	//     they were hidden, and the funder raises ErrContention itself, then
	//     retries it as the retrier of the case above.
	//
	//   - A row that would not hold still. A guarded write — a CAS in aerostore,
	//     a guard-carrying UPDATE/DELETE in sqlstore — matched nothing, yet the
	//     classifying read that followed found the row eligible again: a peer is
	//     releasing and re-taking this exact coin right now. After a bounded
	//     number of passes neither a success nor a typed refusal would be true.
	//
	//     Which operations reach that exit differs by backend, so read the
	//     method's own doc rather than assuming parity. Both backends raise it
	//     from the guarded deletes (Remove, RemoveByMintTx). sqlstore raises it
	//     from Spend in BOTH modes. aerostore raises it from fact-mode Spend
	//     only: its guarded-mode exhaustion reports [ReservedError] naming the
	//     current holder instead, because under a guard a token mismatch is a
	//     refusal it can state. A caller recording an already-accepted broadcast
	//     must tolerate the fact-mode error rather than read it as a refused
	//     coin.
	//
	// Neither the delete nor the spend producer has an outer retry owner. Their
	// callers either tolerate the error — an un-forced Remove reports it per
	// item under [ErrBatch], so a caller filtering on ErrBatch reads it as one
	// more refusal — or propagate it and rely on their own re-drive; e.g. the
	// reconciler's next tick, an abort returning to the user, or a rejected
	// ProcessAction whose enclosing metastore transaction rolls back with it,
	// undoing that pass's failed-status marking so a later pass redoes both.
	//
	// The memstore reference implementation never raises it, because one mutex
	// spans its whole check-and-write and leaves no window for a peer to race
	// into. That is a property of that implementation alone.
	ErrContention = errors.New("utxostore: contention — candidates exhausted, retry")
)

// NotFoundError reports that an outpoint is absent from the store.
//
// errors.Is(err, &NotFoundError{}) matches any NotFoundError (type-only
// match); use errors.As to extract the outpoint.
type NotFoundError struct {
	Op Outpoint
}

// Error implements the error interface.
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("utxostore: outpoint %s not found", e.Op)
}

// Is reports a type-only match against another *NotFoundError.
func (e *NotFoundError) Is(target error) bool {
	_, ok := target.(*NotFoundError)
	return ok
}

// AlreadyExistsError reports a mint conflict: the outpoint exists with
// DIFFERENT identity data (UserID, Basket, Satoshis, or InputSize). A mint
// replay with identical identity is an idempotent success, not this error.
//
// errors.Is(err, &AlreadyExistsError{}) matches any AlreadyExistsError; use
// errors.As to extract the outpoint.
type AlreadyExistsError struct {
	Op Outpoint
}

// Error implements the error interface.
func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("utxostore: outpoint %s already exists with different data", e.Op)
}

// Is reports a type-only match against another *AlreadyExistsError.
func (e *AlreadyExistsError) Is(target error) bool {
	_, ok := target.(*AlreadyExistsError)
	return ok
}

// ReservedError reports a reservation-guard failure. HeldBy names the
// reservation currently holding the row; HeldBy == "" means the row IS in the
// inventory but is currently unreserved (e.g. a GUARDED Spend of a released
// row). It never means the outpoint is external — only [NotFoundError] means
// that. It is also the refusal returned by Remove (without force) for a
// reserved row.
//
// errors.Is(err, &ReservedError{}) matches any ReservedError; use errors.As
// to extract the outpoint and holder.
type ReservedError struct {
	Op     Outpoint
	HeldBy string
}

// Error implements the error interface.
func (e *ReservedError) Error() string {
	if e.HeldBy == "" {
		return fmt.Sprintf("utxostore: outpoint %s is not reserved", e.Op)
	}
	return fmt.Sprintf("utxostore: outpoint %s is reserved by %q", e.Op, e.HeldBy)
}

// Is reports a type-only match against another *ReservedError.
func (e *ReservedError) Is(target error) bool {
	_, ok := target.(*ReservedError)
	return ok
}

// SpentError reports that a row is already spent. Winner is the transaction
// that spent it — on a Spend refusal in either mode it identifies the
// competing spender; a Spend replay by the SAME spender is an idempotent
// success, not this error. It is the one state refusal a forced (fact-mode)
// Spend keeps: two spend facts cannot both hold.
//
// errors.Is(err, &SpentError{}) matches any SpentError; use errors.As to
// extract the outpoint and winner.
type SpentError struct {
	Op     Outpoint
	Winner chainhash.Hash
}

// Error implements the error interface.
func (e *SpentError) Error() string {
	return fmt.Sprintf("utxostore: outpoint %s already spent by %s", e.Op, e.Winner.String())
}

// Is reports a type-only match against another *SpentError.
func (e *SpentError) Is(target error) bool {
	_, ok := target.(*SpentError)
	return ok
}

// FrozenError reports that a row is frozen: it refuses Spend and Remove
// without force while the hold is in place. A forced Spend records the coin as
// spent and a forced Remove deletes it; neither lifts the freeze itself.
//
// errors.Is(err, &FrozenError{}) matches any FrozenError; use errors.As to
// extract the outpoint.
type FrozenError struct {
	Op Outpoint
}

// Error implements the error interface.
func (e *FrozenError) Error() string {
	return fmt.Sprintf("utxostore: outpoint %s is frozen", e.Op)
}

// Is reports a type-only match against another *FrozenError.
func (e *FrozenError) Is(target error) bool {
	_, ok := target.(*FrozenError)
	return ok
}
