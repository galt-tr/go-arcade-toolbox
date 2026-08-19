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

	// ErrContention is returned by optimistic providers when their CAS
	// candidate set is exhausted by concurrent claimers. It is transient:
	// the funder performs the outer retry. Lock-based providers (SQL,
	// memstore) never return it.
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
