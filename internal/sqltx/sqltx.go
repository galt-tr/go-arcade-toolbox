// Package sqltx carries an ambient *sql.Tx through a context.Context so that
// several SQL-backed stores sharing one *sql.DB (Mode A: the utxostore and the
// metastore in the same database) can enlist in a single caller-owned
// transaction without threading a *sql.Tx explicitly through every interface.
//
// The seam is deliberately tiny: a store looks for a transaction with [From],
// passing its own *sql.DB as the owner; if the ambient transaction was opened
// over that same *sql.DB it runs its statements against that transaction,
// otherwise it uses its own connection pool. Ownership is enforced
// STRUCTURALLY, not by convention: [With] records the *sql.DB a transaction
// was opened over alongside the transaction itself, and [From] reports a hit
// only when the owner it is passed is pointer-equal to the one [With]
// recorded. A store that calls From with its own db can therefore never be
// handed a transaction opened over a different *sql.DB — a Mode B wiring
// (utxostore and metastore on separate databases) cannot leak one store's
// ambient transaction into the other's statements, even by accident.
//
// The seam carries at most ONE ambient transaction: a second [With] replaces
// the first for every owner, including the store that placed it. Nesting
// transactions from two different *sql.DBs in one context is not supported —
// the inner one wins and the outer store falls back to its pool.
package sqltx

import (
	"context"
	"database/sql"
)

// ctxKey is an unexported context key type, so only this package can set or
// read the ambient transaction and no other package can collide with the key.
type ctxKey struct{}

// entry pairs an ambient transaction with the *sql.DB it was opened over, so
// [From] can verify a caller's own database actually owns the transaction
// before handing it back.
type entry struct {
	tx    *sql.Tx
	owner *sql.DB
}

// With returns a copy of ctx carrying tx, recorded as having been opened over
// owner, which must be non-nil whenever tx is non-nil. A store that later
// calls [From] on a derived context with the SAME owner will run its
// statements inside tx instead of opening its own; a store built over a
// different *sql.DB will not find it, however deep the derived context.
//
// Passing a nil tx removes any ambient transaction from the returned context
// (From reports false for every owner). The clear is deliberately global, not
// per-owner: there is only one slot to clear, so a nil With always empties it
// regardless of which owner placed the transaction being replaced.
func With(ctx context.Context, tx *sql.Tx, owner *sql.DB) context.Context {
	if tx == nil || owner == nil {
		return context.WithValue(ctx, ctxKey{}, entry{})
	}
	return context.WithValue(ctx, ctxKey{}, entry{tx: tx, owner: owner})
}

// From returns the ambient transaction carried by ctx, and whether one is
// present that was opened over owner. A context that never had a transaction,
// one whose transaction was cleared with a nil [With], or one carrying a
// transaction opened over a DIFFERENT *sql.DB, all report (nil, false) — the
// last case is the ownership check that makes the seam safe to use across
// stores wired on separate databases (Mode B).
func From(ctx context.Context, owner *sql.DB) (*sql.Tx, bool) {
	e, ok := ctx.Value(ctxKey{}).(entry)
	if !ok || e.tx == nil || e.owner != owner {
		return nil, false
	}
	return e.tx, true
}
