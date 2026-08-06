// Package sqltx carries an ambient *sql.Tx through a context.Context so that
// several SQL-backed stores sharing one *sql.DB (Mode A: the utxostore and the
// metastore in the same database) can enlist in a single caller-owned
// transaction without threading a *sql.Tx explicitly through every interface.
//
// The seam is deliberately tiny: a store looks for a transaction with [From];
// if one is present it runs its statements against that transaction, otherwise
// it uses its own connection pool. The store is responsible for verifying that
// a transaction it finds actually belongs to the *sql.DB it was built over —
// there is no portable way to prove that from a *sql.Tx alone — so the contract
// is that callers only ever place a transaction opened from the shared handle.
//
// sqlstore is the first package to need this seam; the metastore task reuses it.
package sqltx

import (
	"context"
	"database/sql"
)

// ctxKey is an unexported context key type, so only this package can set or
// read the ambient transaction and no other package can collide with the key.
type ctxKey struct{}

// With returns a copy of ctx carrying tx. Stores that later call [From] on a
// derived context will run their statements inside tx instead of opening their
// own. Passing a nil tx removes any ambient transaction from the returned
// context (From reports false).
func With(ctx context.Context, tx *sql.Tx) context.Context {
	if tx == nil {
		return context.WithValue(ctx, ctxKey{}, (*sql.Tx)(nil))
	}
	return context.WithValue(ctx, ctxKey{}, tx)
}

// From returns the ambient transaction carried by ctx, and whether one is
// present. A context that never had a transaction, or one whose transaction
// was cleared with a nil [With], reports (nil, false).
func From(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(ctxKey{}).(*sql.Tx)
	if !ok || tx == nil {
		return nil, false
	}
	return tx, true
}
