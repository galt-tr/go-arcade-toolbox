package sqltx_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
)

func TestFromAndWith(t *testing.T) {
	base := context.Background()

	// Fake *sql.DB pointers used only for pointer-equality comparisons — the
	// seam never calls a method on them.
	dbA := &sql.DB{}
	dbB := &sql.DB{}

	// Absent: no ambient transaction.
	got, ok := sqltx.From(base, dbA)
	require.False(t, ok)
	require.Nil(t, got)

	// Round-trip: With carries a transaction that From returns to its owner. A
	// zero-value *sql.Tx is a valid non-nil sentinel — the seam only stores and
	// type-asserts the pointer, it never calls methods on it.
	tx := &sql.Tx{}
	ctx := sqltx.With(base, tx, dbA)
	got, ok = sqltx.From(ctx, dbA)
	require.True(t, ok)
	require.Same(t, tx, got)

	// Owner mismatch: a *sql.DB that did not place the transaction never
	// receives it, even though one is present on the context. This is the
	// structural enforcement — a store passing its own db can never be handed a
	// foreign transaction (Mode B).
	got, ok = sqltx.From(ctx, dbB)
	require.False(t, ok)
	require.Nil(t, got)

	// nil-clear: With(nil) removes any ambient transaction, even one already
	// present in the parent context.
	cleared := sqltx.With(ctx, nil, dbA)
	got, ok = sqltx.From(cleared, dbA)
	require.False(t, ok)
	require.Nil(t, got)

	// The original derived context is unaffected by the clear.
	got, ok = sqltx.From(ctx, dbA)
	require.True(t, ok)
	require.Same(t, tx, got)

	// nil owner: With records nothing when told a transaction it cannot
	// attribute, and From never matches a nil owner even so. Without the first
	// half, a caller that passed a nil *sql.DB by accident would place an
	// unowned transaction that every store calling From with its own (non-nil)
	// db would correctly miss — but a second store passing nil would then pick
	// it up, which is exactly the cross-database leak the owner check exists to
	// prevent.
	got, ok = sqltx.From(sqltx.With(base, tx, nil), nil)
	require.False(t, ok)
	require.Nil(t, got)
}
