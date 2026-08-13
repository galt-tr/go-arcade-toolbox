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

	// Absent: no ambient transaction.
	got, ok := sqltx.From(base)
	require.False(t, ok)
	require.Nil(t, got)

	// Round-trip: With carries a transaction that From returns. A zero-value
	// *sql.Tx is a valid non-nil sentinel — the seam only stores and type-
	// asserts the pointer, it never calls methods on it.
	tx := &sql.Tx{}
	ctx := sqltx.With(base, tx)
	got, ok = sqltx.From(ctx)
	require.True(t, ok)
	require.Same(t, tx, got)

	// nil-clear: With(nil) removes any ambient transaction, even one already
	// present in the parent context.
	cleared := sqltx.With(ctx, nil)
	got, ok = sqltx.From(cleared)
	require.False(t, ok)
	require.Nil(t, got)

	// The original derived context is unaffected by the clear.
	got, ok = sqltx.From(ctx)
	require.True(t, ok)
	require.Same(t, tx, got)
}
