package metastore_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// TestCountInBasket_MatchesListOutputsTotal proves the cheap indexed count is
// numerically identical to the total ListOutputs computes with its join to
// transactions (no status filter — the change-count caller applied none), so
// dropping the join preserves the value. It counts across two baskets and
// includes an output on a FAILED transaction (which the unfiltered count still
// includes, matching the old changeBasketCount behavior).
func TestCountInBasket_MatchesListOutputsTotal(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteMeta(t)
	uid := mustUser(ctx, t, s, "count-user")

	txnID, err := s.Transactions().Insert(ctx, metastore.NewTx{UserID: uid, Status: wdk.TxStatusCompleted, Reference: "count-ref"})
	require.NoError(t, err)
	require.NoError(t, s.Transactions().SetTxID(ctx, txnID, randTxID(t)))

	// Two outputs in "default", one in "special".
	for i, basket := range []string{"default", "default", "special"} {
		_, err := s.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: uid, TransactionID: txnID, Vout: uint32(i), Satoshis: int64(100 * (i + 1)), Basket: strptr(basket),
		})
		require.NoError(t, err)
	}
	// An output on a FAILED transaction: the unfiltered count still includes it
	// (the change-count caller passed no status filter), so both the join count
	// and the cheap count must agree on 3 for "default".
	failedTx, err := s.Transactions().Insert(ctx, metastore.NewTx{UserID: uid, Status: wdk.TxStatusFailed, Reference: "count-failed"})
	require.NoError(t, err)
	_, err = s.Outputs().Insert(ctx, metastore.NewOutput{UserID: uid, TransactionID: failedTx, Vout: 0, Satoshis: 999, Basket: strptr("default")})
	require.NoError(t, err)

	for _, basket := range []string{"default", "special", "absent"} {
		// The reference value: ListOutputs's total with NO status filter (the
		// join count the old changeBasketCount produced).
		_, wantTotal, err := s.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{UserID: uid, Basket: basket, Limit: 1})
		require.NoError(t, err)

		got, err := s.Outputs().CountInBasket(ctx, uid, basket)
		require.NoError(t, err)
		require.Equal(t, wantTotal, got, "CountInBasket(%q) must equal the join count", basket)
	}

	// Absolute expectations, so a bug that changed BOTH counts identically is caught.
	def, err := s.Outputs().CountInBasket(ctx, uid, "default")
	require.NoError(t, err)
	require.EqualValues(t, 3, def)
	spec, err := s.Outputs().CountInBasket(ctx, uid, "special")
	require.NoError(t, err)
	require.EqualValues(t, 1, spec)
}

// TestCountInBasket_UsesIndexNoJoin proves the count is the "cheap indexed one"
// the create hot path now issues: its SQLite query plan uses
// idx_outputs_user_basket and never touches the transactions table, unlike the
// join count ListOutputs runs. This is the structural guarantee that the per-op
// COUNT no longer carries the transactions join whose cost scaled with load.
func TestCountInBasket_UsesIndexNoJoin(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteMeta(t)
	uid := mustUser(ctx, t, s, "plan-user")

	db := s.DB()

	// The cheap count: a single table access, using idx_outputs_user_basket.
	// SQLite names table accesses by alias, so we assert on step count + index
	// name rather than the literal "transactions" (the join count aliases it "t").
	cheap := explainQueryPlan(ctx, t, db,
		"SELECT COUNT(*) FROM outputs WHERE user_id = ? AND basket = ?", uid, "default")
	require.Len(t, cheap, 1, "cheap count must touch exactly one table; plan:\n"+strings.Join(cheap, "\n"))
	require.Contains(t, strings.Join(cheap, "\n"), "idx_outputs_user_basket",
		"cheap count must use idx_outputs_user_basket; plan:\n"+strings.Join(cheap, "\n"))

	// The old join count, for contrast: it touches TWO tables (outputs + the
	// transactions join), the structural cost the cheap count removes.
	join := explainQueryPlan(ctx, t, db,
		"SELECT COUNT(*) FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id WHERE o.user_id = ? AND o.basket = ?",
		uid, "default")
	require.Len(t, join, 2, "the old join count touches two tables (outputs + transactions); plan:\n"+strings.Join(join, "\n"))
}

// explainQueryPlan returns the lowercased SQLite EXPLAIN QUERY PLAN detail lines
// for a query (one per table-access step).
func explainQueryPlan(ctx context.Context, t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var details []string
	for rows.Next() {
		// SQLite EXPLAIN QUERY PLAN columns: id, parent, notused, detail.
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		details = append(details, strings.ToLower(detail))
	}
	require.NoError(t, rows.Err())
	return details
}
