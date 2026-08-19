//go:build integration

package sqlstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// TestClaimUsesPartialIndex is the regression guard for the partial-index
// lesson: over a 250k-row equal-value pool the planner must resolve EVERY claim
// shape via idx_utxos_claim, with no sequential scan and no sort. It EXPLAINs
// the EXACT production statements (the three exported claim SQLs) so a silent
// index-degrade in any shape — a dropped index or a claim predicate that stops
// matching the index's partial WHERE clause — fails a test.
func TestClaimUsesPartialIndex(t *testing.T) {
	pg := testenv.StartPostgres(t)
	s := newPostgresStore(t, pg)
	ctx := context.Background()

	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	const pool = 250_000
	seedUTXOsPostgres(t, s, sc, benchSats, pool)

	// Force a fresh, statistics-aware plan.
	_, err := s.DB().ExecContext(ctx, "ANALYZE utxos")
	require.NoError(t, err)

	// All rows have satoshis == benchSats, so >= 1, < benchSats+1, and
	// == benchSats each match the whole pool. Trailing binds are the
	// reservation token and reserved_at, as the hot path uses.
	res := time.Now().UTC()
	shapes := []struct {
		name string
		sql  string
		args []any
	}{
		{
			"SmallestSufficient", sqlstore.ClaimSmallestSufficientPGSQL,
			[]any{sc.UserID, sc.Basket, int64(sc.Tier), uint64(1), "explain", res},
		},
		{
			"LargestInsufficient", sqlstore.ClaimLargestInsufficientPGSQL,
			[]any{sc.UserID, sc.Basket, int64(sc.Tier), benchSats + 1, 3, "explain", res},
		},
		{
			"Exact", sqlstore.ClaimExactPGSQL,
			[]any{sc.UserID, sc.Basket, int64(sc.Tier), benchSats, 3, "explain", res},
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			plan := explainJSON(t, s, shape.sql, shape.args...)
			t.Logf("%s claim plan:\n%s", shape.name, plan.raw)

			require.Contains(t, plan.indexes, "idx_utxos_claim",
				"%s claim must scan the partial index idx_utxos_claim; node types seen: %v",
				shape.name, plan.nodeTypes)
			requireNoSeqScan(t, plan, shape.name+" claim must not sequential-scan the pool")
			for _, nt := range plan.nodeTypes {
				require.NotContains(t, nt, "Sort", "%s claim must be a pure ordered index walk (no sort node)", shape.name)
			}
		})
	}
}

// TestClaimableProbeUsesPartialIndex is the same guard for the claimable probe
// the funder runs when a funding pass allocated nothing. It is only defensible
// as an index descent: it runs on the not-enough-funds path, which under Mode A
// contention is exactly the path a loaded system takes most often.
//
// The plan-sensitive case is the FALSE answer — no claimable row matches — where
// the planner has to walk the index to prove a negative rather than stopping at
// the first heap tuple it happens to find. So the bound is above every coin in
// the pool.
func TestClaimableProbeUsesPartialIndex(t *testing.T) {
	pg := testenv.StartPostgres(t)
	s := newPostgresStore(t, pg)
	ctx := context.Background()

	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	const pool = 250_000
	seedUTXOsPostgres(t, s, sc, benchSats, pool)

	_, err := s.DB().ExecContext(ctx, "ANALYZE utxos")
	require.NoError(t, err)

	plan := explainJSON(t, s, sqlstore.ClaimableProbePGSQL,
		sc.UserID, sc.Basket, int64(sc.Tier), benchSats+1)
	t.Logf("claimable probe plan:\n%s", plan.raw)

	require.Contains(t, plan.indexes, "idx_utxos_claim",
		"the claimable probe must scan the partial index; node types seen: %v", plan.nodeTypes)
	requireNoSeqScan(t, plan, "the claimable probe must not sequential-scan the pool to prove a negative")

	// And it must answer that negative correctly, not just cheaply.
	exists, err := s.ClaimableExists(ctx, sc, benchSats+1)
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = s.ClaimableExists(ctx, sc, 0)
	require.NoError(t, err)
	require.True(t, exists, "every coin in the pool is claimable, so the any-coin probe must say so")
}

// The EXPLAIN plumbing these guards share with sweep_explain_test.go —
// explainJSON, explainedPlan and requireNoSeqScan — lives there, next to the
// index-only assertion that needs a node's type and its index name kept
// together.
