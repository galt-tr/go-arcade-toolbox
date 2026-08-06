//go:build integration

package sqlstore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/sqlstore"
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
			var raw []byte
			err := s.DB().QueryRowContext(ctx, "EXPLAIN (FORMAT JSON) "+shape.sql, shape.args...).Scan(&raw)
			require.NoError(t, err)
			t.Logf("%s claim plan:\n%s", shape.name, raw)

			var plans []struct {
				Plan map[string]any `json:"Plan"`
			}
			require.NoError(t, json.Unmarshal(raw, &plans))
			require.Len(t, plans, 1)

			nodeTypes, indexes := walkPlan(plans[0].Plan)

			require.Contains(t, indexes, "idx_utxos_claim",
				"%s claim must scan the partial index idx_utxos_claim; node types seen: %v", shape.name, nodeTypes)
			for _, nt := range nodeTypes {
				require.NotEqual(t, "Seq Scan", nt, "%s claim must not sequential-scan the pool", shape.name)
				require.NotContains(t, nt, "Sort", "%s claim must be a pure ordered index walk (no sort node)", shape.name)
			}
		})
	}
}

// walkPlan recursively collects every node's "Node Type" and "Index Name" from
// an EXPLAIN (FORMAT JSON) plan tree.
func walkPlan(node map[string]any) (nodeTypes []string, indexes []string) {
	if nt, ok := node["Node Type"].(string); ok {
		nodeTypes = append(nodeTypes, nt)
	}
	if idx, ok := node["Index Name"].(string); ok {
		indexes = append(indexes, idx)
	}
	if children, ok := node["Plans"].([]any); ok {
		for _, c := range children {
			if cm, ok := c.(map[string]any); ok {
				nt, ix := walkPlan(cm)
				nodeTypes = append(nodeTypes, nt...)
				indexes = append(indexes, ix...)
			}
		}
	}
	return nodeTypes, indexes
}
