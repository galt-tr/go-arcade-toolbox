//go:build integration

package sqlstore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// The seeded sweep population. The proportions are the point, not the totals:
// the SPENT rows outnumber the live ones, because a spend leaves reserved_by
// set as provenance and 00001's holder index therefore accumulated one entry
// per coin ever spent while the live hold set stayed small. Post-00003 those
// rows are not in the index at all, so a plan that still reaches them is a plan
// that stopped using it.
const (
	sweepTokens      = 200                         // live reservations
	sweepPerToken    = 500                         // unspent, unpinned rows each
	sweepLiveRows    = sweepTokens * sweepPerToken // 100k rows the sweep must group
	sweepSpentRows   = 150_000                     // spent, reserved_by retained as provenance
	sweepPinnedRows  = 5_000                       // reserved and unspent, but pinned: in the index, filtered out
	sweepUserID      = int64(1)
	sweepStaleCutoff = 30 * time.Minute
)

// TestSweepUsesReservedIndex is the regression guard for the 00003 covering
// rewrite of idx_utxos_reserved, over a pool shaped like a busy wallet's: 100k
// live reserved rows, 150k spent-but-still-attributed rows, and a few thousand
// pinned holds.
//
// Two statements share that index and neither is a keyed lookup, so a lost
// index is invisible until the pool is large — the sweep is a background tick
// that would simply get slower, and the release walks a whole reservation:
//
//  1. FindStaleReservations' inner aggregate — GROUP BY (user_id, reserved_by)
//     with a HAVING over MIN(reserved_at). There is no range predicate to seek
//     on, so this reads the entire live hold set every tick, and before 00003
//     it read it FROM THE HEAP (a Hashed Aggregate over a bitmap heap scan,
//     re-checking spent_by and pinned per tuple). It must now be an INDEX-ONLY
//     scan of idx_utxos_reserved: every column it touches is a key column, an
//     INCLUDEd column, or the index's own partial predicate — which also lets
//     the aggregate group in index order (Strategy "Sorted") instead of hashing
//     100k rows.
//  2. ReleaseReservation's UPDATE — user_id + reserved_by + spent_by IS NULL +
//     NOT pinned. Its filter is the index's predicate plus its covering
//     columns, which is what 00001's two-column form could not express.
func TestSweepUsesReservedIndex(t *testing.T) {
	pg := testenv.StartPostgres(t)
	s := newPostgresStore(t, pg)
	ctx := context.Background()

	seedReservedPostgres(t, s)

	// VACUUM, not just ANALYZE. An index-only scan is only chosen — and only
	// worth choosing — when the visibility map says the heap need not be
	// consulted, and a freshly COPYed table has an empty one. Production tables
	// are autovacuumed; the test has to do it by hand or it would assert
	// against a plan no running system would produce.
	_, err := s.DB().ExecContext(ctx, "VACUUM (ANALYZE) utxos")
	require.NoError(t, err)

	cutoff := baseTime.Add(sweepStaleCutoff)

	t.Run("StaleReservations", func(t *testing.T) {
		plan := explainJSON(t, s, sqlstore.StaleReservationsPGSQL, cutoff, 10)
		t.Logf("stale sweep plan:\n%s", plan.raw)

		require.Contains(t, plan.indexes, "idx_utxos_reserved",
			"the stale aggregate must resolve through idx_utxos_reserved; node types seen: %v", plan.nodeTypes)
		requireNoSeqScan(t, plan, "the stale sweep must not scan the utxo pool once per tick")

		// The covering half — and the assertion that actually has teeth. On the
		// pre-00003 schema the two above BOTH pass: idx_utxos_reserved_at's
		// bitmap keeps it off a Seq Scan, and the outer expansion names
		// idx_utxos_reserved. What the aggregate did was a Hashed Aggregate over
		// a Bitmap HEAP Scan — 100k heap tuples per tick, re-checking spent_by
		// and pinned on each. Only "index-only, on this index" tells the two
		// plans apart, and it is stable because reserved_at, seq and pinned are
		// INCLUDEd for exactly this purpose.
		require.True(t, plan.hasNode("Index Only Scan", "idx_utxos_reserved"),
			"the stale aggregate must be an INDEX-ONLY scan of idx_utxos_reserved "+
				"(reserved_at/seq/pinned are INCLUDEd for that); nodes seen: %v", plan.nodes)
	})

	t.Run("ReleaseReservation", func(t *testing.T) {
		plan := explainJSON(t, s, sqlstore.ReleaseReservationPGSQL, sweepUserID, sweepToken(0))
		t.Logf("release plan:\n%s", plan.raw)

		require.Contains(t, plan.indexes, "idx_utxos_reserved",
			"the release must find its rows through idx_utxos_reserved; node types seen: %v", plan.nodeTypes)
		requireNoSeqScan(t, plan, "the release must not scan the pool to find one reservation's rows")

		// Expect this plan's ESTIMATED cost to have risen against the
		// pre-00003 one (~22 to ~820) and do not read that as a regression:
		// the work is identical — one token's ~500 rows either way — and only
		// the estimate changed. The old index could not express spent_by or
		// pinned, so the planner stacked their selectivities on top of it as
		// filters and predicted 2 rows; the new one predicts 235 against a true
		// 500. Less wrong, and it is why the planner now prefers a bitmap scan.
	})

	// And the plans must answer correctly, not just cheaply. Token 0's rows are
	// the oldest, so it heads the stale list; the pinned rows are held under
	// their own tokens and must appear nowhere.
	t.Run("answers", func(t *testing.T) {
		refs, ferr := s.FindStaleReservations(ctx, cutoff, 5)
		require.NoError(t, ferr)
		require.Len(t, refs, 5, "the sweep returns whole reservations, oldest first, up to the limit")
		require.Equal(t, sweepToken(0), refs[0].Reservation)
		require.Len(t, refs[0].Outpoints, sweepPerToken,
			"a stale reservation expands to all of its unspent, unpinned rows")
		for _, ref := range refs {
			require.NotContains(t, ref.Reservation, "pin-", "a pinned hold is never swept")
			require.NotContains(t, ref.Reservation, "spent-", "a spent row's provenance token is never swept")
		}

		released, rerr := s.ReleaseReservation(ctx, sweepUserID, sweepToken(0))
		require.NoError(t, rerr)
		require.Equal(t, sweepPerToken, released)
	})
}

// sweepToken names the reservation holding live group i.
func sweepToken(i int) string { return fmt.Sprintf("sweep-%03d", i) }

// seedReservedPostgres bulk-loads the sweep population via pgx COPY: live
// reserved rows under sweepTokens tokens (staggered so each group has a
// distinct MIN(reserved_at) and the oldest is deterministic), spent rows that
// keep their reserved_by provenance, and pinned holds. seq is the IDENTITY
// column's, so it is not supplied.
func seedReservedPostgres(tb testing.TB, s *sqlstore.Store) {
	tb.Helper()
	ctx := context.Background()

	rows := make([][]any, 0, sweepLiveRows+sweepSpentRows+sweepPinnedRows)
	row := func(id int, reservedBy string, reservedAt time.Time, spentBy []byte, pinned bool) []any {
		return []any{
			txidOf(id), int32(0), sweepUserID, "default", int16(utxostore.TierMined),
			int64(benchSats), int32(107), baseTime, reservedBy, reservedAt, spentBy, pinned,
		}
	}

	id := 0
	for tok := 0; tok < sweepTokens; tok++ {
		// Group 0 is the oldest and group sweepTokens-1 the youngest; all are
		// well inside the cutoff so the HAVING keeps every group.
		at := baseTime.Add(time.Duration(tok) * time.Second)
		for i := 0; i < sweepPerToken; i++ {
			rows = append(rows, row(id, sweepToken(tok), at, nil, false))
			id++
		}
	}
	for i := 0; i < sweepSpentRows; i++ {
		rows = append(rows, row(id, fmt.Sprintf("spent-%06d", i), baseTime, txidOf(1<<30+i), false))
		id++
	}
	for i := 0; i < sweepPinnedRows; i++ {
		rows = append(rows, row(id, fmt.Sprintf("pin-%05d", i), baseTime, nil, true))
		id++
	}

	conn, err := s.DB().Conn(ctx)
	require.NoError(tb, err)
	defer func() { _ = conn.Close() }()

	err = conn.Raw(func(dc any) error {
		pgxConn := dc.(*stdlib.Conn).Conn()
		_, cErr := pgxConn.CopyFrom(ctx,
			pgx.Identifier{"utxos"},
			[]string{
				"txid", "vout", "user_id", "basket", "tier", "satoshis", "input_size",
				"created_at", "reserved_by", "reserved_at", "spent_by", "pinned",
			},
			pgx.CopyFromRows(rows),
		)
		return cErr
	})
	require.NoError(tb, err)
}

// planNode is one node of an EXPLAIN tree, reduced to the two facts these
// guards assert on.
type planNode struct {
	Type  string
	Index string
}

// explainedPlan is a parsed EXPLAIN (FORMAT JSON) result.
type explainedPlan struct {
	raw       []byte
	nodes     []planNode
	nodeTypes []string
	indexes   []string
}

// hasNode reports whether the plan contains a node of the given type scanning
// the given index.
func (p explainedPlan) hasNode(nodeType, index string) bool {
	for _, n := range p.nodes {
		if n.Type == nodeType && n.Index == index {
			return true
		}
	}
	return false
}

// requireNoSeqScan fails if any node in the plan is a sequential scan, parallel
// or not.
func requireNoSeqScan(t *testing.T, p explainedPlan, msg string) {
	t.Helper()
	for _, nt := range p.nodeTypes {
		require.NotContains(t, nt, "Seq Scan", "%s; node types seen: %v", msg, p.nodeTypes)
	}
}

// explainJSON plans query with args and parses the node tree. EXPLAIN without
// ANALYZE does not execute the statement, so planning an UPDATE mutates
// nothing.
func explainJSON(t *testing.T, s *sqlstore.Store, query string, args ...any) explainedPlan {
	t.Helper()
	var raw []byte
	require.NoError(t, s.DB().
		QueryRowContext(context.Background(), "EXPLAIN (FORMAT JSON) "+query, args...).
		Scan(&raw))

	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal(raw, &plans))
	require.Len(t, plans, 1)

	p := explainedPlan{raw: raw}
	p.nodes = walkPlanNodes(plans[0].Plan)
	for _, n := range p.nodes {
		p.nodeTypes = append(p.nodeTypes, n.Type)
		if n.Index != "" {
			p.indexes = append(p.indexes, n.Index)
		}
	}
	return p
}

// walkPlanNodes flattens an EXPLAIN (FORMAT JSON) plan tree, pairing each
// node's type with the index it scans (empty for nodes that scan none). The
// pairing is what lets a guard say "index-only scan OF THIS index" rather than
// "an index-only scan somewhere and this index somewhere".
func walkPlanNodes(node map[string]any) []planNode {
	var out []planNode
	n := planNode{}
	if nt, ok := node["Node Type"].(string); ok {
		n.Type = nt
	}
	if idx, ok := node["Index Name"].(string); ok {
		n.Index = idx
	}
	out = append(out, n)
	if children, ok := node["Plans"].([]any); ok {
		for _, c := range children {
			if cm, ok := c.(map[string]any); ok {
				out = append(out, walkPlanNodes(cm)...)
			}
		}
	}
	return out
}
