//go:build integration

package metastore_test

import (
	"context"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// pageStats is what the write-amplification question is actually asked in: how
// many updates had to move their row to a NEW heap page, and how much the heap
// grew as a result.
type pageStats struct {
	updates    int64
	hotUpdates int64
	newPage    int64
	heapPages  int64
}

func pageSnapshot(t *testing.T, db *sql.DB, schema string) pageStats {
	t.Helper()
	ctx := context.Background()
	var s pageStats
	// pg_stat_* is collected asynchronously; force it so the deltas are exact.
	_, err := db.ExecContext(ctx, "SELECT pg_stat_force_next_flush()")
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT coalesce(n_tup_upd,0), coalesce(n_tup_hot_upd,0), coalesce(n_tup_newpage_upd,0)
		FROM pg_stat_all_tables WHERE schemaname = $1 AND relname = 'known_txs'`, schema).
		Scan(&s.updates, &s.hotUpdates, &s.newPage))
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT (pg_relation_size($1::regclass) / 8192)::bigint`, schema+".known_txs").Scan(&s.heapPages))
	return s
}

// churnArcadeStatus seeds rows and drives them through the four arcade status
// transitions every transaction makes — the dominant known_txs write path —
// returning the page statistics for exactly that churn.
func churnArcadeStatus(t *testing.T, s *metastore.Store, rows int) (before, after pageStats) {
	t.Helper()
	ctx := context.Background()
	db := s.DB()

	var schema string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema))

	ids := make([]string, rows)
	for i := range rows {
		ids[i] = hex.EncodeToString(randBytes(t, 32))
		require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{
			TxID:      ids[i],
			Status:    wdk.ProvenTxStatusUnprocessed,
			Notify:    "{}",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}))
	}
	// Start from a table packed to its configured fillfactor.
	_, err := db.ExecContext(ctx, "VACUUM FULL known_txs")
	require.NoError(t, err)

	before = pageSnapshot(t, db, schema)
	for _, st := range []string{"ACCEPTED_BY_NETWORK", "SEEN_ON_NETWORK", "SEEN_MULTIPLE_NODES", "MINED"} {
		require.NoError(t, s.KnownTx().BulkSetArcadeStatus(ctx, ids, st))
	}
	after = pageSnapshot(t, db, schema)
	return before, after
}

// TestKnownTxsFillfactor_ReducesNewPageUpdates is the measured justification for
// migration 00005, and the correction to the hypothesis that produced it.
//
// The hypothesis was that known_txs' n_tup_hot_upd = 0 across 52.4M live updates
// could be fixed with page headroom. It cannot: HOT additionally requires that
// no INDEXED column changed, and postgres counts a partial index's PREDICATE
// columns as indexed — so arcade_status (the predicate of
// idx_known_txs_no_arcade_status) and updated_at (the second key of
// idx_known_txs_status) each block HOT on their own, at any fillfactor. This
// test pins that: HOT stays at zero on both sides.
//
// What the fillfactor does buy is NEW-PAGE updates, and that is real: each one
// is a page allocation plus a WAL full-page image plus permanent heap growth
// until VACUUM. This asserts the migration's actual effect against the same
// table at the postgres default.
func TestKnownTxsFillfactor_ReducesNewPageUpdates(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	const rows = 3000

	packed := newPostgresMeta(t, pg)
	var ff int
	require.NoError(t, packed.DB().QueryRowContext(ctx, `
		SELECT substring(unnest(reloptions) from 'fillfactor=([0-9]+)')::int
		FROM pg_class WHERE relname = 'known_txs'
		  AND relnamespace = current_schema()::regnamespace`).Scan(&ff))
	require.Equal(t, 70, ff, "migration 00005 must leave known_txs at fillfactor 70")

	// The same table, reset to the postgres default, is the control.
	control := newPostgresMeta(t, pg)
	_, err := control.DB().ExecContext(ctx, "ALTER TABLE known_txs RESET (fillfactor)")
	require.NoError(t, err)

	cBefore, cAfter := churnArcadeStatus(t, control, rows)
	pBefore, pAfter := churnArcadeStatus(t, packed, rows)

	const updates = 4 * rows
	require.EqualValues(t, updates, cAfter.updates-cBefore.updates)
	require.EqualValues(t, updates, pAfter.updates-pBefore.updates)

	// HOT is unreachable either way: the write touches indexed columns.
	require.Zero(t, cAfter.hotUpdates-cBefore.hotUpdates)
	require.Zero(t, pAfter.hotUpdates-pBefore.hotUpdates,
		"page headroom alone cannot make an indexed-column update HOT")

	controlNewPage := cAfter.newPage - cBefore.newPage
	packedNewPage := pAfter.newPage - pBefore.newPage
	t.Logf("new-page updates: default=%d of %d, fillfactor 70=%d of %d", controlNewPage, updates, packedNewPage, updates)
	t.Logf("heap pages: default %d->%d, fillfactor 70 %d->%d", cBefore.heapPages, cAfter.heapPages, pBefore.heapPages, pAfter.heapPages)

	// Measured ~3x fewer (11,945 -> 3,848 of 12,000); assert well inside that.
	require.Less(t, packedNewPage, controlNewPage/2,
		"fillfactor 70 must at least halve the new-page updates this churn causes")
	// And the table it leaves behind is materially smaller after identical work.
	require.Less(t, pAfter.heapPages, cAfter.heapPages,
		"fillfactor 70 must leave a smaller heap after identical churn")
}
