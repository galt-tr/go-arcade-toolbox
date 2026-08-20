package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// TestMigrationsRollBack exercises the SQLite Down path of every migration.
// The pinned column is the reason this exists: SQLite refuses to drop a column
// an index still references, so every migration that puts `pinned` into an
// index has to retire that reference in its OWN Down — 00002 for
// idx_utxos_reserved_at, 00003 for idx_utxos_reserved — and both must do so
// BEFORE 00002's Down reaches the DROP COLUMN. That ordering is invisible to
// the rest of the build, and it only breaks in the direction nobody runs by
// hand.
func TestMigrationsRollBack(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteDB(t)

	require.NoError(t, migrate(ctx, db, EngineSQLite))
	require.True(t, hasColumn(t, db, "pinned"), "migrating up must add the pinned column")
	require.Contains(t, indexSQL(t, db, "idx_utxos_reserved"), "spent_by IS NULL",
		"00003 must evict spent rows from the holder index")

	down := func() error {
		return sqlkit.MigrateDown(ctx, db, migrationsFS, EngineSQLite, versionTable)
	}

	// Down 00003: the holder index returns to 00001's two-column form —
	// which is also what frees the pinned column for 00002's Down below.
	require.NoError(t, down())
	require.NotContains(t, indexSQL(t, db, "idx_utxos_reserved"), "pinned",
		"rolling back 00003 must give up the covering columns, pinned included")
	require.NotContains(t, indexSQL(t, db, "idx_utxos_reserved"), "spent_by")
	require.True(t, hasColumn(t, db, "pinned"), "00003's Down touches indexes only")

	// Down 00002: the pin column and its index predicate go, the table stays.
	require.NoError(t, down())
	require.False(t, hasColumn(t, db, "pinned"), "rolling back 00002 must drop the pinned column")
	require.Contains(t, indexSQL(t, db, "idx_utxos_reserved_at"), "spent_by IS NULL",
		"the stale-scan index must be restored without the pin predicate")
	require.NotContains(t, indexSQL(t, db, "idx_utxos_reserved_at"), "pinned")

	// Down 00001: back to an empty schema, then all the way up again.
	require.NoError(t, down())
	require.NoError(t, migrate(ctx, db, EngineSQLite))
	require.True(t, hasColumn(t, db, "pinned"))
	require.Contains(t, indexSQL(t, db, "idx_utxos_reserved_at"), notPinned,
		"the stale-scan index predicate must be the same text the sweep's queries use")
	require.Contains(t, indexSQL(t, db, "idx_utxos_reserved"), "reserved_at",
		"the holder index must come back covering")
}

// TestStaleScanIsIndexDriven plans the EXACT production stale-reservation
// statement (the sweep that a pin must hide rows from) and pins the two
// properties the pin predicate put at risk:
//
//  1. the sweep stays index-driven — no full table scan over the UTXO pool;
//  2. idx_utxos_reserved_at still MATCHES the sweep's pin predicate. SQLite
//     matches a partial index by predicate text, so this is the coupling
//     between notPinned and the 00002 migration. INDEXED BY makes it a hard
//     assertion: if the index's WHERE no longer covers the query's terms,
//     SQLite refuses the statement with "no query solution" instead of
//     quietly planning something else.
//
// The planner's actual choice for the grouped scan is idx_utxos_reserved: the
// staleness filter is a HAVING over MIN(reserved_at), not a WHERE range, so
// there is no reserved_at term to seek on. That is why (1) asserts
// "index-driven" rather than naming one index.
//
// Since the 00003 migration made that index covering, (1) has a third clause
// asserting the IMPROVEMENT rather than merely tolerating it: the grouped scan
// is now a COVERING index search, so it reads no table rows at all. The plan
// line moved from
//
//	SEARCH utxos USING INDEX idx_utxos_reserved (reserved_by>?)
//
// to
//
//	SEARCH utxos USING COVERING INDEX idx_utxos_reserved (reserved_by>?)
//
// which is worth pinning because it is fragile in a way peculiar to SQLite:
// unlike PostgreSQL, SQLite does not discharge the query's "spent_by IS NULL"
// against the partial index's identical predicate, so it re-reads that column
// per row and only the presence of spent_by IN the index keeps the scan off
// the table. Drop that column from 00003 believing it redundant — it is
// redundant on PostgreSQL — and this assertion is what says otherwise.
//
// The OUTER expansion is deliberately NOT covering: it projects u.txid and
// u.vout, which 00003 leaves out on purpose (see its comment), so its line
// stays a plain "USING INDEX".
func TestStaleScanIsIndexDriven(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "plan.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	seedReservations(t, s, 600, 20)
	_, err = s.DB().ExecContext(ctx, "ANALYZE")
	require.NoError(t, err)

	cutoff := s.encTime(time.Now())
	// Both shapes: the ordinary sweep's and the fence-first sweep's
	// pinned-INCLUSIVE twin (C4). The inclusive one drops `NOT pinned` and
	// nothing else, so it must plan identically — pinned is a trailing key
	// column of idx_utxos_reserved, not the reason the scan is covering.
	for _, tc := range []struct {
		name          string
		includePinned bool
	}{{"excluding pinned", false}, {"including pinned", true}} {
		plan := explainQueryPlan(t, s, s.staleReservationsSQL(tc.includePinned), cutoff, 10)
		t.Logf("stale scan plan (%s):\n%s", tc.name, strings.Join(plan, "\n"))
		for _, step := range plan {
			require.NotContains(t, step, "SCAN utxos",
				"the stale scan (%s) must never table-scan the pool; plan:\n%s", tc.name, strings.Join(plan, "\n"))
		}
		require.Contains(t, strings.Join(plan, "\n"), "USING INDEX",
			"the stale scan (%s) must resolve through an index", tc.name)
		require.Contains(t, strings.Join(plan, "\n"), "USING COVERING INDEX idx_utxos_reserved",
			"the grouped scan (%s) must read the whole live hold set from idx_utxos_reserved alone; "+
				"losing 'covering' means 00003's trailing spent_by went missing and every row "+
				"costs a table read again. plan:\n%s", tc.name, strings.Join(plan, "\n"))
	}

	probe := `SELECT reserved_at FROM utxos INDEXED BY idx_utxos_reserved_at
		WHERE reserved_by IS NOT NULL AND spent_by IS NULL AND ` + notPinned + ` AND reserved_at < ?`
	require.Contains(t, strings.Join(explainQueryPlan(t, s, probe, cutoff), "\n"), "idx_utxos_reserved_at",
		"the partial index must still match the sweep's pin predicate")
}

// openSQLiteDB opens a raw, migration-free SQLite handle on a temp file (WAL
// needs a real file) with the store's single-writer posture.
func openSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", sqlkit.SQLiteDSN(filepath.Join(t.TempDir(), "down.db")))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedReservations mints coins and reserves them under tokens spread evenly, so
// the planner weighs a realistic stale-scan population.
func seedReservations(t *testing.T, s *Store, coins, tokens int) {
	t.Helper()
	ctx := context.Background()

	mints := make([]*utxostore.Mint, coins)
	for i := range mints {
		op := utxostore.Outpoint{Vout: uint32(i)} //nolint:gosec // i < coins, never overflows
		copy(op.TxID[:], fmt.Sprintf("plan-%d", i))
		mints[i] = &utxostore.Mint{
			Outpoint: op, UserID: 1, Basket: "default",
			Satoshis: 100, InputSize: utxostore.DefaultP2PKHInputSize, Tier: utxostore.TierMined,
		}
	}
	require.NoError(t, s.Mint(ctx, mints))

	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	for i := 0; i < tokens; i++ {
		claimed, err := s.ClaimExact(ctx, sc, fmt.Sprintf("tok-%d", i), 100, coins/(tokens*2))
		require.NoError(t, err)
		require.NotEmpty(t, claimed)
	}
}

// explainQueryPlan returns SQLite's plan for query as one string per step. A
// statement the planner cannot satisfy (an INDEXED BY the index no longer
// matches) fails here rather than returning a plan.
func explainQueryPlan(t *testing.T, s *Store, query string, args ...any) []string {
	t.Helper()
	rows, err := s.DB().QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, plan)
	return plan
}

func hasColumn(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	rows, err := db.Query("SELECT name FROM pragma_table_info('utxos') WHERE name = ?", name)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	found := rows.Next()
	require.NoError(t, rows.Err())
	return found
}

func indexSQL(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var stmt string
	require.NoError(t, db.QueryRow("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&stmt))
	return stmt
}
