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
// The pinned migration is the reason this exists: SQLite refuses to drop a
// column a partial index still references, so 00002's Down must drop
// idx_utxos_reserved_at, recreate it without the pin predicate, and only then
// drop the column — an ordering nothing else in the build would catch.
func TestMigrationsRollBack(t *testing.T) {
	ctx := context.Background()
	db := openSQLiteDB(t)

	require.NoError(t, migrate(ctx, db, EngineSQLite))
	require.True(t, hasColumn(t, db, "pinned"), "migrating up must add the pinned column")

	down := func() error {
		return sqlkit.MigrateDown(ctx, db, migrationsFS, EngineSQLite, versionTable)
	}

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
// The planner's actual choice for the grouped scan is idx_utxos_reserved
// (reserved_by, user_id): the staleness filter is a HAVING over
// MIN(reserved_at), not a WHERE range, so there is no reserved_at term to seek
// on. That is why (1) asserts "index-driven" rather than naming one index.
func TestStaleScanIsIndexDriven(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "plan.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	seedReservations(t, s, 600, 20)
	_, err = s.DB().ExecContext(ctx, "ANALYZE")
	require.NoError(t, err)

	cutoff := s.encTime(time.Now())
	plan := explainQueryPlan(t, s, s.staleReservationsSQL(), cutoff, 10)
	for _, step := range plan {
		require.NotContains(t, step, "SCAN utxos",
			"the stale scan must never table-scan the pool; plan:\n%s", strings.Join(plan, "\n"))
	}
	require.Contains(t, strings.Join(plan, "\n"), "USING INDEX",
		"the stale scan must resolve through an index")

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
