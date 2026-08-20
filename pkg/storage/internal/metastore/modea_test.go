package metastore_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// testModeA is the linchpin of the hybrid design: over ONE *sql.DB carrying
// BOTH migration chains (metadata + utxo inventory), a single UnitOfWork
// commits a metadata write and a utxostore write together, and a forced error
// mid-flight rolls BOTH back. It proves the internal/sqltx shared-transaction
// seam end to end.
func testModeA(t *testing.T, factory storeFactory) {
	ctx := context.Background()

	// The factory built a migrated metastore; enlist the utxostore over the
	// SAME handle (Mode A) with the matching dialect.
	meta := factory(t)
	db := meta.DB()
	utxo, err := sqlstore.New(ctx, db, sqlstore.Engine(string(meta.Engine())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	require.True(t, meta.SharesDatabase(db), "metastore shares the handle")
	require.True(t, utxo.SharesDatabase(db), "utxostore shares the handle")

	uid := mustUser(ctx, t, meta, "modea-user") // committed up front (FK parent)

	// --- commit path: both writes land atomically ---
	commitOp := utxostoretest.NewOutpoint("modea-commit", 0)
	commitMint := utxostoretest.NewMint(commitOp, int64(uid), "default", utxostore.TierMined, 500)
	require.NoError(t, meta.Do(ctx, func(ctx context.Context) error {
		if _, err := meta.Transactions().Insert(ctx, metastore.NewTx{
			UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "modea-commit",
		}); err != nil {
			return err
		}
		if err := utxo.Mint(ctx, []*utxostore.Mint{commitMint}); err != nil {
			return err
		}
		return commitMint.Err
	}))

	_, found, err := meta.Transactions().FindByReference(ctx, uid, "modea-commit")
	require.NoError(t, err)
	require.True(t, found, "metadata transaction committed")
	coin, err := utxo.Get(ctx, commitOp)
	require.NoError(t, err)
	require.Equal(t, uint64(500), coin.Satoshis, "utxo committed in the SAME transaction")

	// --- rollback path: a forced error undoes BOTH writes ---
	rollbackOp := utxostoretest.NewOutpoint("modea-rollback", 0)
	rollbackMint := utxostoretest.NewMint(rollbackOp, int64(uid), "default", utxostore.TierMined, 700)
	boom := errors.New("forced mid-transaction failure")
	err = meta.Do(ctx, func(ctx context.Context) error {
		if _, err := meta.Transactions().Insert(ctx, metastore.NewTx{
			UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "modea-rollback",
		}); err != nil {
			return err
		}
		if err := utxo.Mint(ctx, []*utxostore.Mint{rollbackMint}); err != nil {
			return err
		}
		return boom
	})
	require.ErrorIs(t, err, boom)

	_, found, err = meta.Transactions().FindByReference(ctx, uid, "modea-rollback")
	require.NoError(t, err)
	require.False(t, found, "metadata transaction rolled back — nothing persisted")
	_, err = utxo.Get(ctx, rollbackOp)
	require.ErrorIs(t, err, &utxostore.NotFoundError{}, "utxo rolled back — nothing persisted")
}

// TestModeB_ForeignTransactionNotReused is the symmetric counterpart of
// testModeA: a transaction opened over a DIFFERENT *sql.DB than the store's
// own must never be reused. Before the ownership fix, [metastore.Store.Do]
// would find ANY *sql.Tx on the context and reuse it, committed or rolled
// back only by whoever placed it — so a metastore called with a context that
// happens to carry a foreign store's ambient transaction (Mode B) would
// silently defer its own write's fate to that unrelated transaction. Ownership
// is now structural: [sqltx.From] reports a hit only when the owner matches
// this store's own *sql.DB, so Do falls back to opening (and owning) its own
// transaction exactly as if ctx carried none at all. SQLite-based; no
// containers needed.
func TestModeB_ForeignTransactionNotReused(t *testing.T) {
	ctx := context.Background()
	meta := newSQLiteMeta(t)  // the store under test
	other := newSQLiteMeta(t) // a different database, standing in for Mode B

	foreignTx, err := other.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = foreignTx.Rollback() })

	// ctx carries a transaction owned by other's *sql.DB, not meta's.
	foreignCtx := sqltx.With(ctx, foreignTx, other.DB())

	uid := mustUser(ctx, t, meta, "modeb-user")

	// Do must not reuse foreignTx: sqltx.From(foreignCtx, meta.DB()) reports
	// false because the owners differ, so Do opens and commits its own
	// transaction instead of deferring to (and never committing) foreignTx.
	require.NoError(t, meta.Do(foreignCtx, func(ctx context.Context) error {
		_, err := meta.Transactions().Insert(ctx, metastore.NewTx{
			UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "modeb-commit",
		})
		return err
	}))

	// Committed already, on meta's own transaction — independent of foreignTx.
	_, found, err := meta.Transactions().FindByReference(ctx, uid, "modeb-commit")
	require.NoError(t, err)
	require.True(t, found, "Do opened and committed its own transaction")

	// Rolling back the foreign transaction must not undo it, proving the
	// write never ran inside foreignTx in the first place.
	require.NoError(t, foreignTx.Rollback())
	_, found, err = meta.Transactions().FindByReference(ctx, uid, "modeb-commit")
	require.NoError(t, err)
	require.True(t, found, "write survives the foreign transaction's rollback")
}

// TestNewRejectsUnpinnedSQLitePool is the metastore half of the Mode A
// construction guard (audit P1-7). [metastore.OpenSQLite] pins the pool to one
// connection because SQLite serializes writes; [metastore.New] is handed a pool
// it does not own and so can only INSIST on that posture. Validating beats
// silently resizing: the handle is typically the one the utxostore is about to
// share, and a constructor that reshapes a caller's pool is a worse surprise
// than one that refuses to start.
func TestNewRejectsUnpinnedSQLitePool(t *testing.T) {
	ctx := context.Background()

	openRaw := func(t *testing.T) *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", sqlkit.SQLiteDSN(filepath.Join(t.TempDir(), "shared-meta.db")))
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	t.Run("unpinned is refused", func(t *testing.T) {
		_, err := metastore.New(ctx, openRaw(t), metastore.EngineSQLite)
		require.Error(t, err)
		require.Contains(t, err.Error(), "SetMaxOpenConns(1)",
			"the error must name the fix, not just the symptom")
	})

	t.Run("pinned is accepted", func(t *testing.T) {
		db := openRaw(t)
		db.SetMaxOpenConns(1)
		s, err := metastore.New(ctx, db, metastore.EngineSQLite)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(ctx) })
		require.NotZero(t, mustUser(ctx, t, s, "shared-pinned-user"), "migrations ran on the shared handle")
	})

	t.Run("OpenSQLite pins its own pool", func(t *testing.T) {
		require.Equal(t, 1, newSQLiteMeta(t).DB().Stats().MaxOpenConnections)
	})
}
