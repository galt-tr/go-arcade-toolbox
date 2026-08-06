package metastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/utxostoretest"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
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
