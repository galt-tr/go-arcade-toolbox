//go:build integration

package sqlstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// The Mode A false-empty claim (audit finding P2-4), at the store level.
//
// A claim's FOR UPDATE SKIP LOCKED locks live until the enclosing transaction
// ends. In Mode B that is the claim statement itself — microseconds. In Mode A
// the lock lives until the whole CreateAction commits, so a concurrent
// CreateAction skips those rows and its claim comes back EMPTY while the coins
// are still there and may yet be released by a rollback.
//
// The claim keeps answering (nil, nil) — that is the contract, and paying for a
// second query on every empty claim would tax the allocating path. What the
// store adds is the missing fact: ClaimableExists, a non-locking probe that
// still sees those rows. The funder calls it once, when a whole funding pass
// allocated nothing, and turns a true into ErrContention (see the funder's
// TestFund_* contention tests for that half).
//
// These tests build the real scenario — a competitor holding the pool's only
// coin inside an UNCOMMITTED ambient transaction — and pin both halves of the
// answer: the claim is empty, AND the probe says the coin is there.

// beginAmbient opens a raw transaction on the store's own *sql.DB and returns a
// context carrying it as the ambient (Mode A) transaction, plus the transaction
// so the test can decide its fate. A rollback is registered as a safety net; the
// tests that unwind the competitor deliberately roll back early (the second
// rollback is a no-op ErrTxDone).
func beginAmbient(t *testing.T, s *sqlstore.Store) (context.Context, *sql.Tx) {
	t.Helper()
	tx, err := s.DB().BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	return sqltx.With(context.Background(), tx, s.DB()), tx
}

func TestClaimableExistsSeesWhatAnAmbientTxHidFromTheClaim(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()

	t.Run("SmallestSufficient", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-smallest", 1, "default", utxostore.TierMined, 50_000)

		txCtx, tx := beginAmbient(t, s)
		held, err := s.ClaimSmallestSufficient(txCtx, minedScope, "competitor", 10_000)
		require.NoError(t, err)
		require.NotNil(t, held, "the competitor must hold the only coin for this test to mean anything")

		got, err := s.ClaimSmallestSufficient(ctx, minedScope, "victim", 10_000)
		require.NoError(t, err)
		require.Nil(t, got, "SKIP LOCKED hides the locked row, so the claim itself still reports none")

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.True(t, exists,
			"the coin is locked by an uncommitted ambient transaction, not gone. If the probe "+
				"cannot see it the funder has no way to tell this from an empty pool, and the "+
				"user gets a spurious insufficient-funds while their coin sits in the wallet")

		// The competitor unwinds: the coin was never really gone, so the retry
		// the funder performs on contention finds it — the convergence the whole
		// design rests on.
		require.NoError(t, tx.Rollback())
		got, err = s.ClaimSmallestSufficient(ctx, minedScope, "victim", 10_000)
		require.NoError(t, err)
		require.NotNil(t, got, "the released coin must be claimable on the retry")
		require.Equal(t, "victim", got.ReservedBy)
	})

	t.Run("Exact", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-exact", 1, "default", utxostore.TierMined, 25_000)

		txCtx, tx := beginAmbient(t, s)
		held, err := s.ClaimExact(txCtx, minedScope, "competitor", 25_000, 1)
		require.NoError(t, err)
		require.Len(t, held, 1)

		got, err := s.ClaimExact(ctx, minedScope, "victim", 25_000, 1)
		require.NoError(t, err)
		require.Empty(t, got)

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.True(t, exists, "a denominated fuel coin locked by an uncommitted peer is not a missing fuel coin")

		require.NoError(t, tx.Rollback())
		got, err = s.ClaimExact(ctx, minedScope, "victim", 25_000, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})

	t.Run("LargestInsufficient", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-largest", 1, "default", utxostore.TierMined, 5_000)

		txCtx, tx := beginAmbient(t, s)
		held, err := s.ClaimLargestInsufficient(txCtx, minedScope, "competitor", 10_000, 4)
		require.NoError(t, err)
		require.Len(t, held, 1)

		got, err := s.ClaimLargestInsufficient(ctx, minedScope, "victim", 10_000, 4)
		require.NoError(t, err)
		require.Empty(t, got)

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.True(t, exists)

		require.NoError(t, tx.Rollback())
		got, err = s.ClaimLargestInsufficient(ctx, minedScope, "victim", 10_000, 4)
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
}

// TestClaimableExistsAnswersFalseForEveryUnclaimablePool is the other half of
// the contract, and the one that keeps an honest insufficient-funds honest: a
// true here sends the funder into a retry it cannot win, ending in
// ErrUTXOContention where the user should have been told they are out of money.
func TestClaimableExistsAnswersFalseForEveryUnclaimablePool(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()

	t.Run("empty pool", func(t *testing.T) {
		s := newPostgresStore(t, pg)

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.False(t, exists)
	})

	// A COMMITTED reservation is emptiness, not contention: nothing is going to
	// release it on its own, so a retry would only burn the funder's budget.
	t.Run("committed reservation", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-committed", 1, "default", utxostore.TierMined, 50_000)

		held, err := s.ClaimSmallestSufficient(ctx, minedScope, "holder", 10_000)
		require.NoError(t, err)
		require.NotNil(t, held)

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("frozen coins", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		ops := utxostoretest.MintTx(t, s, "probe-frozen", 1, "default", utxostore.TierMined, 50_000)
		require.NoError(t, s.Freeze(ctx, ops))

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("coins below the bound", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-small", 1, "default", utxostore.TierMined, 900)

		exists, err := s.ClaimableExists(ctx, minedScope, 10_000)
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.True(t, exists, "the same coin IS claimable when nothing is required of its value")
	})

	t.Run("coins in another scope", func(t *testing.T) {
		s := newPostgresStore(t, pg)
		utxostoretest.MintTx(t, s, "probe-tier", 1, "default", utxostore.TierUnproven, 50_000)
		utxostoretest.MintTx(t, s, "probe-basket", 1, "fuel", utxostore.TierMined, 50_000)
		utxostoretest.MintTx(t, s, "probe-user", 2, "default", utxostore.TierMined, 50_000)

		exists, err := s.ClaimableExists(ctx, minedScope, 0)
		require.NoError(t, err)
		require.False(t, exists)

		// ...but the tier that does hold one answers true, so the funder's
		// per-tier walk sees the coin it could still use.
		unproven := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierUnproven}
		exists, err = s.ClaimableExists(ctx, unproven, 0)
		require.NoError(t, err)
		require.True(t, exists)
	})
}
