//go:build integration

package funder_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// storeFactory builds a fresh, migrated store for one subtest.
type storeFactory func(t *testing.T) utxostore.Store

func newSQLiteFactory() storeFactory {
	return func(t *testing.T) utxostore.Store {
		t.Helper()
		path := filepath.Join(t.TempDir(), "funder.db")
		s, err := sqlstore.OpenSQLite(context.Background(), path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		return s
	}
}

// TestIntegration_Funder_SQLite runs the funder against a real SQLite store.
func TestIntegration_Funder_SQLite(t *testing.T) {
	runFunderIntegration(t, newSQLiteFactory())
}

// TestIntegration_Funder_Postgres runs the funder against a real PostgreSQL
// store (one isolated schema per subtest). Skips gracefully with no runtime.
func TestIntegration_Funder_Postgres(t *testing.T) {
	pg := testenv.StartPostgres(t)
	factory := func(t *testing.T) utxostore.Store {
		t.Helper()
		dsn := pg.IsolatedSchemaDSN(t)
		s, err := sqlstore.OpenPostgres(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		return s
	}
	runFunderIntegration(t, factory)
}

func runFunderIntegration(t *testing.T, newStore storeFactory) {
	t.Run("funds from the real claim path and reserves the coins", func(t *testing.T) {
		store := newStore(t)
		mintCoins(t, store, "tx", utxostore.TierMined, 5000, 150, 1000, 50)
		f := funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1})

		result, err := f.Fund(t.Context(), baseArgs(100, 44, 1))
		require.NoError(t, err)
		require.Equal(t, []uint64{150}, allocatedSats(result), "best-fit smallest sufficient on the real store")

		// The allocated coin must actually be reserved under our token.
		got, err := store.Get(t.Context(), result.AllocatedUTXOs[0].Outpoint)
		require.NoError(t, err)
		require.Equal(t, testRef, got.ReservedBy)
	})

	t.Run("not enough funds releases every reservation", func(t *testing.T) {
		store := newStore(t)
		mintCoins(t, store, "tx", utxostore.TierMined, 100, 200)
		f := funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1})

		_, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))
		require.ErrorIs(t, err, funder.ErrNotEnoughFunds)

		bal, err := store.Balance(t.Context(), testUserID, testBasket)
		require.NoError(t, err)
		require.Zero(t, bal.Reserved, "the real store must hold no reservations after a failed fund")
	})

	t.Run("multi-input drain and release on the real store", func(t *testing.T) {
		store := newStore(t)
		mintCoins(t, store, "tx", utxostore.TierMined, 300, 200, 150, 100)
		f := funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1})

		result, err := f.Fund(t.Context(), baseArgs(549, 44, 1))
		require.NoError(t, err)
		require.Equal(t, []uint64{300, 200, 100}, allocatedSats(result))

		// 150 was released by the drain rule and must be claimable again.
		bal, err := store.Balance(t.Context(), testUserID, testBasket)
		require.NoError(t, err)
		require.EqualValues(t, 150, bal.Claimable[utxostore.TierMined])
	})

	t.Run("concurrent funders never double-allocate a coin", func(t *testing.T) {
		testConcurrentFunding(t, newStore)
	})
}

// testConcurrentFunding launches many funders against one shared pool and
// asserts the core safety invariant of claim-as-you-go under REAL concurrency:
// no coin is ever allocated to two funders, and the store's reserved balance
// matches the disjoint union of all allocations.
//
// Pool sizing: every coin is equal-valued (below target), so each funder takes
// the largest-insufficient path, which transiently reserves a whole batch
// (insufficientBatchSize = 16) before releasing the surplus. With G funders the
// peak transient reservation is G × 16; sizing the pool above that guarantees
// no funder is starved into a (correct but here-undesired) ErrNotEnoughFunds,
// keeping the liveness assertion deterministic. A smaller pool would still be
// SAFE — never a double-allocation — only some funders would fail cleanly.
func testConcurrentFunding(t *testing.T, newStore storeFactory) {
	const (
		goroutines = 16
		coinValue  = uint64(1000)
		target     = satoshi.Value(3000)
		// Above the G × insufficientBatchSize (16) peak transient reservation.
		poolCoins = goroutines * 30
	)

	store := newStore(t)
	sats := make([]uint64, poolCoins)
	for i := range sats {
		sats[i] = coinValue
	}
	mintCoins(t, store, "pool", utxostore.TierMined, sats...)

	f := funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1})

	type outcome struct {
		result *funder.Result
		err    error
	}
	outcomes := make([]outcome, goroutines)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			args := baseArgs(target, 44, 1)
			args.Reservation = fmt.Sprintf("ref-%d", idx)
			result, err := f.Fund(context.Background(), args)
			outcomes[idx] = outcome{result: result, err: err}
		}(i)
	}
	wg.Wait()

	seen := make(map[utxostore.Outpoint]int)
	successes := 0
	var totalAllocated uint64
	for idx, o := range outcomes {
		if o.err != nil {
			require.ErrorIs(t, o.err, funder.ErrNotEnoughFunds,
				"the only acceptable failure is exhaustion, got %v", o.err)
			continue
		}
		successes++
		for _, u := range o.result.AllocatedUTXOs {
			if prev, dup := seen[u.Outpoint]; dup {
				t.Fatalf("coin %s double-allocated to funders %d and %d", u.Outpoint, prev, idx)
			}
			seen[u.Outpoint] = idx
			totalAllocated += u.Satoshis
		}
	}

	require.Equal(t, goroutines, successes,
		"pool sized above peak transient reservation: every funder should succeed")

	// The store's reserved balance must equal exactly what the funders hold —
	// no coin leaked, no coin double-counted.
	bal, err := store.Balance(t.Context(), testUserID, testBasket)
	require.NoError(t, err)
	require.EqualValues(t, totalAllocated, bal.Reserved,
		"reserved balance must match the disjoint union of all allocations")
}
