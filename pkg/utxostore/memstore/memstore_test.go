package memstore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// TestMemStoreConformance runs the full backend-agnostic conformance suite
// against the in-memory reference implementation. Memstore implements true
// best-fit selection, so the exact-selection assertions are enabled, and its
// injectable clock powers the deterministic timestamp subtests.
func TestMemStoreConformance(t *testing.T) {
	utxostoretest.RunStoreSuite(t,
		func(t *testing.T) utxostore.Store {
			s := memstore.New()
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			return s
		},
		utxostoretest.WithExactSelection(),
		utxostoretest.WithManualClock(func(t *testing.T) (utxostore.Store, utxostoretest.AdvanceFunc) {
			var (
				mu  sync.Mutex
				now = time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
			)
			s := memstore.New(memstore.WithClock(func() time.Time {
				mu.Lock()
				defer mu.Unlock()
				return now
			}))
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			return s, func(d time.Duration) {
				mu.Lock()
				now = now.Add(d)
				mu.Unlock()
			}
		}),
	)
}

// TestWithClock verifies the injectable clock stamps CreatedAt/ReservedAt,
// which later tasks rely on when using memstore as a deterministic fake.
func TestWithClock(t *testing.T) {
	ctx := context.Background()
	frozen := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s := memstore.New(memstore.WithClock(func() time.Time { return frozen }))
	t.Cleanup(func() { _ = s.Close(ctx) })

	op := utxostoretest.NewOutpoint("clock", 0)
	m := utxostoretest.NewMint(op, 1, "default", utxostore.TierMined, 42)
	require.NoError(t, s.Mint(ctx, []*utxostore.Mint{m}))

	u, err := s.Get(ctx, op)
	require.NoError(t, err)
	require.Equal(t, frozen, u.CreatedAt)

	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	claimed, err := s.ClaimSmallestSufficient(ctx, sc, "res-clock", 1)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, frozen, claimed.ReservedAt)
}
