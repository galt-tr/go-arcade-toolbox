//go:build integration

package aerostore_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/aerostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

var setCounter atomic.Int64

// newAeroStore builds a fresh store on a UNIQUE Aerospike set (its own set of
// secondary indexes), so each conformance subtest is fully isolated despite
// sharing one container and the suite's fixed user/basket.
func newAeroStore(t *testing.T, ctr *testenv.AerospikeContainer, opts ...aerostore.Option) *aerostore.Store {
	t.Helper()
	ctx := context.Background()
	set := fmt.Sprintf("s%d", setCounter.Add(1))
	all := append([]aerostore.Option{aerostore.WithSet(set), aerostore.WithLogger(quietLogger)}, opts...)
	s, err := aerostore.New(ctx, ctr.Host(), ctr.Port(), ctr.Namespace(), all...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })
	return s
}

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// TestAerostore_Conformance runs the full backend-agnostic conformance suite
// against Aerospike WITHOUT WithExactSelection (its selection is bucket-
// approximate); correctness is asserted, selection-optimality relaxed. Run
// under -race, the ClaimExclusivityConcurrent subtest is the core no-double-
// claim / no-lost-coin property.
func TestAerostore_Conformance(t *testing.T) {
	ctr := testenv.StartAerospike(t)

	newStore := func(t *testing.T) utxostore.Store {
		return newAeroStore(t, ctr)
	}
	newStoreClock := func(t *testing.T) (utxostore.Store, utxostoretest.AdvanceFunc) {
		clk := &manualClock{t: baseTime}
		return newAeroStore(t, ctr, aerostore.WithClock(clk.now)), clk.advance
	}

	utxostoretest.RunStoreSuite(
		t, newStore,
		// NO WithExactSelection: bucket-approximate selection.
		utxostoretest.WithManualClock(newStoreClock),
	)
}

// TestAerostore_RefusesNonZeroTTL proves the fund-safety guard: the store
// refuses to start against a namespace whose default-ttl is not 0. It flips the
// container's namespace default-ttl dynamically via asinfo set-config, asserts
// New fails, then restores default-ttl=0.
func TestAerostore_RefusesNonZeroTTL(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()

	client, cerr := as.NewClientWithPolicyAndHost(as.NewClientPolicy(), as.NewHost(ctr.Host(), ctr.Port()))
	require.NoError(t, cerr)
	defer client.Close()

	// Flip the (ephemeral) container's namespace default-ttl to a nonzero value
	// via asinfo set-config. The node is re-fetched each attempt because a
	// reconfig can invalidate a previously-captured node handle.
	cmd := "set-config:context=namespace;id=" + ctr.Namespace() + ";default-ttl=2592000" // 30 days
	deadline := time.Now().Add(10 * time.Second)
	for {
		nodes := client.GetNodes()
		if len(nodes) > 0 {
			resp, ierr := nodes[0].RequestInfo(as.NewInfoPolicy(), cmd)
			if ierr == nil {
				for _, r := range resp {
					require.Equal(t, "ok", r, "set-config default-ttl")
				}
				break
			}
		}
		require.False(t, time.Now().After(deadline), "set-config default-ttl timed out")
		time.Sleep(100 * time.Millisecond)
	}

	// The store must now REFUSE to start (fund-safety guard). The container is
	// discarded after this test, so there is no need to restore default-ttl=0.
	_, nerr := aerostore.New(ctx, ctr.Host(), ctr.Port(), ctr.Namespace(), aerostore.WithLogger(quietLogger))
	require.Error(t, nerr, "store must refuse a namespace with default-ttl != 0")
	require.Contains(t, nerr.Error(), "REFUSING TO START")
}

// TestAerostore_RemoveDurableDelete proves Remove deletes durably on the
// running edition (durable delete auto-disabled on Community so the call does
// not fail with ENTERPRISE_ONLY) and the row is gone afterward.
func TestAerostore_RemoveDurableDelete(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newAeroStore(t, ctr)

	ops := utxostoretest.MintTx(t, s, "dd", 1, "default", utxostore.TierMined, 1000)
	require.NoError(t, s.Remove(ctx, ops, false))
	_, err := s.Get(ctx, ops[0])
	require.ErrorIs(t, err, &utxostore.NotFoundError{})

	// Force delete of a reserved row also succeeds durably.
	ops = utxostoretest.MintTx(t, s, "dd2", 1, "default", utxostore.TierMined, 2000)
	claimed, err := s.ClaimExact(ctx, utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}, "r", 2000, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, s.Remove(ctx, ops, true))
	_, err = s.Get(ctx, ops[0])
	require.ErrorIs(t, err, &utxostore.NotFoundError{})
}

// TestAerostore_BucketWalkCompleteness proves the log2-bucket claim walk never
// misses a sufficient coin that exists: with only insufficient coins in low
// buckets and exactly one sufficient coin in a distant high bucket, both
// ClaimSmallestSufficient and ClaimLargestInsufficient locate the right coin.
func TestAerostore_BucketWalkCompleteness(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newAeroStore(t, ctr)
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	// Coins spread across very different buckets: 3 (b1), 40 (b5), 5000 (b12),
	// 9_000_000 (b23). Only 5000 and 9_000_000 are >= 500.
	utxostoretest.MintTx(t, s, "bw", 1, "default", utxostore.TierMined, 3, 40, 5000, 9_000_000)

	// Smallest sufficient >= 500: must find a coin, and it must be >= 500 (the
	// 5000 one under any reasonable approximation, definitely not 3 or 40).
	u, err := s.ClaimSmallestSufficient(ctx, sc, "r1", 500)
	require.NoError(t, err)
	require.NotNil(t, u, "a sufficient coin exists and must not be missed")
	require.GreaterOrEqual(t, u.Satoshis, uint64(500))

	// Smallest sufficient >= 6000: only the 9_000_000 coin qualifies (a bucket
	// far above the start). The walk must reach it.
	u, err = s.ClaimSmallestSufficient(ctx, sc, "r2", 6000)
	require.NoError(t, err)
	require.NotNil(t, u, "the single high-bucket sufficient coin must not be missed")
	require.Equal(t, uint64(9_000_000), u.Satoshis)

	// Nothing >= 10_000_000: genuine miss returns (nil, nil), not an error.
	u, err = s.ClaimSmallestSufficient(ctx, sc, "r3", 10_000_000)
	require.NoError(t, err)
	require.Nil(t, u)

	// Largest strictly below 5000: walks down and finds the 40 (largest < 5000
	// among the remaining 3, 40).
	got, err := s.ClaimLargestInsufficient(ctx, sc, "r4", 5000, 5)
	require.NoError(t, err)
	require.NotEmpty(t, got)
	for _, c := range got {
		require.Less(t, c.Satoshis, uint64(5000))
	}
}

// TestAerostore_ClaimExactPoolAndUnderflow proves the 1000-TPS denominated
// path: a denomination pool is a single index probe, and underflow returns
// short WITHOUT error (pool exhausted, not a failure).
func TestAerostore_ClaimExactPoolAndUnderflow(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newAeroStore(t, ctr)
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	const denom = 1000
	sats := make([]uint64, 20)
	for i := range sats {
		sats[i] = denom
	}
	utxostoretest.MintTx(t, s, "exactpool", 1, "default", utxostore.TierMined, sats...)

	got, err := s.ClaimExact(ctx, sc, "r1", denom, 15)
	require.NoError(t, err)
	require.Len(t, got, 15)
	for _, u := range got {
		require.Equal(t, uint64(denom), u.Satoshis)
	}

	// 5 left; asking for 12 underflows: short result, NO error.
	got, err = s.ClaimExact(ctx, sc, "r2", denom, 12)
	require.NoError(t, err)
	require.Len(t, got, 5, "underflow returns short, not an error")

	// Pool now empty: empty result, no error.
	got, err = s.ClaimExact(ctx, sc, "r3", denom, 3)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestAerostore_ContentionNoDoubleClaim hammers a denomination pool with N
// goroutines and asserts every coin is claimed exactly once (exclusivity), with
// ErrContention tolerated as a retryable transient. Complements the conformance
// suite's ClaimExclusivityConcurrent with a focused, -race-friendly check.
func TestAerostore_ContentionNoDoubleClaim(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newAeroStore(t, ctr)
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	const (
		coins    = 200
		workers  = 12
		denom    = 500
		perClaim = 4
	)
	sats := make([]uint64, coins)
	for i := range sats {
		sats[i] = denom
	}
	utxostoretest.MintTx(t, s, "race", 1, "default", utxostore.TierMined, sats...)

	var mu sync.Mutex
	claimedBy := make(map[utxostore.Outpoint]string, coins)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		token := fmt.Sprintf("w-%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := s.ClaimExact(ctx, sc, token, denom, perClaim)
				if err == utxostore.ErrContention {
					continue
				}
				require.NoError(t, err)
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, u := range got {
					require.Equal(t, token, u.ReservedBy)
					_, dup := claimedBy[u.Outpoint]
					require.Falsef(t, dup, "double claim of %s", u.Outpoint)
					claimedBy[u.Outpoint] = token
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Len(t, claimedBy, coins, "every coin claimed exactly once, none lost")
}
