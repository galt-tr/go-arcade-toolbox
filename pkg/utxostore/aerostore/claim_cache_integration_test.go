//go:build integration

package aerostore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// This file is an INTERNAL (package aerostore) integration test so it can drive
// the unexported restoreRaceHook seam and read claimKey secondary-index
// membership directly through s.probe. It targets the claim cache's three
// availability failures against a real server: a negative cache no other process
// can invalidate (P2-5), a whole-bucket sample that hides a matching coin
// (P2-6b), and a restore that invalidates the wrong tier's bucket (P2-6c).

var cacheSetCounter atomic.Int64

// newCacheStore builds a store on the given set, so two of them can be pointed
// at the SAME data — which is how a second process is modeled.
func newCacheStore(t *testing.T, ctr *testenv.AerospikeContainer, set string, opts ...Option) *Store {
	t.Helper()
	all := append([]Option{WithSet(set), WithLogger(raceQuietLogger)}, opts...)
	s, err := New(context.Background(), ctr.Host(), ctr.Port(), ctr.Namespace(), all...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func uniqueCacheSet() string { return fmt.Sprintf("cc%d", cacheSetCounter.Add(1)) }

// TestClaimCache_CrossProcessMintVisibility is the audit P2-5 repro against a
// real cluster, with two Stores over one set standing in for two replicas.
//
// Replica A probes a bucket while it is empty and caches that verdict. Replica B
// then funds the wallet. A's event-driven invalidation cannot possibly hear
// about it — markClaimable is an in-process call — so before the TTL existed, A
// refused payments on a funded wallet FOREVER: only a restart cleared the
// verdict. The TTL is what turns "forever" into a bounded second.
func TestClaimCache_CrossProcessMintVisibility(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	set := uniqueCacheSet()

	const (
		userID = int64(1)
		basket = "default"
		sats   = uint64(1000)
	)
	sc := utxostore.Scope{UserID: userID, Basket: basket, Tier: utxostore.TierMined}

	clk := newFakeClock()
	a := newCacheStore(t, ctr, set, WithClock(clk.now), WithClaimCacheEmptyTTL(time.Second))
	b := newCacheStore(t, ctr, set)

	// A looks first, and finds nothing: this is the verdict that gets cached.
	u, err := a.ClaimSmallestSufficient(ctx, sc, "r0", 100)
	require.NoError(t, err)
	require.Nil(t, u, "the wallet really is empty at this point")

	// B funds the wallet. A is never told.
	utxostoretest.MintTx(t, b, "cross-process", userID, basket, utxostore.TierMined, sats)

	// Inside the TTL, A is allowed to be stale — that is the bargain the
	// negative cache strikes for a payment path that walks empty tiers.
	u, err = a.ClaimSmallestSufficient(ctx, sc, "r1", 100)
	require.NoError(t, err)
	require.Nil(t, u, "within the TTL the stale empty verdict is expected")

	// Past it, A must see the coin. Before the fix this claim failed forever.
	clk.advance(2 * time.Second)
	u, err = a.ClaimSmallestSufficient(ctx, sc, "r2", 100)
	require.NoError(t, err)
	require.NotNil(t, u, "past the TTL the coin another process minted must be claimable")
	require.Equal(t, sats, u.Satoshis)

	// And B, which never cached an empty verdict for this bucket, sees the coin
	// as reserved by A — the two replicas agree on the server state.
	got, err := b.Get(ctx, u.Outpoint)
	require.NoError(t, err)
	require.Equal(t, "r2", got.ReservedBy)
}

// TestClaimCache_SufficientCoinOutsideSample is the audit P2-6b repro. The
// whole-bucket refill asks the index for at most `refill` rows with NO value
// filter, so a bucket holding more coins than that can return a sample in which
// nothing satisfies the caller's minimum — and the claim then reported the pool
// exhausted while a sufficient coin sat in that very bucket.
//
// The refill is set to 1 against 50 filler coins to make the shape (bucket
// population ≫ sample) cheap to build; the production default is 512, which
// needs 513 coins in one power-of-two band to hit the same wall. Five
// independent baskets are checked because the sample is chosen by the index, not
// by the test: each one has a 1-in-51 chance of drawing the sufficient coin by
// luck, so all five passing without the fallback probe is a ~1e-8 event.
func TestClaimCache_SufficientCoinOutsideSample(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newCacheStore(t, ctr, uniqueCacheSet(),
		WithClaimCache(1), WithClaimCacheEmptyTTL(time.Hour))

	const (
		userID  = int64(1)
		fillers = 50
		small   = uint64(512)  // bucket 9
		target  = uint64(1000) // bucket 9 as well: same claimKey, bigger value
		minSats = uint64(900)
	)

	for i := 0; i < 5; i++ {
		basket := fmt.Sprintf("b%d", i)
		sats := make([]uint64, 0, fillers+1)
		for j := 0; j < fillers; j++ {
			sats = append(sats, small)
		}
		sats = append(sats, target)
		utxostoretest.MintTx(t, s, "sample-"+basket, userID, basket, utxostore.TierMined, sats...)

		sc := utxostore.Scope{UserID: userID, Basket: basket, Tier: utxostore.TierMined}
		u, err := s.ClaimSmallestSufficient(ctx, sc, "r-"+basket, minSats)
		require.NoErrorf(t, err, "basket %s", basket)
		require.NotNilf(t, u, "basket %s: the bucket holds a sufficient coin the unfiltered sample missed", basket)
		require.Equal(t, target, u.Satoshis)
	}
}

// TestClaimCache_PromoteRaceRestoreVisibleInNewTier is the audit P2-6c repro. A
// restore derives the claimKey's tier from the record's LIVE tier bin (so a
// racing Promote cannot strand a stale-tier key on the server), but the cache
// was told about the SNAPSHOT's tier — so the bucket the coin actually landed in
// kept whatever empty-probe suppression it had, and no claim in this process
// could see a coin that was plainly claimable on the server.
//
// The interleaving is staged deterministically through the restore-race seam,
// which fires immediately before the restore Operate: inside that window the
// coin is promoted to mined AND a mined-tier claim probes the (still empty)
// mined bucket, caching the verdict that the restore then has to invalidate.
//
// All THREE restore paths are driven, because they are three separate call
// sites of the same rule and nothing else covers two of them: reverting
// releaseRow alone is caught by the release case only, and Unspend and Unfreeze
// were previously asserted by no test at all.
func TestClaimCache_PromoteRaceRestoreVisibleInNewTier(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()

	const (
		userID = int64(1)
		basket = "default"
		sats   = uint64(1000)
	)
	sending := utxostore.Scope{UserID: userID, Basket: basket, Tier: utxostore.TierSending}
	mined := utxostore.Scope{UserID: userID, Basket: basket, Tier: utxostore.TierMined}
	spendTxID := utxostoretest.NewTxID("cache-promote-race-spend")

	claimSending := func(t *testing.T, s *Store) {
		t.Helper()
		claimed, err := s.ClaimExact(ctx, sending, "r", sats, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
	}

	cases := []struct {
		name string
		// setup drives the freshly minted (sending) coin into the state the
		// restore under test undoes.
		setup func(t *testing.T, s *Store, op utxostore.Outpoint)
		// restore runs the path whose in-window Promote is under test.
		restore func(t *testing.T, s *Store, op utxostore.Outpoint)
	}{
		{
			name:  "ReleaseReservation",
			setup: func(t *testing.T, s *Store, _ utxostore.Outpoint) { claimSending(t, s) },
			restore: func(t *testing.T, s *Store, _ utxostore.Outpoint) {
				n, err := s.ReleaseReservation(ctx, userID, "r")
				require.NoError(t, err)
				require.Equal(t, 1, n)
			},
		},
		{
			name: "Unspend",
			setup: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				claimSending(t, s)
				sp := &utxostore.SpendOp{Outpoint: op, Reservation: "r", SpendingTxID: spendTxID}
				require.NoError(t, s.Spend(ctx, []*utxostore.SpendOp{sp}, false))
				require.NoError(t, sp.Err)
			},
			restore: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				n, err := s.Unspend(ctx, spendTxID, []utxostore.Outpoint{op})
				require.NoError(t, err)
				require.Equal(t, 1, n)
			},
		},
		{
			name: "Unfreeze",
			setup: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				require.NoError(t, s.Freeze(ctx, []utxostore.Outpoint{op}))
			},
			restore: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				require.NoError(t, s.Unfreeze(ctx, []utxostore.Outpoint{op}))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A long TTL so the assertion can only be satisfied by invalidating
			// the right bucket, never by the negative cache expiring on its own.
			s := newCacheStore(t, ctr, uniqueCacheSet(), WithClaimCacheEmptyTTL(time.Hour))
			op := utxostoretest.MintTx(t, s, "promote-cache-race-"+tc.name, userID, basket, utxostore.TierSending, sats)[0]
			tc.setup(t, s, op)

			var once sync.Once
			s.restoreRaceHook = func() {
				once.Do(func() {
					n, perr := s.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierMined)
					require.NoError(t, perr)
					require.Equal(t, 1, n, "the in-window Promote must retier the coin")

					// A mined-tier claim lands after the Promote and before the
					// restore: the coin is still held (reserved, spent or frozen),
					// so it probes an empty bucket and caches that. This is the
					// verdict the restore has to invalidate.
					got, cerr := s.ClaimExact(ctx, mined, "poison", sats, 1)
					require.NoError(t, cerr)
					require.Empty(t, got, "the coin is not claimable yet at this point")
				})
			}
			tc.restore(t, s, op)
			s.restoreRaceHook = nil

			// The server has it right: the coin is claimable under the MINED
			// bucket, because the restore derived the key from the live tier bin.
			minedKey := claimKeyFor(userID, basket, utxostore.TierMined, bucketOf(sats))
			cands, err := s.probe(ctx, minedKey, nil, 100)
			require.NoError(t, err)
			require.Len(t, cands, 1, "precondition: the restore made the coin claimable in the mined bucket")

			// So a claim must find it. Before the fix the restore invalidated the
			// SENDING bucket and this returned nothing — a coin visible to every
			// other process, unclaimable by this one.
			got, err := s.ClaimExact(ctx, mined, "r2", sats, 1)
			require.NoError(t, err)
			require.Len(t, got, 1, "the restored coin must be claimable in the tier it actually landed in")
			require.Equal(t, op, got[0].Outpoint)
		})
	}
}

// TestClaimCache_EvictedBucketStillClaims pins the eviction safety property
// (audit P2-8) end to end: with a cap of one bucket, every claim evicts the
// previous one, and claims must still be exactly correct — an evicted bucket can
// only cost a re-probe, because the sole thing it could have carried forward is
// a suppression.
func TestClaimCache_EvictedBucketStillClaims(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newCacheStore(t, ctr, uniqueCacheSet(), WithClaimCacheBuckets(1))

	const (
		userID = int64(1)
		basket = "default"
		denom  = uint64(1000)
		coins  = 12
	)
	sc := utxostore.Scope{UserID: userID, Basket: basket, Tier: utxostore.TierMined}

	sats := make([]uint64, coins)
	for i := range sats {
		sats[i] = denom
	}
	utxostoretest.MintTx(t, s, "evict", userID, basket, utxostore.TierMined, sats...)

	seen := make(map[utxostore.Outpoint]bool, coins)
	for i := 0; i < coins; i++ {
		// Alternate buckets so each claim evicts the other's snapshot.
		got, err := s.ClaimExact(ctx, sc, fmt.Sprintf("r-%d", i), denom, 1)
		require.NoError(t, err)
		require.Len(t, got, 1, "claim %d must still find a coin with a one-bucket cache", i)
		require.False(t, seen[got[0].Outpoint], "no coin may be claimed twice")
		seen[got[0].Outpoint] = true

		_, err = s.ClaimExact(ctx, sc, "other", denom*4, 1) // a different value-bucket
		require.NoError(t, err)
	}
	require.Len(t, seen, coins, "every coin claimed exactly once, none lost to eviction")
	require.LessOrEqual(t, len(s.claimCache.buckets), 1, "the cap holds")
}
