//go:build integration

package aerostore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// This file is an INTERNAL (package aerostore) integration test so it can drive
// the unexported restoreRaceHook seam and inspect claimKey secondary-index
// membership directly via s.probe / s.getRecord. It targets the stale-tier
// claimKey window: the restore paths (release/unspend/unfreeze) must restore a
// claimKey whose embedded tier tracks the record's LIVE tier bin, so a Promote
// that races the caller's snapshot read never leaves the coin claimable under a
// stale tier's bucket.

var raceQuietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

var raceSetCounter atomic.Int64

const (
	raceUserID = int64(1)
	raceBasket = "default"
	raceSats   = uint64(1000) // bucket 9
)

// newRaceStore builds a store on a UNIQUE set (its own indexes) so each subtest
// is isolated, mirroring the external suite's newAeroStore.
func newRaceStore(t *testing.T, ctr *testenv.AerospikeContainer) *Store {
	t.Helper()
	set := fmt.Sprintf("rp%d", raceSetCounter.Add(1))
	s, err := New(context.Background(), ctr.Host(), ctr.Port(), ctr.Namespace(),
		WithSet(set), WithLogger(raceQuietLogger))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// assertClaimableUnderLiveTierOnly verifies the record's tier bin is mined, the
// coin is fully claimable (unreserved, unspent, unfrozen), and its claimKey is
// visible in the secondary index under the MINED bucket only — never the stale
// SENDING bucket.
func assertClaimableUnderLiveTierOnly(t *testing.T, s *Store, op utxostore.Outpoint) {
	t.Helper()
	rec, found, err := s.getRecord(op)
	require.NoError(t, err)
	require.True(t, found, "coin must still exist")
	u, err := recordToUTXO(rec)
	require.NoError(t, err)

	require.Equal(t, utxostore.TierMined, u.Tier, "Promote must have advanced the tier bin")
	require.Empty(t, u.ReservedBy, "coin must end unreserved")
	require.Nil(t, u.SpentBy, "coin must end unspent")
	require.False(t, u.Frozen, "coin must end unfrozen")

	bucket := bucketOf(raceSats)
	minedKey := claimKeyFor(raceUserID, raceBasket, utxostore.TierMined, bucket)
	sendingKey := claimKeyFor(raceUserID, raceBasket, utxostore.TierSending, bucket)

	minedCands, err := s.probe(context.Background(), minedKey, nil, 100)
	require.NoError(t, err)
	require.Len(t, minedCands, 1, "coin must be claimable under its LIVE (mined) tier bucket, not stuck claimKey-absent")

	staleCands, err := s.probe(context.Background(), sendingKey, nil, 100)
	require.NoError(t, err)
	require.Empty(t, staleCands, "coin must NOT be claimable under the STALE (sending) tier bucket")
}

// TestAerostore_PromoteRestoreRace lands a Promote(sending -> mined)
// DETERMINISTICALLY inside each restore path's snapshot->restore window (via the
// restoreRaceHook seam, which fires immediately before the server-side restore
// Operate). Because the restored claimKey is derived from the record's live tier
// bin, the coin must end claimable under the mined bucket only. The pre-fix code
// restored a key computed off the pre-Promote snapshot tier and would leave the
// coin claimable under the stale sending bucket, failing
// assertClaimableUnderLiveTierOnly.
func TestAerostore_PromoteRestoreRace(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	sc := utxostore.Scope{UserID: raceUserID, Basket: raceBasket, Tier: utxostore.TierSending}
	spendTxID := utxostoretest.NewTxID("promote-restore-race-spend")

	cases := []struct {
		name string
		// setup drives the freshly minted (sending) coin into the pre-restore
		// state (reserved / spent / frozen).
		setup func(t *testing.T, s *Store, op utxostore.Outpoint)
		// restore performs the release/unspend/unfreeze call whose in-window
		// Promote is under test.
		restore func(t *testing.T, s *Store, op utxostore.Outpoint)
	}{
		{
			name: "ReleaseReservation",
			setup: func(t *testing.T, s *Store, _ utxostore.Outpoint) {
				got, err := s.ClaimExact(ctx, sc, "r", raceSats, 1)
				require.NoError(t, err)
				require.Len(t, got, 1)
			},
			restore: func(t *testing.T, s *Store, _ utxostore.Outpoint) {
				n, err := s.ReleaseReservation(ctx, raceUserID, "r")
				require.NoError(t, err)
				require.Equal(t, 1, n)
			},
		},
		{
			name: "ReleaseOutpoints",
			setup: func(t *testing.T, s *Store, _ utxostore.Outpoint) {
				got, err := s.ClaimExact(ctx, sc, "r", raceSats, 1)
				require.NoError(t, err)
				require.Len(t, got, 1)
			},
			restore: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				require.NoError(t, s.ReleaseOutpoints(ctx, "r", []utxostore.Outpoint{op}))
			},
		},
		{
			name: "Unspend",
			setup: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				got, err := s.ClaimExact(ctx, sc, "r", raceSats, 1)
				require.NoError(t, err)
				require.Len(t, got, 1)
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
			s := newRaceStore(t, ctr)
			op := utxostoretest.MintTx(t, s, tc.name, raceUserID, raceBasket, utxostore.TierSending, raceSats)[0]
			tc.setup(t, s, op)

			// Land the Promote exactly once, inside the restore path's
			// snapshot->restore window (the hook fires just before the restore
			// Operate). Single-threaded, so -race clean by construction.
			var once sync.Once
			s.restoreRaceHook = func() {
				once.Do(func() {
					n, err := s.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierMined)
					require.NoError(t, err)
					require.Equal(t, 1, n, "in-window Promote must retier the coin")
				})
			}

			tc.restore(t, s, op)
			s.restoreRaceHook = nil

			assertClaimableUnderLiveTierOnly(t, s, op)
		})
	}
}

// TestAerostore_PromoteReleaseConcurrent races a real concurrent Promote against
// ReleaseReservation (no seam) over many iterations and asserts the invariant
// holds under every interleaving: the coin ends at the mined tier and claimable
// under the mined bucket only. This is the -race check on the client-side logic
// under genuine concurrency; the outcome is interleaving-independent because the
// restored claimKey is derived server-side from the live tier bin.
func TestAerostore_PromoteReleaseConcurrent(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newRaceStore(t, ctr)
	sc := utxostore.Scope{UserID: raceUserID, Basket: raceBasket, Tier: utxostore.TierSending}
	bucket := bucketOf(raceSats)
	minedKey := claimKeyFor(raceUserID, raceBasket, utxostore.TierMined, bucket)
	sendingKey := claimKeyFor(raceUserID, raceBasket, utxostore.TierSending, bucket)

	const iterations = 60
	for i := 0; i < iterations; i++ {
		token := fmt.Sprintf("r-%d", i)
		op := utxostoretest.MintTx(t, s, fmt.Sprintf("cc-%d", i), raceUserID, raceBasket, utxostore.TierSending, raceSats)[0]

		got, err := s.ClaimExact(ctx, sc, token, raceSats, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)

		start := make(chan struct{})
		var (
			wg         sync.WaitGroup
			perr, rerr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, perr = s.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierMined)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, rerr = s.ReleaseReservation(ctx, raceUserID, token)
		}()
		close(start)
		wg.Wait()
		require.NoError(t, perr)
		require.NoError(t, rerr)

		// After both settle: tier is mined (Promote sets it; release never
		// touches it) and the claimKey tracks it under the mined bucket only.
		rec, found, gerr := s.getRecord(op)
		require.NoError(t, gerr)
		require.True(t, found)
		u, cerr := recordToUTXO(rec)
		require.NoError(t, cerr)
		require.Equal(t, utxostore.TierMined, u.Tier)
		require.Empty(t, u.ReservedBy)

		minedCands, mErr := s.probe(ctx, minedKey, nil, 1000)
		require.NoError(t, mErr)
		require.Truef(t, containsOutpoint(t, minedCands, op), "iter %d: coin missing from mined bucket", i)

		staleCands, sErr := s.probe(ctx, sendingKey, nil, 1000)
		require.NoError(t, sErr)
		require.Falsef(t, containsOutpoint(t, staleCands, op), "iter %d: coin STILL claimable under stale sending bucket", i)
	}
}

func containsOutpoint(t *testing.T, cands []candidate, op utxostore.Outpoint) bool {
	t.Helper()
	for _, c := range cands {
		u, err := recordToUTXO(c.rec)
		require.NoError(t, err)
		if u.Outpoint == op {
			return true
		}
	}
	return false
}
