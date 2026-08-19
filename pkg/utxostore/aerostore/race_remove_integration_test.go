//go:build integration

package aerostore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// This file is an INTERNAL (package aerostore) integration test so it can drive
// the unexported removeRaceHook seam and inspect records directly via
// s.getRecord. It targets the delete window of audit finding P1-1: every delete
// path classifies a row from a getRecord snapshot and then deletes it, so a coin
// reserved, spent or unspent in between used to be destroyed anyway — silently
// removing a live input of an in-flight broadcast. Each delete now carries a
// FilterExpression re-asserting the classification, and the hook lands the
// concurrent transition deterministically inside that window.
//
// The seam-driven cases are written one per FILTER CONJUNCT, so deleting any
// single conjunct from removableFilter or phantomFilter turns exactly one of
// them red. Reaching the spentBy conjunct needs a spent row that is NOT also
// reserved (a guarded spend keeps its reservation as provenance, which the
// resBy conjunct would catch anyway), so those cases record the spend in fact
// mode — the reorg/double-spend path, and the only producer of that state.

// raceOtherSats is a second denomination (a different value bucket than
// raceSats) so a test can address one of two coins by exact amount.
const raceOtherSats = uint64(3000)

// onceHook installs a removeRaceHook that runs fn exactly once, immediately
// before a guarded delete, and clears it when the subtest ends. The hook is
// invoked synchronously on the calling goroutine, so the interleaving is
// deterministic and -race clean by construction.
func onceHook(t *testing.T, s *Store, fn func()) {
	t.Helper()
	var once sync.Once
	s.removeRaceHook = func() { once.Do(fn) }
	t.Cleanup(func() { s.removeRaceHook = nil })
}

// requireLive asserts the record still exists (the guarded delete refused to
// destroy it) and returns its current state.
func requireLive(t *testing.T, s *Store, op utxostore.Outpoint) *utxostore.UTXO {
	t.Helper()
	rec, found, err := s.getRecord(op)
	require.NoError(t, err)
	require.True(t, found, "the guarded delete must have left the record in place")
	u, cerr := recordToUTXO(rec)
	require.NoError(t, cerr)
	return u
}

// requireGone asserts the record was deleted.
func requireGone(t *testing.T, s *Store, op utxostore.Outpoint) {
	t.Helper()
	_, found, err := s.getRecord(op)
	require.NoError(t, err)
	require.False(t, found, "the uncontested row must have been removed")
}

// claimCoin reserves op (addressed by its exact denomination) under token.
func claimCoin(t *testing.T, s *Store, sc utxostore.Scope, token string, sats uint64, op utxostore.Outpoint) {
	t.Helper()
	got, err := s.ClaimExact(context.Background(), sc, token, sats, 1)
	require.NoError(t, err)
	require.Len(t, got, 1, "the coin under test must be claimable")
	require.Equal(t, op, got[0].Outpoint)
}

// spendCoinAsFact records spendTxID as the spender of op in fact mode, which —
// unlike a guarded spend — leaves the row spent WITHOUT a reservation.
func spendCoinAsFact(t *testing.T, s *Store, op utxostore.Outpoint, spendTxID chainhash.Hash) {
	t.Helper()
	sp := &utxostore.SpendOp{Outpoint: op, Reservation: "fact", SpendingTxID: spendTxID}
	require.NoError(t, s.Spend(context.Background(), []*utxostore.SpendOp{sp}, true))
	require.NoError(t, sp.Err)
}

// TestAerostore_RemoveDeleteRace lands a state transition DETERMINISTICALLY
// inside each delete path's snapshot→delete window (via the removeRaceHook seam,
// which fires immediately before the guarded delete) and asserts the row
// survives and is reported truthfully. Without the delete-time FilterExpression
// every subtest destroys the raced record instead.
func TestAerostore_RemoveDeleteRace(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	sc := utxostore.Scope{UserID: raceUserID, Basket: raceBasket, Tier: utxostore.TierSending}
	raceSpendTxID := utxostoretest.NewTxID("remove-race-in-window-spend")

	// Remove(!force) — one case per conjunct of removableFilter. A coin that
	// entered a refused state inside the window must be REFUSED with the typed
	// error for that state, never removed out from under whoever now holds it.
	removeCases := []struct {
		name string
		// transition lands inside the snapshot→delete window and must make the
		// guarded delete FILTERED_OUT.
		transition func(t *testing.T, s *Store, op utxostore.Outpoint)
		// wantErr asserts the per-item refusal Remove reports for that state.
		wantErr func(t *testing.T, err error, op utxostore.Outpoint)
		// wantRow asserts the state the surviving record must be left in.
		wantRow func(t *testing.T, u *utxostore.UTXO)
	}{
		{
			name: "ReservedInWindow", // guards the binResBy conjunct
			transition: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				claimCoin(t, s, sc, "racer", raceSats, op)
			},
			wantErr: func(t *testing.T, err error, op utxostore.Outpoint) {
				var resErr *utxostore.ReservedError
				require.ErrorAs(t, err, &resErr)
				require.Equal(t, op, resErr.Op)
				require.Equal(t, "racer", resErr.HeldBy)
			},
			wantRow: func(t *testing.T, u *utxostore.UTXO) {
				require.Equal(t, "racer", u.ReservedBy, "the reservation must survive intact")
				require.Nil(t, u.SpentBy)
			},
		},
		{
			name: "SpentInWindow", // guards the binSpentBy conjunct
			transition: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				spendCoinAsFact(t, s, op, raceSpendTxID)
			},
			wantErr: func(t *testing.T, err error, op utxostore.Outpoint) {
				var spentErr *utxostore.SpentError
				require.ErrorAs(t, err, &spentErr)
				require.Equal(t, op, spentErr.Op)
				require.Equal(t, raceSpendTxID, spentErr.Winner)
			},
			wantRow: func(t *testing.T, u *utxostore.UTXO) {
				require.NotNil(t, u.SpentBy, "the recorded spend must survive intact")
				require.Equal(t, raceSpendTxID, *u.SpentBy)
			},
		},
		{
			name: "FrozenInWindow", // guards the binFrozen conjunct
			transition: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				require.NoError(t, s.Freeze(context.Background(), []utxostore.Outpoint{op}))
			},
			wantErr: func(t *testing.T, err error, op utxostore.Outpoint) {
				var frozenErr *utxostore.FrozenError
				require.ErrorAs(t, err, &frozenErr)
				require.Equal(t, op, frozenErr.Op)
			},
			wantRow: func(t *testing.T, u *utxostore.UTXO) {
				require.True(t, u.Frozen, "the hold must survive intact")
			},
		},
	}

	for _, tc := range removeCases {
		t.Run("Remove/"+tc.name, func(t *testing.T) {
			s := newRaceStore(t, ctr)
			op := utxostoretest.MintTx(t, s, "remove-race-"+tc.name, raceUserID, raceBasket, utxostore.TierSending, raceSats)[0]

			onceHook(t, s, func() { tc.transition(t, s, op) })

			err := s.Remove(ctx, []utxostore.Outpoint{op}, false)
			require.Error(t, err, "a coin held inside the delete window must be refused, not destroyed")
			require.ErrorIs(t, err, utxostore.ErrBatch)
			tc.wantErr(t, err, op)
			tc.wantRow(t, requireLive(t, s, op))
		})
	}

	// Remove(force): unchanged semantics — an unguarded delete that removes the
	// row whatever its state.
	t.Run("RemoveForce", func(t *testing.T) {
		s := newRaceStore(t, ctr)
		op := utxostoretest.MintTx(t, s, "remove-force-race", raceUserID, raceBasket, utxostore.TierSending, raceSats)[0]

		hookFired := false
		onceHook(t, s, func() { hookFired = true })

		require.NoError(t, s.Remove(ctx, []utxostore.Outpoint{op}, true))
		require.False(t, hookFired, "the force path installs no guard")
		requireGone(t, s, op)
	})

	// RemoveByMintTx — one case per conjunct of phantomFilter. A phantom coin
	// that acquired a descendant inside the window must be reported as the
	// survivor it now is, never as Removed. The second coin of the same mint tx
	// is an uncontested phantom and must still go.
	mintTxCases := []struct {
		name       string
		transition func(t *testing.T, s *Store, op utxostore.Outpoint)
		wantReport func(t *testing.T, report utxostore.RemoveByMintReport, raced utxostore.Outpoint)
		wantRow    func(t *testing.T, u *utxostore.UTXO)
	}{
		{
			name: "ReservedInWindow", // guards the binResBy conjunct
			transition: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				claimCoin(t, s, sc, "racer", raceSats, op)
			},
			wantReport: func(t *testing.T, report utxostore.RemoveByMintReport, raced utxostore.Outpoint) {
				require.Empty(t, report.AlreadySpentBy)
				require.Len(t, report.AlreadyReserved, 1, "the raced coin must be reported reserved, not removed")
				require.Equal(t, "racer", report.AlreadyReserved[0].Reservation)
				require.Equal(t, raceUserID, report.AlreadyReserved[0].UserID)
				require.Equal(t, []utxostore.Outpoint{raced}, report.AlreadyReserved[0].Outpoints)
			},
			wantRow: func(t *testing.T, u *utxostore.UTXO) {
				require.Equal(t, "racer", u.ReservedBy)
			},
		},
		{
			name: "SpentInWindow", // guards the binSpentBy conjunct
			transition: func(t *testing.T, s *Store, op utxostore.Outpoint) {
				spendCoinAsFact(t, s, op, raceSpendTxID)
			},
			wantReport: func(t *testing.T, report utxostore.RemoveByMintReport, _ utxostore.Outpoint) {
				require.Empty(t, report.AlreadyReserved)
				require.Equal(t, []chainhash.Hash{raceSpendTxID}, report.AlreadySpentBy,
					"the raced coin's spender must be reported so the monitor re-verifies that child")
			},
			wantRow: func(t *testing.T, u *utxostore.UTXO) {
				require.NotNil(t, u.SpentBy)
				require.Equal(t, raceSpendTxID, *u.SpentBy)
			},
		},
	}

	for _, tc := range mintTxCases {
		t.Run("RemoveByMintTx/"+tc.name, func(t *testing.T) {
			s := newRaceStore(t, ctr)
			seed := "remove-by-mint-tx-race-" + tc.name
			mintTxID := utxostoretest.NewTxID(seed)
			ops := utxostoretest.MintTx(t, s, seed, raceUserID, raceBasket, utxostore.TierSending, raceSats, raceOtherSats)
			raced, phantom := ops[0], ops[1]

			onceHook(t, s, func() { tc.transition(t, s, raced) })

			report, err := s.RemoveByMintTx(ctx, mintTxID, ops)
			require.NoError(t, err)
			require.Equal(t, []utxostore.Outpoint{phantom}, report.Removed,
				"only the uncontested phantom coin may be reported removed")
			tc.wantReport(t, report, raced)

			tc.wantRow(t, requireLive(t, s, raced))
			requireGone(t, s, phantom)
		})
	}

	// RemoveSpentBy: a row a concurrent Unspend reclaimed inside the window must
	// survive and must not be counted as removed.
	t.Run("RemoveSpentBy", func(t *testing.T) {
		s := newRaceStore(t, ctr)
		spendTxID := utxostoretest.NewTxID("remove-spent-by-race-spend")
		ops := utxostoretest.MintTx(t, s, "remove-spent-by-race", raceUserID, raceBasket, utxostore.TierSending, raceSats, raceOtherSats)
		reclaimed, doomed := ops[0], ops[1]

		// Reserve and spend both coins with the same spending transaction.
		for i, sats := range []uint64{raceSats, raceOtherSats} {
			claimCoin(t, s, sc, "r", sats, ops[i])
			sp := &utxostore.SpendOp{Outpoint: ops[i], Reservation: "r", SpendingTxID: spendTxID}
			require.NoError(t, s.Spend(ctx, []*utxostore.SpendOp{sp}, false))
			require.NoError(t, sp.Err)
		}

		// The scan's row order is unspecified, so unspend a NAMED row before the
		// first delete: whichever row that delete targets, `reclaimed` ends
		// unspent-and-alive and `doomed` ends removed.
		onceHook(t, s, func() {
			n, uerr := s.Unspend(ctx, spendTxID, []utxostore.Outpoint{reclaimed})
			require.NoError(t, uerr)
			require.Equal(t, 1, n)
		})

		n, err := s.RemoveSpentBy(ctx, spendTxID)
		require.NoError(t, err)
		require.Equal(t, 1, n, "the row the concurrent Unspend reclaimed must not count as removed")

		u := requireLive(t, s, reclaimed)
		require.Nil(t, u.SpentBy, "the reclaimed row must be unspent, not deleted")
		require.Empty(t, u.ReservedBy)
		requireGone(t, s, doomed)
	})
}

// TestAerostore_RemoveClaimConcurrent races un-forced Removes against real
// reserve→release→re-reserve churn (ClaimExact and ReleaseReservation looping
// on their own goroutines until the remover finishes) over many iterations,
// with no seam involved. It asserts the invariant under every interleaving: a
// reservation that was never released is never destroyed, and a Remove that
// reports success leaves no row behind.
//
// It does NOT reliably cover removeOne's budget-exhaustion exit, and is not
// claimed to. Losing the guard even once needs a reserve to commit inside the
// sub-millisecond window between the remover's read and its delete, which this
// churn produced on the order of once per thousand remove attempts; exhausting
// the budget needs that twice with a release landing in between, which never
// occurred in any measured run. Those ErrContention branches stay defensive
// code — the invariant this test does enforce is that neither their absence nor
// their presence can cost a live coin. The subtest logs how many it saw.
func TestAerostore_RemoveClaimConcurrent(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()
	s := newRaceStore(t, ctr)
	sc := utxostore.Scope{UserID: raceUserID, Basket: raceBasket, Tier: utxostore.TierSending}

	// Sizing, all of it about hitting a sub-millisecond window often enough. A
	// guard loss needs a reserve to commit between the remover's read and its
	// delete; budget exhaustion needs that twice with a release in between. One
	// claimer cannot do it — a ClaimExact (index probe plus CAS) is slower than
	// the delete it has to interleave with — so the churn runs several claimers
	// and releasers, and each iteration drives many removes through them.
	const (
		iterations  = 60
		removeTries = 20
		claimers    = 4
		releasers   = 2
	)
	var contended int64
	for i := 0; i < iterations; i++ {
		token := fmt.Sprintf("r-%d", i)
		op := utxostoretest.MintTx(t, s, fmt.Sprintf("rc-%d", i), raceUserID, raceBasket, utxostore.TierSending, raceSats)[0]

		// Start the iteration with the coin HELD. Otherwise the remover — one
		// read plus one delete — beats the claimers' index probes and wins before
		// any churn exists to race.
		claimCoin(t, s, sc, token, raceSats, op)

		var (
			wg             sync.WaitGroup
			claims         atomic.Int64 // successful reserves, including the one above
			releases       atomic.Int64 // rows the releasers actually freed
			removedAny     atomic.Bool  // some attempt deleted the row
			churnErr, rerr error
			errMu          sync.Mutex
			iterContended  int
		)
		claims.Store(1)
		done := make(chan struct{}) // closed when the remover gives up or wins
		running := func() bool {
			select {
			case <-done:
				return false
			default:
				return true
			}
		}
		failChurn := func(err error) {
			errMu.Lock()
			defer errMu.Unlock()
			if churnErr == nil {
				churnErr = err
			}
		}

		wg.Add(claimers + releasers + 1)
		for c := 0; c < claimers; c++ {
			go func() { // re-reserve the coin as often as it comes free
				defer wg.Done()
				for running() {
					got, err := s.ClaimExact(ctx, sc, token, raceSats, 1)
					if err != nil && !errorsIsContention(err) {
						failChurn(err)
						return
					}
					if len(got) == 1 && got[0].Outpoint == op {
						claims.Add(1)
					}
				}
			}()
		}
		for r := 0; r < releasers; r++ {
			go func() { // and free it again, so the row keeps flipping
				defer wg.Done()
				for running() {
					n, err := s.ReleaseReservation(ctx, raceUserID, token)
					if err != nil {
						failChurn(err)
						return
					}
					releases.Add(int64(n))
				}
			}()
		}
		go func() { // keep removing into the churn until one attempt wins
			defer wg.Done()
			defer close(done)
			for attempt := 0; attempt < removeTries; attempt++ {
				err := s.Remove(ctx, []utxostore.Outpoint{op}, false)
				if err == nil {
					removedAny.Store(true)
					return
				}
				// A refusal is expected while the coin is held; anything not
				// reported per item is a real failure.
				if !errors.Is(err, utxostore.ErrBatch) {
					rerr = err
					return
				}
				if errorsIsContention(err) {
					iterContended++
				}
			}
		}()
		wg.Wait()

		require.NoErrorf(t, churnErr, "iter %d: unexpected churn error", i)
		require.NoErrorf(t, rerr, "iter %d: unexpected remove error", i)
		contended += int64(iterContended)

		// Only this iteration's token ever reserves this coin, and only the
		// releasers free it, so claims-releases is the number of reservations
		// still outstanding when everything settled.
		outstanding := claims.Load() - releases.Load()
		require.GreaterOrEqualf(t, outstanding, int64(0), "iter %d: more releases than claims", i)
		removed := removedAny.Load()

		_, found, gerr := s.getRecord(op)
		require.NoError(t, gerr)
		if outstanding > 0 {
			require.Truef(t, found, "iter %d: a live reservation was destroyed by the delete", i)
			require.Equalf(t, token, requireLive(t, s, op).ReservedBy, "iter %d: the reservation must survive", i)
			require.Falsef(t, removed, "iter %d: Remove reported success on a coin still reserved", i)
		}
		if removed {
			// The delete landed at an instant when nothing held the coin, and no
			// claim can reserve a row that is gone.
			require.Falsef(t, found, "iter %d: Remove reported success, so the row must be gone", i)
			require.Zerof(t, outstanding, "iter %d: a reservation outlived the row it held", i)
		}

		// Leave the bucket empty so the next iteration's claims can only see
		// their own coin (force, since the row may be reserved).
		require.NoError(t, s.Remove(ctx, []utxostore.Outpoint{op}, true))
	}
	// Diagnostic, not an assertion: how often the churn drove removeOne all the
	// way to its budget-exhaustion exit in this run.
	t.Logf("budget-exhausted removes: %d (over %d iterations)", contended, iterations)
}

// errorsIsContention reports whether err carries [utxostore.ErrContention].
func errorsIsContention(err error) bool { return errors.Is(err, utxostore.ErrContention) }
