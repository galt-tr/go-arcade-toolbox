package funder_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

func contentionFunder(t *testing.T, store utxostore.Store, backoffs *atomic.Int64) *funder.Funder {
	t.Helper()
	backoff := func(context.Context, int) {
		if backoffs != nil {
			backoffs.Add(1)
		}
	}
	return funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1}, funder.WithBackoffForTest(backoff))
}

// TestFund_ContentionRetriesThenSucceeds: an optimistic store that contends on
// the first two claim calls must be retried and ultimately succeed within the
// bounded attempt budget.
func TestFund_ContentionRetriesThenSucceeds(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 5000)
	store := newContentionStore(inner, 2) // fail twice, then delegate

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	result, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

	require.NoError(t, err, "must succeed after retrying past the transient contention")
	require.Equal(t, []uint64{5000}, allocatedSats(result))
	require.EqualValues(t, 2, backoffs.Load(), "two contended attempts must each back off before the third succeeds")
}

// TestFund_ContentionExhaustedReleasesReservation: persistent contention across
// every attempt yields ErrUTXOContention and leaves nothing reserved.
func TestFund_ContentionExhaustedReleasesReservation(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 5000)
	store := newContentionStore(inner, 1_000) // always contend

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	_, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

	require.ErrorIs(t, err, funder.ErrUTXOContention)
	require.Zero(t, reservedSats(t, inner), "the reservation must be released on contention exhaustion")
	// maxFundingAttempts = 3 -> two backoffs between the three attempts.
	require.EqualValues(t, 2, backoffs.Load())
}

// The terminal claimable probe (audit finding P2-4).
//
// A SQL claim that comes back empty is not proof of an empty pool: in Mode A a
// peer's uncommitted CreateAction holds row locks that FOR UPDATE SKIP LOCKED
// hides. A pass that allocated nothing therefore asks the store, once per tier,
// whether claimable coins exist after all — and reports contention (retryable)
// instead of insufficient funds (not) when they do.

// TestFund_HiddenInventoryIsContentionNotInsufficientFunds: the pool holds a
// coin that covers the target, every claim comes back empty (as it does behind
// an uncommitted peer's locks), and the probe says the coin is there.
func TestFund_HiddenInventoryIsContentionNotInsufficientFunds(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 5000)
	store := newLockedTierStore(inner, utxostore.TierMined)

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	_, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

	require.ErrorIs(t, err, funder.ErrUTXOContention,
		"the coins are there and merely locked, so this is transient. Reporting "+
			"ErrNotEnoughFunds would fail the payment outright while the user's money "+
			"sits in the wallet")
	require.NotErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.EqualValues(t, 2, backoffs.Load(), "each contended attempt must back off before the next")

	// One probe per tier per attempt, and NOT ONE MORE: the probe must never run
	// on the allocating path. Three attempts x two tiers, and the second tier is
	// only reached because the first answered false... which it does not here, so
	// exactly one probe per attempt.
	require.Equal(t,
		[]utxostore.Tier{utxostore.TierMined, utxostore.TierMined, utxostore.TierMined},
		store.probedTiers())
}

// TestFund_LockedTierDoesNotPreemptAnotherTier is the regression test for the
// design this replaced.
//
// Probing INSIDE the store, on every empty claim, makes a locked coin in the
// safest tier fatal to the whole pass: the claim returns ErrContention, the walk
// aborts instead of falling through to the next tier, and a fund that a lower
// tier could have covered turns into a retry — then, since a peer's Mode A
// transaction easily outlives three backoffs, into ErrUTXOContention. The user
// loses a payment they used to get.
//
// The two halves below are the same scenario under the two designs.
func TestFund_LockedTierDoesNotPreemptAnotherTier(t *testing.T) {
	seed := func(t *testing.T) utxostore.Store {
		t.Helper()
		inner := newMemStore()
		mintCoins(t, inner, "mined", utxostore.TierMined, 5000)       // locked away by a peer
		mintCoins(t, inner, "unproven", utxostore.TierUnproven, 5000) // free, and enough
		return inner
	}

	t.Run("empty claims let the walk continue", func(t *testing.T) {
		inner := seed(t)
		store := newLockedTierStore(inner, utxostore.TierMined)
		f := contentionFunder(t, store, nil)

		result, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

		require.NoError(t, err,
			"the unproven tier can cover this. A locked coin in a safer tier must not "+
				"pre-empt a fund that another tier can serve")
		require.Equal(t, []uint64{5000}, allocatedSats(result))
		require.Empty(t, store.probedTiers(),
			"the pass allocated, so there was nothing to diagnose: a probe here would be "+
				"pure overhead on the allocating path")
	})

	t.Run("mid-walk contention pre-empts it", func(t *testing.T) {
		inner := seed(t)
		store := &contendingTierStore{Store: inner, locked: utxostore.TierMined}
		f := contentionFunder(t, store, nil)

		_, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

		require.ErrorIs(t, err, funder.ErrUTXOContention,
			"documenting the cost of reporting contention mid-walk: the unproven coin was "+
				"there the whole time and never got claimed")
	})
}

// TestFund_NoHiddenInventoryStaysInsufficientFunds: with nothing claimable
// anywhere the answer must stay ErrNotEnoughFunds, and must not be retried —
// three attempts at a genuinely empty pool is latency the caller pays for
// nothing.
func TestFund_NoHiddenInventoryStaysInsufficientFunds(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 100)
	// Nothing is locked; the pool is simply too small.
	store := newLockedTierStore(inner, utxostore.Tier(0))

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	_, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))

	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.NotErrorIs(t, err, funder.ErrUTXOContention)
	require.Zero(t, backoffs.Load(), "an honest insufficient-funds must not be retried")
	require.Equal(t, []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven}, store.probedTiers(),
		"every tier is asked before the verdict stands, and only once")
}

// TestFund_ProbeFailureFallsBackToInsufficientFunds: the diagnosis is
// best-effort. If it cannot run, the pass keeps the answer it already had
// rather than upgrading a funding failure into a hard error.
func TestFund_ProbeFailureFallsBackToInsufficientFunds(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 100)
	store := newLockedTierStore(inner, utxostore.TierMined)
	store.probeErr = errors.New("probe exploded")

	f := contentionFunder(t, store, nil)

	_, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))

	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.NotErrorIs(t, err, funder.ErrUTXOContention)
}

// TestFund_StoreWithoutProbeIsUnchanged: a store that does not implement the
// capability (memstore here, whose claims are mutex-serialized so an empty
// claim IS an empty pool) must behave exactly as it did before the probe
// existed.
func TestFund_StoreWithoutProbeIsUnchanged(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 100)

	var backoffs atomic.Int64
	f := contentionFunder(t, inner, &backoffs)

	_, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))

	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.Zero(t, backoffs.Load())
}
