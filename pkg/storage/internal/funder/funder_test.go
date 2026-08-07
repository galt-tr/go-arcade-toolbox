package funder_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/txutils"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

func newFunder(t *testing.T, store utxostore.Store, satPerKB int64) *funder.Funder {
	t.Helper()
	return funder.New(logging.NewTestLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: satPerKB})
}

// TestFund_FeeCoversFullSerializedInputSize is a regression guard for an
// insufficient-fee reject observed against a live arcade (GoBDK min-fee floor
// == the configured 100 sat/kB, zero margin): the funder priced each allocated
// input at only its unlocking-SCRIPT length (InputSize, 107 for P2PKH) instead
// of the FULL serialized input size (txid+vout+scriptLen-varint+script+sequence
// = 148 for P2PKH). The 41-byte-per-input undercount dropped the effective fee
// below the configured rate — e.g. 43 sats on a 464-byte tx (~92.7 sat/kB) —
// and arcade rejected the broadcast.
//
// The fee the funder commits MUST cover the transaction's real serialized size
// at the configured rate. Reconstruct that size from the funding decision (the
// allocated inputs' script lengths + the payment output + the change outputs)
// and assert the committed fee is at least the rate applied to it.
func TestFund_FeeCoversFullSerializedInputSize(t *testing.T) {
	const rate = int64(100) // matches arcade's GoBDK floor: any undercount => reject
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 1_000_000)
	f := newFunder(t, store, rate)

	// baseArgs' CurrentTxSize (44) is envelope(8) + input-count varint(1) +
	// output-count varint(1) + the single P2PKH payment output(34).
	result, err := f.Fund(t.Context(), baseArgs(21_044, 44, 1))
	require.NoError(t, err)
	require.Len(t, result.AllocatedUTXOs, 1, "one 1M coin funds the payment")

	inputScriptLens := make([]uint64, 0, len(result.AllocatedUTXOs))
	for _, u := range result.AllocatedUTXOs {
		inputScriptLens = append(inputScriptLens, uint64(u.InputSize))
	}
	// The payment output plus every change output the funder decided to create.
	outputScriptLens := make([]uint64, 0, 1+int(result.ChangeOutputsCount))
	outputScriptLens = append(outputScriptLens, txutils.P2PKHLockingScriptLength)
	for i := uint64(0); i < result.ChangeOutputsCount; i++ {
		outputScriptLens = append(outputScriptLens, txutils.P2PKHLockingScriptLength)
	}

	actualSize := txutils.TransactionSizeFromScriptLengths(inputScriptLens, outputScriptLens)
	minFee := int64(math.Ceil(float64(actualSize) / 1000 * float64(rate)))

	require.GreaterOrEqualf(t, result.Fee.Int64(), minFee,
		"committed fee %d must cover the %d-byte serialized tx at %d sat/kB (need >= %d); "+
			"a lower fee is what arcade rejects as insufficient-fee",
		result.Fee.Int64(), actualSize, rate, minFee)
}

// TestFund_FeeGrowthRecomputesRemaining pins the CRITICAL edge of the bounded
// loop: allocating an input grows the transaction size, so the fee — and thus
// the remaining need — must be recomputed before EVERY query. Ported from the
// go-wallet-toolbox bounded funder test.
//
// At 1000 sat/kB, target 10000, txSize 44: initial need is 10044. Rows: 8000,
// 2400, 2100 (all mined, all insufficient). Allocating 8000 grows the fee by
// 148 sats, so the true remaining is 10044-8000+148 = 2192 — making 2100
// insufficient even though it would satisfy the stale 2044. The correct loop
// funds with exactly [8000, 2400].
func TestFund_FeeGrowthRecomputesRemaining(t *testing.T) {
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 8000, 2400, 2100)
	f := newFunder(t, store, 1000)

	result, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))

	require.NoError(t, err)
	require.Equal(t, []uint64{8000, 2400}, allocatedSats(result),
		"fee growth must be reflected in remaining: 2400 (smallest >= 2192) chosen, not 2100")
}

// TestFund_DrainStopsWhenBatchRowBecomesSufficient pins the batch-consumption
// rule: rows from the largest-insufficient batch are consumed only while still
// strictly below the freshly recomputed remaining. Once the next batch row
// would be sufficient, the funder stops draining, releases the untouched tail,
// and re-runs the sufficient claim so the SMALLEST sufficient row is picked.
//
// Rows 300, 200, 150, 100 (mined), need 549: drain 300 (remaining 250) then 200
// (remaining 50). The next batch row 150 is now sufficient — best-fit picks the
// smallest sufficient row, 100, not the batch row 150.
func TestFund_DrainStopsWhenBatchRowBecomesSufficient(t *testing.T) {
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 300, 200, 150, 100)
	f := newFunder(t, store, 1)

	result, err := f.Fund(t.Context(), baseArgs(549, 44, 1))

	require.NoError(t, err)
	require.Equal(t, []uint64{300, 200, 100}, allocatedSats(result),
		"after draining 300+200 the remaining is 50; smallest sufficient (100) picked, not next batch row (150)")

	// The released row (150) must be claimable again: nothing left reserved beyond the allocation.
	require.EqualValues(t, 300+200+100, reservedSats(t, store))
}

// TestFund_TierWalkOrder ensures mined is preferred over unproven even when the
// unproven tier holds an exact match, and that a sufficient mined row satisfies
// the call with a single query (unproven never probed).
func TestFund_TierWalkOrder(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "unproven", utxostore.TierUnproven, 101) // exact for the 101-sat need
	mintCoins(t, inner, "mined", utxostore.TierMined, 500)
	store := newRecordingStore(inner)
	f := newFunder(t, store, 1)

	result, err := f.Fund(t.Context(), baseArgs(100, 44, 1))

	require.NoError(t, err)
	require.Equal(t, []uint64{500}, allocatedSats(result), "mined 500 must win over the exact-match unproven 101")
	require.Equal(t, []utxostore.Tier{utxostore.TierMined}, store.tiersQueried(),
		"a sufficient mined row must satisfy the call with a single query; unproven must not be probed")
}

// TestFund_TierGate covers the SpendPolicy tier list: tiers absent from the
// walk are never queried and never fund.
func TestFund_TierGate(t *testing.T) {
	t.Run("sending tier absent is never queried", func(t *testing.T) {
		inner := newMemStore()
		mintCoins(t, inner, "mined", utxostore.TierMined, 50)
		mintCoins(t, inner, "sending", utxostore.TierSending, 500) // would fund if eligible
		store := newRecordingStore(inner)
		f := newFunder(t, store, 1)

		_, err := f.Fund(t.Context(), baseArgs(100, 44, 1)) // tiers: mined, unproven

		require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
		require.NotContains(t, store.tiersQueried(), utxostore.TierSending,
			"sending must never be queried when it is not in the tier list")
	})

	t.Run("sending tier present funds after mined and unproven", func(t *testing.T) {
		inner := newMemStore()
		mintCoins(t, inner, "sending", utxostore.TierSending, 500)
		store := newRecordingStore(inner)
		f := newFunder(t, store, 1)

		args := baseArgs(100, 44, 1)
		args.Tiers = funder.SpendTiers(defs.SpendPolicyAny) // mined, unproven, sending

		result, err := f.Fund(t.Context(), args)
		require.NoError(t, err)
		require.Equal(t, []uint64{500}, allocatedSats(result))

		tiers := store.tiersQueried()
		require.Contains(t, tiers, utxostore.TierSending)
		require.Less(t, indexOf(tiers, utxostore.TierMined), indexOf(tiers, utxostore.TierUnproven))
		require.Less(t, indexOf(tiers, utxostore.TierUnproven), indexOf(tiers, utxostore.TierSending))
	})
}

// TestFund_ExhaustionReturnsErrNotEnoughFunds: a full pass over all tiers
// without a single eligible row surfaces ErrNotEnoughFunds, having probed both
// queries of both tiers.
func TestFund_ExhaustionReturnsErrNotEnoughFunds(t *testing.T) {
	store := newRecordingStore(newMemStore())
	f := newFunder(t, store, 1)

	_, err := f.Fund(t.Context(), baseArgs(100, 44, 1))

	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.Equal(t,
		[]utxostore.Tier{utxostore.TierMined, utxostore.TierMined, utxostore.TierUnproven, utxostore.TierUnproven},
		store.tiersQueried(),
		"both queries of both tiers must be probed before giving up")
}

// TestFund_NotEnoughFundsReleasesReservation is the KEY claim-as-you-go
// invariant: a partial allocation that ultimately cannot cover the target must
// release every coin it reserved before returning ErrNotEnoughFunds.
func TestFund_NotEnoughFundsReleasesReservation(t *testing.T) {
	store := newMemStore()
	// Two small mined coins totalling 300 cannot cover a 10_000 target.
	mintCoins(t, store, "tx", utxostore.TierMined, 100, 200)
	f := newFunder(t, store, 1)

	_, err := f.Fund(t.Context(), baseArgs(10_000, 44, 1))

	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.Zero(t, reservedSats(t, store), "every coin reserved during the failed pass must be released")
}

// TestFund_SmallestSufficientSingleClaim: a single sufficient coin funds the
// call in one allocation (the common path).
func TestFund_SmallestSufficientSingleClaim(t *testing.T) {
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 5000, 150, 1000, 50)
	f := newFunder(t, store, 1)

	// need 101; best-fit picks smallest sufficient: 150.
	result, err := f.Fund(t.Context(), baseArgs(100, 44, 1))
	require.NoError(t, err)
	require.Equal(t, []uint64{150}, allocatedSats(result), "best-fit picks the smallest sufficient coin, not the largest")
}

// TestFund_ChangeCountMath ports the go-wallet-toolbox change-count table. Each
// case drives Fund with a negative target (simulating user-provided inputs that
// over-cover) and outputCount 0, isolating the collector's change-count / clamp
// math. Fee is a constant 1 sat at these sizes, so the change equals the value.
func TestFund_ChangeCountMath(t *testing.T) {
	const fee = 1
	cases := []struct {
		name        string
		changeValue int64
		expectCount uint64
	}{
		{"249 below minimum desired -> single output", 249, 1},
		{"250 below minimum desired -> single output", 250, 1},
		{"1000 equal to minimum desired -> single output", 1000, 1},
		{"1001 below 125% -> single output", 1001, 1},
		{"1249 below 125% -> single output", 1249, 1},
		{"1250 equal to 125% -> two outputs", 1250, 2},
		{"2000 equal to 200% -> two outputs", 2000, 2},
		{"2249 below 225% -> two outputs", 2249, 2},
		{"2250 above 225% -> three outputs", 2250, 3},
		{"3000 desired*minimum -> desired count", 3000, 3},
		{"10000 above desired*minimum -> capped at desired count", 10000, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newMemStore()
			f := newFunder(t, store, 1)

			args := baseArgs(satoshi.MustSubtract(0, tc.changeValue+fee), 44, 0)
			args.NumberOfDesiredUTXOs = 3

			result, err := f.Fund(t.Context(), args)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectCount, result.ChangeOutputsCount)
			require.EqualValues(t, tc.changeValue, result.ChangeAmount.Int64())
		})
	}
}

// TestFund_ChangeCapMaxOutputs: a huge change is clamped to MaxChangeOutputsPerTx,
// not to NumberOfDesiredUTXOs.
func TestFund_ChangeCapMaxOutputs(t *testing.T) {
	store := newMemStore()
	f := newFunder(t, store, 1)

	args := baseArgs(-100_000_001, 44, 0) // huge over-fund -> change 100_000_000
	args.NumberOfDesiredUTXOs = 100
	args.MaxChangeOutputsPerTx = defs.DefaultChangeBasket().MaxChangeOutputsPerTx // 8

	result, err := f.Fund(t.Context(), args)
	require.NoError(t, err)
	require.EqualValues(t, defs.DefaultChangeBasket().MaxChangeOutputsPerTx, result.ChangeOutputsCount,
		"change outputs must be capped at MaxChangeOutputsPerTx, not NumberOfDesiredUTXOs")
	require.EqualValues(t, 100_000_000, result.ChangeAmount.Int64())
}

// TestFund_ExistingBasketCountClamp: the change-output count is clamped by
// (NumberOfDesiredUTXOs - ExistingBasketCount), so a basket already near its
// desired size gets fewer new change outputs.
func TestFund_ExistingBasketCountClamp(t *testing.T) {
	store := newMemStore()
	f := newFunder(t, store, 1)

	// Change of 10000 would otherwise want NumberOfDesiredUTXOs (5) outputs, but
	// 3 already exist, so only 5-3 = 2 new change outputs are allowed.
	args := baseArgs(-10_001, 44, 0)
	args.NumberOfDesiredUTXOs = 5
	args.MinimumDesiredUTXOValue = 1000
	args.MaxChangeOutputsPerTx = 8
	args.ExistingBasketCount = 3

	result, err := f.Fund(t.Context(), args)
	require.NoError(t, err)
	require.EqualValues(t, 2, result.ChangeOutputsCount,
		"change count clamped to NumberOfDesiredUTXOs - ExistingBasketCount")
}

// TestFund_DustDonation: at a high fee rate, change below the dust floor is
// donated to the miner (no change output), yet the call still succeeds.
func TestFund_DustDonation(t *testing.T) {
	store := newMemStore()
	f := newFunder(t, store, 500)

	args := baseArgs(-50, 44, 1) // one output; small over-fund
	args.NumberOfDesiredUTXOs = 1
	args.MaxChangeOutputsPerTx = 1

	result, err := f.Fund(t.Context(), args)
	require.NoError(t, err)
	require.Zero(t, result.ChangeOutputsCount, "sub-dust change must not create an output")
	require.Positive(t, result.ChangeAmount.Int64(), "sub-dust change is positive (donated to fee)")
	require.Less(t, result.ChangeAmount.Int64(), int64(192), "change must be below the dust floor (192 at 500 sat/kb)")
	require.EqualValues(t, 192, result.DustFloor.Int64())
}

// TestFund_BurnStillGetsChange: a no-output transaction whose only coin covers
// just the fee must still produce one change output (a valid tx needs an
// output) — the outputCount==0 amortization branch of remaining()/IsFunded().
func TestFund_BurnStillGetsChange(t *testing.T) {
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 2)
	f := newFunder(t, store, 1)

	args := baseArgs(-1, 44, 0)
	args.NumberOfDesiredUTXOs = 1
	args.MaxChangeOutputsPerTx = 1

	result, err := f.Fund(t.Context(), args)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, allocatedSats(result))
	require.EqualValues(t, 1, result.ChangeOutputsCount)
	require.EqualValues(t, 2, result.ChangeAmount.Int64())
	require.EqualValues(t, 1, result.Fee.Int64())
}

// TestFund_BurnErrorsWhenNoViableChange: when the only coin cannot fund even a
// dust-floor change output for a no-output tx, funding fails and releases.
func TestFund_BurnErrorsWhenNoViableChange(t *testing.T) {
	store := newMemStore()
	mintCoins(t, store, "tx", utxostore.TierMined, 1)
	f := newFunder(t, store, 1)

	args := baseArgs(-1, 44, 0)
	args.NumberOfDesiredUTXOs = 1
	args.MaxChangeOutputsPerTx = 1

	_, err := f.Fund(t.Context(), args)
	require.ErrorIs(t, err, funder.ErrNotEnoughFunds)
	require.Zero(t, reservedSats(t, store), "the coin reserved during the failed pass must be released")
}

// TestFund_Validation rejects malformed args.
func TestFund_Validation(t *testing.T) {
	f := newFunder(t, newMemStore(), 1)
	valid := baseArgs(100, 44, 1)

	bad := valid
	bad.Reservation = ""
	_, err := f.Fund(t.Context(), bad)
	require.Error(t, err)

	bad = valid
	bad.Tiers = nil
	_, err = f.Fund(t.Context(), bad)
	require.Error(t, err)

	bad = valid
	bad.MaxChangeOutputsPerTx = 0
	_, err = f.Fund(t.Context(), bad)
	require.Error(t, err)
}

// TestFund_BoundedClaimCountFlatInPoolSize is the deterministic form of the
// benchmark's flat-scaling assertion: funding the same target issues the SAME
// number of store claims whether the pool holds 1000 or 10000 coins. That
// constant is the bounded-selection guarantee — the funder never scans or locks
// the whole pool.
func TestFund_BoundedClaimCountFlatInPoolSize(t *testing.T) {
	f1000, rec1000 := seedPool(t, 1000)
	_, err := f1000.Fund(t.Context(), baseArgs(benchTarget, benchTxSize, benchOutputCount))
	require.NoError(t, err)

	f10000, rec10000 := seedPool(t, 10000)
	_, err = f10000.Fund(t.Context(), baseArgs(benchTarget, benchTxSize, benchOutputCount))
	require.NoError(t, err)

	require.Equal(t, rec1000.count(), rec10000.count(),
		"claim count must be identical across pool sizes (bounded selection)")
	require.LessOrEqual(t, rec10000.count(), 8, "claim count must be a small constant, not O(pool)")
}

func indexOf(tiers []utxostore.Tier, target utxostore.Tier) int {
	for i, tier := range tiers {
		if tier == target {
			return i
		}
	}
	return -1
}
