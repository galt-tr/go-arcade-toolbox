package funder_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

func throughputArgs(target satoshi.Value, denomination uint64) funder.FundArgs {
	args := baseArgs(target, 44, 1)
	args.Denomination = denomination
	return args
}

// TestFund_Throughput_ExactMatch: a single denomination coin funds a
// small action in one ClaimExact (the fast-path happy case).
func TestFund_Throughput_ExactMatch(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "fuel", utxostore.TierMined, 1000, 1000, 1000)
	store := newRecordingStore(inner)
	f := newFunder(t, store, 1)

	result, err := f.Fund(t.Context(), throughputArgs(900, 1000))

	require.NoError(t, err)
	require.Equal(t, []uint64{1000}, allocatedSats(result), "one denomination coin covers the target")
	require.Equal(t, []claimKind{kindExact}, store.kinds(), "a single ClaimExact funds the call; no bounded walk")
}

// TestFund_Throughput_MultiClaimClosedForm: a larger action needs n coins,
// claimed in a single closed-form ClaimExact, with no bounded top-up.
func TestFund_Throughput_MultiClaimClosedForm(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "fuel", utxostore.TierMined, 1000, 1000, 1000, 1000, 1000)
	store := newRecordingStore(inner)
	f := newFunder(t, store, 1)

	// need = 2500 + baseFee(1) = 2501; net per coin = 1000 - 1 = 999; n = ceil(2501/999) = 3.
	result, err := f.Fund(t.Context(), throughputArgs(2500, 1000))

	require.NoError(t, err)
	require.Equal(t, []uint64{1000, 1000, 1000}, allocatedSats(result))
	require.Equal(t, []claimKind{kindExact}, store.kinds(),
		"the closed-form count claims every needed coin in one round trip")
}

// TestFund_Throughput_UnderflowTopUp: when the denominated pool is short, the
// fast path claims what it can and the bounded tiered walk tops up the rest.
func TestFund_Throughput_UnderflowTopUp(t *testing.T) {
	inner := newMemStore()
	// Only two 1000-denomination coins, plus one off-denomination 800 coin.
	mintCoins(t, inner, "fuel", utxostore.TierMined, 1000, 1000, 800)
	store := newRecordingStore(inner)
	f := newFunder(t, store, 1)

	// n = 3, but only two exact coins exist -> underflow, top up with the 800.
	result, err := f.Fund(t.Context(), throughputArgs(2500, 1000))

	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1000, 1000, 800}, allocatedSats(result))

	kinds := store.kinds()
	require.Equal(t, kindExact, kinds[0], "the fast path runs first")
	require.Contains(t, kinds, kindSufficient, "the bounded walk tops up the underflow")
}

// TestFund_Throughput_ContentionExhausted proves the throughput path is subject
// to the same bounded contention retry as the bounded path.
func TestFund_Throughput_ContentionExhausted(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "fuel", utxostore.TierMined, 1000, 1000)
	store := newContentionStore(inner, 1_000) // always contend
	f := funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1}, funder.WithBackoffForTest(noBackoff))

	_, err := f.Fund(t.Context(), throughputArgs(900, 1000))
	require.ErrorIs(t, err, funder.ErrUTXOContention)
}

// TestFund_Throughput_ExactClaimsUnprovenTier proves the fast path stays alive
// when the pool lingers at TierUnproven (the real-network case: broadcast-
// accepted fuel that is never mined). It must claim the unproven coin via
// ClaimExact by walking the spend tiers — not fall back to the bounded walk.
func TestFund_Throughput_ExactClaimsUnprovenTier(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "fuel", utxostore.TierUnproven, 1000, 1000, 1000)
	store := newRecordingStore(inner)
	f := newFunder(t, store, 1)

	result, err := f.Fund(t.Context(), throughputArgs(900, 1000))

	require.NoError(t, err)
	require.Equal(t, []uint64{1000}, allocatedSats(result), "an unproven denomination coin funds the target")
	// The fast path probes mined (empty) then unproven (hit); both are ClaimExact
	// and the bounded walk is never reached.
	require.Equal(t, []claimKind{kindExact, kindExact}, store.kinds(),
		"unproven fuel is claimed by the fast path across tiers, not the bounded walk")
}

// TestSpendTiers pins the policy-to-tier mapping.
func TestSpendTiers(t *testing.T) {
	require.Equal(t, []utxostore.Tier{utxostore.TierMined}, funder.SpendTiers(defs.SpendPolicyMinedOnly))
	require.Equal(t, []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven}, funder.SpendTiers(defs.SpendPolicyPreferMined))
	require.Equal(t, []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven, utxostore.TierSending}, funder.SpendTiers(defs.SpendPolicyAny))
	require.Equal(t, funder.SpendTiers(defs.SpendPolicyPreferMined), funder.SpendTiers("unknown"), "unknown policy defaults to prefer_mined")
}

// TestShapeChange pins the fan-out shaping arithmetic.
func TestShapeChange(t *testing.T) {
	cases := []struct {
		name         string
		amount       satoshi.Value
		denomination uint64
		maxOutputs   uint64
		expect       uint64
	}{
		{"five whole denominations", 5000, 1000, 100, 5},
		{"capped at maxOutputs", 5000, 1000, 3, 3},
		{"below one denomination", 999, 1000, 100, 0},
		{"exact boundary", 2000, 1000, 100, 2},
		{"zero amount", 0, 1000, 100, 0},
		{"negative amount", -5, 1000, 100, 0},
		{"zero denomination", 5000, 0, 100, 0},
		{"zero maxOutputs", 5000, 1000, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, funder.ShapeChange(tc.amount, tc.denomination, tc.maxOutputs))
		})
	}
}
