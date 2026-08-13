package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// listAndBalanceConsistency: ListOutputs' spendable set matches GetBalance
// exactly, and reserved outputs are excluded from both while remaining
// listed (still-descriptive) rows.
func (s *suite) listAndBalanceConsistency(t *testing.T) {
	ctx := context.Background()
	p := s.freshProvider(t)
	sender := NewIdentityKey(t)
	auth := s.newAuth(t, p, NewIdentityKey(t))

	atomic, _ := BuildMinedAtomicBEEF(t, 0x60, 900_000, 20_000, 30_000, 10_000)
	_, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{
			WalletPaymentOutput(0, sender),
			WalletPaymentOutput(1, sender),
			WalletPaymentOutput(2, sender),
		},
	})
	require.NoError(t, err)

	bal0, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, uint64(60_000), bal0)

	// Reserve one coin (smallest-sufficient for 15_000 leaves the other two
	// spendable, regardless of which particular coin gets picked).
	_, err = p.CreateAction(ctx, auth, PaymentArgs(15_000))
	require.NoError(t, err)

	// NOTE: outs.Outputs is NOT limited to the 3 seeded coins. CreateAction
	// plans its (possibly several — up to the change-basket's
	// MaxChangeOutputsPerTx) storage-derived change output rows immediately,
	// long before that change is actually minted at ProcessAction; those
	// planned rows list here too (unspendable: no resolved txid yet). The
	// invariant under test does not depend on the row count, only on the
	// spendable subset summing to GetBalance.
	outs, err := p.ListOutputs(ctx, auth, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange), Limit: 50,
	})
	require.NoError(t, err)
	require.NotEmpty(t, outs.Outputs)

	var spendableSum uint64
	var sawReserved bool
	for _, o := range outs.Outputs {
		if o.Spendable {
			spendableSum += uint64(o.Satoshis)
		} else {
			sawReserved = true
		}
	}
	assert.True(t, sawReserved, "the coin allocated to the in-flight CreateAction must show unspendable")

	bal1, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	assert.Equal(t, spendableSum, bal1, "GetBalance must equal the sum of spendable ListOutputs entries")
	assert.Less(t, bal1, bal0, "the reserved coin must no longer count toward the balance")

	// FindOutputsAuth(Spendable=true) must return exactly the same set.
	spendableTrue := true
	findRes, err := p.FindOutputsAuth(ctx, auth, wdk.FindOutputsArgs{Spendable: &spendableTrue})
	require.NoError(t, err)
	var findSum uint64
	for _, o := range findRes {
		findSum += uint64(o.Satoshis) //nolint:gosec // satoshi values non-negative
	}
	assert.Equal(t, spendableSum, findSum)
}
