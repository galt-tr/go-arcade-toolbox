package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// internalize: a wallet-payment output and a basket-insertion output in the
// same transaction land in their correct baskets (only the wallet payment
// credits the spendable wallet balance); a bad BUMP — a headers source that
// does not recognize the given merkle root — is rejected and nothing is
// persisted.
func (s *suite) internalize(t *testing.T) {
	t.Run("WalletPaymentAndBasketInsertion", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		atomic, txid := BuildMinedAtomicBEEF(t, 0x30, 810_000, 15_000, 7_000)
		res, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
			Tx: primitives.ExplicitByteArray(atomic),
			Outputs: []*wdk.InternalizeOutput{
				WalletPaymentOutput(0, sender),
				BasketInsertionOutput(1, "collectibles"),
			},
		})
		require.NoError(t, err)
		assert.True(t, res.Accepted)
		assert.False(t, res.IsMerge)
		assert.Equal(t, txid, res.TxID)
		assert.Equal(t, int64(15_000), res.Satoshis, "only the wallet payment credits the wallet balance")

		// Wallet payment landed in the change basket and is spendable.
		changeList, err := p.ListOutputs(ctx, auth, wdk.ListOutputsArgs{
			Basket: primitives.StringUnder300(wdk.BasketNameForChange), Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, changeList.Outputs, 1, "only the wallet-payment output lands in the change basket")
		assert.Equal(t, primitives.NewOutpointString(txid, 0), changeList.Outputs[0].Outpoint)
		assert.True(t, changeList.Outputs[0].Spendable)
		assert.Equal(t, primitives.SatoshiValue(15_000), changeList.Outputs[0].Satoshis)

		// Basket-insertion landed in its own basket, not the change basket.
		custom, err := p.ListOutputs(ctx, auth, wdk.ListOutputsArgs{
			Basket: "collectibles", Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, custom.Outputs, 1)
		assert.Equal(t, primitives.NewOutpointString(txid, 1), custom.Outputs[0].Outpoint)
		assert.Equal(t, primitives.SatoshiValue(7_000), custom.Outputs[0].Satoshis)

		bal, err := p.GetBalance(ctx, auth, "")
		require.NoError(t, err)
		assert.Equal(t, uint64(15_000), bal)
	})

	t.Run("BadBumpRejected", func(t *testing.T) {
		if s.cfg.rejectingProvider == nil {
			t.Skip("suite configured without WithRejectingHeadersProvider: this backend did not supply an alternate rejecting-headers constructor")
		}
		ctx := context.Background()
		p := s.freshProviderFrom(t, s.cfg.rejectingProvider)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		atomic, _ := BuildMinedAtomicBEEF(t, 0x31, 820_000, 9_000)
		_, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
			Tx: primitives.ExplicitByteArray(atomic),
			Outputs: []*wdk.InternalizeOutput{
				WalletPaymentOutput(0, NewIdentityKey(t)),
			},
		})
		require.Error(t, err, "a bad BUMP must be rejected")

		// Nothing persisted.
		bal, err := p.GetBalance(ctx, auth, "")
		require.NoError(t, err)
		assert.Zero(t, bal)
		txs, err := p.ListTransactions(ctx, auth, wdk.ListTransactionsArgs{Limit: 10})
		require.NoError(t, err)
		assert.EqualValues(t, 0, txs.TotalTransactions)
		assert.Empty(t, txs.Transactions)
	})
}
