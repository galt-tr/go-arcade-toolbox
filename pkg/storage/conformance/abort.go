package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// abortRestores: CreateAction then AbortAction releases the funding
// reservation, removes any minted change, restores the funding coin as
// claimable again, and marks the transaction aborted — both for a
// never-processed (unsigned) action and for a noSend-processed one (which
// mints change before the abort).
func (s *suite) abortRestores(t *testing.T) {
	t.Run("Unsigned", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		fundTxID := internalizeMinedPayment(t, p, auth, sender, 0x40, 60_000)

		res, err := p.CreateAction(ctx, auth, PaymentArgs(20_000))
		require.NoError(t, err)
		assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin reserved before abort")

		abr, err := p.AbortAction(ctx, auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
		require.NoError(t, err)
		assert.True(t, abr.Aborted)

		assertSpendable(t, p, auth, fundTxID, 0, true, "funding coin released on abort")
		status := txStatusByReference(t, p, auth, res.Reference)
		assert.Equal(t, wdk.TxUpdateStatusAborted, status.Status)
	})

	t.Run("NoSendRestores", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		fundTxID := internalizeMinedPayment(t, p, auth, sender, 0x41, 80_000)

		res, err := p.CreateAction(ctx, auth, PaymentArgs(30_000))
		require.NoError(t, err)

		signed := BuildSignedTx(t, res)
		txid := signed.TxID().String()
		_, err = p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
			IsNewTx:   true,
			IsNoSend:  true,
			Reference: strPtr(res.Reference),
			TxID:      txidPtr(txid),
			RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
		})
		require.NoError(t, err)

		// Change minted; funding coin still reserved (noSend never spends).
		changeTxID, changeVout := ChangeOutpoint(t, res, txid)
		assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin reserved pre-abort")

		abr, err := p.AbortAction(ctx, auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
		require.NoError(t, err)
		assert.True(t, abr.Aborted)

		assertSpendable(t, p, auth, fundTxID, 0, true, "funding coin restored on abort")
		assertSpendable(t, p, auth, changeTxID, changeVout, false, "minted change is no longer a live coin after abort")
	})
}
