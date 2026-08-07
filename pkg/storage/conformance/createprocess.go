package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// createProcessRoundtrip: FindOrInsertUser -> InternalizeAction (seed funds
// via a mined payment) -> CreateAction -> ProcessAction (accept via the fake
// oracle) -> assert tx status, inputs spent, change promoted, and GetBalance
// reflects it — all observed through the wdk.WalletStorageProvider surface.
func (s *suite) createProcessRoundtrip(t *testing.T) {
	ctx := context.Background()
	p := s.freshProvider(t)
	sender := NewIdentityKey(t)
	auth := s.newAuth(t, p, NewIdentityKey(t))

	fundTxID := internalizeMinedPayment(t, p, auth, sender, 0x01, 100_000)

	bal0, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, uint64(100_000), bal0, "seeded funds are spendable")
	assertSpendable(t, p, auth, fundTxID, 0, true, "funding coin starts spendable")

	res, err := p.CreateAction(ctx, auth, PaymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, res.Inputs, "at least the allocated funding input")
	require.NotEmpty(t, res.Outputs, "payment + change outputs")

	// Reserved by CreateAction: no longer spendable, but not yet gone
	// (CreateAction mints no inventory and spends nothing — see the storage
	// package doc).
	assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin reserved by CreateAction")

	signed := BuildSignedTx(t, res)
	txid := signed.TxID().String()
	out, err := p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strPtr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusSuccess, out.NotDelayedResults[0].Status)

	// Tx status: broadcasted, under the reference this CreateAction returned.
	status := txStatusByReference(t, p, auth, res.Reference)
	assert.Equal(t, wdk.TxUpdateStatusBroadcasted, status.Status)
	assert.Equal(t, res.Reference, status.Reference)
	assert.Equal(t, txid, status.TxID)

	// Input spent: the funding coin remains unspendable after acceptance.
	assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin spent on acceptance")

	// Change promoted: the minted change output is now live and spendable.
	changeTxID, changeVout := ChangeOutpoint(t, res, txid)
	assertSpendable(t, p, auth, changeTxID, changeVout, true, "change promoted to spendable on acceptance")

	// GetBalance reflects it: strictly less than the seed (the spend + fee),
	// but still positive (the change).
	bal1, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	assert.Positive(t, bal1, "change remains spendable")
	assert.Less(t, bal1, bal0, "spending reduced the spendable balance")
}
