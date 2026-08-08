package storage

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// createAndSign runs a happy-path CreateAction and returns the result, the
// signed transaction, and the funding coin's outpoint.
func (h *harness) createAndSign(t *testing.T, fundSeed byte, fund, pay uint64) (*wdk.StorageCreateActionResult, *transaction.Transaction, utxostore.Outpoint) {
	t.Helper()
	ctx := context.Background()
	coin := h.mintFunding(t, fundSeed, fund)
	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(pay))
	require.NoError(t, err)
	signed := buildSignedTx(t, res)
	return res, signed, coin
}

func TestProcessAction_Accept(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x11, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusSuccess, out.NotDelayedResults[0].Status)
	assert.Equal(t, 1, h.oracle.calls)

	// tx unproven.
	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.TxStatusUnproven, txRow.Status)

	// funding input spent.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "input spent on acceptance")

	// change minted but NOT yet claimable: a 202 is acceptance-for-processing,
	// not validation, so change stays at TierSending until arcade SEENs the tx.
	chOp := changeOutpoint(t, res, txid)
	ch, err := h.utxo.Get(ctx, chOp)
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierSending, ch.Tier, "change is not claimable on the 202, only on SEEN")

	// On the SEEN status the change is promoted to claimable (TierUnproven).
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))
	ch, err = h.utxo.Get(ctx, chOp)
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierUnproven, ch.Tier, "SEEN promotes change to claimable")
}

func TestProcessAction_Reject(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.oracle.broadcast = func(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusRejected, Rejected: true, ExtraInfo: "bad tx"}, nil
	}
	res, signed, coin := h.createAndSign(t, 0x12, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusFailed, out.SendWithResults[0].Status)
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusInvalidTx, out.NotDelayedResults[0].Status)

	// tx failed.
	txRow, _, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	assert.Equal(t, wdk.TxStatusFailed, txRow.Status)

	// known tx marked suspectFailed.
	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, metastore.KnownTxStatusSuspectFailed, kt.Status)

	// The phantom change minted in this ProcessAction is removed: a final 4xx
	// rejection means it never reached the chain.
	_, err = h.utxo.Get(ctx, changeOutpoint(t, res, txid))
	assert.ErrorIs(t, err, &utxostore.NotFoundError{}, "rejected change removed")

	// Balance is not inflated (change gone; input reserved => not claimable) and
	// therefore nothing is selectable under any spend policy.
	bal, err := h.p.GetBalance(ctx, h.auth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bal, "rejected change does not inflate the balance")

	// inputs NOT released (reconciler's job in M4): coin still reserved, unspent.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy, "input NOT released on rejection")
	assert.Nil(t, u.SpentBy)
}

func TestProcessAction_Delayed(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x13, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsDelayed: true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	assert.Equal(t, 0, h.oracle.calls, "delayed does not broadcast now")
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusSending, out.SendWithResults[0].Status)

	// tx pre-broadcast (abortable), known tx unsent.
	txRow, _, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	assert.Equal(t, wdk.TxStatusUnprocessed, txRow.Status)
	kt, _, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	assert.Equal(t, wdk.ProvenTxStatusUnsent, kt.Status)

	// change minted at TierSending, input still reserved.
	ch, err := h.utxo.Get(ctx, changeOutpoint(t, res, txid))
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierSending, ch.Tier)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy)
}

func TestProcessAction_NoSend(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x14, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsNoSend:  true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	assert.Equal(t, 0, h.oracle.calls, "noSend does not broadcast")
	assert.Empty(t, out.SendWithResults)

	txRow, _, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	assert.Equal(t, wdk.TxStatusNoSend, txRow.Status)

	// change minted, input reserved.
	ch, err := h.utxo.Get(ctx, changeOutpoint(t, res, txid))
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierSending, ch.Tier)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy)
}

func TestProcessAction_BackpressureThenError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.oracle.broadcast = func(context.Context, string, []byte) (*arcade.BroadcastResult, error) {
		return nil, &arcade.BackpressureError{RetryAfter: 0}
	}
	res, signed, _ := h.createAndSign(t, 0x15, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	assert.Equal(t, 2, h.oracle.calls, "one retry after backpressure")
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusServiceError, out.NotDelayedResults[0].Status)
}
