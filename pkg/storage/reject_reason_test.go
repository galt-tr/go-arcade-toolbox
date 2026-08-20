package storage

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// The rejection reason is perishable: arcade drops a rejected transaction's
// record on its own schedule, and we have seen a REJECTED event whose GET /tx
// answered 404 immediately afterwards. These tests pin the rule that every path
// which learns a reason writes it down before it can be lost.

func TestProcessAction_Reject_PersistsReason(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	const reason = "PROCESSING (4): failed to validate transaction: insufficient fee"
	h.oracle.broadcast = func(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{
			TxID: txid, Status: arcade.StatusRejected, Rejected: true, ExtraInfo: reason,
		}, nil
	}
	res, signed, _ := h.createAndSign(t, 0x31, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, kt.RejectReason, "the 4xx body is the only statement of cause that will ever exist")
	assert.Equal(t, reason, *kt.RejectReason)
}

func TestApplyStatusUpdate_Rejected_PersistsReason(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x32, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	const reason = "TX_INVALID (4): script evaluated to false"
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{
		TxID: txid, Status: arcade.StatusRejected, ExtraInfo: reason,
	}))

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, kt.RejectReason)
	assert.Equal(t, reason, *kt.RejectReason)
}

// A REJECTED record with an empty extraInfo is exactly the incident that
// motivated this column. It must not erase a reason an earlier event supplied —
// otherwise the second, emptier event destroys the only diagnosis we have.
func TestApplyStatusUpdate_Rejected_EmptyReasonKeepsEarlierOne(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x33, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	const reason = "UTXO_SPENT (70): utxo already spent"
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{
		TxID: txid, Status: arcade.StatusRejected, ExtraInfo: reason,
	}))
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{
		TxID: txid, Status: arcade.StatusRejected, ExtraInfo: "",
	}))

	kt, _, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.NotNil(t, kt.RejectReason, "an empty later event must not blank the reason")
	assert.Equal(t, reason, *kt.RejectReason)
}

// A rejection arcade cannot explain leaves the column NULL rather than an empty
// string, so "arcade told us nothing" stays distinguishable from "nobody looked".
func TestApplyStatusUpdate_Rejected_NoReasonStaysNull(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x34, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{
		TxID: txid, Status: arcade.StatusRejected,
	}))

	kt, _, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	assert.Nil(t, kt.RejectReason)
}

// The reason has to survive the drill-down that an operator actually reads,
// because by then arcade may no longer hold the record at all.
func TestListKnownTxByArcadeStatus_CarriesRejectReason(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x35, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	const reason = "PROCESSING (4): failed to validate transaction"
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{
		TxID: txid, Status: arcade.StatusRejected, ExtraInfo: reason,
	}))

	rows, err := h.p.ListKnownTxByArcadeStatus(ctx, string(arcade.StatusRejected), 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, txid, rows[0].TxID)
	assert.Equal(t, reason, rows[0].RejectReason)
}

// processDelayed persists a signed transaction without broadcasting it: the
// monitor sends it later. Two kinds of test need that.
//
// One wants a known tx that exists, so it can drive a status event at it.
//
// The other wants the window the pre-broadcast pin exists for — raw tx stored
// and broadcastable, inputs still reserved and unspent, a janitor able to reach
// them. The immediate path cannot show that state at all: it spends the inputs
// inside the same call, so by the time ProcessAction returns the pin has
// already been lifted and there is nothing left to observe.
func (h *harness) processDelayed(t *testing.T, res *wdk.StorageCreateActionResult, signed *transaction.Transaction) (*wdk.ProcessActionResult, error) {
	t.Helper()
	return h.p.ProcessAction(context.Background(), h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsDelayed: true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(signed.TxID().String()),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
}
