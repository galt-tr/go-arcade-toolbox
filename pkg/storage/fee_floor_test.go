package storage

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// The committed fee is computed from a size ESTIMATE made before the unlocking
// scripts exist. When the finished transaction is bigger than the plan, the fee
// silently falls under the network's floor and the only signal is a remote 4xx
// with no local record of why. These tests pin the local refusal instead.

func TestProcessAction_FeeBelowFloor_RefusedLocally(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, WithMinBroadcastFeeRate(100))
	res, signed, coin := h.createAndSign(t, 0x41, 100_000, 40_000)

	// Ten kilobytes of unlocking script the funder never priced: at 100 sat/kB
	// the transaction now owes ~1000 satoshis and pays the few the estimate
	// implied.
	signed.Inputs[0].UnlockingScript = script.NewFromBytes(make([]byte, 10_000))
	txid := signed.TxID().String()

	_, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.ErrorIs(t, err, ErrFeeBelowFloor)
	assert.Contains(t, err.Error(), "short of the 100 sat/kB floor",
		"the error must name the shortfall, not just the fact")

	// Nothing was offered to the network, and nothing was left behind: the
	// refusal happens before the transaction is persisted or change is minted.
	assert.Equal(t, 0, h.oracle.calls, "an underpriced transaction is never broadcast")
	_, found, ferr := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, ferr)
	assert.False(t, found, "no known-tx row for a transaction that was refused")

	// The funding coin is untouched and still reserved to this reference, so the
	// caller can abort and rebuild without a stranded or double-spendable coin.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy)
	assert.Nil(t, u.SpentBy)
}

func TestProcessAction_FeeAboveFloor_Accepted(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, WithMinBroadcastFeeRate(100))
	res, signed, _ := h.createAndSign(t, 0x42, 100_000, 40_000)
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
}

// The floor is the receiving deployment's policy, not a protocol constant (an
// arcade with accept_zero_fee has none), so it stays off until asked for.
func TestProcessAction_FeeFloorOffByDefault(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x43, 100_000, 40_000)
	signed.Inputs[0].UnlockingScript = script.NewFromBytes(make([]byte, 10_000))

	_, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(signed.TxID().String()),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err, "no floor configured means no local fee opinion")
	assert.Equal(t, 1, h.oracle.calls)
}
