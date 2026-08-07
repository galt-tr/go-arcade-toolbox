package storage

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// buildMinedAtomicBEEF builds a single-transaction, single-leaf-proven atomic
// BEEF: a transaction with the given output satoshis, a merkle path whose root
// equals its txid (single-tx block), at the given block height.
func buildMinedAtomicBEEF(t *testing.T, seed byte, height uint32, outputSats ...uint64) ([]byte, string) {
	t.Helper()

	tx := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = seed
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	for _, sats := range outputSats {
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: testP2PKH(t)})
	}

	txidHash := tx.TxID()
	trueVal := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	require.NoError(t, tx.AddMerkleProof(mp))

	beef := transaction.NewBeefV2()
	_, err := beef.MergeTransaction(tx)
	require.NoError(t, err)
	atomic, err := beef.AtomicBytes(txidHash)
	require.NoError(t, err)
	return atomic, txidHash.String()
}

// internalizeMinedPayment internalizes a single mined P2PKH wallet-payment
// output and returns its txid. Used by ListOutputs tests.
func (h *harness) internalizeMinedPayment(t *testing.T, seed byte, sats uint64) string {
	t.Helper()
	ctx := context.Background()
	atomic, txid := buildMinedAtomicBEEF(t, seed, 800000, sats)
	_, err := h.p.InternalizeAction(ctx, h.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    wdk.WalletPaymentProtocol,
			PaymentRemittance: &wdk.WalletPayment{
				DerivationPrefix:  "cHJlZml4",
				DerivationSuffix:  "c3VmZml4",
				SenderIdentityKey: testIdentityKey,
			},
		}},
	})
	require.NoError(t, err)
	return txid
}

func TestInternalizeAction_WalletPaymentAndBasketInsertion(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	atomic, txid := buildMinedAtomicBEEF(t, 0x51, 810000, 15_000, 7_000)
	res, err := h.p.InternalizeAction(ctx, h.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{
			{
				OutputIndex: 0,
				Protocol:    wdk.WalletPaymentProtocol,
				PaymentRemittance: &wdk.WalletPayment{
					DerivationPrefix:  "cHJlZml4",
					DerivationSuffix:  "c3VmZml4",
					SenderIdentityKey: testIdentityKey,
				},
			},
			{
				OutputIndex: 1,
				Protocol:    wdk.BasketInsertionProtocol,
				InsertionRemittance: &wdk.BasketInsertion{
					Basket: "collectibles",
					Tags:   []primitives.StringUnder300{"nft"},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, res.Accepted)
	assert.False(t, res.IsMerge)
	assert.Equal(t, txid, res.TxID)
	assert.Equal(t, int64(15_000), res.Satoshis, "only wallet-payment credits the wallet balance")

	hash, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)

	// Wallet payment minted into the change basket at TierMined.
	pay, err := h.utxo.Get(ctx, utxostore.Outpoint{TxID: *hash, Vout: 0})
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierMined, pay.Tier)
	assert.Equal(t, wdk.BasketNameForChange, pay.Basket)

	// Basket insertion minted into the target basket.
	ins, err := h.utxo.Get(ctx, utxostore.Outpoint{TxID: *hash, Vout: 1})
	require.NoError(t, err)
	assert.Equal(t, "collectibles", ins.Basket)
}

func TestInternalizeAction_BadBumpRejected(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// The header source rejects the merkle root: internalize must fail.
	h.hdrs.verify = func(context.Context, *chainhash.Hash, uint32) (bool, error) { return false, nil }

	atomic, _ := buildMinedAtomicBEEF(t, 0x61, 820000, 9_000)
	_, err := h.p.InternalizeAction(ctx, h.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    wdk.WalletPaymentProtocol,
			PaymentRemittance: &wdk.WalletPayment{
				DerivationPrefix:  "cHJlZml4",
				DerivationSuffix:  "c3VmZml4",
				SenderIdentityKey: testIdentityKey,
			},
		}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "beef verification failed")
}

func TestAbortAction_Unsigned(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	coin := h.mintFunding(t, 0x71, 60_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(20_000))
	require.NoError(t, err)

	// Reserved before abort.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Equal(t, res.Reference, u.ReservedBy)

	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
	require.NoError(t, err)
	assert.True(t, abr.Aborted)

	// Reservation released.
	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, "", u.ReservedBy, "funding coin released on abort")

	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.TxStatusAborted, txRow.Status)
}

func TestAbortAction_NoSendRestores(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	coin := h.mintFunding(t, 0x81, 80_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(30_000))
	require.NoError(t, err)

	signed := buildSignedTx(t, res)
	txid := signed.TxID().String()
	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsNoSend:  true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	// Change minted (TierSending), coin still reserved (noSend does not spend).
	changeOp := changeOutpoint(t, res, txid)
	_, err = h.utxo.Get(ctx, changeOp)
	require.NoError(t, err)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Equal(t, res.Reference, u.ReservedBy)

	// Abort restores everything.
	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
	require.NoError(t, err)
	assert.True(t, abr.Aborted)

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, "", u.ReservedBy, "input released")
	_, err = h.utxo.Get(ctx, changeOp)
	assert.ErrorIs(t, err, &utxostore.NotFoundError{}, "minted change removed")
}

func strptr(s string) *string { return &s }

func txidPtr(s string) *primitives.TXIDHexString {
	v := primitives.TXIDHexString(s)
	return &v
}

// changeOutpoint returns the outpoint of the (single) storage change output.
func changeOutpoint(t *testing.T, res *wdk.StorageCreateActionResult, txid string) utxostore.Outpoint {
	t.Helper()
	hash, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)
	for _, o := range res.Outputs {
		if o.ProvidedBy == wdk.ProvidedByStorage && o.Purpose == wdk.ChangePurpose {
			return utxostore.Outpoint{TxID: *hash, Vout: o.Vout}
		}
	}
	t.Fatal("no change output in result")
	return utxostore.Outpoint{}
}
