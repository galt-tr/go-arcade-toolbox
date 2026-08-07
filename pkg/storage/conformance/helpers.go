package conformance

import (
	"fmt"
	"sort"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// destinationLockingScriptHex is a fixed throwaway P2PKH locking script used
// as the payment destination in [PaymentArgs]. Its value is irrelevant to
// every assertion the suite makes; it only needs to be a well-formed script.
const destinationLockingScriptHex = "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac"

// NewP2PKHLockingScript returns a locking script for a fresh, random P2PKH
// address — a throwaway destination for building test outputs/change.
func NewP2PKHLockingScript(t testing.TB) *script.Script {
	t.Helper()
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), false)
	require.NoError(t, err)
	ls, err := p2pkh.Lock(addr)
	require.NoError(t, err)
	return ls
}

// PaymentArgs returns a simple single-output [wdk.ValidCreateActionArgs]
// paying sats to a fixed throwaway P2PKH destination.
func PaymentArgs(sats uint64) wdk.ValidCreateActionArgs {
	return wdk.ValidCreateActionArgs{
		Description: "conformance payment",
		Outputs: []wdk.ValidCreateActionOutput{{
			LockingScript:     primitives.HexString(destinationLockingScriptHex),
			Satoshis:          primitives.SatoshiValue(sats), //nolint:gosec // test satoshi values are small
			OutputDescription: "payment",
		}},
		IsNewTx: true,
	}
}

// BuildSignedTx assembles a [transaction.Transaction] from a CreateAction
// result: inputs in Vin order (dummy unlocking scripts — the suite pairs this
// with [AlwaysValidScripts] so no real signing is required) and outputs in
// vout order (the provided scripts, or a fresh throwaway P2PKH for
// storage-derived change).
func BuildSignedTx(t testing.TB, res *wdk.StorageCreateActionResult) *transaction.Transaction {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.Version = res.Version
	tx.LockTime = res.LockTime

	ins := append([]*wdk.StorageCreateTransactionSdkInput(nil), res.Inputs...)
	sort.Slice(ins, func(i, j int) bool { return ins[i].Vin < ins[j].Vin })
	for _, in := range ins {
		h, err := chainhash.NewHashFromHex(in.SourceTxID)
		require.NoError(t, err)
		tx.AddInput(&transaction.TransactionInput{
			SourceTXID:       h,
			SourceTxOutIndex: in.SourceVout,
			UnlockingScript:  script.NewFromBytes([]byte{0x00}),
			SequenceNumber:   transaction.DefaultSequenceNumber,
		})
	}

	outs := append([]*wdk.StorageCreateTransactionSdkOutput(nil), res.Outputs...)
	sort.Slice(outs, func(i, j int) bool { return outs[i].Vout < outs[j].Vout })
	for _, out := range outs {
		var ls *script.Script
		if out.LockingScript != "" {
			var err error
			ls, err = script.NewFromHex(out.LockingScript.String())
			require.NoError(t, err)
		} else {
			ls = NewP2PKHLockingScript(t) // storage-derived change
		}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: uint64(out.Satoshis), LockingScript: ls})
	}
	return tx
}

// ChangeOutpoint returns the outpoint of the (first) storage change output in
// a CreateAction result, once its transaction id txid is known (post-sign).
func ChangeOutpoint(t testing.TB, res *wdk.StorageCreateActionResult, txid string) (string, uint32) {
	t.Helper()
	for _, o := range res.Outputs {
		if o.ProvidedBy == wdk.ProvidedByStorage && o.Purpose == wdk.ChangePurpose {
			return txid, o.Vout
		}
	}
	t.Fatal("no change output in result")
	return "", 0
}

// BuildMinedAtomicBEEF builds a single-transaction, single-leaf-proven atomic
// BEEF: a transaction with one output per entry in outputSats, a merkle path
// whose root equals its txid (a single-tx block), at the given block height.
// Internalizing it (with [FakeHeaders]'s default accept-everything behavior)
// credits every requested output as "mined".
func BuildMinedAtomicBEEF(t testing.TB, seed byte, height uint32, outputSats ...uint64) (atomicBEEF []byte, txid string) {
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
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: NewP2PKHLockingScript(t)})
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

// WalletPaymentOutput builds an [wdk.InternalizeOutput] crediting outputIndex
// as a wallet payment (into the change basket) from senderIdentityKey.
func WalletPaymentOutput(outputIndex uint32, senderIdentityKey string) *wdk.InternalizeOutput {
	return &wdk.InternalizeOutput{
		OutputIndex: outputIndex,
		Protocol:    wdk.WalletPaymentProtocol,
		PaymentRemittance: &wdk.WalletPayment{
			DerivationPrefix:  "cHJlZml4",
			DerivationSuffix:  "c3VmZml4",
			SenderIdentityKey: primitives.PubKeyHex(senderIdentityKey),
		},
	}
}

// BasketInsertionOutput builds an [wdk.InternalizeOutput] crediting
// outputIndex into basket via the basket-insertion protocol.
func BasketInsertionOutput(outputIndex uint32, basket string) *wdk.InternalizeOutput {
	return &wdk.InternalizeOutput{
		OutputIndex: outputIndex,
		Protocol:    wdk.BasketInsertionProtocol,
		InsertionRemittance: &wdk.BasketInsertion{
			Basket: primitives.StringUnder300(basket),
		},
	}
}

// NewIdentityKey returns a fresh, valid compressed-secp256k1-pubkey-hex
// identity key, suitable both as an [wdk.AuthID] identity key and as a
// [wdk.WalletPayment] SenderIdentityKey.
func NewIdentityKey(t testing.TB) string {
	t.Helper()
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	return fmt.Sprintf("%x", priv.PubKey().Compressed())
}

func strPtr(s string) *string { return &s }

func txidPtr(s string) *primitives.TXIDHexString {
	v := primitives.TXIDHexString(s)
	return &v
}
