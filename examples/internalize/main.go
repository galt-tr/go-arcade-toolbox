// Command internalize shows how an arcade-only wallet is funded: by RECEIVING
// an external payment through InternalizeAction. A fresh wallet has zero
// spendable balance and there is no restore-from-seed, so InternalizeAction is
// the only way coins enter a wallet that did not create them itself.
//
// Run it:
//
//	go run ./examples/internalize
//
// The program plays both roles: first the SENDER builds a mined BRC-29 payment
// to the wallet's identity key (and we register its proof in the mock
// ChainTracks); then the RECIPIENT wallet internalizes it. In production the
// sender is someone else and the BEEF + derivation material arrive out of band
// (e.g. over a BRC-29 payment protocol); the recipient half — the
// InternalizeAction call — is identical.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/go-arcade-toolbox/examples/internal/demoenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/brc29"
)

// The external sender's key, and the per-payment BRC-29 derivation material the
// sender and recipient agree on. The KeyID carries the base64 form; the
// InternalizeAction remittance carries the raw bytes — they describe the same
// prefix/suffix.
const senderKeyHex = "00000000000000000000000000000000000000000000000000000000000000aa"

var (
	derivationPrefix = []byte("invoice-2026-08-07")
	derivationSuffix = []byte("line-item-1")
)

func main() {
	ctx := context.Background()

	env, err := demoenv.Setup(ctx)
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
	defer env.Close()

	fmt.Printf("recipient wallet key: %s\n", env.RecipientHex)

	// --- SENDER SIDE -------------------------------------------------------
	// Build a mined transaction whose sole output pays the wallet via BRC-29,
	// and register its merkle root so the recipient's headers client can verify
	// the proof. (In production the sender broadcasts this; here we hand the
	// recipient a mined BEEF directly.)
	const paymentSats = 25_000
	const blockHeight = 810_000
	beefBytes, root, txid, err := buildMinedPayment(env.RecipientHex, paymentSats, blockHeight)
	if err != nil {
		log.Fatalf("build payment: %v", err)
	}
	env.ChainTracks.RegisterHeader(blockHeight, root)
	fmt.Printf("sender built mined payment tx %s for %d sat\n", txid, paymentSats)

	// --- RECIPIENT SIDE ----------------------------------------------------
	// Internalize the payment. The wallet re-derives the BRC-29 locking script
	// from the remittance, verifies the merkle proof against ChainTracks, and
	// records the output as a spendable coin.
	senderPriv, err := ec.PrivateKeyFromHex(senderKeyHex)
	if err != nil {
		log.Fatalf("sender key: %v", err)
	}
	res, err := env.Wallet.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx: beefBytes,
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: &sdk.Payment{
				DerivationPrefix:  derivationPrefix,
				DerivationSuffix:  derivationSuffix,
				SenderIdentityKey: senderPriv.PubKey(),
			},
		}},
		Description: "incoming payment for invoice",
		Labels:      []string{"received"},
	}, demoenv.Originator)
	if err != nil {
		log.Fatalf("internalize: %v", err)
	}
	fmt.Printf("internalize accepted: %t\n", res.Accepted)

	balance, err := env.Wallet.Balance(ctx)
	if err != nil {
		log.Fatalf("balance: %v", err)
	}
	fmt.Printf("wallet balance after receiving: %d sat\n", balance)

	// The received coin is now spendable and can fund a CreateAction (see the
	// quickstart example).
	outputs, err := env.Wallet.ListOutputs(ctx, sdk.ListOutputsArgs{}, demoenv.Originator)
	if err != nil {
		log.Fatalf("list outputs: %v", err)
	}
	fmt.Printf("wallet has %d output(s):\n", outputs.TotalOutputs)
	for _, o := range outputs.Outputs {
		fmt.Printf("  %s  %d sat  spendable=%t\n", o.Outpoint.String(), o.Satoshis, o.Spendable)
	}

	// NOTE on the other protocol: sdk.InternalizeProtocolBasketInsertion can
	// tag any output into a named basket, but it records no BRC-29 derivation
	// material, so basket-inserted coins are NOT wallet-signable today. Use the
	// wallet-payment protocol above for coins you intend to spend.
}

// buildMinedPayment builds a single-transaction, single-leaf-proven atomic BEEF
// whose sole output is a BRC-29 payment to recipientHex. The one-leaf merkle
// path makes the computed root equal the txid, so registering that root at
// height lets the recipient's headers client validate the proof.
func buildMinedPayment(recipientHex string, sats uint64, height uint32) ([]byte, chainhash.Hash, string, error) {
	keyID := brc29.KeyID{
		DerivationPrefix: base64.StdEncoding.EncodeToString(derivationPrefix),
		DerivationSuffix: base64.StdEncoding.EncodeToString(derivationSuffix),
	}
	lockingScript, err := brc29.LockForCounterparty(brc29.PrivHex(senderKeyHex), keyID, brc29.PubHex(recipientHex))
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("derive locking script: %w", err)
	}

	tx := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = 0x22
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: lockingScript})

	txidHash := tx.TxID()
	trueVal := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	if err := tx.AddMerkleProof(mp); err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("add merkle proof: %w", err)
	}
	computedRoot, err := mp.ComputeRoot(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("compute root: %w", err)
	}

	beef := transaction.NewBeefV2()
	if _, err := beef.MergeTransaction(tx); err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("merge into BEEF: %w", err)
	}
	beefBytes, err := beef.AtomicBytes(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("encode atomic BEEF: %w", err)
	}
	return beefBytes, *computedRoot, txidHash.String(), nil
}
