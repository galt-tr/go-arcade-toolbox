package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// The rules a script is judged by belong to the OUTPUT being spent, not to the
// transaction spending it. These tests pin that the verifier reads each input's
// own source height rather than applying one era to the whole transaction —
// the divergence behind `utxoHeights=410|410 error=Push value size limit
// exceeded`, a teranode refusal on a transaction our verifier had passed.

// bigPushTx builds a one-input transaction that pushes pushSize bytes and drops
// them, leaving OP_TRUE. Only the pre-Genesis 520-byte element limit can reject
// it, so it isolates the era from every other rule.
//
// The source output is carried on a real source transaction, the way
// hydrateInputs supplies it in production — that is also the only place a merkle
// proof, and therefore a height, can live.
func bigPushTx(t *testing.T, pushSize int) *transaction.Transaction {
	t.Helper()
	unlock := &script.Script{}
	if err := unlock.AppendPushData(make([]byte, pushSize)); err != nil {
		t.Fatalf("append push: %v", err)
	}

	src := transaction.NewTransaction()
	src.AddOutput(&transaction.TransactionOutput{
		Satoshis:      1000,
		LockingScript: script.NewFromBytes([]byte{script.OpDROP, script.OpTRUE}),
	})

	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:        src.TxID(),
		SourceTxOutIndex:  0,
		SequenceNumber:    transaction.DefaultSequenceNumber,
		UnlockingScript:   unlock,
		SourceTransaction: src,
	})
	return tx
}

// provenAt gives the input's source transaction a merkle proof at the given
// height — the only evidence a wallet ever has for when an output was created.
func provenAt(t *testing.T, tx *transaction.Transaction, height uint32) {
	t.Helper()
	src := tx.Inputs[0].SourceTransaction
	src.MerklePath = transaction.NewMerklePath(height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: src.TxID(), Txid: boolPtr(true)},
	}})
}

func boolPtr(b bool) *bool { return &b }

func TestScriptsVerifier_PreGenesisUTXO_EnforcesPushLimit(t *testing.T) {
	const genesisHeight = 1000

	t.Run("pre-Genesis source rejects an oversized push", func(t *testing.T) {
		tx := bigPushTx(t, 521)
		provenAt(t, tx, genesisHeight-1)

		ok, err := newDefaultScriptsVerifier(true, genesisHeight).VerifyScripts(context.Background(), tx)
		if err == nil || ok {
			t.Fatalf("a 521-byte push against a pre-Genesis UTXO must be refused, got ok=%v err=%v", ok, err)
		}
		// go-sdk phrases the pre-Genesis MAX_SCRIPT_ELEMENT_SIZE failure as an
		// element-size error; teranode phrases the same rejection as "Push value
		// size limit exceeded". Assert on the numbers, which both agree on.
		if !strings.Contains(err.Error(), "521") || !strings.Contains(err.Error(), "520") {
			t.Errorf("the error must name the limit that rejected it, got: %v", err)
		}
	})

	t.Run("pre-Genesis source still accepts a legal push", func(t *testing.T) {
		tx := bigPushTx(t, 520)
		provenAt(t, tx, genesisHeight-1)

		if ok, err := newDefaultScriptsVerifier(true, genesisHeight).VerifyScripts(context.Background(), tx); err != nil || !ok {
			t.Fatalf("520 bytes is exactly at the pre-Genesis limit and must pass, got ok=%v err=%v", ok, err)
		}
	})

	t.Run("post-Genesis source accepts the same oversized push", func(t *testing.T) {
		tx := bigPushTx(t, 521)
		provenAt(t, tx, genesisHeight)

		if ok, err := newDefaultScriptsVerifier(true, genesisHeight).VerifyScripts(context.Background(), tx); err != nil || !ok {
			t.Fatalf("the limit is gone after Genesis, got ok=%v err=%v", ok, err)
		}
	})
}

// An unproven parent cannot predate anything already mined, so it gets the
// newest era. Guessing pre-Genesis here would refuse a wallet its own change.
func TestScriptsVerifier_UnprovenSourceUsesNewestEra(t *testing.T) {
	tx := bigPushTx(t, 5210) // no source transaction, so no height

	if ok, err := newDefaultScriptsVerifier(true, 1000).VerifyScripts(context.Background(), tx); err != nil || !ok {
		t.Fatalf("an input with no proof must be judged newest-era, got ok=%v err=%v", ok, err)
	}
}

// Without a configured activation height the toolbox has no basis for an
// opinion, so behavior is exactly what it was before per-input selection.
func TestScriptsVerifier_NoGenesisHeightKeepsGlobalEra(t *testing.T) {
	tx := bigPushTx(t, 5210)
	provenAt(t, tx, 1) // as pre-Genesis as a height gets

	if ok, err := newDefaultScriptsVerifier(true, 0).VerifyScripts(context.Background(), tx); err != nil || !ok {
		t.Fatalf("height 0 means the feature is off; the global era must still apply, got ok=%v err=%v", ok, err)
	}
}

func TestWithGenesisActivationHeightOption(t *testing.T) {
	var p Provider
	WithGenesisActivationHeight(620538)(&p)
	if p.genesisHeight != 620538 {
		t.Fatalf("WithGenesisActivationHeight did not set genesisHeight, got %d", p.genesisHeight)
	}
	if v := newDefaultScriptsVerifier(p.chronicleScripts, p.genesisHeight); v.genesisHeight != 620538 {
		t.Fatalf("verifier did not inherit the activation height, got %d", v.genesisHeight)
	}
}
