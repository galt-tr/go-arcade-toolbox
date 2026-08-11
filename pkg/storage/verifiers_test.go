package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
)

// txSpending builds a one-input transaction whose input carries the given
// unlocking script against a source output carrying the given locking script.
func txSpending(t *testing.T, lockingASM, unlockingASM string) *transaction.Transaction {
	t.Helper()

	lock, err := script.NewFromASM(lockingASM)
	if err != nil {
		t.Fatalf("parse locking script %q: %v", lockingASM, err)
	}
	unlock, err := script.NewFromASM(unlockingASM)
	if err != nil {
		t.Fatalf("parse unlocking script %q: %v", unlockingASM, err)
	}

	txid, err := chainhash.NewHashFromHex(strings.Repeat("11", 32))
	if err != nil {
		t.Fatalf("txid: %v", err)
	}

	tx := transaction.NewTransaction()
	tx.AddInputWithOutput(&transaction.TransactionInput{
		SourceTXID:       txid,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  unlock,
	}, &transaction.TransactionOutput{Satoshis: 1000, LockingScript: lock})
	return tx
}

// TestScriptsVerifier_ChronicleOpcodes pins the era switch. OP_2MUL is disabled
// under Genesis rules and re-enabled under Chronicle, so the same transaction
// must be rejected by one verifier and accepted by the other.
//
// This is the check that decides whether a Rúnar stateful covenant can be spent
// through this wallet at all: its OP_PUSH_TX preamble contains OP_2MUL.
func TestScriptsVerifier_ChronicleOpcodes(t *testing.T) {
	// 2 * 2 == 4
	tx := txSpending(t, "OP_2MUL OP_4 OP_EQUAL", "OP_2")

	t.Run("genesis rejects OP_2MUL", func(t *testing.T) {
		ok, err := newDefaultScriptsVerifier(false, 0).VerifyScripts(context.Background(), tx)
		if err == nil || ok {
			t.Fatalf("expected Genesis rules to reject OP_2MUL, got ok=%v err=%v", ok, err)
		}
		if !strings.Contains(err.Error(), "OP_2MUL") {
			t.Errorf("error should name the disabled opcode, got: %v", err)
		}
	})

	t.Run("chronicle accepts OP_2MUL", func(t *testing.T) {
		ok, err := newDefaultScriptsVerifier(true, 0).VerifyScripts(context.Background(), tx)
		if err != nil || !ok {
			t.Fatalf("expected Chronicle rules to accept OP_2MUL, got ok=%v err=%v", ok, err)
		}
	})
}

// TestScriptsVerifier_ChronicleStillRejectsBadScripts guards against the era
// flag being mistaken for a "skip verification" switch.
func TestScriptsVerifier_ChronicleStillRejectsBadScripts(t *testing.T) {
	tx := txSpending(t, "OP_2MUL OP_5 OP_EQUAL", "OP_2") // 2*2 == 4, not 5

	ok, err := newDefaultScriptsVerifier(true, 0).VerifyScripts(context.Background(), tx)
	if err == nil || ok {
		t.Fatalf("Chronicle verifier accepted a script that evaluates false: ok=%v err=%v", ok, err)
	}
}

// TestScriptsVerifier_GenesisUnchanged confirms ordinary scripts are unaffected
// by the new field, so the default path keeps its existing behavior.
func TestScriptsVerifier_GenesisUnchanged(t *testing.T) {
	tx := txSpending(t, "OP_3 OP_EQUAL", "OP_3")

	for _, chronicle := range []bool{false, true} {
		ok, err := newDefaultScriptsVerifier(chronicle, 0).VerifyScripts(context.Background(), tx)
		if err != nil || !ok {
			t.Errorf("chronicle=%v: ordinary script should verify, got ok=%v err=%v", chronicle, ok, err)
		}
	}
}

// TestScriptsVerifier_RequiresSourceOutput keeps the existing precondition
// covered now that option construction moved into a helper.
func TestScriptsVerifier_RequiresSourceOutput(t *testing.T) {
	txid, _ := chainhash.NewHashFromHex(strings.Repeat("11", 32))
	unlock, _ := script.NewFromASM("OP_3")

	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       txid,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  unlock,
	})

	if ok, err := newDefaultScriptsVerifier(false, 0).VerifyScripts(context.Background(), tx); err == nil || ok {
		t.Fatalf("expected an error when the input has no source output, got ok=%v err=%v", ok, err)
	}

	if ok, err := newDefaultScriptsVerifier(false, 0).VerifyScripts(context.Background(), nil); err == nil || ok {
		t.Fatalf("expected an error for a nil transaction, got ok=%v err=%v", ok, err)
	}
}

// TestWithChronicleOpcodesOption checks the Option actually reaches the
// constructed verifier.
func TestWithChronicleOpcodesOption(t *testing.T) {
	var p Provider
	WithChronicleOpcodes()(&p)
	if !p.chronicleScripts {
		t.Fatal("WithChronicleOpcodes did not set chronicleScripts")
	}

	v := newDefaultScriptsVerifier(p.chronicleScripts, p.genesisHeight)
	if !v.chronicle {
		t.Fatal("verifier did not inherit the Chronicle setting")
	}
}
