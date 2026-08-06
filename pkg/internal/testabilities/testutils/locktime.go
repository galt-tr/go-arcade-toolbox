// Package testutils holds small, shared helpers used by go-arcade-toolbox
// tests to build transactions and stub chain-height lookups.
package testutils

import (
	"context"
	"testing"

	sdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// NewTestTransactionWithLocktime builds a minimal tx with provided locktime and sequences.
func NewTestTransactionWithLocktime(t testing.TB, lock uint32, inputSequences ...uint32) *sdk.Transaction {
	t.Helper()
	tx := sdk.NewTransaction()
	tx.LockTime = lock
	tx.Inputs = make([]*sdk.TransactionInput, len(inputSequences))
	for i, s := range inputSequences {
		tx.Inputs[i] = &sdk.TransactionInput{SequenceNumber: s}
	}
	return tx
}

// StubHeight is a stub chain-height source for tests.
type StubHeight struct {
	h   uint32
	err error
}

// CurrentHeight returns the stubbed height and error.
func (s StubHeight) CurrentHeight(context.Context) (uint32, error) { return s.h, s.err }

// NewStubHeight creates a StubHeight that always returns h and err.
func NewStubHeight(h uint32, err error) StubHeight {
	return StubHeight{h: h, err: err}
}
