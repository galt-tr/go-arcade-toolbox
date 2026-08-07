package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// headersChainTracker adapts a [headers.Headers] source to go-sdk's
// chaintracker.ChainTracker (IsValidRootForHeight + CurrentHeight) so BEEF
// merkle-root verification can delegate to the header service.
type headersChainTracker struct {
	h headers.Headers
}

// IsValidRootForHeight reports whether root is the merkle root at height.
func (c headersChainTracker) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	return c.h.VerifyMerkleRoot(ctx, root, height)
}

// CurrentHeight returns the current chain tip height.
func (c headersChainTracker) CurrentHeight(ctx context.Context) (uint32, error) {
	return c.h.CurrentHeight(ctx)
}

// defaultBeefVerifier verifies a BEEF's structure and its BUMP merkle roots
// against the chain tracker (the header source). It is the local implementation
// of [wdk.BeefVerifier], a thin wrapper over go-sdk's beef.Verify.
type defaultBeefVerifier struct {
	tracker headersChainTracker
}

func newDefaultBeefVerifier(tracker headersChainTracker) *defaultBeefVerifier {
	return &defaultBeefVerifier{tracker: tracker}
}

var _ wdk.BeefVerifier = (*defaultBeefVerifier)(nil)

// VerifyBeef validates beef and its merkle roots. allowTxidOnly permits
// txid-only (stub) entries in the graph.
func (v *defaultBeefVerifier) VerifyBeef(ctx context.Context, beef *transaction.Beef, allowTxidOnly bool) (bool, error) {
	if beef == nil {
		return false, fmt.Errorf("storage: nil beef")
	}
	ok, err := beef.Verify(ctx, v.tracker, allowTxidOnly)
	if err != nil {
		return false, fmt.Errorf("storage: verify beef: %w", err)
	}
	return ok, nil
}

// defaultScriptsVerifier executes each input's unlocking+locking script pair
// through the go-sdk interpreter. It verifies SCRIPTS ONLY — no merkle proofs,
// no ancestor recursion (proofs belong to internalize). It is the local
// implementation of [wdk.ScriptsVerifier].
type defaultScriptsVerifier struct{}

func newDefaultScriptsVerifier() *defaultScriptsVerifier { return &defaultScriptsVerifier{} }

var _ wdk.ScriptsVerifier = (*defaultScriptsVerifier)(nil)

// VerifyScripts runs the script engine for every input of tx. Each input must
// carry its source output (SourceTransaction or an explicitly-set source
// output) so the locking script and satoshis are available.
func (v *defaultScriptsVerifier) VerifyScripts(_ context.Context, tx *transaction.Transaction) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("storage: nil transaction")
	}
	engine := interpreter.NewEngine()
	for i := range tx.Inputs {
		src := tx.Inputs[i].SourceTxOutput()
		if src == nil {
			return false, fmt.Errorf("storage: input %d has no source output to verify against", i)
		}
		err := engine.Execute(
			interpreter.WithTx(tx, i, src),
			interpreter.WithForkID(),
			interpreter.WithAfterGenesis(),
		)
		if err != nil {
			return false, fmt.Errorf("storage: script verification failed for input %d: %w", i, err)
		}
	}
	return true, nil
}
