package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
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
type defaultScriptsVerifier struct {
	// chronicle selects Chronicle-era script rules. See [WithChronicleOpcodes].
	chronicle bool
	// genesisHeight is the block height at which Genesis activated on this
	// network. 0 disables per-input era selection. See
	// [WithGenesisActivationHeight].
	genesisHeight uint32
}

func newDefaultScriptsVerifier(chronicle bool, genesisHeight uint32) *defaultScriptsVerifier {
	return &defaultScriptsVerifier{chronicle: chronicle, genesisHeight: genesisHeight}
}

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
		err := engine.Execute(v.executionOptions(tx, i, src)...)
		if err != nil {
			return false, fmt.Errorf("storage: script verification failed for input %d: %w", i, err)
		}
	}
	return true, nil
}

// executionOptions builds the interpreter flags for one input. Chronicle
// implies after-genesis, so the two eras are mutually exclusive rather than
// additive.
//
// The era is a property of the INPUT, not of the transaction. go-sdk names the
// flag UTXOAfterGenesis for exactly that reason: the rules a script is judged by
// are those in force when the output it spends was created, and a node consults
// each input's own source height. A wallet that picks one ruleset for the whole
// transaction is therefore making a claim it has no right to make — see
// [WithGenesisActivationHeight].
func (v *defaultScriptsVerifier) executionOptions(
	tx *transaction.Transaction, i int, src *transaction.TransactionOutput,
) []interpreter.ExecutionOptionFunc {
	opts := []interpreter.ExecutionOptionFunc{
		interpreter.WithTx(tx, i, src),
		interpreter.WithForkID(),
	}
	if v.isPreGenesisUTXO(tx.Inputs[i]) {
		// No era option at all IS the pre-Genesis ruleset: go-sdk's thread starts
		// on beforeGenesisConfig and the era options only ever move it forward.
		// That restores MAX_SCRIPT_ELEMENT_SIZE_BEFORE_GENESIS (520 bytes), the
		// 500-opcode budget, and 4-byte script numbers.
		//
		// Bip16 (P2SH), which was also in force before Genesis, is deliberately
		// NOT added: it changes how a locking script matching the exact P2SH
		// pattern is evaluated, and this option exists to catch the limits that
		// actually reject transactions, not to fully re-enact a retired era.
		return opts
	}
	if v.chronicle {
		return append(opts, interpreter.WithAfterChronicle())
	}
	return append(opts, interpreter.WithAfterGenesis())
}

// isPreGenesisUTXO reports whether an input provably spends an output created
// before Genesis activated, and so must be judged by the pre-Genesis rules.
//
// "Provably" is the operative word. The only evidence is a merkle proof on the
// input's source transaction, which pins it to a block height; the BEEF the
// wallet stores for its inputs carries exactly that for every proven ancestor.
// An input with no proof is spending an unconfirmed or unproven output, which by
// definition cannot predate anything already mined — so an unknown height means
// the newest era, never the oldest. Guessing the other way would refuse a wallet
// its own freshly-created coins.
//
// Nothing here VERIFIES the proof; a caller-supplied BEEF could understate a
// height and provoke a spurious local refusal. That is a denial of service
// against the caller by the caller, not a way to get an invalid transaction
// accepted, and the merkle roots are checked where it matters (internalize).
func (v *defaultScriptsVerifier) isPreGenesisUTXO(in *transaction.TransactionInput) bool {
	if v.genesisHeight == 0 || in == nil || in.SourceTransaction == nil {
		return false
	}
	mp := in.SourceTransaction.MerklePath
	if mp == nil {
		return false
	}
	return mp.BlockHeight < v.genesisHeight
}
