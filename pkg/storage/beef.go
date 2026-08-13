package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
)

// maxBEEFDepth bounds the ancestry walk in getBEEFForTxIDs.
const maxBEEFDepth = 16

// getBEEFForTxIDs assembles a BEEF covering txids and (recursively) their local
// ancestry, drawn from the metastore known_txs. base is an optional BEEF to
// merge into (nil starts a fresh V2 BEEF). knownTxids are txids the caller
// asserts the wallet already has, merged as txid-only stubs and not walked.
//
// A txid with no local known_txs row is merged as a txid-only stub rather than
// failing: the wallet may already hold it, and CreateAction's own coins always
// have a stored raw tx to walk.
func (p *Provider) getBEEFForTxIDs(ctx context.Context, txids []string, base *transaction.Beef, knownTxids []string) (*transaction.Beef, error) {
	beef := base
	if beef == nil {
		beef = transaction.NewBeefV2()
	}
	known := make(map[string]struct{}, len(knownTxids))
	for _, k := range knownTxids {
		known[k] = struct{}{}
	}
	for _, txid := range txids {
		if err := p.buildBEEF(ctx, beef, txid, known, 0); err != nil {
			return nil, err
		}
	}
	return beef, nil
}

// inputBEEFFor resolves the ancestry BEEF retained for a known transaction.
//
// The blob is written by CreateAction to the TRANSACTIONS row — before the txid
// exists, which is exactly why that copy and not this one is authoritative:
// CreateAction and ProcessAction are separate requests, possibly separate
// processes, and the wallet holds its own copy only in memory with no API to
// hand it back. known_txs therefore cannot be its home.
//
// known_txs.input_beef is still PREFERRED when present. That is not a
// correctness fallback but a cost one: it arrived free with the row the caller
// already loaded, and two writers still fill it — InternalizeAction (whose txid
// is known up front, so it has no pre-txid window to bridge) and every row
// created before this de-duplication. Only rows written by the new create path
// pay the extra lookup.
//
// A miss is not an error. Both copies are dropped at MINED and hydrateInputs
// no-ops on empty bytes, so "no retained ancestry" is a legitimate answer.
func (p *Provider) inputBEEFFor(ctx context.Context, txid string, ktInputBEEF []byte) ([]byte, error) {
	if len(ktInputBEEF) > 0 {
		return ktInputBEEF, nil
	}
	blob, found, err := p.meta.Transactions().GetInputBEEFByTxID(ctx, txid)
	if err != nil {
		return nil, fmt.Errorf("storage: load input beef for %s: %w", txid, err)
	}
	if !found {
		return nil, nil
	}
	return blob, nil
}

// inputBEEFForBatch is inputBEEFFor over a batch, in ONE query. Rows carrying
// their own copy are answered from it and never reach the database.
//
// The delayed broadcaster fans out at sendConcurrency over a whole sweep, so a
// per-transaction round trip there would trade the write saving this change
// exists for against read latency on the same path.
func (p *Provider) inputBEEFForBatch(ctx context.Context, kts []metastore.KnownTx) (map[string][]byte, error) {
	out := make(map[string][]byte, len(kts))
	missing := make([]string, 0, len(kts))
	for i := range kts {
		if len(kts[i].InputBEEF) > 0 {
			out[kts[i].TxID] = kts[i].InputBEEF
			continue
		}
		missing = append(missing, kts[i].TxID)
	}
	if len(missing) == 0 {
		return out, nil
	}
	found, err := p.meta.Transactions().InputBEEFByTxIDs(ctx, missing)
	if err != nil {
		return nil, fmt.Errorf("storage: load input beefs: %w", err)
	}
	for txid, blob := range found {
		out[txid] = blob
	}
	return out, nil
}

func (p *Provider) buildBEEF(ctx context.Context, beef *transaction.Beef, txid string, known map[string]struct{}, depth int) error {
	if depth > maxBEEFDepth {
		return fmt.Errorf("storage: beef ancestry too deep at %s", txid)
	}
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: parse txid %q: %w", txid, err)
	}
	if beef.FindTransactionByHash(hash) != nil {
		return nil // already present
	}
	if _, ok := known[txid]; ok {
		beef.MergeTxidOnly(hash)
		return nil
	}

	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, txid)
	if err != nil {
		return fmt.Errorf("storage: load known tx %s: %w", txid, err)
	}
	if !found || len(kt.RawTx) == 0 {
		// Not stored locally: leave a txid-only stub for the wallet to resolve.
		beef.MergeTxidOnly(hash)
		return nil
	}

	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		return fmt.Errorf("storage: parse raw tx %s: %w", txid, err)
	}

	if len(kt.MerklePath) > 0 {
		mp, err := transaction.NewMerklePathFromBinary(kt.MerklePath)
		if err != nil {
			return fmt.Errorf("storage: parse merkle path %s: %w", txid, err)
		}
		if err := tx.AddMerkleProof(mp); err != nil {
			return fmt.Errorf("storage: attach merkle proof %s: %w", txid, err)
		}
		if _, err := beef.MergeTransaction(tx); err != nil {
			return fmt.Errorf("storage: merge proven tx %s: %w", txid, err)
		}
		return nil // proven: terminal ancestor
	}

	if _, err := beef.MergeRawTx(kt.RawTx, nil); err != nil {
		return fmt.Errorf("storage: merge raw tx %s: %w", txid, err)
	}

	// Direct-only mode: the spent coin's own transaction (just merged) carries
	// the output arcade needs as the EF prevout. Stop here rather than merging
	// this tx's stored input BEEF and recursing — that walk is O(chain length)
	// and, under delayed broadcast with nothing yet mined, an unbounded unproven
	// chain would blow up MergeBump/ComputeRoot. Deep ancestry is BRC-62
	// completeness the arcade-only model does not require.
	if p.directInputBEEF {
		return nil
	}

	// Ancestry is retained on the transactions row (see inputBEEFFor). This is
	// reached only in the non-direct mode and only for an UNPROVEN ancestor —
	// the proven branch above returns — so a mined tx never issues the lookup.
	stored, err := p.inputBEEFFor(ctx, txid, kt.InputBEEF)
	if err != nil {
		return err
	}
	if len(stored) > 0 {
		if err := beef.MergeBeefBytes(stored); err != nil {
			return fmt.Errorf("storage: merge input beef %s: %w", txid, err)
		}
	}
	// Recurse into inputs whose source is not yet fully present.
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		src := beef.FindTransactionByHash(in.SourceTXID)
		if src != nil {
			continue
		}
		if err := p.buildBEEF(ctx, beef, in.SourceTXID.String(), known, depth+1); err != nil {
			return err
		}
	}
	return nil
}
