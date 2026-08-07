package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
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
	if len(kt.InputBEEF) > 0 {
		if err := beef.MergeBeefBytes(kt.InputBEEF); err != nil {
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
