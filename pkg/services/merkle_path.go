package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// MerklePath polls Arcade (GET /tx/{txid} via [arcade.TxOracle.GetTx]) for a
// merkle proof (BUMP). This is Arcade-only: proofs are not consulted from any
// local source, since Arcade is the sole authority on whether — and via which
// proof — a transaction is mined (see the arcade-only design doc).
//
// Three outcomes:
//   - Arcade has no record of txid at all -> error matching [wdk.ErrNotFoundError].
//   - Arcade knows txid but it is not yet mined (no merklePath on the record)
//     -> a non-error, empty result (Name set, MerklePath/BlockHeader nil). This
//     mirrors the old services' "known but not yet proven" behavior: it is not
//     a failure, just "no proof yet".
//   - Arcade reports a merklePath -> the parsed [transaction.MerklePath] plus
//     the block header identity the proof is anchored to.
func (s *Services) MerklePath(ctx context.Context, txid string) (*wdk.MerklePathResult, error) {
	rec, err := s.oracle.GetTx(ctx, txid)
	if err != nil {
		if errors.Is(err, arcade.ErrTxNotFound) {
			return nil, fmt.Errorf("services: tx %s not found: %w", txid, wdk.ErrNotFoundError)
		}
		return nil, fmt.Errorf("services: get tx %s: %w", txid, err)
	}

	if len(rec.MerklePath) == 0 {
		return &wdk.MerklePathResult{Name: defs.ArcadeServiceName}, nil
	}

	mp, err := transaction.NewMerklePathFromBinary(rec.MerklePath)
	if err != nil {
		return nil, fmt.Errorf("services: parse merkle path for %s: %w", txid, err)
	}

	if uint64(mp.BlockHeight) != rec.BlockHeight {
		return nil, fmt.Errorf(
			"services: merkle path block height %d does not match tx block height %d for %s",
			mp.BlockHeight, rec.BlockHeight, txid,
		)
	}

	txHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return nil, fmt.Errorf("services: parse txid %q: %w", txid, err)
	}
	root, err := mp.ComputeRoot(txHash)
	if err != nil {
		return nil, fmt.Errorf("services: compute merkle root for %s: %w", txid, err)
	}

	return &wdk.MerklePathResult{
		Name:       defs.ArcadeServiceName,
		MerklePath: mp,
		BlockHeader: &wdk.MerklePathBlockHeader{
			Height:     mp.BlockHeight,
			MerkleRoot: root.String(),
			Hash:       rec.BlockHash,
		},
	}, nil
}
