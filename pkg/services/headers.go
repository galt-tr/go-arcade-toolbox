package services

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// FindChainTipHeader returns the header of the current chain tip, resolved as
// headers.CurrentHeight followed by headers.HeaderByHeight.
func (s *Services) FindChainTipHeader(ctx context.Context) (*wdk.ChainBlockHeader, error) {
	height, err := s.hdrs.CurrentHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("services: find chain tip: current height: %w", err)
	}
	return s.ChainHeaderByHeight(ctx, height)
}

// ChainHeaderByHeight returns the block header at height from the configured
// [headers.Headers] source.
func (s *Services) ChainHeaderByHeight(ctx context.Context, height uint32) (*wdk.ChainBlockHeader, error) {
	h, err := s.hdrs.HeaderByHeight(ctx, height)
	if err != nil {
		return nil, fmt.Errorf("services: get header at height %d: %w", height, err)
	}
	return toChainBlockHeader(h), nil
}

// CurrentHeight returns the current chain tip height from the configured
// [headers.Headers] source. It also satisfies [wdk.HeightProvider], which is
// what NLockTimeIsFinal's height-based branch consumes.
func (s *Services) CurrentHeight(ctx context.Context) (uint32, error) {
	height, err := s.hdrs.CurrentHeight(ctx)
	if err != nil {
		return 0, fmt.Errorf("services: current height: %w", err)
	}
	return height, nil
}

// IsValidRootForHeight verifies root against the merkle root of the block at
// height, delegating to [headers.Headers.VerifyMerkleRoot] (performed LOCALLY
// by that client — see its doc comment).
func (s *Services) IsValidRootForHeight(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	ok, err := s.hdrs.VerifyMerkleRoot(ctx, root, height)
	if err != nil {
		return false, fmt.Errorf("services: verify merkle root at height %d: %w", height, err)
	}
	return ok, nil
}

// NLockTimeIsFinal checks whether the provided value (a transaction, its raw
// bytes/hex, or a raw locktime) is a final nLockTime, using [wdk.NLockTimeIsFinal]
// with this Services as the [wdk.HeightProvider] for the height-based branch.
func (s *Services) NLockTimeIsFinal(ctx context.Context, txOrLockTime any) (bool, error) {
	isFinal, err := wdk.NLockTimeIsFinal(ctx, s, txOrLockTime)
	if err != nil {
		return false, fmt.Errorf("services: nlocktime is final: %w", err)
	}
	return isFinal, nil
}

// toChainBlockHeader projects a [headers.Header] onto the wdk.Services
// [wdk.ChainBlockHeader] shape. Hash fields render via chainhash.Hash.String(),
// which is the conventional byte-reversed hex representation
// [wdk.ChainBaseBlockHeader] expects.
func toChainBlockHeader(h *headers.Header) *wdk.ChainBlockHeader {
	return &wdk.ChainBlockHeader{
		ChainBaseBlockHeader: wdk.ChainBaseBlockHeader{
			//nolint:gosec // G115: block version is a small protocol field; wrap-around on a
			// (theoretical) negative value round-trips identically to the wire bytes.
			Version:      uint32(h.Version),
			PreviousHash: h.PrevBlock.String(),
			MerkleRoot:   h.MerkleRoot.String(),
			Time:         h.Timestamp,
			Bits:         h.Bits,
			Nonce:        h.Nonce,
		},
		Height: uint(h.Height),
		Hash:   h.Hash.String(),
	}
}
