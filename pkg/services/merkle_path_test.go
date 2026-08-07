package services

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// buildSingleLeafMerklePath returns a BUMP whose block contains only txid, so
// ComputeRoot(txid) trivially returns txid itself as the root.
func buildSingleLeafMerklePath(txid *chainhash.Hash, blockHeight uint32) *transaction.MerklePath {
	isTxid := true
	return &transaction.MerklePath{
		BlockHeight: blockHeight,
		Path: [][]*transaction.PathElement{
			{{Offset: 0, Hash: txid, Txid: &isTxid}},
		},
	}
}

func TestMerklePathNotFound(t *testing.T) {
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())

	_, err := s.MerklePath(context.Background(), "deadbeef")
	require.Error(t, err)
	assert.ErrorIs(t, err, wdk.ErrNotFoundError)
}

func TestMerklePathKnownButNotYetMined(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecord("abc", arcade.StatusSeenOnNetwork))
	s := newTestServices(t, oracle, newFakeHeaders())

	result, err := s.MerklePath(context.Background(), "abc")
	require.NoError(t, err, "known-but-unproven is a non-error empty result, not a failure")
	require.NotNil(t, result)
	assert.Equal(t, defs.ArcadeServiceName, result.Name)
	assert.Nil(t, result.MerklePath)
	assert.Nil(t, result.BlockHeader)
}

func TestMerklePathMined(t *testing.T) {
	txHash := chainhash.HashH([]byte("txid"))
	txid := txHash.String()

	mp := buildSingleLeafMerklePath(&txHash, 100)

	oracle := newFakeOracle()
	rec := arcadeRecord(txid, arcade.StatusMined)
	rec.MerklePath = mp.Bytes()
	rec.BlockHeight = 100
	rec.BlockHash = "0000000000000000000000000000000000000000000000000000000000cafe"
	oracle.setTx(rec)
	s := newTestServices(t, oracle, newFakeHeaders())

	result, err := s.MerklePath(context.Background(), txid)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, defs.ArcadeServiceName, result.Name)
	require.NotNil(t, result.MerklePath)
	assert.Equal(t, uint32(100), result.MerklePath.BlockHeight)
	require.NotNil(t, result.BlockHeader)
	assert.Equal(t, uint32(100), result.BlockHeader.Height)
	assert.Equal(t, rec.BlockHash, result.BlockHeader.Hash)
	// Single-leaf block: the computed root is the txid itself.
	assert.Equal(t, txid, result.BlockHeader.MerkleRoot)
}

func TestMerklePathHeightMismatchIsAnError(t *testing.T) {
	txHash := chainhash.HashH([]byte("txid2"))
	txid := txHash.String()

	mp := buildSingleLeafMerklePath(&txHash, 100)

	oracle := newFakeOracle()
	rec := arcadeRecord(txid, arcade.StatusMined)
	rec.MerklePath = mp.Bytes()
	rec.BlockHeight = 200 // deliberately inconsistent with mp.BlockHeight
	oracle.setTx(rec)
	s := newTestServices(t, oracle, newFakeHeaders())

	_, err := s.MerklePath(context.Background(), txid)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestMerklePathOracleOpaqueError(t *testing.T) {
	boom := errors.New("transport exploded")
	oracle := newFakeOracle()
	oracle.getTxFunc = func(context.Context, string) (*arcade.TxRecord, error) { return nil, boom }
	s := newTestServices(t, oracle, newFakeHeaders())

	_, err := s.MerklePath(context.Background(), "abc")
	require.Error(t, err)
	// An opaque failure must NOT be mistaken for "not found".
	assert.NotErrorIs(t, err, wdk.ErrNotFoundError)
	assert.ErrorIs(t, err, boom)
}
