package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
)

func TestGetBEEFLocalOnly(t *testing.T) {
	parent, child, _ := buildChainedTx(t)
	parentID := parent.TxID().String()
	childID := child.TxID().String()

	src := newFakeRawTxSource()
	src.set(parentID, parent.Bytes())
	src.set(childID, child.Bytes())

	oracle := newFakeOracle() // never consulted: local source has both txs
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	beef, err := s.GetBEEF(context.Background(), childID, nil)
	require.NoError(t, err)
	require.NotNil(t, beef)

	assert.NotNil(t, beef.FindTransaction(childID))
	assert.NotNil(t, beef.FindTransaction(parentID))
	assert.Equal(t, 0, oracle.broadcastCalls)
}

func TestGetBEEFOracleFallbackForMissingAncestor(t *testing.T) {
	parent, child, _ := buildChainedTx(t)
	parentID := parent.TxID().String()
	childID := child.TxID().String()

	// Local source only has the child; the parent must come from Arcade.
	src := newFakeRawTxSource()
	src.set(childID, child.Bytes())

	oracle := newFakeOracle()
	childRec := arcadeRecord(childID, arcade.StatusSeenOnNetwork)
	childRec.RawTx = child.Bytes()
	oracle.setTx(childRec)
	parentRec := arcadeRecord(parentID, arcade.StatusSeenOnNetwork)
	parentRec.RawTx = parent.Bytes()
	oracle.setTx(parentRec)

	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	beef, err := s.GetBEEF(context.Background(), childID, nil)
	require.NoError(t, err)
	require.NotNil(t, beef)
	assert.NotNil(t, beef.FindTransaction(childID))
	assert.NotNil(t, beef.FindTransaction(parentID))
	assert.GreaterOrEqual(t, oracle.getTxCallCount(parentID), 1)
}

func TestGetBEEFKnownTxIDsStopTheWalk(t *testing.T) {
	parent, child, _ := buildChainedTx(t)
	parentID := parent.TxID().String()
	childID := child.TxID().String()

	src := newFakeRawTxSource()
	src.set(childID, child.Bytes()) // parent intentionally absent everywhere

	oracle := newFakeOracle() // parent absent here too
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	beef, err := s.GetBEEF(context.Background(), childID, []string{parentID})
	require.NoError(t, err)
	require.NotNil(t, beef)
	assert.NotNil(t, beef.FindTransaction(childID))
	// The parent is present only as a txid-only stub: FindTransaction (which
	// requires a full transaction) must not find it, but the beef must
	// nonetheless carry a hash-indexed entry for it.
	assert.Nil(t, beef.FindTransaction(parentID))
	assert.NotZero(t, len(beef.Transactions))
}

func TestGetBEEFMissingAncestorEverywhereIsAnError(t *testing.T) {
	_, child, _ := buildChainedTx(t)
	childID := child.TxID().String()

	src := newFakeRawTxSource()
	src.set(childID, child.Bytes())
	oracle := newFakeOracle() // parent absent

	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	_, err := s.GetBEEF(context.Background(), childID, nil)
	require.Error(t, err)
}

func TestGetBEEFMaxDepthExceeded(t *testing.T) {
	grandparent, parent, child := buildThreeLevelChain(t)
	childID := child.TxID().String()

	src := newFakeRawTxSource()
	src.set(grandparent.TxID().String(), grandparent.Bytes())
	src.set(parent.TxID().String(), parent.Bytes())
	src.set(childID, child.Bytes())

	cfg := testCfg()
	cfg.GetBeefMaxDepth = 1 // child (depth 0) -> parent (depth 1, OK) -> grandparent (depth 2, exceeds).
	s := New(logging.NewTestLogger(t), newFakeOracle(), newFakeHeaders(), cfg, WithLocalRawTxSource(src))

	_, err := s.GetBEEF(context.Background(), childID, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max depth")
}

// TestGetBEEFAlreadyMinedAncestorStopsTheWalk verifies that once an ancestor
// carries a merkle proof, its own inputs are not walked further (mirroring
// the reference implementation). The parent transaction in buildChainedTx has
// a synthetic seed input that resolves nowhere; if the walk tried to descend
// into it after the parent was proven mined, this call would error.
func TestGetBEEFAlreadyMinedAncestorStopsTheWalk(t *testing.T) {
	parent, child, _ := buildChainedTx(t)
	parentID := parent.TxID().String()
	childID := child.TxID().String()

	src := newFakeRawTxSource()
	src.set(parentID, parent.Bytes())
	src.set(childID, child.Bytes())

	mp := buildSingleLeafMerklePath(parent.TxID(), 42)

	oracle := newFakeOracle()
	rec := arcadeRecord(parentID, arcade.StatusMined)
	rec.MerklePath = mp.Bytes()
	rec.BlockHeight = 42
	rec.BlockHash = "cafe"
	oracle.setTx(rec)

	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	beef, err := s.GetBEEF(context.Background(), childID, nil)
	require.NoError(t, err)
	parentTx := beef.FindTransaction(parentID)
	require.NotNil(t, parentTx)
	assert.NotNil(t, parentTx.MerklePath)
}
