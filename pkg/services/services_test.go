package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

func testCfg() defs.WalletServices {
	return defs.WalletServices{Chain: defs.NetworkTestnet}
}

func newTestServices(t *testing.T, oracle *fakeOracle, hdrs *fakeHeaders, opts ...Option) *Services {
	t.Helper()
	return New(logging.NewTestLogger(t), oracle, hdrs, testCfg(), opts...)
}

func TestNewDefaultsGetBeefMaxDepth(t *testing.T) {
	s := New(logging.NewTestLogger(t), newFakeOracle(), newFakeHeaders(), defs.WalletServices{})
	assert.Equal(t, uint(defs.DefaultGetBeefMaxDepth), s.getBeefMaxDepth)
}

func TestNewHonorsConfiguredGetBeefMaxDepth(t *testing.T) {
	cfg := testCfg()
	cfg.GetBeefMaxDepth = 3
	s := New(logging.NewTestLogger(t), newFakeOracle(), newFakeHeaders(), cfg)
	assert.Equal(t, uint(3), s.getBeefMaxDepth)
}

func TestRawTxLocalSourceHit(t *testing.T) {
	oracle := newFakeOracle()
	src := newFakeRawTxSource()
	src.set("abc", []byte{1, 2, 3})
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	result, err := s.RawTx(context.Background(), "abc")
	require.NoError(t, err)
	assert.Equal(t, "abc", result.TxID)
	assert.Equal(t, localSourceName, result.Name)
	assert.Equal(t, []byte{1, 2, 3}, result.RawTx)
	assert.Equal(t, 0, oracle.getTxCallCount("abc"), "oracle must not be consulted on a local hit")
}

func TestRawTxFallsBackToOracleOnLocalMiss(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecord("abc", arcade.StatusReceived))
	oracle.txs["abc"].RawTx = []byte{9, 9, 9}
	src := newFakeRawTxSource() // empty: always misses
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	result, err := s.RawTx(context.Background(), "abc")
	require.NoError(t, err)
	assert.Equal(t, defs.ArcadeServiceName, result.Name)
	assert.Equal(t, []byte{9, 9, 9}, result.RawTx)
	assert.Equal(t, 1, src.calls)
}

func TestRawTxWithoutLocalSourceGoesStraightToOracle(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecord("abc", arcade.StatusReceived))
	oracle.txs["abc"].RawTx = []byte{4, 5, 6}
	s := newTestServices(t, oracle, newFakeHeaders())

	result, err := s.RawTx(context.Background(), "abc")
	require.NoError(t, err)
	assert.Equal(t, []byte{4, 5, 6}, result.RawTx)
}

func TestRawTxNotFoundAnywhere(t *testing.T) {
	oracle := newFakeOracle()
	src := newFakeRawTxSource()
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	_, err := s.RawTx(context.Background(), "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, wdk.ErrNotFoundError)
}

func TestRawTxOracleKnowsTxButHasNoBytes(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecord("abc", arcade.StatusReceived)) // no RawTx set
	s := newTestServices(t, oracle, newFakeHeaders())

	_, err := s.RawTx(context.Background(), "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, wdk.ErrNotFoundError)
}

func TestRawTxLocalSourceErrorPropagates(t *testing.T) {
	oracle := newFakeOracle()
	src := newFakeRawTxSource()
	src.err = errors.New("boom")
	s := newTestServices(t, oracle, newFakeHeaders(), WithLocalRawTxSource(src))

	_, err := s.RawTx(context.Background(), "abc")
	require.Error(t, err)
	assert.ErrorContains(t, err, "boom")
	assert.Equal(t, 0, oracle.getTxCallCount("abc"), "oracle must not be consulted when the local source errors")
}
