package services

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

func TestPostFromBEEFRequiresNonNilBeef(t *testing.T) {
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())
	_, err := s.PostFromBEEF(context.Background(), nil, []string{"abc"})
	require.Error(t, err)
}

func TestPostFromBEEFAcceptsAndReportsSuccess(t *testing.T) {
	_, child, beef := buildChainedTx(t)
	childID := child.TxID().String()

	oracle := newFakeOracle()
	s := newTestServices(t, oracle, newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{childID})
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.NoError(t, r.Error)
	require.NotNil(t, r.PostedBEEFResult)
	require.Len(t, r.PostedBEEFResult.TxIDResults, 1)
	assert.Equal(t, wdk.PostedTxIDResultSuccess, r.PostedBEEFResult.TxIDResults[0].Result)
	assert.True(t, r.Success())
	assert.Equal(t, 1, oracle.broadcastCalls)
}

func TestPostFromBEEFRejection(t *testing.T) {
	_, child, beef := buildChainedTx(t)
	childID := child.TxID().String()

	oracle := newFakeOracle()
	oracle.broadcastFunc = func(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusRejected, Rejected: true, ExtraInfo: "bad fee"}, nil
	}
	s := newTestServices(t, oracle, newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{childID})
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	// A rejection is a successful call to arcade carrying an application-level
	// verdict: the outer wrapper has no top-level Error...
	assert.NoError(t, r.Error)
	require.NotNil(t, r.PostedBEEFResult)
	require.Len(t, r.PostedBEEFResult.TxIDResults, 1)
	posted := r.PostedBEEFResult.TxIDResults[0]
	// ...but the nested result carries the failure, and the wrapper is
	// therefore not a "success" per wdk's own Success() semantics.
	assert.Equal(t, wdk.PostedTxIDResultError, posted.Result)
	require.Error(t, posted.Error)
	assert.Contains(t, posted.Error.Error(), "bad fee")

	// wdk's own Aggregated() classifies any nested Result with a non-nil Error
	// (that isn't a DoubleSpend) as a "serviceError", regardless of whether the
	// error originated from a genuine tx-level rejection or a transport
	// failure — that conflation lives in wdk, not in this shim.
	agg := results.Aggregated([]string{childID})
	assert.Equal(t, wdk.AggregatedPostedTxIDServiceError, agg[childID].Status)
}

func TestPostFromBEEFBackpressureIsAnOuterError(t *testing.T) {
	_, child, beef := buildChainedTx(t)
	childID := child.TxID().String()

	oracle := newFakeOracle()
	oracle.broadcastFunc = func(context.Context, string, []byte) (*arcade.BroadcastResult, error) {
		return nil, &arcade.BackpressureError{}
	}
	s := newTestServices(t, oracle, newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{childID})
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	require.Error(t, r.Error)
	var bp *arcade.BackpressureError
	assert.True(t, errors.As(r.Error, &bp), "backpressure error type must survive the wrap")
	assert.False(t, r.Success())

	serviceErrs := results.ServiceErrors()
	require.Contains(t, serviceErrs, defs.ArcadeServiceName)
}

func TestPostFromBEEFCircuitOpenIsAnOuterError(t *testing.T) {
	_, child, beef := buildChainedTx(t)
	childID := child.TxID().String()

	oracle := newFakeOracle()
	oracle.broadcastFunc = func(context.Context, string, []byte) (*arcade.BroadcastResult, error) {
		return nil, arcade.ErrCircuitOpen
	}
	s := newTestServices(t, oracle, newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{childID})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.ErrorIs(t, results[0].Error, arcade.ErrCircuitOpen)
}

func TestPostFromBEEFAlreadyMinedIsSkipped(t *testing.T) {
	_, child, beef := buildChainedTx(t)
	childID := child.TxID().String()

	// Mark the child as already mined directly on the transaction object held
	// by the Beef.
	minedTx := beef.FindTransactionForSigning(childID)
	require.NotNil(t, minedTx)
	minedTx.MerklePath = &transaction.MerklePath{}

	oracle := newFakeOracle()
	s := newTestServices(t, oracle, newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{childID})
	require.NoError(t, err)
	assert.Empty(t, results, "an already-mined tx produces no result and is not broadcast")
	assert.Equal(t, 0, oracle.broadcastCalls)
}

func TestPostFromBEEFMissingTxIDInBeef(t *testing.T) {
	_, _, beef := buildChainedTx(t)
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())

	results, err := s.PostFromBEEF(context.Background(), beef, []string{"deadbeef"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Error(t, results[0].Error)
	assert.False(t, results[0].Success())
}
