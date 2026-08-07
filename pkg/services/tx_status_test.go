package services

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

func TestGetStatusForTxIDsRequiresTxIDs(t *testing.T) {
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())
	_, err := s.GetStatusForTxIDs(context.Background(), nil)
	require.Error(t, err)
}

func TestGetStatusForTxIDsMapping(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecordWithHeight("mined", arcade.StatusMined, 90))
	oracle.setTx(arcadeRecordWithHeight("immutable", arcade.StatusImmutable, 50))
	oracle.setTx(arcadeRecord("seen", arcade.StatusSeenOnNetwork))
	oracle.setTx(arcadeRecord("pending", arcade.StatusPendingRetry))
	oracle.setTx(arcadeRecord("rejected", arcade.StatusRejected))
	oracle.setTx(arcadeRecord("unknown-status", arcade.StatusUnknown))
	// "missing" is never registered -> ErrTxNotFound from the fake.

	hdrs := newFakeHeaders()
	hdrs.height = 100

	s := newTestServices(t, oracle, hdrs)

	txids := []string{"mined", "immutable", "seen", "pending", "rejected", "unknown-status", "missing"}
	result, err := s.GetStatusForTxIDs(context.Background(), txids)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, defs.ArcadeServiceName, result.Name)
	assert.Equal(t, wdk.GetStatusSuccess, result.Status)
	require.Len(t, result.Results, len(txids))

	byTxID := make(map[string]wdk.TxStatusDetail, len(result.Results))
	for _, d := range result.Results {
		byTxID[d.TxID] = d
	}

	mined := byTxID["mined"]
	assert.Equal(t, wdk.ResultStatusForTxIDMined.String(), mined.Status)
	require.NotNil(t, mined.Depth)
	assert.Equal(t, 100-90+1, *mined.Depth)

	immutable := byTxID["immutable"]
	assert.Equal(t, wdk.ResultStatusForTxIDMined.String(), immutable.Status)
	require.NotNil(t, immutable.Depth)
	assert.Equal(t, 100-50+1, *immutable.Depth)

	for _, known := range []string{"seen", "pending"} {
		d := byTxID[known]
		assert.Equal(t, wdk.ResultStatusForTxIDKnown.String(), d.Status, known)
		require.NotNil(t, d.Depth, known)
		assert.Equal(t, 0, *d.Depth, known)
	}

	for _, notFound := range []string{"rejected", "unknown-status", "missing"} {
		d := byTxID[notFound]
		assert.Equal(t, wdk.ResultStatusForTxIDNotFound.String(), d.Status, notFound)
		assert.Nil(t, d.Depth, notFound)
	}
}

func TestGetStatusForTxIDsMinedWithoutResolvableTipUsesMinimumDepth(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecordWithHeight("mined", arcade.StatusMined, 90))

	hdrs := newFakeHeaders() // height 0 (unresolved)

	s := newTestServices(t, oracle, hdrs)
	result, err := s.GetStatusForTxIDs(context.Background(), []string{"mined"})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].Depth)
	assert.Equal(t, 1, *result.Results[0].Depth)
}

func TestGetStatusForTxIDsTipLookupFailureIsTolerated(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecordWithHeight("mined", arcade.StatusMined, 90))

	hdrs := newFakeHeaders()
	hdrs.heightErr = errors.New("headers unreachable")

	s := newTestServices(t, oracle, hdrs)
	result, err := s.GetStatusForTxIDs(context.Background(), []string{"mined"})
	require.NoError(t, err, "a failed tip lookup must not fail the whole call")
	require.Len(t, result.Results, 1)
	require.NotNil(t, result.Results[0].Depth)
	assert.Equal(t, 1, *result.Results[0].Depth)
}

func TestGetStatusForTxIDsToleratesPartialFailures(t *testing.T) {
	oracle := newFakeOracle()
	oracle.setTx(arcadeRecord("ok", arcade.StatusSeenOnNetwork))
	boom := errors.New("transport exploded")
	oracle.getTxFunc = func(_ context.Context, txid string) (*arcade.TxRecord, error) {
		if txid == "bad" {
			return nil, boom
		}
		rec, ok := oracle.txs[txid]
		if !ok {
			return nil, arcade.ErrTxNotFound
		}
		cp := *rec
		return &cp, nil
	}

	s := newTestServices(t, oracle, newFakeHeaders())
	result, err := s.GetStatusForTxIDs(context.Background(), []string{"ok", "bad"})
	require.NoError(t, err, "at least one success means the call as a whole succeeds")
	require.Len(t, result.Results, 1)
	assert.Equal(t, "ok", result.Results[0].TxID)
}

func TestGetStatusForTxIDsAllFailuresReturnsError(t *testing.T) {
	boom := errors.New("transport exploded")
	oracle := newFakeOracle()
	oracle.getTxFunc = func(context.Context, string) (*arcade.TxRecord, error) { return nil, boom }

	s := newTestServices(t, oracle, newFakeHeaders())
	_, err := s.GetStatusForTxIDs(context.Background(), []string{"a", "b"})
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

// TestGetStatusForTxIDsConcurrencyIsRaceClean exercises the bounded-concurrency
// path with far more txids than the concurrency limit, so it is meaningful
// under -race and would catch any shared-state bug in the fan-out.
func TestGetStatusForTxIDsConcurrencyIsRaceClean(t *testing.T) {
	oracle := newFakeOracle()
	const n = 200
	txids := make([]string, n)
	for i := 0; i < n; i++ {
		txid := fmt.Sprintf("tx-%d", i)
		txids[i] = txid
		oracle.setTx(arcadeRecord(txid, arcade.StatusSeenOnNetwork))
	}

	s := newTestServices(t, oracle, newFakeHeaders())
	result, err := s.GetStatusForTxIDs(context.Background(), txids)
	require.NoError(t, err)
	assert.Len(t, result.Results, n)
	for _, txid := range txids {
		assert.Equal(t, 1, oracle.getTxCallCount(txid))
	}
}

// --- test helpers ------------------------------------------------------------

func arcadeRecordWithHeight(txid string, status arcade.Status, height uint64) arcade.TxRecord {
	rec := arcadeRecord(txid, status)
	rec.BlockHeight = height
	return rec
}
