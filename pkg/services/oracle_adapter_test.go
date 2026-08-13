package services

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// stubWDKServices is a fully controllable wdk.Services double for exercising
// OracleFromServices — the reverse bridge — independent of this package's own
// Services implementation.
type stubWDKServices struct {
	postFromBEEFFunc  func(ctx context.Context, beef *transaction.Beef, txids []string) (wdk.PostFromBeefResult, error)
	getStatusFunc     func(ctx context.Context, txids []string) (*wdk.GetStatusForTxIDsResult, error)
	merklePathFunc    func(ctx context.Context, txid string) (*wdk.MerklePathResult, error)
	rawTxFunc         func(ctx context.Context, txid string) (wdk.RawTxResult, error)
	currentHeightFunc func(ctx context.Context) (uint32, error)
}

var _ wdk.Services = (*stubWDKServices)(nil)

func (s *stubWDKServices) ChainHeaderByHeight(context.Context, uint32) (*wdk.ChainBlockHeader, error) {
	return nil, nil
}

func (s *stubWDKServices) IsValidRootForHeight(context.Context, *chainhash.Hash, uint32) (bool, error) {
	return false, nil
}

func (s *stubWDKServices) CurrentHeight(ctx context.Context) (uint32, error) {
	if s.currentHeightFunc != nil {
		return s.currentHeightFunc(ctx)
	}
	return 0, nil
}

func (s *stubWDKServices) PostFromBEEF(ctx context.Context, beef *transaction.Beef, txids []string) (wdk.PostFromBeefResult, error) {
	if s.postFromBEEFFunc != nil {
		return s.postFromBEEFFunc(ctx, beef, txids)
	}
	return nil, nil
}

func (s *stubWDKServices) MerklePath(ctx context.Context, txid string) (*wdk.MerklePathResult, error) {
	if s.merklePathFunc != nil {
		return s.merklePathFunc(ctx, txid)
	}
	return nil, wdk.ErrNotFoundError
}

func (s *stubWDKServices) FindChainTipHeader(context.Context) (*wdk.ChainBlockHeader, error) {
	return nil, nil
}

func (s *stubWDKServices) RawTx(ctx context.Context, txid string) (wdk.RawTxResult, error) {
	if s.rawTxFunc != nil {
		return s.rawTxFunc(ctx, txid)
	}
	return wdk.RawTxResult{}, wdk.ErrNotFoundError
}

func (s *stubWDKServices) GetBEEF(context.Context, string, []string) (*transaction.Beef, error) {
	return nil, nil
}

func (s *stubWDKServices) NLockTimeIsFinal(context.Context, any) (bool, error) {
	return false, nil
}

func (s *stubWDKServices) GetStatusForTxIDs(ctx context.Context, txids []string) (*wdk.GetStatusForTxIDsResult, error) {
	if s.getStatusFunc != nil {
		return s.getStatusFunc(ctx, txids)
	}
	return nil, wdk.ErrNotFoundError
}

func successPostedResult(txid string) wdk.PostFromBeefResult {
	return wdk.PostFromBeefResult{{
		Name:             "stub",
		PostedBEEFResult: &wdk.PostedBEEF{TxIDResults: []wdk.PostedTxID{{TxID: txid, Result: wdk.PostedTxIDResultSuccess}}},
	}}
}

func TestOracleFromServicesBroadcastSuccess(t *testing.T) {
	tx := buildSimpleTx(t)
	ef, err := tx.EF()
	require.NoError(t, err)
	txid := tx.TxID().String()

	var seenTxIDs []string
	svc := &stubWDKServices{
		postFromBEEFFunc: func(_ context.Context, _ *transaction.Beef, txids []string) (wdk.PostFromBeefResult, error) {
			seenTxIDs = txids
			return successPostedResult(txid), nil
		},
	}

	oracle := OracleFromServices(svc)
	res, err := oracle.Broadcast(context.Background(), txid, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, txid, res.TxID)
	assert.False(t, res.Rejected)
	assert.Equal(t, []string{txid}, seenTxIDs)
}

func TestOracleFromServicesBroadcastRejection(t *testing.T) {
	tx := buildSimpleTx(t)
	ef, err := tx.EF()
	require.NoError(t, err)
	txid := tx.TxID().String()

	svc := &stubWDKServices{
		postFromBEEFFunc: func(context.Context, *transaction.Beef, []string) (wdk.PostFromBeefResult, error) {
			return wdk.PostFromBeefResult{{
				Name: "stub",
				PostedBEEFResult: &wdk.PostedBEEF{TxIDResults: []wdk.PostedTxID{{
					TxID:   txid,
					Result: wdk.PostedTxIDResultError,
					Error:  errors.New("bad fee"),
				}}},
			}}, nil
		},
	}

	oracle := OracleFromServices(svc)
	res, err := oracle.Broadcast(context.Background(), txid, ef)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.Rejected)
	assert.Contains(t, res.ExtraInfo, "bad fee")
}

func TestOracleFromServicesBroadcastGoErrorPropagates(t *testing.T) {
	tx := buildSimpleTx(t)
	ef, err := tx.EF()
	require.NoError(t, err)
	txid := tx.TxID().String()

	boom := errors.New("call failed")
	svc := &stubWDKServices{
		postFromBEEFFunc: func(context.Context, *transaction.Beef, []string) (wdk.PostFromBeefResult, error) {
			return nil, boom
		},
	}

	oracle := OracleFromServices(svc)
	_, err = oracle.Broadcast(context.Background(), txid, ef)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

func TestOracleFromServicesBroadcastRejectsMismatchedTxID(t *testing.T) {
	tx := buildSimpleTx(t)
	ef, err := tx.EF()
	require.NoError(t, err)

	svc := &stubWDKServices{}
	oracle := OracleFromServices(svc)
	_, err = oracle.Broadcast(context.Background(), "not-the-real-txid", ef)
	require.Error(t, err)
}

func TestOracleFromServicesGetTxMined(t *testing.T) {
	depth := 5
	svc := &stubWDKServices{
		getStatusFunc: func(context.Context, []string) (*wdk.GetStatusForTxIDsResult, error) {
			return &wdk.GetStatusForTxIDsResult{Results: []wdk.TxStatusDetail{{
				TxID: "abc", Status: wdk.ResultStatusForTxIDMined.String(), Depth: &depth,
			}}}, nil
		},
		merklePathFunc: func(context.Context, string) (*wdk.MerklePathResult, error) {
			return &wdk.MerklePathResult{
				MerklePath:  &transaction.MerklePath{BlockHeight: 10},
				BlockHeader: &wdk.MerklePathBlockHeader{Height: 10, Hash: "cafe"},
			}, nil
		},
		rawTxFunc: func(context.Context, string) (wdk.RawTxResult, error) {
			return wdk.RawTxResult{RawTx: []byte{1, 2, 3}}, nil
		},
	}

	oracle := OracleFromServices(svc)
	rec, err := oracle.GetTx(context.Background(), "abc")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, arcade.StatusMined, rec.Status)
	assert.Equal(t, uint64(10), rec.BlockHeight)
	assert.Equal(t, "cafe", rec.BlockHash)
	assert.Equal(t, arcade.HexBytes{1, 2, 3}, rec.RawTx)
	assert.NotEmpty(t, rec.MerklePath)
}

func TestOracleFromServicesGetTxKnown(t *testing.T) {
	svc := &stubWDKServices{
		getStatusFunc: func(context.Context, []string) (*wdk.GetStatusForTxIDsResult, error) {
			return &wdk.GetStatusForTxIDsResult{Results: []wdk.TxStatusDetail{{
				TxID: "abc", Status: wdk.ResultStatusForTxIDKnown.String(),
			}}}, nil
		},
	}

	oracle := OracleFromServices(svc)
	rec, err := oracle.GetTx(context.Background(), "abc")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, arcade.StatusSeenOnNetwork, rec.Status)
}

func TestOracleFromServicesGetTxNotFound(t *testing.T) {
	svc := &stubWDKServices{
		getStatusFunc: func(context.Context, []string) (*wdk.GetStatusForTxIDsResult, error) {
			return &wdk.GetStatusForTxIDsResult{Results: []wdk.TxStatusDetail{{
				TxID: "abc", Status: wdk.ResultStatusForTxIDNotFound.String(),
			}}}, nil
		},
	}

	oracle := OracleFromServices(svc)
	_, err := oracle.GetTx(context.Background(), "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, arcade.ErrTxNotFound)
}

func TestOracleFromServicesGetTxServiceErrorNotFound(t *testing.T) {
	svc := &stubWDKServices{} // default getStatusFunc returns wdk.ErrNotFoundError

	oracle := OracleFromServices(svc)
	_, err := oracle.GetTx(context.Background(), "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, arcade.ErrTxNotFound)
}

func TestOracleFromServicesGetTxOpaqueError(t *testing.T) {
	boom := errors.New("call failed")
	svc := &stubWDKServices{
		getStatusFunc: func(context.Context, []string) (*wdk.GetStatusForTxIDsResult, error) { return nil, boom },
	}

	oracle := OracleFromServices(svc)
	_, err := oracle.GetTx(context.Background(), "abc")
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, arcade.ErrTxNotFound)
}

func TestOracleFromServicesStreamStatusNotSupported(t *testing.T) {
	oracle := OracleFromServices(&stubWDKServices{})
	err := oracle.StreamStatus(context.Background(), "", func(arcade.StatusEvent) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrStreamingNotSupported)
}

func TestOracleFromServicesHealth(t *testing.T) {
	svc := &stubWDKServices{
		currentHeightFunc: func(context.Context) (uint32, error) { return 123, nil },
	}
	oracle := OracleFromServices(svc)
	health, err := oracle.Health(context.Background())
	require.NoError(t, err)
	require.NotNil(t, health)
	assert.True(t, health.Healthy)
	assert.Equal(t, uint64(123), health.BlockHeight)
}

func TestOracleFromServicesHealthFailure(t *testing.T) {
	boom := errors.New("no height")
	svc := &stubWDKServices{
		currentHeightFunc: func(context.Context) (uint32, error) { return 0, boom },
	}
	oracle := OracleFromServices(svc)
	health, err := oracle.Health(context.Background())
	require.Error(t, err)
	require.NotNil(t, health)
	assert.False(t, health.Healthy)
}
