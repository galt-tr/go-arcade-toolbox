package services

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	sdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
)

func TestFindChainTipHeader(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.height = 42
	var hash chainhash.Hash
	hash[0] = 0x11
	hdrs.byHeight[42] = &headers.Header{Height: 42, Hash: hash, Version: 1, Bits: 7, Nonce: 9, Timestamp: 123}

	s := newTestServices(t, newFakeOracle(), hdrs)
	h, err := s.FindChainTipHeader(context.Background())
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.Equal(t, uint(42), h.Height)
	assert.Equal(t, hash.String(), h.Hash)
	assert.Equal(t, uint32(1), h.Version)
	assert.Equal(t, uint32(7), h.Bits)
	assert.Equal(t, uint32(9), h.Nonce)
	assert.Equal(t, uint32(123), h.Time)
}

func TestChainHeaderByHeight(t *testing.T) {
	hdrs := newFakeHeaders()
	var hash chainhash.Hash
	hash[0] = 0x22
	hdrs.byHeight[10] = &headers.Header{Height: 10, Hash: hash}

	s := newTestServices(t, newFakeOracle(), hdrs)
	h, err := s.ChainHeaderByHeight(context.Background(), 10)
	require.NoError(t, err)
	require.NotNil(t, h)
	assert.Equal(t, uint(10), h.Height)
	assert.Equal(t, hash.String(), h.Hash)
}

func TestCurrentHeight(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.height = 777
	s := newTestServices(t, newFakeOracle(), hdrs)

	height, err := s.CurrentHeight(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(777), height)
}

func TestIsValidRootForHeight(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.verifyFunc = func(context.Context, *chainhash.Hash, uint32) (bool, error) { return true, nil }
	s := newTestServices(t, newFakeOracle(), hdrs)

	var root chainhash.Hash
	ok, err := s.IsValidRootForHeight(context.Background(), &root, 5)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestIsValidRootForHeightRejects(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.verifyFunc = func(context.Context, *chainhash.Hash, uint32) (bool, error) { return false, nil }
	s := newTestServices(t, newFakeOracle(), hdrs)

	var root chainhash.Hash
	ok, err := s.IsValidRootForHeight(context.Background(), &root, 5)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestNLockTimeIsFinalZeroLockTime(t *testing.T) {
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())
	final, err := s.NLockTimeIsFinal(context.Background(), uint32(0))
	require.NoError(t, err)
	assert.True(t, final)
}

func TestNLockTimeIsFinalHeightBased(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.height = 100
	s := newTestServices(t, newFakeOracle(), hdrs)

	final, err := s.NLockTimeIsFinal(context.Background(), uint32(50))
	require.NoError(t, err)
	assert.True(t, final, "a locktime below the current height is final")

	final, err = s.NLockTimeIsFinal(context.Background(), uint32(150))
	require.NoError(t, err)
	assert.False(t, final, "a locktime above the current height is not final")
}

func TestNLockTimeIsFinalTimestampBased(t *testing.T) {
	s := newTestServices(t, newFakeOracle(), newFakeHeaders())

	// Values >= 500,000,000 are interpreted as a unix timestamp, not a height.
	past := uint32(500_000_001)
	final, err := s.NLockTimeIsFinal(context.Background(), past)
	require.NoError(t, err)
	assert.True(t, final, "a timestamp in the past is final")

	future, err := to32(time.Now().Add(24 * time.Hour).Unix())
	require.NoError(t, err)
	final, err = s.NLockTimeIsFinal(context.Background(), future)
	require.NoError(t, err)
	assert.False(t, final, "a timestamp in the future is not final")
}

func TestNLockTimeIsFinalTransactionWithMaxSequenceIsAlwaysFinal(t *testing.T) {
	hdrs := newFakeHeaders()
	hdrs.height = 0 // even at height 0, a nonzero-but-irrelevant locktime is
	// overridden by every input being final (sequence == MaxUint32).
	s := newTestServices(t, newFakeOracle(), hdrs)

	tx := sdk.NewTransaction()
	tx.LockTime = 500 // would otherwise not be final at height 0
	tx.AddInput(&sdk.TransactionInput{SequenceNumber: 0xFFFFFFFF})

	final, err := s.NLockTimeIsFinal(context.Background(), tx)
	require.NoError(t, err)
	assert.True(t, final)
}

func to32(v int64) (uint32, error) {
	return uint32(v), nil //nolint:gosec // test helper, values are small and controlled
}
