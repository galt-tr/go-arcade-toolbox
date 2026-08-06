package headers

import (
	"context"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubHeaders is a minimal in-memory implementation used to prove the contracts
// are coherent and implementable, and to exercise the documented LOCAL
// VerifyMerkleRoot semantics (fetch header for height, byte-compare roots).
type stubHeaders struct {
	byHeight map[uint32]*Header
}

func (s *stubHeaders) CurrentHeight(context.Context) (uint32, error) {
	var maxH uint32
	for h := range s.byHeight {
		if h > maxH {
			maxH = h
		}
	}
	return maxH, nil
}

func (s *stubHeaders) HeaderByHeight(_ context.Context, height uint32) (*Header, error) {
	h, ok := s.byHeight[height]
	if !ok {
		return nil, errors.New("unknown height")
	}
	return h, nil
}

// VerifyMerkleRoot implements the documented local-verification behavior.
func (s *stubHeaders) VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	h, err := s.HeaderByHeight(ctx, height)
	if err != nil {
		return false, err
	}
	return h.MerkleRoot.IsEqual(root), nil
}

func (s *stubHeaders) SubscribeTip(ctx context.Context) <-chan TipEvent {
	ch := make(chan TipEvent)
	go func() { <-ctx.Done(); close(ch) }()
	return ch
}

func (s *stubHeaders) SubscribeReorg(ctx context.Context) <-chan ReorgEvent {
	ch := make(chan ReorgEvent)
	go func() { <-ctx.Done(); close(ch) }()
	return ch
}

// Compile-time assertions that the contracts are implementable.
var (
	_ Headers         = (*stubHeaders)(nil)
	_ ChainSubscriber = (*stubHeaders)(nil)
)

func TestVerifyMerkleRoot_Local(t *testing.T) {
	root := chainhash.Hash{0x01, 0x02, 0x03}
	other := chainhash.Hash{0xff}
	h := &stubHeaders{byHeight: map[uint32]*Header{
		800000: {Height: 800000, MerkleRoot: root},
	}}

	ok, err := h.VerifyMerkleRoot(context.Background(), &root, 800000)
	require.NoError(t, err)
	assert.True(t, ok, "matching root should verify")

	ok, err = h.VerifyMerkleRoot(context.Background(), &other, 800000)
	require.NoError(t, err)
	assert.False(t, ok, "non-matching root should fail")

	_, err = h.VerifyMerkleRoot(context.Background(), &root, 999999)
	assert.Error(t, err, "unknown height should error")
}

func TestSubscribeChannelsCloseOnCancel(t *testing.T) {
	h := &stubHeaders{}
	ctx, cancel := context.WithCancel(context.Background())
	tip := h.SubscribeTip(ctx)
	reorg := h.SubscribeReorg(ctx)
	cancel()
	_, tipOpen := <-tip
	_, reorgOpen := <-reorg
	assert.False(t, tipOpen, "tip channel should close on cancel")
	assert.False(t, reorgOpen, "reorg channel should close on cancel")
}
