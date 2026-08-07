package conformance

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
)

// FakeOracle is a minimal, controllable [arcade.TxOracle] for provider
// conformance harnesses. The zero value accepts every broadcast
// (arcade.StatusReceived); set BroadcastFunc to simulate a different outcome
// (rejection, backpressure, an opaque error). Safe for concurrent use.
type FakeOracle struct {
	// BroadcastFunc overrides Broadcast's response; nil accepts every
	// broadcast.
	BroadcastFunc func(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error)

	mu    sync.Mutex
	calls int
}

var _ arcade.TxOracle = (*FakeOracle)(nil)

// Broadcast implements [arcade.TxOracle].
func (f *FakeOracle) Broadcast(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.BroadcastFunc == nil {
		return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusReceived}, nil
	}
	return f.BroadcastFunc(ctx, txid, ef)
}

// Calls reports how many times Broadcast has been invoked so far.
func (f *FakeOracle) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// GetTx implements [arcade.TxOracle]; the fake never has a verdict.
func (f *FakeOracle) GetTx(context.Context, string) (*arcade.TxRecord, error) {
	return nil, arcade.ErrTxNotFound
}

// StreamStatus implements [arcade.TxOracle] as a no-op.
func (f *FakeOracle) StreamStatus(context.Context, string, func(arcade.StatusEvent) error) error {
	return nil
}

// Health implements [arcade.TxOracle], always healthy.
func (f *FakeOracle) Health(context.Context) (*arcade.Health, error) {
	return &arcade.Health{Healthy: true}, nil
}

// FakeHeaders is a minimal, controllable [headers.Headers] for provider
// conformance harnesses. The zero value accepts every merkle root (so ordinary
// mined BEEFs verify); set VerifyFunc to simulate a headers source that does
// NOT recognize a given root at a given height — the "bad BUMP" case — for
// [WithRejectingHeadersProvider].
type FakeHeaders struct {
	// VerifyFunc overrides VerifyMerkleRoot; nil accepts every root.
	VerifyFunc func(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error)
	// Height is the fake chain tip returned by CurrentHeight.
	Height uint32
	// Hash is the header hash returned by HeaderByHeight for every height.
	Hash chainhash.Hash
}

var _ headers.Headers = (*FakeHeaders)(nil)

// CurrentHeight implements [headers.Headers].
func (f *FakeHeaders) CurrentHeight(context.Context) (uint32, error) { return f.Height, nil }

// HeaderByHeight implements [headers.Headers].
func (f *FakeHeaders) HeaderByHeight(_ context.Context, height uint32) (*headers.Header, error) {
	return &headers.Header{Height: height, Hash: f.Hash}, nil
}

// VerifyMerkleRoot implements [headers.Headers].
func (f *FakeHeaders) VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	if f.VerifyFunc == nil {
		return true, nil
	}
	return f.VerifyFunc(ctx, root, height)
}

// RejectingHeaders returns a [FakeHeaders] whose VerifyMerkleRoot always
// reports false — a headers source that never recognizes a given root at a
// given height, standing in for a bad BUMP. Convenience for wiring
// [WithRejectingHeadersProvider].
func RejectingHeaders() *FakeHeaders {
	return &FakeHeaders{VerifyFunc: func(context.Context, *chainhash.Hash, uint32) (bool, error) {
		return false, nil
	}}
}

// AlwaysValidScripts is a [wdk.ScriptsVerifier] that accepts every
// transaction, so conformance harnesses can build unsigned/dummy-signed
// transactions without real keys.
type AlwaysValidScripts struct{}

// VerifyScripts always reports success.
func (AlwaysValidScripts) VerifyScripts(context.Context, *transaction.Transaction) (bool, error) {
	return true, nil
}
