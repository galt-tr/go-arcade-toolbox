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
// (arcade.StatusReceived) and has a verdict for nothing; set BroadcastFunc to
// simulate a different broadcast outcome (rejection, backpressure, an opaque
// error) and ScriptTx to give GetTx an answer. Safe for concurrent use.
type FakeOracle struct {
	// BroadcastFunc overrides Broadcast's response; nil accepts every
	// broadcast.
	BroadcastFunc func(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error)

	// GetTxFunc overrides GetTx entirely. Prefer ScriptTx unless a test needs to
	// simulate the oracle being unreachable, which is its own branch: "we could
	// not ask" is not the same answer as "no such transaction".
	GetTxFunc func(ctx context.Context, txid string) (*arcade.TxRecord, error)

	mu      sync.Mutex
	calls   int
	getCall int
	scripts map[string]arcade.TxRecord
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

// ScriptTx gives GetTx a verdict for one transaction, replacing any previous
// one. Safe to call while the provider is running.
//
// Without this the fake could only ever answer ErrTxNotFound, which made the
// exported harness unable to express the single most important thing a
// consumer has to survive: arcade REJECTING a transaction. The reconciler's
// whole release path keys off that verdict, so a rejection could not be tested
// through the public seam at all.
func (f *FakeOracle) ScriptTx(txid string, status arcade.Status, opts ...func(*arcade.TxRecord)) {
	rec := arcade.TxRecord{TxID: txid, Status: status}
	for _, o := range opts {
		o(&rec)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.scripts == nil {
		f.scripts = map[string]arcade.TxRecord{}
	}
	f.scripts[txid] = rec
}

// WithExtraInfo attaches arcade's reason text to a scripted record. The
// reconciler parses this for spend conflicts, so its exact shape matters — see
// the UTXO_SPENT forms in spend_conflict.go.
func WithExtraInfo(info string) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) { r.ExtraInfo = info }
}

// WithCompeting attaches the competing txids arcade reports for a double spend.
func WithCompeting(txids ...string) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) { r.CompetingTxs = txids }
}

// GetTxCalls reports how many times GetTx has been invoked so far.
func (f *FakeOracle) GetTxCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCall
}

// GetTx implements [arcade.TxOracle]. It answers from ScriptTx, and reports
// ErrTxNotFound for anything unscripted.
func (f *FakeOracle) GetTx(ctx context.Context, txid string) (*arcade.TxRecord, error) {
	f.mu.Lock()
	f.getCall++
	get, rec, ok := f.GetTxFunc, arcade.TxRecord{}, false
	if f.scripts != nil {
		rec, ok = f.scripts[txid]
	}
	f.mu.Unlock()

	if get != nil {
		return get(ctx, txid)
	}
	if !ok {
		return nil, arcade.ErrTxNotFound
	}
	out := rec // copy: callers must not be able to mutate the script
	return &out, nil
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
