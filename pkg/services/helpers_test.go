package services

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
)

// arcadeRecord is a small builder for oracle-side fixtures.
func arcadeRecord(txid string, status arcade.Status) arcade.TxRecord {
	return arcade.TxRecord{TxID: txid, Status: status}
}

// --- fakes -----------------------------------------------------------------

// fakeOracle is a controllable arcade.TxOracle. The zero value has no known
// transactions (GetTx always misses) and accepts every broadcast.
type fakeOracle struct {
	mu             sync.Mutex
	txs            map[string]*arcade.TxRecord
	broadcastFunc  func(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error)
	broadcastCalls int
	getTxCalls     map[string]int
	// getTxFunc, when set, fully overrides GetTx's default map-lookup
	// behavior (used to simulate an opaque transport error).
	getTxFunc func(ctx context.Context, txid string) (*arcade.TxRecord, error)
}

var _ arcade.TxOracle = (*fakeOracle)(nil)

func newFakeOracle() *fakeOracle {
	return &fakeOracle{
		txs:        make(map[string]*arcade.TxRecord),
		getTxCalls: make(map[string]int),
	}
}

func (f *fakeOracle) setTx(rec arcade.TxRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.txs[rec.TxID] = &rec
}

func (f *fakeOracle) Broadcast(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	f.mu.Lock()
	f.broadcastCalls++
	fn := f.broadcastFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, txid, ef)
	}
	return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusReceived}, nil
}

func (f *fakeOracle) GetTx(ctx context.Context, txid string) (*arcade.TxRecord, error) {
	f.mu.Lock()
	f.getTxCalls[txid]++
	fn := f.getTxFunc
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, txid)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.txs[txid]
	if !ok {
		return nil, arcade.ErrTxNotFound
	}
	cp := *rec
	return &cp, nil
}

func (f *fakeOracle) getTxCallCount(txid string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getTxCalls[txid]
}

func (f *fakeOracle) StreamStatus(context.Context, string, func(arcade.StatusEvent) error) error {
	return nil
}

func (f *fakeOracle) Health(context.Context) (*arcade.Health, error) {
	return &arcade.Health{Healthy: true}, nil
}

// fakeHeaders is a controllable headers.Headers.
type fakeHeaders struct {
	mu         sync.Mutex
	height     uint32
	heightErr  error
	byHeight   map[uint32]*headers.Header
	verifyFunc func(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error)
}

var _ headers.Headers = (*fakeHeaders)(nil)

func newFakeHeaders() *fakeHeaders {
	return &fakeHeaders{byHeight: make(map[uint32]*headers.Header)}
}

func (f *fakeHeaders) CurrentHeight(context.Context) (uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heightErr != nil {
		return 0, f.heightErr
	}
	return f.height, nil
}

func (f *fakeHeaders) HeaderByHeight(_ context.Context, height uint32) (*headers.Header, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if h, ok := f.byHeight[height]; ok {
		return h, nil
	}
	return &headers.Header{Height: height}, nil
}

func (f *fakeHeaders) VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	f.mu.Lock()
	fn := f.verifyFunc
	f.mu.Unlock()
	if fn == nil {
		return true, nil
	}
	return fn(ctx, root, height)
}

// fakeRawTxSource is a controllable RawTxSource.
type fakeRawTxSource struct {
	mu    sync.Mutex
	data  map[string][]byte
	err   error
	calls int
}

var _ RawTxSource = (*fakeRawTxSource)(nil)

func newFakeRawTxSource() *fakeRawTxSource {
	return &fakeRawTxSource{data: make(map[string][]byte)}
}

func (f *fakeRawTxSource) set(txid string, raw []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[txid] = raw
}

func (f *fakeRawTxSource) LocalRawTx(_ context.Context, txid string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, false, f.err
	}
	raw, ok := f.data[txid]
	return raw, ok, nil
}

// --- transaction builders ---------------------------------------------------

func testP2PKH(t *testing.T) *script.Script {
	t.Helper()
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), false)
	require.NoError(t, err)
	ls, err := p2pkh.Lock(addr)
	require.NoError(t, err)
	return ls
}

// buildChainedTx returns a parent transaction (one P2PKH output, deliberately
// with NO inputs — a root the BEEF ancestry walk cannot descend past) and a
// child transaction spending the parent's output, along with a Beef
// containing both — wired so that beef.FindTransactionForSigning(child's
// txid) succeeds (the parent is present, so the child's direct
// SourceTransaction resolves).
func buildChainedTx(t *testing.T) (parent, child *transaction.Transaction, beef *transaction.Beef) {
	t.Helper()

	parent = transaction.NewTransaction()
	parent.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: testP2PKH(t)})

	child = transaction.NewTransaction()
	child.AddInput(&transaction.TransactionInput{
		SourceTXID:       parent.TxID(),
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
	})
	child.AddOutput(&transaction.TransactionOutput{Satoshis: 900, LockingScript: testP2PKH(t)})

	beef = transaction.NewBeefV2()
	_, err := beef.MergeTransaction(parent)
	require.NoError(t, err)
	_, err = beef.MergeTransaction(child)
	require.NoError(t, err)

	return parent, child, beef
}

// buildThreeLevelChain returns grandparent (no inputs) -> parent -> child,
// each spending the previous transaction's sole output. Used to exercise
// GetBEEF's max-depth bound, which buildChainedTx's two-level fixture is too
// shallow for.
func buildThreeLevelChain(t *testing.T) (grandparent, parent, child *transaction.Transaction) {
	t.Helper()

	grandparent = transaction.NewTransaction()
	grandparent.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: testP2PKH(t)})

	parent = transaction.NewTransaction()
	parent.AddInput(&transaction.TransactionInput{
		SourceTXID:       grandparent.TxID(),
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
	})
	parent.AddOutput(&transaction.TransactionOutput{Satoshis: 900, LockingScript: testP2PKH(t)})

	child = transaction.NewTransaction()
	child.AddInput(&transaction.TransactionInput{
		SourceTXID:       parent.TxID(),
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
	})
	child.AddOutput(&transaction.TransactionOutput{Satoshis: 800, LockingScript: testP2PKH(t)})

	return grandparent, parent, child
}

// buildSimpleTx returns a standalone transaction with a directly-set
// SourceTransaction on its one input (so EF() succeeds without needing a
// Beef) and one P2PKH output.
func buildSimpleTx(t *testing.T) *transaction.Transaction {
	t.Helper()

	source := transaction.NewTransaction()
	var seedHash chainhash.Hash
	seedHash[0] = 0xBB
	source.AddInput(&transaction.TransactionInput{
		SourceTXID:       &seedHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
		UnlockingScript:  script.NewFromBytes([]byte{0x00}),
	})
	source.AddOutput(&transaction.TransactionOutput{Satoshis: 2000, LockingScript: testP2PKH(t)})

	tx := transaction.NewTransaction()
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:        source.TxID(),
		SourceTransaction: source,
		SourceTxOutIndex:  0,
		SequenceNumber:    transaction.DefaultSequenceNumber,
		UnlockingScript:   script.NewFromBytes([]byte{0x00}),
	})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1800, LockingScript: testP2PKH(t)})

	return tx
}
