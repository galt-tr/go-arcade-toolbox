package storage_test

// Unit tests for the provider-side async status hooks (status_updates.go): the
// monitor's ApplyStatusUpdate entry point and the sweep methods. These use a
// direct metastore (SQLite temp file) + memstore harness with a controllable
// fake oracle and fake headers, so they can seed known_tx / transaction /
// output / utxo rows and assert the exact wallet-state transitions (statuses,
// tiers, stored proofs, suspect marking).

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/monitor"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// Compile-time assertion that the storage.Provider satisfies the monitor's
// MonitoredStorage contract (kept in the test package so production storage
// does not import monitor).
var _ monitor.MonitoredStorage = (*storage.Provider)(nil)

// --- fakes -----------------------------------------------------------------

type fakeOracle struct {
	broadcast  func(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error)
	getTx      func(ctx context.Context, txid string) (*arcade.TxRecord, error)
	broadcasts atomic.Int32
}

func (f *fakeOracle) Broadcast(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	f.broadcasts.Add(1)
	if f.broadcast != nil {
		return f.broadcast(ctx, txid, ef)
	}
	return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusSeenOnNetwork}, nil
}

func (f *fakeOracle) GetTx(ctx context.Context, txid string) (*arcade.TxRecord, error) {
	if f.getTx != nil {
		return f.getTx(ctx, txid)
	}
	return nil, arcade.ErrTxNotFound
}

func (f *fakeOracle) StreamStatus(ctx context.Context, _ string, _ func(arcade.StatusEvent) error) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeOracle) Health(context.Context) (*arcade.Health, error) {
	return &arcade.Health{Healthy: true}, nil
}

type fakeHeaders struct {
	roots       map[uint32]chainhash.Hash
	verifyCalls atomic.Int32
}

func newFakeHeaders() *fakeHeaders { return &fakeHeaders{roots: map[uint32]chainhash.Hash{}} }

func (f *fakeHeaders) register(height uint32, root chainhash.Hash) { f.roots[height] = root }

func (f *fakeHeaders) CurrentHeight(context.Context) (uint32, error) {
	var maxH uint32
	for h := range f.roots {
		if h > maxH {
			maxH = h
		}
	}
	return maxH, nil
}

func (f *fakeHeaders) HeaderByHeight(_ context.Context, height uint32) (*headers.Header, error) {
	root, ok := f.roots[height]
	if !ok {
		return nil, fmt.Errorf("no header at height %d", height)
	}
	var hash chainhash.Hash
	hash[0] = byte(height)
	hash[1] = 0xbb
	return &headers.Header{Height: height, MerkleRoot: root, Hash: hash}, nil
}

func (f *fakeHeaders) VerifyMerkleRoot(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	f.verifyCalls.Add(1)
	want, ok := f.roots[height]
	if !ok {
		return false, fmt.Errorf("no header at height %d", height)
	}
	return want.IsEqual(root), nil
}

type stubScripts struct{}

func (stubScripts) VerifyScripts(context.Context, *transaction.Transaction) (bool, error) {
	return true, nil
}

// --- harness ---------------------------------------------------------------

type hookStack struct {
	t      *testing.T
	p      *storage.Provider
	meta   *metastore.Store
	utxo   utxostore.Store
	oracle *fakeOracle
	hdrs   *fakeHeaders
	userID int
	refSeq int
	now    time.Time // provider clock; advanceable to make rows look stale
}

func newHookStack(t *testing.T) *hookStack {
	t.Helper()
	ctx := context.Background()
	logger := logging.NewTestLogger(t)

	meta, err := metastore.OpenSQLite(ctx, t.TempDir()+"/meta.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New()
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &fakeOracle{}
	hdrs := newFakeHeaders()

	h := &hookStack{t: t, meta: meta, utxo: utxo, oracle: oracle, hdrs: hdrs, now: time.Now()}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithScriptsVerifier(stubScripts{}),
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("hook-test"),
		storage.WithClock(func() time.Time { return h.now }),
	)
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "hook-test", "hook-identity-key")
	require.NoError(t, err)

	resp, err := p.FindOrInsertUser(ctx, "02"+chainhash.Hash{}.String()[2:])
	require.NoError(t, err)

	h.p = p
	h.userID = resp.User.UserID
	return h
}

func newTxID(seed byte) string {
	var h chainhash.Hash
	for i := range h {
		h[i] = seed
	}
	return h.String()
}

func mustHash(t *testing.T, txid string) chainhash.Hash {
	t.Helper()
	h, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)
	return *h
}

// seedChangeTx creates a transaction row + one change output + a known_tx and
// mints the change coin at tier. It returns the change outpoint.
func (h *hookStack) seedChangeTx(txid string, txStatus wdk.TxStatus, ktStatus wdk.ProvenTxReqStatus, sats uint64, tier utxostore.Tier) utxostore.Outpoint {
	h.t.Helper()
	ctx := context.Background()
	require.NoError(h.t, h.meta.Baskets().FindOrCreate(ctx, h.userID, wdk.BasketNameForChange))

	h.refSeq++
	txID, err := h.meta.Transactions().Insert(ctx, metastore.NewTx{
		UserID:     h.userID,
		Status:     txStatus,
		Reference:  fmt.Sprintf("ref-%s-%d", txid[:8], h.refSeq),
		IsOutgoing: true,
		Satoshis:   int64(sats),
	})
	require.NoError(h.t, err)
	require.NoError(h.t, h.meta.Transactions().SetTxID(ctx, txID, txid))

	basket := wdk.BasketNameForChange
	_, err = h.meta.Outputs().Insert(ctx, metastore.NewOutput{
		UserID: h.userID, TransactionID: txID, Vout: 0, Satoshis: int64(sats),
		Basket: &basket, Change: true, Type: "P2PKH", ProvidedBy: "storage", Purpose: "change",
	})
	require.NoError(h.t, err)

	require.NoError(h.t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: txid, Status: ktStatus}))

	hash := mustHash(h.t, txid)
	op := utxostore.Outpoint{TxID: hash, Vout: 0}
	m := &utxostore.Mint{Outpoint: op, UserID: int64(h.userID), Basket: wdk.BasketNameForChange, Satoshis: sats, InputSize: utxostore.DefaultP2PKHInputSize, Tier: tier}
	require.NoError(h.t, h.utxo.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(h.t, m.Err)
	return op
}

func (h *hookStack) tier(op utxostore.Outpoint) utxostore.Tier {
	h.t.Helper()
	u, err := h.utxo.Get(context.Background(), op)
	require.NoError(h.t, err)
	return u.Tier
}

func (h *hookStack) knownTx(txid string) *metastore.KnownTx {
	h.t.Helper()
	kt, found, err := h.meta.KnownTx().FindByTxID(context.Background(), txid)
	require.NoError(h.t, err)
	require.True(h.t, found, "known tx %s must exist", txid)
	return kt
}

func (h *hookStack) txStatus(txid string) wdk.TxStatus {
	h.t.Helper()
	rows, err := h.meta.Transactions().FindByTxIDAllUsers(context.Background(), txid)
	require.NoError(h.t, err)
	require.NotEmpty(h.t, rows)
	return rows[0].Status
}

// minedRecord builds a MINED TxRecord whose single-leaf BUMP computes to a root
// equal to the txid, plus that root (register it in fakeHeaders to make the
// proof verify).
func minedRecord(t *testing.T, txid string, height uint32) (arcade.TxRecord, chainhash.Hash) {
	t.Helper()
	h := mustHash(t, txid)
	trueVal := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: &h, Txid: &trueVal}},
	})
	root, err := mp.ComputeRoot(&h)
	require.NoError(t, err)
	return arcade.TxRecord{TxID: txid, Status: arcade.StatusMined, BlockHeight: uint64(height), MerklePath: mp.Bytes()}, *root
}

// --- ApplyStatusUpdate tests -----------------------------------------------

func TestApplyStatusUpdate_Seen(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x11)
	op := h.seedChangeTx(txid, wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed, 5000, utxostore.TierSending)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))

	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(txid))
	kt := h.knownTx(txid)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, kt.Status)
	require.NotNil(t, kt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusSeenOnNetwork), *kt.ArcadeStatus)
	require.Equal(t, utxostore.TierUnproven, h.tier(op), "change promoted to unproven")
}

func TestApplyStatusUpdate_Mined(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x22)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	const height = uint32(850000)
	rec, root := minedRecord(t, txid, height)
	h.hdrs.register(height, root)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	kt := h.knownTx(txid)
	require.Equal(t, wdk.ProvenTxStatusCompleted, kt.Status)
	require.NotEmpty(t, kt.MerklePath, "BUMP stored")
	require.NotEmpty(t, kt.MerkleRoot, "computed root stored")
	require.NotEmpty(t, kt.BlockHash, "block hash stored (from headers)")
	require.NotNil(t, kt.BlockHeight)
	require.Equal(t, height, *kt.BlockHeight)
	require.Equal(t, utxostore.TierMined, h.tier(op), "change promoted to mined")
}

func TestApplyStatusUpdate_MinedBadRoot_NotStored(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x33)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	const height = uint32(850001)
	rec, _ := minedRecord(t, txid, height)
	// Register a DIFFERENT root at the height: VerifyMerkleRoot returns false.
	var wrong chainhash.Hash
	for i := range wrong {
		wrong[i] = 0xff
	}
	h.hdrs.register(height, wrong)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec), "a bad proof is skipped, not an error")

	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(txid), "tx stays unproven on a bad proof")
	kt := h.knownTx(txid)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, kt.Status, "known tx not completed")
	require.Empty(t, kt.MerklePath, "no proof stored for a bad root")
	require.Equal(t, utxostore.TierUnproven, h.tier(op), "change not promoted to mined")
}

// TestApplyStatusUpdate_RejectedThenMined_Recovers proves the dangerous
// recovery path at the real provider: a suspect-marked (async-rejected) tx that
// a later MINED supersedes ends up completed with change at TierMined — arcade's
// lattice lets a peer's acceptance recover a locally-rejected tx. This is the
// per-txid arrival-order outcome the batch sharding guarantees (scenario c).
func TestApplyStatusUpdate_RejectedThenMined_Recovers(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x4C)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	// Earlier arrival: REJECTED → suspectFailed.
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txid).Status)

	// Later arrival: MINED supersedes the suspect state and completes the tx.
	const height = uint32(861234)
	rec, root := minedRecord(t, txid, height)
	h.hdrs.register(height, root)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status)
	require.Equal(t, utxostore.TierMined, h.tier(op))
}

func TestApplyStatusUpdate_Rejected_MarksSuspectNoRelease(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x44)
	h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	// Seed a reserved input coin under a reservation, to prove reject does NOT
	// release it (that is the M4.2 reconciler's job).
	reservation := "resv-reject"
	var inHash chainhash.Hash
	inHash[0] = 0x99
	inOp := utxostore.Outpoint{TxID: inHash, Vout: 3}
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{{Outpoint: inOp, UserID: int64(h.userID), Basket: "funding", Satoshis: 10000, InputSize: 107, Tier: utxostore.TierUnproven}}))
	claimed, err := h.utxo.ClaimSmallestSufficient(ctx, utxostore.Scope{UserID: int64(h.userID), Basket: "funding", Tier: utxostore.TierUnproven}, reservation, 1)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	competing := []string{newTxID(0xAA), newTxID(0xBB)}
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusDoubleSpendAttempted, CompetingTxs: competing}))

	kt := h.knownTx(txid)
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, kt.Status)
	require.Equal(t, competing, kt.CompetingTxs)
	require.NotNil(t, kt.SuspectSince)
	require.NotNil(t, kt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusDoubleSpendAttempted), *kt.ArcadeStatus)

	// The reserved input is untouched: still reserved by the same token, unspent.
	inUTXO, err := h.utxo.Get(ctx, inOp)
	require.NoError(t, err)
	require.Equal(t, reservation, inUTXO.ReservedBy, "inputs must NOT be released by the async reject path")
	require.Nil(t, inUTXO.SpentBy)
}

func TestApplyStatusUpdate_TerminalGuard_SeenAfterMinedIsNoop(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x55)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	const height = uint32(860000)
	rec, root := minedRecord(t, txid, height)
	h.hdrs.register(height, root)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	require.Equal(t, utxostore.TierMined, h.tier(op))

	// A late SEEN must NOT downgrade the mined tx.
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid), "still completed")
	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status)
	require.Equal(t, utxostore.TierMined, h.tier(op), "tier not downgraded")
}

func TestApplyStatusUpdate_Mined_Idempotent(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x66)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)

	const height = uint32(870000)
	rec, root := minedRecord(t, txid, height)
	h.hdrs.register(height, root)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec), "second apply is a no-op")

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	require.Equal(t, utxostore.TierMined, h.tier(op))
	require.EqualValues(t, 1, h.hdrs.verifyCalls.Load(), "verification runs once; the re-apply short-circuits at the terminal guard")
}

func TestApplyStatusUpdate_UnknownTxid_Noop(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: newTxID(0x77), Status: arcade.StatusMined}))
}

// --- sweep tests -----------------------------------------------------------

func TestAbortAbandoned(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x88)
	// A never-broadcast unsigned tx with a reserved input and minted change.
	op := h.seedChangeTx(txid, wdk.TxStatusUnsigned, wdk.ProvenTxStatusUnsent, 4000, utxostore.TierSending)
	rows, err := h.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	require.NoError(t, err)
	reservation := string(rows[0].Reference)
	// Reserve an input under the tx reference so abort can release it.
	var inHash chainhash.Hash
	inHash[0] = 0x8f
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{{Outpoint: utxostore.Outpoint{TxID: inHash, Vout: 1}, UserID: int64(h.userID), Basket: "funding", Satoshis: 9000, InputSize: 107, Tier: utxostore.TierMined}}))
	_, err = h.utxo.ClaimSmallestSufficient(ctx, utxostore.Scope{UserID: int64(h.userID), Basket: "funding", Tier: utxostore.TierMined}, reservation, 1)
	require.NoError(t, err)

	require.NoError(t, h.p.AbortAbandoned(ctx, time.Now().Add(time.Hour), 10))

	require.Equal(t, wdk.TxStatusAborted, h.txStatus(txid))
	// Change coin removed.
	_, err = h.utxo.Get(ctx, op)
	require.Error(t, err, "minted change removed on abort")
	// Reserved input released.
	inUTXO, err := h.utxo.Get(ctx, utxostore.Outpoint{TxID: inHash, Vout: 1})
	require.NoError(t, err)
	require.Empty(t, inUTXO.ReservedBy, "reserved input released on abort")
}

func TestSynchronizeTransactionStatuses_PromotesMined(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)
	txid := newTxID(0x99)
	op := h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 6000, utxostore.TierUnproven)

	const height = uint32(880000)
	rec, root := minedRecord(t, txid, height)
	h.hdrs.register(height, root)
	// GetTx returns the mined record with the BUMP.
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		if id == txid {
			r := rec
			return &r, nil
		}
		return nil, arcade.ErrTxNotFound
	}
	// Advance the provider clock past the staleness threshold so the unconfirmed
	// known tx is eligible for the poll.
	h.now = h.now.Add(5 * time.Minute)

	require.NoError(t, h.p.SynchronizeTransactionStatuses(ctx, 10))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	require.Equal(t, utxostore.TierMined, h.tier(op))
}

func TestSendWaitingTransactions(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	// Build a real spendable tx: srcTx (proven, so it is a valid standalone BEEF
	// leaf) funds mainTx's single input, so mainTx.EF() works. rawTx =
	// mainTx.Bytes(); input_beef carries srcTx for hydration.
	srcTx := transaction.NewTransaction()
	var srcParent chainhash.Hash
	srcParent[0] = 0x01
	srcTx.AddInput(&transaction.TransactionInput{SourceTXID: &srcParent, SourceTxOutIndex: 0, SequenceNumber: transaction.DefaultSequenceNumber})
	srcTx.AddOutput(&transaction.TransactionOutput{Satoshis: 10000, LockingScript: trueScript()})
	srcTxID := srcTx.TxID()
	srcTrue := true
	require.NoError(t, srcTx.AddMerkleProof(transaction.NewMerklePath(100, [][]*transaction.PathElement{
		{{Offset: 0, Hash: srcTxID, Txid: &srcTrue}},
	})))

	mainTx := transaction.NewTransaction()
	mainTx.AddInput(&transaction.TransactionInput{SourceTXID: srcTxID, SourceTxOutIndex: 0, SequenceNumber: transaction.DefaultSequenceNumber})
	mainTx.Inputs[0].SourceTransaction = srcTx
	mainTx.AddOutput(&transaction.TransactionOutput{Satoshis: 6000, LockingScript: trueScript()})
	mainTxID := mainTx.TxID().String()

	beef := transaction.NewBeefV2()
	_, err := beef.MergeTransaction(srcTx)
	require.NoError(t, err)
	beefBytes, err := beef.Bytes()
	require.NoError(t, err)

	// Seed: a delayed (unsent) known tx + a change output + a reserved input coin.
	op := h.seedChangeTx(mainTxID, wdk.TxStatusUnprocessed, wdk.ProvenTxStatusUnsent, 6000, utxostore.TierSending)
	require.NoError(t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: mainTxID, Status: wdk.ProvenTxStatusUnsent, RawTx: mainTx.Bytes(), InputBEEF: beefBytes}))
	rows, err := h.meta.Transactions().FindByTxIDAllUsers(ctx, mainTxID)
	require.NoError(t, err)
	reservation := string(rows[0].Reference)
	// The input coin (srcTx:0) reserved under the tx reference.
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{{Outpoint: utxostore.Outpoint{TxID: *srcTxID, Vout: 0}, UserID: int64(h.userID), Basket: "funding", Satoshis: 10000, InputSize: 107, Tier: utxostore.TierMined}}))
	_, err = h.utxo.ClaimSmallestSufficient(ctx, utxostore.Scope{UserID: int64(h.userID), Basket: "funding", Tier: utxostore.TierMined}, reservation, 1)
	require.NoError(t, err)

	h.oracle.broadcast = func(_ context.Context, _ string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{TxID: mainTxID, Status: arcade.StatusSeenOnNetwork}, nil
	}

	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))

	require.EqualValues(t, 1, h.oracle.broadcasts.Load(), "the waiting tx was broadcast once")
	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(mainTxID))
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(mainTxID).Status)
	require.Equal(t, utxostore.TierUnproven, h.tier(op), "change promoted to unproven")
	// The reserved input flipped to spent by the main tx.
	inUTXO, err := h.utxo.Get(ctx, utxostore.Outpoint{TxID: *srcTxID, Vout: 0})
	require.NoError(t, err)
	require.NotNil(t, inUTXO.SpentBy, "reserved input spent on acceptance")
}

// trueScript returns a tiny non-empty locking script (OP_TRUE) for the
// synthetic transactions built in TestSendWaitingTransactions.
func trueScript() *script.Script {
	s := script.Script([]byte{0x51})
	return &s
}
