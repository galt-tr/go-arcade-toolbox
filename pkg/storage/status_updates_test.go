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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/monitor"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
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
	// verifyDelay simulates the chaintracks round-trip a real VerifyMerkleRoot
	// makes, so a test can prove concurrent verifications are not serialized.
	verifyDelay time.Duration
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
	if f.verifyDelay > 0 {
		time.Sleep(f.verifyDelay)
	}
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

// --- ApplyStatusBatch tests ------------------------------------------------

// TestApplyStatusBatch_StaleSeenOnCompletedIsNoop applies, in ONE batch, a fresh
// SEEN for one txid and a stale SEEN for a DIFFERENT txid whose row is already
// completed. The batched terminal guard must make the stale one a no-op (no tier
// downgrade, no status regression) while the fresh one advances normally — the
// batch equivalent of the per-event terminal guard.
func TestApplyStatusBatch_StaleSeenOnCompletedIsNoop(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	fresh := newTxID(0xA1)
	freshOp := h.seedChangeTx(fresh, wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed, 5000, utxostore.TierSending)

	stale := newTxID(0xA2)
	staleOp := h.seedChangeTx(stale, wdk.TxStatusCompleted, wdk.ProvenTxStatusCompleted, 7000, utxostore.TierMined)
	require.NoError(t, h.meta.KnownTx().SetArcadeStatus(ctx, stale, string(arcade.StatusMined)))

	require.NoError(t, h.p.ApplyStatusBatch(ctx, []arcade.TxRecord{
		{TxID: fresh, Status: arcade.StatusSeenOnNetwork},
		{TxID: stale, Status: arcade.StatusSeenOnNetwork},
	}))

	// The fresh SEEN advanced.
	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(fresh))
	freshKt := h.knownTx(fresh)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, freshKt.Status)
	require.NotNil(t, freshKt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusSeenOnNetwork), *freshKt.ArcadeStatus)
	require.Equal(t, utxostore.TierUnproven, h.tier(freshOp))

	// The stale SEEN on a completed tx is fully a no-op: no regression anywhere.
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(stale), "completed tx not regressed")
	staleKt := h.knownTx(stale)
	require.Equal(t, wdk.ProvenTxStatusCompleted, staleKt.Status)
	require.NotNil(t, staleKt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusMined), *staleKt.ArcadeStatus, "arcade_status not regressed")
	require.Equal(t, utxostore.TierMined, h.tier(staleOp), "mined tier not downgraded")
}

// TestApplyStatusBatch_SeenAndMinedTogether applies a SEEN (one txid) and a
// MINED (another txid) in the SAME batch and asserts both land: the SEEN tx to
// unproven/TierUnproven, the MINED tx to completed/TierMined with a stored,
// header-verified proof.
func TestApplyStatusBatch_SeenAndMinedTogether(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	seenTx := newTxID(0xB1)
	seenOp := h.seedChangeTx(seenTx, wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed, 5000, utxostore.TierSending)

	minedTx := newTxID(0xB2)
	minedOp := h.seedChangeTx(minedTx, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)
	const height = uint32(900001)
	minedRec, root := minedRecord(t, minedTx, height)
	h.hdrs.register(height, root)

	require.NoError(t, h.p.ApplyStatusBatch(ctx, []arcade.TxRecord{
		{TxID: seenTx, Status: arcade.StatusSeenOnNetwork},
		minedRec,
	}))

	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(seenTx))
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(seenTx).Status)
	require.Equal(t, utxostore.TierUnproven, h.tier(seenOp))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(minedTx))
	kt := h.knownTx(minedTx)
	require.Equal(t, wdk.ProvenTxStatusCompleted, kt.Status)
	require.NotEmpty(t, kt.MerklePath, "BUMP stored")
	require.NotEmpty(t, kt.MerkleRoot, "verified root stored")
	require.NotNil(t, kt.BlockHeight)
	require.Equal(t, height, *kt.BlockHeight)
	require.Equal(t, utxostore.TierMined, h.tier(minedOp))
}

// TestApplyStatusBatch_IdempotentReapply applies the same SEEN+MINED batch twice
// and asserts the end state is unchanged and the MINED proof is header-verified
// exactly once (the re-apply short-circuits at the terminal guard, so
// verifyMinedBatch never re-runs it).
func TestApplyStatusBatch_IdempotentReapply(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	seenTx := newTxID(0xC1)
	seenOp := h.seedChangeTx(seenTx, wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed, 5000, utxostore.TierSending)

	minedTx := newTxID(0xC2)
	minedOp := h.seedChangeTx(minedTx, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)
	const height = uint32(900002)
	minedRec, root := minedRecord(t, minedTx, height)
	h.hdrs.register(height, root)

	batch := []arcade.TxRecord{
		{TxID: seenTx, Status: arcade.StatusSeenOnNetwork},
		minedRec,
	}
	require.NoError(t, h.p.ApplyStatusBatch(ctx, batch))
	require.NoError(t, h.p.ApplyStatusBatch(ctx, batch), "second apply is a no-op")

	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(seenTx))
	require.Equal(t, utxostore.TierUnproven, h.tier(seenOp))
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(minedTx))
	require.Equal(t, utxostore.TierMined, h.tier(minedOp))
	require.EqualValues(t, 1, h.hdrs.verifyCalls.Load(),
		"verification runs once; the re-apply short-circuits at the terminal guard")
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

// TestSynchronizeTransactionStatuses_NeverStrandsBehindTheHead is THE regression
// test for the 2026-08-10 incident: after a 30-minute 1000-TPS run, 23,745
// transactions sat with an empty arcade_status and the count never moved, while
// arcade (the source of truth) had every one of them as MINED. The cause was
// head-of-line blocking in this sweep — it ordered its work list by updated_at,
// and a row it could not apply wrote nothing, so the same head rows were
// re-selected on every tick and the rest of the backlog was never SELECTed.
//
// Here the poll can NEVER apply anything (GetTx always fails), so progress has
// to come from recording the ATTEMPT alone: with more rows than the batch limit,
// consecutive ticks must reach the rows BEHIND the head.
func TestSynchronizeTransactionStatuses_NeverStrandsBehindTheHead(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	const (
		rows  = 6
		limit = 2
		ticks = 3 // ticks*limit == rows: a perfect round-robin reaches every row
	)
	for i := range rows {
		h.seedChangeTx(newTxID(byte(0xA0+i)), wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 5000, utxostore.TierUnproven)
	}

	var mu sync.Mutex
	polled := map[string]int{}
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		mu.Lock()
		polled[id]++
		mu.Unlock()
		return nil, fmt.Errorf("arcade unreachable")
	}

	h.now = h.now.Add(5 * time.Minute) // every row is now stale enough to poll
	for range ticks {
		require.NoError(t, h.p.SynchronizeTransactionStatuses(ctx, limit))
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, polled, rows,
		"%d ticks of %d reached only %d of %d rows: the rows behind the head are stranded", ticks, limit, len(polled), rows)
}

// TestSynchronizeTransactionStatuses_RepairsMissingArcadeStatus proves the
// dedicated repair path: a transaction with a perfectly valid LOCAL status but
// NO arcade status is reached and repaired even when the general staleness work
// list is already full of rows that sort ahead of it. That is the shape of the
// 23,745 diverged rows — without its own query (its own index, its own budget) a
// stranded row sitting behind a saturated backlog is never picked up.
func TestSynchronizeTransactionStatuses_RepairsMissingArcadeStatus(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	// Four decoys that ALREADY have an arcade status. They are seeded first (so
	// they sort ahead on updated_at) and there are more of them than the batch
	// limit, so they alone consume the whole staleness page.
	const limit = 2
	for i := range 4 {
		decoy := newTxID(byte(0x10 + i))
		h.seedChangeTx(decoy, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 5000, utxostore.TierUnproven)
		require.NoError(t, h.meta.KnownTx().SetArcadeStatus(ctx, decoy, string(arcade.StatusSeenOnNetwork)))
	}

	// The stranded row: seeded last (newest updated_at) with the highest txid, so
	// it is dead last in the staleness order and unreachable at limit=2.
	stranded := newTxID(0xFE)
	op := h.seedChangeTx(stranded, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 7000, utxostore.TierUnproven)
	require.Nil(t, h.knownTx(stranded).ArcadeStatus, "the stranded row starts with no arcade status")

	const height = uint32(890000)
	rec, root := minedRecord(t, stranded, height)
	h.hdrs.register(height, root)
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		if id == stranded {
			r := rec
			return &r, nil
		}
		return &arcade.TxRecord{TxID: id, Status: arcade.StatusSeenOnNetwork}, nil
	}

	h.now = h.now.Add(5 * time.Minute)
	require.NoError(t, h.p.SynchronizeTransactionStatuses(ctx, limit))

	kt := h.knownTx(stranded)
	require.NotNil(t, kt.ArcadeStatus, "the repair sweep must reach a row with no arcade status in ONE tick")
	require.Equal(t, string(arcade.StatusMined), *kt.ArcadeStatus)
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(stranded))
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
	require.Equal(t, utxostore.TierSending, h.tier(op), "change not claimable on the 202, only on SEEN")
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: mainTxID, Status: arcade.StatusSeenOnNetwork}))
	require.Equal(t, utxostore.TierUnproven, h.tier(op), "SEEN promotes change to claimable")
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

// TestSweepStaleReservations_ReleasesStuckReservation verifies the recovery
// sweep reclaims a funding reservation whose transaction can no longer be sent
// (no stored raw tx — a payment stranded pre-broadcast), so the leaked inputs
// become spendable again instead of permanently locking the wallet.
func TestSweepStaleReservations_ReleasesStuckReservation(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	txid := fmt.Sprintf("%064x", 0x7a)
	h.seedChangeTx(txid, wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed, 5000, utxostore.TierSending)
	rows, err := h.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	require.NoError(t, err)
	reservation := string(rows[0].Reference)

	// Mint a funding coin and reserve it under the tx reference (as a real
	// CreateAction would); the tx then strands — no raw tx, never broadcast.
	var src chainhash.Hash
	src[0] = 0x55
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{{
		Outpoint: utxostore.Outpoint{TxID: src, Vout: 0}, UserID: int64(h.userID),
		Basket: "funding", Satoshis: 9000, InputSize: 107, Tier: utxostore.TierMined,
	}}))
	_, err = h.utxo.ClaimSmallestSufficient(ctx, utxostore.Scope{
		UserID: int64(h.userID), Basket: "funding", Tier: utxostore.TierMined,
	}, reservation, 1)
	require.NoError(t, err)

	before, err := h.utxo.Balance(ctx, int64(h.userID), "funding")
	require.NoError(t, err)
	require.Equal(t, 1, before.ReservedCount, "coin reserved before the sweep")

	// olderThan in the future ⇒ the reservation qualifies as stale; the tx has no
	// raw tx so it is not re-drivable and the sweep reclaims it.
	require.NoError(t, h.p.SweepStaleReservations(ctx, time.Now().Add(time.Hour), 100))

	after, err := h.utxo.Balance(ctx, int64(h.userID), "funding")
	require.NoError(t, err)
	require.Zero(t, after.ReservedCount, "stale reservation released")
	require.Equal(t, uint64(9000), after.Claimable[utxostore.TierMined], "input claimable again")
}

// TestApplyStatusBatch_MinedVerifyDoesNotSerialize pins the fix for a stall that
// froze the ENTIRE arcade event stream (observed 2026-08-11 at ~1000 TPS).
//
// verifyMinedBatch memoizes header verification per (height, root) so a block's
// worth of MINED events costs one HeaderByHeight instead of one per tx. The
// original memo held its mutex across that fetch, so every verify goroutine in
// the errgroup serialized behind one lock no matter which key it wanted. A large
// MINED burst then applied at single-goroutine speed, the monitor's SSE hand-off
// channel filled, and the arcade reader blocked in dispatchFrame — stalling
// delivery of every status, not just MINED. A goroutine dump showed 224
// goroutines parked in sync.Mutex.Lock inside this function.
//
// The assertion is wall-clock: distinct keys must verify CONCURRENTLY (bounded
// by minedVerifyConcurrency), not one-at-a-time behind the memo lock.
func TestApplyStatusBatch_MinedVerifyDoesNotSerialize(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	const (
		n     = 24
		delay = 40 * time.Millisecond
	)

	// Distinct heights => distinct memo keys => every record needs its own fetch.
	// Serialized that is n*delay (~960ms); concurrent it is bounded by the verify
	// pool, so it must land far below that.
	recs := make([]arcade.TxRecord, 0, n)
	for i := range n {
		txid := newTxID(byte(0x40 + i))
		h.seedChangeTx(txid, wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 6000, utxostore.TierUnproven)
		height := uint32(870000 + i)
		rec, root := minedRecord(t, txid, height)
		h.hdrs.register(height, root)
		recs = append(recs, rec)
	}
	h.hdrs.verifyDelay = delay

	start := time.Now()
	require.NoError(t, h.p.ApplyStatusBatch(ctx, recs))
	elapsed := time.Since(start)

	require.EqualValues(t, n, h.hdrs.verifyCalls.Load(), "one fetch per distinct (height, root)")
	require.Less(t, elapsed, time.Duration(n/2)*delay,
		"MINED verify serialized on the memo mutex: %v for %d distinct keys at %v/fetch", elapsed, n, delay)
}

// TestSynchronizeTransactionStatuses_RepairRateScalesWithBacklog is the cadence
// half of the repair path. Reaching the rows behind the head is necessary but
// not sufficient: the sweep also has to reach them FAST ENOUGH. It used to take
// exactly one page per tick whatever the backlog, which the live run measured at
// 4,000 rows per ≈60s tick — 67/s against a 269k divergence, i.e. ~65 minutes,
// and that backlog was in fact rescued by a block catchup rather than by this
// sweep.
//
// A backlog deeper than one page must therefore be paged within a single sweep,
// so recovery scales with the size of the hole instead of trickling.
func TestSynchronizeTransactionStatuses_RepairRateScalesWithBacklog(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	// Five pages' worth of diverged rows: every one has a valid local status and
	// no arcade status at all.
	const (
		rows  = 10
		limit = 2
	)
	stranded := make([]string, rows)
	for i := range rows {
		stranded[i] = newTxID(byte(0xB0 + i))
		h.seedChangeTx(stranded[i], wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 5000, utxostore.TierUnproven)
	}
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		return &arcade.TxRecord{TxID: id, Status: arcade.StatusSeenOnNetwork}, nil
	}
	h.now = h.now.Add(5 * time.Minute) // every row is stale enough to poll

	require.NoError(t, h.p.SynchronizeTransactionStatuses(ctx, limit))

	for _, txid := range stranded {
		require.NotNilf(t, h.knownTx(txid).ArcadeStatus,
			"one sweep at limit=%d must drain a %d-row divergence, not %d of it", limit, rows, limit)
	}
}

// maxRepairPages mirrors the storage package's unexported cap on how many EXTRA
// pages of the repair list one sweep may drain.
const maxRepairPages = 16

// TestSynchronizeTransactionStatuses_RepairPagingIsBounded holds the other side
// of the same knob: scaling with the backlog must not let one sweep run away
// with the process. The sweep is a scheduled job — gocron reschedules it in
// singleton mode — so it has to yield, and maxRepairPages is what makes it.
func TestSynchronizeTransactionStatuses_RepairPagingIsBounded(t *testing.T) {
	ctx := context.Background()
	h := newHookStack(t)

	// Far more diverged rows than maxRepairPages pages can cover at limit=1.
	const (
		rows  = maxRepairPages + 8
		limit = 1
	)
	for i := range rows {
		h.seedChangeTx(newTxID(byte(0x30+i)), wdk.TxStatusUnproven, wdk.ProvenTxStatusUnconfirmed, 5000, utxostore.TierUnproven)
	}

	var mu sync.Mutex
	polled := 0
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		mu.Lock()
		polled++
		mu.Unlock()
		return &arcade.TxRecord{TxID: id, Status: arcade.StatusSeenOnNetwork}, nil
	}
	h.now = h.now.Add(5 * time.Minute)

	require.NoError(t, h.p.SynchronizeTransactionStatuses(ctx, limit))

	mu.Lock()
	defer mu.Unlock()
	// One first page (merged staleness + repair) plus at most maxRepairPages.
	require.LessOrEqual(t, polled, (1+maxRepairPages)*limit,
		"one sweep must not page past maxRepairPages, however deep the backlog")
	require.Less(t, polled, rows, "the remainder is left for the next tick")
}
