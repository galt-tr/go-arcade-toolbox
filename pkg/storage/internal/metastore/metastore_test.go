package metastore_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// storeFactory builds a fresh, migrated store (its own file/schema) with the
// given options, registering teardown. Both engines provide one so the suite
// runs identically on each.
type storeFactory func(t *testing.T, opts ...metastore.Option) *metastore.Store

// baseTime anchors the manual clock at a microsecond-aligned instant so
// timestamps round-trip identically through PostgreSQL TIMESTAMPTZ and SQLite
// INTEGER-microseconds.
var baseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newSQLiteMeta builds a fresh migrated SQLite metastore on a temp-dir file.
func newSQLiteMeta(t *testing.T, opts ...metastore.Option) *metastore.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := metastore.OpenSQLite(context.Background(), path, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// TestMetastore_SQLite runs the full repository suite against SQLite.
func TestMetastore_SQLite(t *testing.T) {
	runMetastoreSuite(t, newSQLiteMeta)
}

func randTxID(t *testing.T) string {
	t.Helper()
	return hex.EncodeToString(randBytes(t, 32))
}

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// mustUser inserts (or finds) a user and returns its id.
func mustUser(ctx context.Context, t *testing.T, s *metastore.Store, identityKey string) int {
	t.Helper()
	u, _, err := s.Users().FindOrInsertUser(ctx, identityKey)
	require.NoError(t, err)
	return u.UserID
}

func strptr(s string) *string { return &s }

// runMetastoreSuite exercises every repository against a factory-built store.
//
// Subtests run in PARALLEL. Each calls factory(t), which mints its own isolated
// schema on PostgreSQL and its own temp file on SQLite, so they share nothing.
// Running them concurrently puts the backend under simultaneous unrelated
// transactions — the condition the deadlock-ordering work in lockorder.go
// exists for, and one a serial suite can never reproduce.
func runMetastoreSuite(t *testing.T, factory storeFactory) {
	t.Run("Users_FindOrInsert", func(t *testing.T) { t.Parallel(); testUsers(t, factory) })
	t.Run("Baskets", func(t *testing.T) { t.Parallel(); testBaskets(t, factory) })
	t.Run("Transactions_ListActions", func(t *testing.T) { t.Parallel(); testTransactions(t, factory) })
	t.Run("Outputs_ListOutputs_Seam", func(t *testing.T) { t.Parallel(); testOutputs(t, factory) })
	t.Run("KnownTx_SuspectFailed", func(t *testing.T) { t.Parallel(); testKnownTx(t, factory) })
	t.Run("KnownTx_PollProgressAndRepair", func(t *testing.T) { t.Parallel(); testKnownTxPollProgress(t, factory) })
	t.Run("KnownTx_BulkMutatorsOverlappingTxIDs", func(t *testing.T) { t.Parallel(); testKnownTxBulkOverlap(t, factory) })
	t.Run("Transactions_InputBEEF", func(t *testing.T) { t.Parallel(); testTransactionsInputBEEF(t, factory) })
	t.Run("KnownTx_InputBEEFNotSticky", func(t *testing.T) { t.Parallel(); testKnownTxInputBEEFNotSticky(t, factory) })
	t.Run("SyncState_RoundTrip", func(t *testing.T) { t.Parallel(); testSyncState(t, factory) })
	t.Run("KeyValue", func(t *testing.T) { t.Parallel(); testKeyValue(t, factory) })
	t.Run("Certificates", func(t *testing.T) { t.Parallel(); testCertificates(t, factory) })
	t.Run("Outbox", func(t *testing.T) { t.Parallel(); testOutbox(t, factory) })
	t.Run("Outbox_ConcurrentDrain", func(t *testing.T) { t.Parallel(); testOutboxConcurrentDrain(t, factory) })
	t.Run("ModeA_SharedTx", func(t *testing.T) { t.Parallel(); testModeA(t, factory) })
}

func testUsers(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	u1, wasNew, err := s.Users().FindOrInsertUser(ctx, "ident-a")
	require.NoError(t, err)
	require.True(t, wasNew, "first insert is new")
	require.Positive(t, u1.UserID)

	u2, wasNew, err := s.Users().FindOrInsertUser(ctx, "ident-a")
	require.NoError(t, err)
	require.False(t, wasNew, "second is idempotent, not new")
	require.Equal(t, u1.UserID, u2.UserID)

	got, found, err := s.Users().FindByIdentityKey(ctx, "ident-a")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, u1.UserID, got.UserID)

	_, found, err = s.Users().FindByIdentityKey(ctx, "nobody")
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, s.Users().SetActiveStorage(ctx, u1.UserID, "storage-key-1"))
	got, found, err = s.Users().FindByID(ctx, u1.UserID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "storage-key-1", got.ActiveStorage)
}

func testBaskets(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "basket-user")

	require.NoError(t, s.Baskets().FindOrCreate(ctx, uid, "default"))
	require.NoError(t, s.Baskets().FindOrCreate(ctx, uid, "default"), "idempotent")

	b, found, err := s.Baskets().FindByName(ctx, uid, "default")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, primitives.StringUnder300("default"), b.Name)

	// Overwrite the desired-utxo knobs.
	require.NoError(t, s.Baskets().Upsert(ctx, uid, wdk.BasketConfiguration{
		Name: "default", NumberOfDesiredUTXOs: 32, MinimumDesiredUTXOValue: 1000,
	}, true))
	b, _, err = s.Baskets().FindByName(ctx, uid, "default")
	require.NoError(t, err)
	require.Equal(t, int64(32), b.NumberOfDesiredUTXOs)
	require.Equal(t, uint64(1000), b.MinimumDesiredUTXOValue)

	require.NoError(t, s.Baskets().FindOrCreate(ctx, uid, "fuel"))
	all, err := s.Baskets().Find(ctx, wdk.FindOutputBasketsArgs{UserID: &uid})
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, primitives.StringUnder300("default"), all[0].Name, "ordered by name")
	require.Equal(t, primitives.StringUnder300("fuel"), all[1].Name)
}

func testTransactions(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "tx-user")

	// t1 [alpha] unsigned, t2 [alpha,beta] completed, t3 [beta] completed.
	id1, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "ref-1", Satoshis: 10, Labels: []string{"alpha"},
	})
	require.NoError(t, err)
	id2, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusCompleted, Reference: "ref-2", Satoshis: 20, Labels: []string{"alpha", "beta"},
	})
	require.NoError(t, err)
	id3, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusCompleted, Reference: "ref-3", Satoshis: 30, Labels: []string{"beta"},
	})
	require.NoError(t, err)
	require.Less(t, id1, id2)
	require.Less(t, id2, id3)

	// No filter: total 3, ordered by id ASC.
	rows, total, err := s.Transactions().ListActions(ctx, metastore.ListActionsFilter{UserID: uid, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, []string{"ref-1", "ref-2", "ref-3"}, refsOf(rows))

	// Status filter.
	rows, total, err = s.Transactions().ListActions(ctx, metastore.ListActionsFilter{
		UserID: uid, Statuses: []wdk.TxStatus{wdk.TxStatusCompleted}, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Equal(t, []string{"ref-2", "ref-3"}, refsOf(rows))

	// Label ANY vs ALL.
	anyMode := defs.QueryModeAny
	all := defs.QueryModeAll
	_, total, err = s.Transactions().ListActions(ctx, metastore.ListActionsFilter{
		UserID: uid, Labels: []string{"alpha", "beta"}, LabelQueryMode: &anyMode, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), total, "ANY matches all three")
	rows, total, err = s.Transactions().ListActions(ctx, metastore.ListActionsFilter{
		UserID: uid, Labels: []string{"alpha", "beta"}, LabelQueryMode: &all, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), total, "ALL matches only t2")
	require.Equal(t, []string{"ref-2"}, refsOf(rows))

	// Pagination.
	rows, total, err = s.Transactions().ListActions(ctx, metastore.ListActionsFilter{UserID: uid, Limit: 2, Offset: 0})
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, []string{"ref-1", "ref-2"}, refsOf(rows))
	rows, _, err = s.Transactions().ListActions(ctx, metastore.ListActionsFilter{UserID: uid, Limit: 2, Offset: 2})
	require.NoError(t, err)
	require.Equal(t, []string{"ref-3"}, refsOf(rows))

	// GetLabelsForTransactions.
	labels, err := s.Transactions().GetLabelsForTransactions(ctx, []uint{id1, id2})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha"}, labels[id1])
	require.Equal(t, []string{"alpha", "beta"}, labels[id2])

	// Guarded status transitions.
	require.NoError(t, s.Transactions().UpdateStatus(ctx, id1, wdk.TxStatusSending, wdk.TxStatusUnsigned))
	got, _, err := s.Transactions().FindByID(ctx, id1)
	require.NoError(t, err)
	require.Equal(t, wdk.TxStatusSending, got.Status)
	err = s.Transactions().UpdateStatus(ctx, id1, wdk.TxStatusCompleted, wdk.TxStatusUnsigned)
	require.ErrorIs(t, err, metastore.ErrStatusUpdateSkipped, "CAS precondition no longer holds")

	// SetTxID + FindByTxID + FindByReference.
	txid := randTxID(t)
	require.NoError(t, s.Transactions().SetTxID(ctx, id2, txid))
	byTxID, err := s.Transactions().FindByTxID(ctx, uid, txid)
	require.NoError(t, err)
	require.Len(t, byTxID, 1)
	require.Equal(t, id2, byTxID[0].TransactionID)
	require.NotNil(t, byTxID[0].TxID)
	require.Equal(t, txid, *byTxID[0].TxID)
	byRef, found, err := s.Transactions().FindByReference(ctx, uid, "ref-2")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, id2, byRef.TransactionID)

	// Abandoned scan: a fresh unsigned tx is abandoned at grace 0, not at grace 1h.
	id4, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "ref-4",
	})
	require.NoError(t, err)
	abandoned, err := s.Transactions().FindAbandoned(ctx, 0, 10)
	require.NoError(t, err)
	require.Contains(t, refsOf(abandoned), "ref-4")
	require.NotContains(t, refsOf(abandoned), "ref-1", "ref-1 is now 'sending', not abandonable")
	abandoned, err = s.Transactions().FindAbandoned(ctx, time.Hour, 10)
	require.NoError(t, err)
	require.Empty(t, abandoned, "nothing is an hour old")
	_ = id4
}

func refsOf(rows []wdk.TableTransaction) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r.Reference)
	}
	return out
}

func testOutputs(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "out-user")

	txid := randTxID(t)
	txnID, err := s.Transactions().Insert(ctx, metastore.NewTx{UserID: uid, Status: wdk.TxStatusCompleted, Reference: "out-ref"})
	require.NoError(t, err)
	require.NoError(t, s.Transactions().SetTxID(ctx, txnID, txid))

	o1, err := s.Outputs().Insert(ctx, metastore.NewOutput{
		UserID: uid, TransactionID: txnID, Vout: 0, Satoshis: 100, Basket: strptr("default"),
		Change: true, Type: "P2PKH", Tags: []string{"red"},
		DerivationPrefix: strptr("pfx"), DerivationSuffix: strptr("sfx"),
		SenderIdentityKey: strptr("sender"), CustomInstructions: strptr("ci"),
	})
	require.NoError(t, err)
	o2, err := s.Outputs().Insert(ctx, metastore.NewOutput{
		UserID: uid, TransactionID: txnID, Vout: 1, Satoshis: 200, Basket: strptr("default"),
		Tags: []string{"red", "blue"},
	})
	require.NoError(t, err)
	_, err = s.Outputs().Insert(ctx, metastore.NewOutput{
		UserID: uid, TransactionID: txnID, Vout: 2, Satoshis: 300, Basket: strptr("special"), Tags: []string{"blue"},
	})
	require.NoError(t, err)

	// An output on a failed transaction must be excluded by the status filter.
	failedTx, err := s.Transactions().Insert(ctx, metastore.NewTx{UserID: uid, Status: wdk.TxStatusFailed, Reference: "failed-ref"})
	require.NoError(t, err)
	_, err = s.Outputs().Insert(ctx, metastore.NewOutput{UserID: uid, TransactionID: failedTx, Vout: 0, Satoshis: 999, Basket: strptr("default")})
	require.NoError(t, err)

	statuses := metastore.DefaultListOutputsStatuses

	// Basket filter + spendability seam (Spendable must be false, never set here).
	rows, total, err := s.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{
		UserID: uid, Basket: "default", Statuses: statuses, Limit: 10, IncludeTags: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), total, "the failed-tx output is excluded by status")
	require.Equal(t, []uint{o1, o2}, outIDs(rows), "ordered by output_id ASC")
	for _, r := range rows {
		require.False(t, r.Spendable, "spendability is EXTERNAL: metastore never sets it")
	}
	require.Equal(t, []string{"red"}, rows[0].Tags)
	require.ElementsMatch(t, []string{"red", "blue"}, rows[1].Tags)
	require.NotNil(t, rows[0].Basket)
	require.Equal(t, "default", *rows[0].Basket)
	require.NotNil(t, rows[0].TxID)
	require.Equal(t, txid, *rows[0].TxID)

	// Tag filter ALL vs ANY.
	all := defs.QueryModeAll
	rows, total, err = s.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{
		UserID: uid, Basket: "default", Tags: []string{"red", "blue"}, TagQueryMode: &all, Statuses: statuses, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Equal(t, []uint{o2}, outIDs(rows))

	anyMode := defs.QueryModeAny
	_, total, err = s.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{
		UserID: uid, Tags: []string{"blue"}, TagQueryMode: &anyMode, Statuses: statuses, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), total, "o2 and o3 carry blue")

	// FindOutputs by structured filters (ignores Spendable).
	changeTrue := true
	found, err := s.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{UserID: &uid, Change: &changeTrue})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Equal(t, o1, found[0].OutputID)

	// RelinquishOutput error cases: unknown outpoint vs wrong basket.
	require.ErrorIs(t, s.Outputs().RelinquishOutput(ctx, uid, "default", randTxID(t), 0), metastore.ErrNotFound)
	require.ErrorIs(t, s.Outputs().RelinquishOutput(ctx, uid, "special", txid, 0), metastore.ErrOutputBasketMismatch,
		"o1 (vout 0) is in default, not special")

	// RelinquishOutput removes o1 from the default basket.
	require.NoError(t, s.Outputs().RelinquishOutput(ctx, uid, "default", txid, 0))
	rows, total, err = s.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{
		UserID: uid, Basket: "default", Statuses: statuses, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Equal(t, []uint{o2}, outIDs(rows))

	// MarkSpent / ClearSpentBy history.
	require.NoError(t, s.Outputs().MarkSpent(ctx, txnID, []uint{o2}))
	found, err = s.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{UserID: &uid, OutputID: &o2})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.NotNil(t, found[0].SpentBy)
	require.NoError(t, s.Outputs().ClearSpentBy(ctx, txnID))
	found, err = s.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{UserID: &uid, OutputID: &o2})
	require.NoError(t, err)
	require.Nil(t, found[0].SpentBy)

	// BRC-29 derivation material.
	mat, ok, err := s.Outputs().GetBRC29Material(ctx, o1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "pfx", *mat.DerivationPrefix)
	require.Equal(t, "sfx", *mat.DerivationSuffix)
	require.Equal(t, "sender", *mat.SenderIdentityKey)
	require.Equal(t, "ci", *mat.CustomInstructions)
}

func outIDs(rows []metastore.OutputRow) []uint {
	out := make([]uint, len(rows))
	for i, r := range rows {
		out[i] = r.OutputID
	}
	return out
}

func testKnownTx(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	txid := randTxID(t)
	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID: txid, Status: wdk.ProvenTxStatusUnsent, RawTx: []byte{0x01, 0x02},
		Notify: `{"webhook":"x"}`, CompetingTxs: []string{"aa", "bb"},
	}))

	kt, found, err := s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusUnsent, kt.Status)
	require.Equal(t, []string{"aa", "bb"}, kt.CompetingTxs, "competing_txs JSON round-trips")
	require.Equal(t, `{"webhook":"x"}`, kt.Notify)
	require.False(t, kt.WasBroadcast)

	// UpdateStatus against an unknown txid reports ErrNotFound (not a skip).
	require.ErrorIs(t, s.KnownTx().UpdateStatus(ctx, randTxID(t), wdk.ProvenTxStatusUnmined), metastore.ErrNotFound)

	// Advance to a broadcast status: was_broadcast becomes sticky-true.
	require.NoError(t, s.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnmined, wdk.ProvenTxStatusCompleted))
	kt, _, err = s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, kt.WasBroadcast)

	// UpdateStatus whose guard blocks an EXISTING row reports ErrStatusUpdateSkipped.
	require.ErrorIs(t,
		s.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnsent, wdk.ProvenTxStatusUnmined),
		metastore.ErrStatusUpdateSkipped)

	// Guarded upsert: current status (unmined) is in the skip set → skip signal.
	require.ErrorIs(t,
		s.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: txid, Status: wdk.ProvenTxStatusUnsent}, wdk.ProvenTxStatusUnmined),
		metastore.ErrStatusUpdateSkipped)
	kt, _, err = s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusUnmined, kt.Status, "status unchanged")
	require.True(t, kt.WasBroadcast, "was_broadcast stays sticky")

	// SetProof.
	require.NoError(t, s.KnownTx().SetProof(ctx, txid, 800000, randBytes(t, 32), []byte{0x0a}, randBytes(t, 32)))
	kt, _, err = s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusCompleted, kt.Status)
	require.NotNil(t, kt.BlockHeight)
	require.Equal(t, uint32(800000), *kt.BlockHeight)

	statuses, err := s.KnownTx().FindStatuses(ctx, []string{txid})
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusCompleted, statuses[txid])

	rawTx, ok, err := s.KnownTx().GetRawTx(ctx, txid)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte{0x01, 0x02}, rawTx)

	// Suspect-failed grace window.
	old := randTxID(t)
	recent := randTxID(t)
	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: old, Status: wdk.ProvenTxStatusUnsent}))
	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: recent, Status: wdk.ProvenTxStatusUnsent}))
	now := time.Now().UTC()
	require.NoError(t, s.KnownTx().MarkSuspectFailed(ctx, old, now.Add(-time.Hour), "insufficient fee"))
	require.NoError(t, s.KnownTx().MarkSuspectFailed(ctx, recent, now, ""))

	suspects, err := s.KnownTx().FindSuspectFailed(ctx, 30*time.Minute, 10)
	require.NoError(t, err)
	ids := knownTxIDs(suspects)
	require.Contains(t, ids, old, "past the grace window")
	require.NotContains(t, ids, recent, "still inside the grace window")

	require.NoError(t, s.KnownTx().IncreaseAttempts(ctx, []string{old}))
	require.NoError(t, s.KnownTx().SetBatch(ctx, []string{old}, "batch-1"))
	kt, _, err = s.KnownTx().FindByTxID(ctx, old)
	require.NoError(t, err)
	require.Equal(t, uint64(1), kt.Attempts)
	require.NotNil(t, kt.Batch)
	require.Equal(t, "batch-1", *kt.Batch)
}

func knownTxIDs(rows []metastore.KnownTx) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.TxID
	}
	return out
}

// testKnownTxPollProgress is the repository-level regression test for the
// stranded-transaction incident: a 30-minute 1000-TPS run left 23,745 known txs
// with an EMPTY arcade_status and the count never drained, because the poll
// ordered its work list by updated_at and a row it could not apply wrote nothing
// — so the same head rows were re-selected every single tick and everything
// behind them was never SELECTed at all.
//
// It asserts the two halves of the fix:
//
//   - PROGRESS: with more rows than the batch limit and NOTHING ever applied,
//     consecutive ticks must hand back rows behind the head (MarkPolled records
//     the attempt; the finders order by that stamp), so every row is reached.
//   - REPAIR: a row with a perfectly good local status but no arcade status is
//     findable by its own query and leaves that list once arcade's status lands.
func testKnownTxPollProgress(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	clock := &manualClock{t: baseTime}
	s := factory(t, metastore.WithClock(clock.now))

	const (
		rows  = 5
		batch = 2
		ticks = 3 // ticks*batch >= rows: enough to reach every row exactly once
	)
	txids := make([]string, 0, rows)
	for range rows {
		id := randTxID(t)
		require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: id, Status: wdk.ProvenTxStatusUnconfirmed}))
		txids = append(txids, id)
	}
	cutoff := baseTime.Add(24 * time.Hour) // every row is "stale"

	selected := map[string]int{}
	var head []string
	for tick := range ticks {
		got, fErr := s.KnownTx().FindByStatusOlderThan(ctx, cutoff, batch, wdk.ProvenTxStatusUnconfirmed)
		require.NoError(t, fErr)
		require.Len(t, got, batch, "tick %d must select a full batch", tick)
		ids := knownTxIDs(got)
		if tick == 0 {
			head = ids
		} else {
			require.NotEqual(t, head, ids,
				"tick %d re-selected the head batch: the poll is not making progress and the backlog is frozen", tick)
		}
		for _, id := range ids {
			selected[id]++
		}
		// The poll applied NOTHING (no status was written) — it only records that
		// it attempted these rows. That alone must be enough to advance.
		require.NoError(t, s.KnownTx().MarkPolled(ctx, ids))
		clock.advance(time.Minute)
	}
	require.Len(t, selected, rows,
		"%d ticks of %d must reach all %d rows; a shorter reach means the head is starving the tail", ticks, batch, rows)

	// MarkPolled is an ATTEMPT stamp, not a state change: updated_at must not move
	// (the resend grace and the staleness cutoff both read it as "last changed").
	after, found, err := s.KnownTx().FindByTxID(ctx, txids[0])
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, baseTime.UTC(), after.UpdatedAt.UTC(), "MarkPolled must not touch updated_at")

	// --- repair list -------------------------------------------------------
	stranded, err := s.KnownTx().FindMissingArcadeStatus(ctx, cutoff, rows, wdk.ProvenTxStatusUnconfirmed)
	require.NoError(t, err)
	require.ElementsMatch(t, txids, knownTxIDs(stranded), "every row is diverged: none ever got an arcade status")

	n, err := s.KnownTx().CountMissingArcadeStatus(ctx, wdk.ProvenTxStatusUnconfirmed)
	require.NoError(t, err)
	require.Equal(t, rows, n)

	// Once arcade's status lands, the row is repaired and leaves the list.
	require.NoError(t, s.KnownTx().SetArcadeStatus(ctx, txids[0], "MINED"))
	stranded, err = s.KnownTx().FindMissingArcadeStatus(ctx, cutoff, rows, wdk.ProvenTxStatusUnconfirmed)
	require.NoError(t, err)
	require.NotContains(t, knownTxIDs(stranded), txids[0], "a repaired row leaves the repair list")
	require.Len(t, stranded, rows-1)

	// An empty-string arcade status is "missing" exactly like NULL.
	require.NoError(t, s.KnownTx().SetArcadeStatus(ctx, txids[0], ""))
	stranded, err = s.KnownTx().FindMissingArcadeStatus(ctx, cutoff, rows, wdk.ProvenTxStatusUnconfirmed)
	require.NoError(t, err)
	require.Contains(t, knownTxIDs(stranded), txids[0], "an empty arcade status counts as missing, like NULL")

	// A terminal row is out of scope for the repair sweep (it is not pollable).
	require.NoError(t, s.KnownTx().UpdateStatus(ctx, txids[1], wdk.ProvenTxStatusCompleted))
	stranded, err = s.KnownTx().FindMissingArcadeStatus(ctx, cutoff, rows, wdk.ProvenTxStatusUnconfirmed)
	require.NoError(t, err)
	require.NotContains(t, knownTxIDs(stranded), txids[1])
}

// testKnownTxBulkOverlap drives the bulk mutators concurrently over OVERLAPPING
// txid sets — the shape that deadlocked in production (SQLSTATE 40P01) when each
// statement was free to lock its rows in arrival order. With every set-based
// mutator binding and locking in ascending storage-txid order a lock-order
// inversion is impossible, so all of these must complete cleanly.
func testKnownTxBulkOverlap(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	const rows = 24
	txids := make([]string, 0, rows)
	for range rows {
		id := randTxID(t)
		require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: id, Status: wdk.ProvenTxStatusUnconfirmed}))
		txids = append(txids, id)
	}

	// Two writers over sets that overlap in the middle, each presenting its half
	// in the OPPOSITE arrival order — the classic inversion.
	forward := txids[:16]
	reverse := make([]string, 0, 16)
	for i := len(txids) - 1; i >= 8; i-- {
		reverse = append(reverse, txids[i])
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i, work := range []func() error{
		func() error { return s.KnownTx().BulkSetArcadeStatus(ctx, forward, "SEEN_ON_NETWORK") },
		func() error { return s.KnownTx().BulkSetArcadeStatus(ctx, reverse, "SEEN_ON_NETWORK") },
		func() error { return s.KnownTx().MarkPolled(ctx, forward) },
		func() error { return s.KnownTx().MarkPolled(ctx, reverse) },
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = work()
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent bulk mutator %d", i)
	}

	// And the writes landed: every txid carries the arcade status.
	for _, id := range txids {
		kt, found, err := s.KnownTx().FindByTxID(ctx, id)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, kt.ArcadeStatus, "txid %s", id)
		require.Equal(t, "SEEN_ON_NETWORK", *kt.ArcadeStatus)
	}
}

func testSyncState(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "sync-user")

	sm := wdk.NewSyncMap()
	sm[wdk.TransactionEntityName].Count = 7
	smJSON, err := sm.JSON()
	require.NoError(t, err)

	sats := primitives.SatoshiValue(4200)
	id, err := s.SyncState().Insert(ctx, wdk.TableSyncState{
		UserID: uid, StorageIdentityKey: "sik-1", StorageName: "store-1",
		Status: wdk.SyncStatusIdentified, RefNum: "ref-sync-1", SyncMap: string(smJSON), Satoshis: &sats,
	})
	require.NoError(t, err)
	require.Positive(t, id)

	got, found, err := s.SyncState().FindByUserAndStorage(ctx, uid, "sik-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ref-sync-1", got.RefNum)
	require.NotNil(t, got.Satoshis)
	require.Equal(t, primitives.SatoshiValue(4200), *got.Satoshis)

	roundTripped, err := wdk.NewSyncMapFromJSON([]byte(got.SyncMap))
	require.NoError(t, err)
	require.Equal(t, uint64(7), roundTripped[wdk.TransactionEntityName].Count, "sync_map JSON round-trips")

	byRef, found, err := s.SyncState().FindByRefNum(ctx, "ref-sync-1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, got.SyncStateID, byRef.SyncStateID)

	when := time.Now().UTC().Truncate(time.Microsecond)
	got.Status = wdk.SyncStatusSuccess
	got.When = &when
	require.NoError(t, s.SyncState().Update(ctx, *got))
	got, _, err = s.SyncState().FindByRefNum(ctx, "ref-sync-1")
	require.NoError(t, err)
	require.Equal(t, wdk.SyncStatusSuccess, got.Status)
	require.NotNil(t, got.When)
	require.WithinDuration(t, when, *got.When, time.Millisecond)
}

func testKeyValue(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	_, found, err := s.KeyValue().Get(ctx, "sse:cursor")
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, s.KeyValue().Set(ctx, "sse:cursor", []byte("abc")))
	v, found, err := s.KeyValue().Get(ctx, "sse:cursor")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("abc"), v)

	require.NoError(t, s.KeyValue().Set(ctx, "sse:cursor", []byte("def")))
	v, _, err = s.KeyValue().Get(ctx, "sse:cursor")
	require.NoError(t, err)
	require.Equal(t, []byte("def"), v)
}

func testCertificates(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "cert-user")

	certifier := "02" + hex.EncodeToString(randBytes(t, 32))
	subject := "03" + hex.EncodeToString(randBytes(t, 32))
	id, err := s.Certificates().Insert(ctx, wdk.TableCertificateX{
		TableCertificate: wdk.TableCertificate{
			UserID: uid, Type: "dGVzdA==", SerialNumber: "c2VyaWFs",
			Certifier: primitives.PubKeyHex(certifier), Subject: primitives.PubKeyHex(subject),
			RevocationOutpoint: "ab.0", Signature: "ab12",
		},
		Fields: []*wdk.TableCertificateField{
			{FieldName: "name", FieldValue: "alice", MasterKey: "bWs="},
		},
	})
	require.NoError(t, err)
	require.Positive(t, id)

	got, found, err := s.Certificates().FindByID(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, primitives.Base64String("dGVzdA=="), got.Type)
	require.Len(t, got.Fields, 1)
	require.Equal(t, "name", got.Fields[0].FieldName)
	require.Equal(t, "alice", got.Fields[0].FieldValue)

	list, err := s.Certificates().ListForUser(ctx, uid)
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, s.Certificates().SoftDelete(ctx, uid, "dGVzdA==", "c2VyaWFs", certifier))
	list, err = s.Certificates().ListForUser(ctx, uid)
	require.NoError(t, err)
	require.Empty(t, list, "soft-deleted certs are hidden from ListForUser")
	got, found, err = s.Certificates().FindByID(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, got.IsDeleted)
}

func testOutbox(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	clk := &manualClock{t: baseTime}
	s := factory(t, metastore.WithClock(clk.now))

	txA := randBytes(t, 32)
	txB := randBytes(t, 32)

	require.NoError(t, s.Outbox().Enqueue(ctx, txA, "spend", 0, []byte(`{"n":1}`)))
	// Duplicate key is a no-op (PK dedupe).
	require.NoError(t, s.Outbox().Enqueue(ctx, txA, "spend", 0, []byte(`{"n":2}`)))
	n, err := s.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "PK dedupe: still one row")

	clk.advance(time.Second)
	require.NoError(t, s.Outbox().Enqueue(ctx, txA, "spend", 1, []byte(`{"n":3}`)))
	clk.advance(time.Second)
	require.NoError(t, s.Outbox().Enqueue(ctx, txB, "mint", 0, []byte(`{"n":4}`)))

	n, err = s.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// FetchPending drains oldest-first; complete them in the same transaction.
	var fetched []metastore.OutboxEntry
	require.NoError(t, s.Do(ctx, func(ctx context.Context) error {
		var err error
		fetched, err = s.Outbox().FetchPending(ctx, 2)
		if err != nil {
			return err
		}
		for _, e := range fetched {
			if err := s.Outbox().MarkDone(ctx, e.TxID, e.OpType, e.Chunk); err != nil {
				return err
			}
		}
		return nil
	}))
	require.Len(t, fetched, 2)
	require.Equal(t, "spend", fetched[0].OpType)
	require.Equal(t, 0, fetched[0].Chunk, "oldest first")
	require.Equal(t, 1, fetched[1].Chunk)

	n, err = s.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "two marked done, one remains")

	// RecordError bumps attempts and stores the message but keeps it pending.
	require.NoError(t, s.Outbox().RecordError(ctx, txB, "mint", 0, "boom"))
	var remaining []metastore.OutboxEntry
	require.NoError(t, s.Do(ctx, func(ctx context.Context) error {
		var err error
		remaining, err = s.Outbox().FetchPending(ctx, 10)
		return err
	}))
	require.Len(t, remaining, 1)
	require.Equal(t, 1, remaining[0].Attempts)
	require.NotNil(t, remaining[0].LastError)
	require.Equal(t, "boom", *remaining[0].LastError)
}

// testOutboxConcurrentDrain proves concurrent workers never double-hand-out a
// row: the union of everything drained equals the enqueued set with no
// duplicate. Run with -race.
func testOutboxConcurrentDrain(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	const rows = 24
	want := make(map[string]struct{}, rows)
	for i := 0; i < rows; i++ {
		txid := randBytes(t, 32)
		require.NoError(t, s.Outbox().Enqueue(ctx, txid, "spend", 0, []byte(fmt.Sprintf(`{"i":%d}`, i))))
		want[hex.EncodeToString(txid)] = struct{}{}
	}

	const workers = 4
	results := make(chan string, rows)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var got []metastore.OutboxEntry
				err := s.Do(ctx, func(ctx context.Context) error {
					var err error
					got, err = s.Outbox().FetchPending(ctx, 1)
					if err != nil || len(got) == 0 {
						return err
					}
					e := got[0]
					return s.Outbox().MarkDone(ctx, e.TxID, e.OpType, e.Chunk)
				})
				if err != nil {
					errCh <- err
					return
				}
				if len(got) == 0 {
					return
				}
				results <- hex.EncodeToString(got[0].TxID)
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	seen := make(map[string]int)
	for id := range results {
		seen[id]++
	}
	require.Len(t, seen, rows, "every row drained exactly once")
	for id, count := range seen {
		require.Equal(t, 1, count, "row %s handed out more than once", id)
		_, ok := want[id]
		require.True(t, ok)
	}
	n, err := s.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}

// testTransactionsInputBEEF covers the read half of the input-BEEF
// de-duplication. known_txs no longer carries a second copy for transactions
// created through CreateAction, so these two methods are the only way the
// broadcast paths reach a transaction's ancestry — and ClearInputBEEFByTxID
// now deletes the ONLY copy rather than one of two.
func testTransactionsInputBEEF(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "beef-user")
	other := mustUser(ctx, t, s, "beef-user-2")

	const txA = "aa11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
	const txB = "bb11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
	blobA := []byte("ancestry-for-A")
	blobOther := []byte("ancestry-for-A-from-the-other-user")
	blobB := []byte("ancestry-for-B")

	// Lowest transaction_id wins: seed the creating user's row FIRST, then a
	// second row for a different user carrying the same txid. transactions.txid
	// is nullable and NOT unique, so this ambiguity is real.
	idA, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "beef-ref-a", InputBEEF: blobA,
	})
	require.NoError(t, err)
	require.NoError(t, s.Transactions().SetTxID(ctx, idA, txA))

	idOther, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: other, Status: wdk.TxStatusUnsigned, Reference: "beef-ref-a2", InputBEEF: blobOther,
	})
	require.NoError(t, err)
	require.NoError(t, s.Transactions().SetTxID(ctx, idOther, txA))
	require.Less(t, idA, idOther)

	idB, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "beef-ref-b", InputBEEF: blobB,
	})
	require.NoError(t, err)
	require.NoError(t, s.Transactions().SetTxID(ctx, idB, txB))

	got, found, err := s.Transactions().GetInputBEEFByTxID(ctx, txA)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, blobA, got, "the lowest transaction_id wins — the row CreateAction wrote")

	// Bulk: same tie-break, and an unknown txid is simply absent.
	const txUnknown = "cc11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
	m, err := s.Transactions().InputBEEFByTxIDs(ctx, []string{txA, txB, txUnknown})
	require.NoError(t, err)
	require.Equal(t, blobA, m[txA], "bulk must use the same lowest-id tie-break as the single read")
	require.Equal(t, blobB, m[txB])
	require.NotContains(t, m, txUnknown, "an unknown txid is absent, not an empty entry")

	// Clearing removes the only remaining copy, for EVERY row carrying the txid.
	require.NoError(t, s.Transactions().ClearInputBEEFByTxID(ctx, txA))
	_, found, err = s.Transactions().GetInputBEEFByTxID(ctx, txA)
	require.NoError(t, err)
	require.False(t, found, "a cleared blob reports found=false, not an empty slice")

	m, err = s.Transactions().InputBEEFByTxIDs(ctx, []string{txA, txB})
	require.NoError(t, err)
	require.NotContains(t, m, txA, "cleared rows drop out of the bulk read")
	require.Equal(t, blobB, m[txB], "clearing one txid must not touch another")

	// The IS NOT NULL guard exists so a re-apply is a cheap no-op: a second
	// clear must not rewrite the rows (which would be pure WAL for nothing).
	before := mustUpdatedAt(ctx, t, s, idA)
	require.NoError(t, s.Transactions().ClearInputBEEFByTxID(ctx, txA))
	require.Equal(t, before, mustUpdatedAt(ctx, t, s, idA), "re-clearing must not touch updated_at")

	// Bulk clear, same three properties.
	require.NoError(t, s.Transactions().BulkClearInputBEEFByTxIDs(ctx, []string{txB}))
	_, found, err = s.Transactions().GetInputBEEFByTxID(ctx, txB)
	require.NoError(t, err)
	require.False(t, found)
}

// mustUpdatedAt reads a transaction row's updated_at for no-op assertions.
func mustUpdatedAt(ctx context.Context, t *testing.T, s *metastore.Store, id uint) string {
	t.Helper()
	row, found, err := s.Transactions().FindByID(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	return row.UpdatedAt.String()
}

// testKnownTxInputBEEFNotSticky pins that Upsert does NOT preserve a previous
// input_beef, deliberately unlike its neighbors was_broadcast and
// reject_reason. That is what lets a re-processed legacy row reclaim its
// duplicate blob instead of carrying it forever.
func testKnownTxInputBEEFNotSticky(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	const txid = "dd11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"

	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID: txid, Status: wdk.ProvenTxStatusUnsent, RawTx: []byte{0x01}, InputBEEF: []byte("legacy-blob"),
	}))
	kt, found, err := s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, kt.InputBEEF)

	// Re-upsert without a blob — what the create path now always does.
	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID: txid, Status: wdk.ProvenTxStatusUnsent, RawTx: []byte{0x01},
	}))
	kt, found, err = s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, kt.InputBEEF, "input_beef is not sticky: the duplicate is reclaimed")
}
