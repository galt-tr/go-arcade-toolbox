package monitor

// White-box test for the batch applier's per-txid serialization (the
// concurrency fix). applyStatusBatch now hands the whole batch to
// storage.ApplyStatusBatch, and that contract — records for one txid applied in
// arrival order and never concurrently, distinct txids in parallel — is modeled
// by modelStorage.ApplyStatusBatch here. The test co-batches, for the SAME txid,
// the four dangerous orderings and asserts arrival-order wins with a MINED tx
// never clobbered by an earlier-arrival stale frame — while distinct txids still
// run in parallel.
//
// modelStorage faithfully reproduces the exact hazard ApplyStatusUpdate has: the
// terminal/lattice guard is read from a snapshot taken OUTSIDE the write, three
// of the writes are unconditional (the tier set can LOWER a tier; the
// arcade-status and suspect writes are txid-only), and a sleep widens the
// read→write window. Were same-txid records applied concurrently a stale frame
// would corrupt state; the per-txid serialization makes these tests pass
// deterministically.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
)

// modeled tiers (mirror utxostore's TierUnproven/TierMined ordering).
const (
	tierUnproven = 2
	tierMined    = 3
)

type modelState struct {
	tier         int
	status       string // "unproven" | "completed" | "suspect"
	arcadeStatus string
}

type modelStorage struct {
	mu           sync.Mutex
	states       map[string]*modelState
	appliedOrder map[string][]arcade.Status
	kv           map[string][]byte

	perTxidActive       map[string]int
	totalActive         int
	maxActive           int
	sameTxidConcurrency atomic.Bool

	// batchCalls counts ApplyStatusBatch invocations — one per non-empty apply
	// shard — so a test can observe how wide the fan-out actually was.
	batchCalls atomic.Int32
	// batchErr, when set, fails every ApplyStatusBatch (the "the database is
	// down mid-batch" case the replay cursor must not skip past).
	batchErr error
}

func newModelStorage(seed []string) *modelStorage {
	m := &modelStorage{
		states:        map[string]*modelState{},
		appliedOrder:  map[string][]arcade.Status{},
		kv:            map[string][]byte{},
		perTxidActive: map[string]int{},
	}
	for _, txid := range seed {
		// Each txid starts broadcast-accepted-but-unproven.
		m.states[txid] = &modelState{tier: tierUnproven, status: "unproven"}
	}
	return m
}

func (m *modelStorage) ApplyStatusUpdate(_ context.Context, rec arcade.TxRecord) error {
	txid := rec.TxID

	m.mu.Lock()
	m.perTxidActive[txid]++
	if m.perTxidActive[txid] > 1 {
		m.sameTxidConcurrency.Store(true)
	}
	m.totalActive++
	if m.totalActive > m.maxActive {
		m.maxActive = m.totalActive
	}
	var snap modelState
	if s := m.states[txid]; s != nil {
		snap = *s
	}
	m.mu.Unlock()

	// Guard on the snapshot, OUTSIDE the write — exactly like ApplyStatusUpdate.
	apply := snap.status != "completed"
	if apply && snap.arcadeStatus != "" && !rec.Status.CanSupersede(arcade.Status(snap.arcadeStatus)) {
		apply = false
	}

	time.Sleep(time.Millisecond) // widen the read→write window

	if apply {
		m.mu.Lock()
		cur := m.states[txid]
		if cur == nil {
			cur = &modelState{}
			m.states[txid] = cur
		}
		switch rec.Status {
		case arcade.StatusSeenOnNetwork, arcade.StatusSeenMultipleNodes, arcade.StatusAcceptedByNetwork:
			cur.tier = tierUnproven // UNCONDITIONAL set — lowers a mined tier
			cur.status = "unproven"
			cur.arcadeStatus = string(rec.Status)
		case arcade.StatusMined, arcade.StatusImmutable:
			cur.tier = tierMined
			cur.status = "completed"
			cur.arcadeStatus = string(rec.Status)
		case arcade.StatusRejected, arcade.StatusDoubleSpendAttempted:
			cur.status = "suspect" // UNCONDITIONAL — clobbers completed on the old race
			cur.arcadeStatus = string(rec.Status)
		}
		m.appliedOrder[txid] = append(m.appliedOrder[txid], rec.Status)
		m.mu.Unlock()
	}

	m.mu.Lock()
	m.perTxidActive[txid]--
	m.totalActive--
	m.mu.Unlock()
	return nil
}

func (m *modelStorage) stateOf(txid string) modelState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *m.states[txid]
}

// ApplyStatusBatch models the real storage.ApplyStatusBatch contract that the
// per-txid serialization now lives behind: records for one txid are applied IN
// ARRIVAL ORDER on a single goroutine (never concurrently), while distinct txids
// run in parallel. That keeps this white-box test's invariants — a MINED tx is
// never clobbered by an earlier-arrival stale frame for the same txid, and the
// cross-txid throughput parallelism is preserved.
func (m *modelStorage) ApplyStatusBatch(ctx context.Context, recs []arcade.TxRecord) error {
	m.batchCalls.Add(1)
	if m.batchErr != nil {
		return m.batchErr
	}
	shards := map[string][]arcade.TxRecord{}
	var order []string
	for _, r := range recs {
		if _, ok := shards[r.TxID]; !ok {
			order = append(order, r.TxID)
		}
		shards[r.TxID] = append(shards[r.TxID], r)
	}
	var wg sync.WaitGroup
	for _, txid := range order {
		rs := shards[txid]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, r := range rs {
				_ = m.ApplyStatusUpdate(ctx, r) // in-order within the txid shard
			}
		}()
	}
	wg.Wait()
	return nil
}

// unused-but-required MonitoredStorage surface.
func (m *modelStorage) SendWaitingTransactions(context.Context, int) error           { return nil }
func (m *modelStorage) AbortAbandoned(context.Context, time.Time, int) error         { return nil }
func (m *modelStorage) SweepStaleReservations(context.Context, time.Time, int) error { return nil }
func (m *modelStorage) SynchronizeTransactionStatuses(context.Context, int) error    { return nil }
func (m *modelStorage) CheckProofs(context.Context, int) error                       { return nil }
func (m *modelStorage) DemoteReorgedProofs(context.Context, uint32) error            { return nil }
func (m *modelStorage) VerifyAndReleaseSuspects(context.Context, time.Duration, time.Duration, int) (defs.ReconcilerReport, error) {
	return defs.ReconcilerReport{}, nil
}

func (m *modelStorage) DrainOutbox(context.Context, int) (defs.OutboxDrainReport, error) {
	return defs.OutboxDrainReport{}, nil
}

func (m *modelStorage) GetKeyValue(_ context.Context, k string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.kv[k]
	return v, ok, nil
}

func (m *modelStorage) SetKeyValue(_ context.Context, k string, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[k] = v
	return nil
}

func (m *modelStorage) AcquireLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (m *modelStorage) ReleaseLease(context.Context, string, string) error { return nil }

func rec(txid string, status arcade.Status) arcade.StatusEvent {
	return arcade.StatusEvent{ID: txid + "-" + string(status), Record: arcade.TxRecord{TxID: txid, Status: status}}
}

func TestApplyStatusBatch_ShardsByTxidArrivalOrderWins(t *testing.T) {
	const (
		a = "aaaa" // (a) SEEN then MINED   → MINED (latest arrival) wins
		b = "bbbb" // (b) MINED then SEEN   → MINED (earlier); later stale SEEN is a no-op
		c = "cccc" // (c) REJECTED then MINED → MINED (latest arrival) wins
		d = "dddd" // (d) MINED then REJECTED → MINED; later stale REJECTED is a no-op
	)
	store := newModelStorage([]string{a, b, c, d})
	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(), WithoutDistributedLock())
	require.NoError(t, err)

	// One batch, arrival order interleaved across the four txids.
	batch := []arcade.StatusEvent{
		rec(a, arcade.StatusSeenOnNetwork),
		rec(b, arcade.StatusMined),
		rec(c, arcade.StatusDoubleSpendAttempted),
		rec(d, arcade.StatusMined),
		rec(a, arcade.StatusMined),
		rec(b, arcade.StatusSeenOnNetwork),
		rec(c, arcade.StatusMined),
		rec(d, arcade.StatusRejected),
	}

	var mu sync.Mutex
	cursor := ""
	dmn.applyStatusBatch(context.Background(), batch, &mu, &cursor)

	// Every txid ends at the MINED outcome — a mined tx is never clobbered by an
	// earlier-arrival stale frame in the same batch.
	for _, txid := range []string{a, b, c, d} {
		st := store.stateOf(txid)
		require.Equalf(t, "completed", st.status, "txid %s status", txid)
		require.Equalf(t, tierMined, st.tier, "txid %s tier not downgraded", txid)
		require.Equalf(t, string(arcade.StatusMined), st.arcadeStatus, "txid %s arcade_status not regressed", txid)
	}

	// The invariant that makes the guards correct: no two events for one txid
	// ever applied concurrently...
	require.False(t, store.sameTxidConcurrency.Load(), "same-txid events must not run concurrently")
	// ...while distinct txids DID run in parallel (the throughput win is kept).
	require.GreaterOrEqual(t, store.maxActive, 2, "distinct txids must still run in parallel")

	// Cursor advanced to the last event id of the batch.
	require.Equal(t, d+"-"+string(arcade.StatusRejected), cursor)
}

// TestApplyShards_FollowsWithApplyConcurrency proves the knob is LIVE. The shard
// count used to be a hardcoded `applyShards = 8` const while WithApplyConcurrency
// wrote a field nothing ever read, so a deployment that raised it still applied
// through 8 workers: the 16384-slot hand-off queue saturated, onEvent blocked the
// SSE reader, and arcade dropped events for us (~25k/s of "dropped events for
// slow SSE client") — which is how transactions ended up with no arcade status.
func TestApplyShards_FollowsWithApplyConcurrency(t *testing.T) {
	newDaemon := func(t *testing.T, opts ...Option) *Daemon {
		t.Helper()
		opts = append([]Option{WithoutDistributedLock()}, opts...)
		d, err := NewDaemon(logging.NewTestLogger(t), newModelStorage(nil), nil, nil, defs.DefaultMonitorConfig(), opts...)
		require.NoError(t, err)
		return d
	}

	require.Equal(t, defaultApplyWorkers, newDaemon(t).applyShards(), "unset falls back to the default")
	require.Equal(t, 64, newDaemon(t, WithApplyConcurrency(64)).applyShards(), "the configured value is what the pipeline uses")
	require.Equal(t, 1, newDaemon(t, WithApplyConcurrency(1)).applyShards())
	require.Equal(t, defaultApplyWorkers, newDaemon(t, WithApplyConcurrency(0)).applyShards(), "non-positive is ignored")
}

// TestApplyRecords_FansOutAcrossConfiguredShards is the behavioral half: the
// number of concurrent ApplyStatusBatch calls a single batch is split into
// tracks the configured concurrency, not the old constant.
func TestApplyRecords_FansOutAcrossConfiguredShards(t *testing.T) {
	records := make([]arcade.TxRecord, applyBatchMax)
	for i := range records {
		records[i] = arcade.TxRecord{TxID: fmt.Sprintf("%064x", i), Status: arcade.StatusSeenOnNetwork}
	}

	fanOut := func(t *testing.T, conc int) int {
		t.Helper()
		store := newModelStorage(nil)
		opts := []Option{WithoutDistributedLock()}
		if conc > 0 {
			opts = append(opts, WithApplyConcurrency(conc))
		}
		d, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(), opts...)
		require.NoError(t, err)
		require.NoError(t, d.applyRecords(context.Background(), records))
		return int(store.batchCalls.Load())
	}

	require.Equal(t, 1, fanOut(t, 1), "one worker applies the batch inline, in a single call")
	require.LessOrEqual(t, fanOut(t, 0), defaultApplyWorkers, "the default is still 8 shards")

	wide := fanOut(t, 32)
	require.Greater(t, wide, defaultApplyWorkers,
		"a batch must fan out past the old hardcoded 8 when WithApplyConcurrency raises it")
	require.LessOrEqual(t, wide, 32)
}

// TestApplyStatusBatch_FailedApplyHoldsCursor proves the replay cursor never
// advances past events that were not applied. It used to advance
// unconditionally — "replay is safe, the polls are the safety net" — but the
// polls could not reach a row that never got a status, so a failed batch's
// events were lost for good and its transactions diverged from arcade forever.
// Holding the cursor at the last FULLY-applied event means the worst case is
// re-delivery of already-applied (idempotent) events.
func TestApplyStatusBatch_FailedApplyHoldsCursor(t *testing.T) {
	const txid = "aaaa"
	store := newModelStorage([]string{txid})
	store.batchErr = errors.New("connection reset by peer")
	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(), WithoutDistributedLock())
	require.NoError(t, err)

	var mu sync.Mutex
	cursor := "id-before"
	batch := []arcade.StatusEvent{rec(txid, arcade.StatusMined)}

	dmn.applyStatusBatch(context.Background(), batch, &mu, &cursor)
	require.Equal(t, "id-before", cursor, "the in-memory cursor must not move past a failed batch")
	_, ok, _ := store.GetKeyValue(context.Background(), LastEventIDKey)
	require.False(t, ok, "the persisted cursor must not move past a failed batch")

	// Once the apply succeeds the cursor resumes advancing normally.
	store.batchErr = nil
	dmn.applyStatusBatch(context.Background(), batch, &mu, &cursor)
	require.Equal(t, txid+"-"+string(arcade.StatusMined), cursor)
	require.Equal(t, cursor, string(mustKV(t, store, LastEventIDKey)))
}

func mustKV(t *testing.T, m *modelStorage, key string) []byte {
	t.Helper()
	v, ok, err := m.GetKeyValue(context.Background(), key)
	require.NoError(t, err)
	require.True(t, ok, "key %s persisted", key)
	return v
}
