package monitor

// White-box test for the batch applier's per-txid sharding (the concurrency fix
// in applyStatusBatch). It co-batches, for the SAME txid, the four dangerous
// orderings and asserts arrival-order wins with a MINED tx never clobbered by an
// earlier-arrival stale frame — while distinct txids still run in parallel.
//
// modelStorage faithfully reproduces the exact hazard ApplyStatusUpdate has: the
// terminal/lattice guard is read from a snapshot taken OUTSIDE the write, three
// of the writes are unconditional (the tier set can LOWER a tier; the
// arcade-status and suspect writes are txid-only), and a sleep widens the
// read→write window. Under the OLD unsharded applier these tests fail (parallel
// same-txid workers race and a stale frame corrupts state); under per-txid
// sharding they pass deterministically.

import (
	"context"
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
