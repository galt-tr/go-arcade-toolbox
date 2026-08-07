package monitor_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv/mockarcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/monitor"
)

// --- mock storage ----------------------------------------------------------

type leaseRow struct {
	owner string
	until time.Time
}

// mockStorage is an in-memory MonitoredStorage. Its lease implements real CAS
// (owner + expiry under a mutex) so two daemons sharing one instance contend
// correctly; ApplyStatusUpdate / DemoteReorgedProofs record their calls.
type mockStorage struct {
	mu       sync.Mutex
	applied  []arcade.TxRecord
	demoted  []uint32
	kv       map[string][]byte
	leases   map[string]leaseRow
	applyErr error

	sendWaitingCalls atomic.Int32
}

func newMockStorage() *mockStorage {
	return &mockStorage{kv: map[string][]byte{}, leases: map[string]leaseRow{}}
}

func (m *mockStorage) ApplyStatusUpdate(_ context.Context, rec arcade.TxRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.applyErr != nil {
		return m.applyErr
	}
	m.applied = append(m.applied, rec)
	return nil
}

func (m *mockStorage) appliedTxIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.applied))
	for i := range m.applied {
		out[i] = m.applied[i].TxID
	}
	return out
}

func (m *mockStorage) hasApplied(txid string) bool {
	for _, id := range m.appliedTxIDs() {
		if id == txid {
			return true
		}
	}
	return false
}

func (m *mockStorage) demotedForks() []uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]uint32(nil), m.demoted...)
}

func (m *mockStorage) SendWaitingTransactions(_ context.Context, _ int) error {
	m.sendWaitingCalls.Add(1)
	time.Sleep(150 * time.Millisecond) // hold the lease for the run
	return nil
}

func (m *mockStorage) AbortAbandoned(context.Context, time.Time, int) error      { return nil }
func (m *mockStorage) SynchronizeTransactionStatuses(context.Context, int) error { return nil }
func (m *mockStorage) CheckProofs(context.Context, int) error                    { return nil }

func (m *mockStorage) DemoteReorgedProofs(_ context.Context, forkHeight uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.demoted = append(m.demoted, forkHeight)
	return nil
}

func (m *mockStorage) GetKeyValue(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.kv[key]
	return v, ok, nil
}

func (m *mockStorage) SetKeyValue(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[key] = append([]byte(nil), value...)
	return nil
}

func (m *mockStorage) getKV(key string) string {
	v, _, _ := m.GetKeyValue(context.Background(), key)
	return string(v)
}

func (m *mockStorage) AcquireLease(_ context.Context, job, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	l, ok := m.leases[job]
	if !ok || now.After(l.until) || l.owner == owner {
		m.leases[job] = leaseRow{owner: owner, until: now.Add(ttl)}
		return true, nil
	}
	return false, nil
}

func (m *mockStorage) ReleaseLease(_ context.Context, job, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.leases[job]; ok && l.owner == owner {
		l.until = time.Now()
		m.leases[job] = l
	}
	return nil
}

// --- mock oracle -----------------------------------------------------------

// stubOracle blocks its StreamStatus until the context is canceled (no events).
type stubOracle struct{}

func (stubOracle) Broadcast(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
	return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusSeenOnNetwork}, nil
}

func (stubOracle) GetTx(context.Context, string) (*arcade.TxRecord, error) {
	return nil, arcade.ErrTxNotFound
}

func (stubOracle) StreamStatus(ctx context.Context, _ string, _ func(arcade.StatusEvent) error) error {
	<-ctx.Done()
	return ctx.Err()
}

func (stubOracle) Health(context.Context) (*arcade.Health, error) {
	return &arcade.Health{Healthy: true}, nil
}

// scriptedOracle emits a preset batch on the first StreamStatus call, returns
// nil to trigger a reconnect, records the cursor of every call, and blocks on
// the second call — so a test can assert the cursor advanced across a reconnect.
type scriptedOracle struct {
	stubOracle
	events      []arcade.StatusEvent
	mu          sync.Mutex
	cursors     []string
	calls       int
	reconnected chan struct{}
	once        sync.Once
}

func (o *scriptedOracle) StreamStatus(ctx context.Context, lastEventID string, onEvent func(arcade.StatusEvent) error) error {
	o.mu.Lock()
	o.calls++
	call := o.calls
	o.cursors = append(o.cursors, lastEventID)
	o.mu.Unlock()

	if call == 1 {
		for _, ev := range o.events {
			_ = onEvent(ev)
		}
		return nil // trigger the reconnect loop
	}
	o.once.Do(func() { close(o.reconnected) })
	<-ctx.Done()
	return ctx.Err()
}

func (o *scriptedOracle) seenCursors() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.cursors...)
}

// --- fake chain subscriber -------------------------------------------------

type fakeSubscriber struct {
	tipIn   chan headers.TipEvent
	reorgIn chan headers.ReorgEvent
}

func newFakeSubscriber() *fakeSubscriber {
	return &fakeSubscriber{tipIn: make(chan headers.TipEvent), reorgIn: make(chan headers.ReorgEvent)}
}

func (f *fakeSubscriber) SubscribeTip(ctx context.Context) <-chan headers.TipEvent {
	out := make(chan headers.TipEvent)
	go forward(ctx, f.tipIn, out)
	return out
}

func (f *fakeSubscriber) SubscribeReorg(ctx context.Context) <-chan headers.ReorgEvent {
	out := make(chan headers.ReorgEvent)
	go forward(ctx, f.reorgIn, out)
	return out
}

// forward relays in→out until ctx is canceled, then closes out (the
// ChainSubscriber contract: channels close only on cancel).
func forward[T any](ctx context.Context, in <-chan T, out chan<- T) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-in:
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

// --- tests -----------------------------------------------------------------

func TestSSE_ResumeAndAdvancesCursorPerBatch(t *testing.T) {
	ctx := context.Background()
	store := newMockStorage()
	oracle := &scriptedOracle{
		reconnected: make(chan struct{}),
		events: []arcade.StatusEvent{
			{ID: "id-1", Record: arcade.TxRecord{TxID: "aa", Status: arcade.StatusSeenOnNetwork}},
			{ID: "id-2", Record: arcade.TxRecord{TxID: "bb", Status: arcade.StatusSeenOnNetwork}},
		},
	}
	d, err := monitor.NewDaemon(logging.NewTestLogger(t), store, nil, oracle, defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
	require.NoError(t, err)
	require.NoError(t, d.Start(ctx, nil))
	t.Cleanup(func() { _ = d.Stop() })

	// Both events applied.
	require.Eventually(t, func() bool {
		return store.hasApplied("aa") && store.hasApplied("bb")
	}, 3*time.Second, 10*time.Millisecond)

	// Cursor persisted to the newest event id of the batch.
	require.Eventually(t, func() bool {
		return store.getKV(monitor.LastEventIDKey) == "id-2"
	}, 3*time.Second, 10*time.Millisecond)

	// The reconnect resumes from the persisted cursor.
	select {
	case <-oracle.reconnected:
	case <-time.After(4 * time.Second):
		t.Fatal("stream never reconnected")
	}
	cursors := oracle.seenCursors()
	require.GreaterOrEqual(t, len(cursors), 2)
	require.Equal(t, "", cursors[0], "cold start has no cursor")
	require.Equal(t, "id-2", cursors[len(cursors)-1], "reconnect resumes from persisted cursor")
}

func TestSSE_DrainsMockArcadeEvents(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewTestLogger(t)
	arc := mockarcade.NewArcade(t)
	oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arc.URL(), EventsURL: arc.URL()})
	store := newMockStorage()

	d, err := monitor.NewDaemon(logger, store, nil, oracle, defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
	require.NoError(t, err)
	require.NoError(t, d.Start(ctx, nil))
	t.Cleanup(func() { _ = d.Stop() })

	// EmitStatus drops events with no connected subscriber, so keep emitting
	// until the (asynchronously connecting) pipeline drains one.
	require.Eventually(t, func() bool {
		arc.EmitStatus("cafe01", "SEEN_ON_NETWORK", nil)
		return store.hasApplied("cafe01")
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return store.getKV(monitor.LastEventIDKey) != ""
	}, 3*time.Second, 20*time.Millisecond, "cursor persisted after applying a real SSE event")
}

func TestReorgHandlerDemotesProofs(t *testing.T) {
	ctx := context.Background()
	store := newMockStorage()
	sub := newFakeSubscriber()
	d, err := monitor.NewDaemon(logging.NewTestLogger(t), store, sub, stubOracle{}, defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
	require.NoError(t, err)
	require.NoError(t, d.Start(ctx, nil))
	t.Cleanup(func() { _ = d.Stop() })

	sub.reorgIn <- headers.ReorgEvent{ForkHeight: 700123}

	require.Eventually(t, func() bool {
		for _, f := range store.demotedForks() {
			if f == 700123 {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
}

func TestTipHandlerTracksWatermark(t *testing.T) {
	ctx := context.Background()
	store := newMockStorage()
	sub := newFakeSubscriber()
	d, err := monitor.NewDaemon(logging.NewTestLogger(t), store, sub, stubOracle{}, defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
	require.NoError(t, err)
	require.NoError(t, d.Start(ctx, nil))
	t.Cleanup(func() { _ = d.Stop() })

	sub.tipIn <- headers.TipEvent{Height: 812345}
	require.Eventually(t, func() bool { return d.TipHeight() == 812345 }, 3*time.Second, 10*time.Millisecond)
}

func TestGracefulStop(t *testing.T) {
	ctx := context.Background()
	store := newMockStorage()
	sub := newFakeSubscriber()
	cfg := defs.DefaultMonitorConfig()
	d, err := monitor.NewDaemon(logging.NewTestLogger(t), store, sub, stubOracle{}, cfg)
	require.NoError(t, err)
	require.NoError(t, d.Start(ctx, cfg.Tasks.EnabledTasks()))

	time.Sleep(100 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- d.Stop() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung")
	}
	require.NoError(t, d.Stop(), "Stop is idempotent")
}

func TestLeaseLocker_AcquireContendRelease(t *testing.T) {
	ctx := context.Background()
	store := newMockStorage()
	lockerA := monitor.NewLeaseLocker(store, "A", logging.NewTestLogger(t))
	lockerB := monitor.NewLeaseLocker(store, "B", logging.NewTestLogger(t))

	lockA, err := lockerA.Lock(ctx, "job")
	require.NoError(t, err)
	require.NotNil(t, lockA)

	_, err = lockerB.Lock(ctx, "job")
	require.Error(t, err, "B is refused while A holds the lease")

	require.NoError(t, lockA.Unlock(ctx))

	lockB, err := lockerB.Lock(ctx, "job")
	require.NoError(t, err, "B acquires after A releases")
	require.NoError(t, lockB.Unlock(ctx))
}

func TestTwoDaemons_LeaseAdmitsOneRunPerSlot(t *testing.T) {
	ctx := context.Background()
	logger := logging.NewTestLogger(t)
	// One shared storage: shared lease table AND shared run counter.
	store := newMockStorage()

	tasks := map[defs.MonitorTask]defs.TaskConfig{
		defs.SendWaitingMonitorTask: {Enabled: true, IntervalSeconds: 1},
	}

	dA, err := monitor.NewDaemon(logger, store, nil, stubOracle{}, defs.DefaultMonitorConfig(), monitor.WithLeaseOwner("daemon-A"))
	require.NoError(t, err)
	dB, err := monitor.NewDaemon(logger, store, nil, stubOracle{}, defs.DefaultMonitorConfig(), monitor.WithLeaseOwner("daemon-B"))
	require.NoError(t, err)

	require.NoError(t, dA.Start(ctx, tasks))
	require.NoError(t, dB.Start(ctx, tasks))

	time.Sleep(3500 * time.Millisecond)

	require.NoError(t, dA.Stop())
	require.NoError(t, dB.Stop())

	total := store.sendWaitingCalls.Load()
	// The job must run across the two daemons (≥2), but the shared lease must
	// admit ~one run per 1s slot (≤5) — a vacuous per-pod lock would yield ~7-8.
	require.GreaterOrEqual(t, total, int32(2), "the job must run")
	require.LessOrEqual(t, total, int32(5), "the lease must admit roughly one run per slot")
}
