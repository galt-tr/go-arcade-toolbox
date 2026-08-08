package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/randomizer"
)

const (
	// defaultBatchLimit caps how many rows each sweep task processes per run.
	defaultBatchLimit = 256
	// defaultFailAbandonedAge is how old a never-broadcast tx must be before the
	// abort-abandoned sweep reaps it.
	defaultFailAbandonedAge = 5 * time.Minute
	// defaultStaleReservationTTL is how old a funding reservation must be before
	// the stale-reservation sweep reclaims it (well beyond the re-drive window,
	// so a tx that can still be re-broadcast keeps its inputs).
	defaultStaleReservationTTL = 15 * time.Minute
	// minLeaseTTL floors a job's lease TTL (max(2*interval, minLeaseTTL)) so a
	// short interval cannot expire a lease mid-run and admit a second instance.
	minLeaseTTL = 5 * time.Minute
	// ownerNameLen is the byte length of the random per-instance lease owner.
	ownerNameLen = 12
)

// limits holds the per-run row cap for each sweep task.
type limits struct {
	sendWaiting   int
	abort         int
	sync          int
	proof         int
	rejectRelease int
}

// Daemon is the background monitor: a gocron scheduler running leased fallback
// tasks, plus long-lived consumers of the arcade status SSE stream and the
// chaintracks tip/reorg streams. It drives the async transaction lifecycle
// through a [MonitoredStorage]. Safe for concurrent use; not restartable after
// Stop.
type Daemon struct {
	scheduler gocron.Scheduler
	logger    *slog.Logger

	storage MonitoredStorage
	headers headers.ChainSubscriber
	oracle  arcade.TxOracle
	cfg     defs.Monitor

	locker          *LeaseLocker
	owner           string
	distributedLock bool

	now                 func() time.Time
	limits              limits
	failAbandonedAge    time.Duration
	staleReservationTTL time.Duration
	suspectGrace        time.Duration
	maxQuarantine       time.Duration
	applyConcurrency    int // parallel SSE status-apply workers (WithApplyConcurrency)

	startLock sync.Mutex
	started   bool
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// tipHeight is the chain-tip watermark tracked from the tip stream, used by
	// staleness math and exposed for observability.
	tipHeight atomic.Uint32
}

// NewDaemon builds a monitor Daemon over the given subsystems. By default it
// wires a SQL lease locker (via storage) so exactly one instance runs each
// scheduled job across a multi-instance deployment; override with
// [WithoutDistributedLock]. headers may be nil to disable the tip/reorg
// consumers.
func NewDaemon(
	logger *slog.Logger,
	storage MonitoredStorage,
	hdrs headers.ChainSubscriber,
	oracle arcade.TxOracle,
	cfg defs.Monitor,
	opts ...Option,
) (*Daemon, error) {
	if storage == nil {
		return nil, fmt.Errorf("monitor: storage is required")
	}
	d := &Daemon{
		logger:              logging.Child(logger, "monitor"),
		storage:             storage,
		headers:             hdrs,
		oracle:              oracle,
		cfg:                 cfg,
		distributedLock:     true,
		now:                 time.Now,
		limits:              limits{sendWaiting: defaultBatchLimit, abort: defaultBatchLimit, sync: defaultBatchLimit, proof: defaultBatchLimit, rejectRelease: defaultBatchLimit},
		applyConcurrency:    defaultApplyWorkers,
		failAbandonedAge:    defaultFailAbandonedAge,
		staleReservationTTL: defaultStaleReservationTTL,
		suspectGrace:        cfg.Reconciler.SuspectGrace(),
		maxQuarantine:       cfg.Reconciler.MaxQuarantine(),
	}
	for _, o := range opts {
		o(d)
	}
	if d.owner == "" {
		owner, err := randomizer.New().Base64(ownerNameLen)
		if err != nil {
			return nil, fmt.Errorf("monitor: generate lease owner: %w", err)
		}
		d.owner = owner
	}

	var schedOpts []gocron.SchedulerOption
	if d.distributedLock {
		d.locker = NewLeaseLocker(storage, d.owner, d.logger)
		schedOpts = append(schedOpts, gocron.WithDistributedLocker(d.locker))
	}
	sched, err := gocron.NewScheduler(schedOpts...)
	if err != nil {
		return nil, fmt.Errorf("monitor: create scheduler: %w", err)
	}
	d.scheduler = sched
	return d, nil
}

// Start registers and starts the given scheduled tasks (typically
// cfg.Tasks.EnabledTasks()), launches the SSE apply pipeline (when an oracle is
// set) and the tip/reorg consumers (when headers is set), and starts the
// scheduler. It is idempotent: a second call while running is a no-op. Start is
// terminal-once — the daemon cannot be Started again after Stop.
func (d *Daemon) Start(ctx context.Context, tasks map[defs.MonitorTask]defs.TaskConfig) error {
	d.startLock.Lock()
	defer d.startLock.Unlock()
	if d.started {
		d.logger.WarnContext(ctx, "monitor already started")
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel

	for name, cfg := range tasks {
		if err := d.registerTask(runCtx, name, cfg); err != nil {
			cancel()
			return err
		}
	}

	if d.oracle != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.handleStatusEvents(runCtx)
		}()
	}
	if d.headers != nil {
		d.wg.Add(2)
		go d.handleTips(runCtx)
		go d.handleReorgs(runCtx)
	}

	d.scheduler.Start()
	d.started = true
	d.logger.InfoContext(ctx, "monitor started", slog.Int("tasks", len(tasks)), slog.Bool("distributedLock", d.distributedLock))
	return nil
}

// registerTask wires one scheduled task into gocron: a fixed-interval job named
// for the distributed lock key, in singleton reschedule mode (so one process
// never overlaps a job — the premise the lease relies on). When a lease locker
// is present it registers a per-job TTL of max(2*interval, minLeaseTTL).
func (d *Daemon) registerTask(ctx context.Context, name defs.MonitorTask, cfg defs.TaskConfig) error {
	runner, ok := d.taskRunner(ctx, name)
	if !ok {
		d.logger.WarnContext(ctx, "monitor: skipping unknown task", slog.String("task", string(name)))
		return nil
	}
	interval := cfg.Interval()
	if interval <= 0 {
		return fmt.Errorf("monitor: task %s has non-positive interval", name)
	}
	jobName := monitorJobName(name)

	if d.locker != nil {
		ttl := 2 * interval
		if ttl < minLeaseTTL {
			ttl = minLeaseTTL
		}
		d.locker.SetLeaseTTL(jobName, ttl)
	}

	jobOpts := []gocron.JobOption{
		gocron.WithName(jobName),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	}
	if cfg.StartImmediately {
		jobOpts = append(jobOpts, gocron.WithStartAt(gocron.WithStartImmediately()))
	}

	if _, err := d.scheduler.NewJob(gocron.DurationJob(interval), gocron.NewTask(runner), jobOpts...); err != nil {
		return fmt.Errorf("monitor: register task %s: %w", name, err)
	}
	return nil
}

// taskRunner returns the run function for a task name, and whether the name is
// known. The returned func is what gocron ticks; it runs against the daemon's
// lifecycle context so a Stop cancels an in-flight run.
func (d *Daemon) taskRunner(ctx context.Context, name defs.MonitorTask) (func(), bool) {
	var run func() error
	switch name {
	case defs.SendWaitingMonitorTask:
		run = func() error { return d.storage.SendWaitingTransactions(ctx, d.limits.sendWaiting) }
	case defs.FailAbandonedMonitorTask:
		run = func() error {
			if err := d.storage.AbortAbandoned(ctx, d.now().Add(-d.failAbandonedAge), d.limits.abort); err != nil {
				return err
			}
			// Reclaim reservations leaked by never-completed CreateActions
			// (failed fan-outs, or txs stranded by an open circuit breaker).
			return d.storage.SweepStaleReservations(ctx, d.now().Add(-d.staleReservationTTL), d.limits.abort)
		}
	case defs.CheckForProofsMonitorTask:
		run = func() error { return d.storage.CheckProofs(ctx, d.limits.proof) }
	case defs.UnFailMonitorTask:
		// Repurposed as the status poll fallback (the arcade analog of the old
		// "un-fail" re-verify sweep): re-poll stale non-terminal txs via GetTx.
		run = func() error { return d.storage.SynchronizeTransactionStatuses(ctx, d.limits.sync) }
	case defs.RejectReleaseMonitorTask:
		// The reject→release reconciler. Each pass first drains the utxo-ops outbox
		// (Mode B crash recovery for a release that crashed mid-inline-execute),
		// then re-verifies and releases provably-dead suspects.
		run = func() error { return d.runRejectRelease(ctx) }
	default:
		return nil, false
	}
	return func() {
		if err := run(); err != nil {
			d.logger.ErrorContext(ctx, "monitor task failed", slog.String("task", string(name)), slog.String("error", err.Error()))
		}
	}, true
}

// runRejectRelease runs one reject→release reconciler pass: drain the outbox
// (heal any Mode B release that crashed mid-execute), then re-verify and release
// provably-dead suspects. The report counters are logged as the reconciler's
// metrics (reconciler_released_total / reconciler_false_positive_total /
// reconciler_stuck_total). A drain error does not abort the reconcile.
func (d *Daemon) runRejectRelease(ctx context.Context) error {
	if drain, err := d.storage.DrainOutbox(ctx, d.limits.rejectRelease); err != nil {
		d.logger.WarnContext(ctx, "reconciler: outbox drain failed", slog.String("error", err.Error()))
	} else if drain.Drained > 0 || drain.Parked > 0 {
		d.logger.InfoContext(ctx, "reconciler: outbox drained",
			slog.Int("drained", drain.Drained), slog.Int("failed", drain.Failed), slog.Int("parked", drain.Parked))
	}

	report, err := d.storage.VerifyAndReleaseSuspects(ctx, d.suspectGrace, d.maxQuarantine, d.limits.rejectRelease)
	if err != nil {
		return err
	}
	if report.Scanned > 0 {
		d.logger.InfoContext(ctx, "reconciler pass complete",
			slog.Int("scanned", report.Scanned),
			slog.Int("reconciler_released_total", report.Released),
			slog.Int("reconciler_false_positive_total", report.FalsePositive),
			slog.Int("reconciler_stuck_total", report.Stuck),
			slog.Int("ambiguous", report.Ambiguous),
			slog.Int("cascaded", report.Cascaded))
	}
	return nil
}

// handleTips tracks the chain-tip watermark from the tip stream. Best-effort
// per the headers contract; the channel closes only when the context cancels.
func (d *Daemon) handleTips(ctx context.Context) {
	defer d.wg.Done()
	for ev := range d.headers.SubscribeTip(ctx) {
		d.tipHeight.Store(ev.Height)
		d.logger.DebugContext(ctx, "chain tip advanced", slog.Uint64("height", uint64(ev.Height)))
	}
}

// handleReorgs demotes stored proofs invalidated by a reorg. Best-effort /
// advisory per the headers contract: a missed event is caught by the proof
// poll, which is the authority.
func (d *Daemon) handleReorgs(ctx context.Context) {
	defer d.wg.Done()
	for ev := range d.headers.SubscribeReorg(ctx) {
		d.logger.InfoContext(ctx, "reorg detected, demoting proofs at/above fork",
			slog.Uint64("forkHeight", uint64(ev.ForkHeight)),
			slog.Int("orphaned", len(ev.OrphanedHashes)))
		if err := d.storage.DemoteReorgedProofs(ctx, ev.ForkHeight); err != nil {
			d.logger.WarnContext(ctx, "reorg demote failed", slog.String("error", err.Error()))
		}
	}
}

// Stop cancels the lifecycle context (stopping the SSE pipeline and the
// tip/reorg consumers, and canceling any in-flight task run), shuts the
// scheduler down, and waits for the background goroutines to drain. It is
// idempotent and terminal — the daemon cannot be restarted.
func (d *Daemon) Stop() error {
	d.startLock.Lock()
	defer d.startLock.Unlock()
	if !d.started {
		return nil
	}
	if d.cancel != nil {
		d.cancel()
	}
	err := d.scheduler.Shutdown()
	d.wg.Wait()
	d.started = false
	if err != nil {
		return fmt.Errorf("monitor: shutdown: %w", err)
	}
	return nil
}

// TipHeight returns the last chain-tip height seen on the tip stream (0 before
// the first tip).
func (d *Daemon) TipHeight() uint32 {
	return d.tipHeight.Load()
}

// monitorJobName is the gocron job name and distributed-lock key for a task.
func monitorJobName(name defs.MonitorTask) string {
	return "monitor_" + string(name)
}
