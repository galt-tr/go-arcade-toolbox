package monitor

import "time"

// Option configures a [Daemon] at construction.
type Option func(*Daemon)

// WithClock overrides the daemon clock (used for the abandoned-sweep cutoff and
// the lease timestamps). Intended for deterministic tests.
func WithClock(now func() time.Time) Option {
	return func(d *Daemon) { d.now = now }
}

// WithBatchLimit sets the per-run row limit shared by every sweep task
// (send-waiting, abort-abandoned, status/proof polling, reject-release). A
// non-positive value is ignored.
func WithBatchLimit(n int) Option {
	return func(d *Daemon) {
		if n > 0 {
			d.limits = limits{sendWaiting: n, abort: n, sync: n, proof: n, rejectRelease: n}
		}
	}
}

// WithApplyConcurrency sets how many SSE status events apply in parallel (still
// sharded by txid, so per-txid order is preserved) — it is the shard count
// [Daemon.applyShards] returns. Once recent block headers are cached (see
// [headers.WithCacheDepth]), proof application is DB-bound, so a sustained
// ~1000-TPS deployment must raise this above the default 8 for the apply
// pipeline to keep pace with mining: when the appliers cannot drain the hand-off
// queue, the SSE reader blocks and arcade drops events for us — which is how
// transactions end up with no arcade status at all. A non-positive value is
// ignored.
func WithApplyConcurrency(n int) Option {
	return func(d *Daemon) {
		if n > 0 {
			d.applyConcurrency = n
		}
	}
}

// WithReconcilerWindows overrides the reject→release reconciler's grace window
// (suspect age / two-pass separation) and max-quarantine ceiling
// (escalate-to-stuck). Non-positive values are ignored. Intended for
// deterministic tests; production reads these from [defs.Monitor.Reconciler].
func WithReconcilerWindows(grace, maxQuarantine time.Duration) Option {
	return func(d *Daemon) {
		if grace > 0 {
			d.suspectGrace = grace
		}
		if maxQuarantine > 0 {
			d.maxQuarantine = maxQuarantine
		}
	}
}

// WithFailAbandonedAge sets how old a never-broadcast transaction must be before
// the abort-abandoned sweep reaps it.
func WithFailAbandonedAge(age time.Duration) Option {
	return func(d *Daemon) {
		if age > 0 {
			d.failAbandonedAge = age
		}
	}
}

// WithLeaseOwner pins the distributed-lease owner name (otherwise a random
// per-instance name is used). Intended for deterministic tests.
func WithLeaseOwner(owner string) Option {
	return func(d *Daemon) { d.owner = owner }
}

// WithoutDistributedLock builds the scheduler with no distributed locker (a
// single-instance daemon). By default the daemon wires a SQL lease locker so
// exactly one instance runs each job across a multi-instance deployment.
func WithoutDistributedLock() Option {
	return func(d *Daemon) { d.distributedLock = false }
}
