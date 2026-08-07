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
// (send-waiting, abort-abandoned, status/proof polling). A non-positive value
// is ignored.
func WithBatchLimit(n int) Option {
	return func(d *Daemon) {
		if n > 0 {
			d.limits = limits{sendWaiting: n, abort: n, sync: n, proof: n}
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
