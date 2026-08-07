package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
)

// defaultLeaseTTL is used for a job whose TTL was not set via SetLeaseTTL.
// Daemon.Start wires a per-job TTL of max(2*interval, 5m); this default only
// matters for a locker driven outside that path.
const defaultLeaseTTL = 10 * time.Minute

// LeaseLocker is a gocron.Locker backed by a per-job lease row in the shared
// metadata database (via [MonitoredStorage.AcquireLease]/ReleaseLease). Every
// instance contends on the SAME stable key per job, so exactly one instance
// runs a job at a time and a crashed owner is reclaimed after its lease
// expires. The SQL two-step (ensure-row + CAS) lives in the metastore; this
// type only owns the owner identity and per-job TTLs.
//
// The unusual placement — the lease SQL in the storage layer rather than a GORM
// model here — keeps the *sql.DB and dialect handling encapsulated behind
// MonitoredStorage (the daemon only ever sees the interface).
type LeaseLocker struct {
	storage MonitoredStorage
	owner   string
	logger  *slog.Logger

	mu   sync.RWMutex
	ttls map[string]time.Duration
}

// NewLeaseLocker creates a LeaseLocker for the given owner (which must be unique
// per running instance — the daemon uses a random worker name).
func NewLeaseLocker(storage MonitoredStorage, owner string, logger *slog.Logger) *LeaseLocker {
	return &LeaseLocker{
		storage: storage,
		owner:   owner,
		logger:  logging.Child(logger, "lease_locker"),
		ttls:    make(map[string]time.Duration),
	}
}

// SetLeaseTTL sets the lease duration for a specific job key. Daemon.Start calls
// it once task intervals are known; the key must match the gocron job name used
// as the lock key (see monitorJobName).
func (l *LeaseLocker) SetLeaseTTL(jobName string, ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ttls[jobName] = ttl
}

func (l *LeaseLocker) ttlFor(jobName string) time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if ttl, ok := l.ttls[jobName]; ok && ttl > 0 {
		return ttl
	}
	return defaultLeaseTTL
}

// Lock acquires the lease for key. It succeeds when no row exists, when this
// instance already owns the row (gocron never overlaps runs of one job within a
// single process, so same-owner re-acquire is the normal renewal path; the
// lease guards ACROSS processes), or when the existing lease expired. When
// another instance holds an unexpired lease it returns an error — gocron then
// skips this run and reschedules.
func (l *LeaseLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	acquired, err := l.storage.AcquireLease(ctx, key, l.owner, l.ttlFor(key))
	if err != nil {
		return nil, fmt.Errorf("lease acquire failed for %s: %w", key, err)
	}
	if !acquired {
		l.logger.DebugContext(ctx, "monitor job skipped: lease held by another instance", slog.String("job", key))
		return nil, fmt.Errorf("lease for %s held by another instance", key)
	}
	return &leaseHandle{locker: l, key: key}, nil
}

// leaseHandle is the gocron.Lock returned on a successful claim.
type leaseHandle struct {
	locker *LeaseLocker
	key    string
}

// Unlock releases the lease early (the metastore only expires it if this owner
// still holds it). gocron defers Unlock after every run; the TTL is the
// crash-safety backstop.
func (h *leaseHandle) Unlock(ctx context.Context) error {
	if err := h.locker.storage.ReleaseLease(ctx, h.key, h.locker.owner); err != nil {
		return fmt.Errorf("lease release failed for %s: %w", h.key, err)
	}
	return nil
}
