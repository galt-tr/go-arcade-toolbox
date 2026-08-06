package sqlkit

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	sqlitedrv "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// maxLockRetries bounds the retry loop for lock/deadlock/serialization errors on
// the guarded (non-claim) mutations. It is single-sourced here because, in Mode
// A, the outer transaction owner's retry governs both stores' statement chains —
// the two must agree on how many times and how long to wait.
const maxLockRetries = 3

// WithRetry runs fn, retrying up to maxLockRetries times with exponential
// backoff (100ms << attempt: 100ms, 200ms, 400ms) while it returns a
// lock/deadlock/serialization error [IsLockError] classifies. It aborts early if
// ctx is canceled during a backoff.
//
// Callers open the transaction inside fn: a store reuses an ambient transaction
// WITHOUT retry (the owner handles that) and only wraps a transaction it opens
// itself in WithRetry, so fn must be safe to re-run.
func WithRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		if err == nil || !IsLockError(err) || attempt >= maxLockRetries {
			return err
		}
		backoff := time.Duration(100<<attempt) * time.Millisecond // 100ms, 200ms, 400ms
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// IsLockError classifies transient lock/deadlock/serialization errors that a
// bounded retry can clear. It mirrors teranode's classifier, adapted to the pgx
// (not lib/pq) and modernc (extended result codes) drivers these stores use.
func IsLockError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 40001 serialization_failure, 40P01 deadlock_detected,
		// 55P03 lock_not_available.
		return pgErr.Code == "40001" || pgErr.Code == "40P01" || pgErr.Code == "55P03"
	}
	var liteErr *sqlitedrv.Error
	if errors.As(err, &liteErr) {
		// modernc enables extended result codes; mask to the primary code.
		code := liteErr.Code() & 0xFF
		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "deadlock")
}
