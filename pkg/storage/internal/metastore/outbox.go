package metastore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// maxOutboxAttempts is the ceiling past which an outbox row is no longer
// handed out by FetchPending (it drops out of the pending partial index). It
// matches the idx_utxo_ops_outbox_pending predicate (attempts < 10).
const maxOutboxAttempts = 10

// OutboxRepo is the crash-recovery outbox for utxostore operations that must be
// replayed after a metadata-side commit (the durable half of the two-store
// write). Rows are keyed by (txid, op_type, chunk); Enqueue dedupes on that key.
type OutboxRepo struct{ s *Store }

// Outbox returns the utxo-ops outbox repository.
func (s *Store) Outbox() OutboxRepo { return OutboxRepo{s} }

// OutboxEntry is one pending outbox row.
type OutboxEntry struct {
	TxID   []byte
	OpType string
	Chunk  int
	// Payload MUST be valid JSON. The column is JSONB on PostgreSQL and TEXT on
	// SQLite; non-JSON bytes pass silently on SQLite but are rejected at write
	// time on PostgreSQL, so the caller is responsible for serializing replay
	// ops as JSON (they always are — the payloads are ours).
	Payload   []byte
	Attempts  int
	LastError *string
	CreatedAt time.Time
}

// Enqueue records an op to replay, deduplicating on (txid, op_type, chunk): a
// re-enqueue of the same key is a no-op success (ON CONFLICT DO NOTHING), which
// is what makes the enqueue safe to retry. payload MUST be valid JSON (see
// [OutboxEntry.Payload]).
func (r OutboxRepo) Enqueue(ctx context.Context, txid []byte, opType string, chunk int, payload []byte) error {
	q := r.s.rebind(
		`INSERT INTO utxo_ops_outbox (txid, op_type, chunk, payload, attempts, created_at)
		 VALUES (?, ?, ?, ?, 0, ?)
		 ON CONFLICT (txid, op_type, chunk) DO NOTHING`)
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, txid, opType, chunk, string(payload), r.s.encTime(r.s.now())); err != nil {
		return fmt.Errorf("metastore: outbox enqueue: %w", err)
	}
	return nil
}

// FetchPending claims up to limit not-yet-exhausted rows (attempts < 10),
// oldest first. On PostgreSQL it uses FOR UPDATE SKIP LOCKED so concurrent
// workers get DISJOINT batches without blocking; call it inside a [Store.Do]
// (or another ambient transaction) so the row locks are held until the same
// transaction MarkDone-s or commits. SQLite's single writer connection makes
// the batch exclusive without a lock clause.
func (r OutboxRepo) FetchPending(ctx context.Context, limit int) ([]OutboxEntry, error) {
	if limit <= 0 {
		return nil, nil
	}
	q := r.s.rebind(
		`SELECT txid, op_type, chunk, payload, attempts, last_error, created_at
		 FROM utxo_ops_outbox
		 WHERE attempts < ?
		 ORDER BY created_at ASC, op_type ASC, chunk ASC
		 LIMIT ?` + r.s.forUpdateSkipLocked())
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, maxOutboxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("metastore: outbox fetch: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []OutboxEntry
	for rows.Next() {
		var (
			e         OutboxEntry
			lastError sql.NullString
			createdAt tsScan
		)
		if err := rows.Scan(&e.TxID, &e.OpType, &e.Chunk, &e.Payload, &e.Attempts,
			&lastError, r.s.tsDest(&createdAt)); err != nil {
			return nil, err
		}
		e.LastError = nullStr(lastError)
		e.CreatedAt = r.s.tsTime(createdAt)
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkDone removes a completed row (the op was successfully replayed).
func (r OutboxRepo) MarkDone(ctx context.Context, txid []byte, opType string, chunk int) error {
	q := r.s.rebind("DELETE FROM utxo_ops_outbox WHERE txid = ? AND op_type = ? AND chunk = ?")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, txid, opType, chunk); err != nil {
		return fmt.Errorf("metastore: outbox mark done: %w", err)
	}
	return nil
}

// RecordError bumps a row's attempt counter and records the last error, leaving
// it for a later retry (until attempts reaches the ceiling and it drops out of
// the pending set).
func (r OutboxRepo) RecordError(ctx context.Context, txid []byte, opType string, chunk int, errMsg string) error {
	q := r.s.rebind(
		"UPDATE utxo_ops_outbox SET attempts = attempts + 1, last_error = ? WHERE txid = ? AND op_type = ? AND chunk = ?")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, errMsg, txid, opType, chunk); err != nil {
		return fmt.Errorf("metastore: outbox record error: %w", err)
	}
	return nil
}

// CountPending returns how many rows are still eligible for FetchPending
// (attempts < 10). Intended for tests and metrics.
func (r OutboxRepo) CountPending(ctx context.Context) (int, error) {
	q := r.s.rebind("SELECT COUNT(*) FROM utxo_ops_outbox WHERE attempts < ?")
	var n int
	if err := r.s.execer(ctx).QueryRowContext(ctx, q, maxOutboxAttempts).Scan(&n); err != nil {
		return 0, fmt.Errorf("metastore: outbox count: %w", err)
	}
	return n, nil
}
