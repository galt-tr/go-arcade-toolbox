package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// KeyValueRepo is a small opaque key→bytes store. The SSE cursor lives here.
type KeyValueRepo struct{ s *Store }

// KeyValue returns the key-value repository.
func (s *Store) KeyValue() KeyValueRepo { return KeyValueRepo{s} }

// Get returns the value for key and whether it was present. A missing key is
// (nil, false, nil), never an error.
func (r KeyValueRepo) Get(ctx context.Context, key string) ([]byte, bool, error) {
	q := r.s.rebind("SELECT value FROM key_values WHERE key = ?")
	var value []byte
	err := r.s.execer(ctx).QueryRowContext(ctx, q, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: kv get: %w", err)
	}
	return value, true, nil
}

// Set upserts value under key (by primary key).
func (r KeyValueRepo) Set(ctx context.Context, key string, value []byte) error {
	now := r.s.encTime(r.s.now())
	q := r.s.rebind(
		`INSERT INTO key_values (key, value, created_at, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`)
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, key, value, now, now); err != nil {
		return fmt.Errorf("metastore: kv set: %w", err)
	}
	return nil
}
