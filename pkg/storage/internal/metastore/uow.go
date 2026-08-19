package metastore

import (
	"context"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
)

// Do runs fn inside a single transaction — the Unit of Work. It opens one
// *sql.Tx over the store's *sql.DB, stashes it on the context via
// internal/sqltx, and commits when fn returns nil or rolls back when fn returns
// an error (nothing is persisted on error). Every repository call fn makes with
// the provided context runs inside that transaction; so does any co-located
// [sqlstore.Store] call over the SAME *sql.DB (Mode A) made with the same
// context — that is the whole point of the shared-transaction seam.
//
// When it owns the transaction (no ambient one already on ctx) Do retries the
// whole callback on a transient lock/deadlock/serialization error, up to a
// bounded number of attempts. fn must therefore be idempotent enough to re-run;
// the repository mutations here are.
//
// If ctx already carries an ambient transaction opened over this store's own
// *sql.DB (a surrounding Do, or a caller-managed one on the same handle), Do
// reuses it and does NOT commit, roll back, or retry — the outermost owner is
// solely responsible for the transaction's fate. This makes Do safely
// nestable. An ambient transaction opened over a DIFFERENT *sql.DB is not
// reused — sqltx's ownership check rejects it — so Do opens and owns its own
// transaction instead, exactly as if ctx carried none.
func (s *Store) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if s.isClosed() {
		return errClosed
	}
	if _, ok := sqltx.From(ctx, s.db); ok {
		return fn(ctx)
	}
	return sqlkit.WithRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(sqltx.With(ctx, tx, s.db)); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}
