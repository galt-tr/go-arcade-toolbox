package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Mint implements [utxostore.Store]. Each item is its own statement (its own
// commit when not in an ambient transaction), so a same-identity replay is a
// no-op success and an identity conflict fails only that item — successful
// items in the same batch are always committed.
func (s *Store) Mint(ctx context.Context, mints []*utxostore.Mint) error {
	if s.isClosed() {
		return errClosed
	}

	x := s.execer(ctx)
	failed := 0
	for _, m := range mints {
		m.Err = s.mintOne(ctx, x, m)
		if m.Err != nil {
			failed++
		}
	}
	return utxostore.BatchCountErr(failed, len(mints))
}

func (s *Store) mintOne(ctx context.Context, x queryer, m *utxostore.Mint) error {
	if err := utxostore.ValidateMint(m); err != nil {
		return err
	}

	res, err := s.insertMint(ctx, x, m)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil // freshly inserted
	}

	// Conflict: the outpoint already exists. Idempotency compares immutable
	// identity only (user, basket, satoshis, input size); tier and lifecycle
	// state may legitimately have moved on since the original mint.
	var (
		uid   int64
		bskt  string
		sats  uint64
		isize uint32
	)
	q := s.rebind("SELECT user_id, basket, satoshis, input_size FROM utxos WHERE txid=? AND vout=?")
	err = x.QueryRowContext(ctx, q, m.TxID[:], m.Vout).Scan(&uid, &bskt, &sats, &isize)
	if err != nil {
		return err
	}
	if uid == m.UserID && bskt == m.Basket && sats == m.Satoshis && isize == m.InputSize {
		return nil // idempotent same-identity replay
	}
	return &utxostore.AlreadyExistsError{Op: m.Outpoint}
}

// insertMint performs the ON CONFLICT DO NOTHING insert, supplying the seq
// value for SQLite (PostgreSQL generates it via IDENTITY).
func (s *Store) insertMint(ctx context.Context, x queryer, m *utxostore.Mint) (sql.Result, error) {
	created := s.encTime(s.now())
	if s.engine == EngineSQLite {
		var seq int64
		if err := x.QueryRowContext(ctx, "UPDATE utxo_seq SET val = val + 1 RETURNING val").Scan(&seq); err != nil {
			return nil, err
		}
		return x.ExecContext(ctx,
			`INSERT INTO utxos (txid, vout, user_id, basket, tier, satoshis, input_size, seq, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (txid, vout) DO NOTHING`,
			m.TxID[:], m.Vout, m.UserID, m.Basket, int64(m.Tier), m.Satoshis, m.InputSize, seq, created)
	}
	return x.ExecContext(ctx, s.rebind(
		`INSERT INTO utxos (txid, vout, user_id, basket, tier, satoshis, input_size, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (txid, vout) DO NOTHING`),
		m.TxID[:], m.Vout, m.UserID, m.Basket, int64(m.Tier), m.Satoshis, m.InputSize, created)
}

// Get implements [utxostore.Store].
func (s *Store) Get(ctx context.Context, op utxostore.Outpoint) (*utxostore.UTXO, error) {
	if s.isClosed() {
		return nil, errClosed
	}
	q := s.rebind("SELECT " + utxoCols + " FROM utxos WHERE txid=? AND vout=?")
	u, _, err := s.scanUTXO(s.execer(ctx).QueryRowContext(ctx, q, op.TxID[:], op.Vout))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &utxostore.NotFoundError{Op: op}
	}
	if err != nil {
		return nil, err
	}
	return u, nil
}

// Remove implements [utxostore.Store]. Missing rows are no-ops; without force,
// reserved/spent/frozen rows are refused per item (precedence spent > reserved
// > frozen). Refusals are joined under [utxostore.ErrBatch] — or
// [utxostore.ErrContention] when a row keeps flipping between removable and
// held, which is transient: retry the call. Removable items in the same batch
// are still removed.
func (s *Store) Remove(ctx context.Context, ops []utxostore.Outpoint, force bool) error {
	if s.isClosed() {
		return errClosed
	}

	var itemErrs []error
	err := s.withTx(ctx, func(x queryer) error {
		itemErrs = itemErrs[:0]
		for _, op := range ops {
			itemErr, fatal := s.removeOne(ctx, x, op, force)
			if fatal != nil {
				return fatal
			}
			if itemErr != nil {
				itemErrs = append(itemErrs, itemErr)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return utxostore.JoinBatch(itemErrs)
}

// removeOne deletes one row, write first: the guarded DELETE carries the same
// predicates the old pre-flight SELECT used to check, so the removable case is
// a single statement and only a refusal pays for a read. force is already
// unconditional — "remove it whatever state it is in" has no predicate worth
// reading first — and stays a lone DELETE.
func (s *Store) removeOne(ctx context.Context, x queryer, op utxostore.Outpoint, force bool) (itemErr, fatal error) {
	if force {
		if _, err := x.ExecContext(ctx, s.rebind("DELETE FROM utxos WHERE txid=? AND vout=?"), op.TxID[:], op.Vout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	for range guardAttempts {
		res, err := x.ExecContext(ctx, s.rebind(
			"DELETE FROM utxos WHERE txid=? AND vout=? AND reserved_by IS NULL AND spent_by IS NULL AND "+notFrozen),
			op.TxID[:], op.Vout)
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n == 1 {
			return nil, nil
		}
		classified, retry, cerr := s.classifyRemoveRefusal(ctx, x, op)
		if cerr != nil {
			return nil, cerr
		}
		if !retry {
			return classified, nil
		}
	}
	// The row kept looking removable between the guarded DELETE and the read
	// that followed it: a peer is cycling this coin's reservation right now.
	// Retryable, not a refusal — the caller re-drives the Remove rather than
	// recording a phantom row as un-removable.
	return fmt.Errorf("sqlstore: remove %s: %w", op, utxostore.ErrContention), nil
}

// classifyRemoveRefusal turns a guarded DELETE that matched nothing into the
// per-item verdict, or nil when the row is simply absent (a missing row is a
// no-op success, not an error). retry is true when the row looks removable
// again — a concurrent release raced the DELETE. Refusal precedence is spent >
// reserved > frozen, matching [Store.classifyForReserve] and the interface doc.
// The read is plain: the DELETE matched nothing, so it holds no lock worth
// extending.
func (s *Store) classifyRemoveRefusal(ctx context.Context, x queryer, op utxostore.Outpoint) (itemErr error, retry bool, fatal error) {
	var (
		spentBy    []byte
		reservedBy sql.NullString
		frozen     boolScan
	)
	q := s.rebind("SELECT spent_by, reserved_by, frozen FROM utxos WHERE txid=? AND vout=?")
	err := x.QueryRowContext(ctx, q, op.TxID[:], op.Vout).Scan(&spentBy, &reservedBy, s.boolDest(&frozen))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil // missing = no-op
	}
	if err != nil {
		return nil, false, err
	}

	switch {
	case len(spentBy) > 0:
		w, herr := decodeHash(spentBy)
		if herr != nil {
			return nil, false, herr
		}
		return &utxostore.SpentError{Op: op, Winner: *w}, false, nil
	case reservedBy.String != "":
		return &utxostore.ReservedError{Op: op, HeldBy: reservedBy.String}, false, nil
	case s.boolGet(frozen):
		return &utxostore.FrozenError{Op: op}, false, nil
	default:
		return nil, true, nil
	}
}
