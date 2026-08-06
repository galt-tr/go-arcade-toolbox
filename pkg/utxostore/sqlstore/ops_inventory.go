package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
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
	if failed > 0 {
		return batchErr(failed, len(mints))
	}
	return nil
}

func (s *Store) mintOne(ctx context.Context, x queryer, m *utxostore.Mint) error {
	switch {
	case m.UserID <= 0:
		return fmt.Errorf("sqlstore: mint %s: user id must be positive", m.Outpoint)
	case m.Basket == "":
		return fmt.Errorf("sqlstore: mint %s: basket must be non-empty", m.Outpoint)
	case !m.Tier.Valid():
		return fmt.Errorf("sqlstore: mint %s: invalid tier %d", m.Outpoint, m.Tier)
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
// > frozen). Refusals are joined under ErrBatch; removable items in the same
// batch are still removed.
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
	return joinBatch(itemErrs)
}

func (s *Store) removeOne(ctx context.Context, x queryer, op utxostore.Outpoint, force bool) (itemErr, fatal error) {
	if force {
		if _, err := x.ExecContext(ctx, s.rebind("DELETE FROM utxos WHERE txid=? AND vout=?"), op.TxID[:], op.Vout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	var (
		spentBy    []byte
		reservedBy sql.NullString
		frozen     boolScan
	)
	q := s.rebind("SELECT spent_by, reserved_by, frozen FROM utxos WHERE txid=? AND vout=?" + s.forUpdate())
	err := x.QueryRowContext(ctx, q, op.TxID[:], op.Vout).Scan(&spentBy, &reservedBy, s.boolDest(&frozen))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // missing = no-op
	}
	if err != nil {
		return nil, err
	}

	switch {
	case len(spentBy) > 0:
		w, herr := decodeHash(spentBy)
		if herr != nil {
			return nil, herr
		}
		return &utxostore.SpentError{Op: op, Winner: *w}, nil
	case reservedBy.String != "":
		return &utxostore.ReservedError{Op: op, HeldBy: reservedBy.String}, nil
	case s.boolGet(frozen):
		return &utxostore.FrozenError{Op: op}, nil
	}

	if _, err := x.ExecContext(ctx, s.rebind("DELETE FROM utxos WHERE txid=? AND vout=?"), op.TxID[:], op.Vout); err != nil {
		return nil, err
	}
	return nil, nil
}
