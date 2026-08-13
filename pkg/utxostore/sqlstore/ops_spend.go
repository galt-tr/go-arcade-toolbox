package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Spend implements [utxostore.Store]: reserved(reservation) -> spent(txid),
// idempotent for the same spender, [utxostore.SpentError] for a different one.
// Guard precedence matches the interface: an already-recorded spend is checked
// BEFORE the freeze, so a same-spender replay on a since-frozen row succeeds.
func (s *Store) Spend(ctx context.Context, spends []*utxostore.SpendOp) error {
	if s.isClosed() {
		return errClosed
	}

	var failed int
	err := s.withTx(ctx, func(x queryer) error {
		failed = 0
		for _, sp := range spends {
			itemErr, fatal := s.spendOne(ctx, x, sp)
			if fatal != nil {
				return fatal
			}
			sp.Err = itemErr
			if itemErr != nil {
				failed++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if failed > 0 {
		return batchErr(failed, len(spends))
	}
	return nil
}

func (s *Store) spendOne(ctx context.Context, x queryer, sp *utxostore.SpendOp) (itemErr, fatal error) {
	if sp.Reservation == "" {
		return fmt.Errorf("sqlstore: spend %s: reservation must be non-empty", sp.Outpoint), nil
	}

	var (
		spentBy    []byte
		frozen     boolScan
		reservedBy sql.NullString
	)
	q := s.rebind("SELECT spent_by, frozen, reserved_by FROM utxos WHERE txid=? AND vout=?" + s.forUpdate())
	err := x.QueryRowContext(ctx, q, sp.TxID[:], sp.Vout).Scan(&spentBy, s.boolDest(&frozen), &reservedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return &utxostore.NotFoundError{Op: sp.Outpoint}, nil
	}
	if err != nil {
		return nil, err
	}

	// Precedence: recorded spend wins over freeze.
	if len(spentBy) > 0 {
		w, herr := decodeHash(spentBy)
		if herr != nil {
			return nil, herr
		}
		if *w == sp.SpendingTxID {
			return nil, nil // idempotent same-spender replay
		}
		return &utxostore.SpentError{Op: sp.Outpoint, Winner: *w}, nil
	}
	if s.boolGet(frozen) {
		return &utxostore.FrozenError{Op: sp.Outpoint}, nil
	}
	if reservedBy.String != sp.Reservation {
		return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: reservedBy.String}, nil
	}

	if _, err := x.ExecContext(ctx, s.rebind("UPDATE utxos SET spent_by=? WHERE txid=? AND vout=?"),
		sp.SpendingTxID[:], sp.TxID[:], sp.Vout); err != nil {
		return nil, err
	}
	return nil, nil
}

// Unspend implements [utxostore.Store]: for each op whose row is spent by
// spendingTxID, clears the spend AND the reservation, returning the coin to the
// claimable pool. Guard mismatches and missing outpoints are skips. Frozen rows
// still apply (the freeze stays in place).
func (s *Store) Unspend(ctx context.Context, spendingTxID chainhash.Hash, ops []utxostore.Outpoint) (int, error) {
	if s.isClosed() {
		return 0, errClosed
	}

	var released int
	err := s.withTx(ctx, func(x queryer) error {
		released = 0
		for _, op := range ops {
			res, err := x.ExecContext(ctx, s.rebind(
				`UPDATE utxos SET spent_by=NULL, reserved_by=NULL, reserved_at=NULL
				 WHERE txid=? AND vout=? AND spent_by=?`),
				op.TxID[:], op.Vout, spendingTxID[:])
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			released += int(n)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// Promote implements [utxostore.Store]: retiers rows in either direction;
// missing rows are skipped, rows already at the target tier do not count.
func (s *Store) Promote(ctx context.Context, ops []utxostore.Outpoint, to utxostore.Tier) (int, error) {
	if s.isClosed() {
		return 0, errClosed
	}
	if !to.Valid() {
		return 0, fmt.Errorf("sqlstore: invalid target tier %d", to)
	}

	var changed int
	err := s.withTx(ctx, func(x queryer) error {
		changed = 0
		for _, op := range ops {
			res, err := x.ExecContext(ctx, s.rebind(
				"UPDATE utxos SET tier=? WHERE txid=? AND vout=? AND tier<>?"),
				int64(to), op.TxID[:], op.Vout, int64(to))
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			changed += int(n)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

// RemoveSpentBy implements [utxostore.Store]: deletes every row spent by
// spendingTxID, a now-terminal (mined) tx whose inputs are permanently
// consumed. One indexed DELETE via idx_utxos_spent_by; Spend stores spent_by as
// SpendingTxID[:], so the match is byte-exact. Idempotent.
func (s *Store) RemoveSpentBy(ctx context.Context, spendingTxID chainhash.Hash) (int, error) {
	if s.isClosed() {
		return 0, errClosed
	}
	var removed int64
	err := s.withTx(ctx, func(x queryer) error {
		res, err := x.ExecContext(ctx, s.rebind("DELETE FROM utxos WHERE spent_by=?"), spendingTxID[:])
		if err != nil {
			return err
		}
		removed, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("sqlstore: remove spent by %s: %w", spendingTxID, err)
	}
	return int(removed), nil
}

// RemoveByMintTx implements [utxostore.Store]: removes phantom coins of an
// invalidated mint transaction and classifies the survivors. Every op must be
// an output of mintTxID; a mismatch fails the whole call with a plain error and
// removes nothing.
func (s *Store) RemoveByMintTx(ctx context.Context, mintTxID chainhash.Hash, ops []utxostore.Outpoint) (utxostore.RemoveByMintReport, error) {
	var report utxostore.RemoveByMintReport
	if s.isClosed() {
		return report, errClosed
	}
	for _, op := range ops {
		if op.TxID != mintTxID {
			return report, fmt.Errorf("sqlstore: outpoint %s is not an output of mint tx %s", op, mintTxID.String())
		}
	}

	err := s.withTx(ctx, func(x queryer) error {
		report = utxostore.RemoveByMintReport{}
		reservedRefs := make(map[string]*utxostore.ReservationRef)
		var refOrder []string
		seenSpenders := make(map[chainhash.Hash]bool)

		for _, op := range ops {
			var (
				spentBy    []byte
				reservedBy sql.NullString
				reservedAt tsScan
				userID     int64
			)
			q := s.rebind("SELECT spent_by, reserved_by, reserved_at, user_id FROM utxos WHERE txid=? AND vout=?" + s.forUpdate())
			err := x.QueryRowContext(ctx, q, op.TxID[:], op.Vout).
				Scan(&spentBy, &reservedBy, s.tsDest(&reservedAt), &userID)
			if errors.Is(err, sql.ErrNoRows) {
				continue // already gone
			}
			if err != nil {
				return err
			}

			switch {
			case len(spentBy) > 0:
				w, herr := decodeHash(spentBy)
				if herr != nil {
					return herr
				}
				if !seenSpenders[*w] {
					seenSpenders[*w] = true
					report.AlreadySpentBy = append(report.AlreadySpentBy, *w)
				}
			case reservedBy.String != "":
				ref, ok := reservedRefs[reservedBy.String]
				if !ok {
					ref = &utxostore.ReservationRef{
						Reservation: reservedBy.String,
						UserID:      userID,
						ReservedAt:  s.tsTime(reservedAt),
					}
					reservedRefs[reservedBy.String] = ref
					refOrder = append(refOrder, reservedBy.String)
				}
				if at := s.tsTime(reservedAt); at.Before(ref.ReservedAt) {
					ref.ReservedAt = at
				}
				ref.Outpoints = append(ref.Outpoints, op)
			default:
				// Unreserved and unspent: remove, even when frozen — a frozen
				// phantom coin is strictly worse than no row.
				if _, err := x.ExecContext(ctx, s.rebind("DELETE FROM utxos WHERE txid=? AND vout=?"), op.TxID[:], op.Vout); err != nil {
					return err
				}
				report.Removed = append(report.Removed, op)
			}
		}

		for _, token := range refOrder {
			report.AlreadyReserved = append(report.AlreadyReserved, *reservedRefs[token])
		}
		return nil
	})
	if err != nil {
		return utxostore.RemoveByMintReport{}, err
	}
	return report, nil
}

// Freeze implements [utxostore.Store]: missing outpoints are per-item
// NotFoundErrors under ErrBatch; present rows are still frozen.
func (s *Store) Freeze(ctx context.Context, ops []utxostore.Outpoint) error {
	return s.setFrozen(ctx, ops, true)
}

// Unfreeze implements [utxostore.Store]; the mirror of Freeze.
func (s *Store) Unfreeze(ctx context.Context, ops []utxostore.Outpoint) error {
	return s.setFrozen(ctx, ops, false)
}

func (s *Store) setFrozen(ctx context.Context, ops []utxostore.Outpoint, frozen bool) error {
	if s.isClosed() {
		return errClosed
	}

	var itemErrs []error
	err := s.withTx(ctx, func(x queryer) error {
		itemErrs = itemErrs[:0]
		for _, op := range ops {
			res, err := x.ExecContext(ctx, s.rebind("UPDATE utxos SET frozen=? WHERE txid=? AND vout=?"),
				s.boolVal(frozen), op.TxID[:], op.Vout)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n == 0 {
				itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return joinBatch(itemErrs)
}

// Balance implements [utxostore.Store].
func (s *Store) Balance(ctx context.Context, userID int64, basket string) (utxostore.Balance, error) {
	b := utxostore.Balance{
		Claimable:      make(map[utxostore.Tier]uint64),
		ClaimableCount: make(map[utxostore.Tier]int),
	}
	if s.isClosed() {
		return b, errClosed
	}

	q := s.rebind(`
		SELECT tier,
			COALESCE(SUM(CASE WHEN reserved_by IS NULL AND NOT frozen THEN satoshis ELSE 0 END), 0) AS claimable,
			COALESCE(SUM(CASE WHEN reserved_by IS NOT NULL THEN satoshis ELSE 0 END), 0) AS reserved,
			COALESCE(SUM(CASE WHEN reserved_by IS NULL AND NOT frozen THEN 1 ELSE 0 END), 0) AS claimable_count,
			COALESCE(SUM(CASE WHEN reserved_by IS NOT NULL THEN 1 ELSE 0 END), 0) AS reserved_count
		FROM utxos
		WHERE user_id=? AND basket=? AND spent_by IS NULL
		GROUP BY tier`)
	// SQLite has no boolean type; "NOT frozen" over an INTEGER 0/1 evaluates
	// correctly (NOT 0 = 1, NOT 1 = 0), so the same text serves both engines.
	rows, err := s.execer(ctx).QueryContext(ctx, q, userID, basket)
	if err != nil {
		return b, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			tier           utxostore.Tier
			claimable      uint64
			reserved       uint64
			claimableCount int
			reservedCount  int
		)
		if err := rows.Scan(&tier, &claimable, &reserved, &claimableCount, &reservedCount); err != nil {
			return b, err
		}
		if claimable > 0 {
			b.Claimable[tier] = claimable
		}
		if claimableCount > 0 {
			b.ClaimableCount[tier] = claimableCount
		}
		b.Reserved += reserved
		b.ReservedCount += reservedCount
	}
	if err := rows.Err(); err != nil {
		return b, err
	}
	return b, nil
}
