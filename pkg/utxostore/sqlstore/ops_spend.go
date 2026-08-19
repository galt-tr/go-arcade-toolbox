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
// With force it records a spend the network has already accepted, skipping the
// reservation and freeze guards (see [utxostore.Store.Spend]).
//
// Refusals are per item under [utxostore.ErrBatch] — or, in EITHER mode,
// [utxostore.ErrContention] when a row keeps flipping between spendable and
// held, which is transient: re-drive the spend. Fact-mode callers recording an
// accepted broadcast must tolerate it rather than read it as a refused coin.
func (s *Store) Spend(ctx context.Context, spends []*utxostore.SpendOp, force bool) error {
	if s.isClosed() {
		return errClosed
	}

	var failed int
	err := s.withTx(ctx, func(x queryer) error {
		failed = 0
		for _, sp := range spends {
			itemErr, fatal := s.spendOne(ctx, x, sp, force)
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

// spendOne records one spend, write first. The guarded UPDATE re-asserts every
// precondition in its own WHERE, so the happy path is a SINGLE statement and no
// row can slip between a check and the write — there is no check to slip away
// from. Only a write that matched nothing pays for a classifying read.
func (s *Store) spendOne(ctx context.Context, x queryer, sp *utxostore.SpendOp, force bool) (itemErr, fatal error) {
	if sp.Reservation == "" {
		return fmt.Errorf("sqlstore: spend %s: reservation must be non-empty", sp.Outpoint), nil
	}

	for range guardAttempts {
		n, err := s.spendUpdate(ctx, x, sp, force)
		if err != nil {
			return nil, err
		}
		if n == 1 {
			return nil, nil
		}
		classified, retry, cerr := s.classifySpendFailure(ctx, x, sp, force)
		if cerr != nil {
			return nil, cerr
		}
		if !retry {
			return classified, nil
		}
	}
	// The row kept looking spendable between the guarded UPDATE and the read
	// that followed it, which is contention, not a refusal: some peer is
	// releasing and re-reserving (or unspending) this coin right now. Say so,
	// rather than inventing a verdict — no refusal would be true of the row, and
	// treating the coin as refused abandons it. This producer has no outer retry
	// owner (see [utxostore.ErrContention]): the caller re-drives the spend, and
	// a fact-mode caller recording an accepted broadcast must tolerate it.
	return fmt.Errorf("sqlstore: spend %s: %w", sp.Outpoint, utxostore.ErrContention), nil
}

// spendUpdate is the guard-carrying write, and the only statement a successful
// spend runs. Guarded mode re-asserts the reservation token, the absence of a
// recorded spend and the absence of a freeze; fact mode (force) keeps only
// "spent_by IS NULL", because the spend is already on the network — neither a
// freeze nor a stale reservation can un-happen it, but two spend facts still
// cannot both hold. On PostgreSQL the UPDATE itself serializes on the row lock
// it takes, so dropping the old SELECT ... FOR UPDATE gives up no exclusion.
//
// The spend consumes the coin, so the pre-broadcast pin it may have carried has
// done its job and goes with it (see [utxostore.Store.Pin]).
func (s *Store) spendUpdate(ctx context.Context, x queryer, sp *utxostore.SpendOp, force bool) (int64, error) {
	q := "UPDATE utxos SET spent_by=?, pinned=" + s.boolLit(false) +
		" WHERE txid=? AND vout=? AND spent_by IS NULL"
	args := []any{sp.SpendingTxID[:], sp.TxID[:], sp.Vout}
	if !force {
		q += " AND reserved_by=? AND " + notFrozen
		args = append(args, sp.Reservation)
	}
	res, err := x.ExecContext(ctx, s.rebind(q), args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// classifySpendFailure turns a guarded UPDATE that matched nothing into the
// per-item verdict. retry is true when the row looks spendable again, meaning a
// concurrent release-and-re-reserve (or, in fact mode, an Unspend) raced the
// write; the caller re-drives it. The read is deliberately plain: the UPDATE
// matched no row, so it holds no lock worth extending, and any answer this read
// gives is confirmed by the next attempt's guarded write anyway.
//
// Precedence matches the interface: a recorded spend wins over a freeze, and
// the spent_by arbiter runs in BOTH modes — two spend facts cannot both hold.
// The freeze and reservation refusals exist only under a guard. The aerostore
// twin of this classifier makes the identical rulings; the two must stay in
// step, and the conformance suite pins them.
func (s *Store) classifySpendFailure(ctx context.Context, x queryer, sp *utxostore.SpendOp, force bool) (itemErr error, retry bool, fatal error) {
	var (
		spentBy    []byte
		frozen     boolScan
		reservedBy sql.NullString
	)
	q := s.rebind("SELECT spent_by, frozen, reserved_by FROM utxos WHERE txid=? AND vout=?")
	err := x.QueryRowContext(ctx, q, sp.TxID[:], sp.Vout).Scan(&spentBy, s.boolDest(&frozen), &reservedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return &utxostore.NotFoundError{Op: sp.Outpoint}, false, nil
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
		if *w == sp.SpendingTxID {
			return nil, false, nil // idempotent same-spender replay
		}
		return &utxostore.SpentError{Op: sp.Outpoint, Winner: *w}, false, nil
	case !force && s.boolGet(frozen):
		return &utxostore.FrozenError{Op: sp.Outpoint}, false, nil
	case !force && reservedBy.String != sp.Reservation:
		return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: reservedBy.String}, false, nil
	default:
		return nil, true, nil
	}
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
				`UPDATE utxos SET spent_by=NULL, reserved_by=NULL, reserved_at=NULL, pinned=`+s.boolLit(false)+`
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
// removes nothing. It fails with [utxostore.ErrContention] when a coin keeps
// flipping between removable and held, which is transient: retry the call.
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
			row, verdict, err := s.resolveMintRow(ctx, x, op)
			if err != nil {
				return err
			}
			switch verdict {
			case mintGone:
				// Nothing to report: the row was already removed, here or by a
				// peer running the same invalidation.
			case mintRemoved:
				report.Removed = append(report.Removed, op)
			case mintSpent:
				w, herr := decodeHash(row.spentBy)
				if herr != nil {
					return herr
				}
				if !seenSpenders[*w] {
					seenSpenders[*w] = true
					report.AlreadySpentBy = append(report.AlreadySpentBy, *w)
				}
			case mintReserved:
				ref, ok := reservedRefs[row.reservedBy.String]
				if !ok {
					ref = &utxostore.ReservationRef{
						Reservation: row.reservedBy.String,
						UserID:      row.userID,
						ReservedAt:  s.tsTime(row.reservedAt),
					}
					reservedRefs[row.reservedBy.String] = ref
					refOrder = append(refOrder, row.reservedBy.String)
				}
				if at := s.tsTime(row.reservedAt); at.Before(ref.ReservedAt) {
					ref.ReservedAt = at
				}
				ref.Outpoints = append(ref.Outpoints, op)
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

// mintVerdict is what one [Store.RemoveByMintTx] outpoint resolved to.
type mintVerdict int

const (
	mintGone     mintVerdict = iota // no row: already removed, nothing to report
	mintRemoved                     // a phantom coin this call deleted
	mintSpent                       // a descendant already spent it
	mintReserved                    // a descendant already holds it
)

// mintRow is the classification projection [Store.RemoveByMintTx] reads.
type mintRow struct {
	spentBy    []byte
	reservedBy sql.NullString
	reservedAt tsScan
	userID     int64
}

// resolveMintRow classifies one outpoint of an invalidated mint and, when it is
// an unreserved unspent phantom, deletes it. Unlike the other guarded mutations
// this one genuinely needs the read — the report carries the holder's identity
// and reservation timestamp — but the DELETE still re-asserts the two
// predicates that decision rested on, so a descendant that reserves or spends
// the row between the read and the write WINS the race instead of losing its
// coin, and the re-read then classifies it as a survivor. Only the removal
// verdict is write-confirmed; mintSpent and mintReserved are snapshot reads,
// true as of the read and no later. The freeze is deliberately not a guard: a
// frozen phantom coin is strictly worse than no row.
//
// Exhausting [guardAttempts] means the row kept flipping between phantom and
// held, so it belongs in NO report slot — it was neither removed nor is it
// reliably a survivor. Reporting it as gone would silently drop a coin that
// still exists, so this escalates to [utxostore.ErrContention] and fails the
// whole call, exactly as the aerostore twin does.
func (s *Store) resolveMintRow(ctx context.Context, x queryer, op utxostore.Outpoint) (mintRow, mintVerdict, error) {
	for range guardAttempts {
		var row mintRow
		q := s.rebind("SELECT spent_by, reserved_by, reserved_at, user_id FROM utxos WHERE txid=? AND vout=?")
		err := x.QueryRowContext(ctx, q, op.TxID[:], op.Vout).
			Scan(&row.spentBy, &row.reservedBy, s.tsDest(&row.reservedAt), &row.userID)
		if errors.Is(err, sql.ErrNoRows) {
			return mintRow{}, mintGone, nil
		}
		if err != nil {
			return mintRow{}, mintGone, err
		}
		// Precedence matches every other classifier here: spent wins over
		// reserved (the aerostore twin rules the same way).
		switch {
		case len(row.spentBy) > 0:
			return row, mintSpent, nil
		case row.reservedBy.String != "":
			return row, mintReserved, nil
		}

		res, err := x.ExecContext(ctx, s.rebind(
			"DELETE FROM utxos WHERE txid=? AND vout=? AND reserved_by IS NULL AND spent_by IS NULL"),
			op.TxID[:], op.Vout)
		if err != nil {
			return mintRow{}, mintGone, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return mintRow{}, mintGone, err
		}
		if n == 1 {
			return row, mintRemoved, nil
		}
	}
	return mintRow{}, mintGone, fmt.Errorf("sqlstore: remove-by-mint-tx %s: %w", op, utxostore.ErrContention)
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
