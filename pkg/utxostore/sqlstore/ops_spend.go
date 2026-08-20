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
//
// On PostgreSQL the whole batch is ONE statement per (spender, guard mode)
// group — in practice one, since a call records a single transaction's inputs —
// instead of a round trip per op. See [Store.spendSet]. SQLite keeps the
// per-op loop.
func (s *Store) Spend(ctx context.Context, spends []*utxostore.SpendOp, force bool) error {
	if s.isClosed() {
		return errClosed
	}

	err := s.withTx(ctx, func(x queryer) error {
		// Clear every verdict up front rather than as each item is decided: a
		// lock-error retry re-runs this whole function, and a stale Err from
		// the aborted attempt must not survive into the new one.
		for _, sp := range spends {
			sp.Err = nil
		}
		if s.engine == EnginePostgres {
			return s.spendSet(ctx, x, spends, force)
		}
		for _, sp := range spends {
			itemErr, fatal := s.spendOne(ctx, x, sp, force)
			if fatal != nil {
				return fatal
			}
			sp.Err = itemErr
		}
		return nil
	})
	if err != nil {
		return err
	}
	var failed int
	for _, sp := range spends {
		if sp.Err != nil {
			failed++
		}
	}
	return utxostore.BatchCountErr(failed, len(spends))
}

// spendOne records one spend, write first. The guarded UPDATE re-asserts every
// precondition in its own WHERE, so the happy path is a SINGLE statement and no
// row can slip between a check and the write — there is no check to slip away
// from. Only a write that matched nothing pays for a classifying read.
func (s *Store) spendOne(ctx context.Context, x queryer, sp *utxostore.SpendOp, force bool) (itemErr, fatal error) {
	if err := utxostore.ValidateSpend(sp); err != nil {
		return err, nil
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

// The set-based (PostgreSQL) spend statements. Each is the per-op guarded
// UPDATE with its outpoint predicate replaced by a join against [pgKeyRel], so
// a whole batch costs ONE round trip; the guard conjuncts are otherwise
// verbatim what [Store.spendUpdate] carries, per mode:
//
//   - guarded: spent_by IS NULL AND reserved_by=$4 AND NOT frozen
//   - fact:    spent_by IS NULL  (a freeze or a stale reservation cannot
//     un-happen a spend the network already accepted)
//
// RETURNING names the keys rather than a count, because a batch needs to know
// WHICH ops the write matched, not how many.
var (
	spendSetGuardedPG = `UPDATE utxos u SET spent_by=$3, pinned=FALSE FROM ` + pgKeyRel +
		` WHERE ` + pgKeyMatch + ` AND u.spent_by IS NULL AND u.reserved_by=$4 AND ` + notFrozenOn("u") +
		` RETURNING u.txid, u.vout`

	spendSetFactPG = `UPDATE utxos u SET spent_by=$3, pinned=FALSE FROM ` + pgKeyRel +
		` WHERE ` + pgKeyMatch + ` AND u.spent_by IS NULL` +
		` RETURNING u.txid, u.vout`

	// spendClassifySetPG reads the state of every miss in ONE query. It serves
	// both modes: fact mode simply ignores the two columns whose refusals it
	// does not honor, which is cheaper than a second statement text.
	spendClassifySetPG = `SELECT u.txid, u.vout, u.spent_by, u.frozen, u.reserved_by
		FROM utxos u JOIN ` + pgKeyRel + ` ON ` + pgKeyMatch
)

// spendGroup is a run of spends that ONE set-based statement can carry: they
// write the same spent_by and, under a guard, assert the same reservation.
type spendGroup struct {
	spendingTxID chainhash.Hash
	reservation  string // "" in fact mode: the guard does not name it
	ops          []*utxostore.SpendOp
}

// groupSpends partitions spends into the fewest statements that preserve each
// item's semantics. The write's SET clause carries the spender and (under a
// guard) its WHERE carries the reservation, so those two are what a group must
// hold constant; fact mode drops the reservation from both, so it groups on the
// spender alone. In practice every caller records one transaction's inputs and
// gets exactly one group.
//
// Groups keep first-appearance order, so when the same outpoint appears under
// two spenders the earlier one wins the row and the later one classifies
// against it — the ruling the sequential loop made.
func groupSpends(spends []*utxostore.SpendOp, force bool) []*spendGroup {
	var (
		order []*spendGroup
		byKey = make(map[string]*spendGroup, 1)
	)
	for _, sp := range spends {
		key := string(sp.SpendingTxID[:])
		res := ""
		if !force {
			res = sp.Reservation
			key += "\x00" + res
		}
		g, ok := byKey[key]
		if !ok {
			g = &spendGroup{spendingTxID: sp.SpendingTxID, reservation: res}
			byKey[key] = g
			order = append(order, g)
		}
		g.ops = append(g.ops, sp)
	}
	return order
}

// outpoints projects a group's ops onto the key array [pairArgs] binds.
func (g *spendGroup) outpoints() []utxostore.Outpoint {
	ops := make([]utxostore.Outpoint, len(g.ops))
	for i, sp := range g.ops {
		ops[i] = sp.Outpoint
	}
	return ops
}

// spendSet is the PostgreSQL arm of [Store.Spend]: the per-op write→classify→
// retry loop, lifted to the whole batch. The happy path is one statement per
// group (one, for a caller recording one transaction's inputs) where the loop
// paid a round trip per input on the broadcast-accept path of every accepted
// transaction.
//
// The 2-attempt budget [guardAttempts] names is preserved at the BATCH level
// and means the same thing it did per op: a write that matched nothing followed
// by a read that finds the row eligible again is a concurrent release or
// unspend, and one more attempt settles it. Only the still-unclassified misses
// are re-driven, so the second round is as small as the contention actually is.
// Items that never settle are reported as [utxostore.ErrContention], exactly as
// the per-op loop reported them.
func (s *Store) spendSet(ctx context.Context, x queryer, spends []*utxostore.SpendOp, force bool) error {
	// Validation is not a row state, so it is settled before any statement and
	// its items never enter a group.
	pending := make([]*utxostore.SpendOp, 0, len(spends))
	for _, sp := range spends {
		if err := utxostore.ValidateSpend(sp); err != nil {
			sp.Err = err
			continue
		}
		pending = append(pending, sp)
	}

	attempt := pending
	for range guardAttempts {
		if len(attempt) == 0 {
			return nil
		}
		misses, err := s.spendWriteSet(ctx, x, attempt, force)
		if err != nil {
			return err
		}
		if len(misses) == 0 {
			return nil
		}
		unresolved, err := s.classifySpendMisses(ctx, x, misses, force)
		if err != nil {
			return err
		}
		if len(unresolved) == 0 {
			return nil
		}
		attempt = unresolved
	}
	// These rows kept looking spendable between the guarded write and the read
	// that followed it; see the per-op twin, [Store.spendOne], for why that is
	// contention rather than a refusal.
	for _, sp := range attempt {
		sp.Err = fmt.Errorf("sqlstore: spend %s: %w", sp.Outpoint, utxostore.ErrContention)
	}
	return nil
}

// spendWriteSet runs one set-based guarded UPDATE per group and returns the ops
// the write did NOT match — the only ones that pay for a classifying read.
func (s *Store) spendWriteSet(ctx context.Context, x queryer, spends []*utxostore.SpendOp, force bool) ([]*utxostore.SpendOp, error) {
	q := spendSetGuardedPG
	if force {
		q = spendSetFactPG
	}

	var misses []*utxostore.SpendOp
	for _, g := range groupSpends(spends, force) {
		txids, vouts := pairArgs(g.outpoints())
		args := []any{txids, vouts, g.spendingTxID[:]}
		if !force {
			args = append(args, g.reservation)
		}
		rows, err := x.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		recorded, err := scanOutpointSet(rows)
		if err != nil {
			return nil, err
		}
		for _, sp := range g.ops {
			if _, ok := recorded[sp.Outpoint]; !ok {
				misses = append(misses, sp)
			}
		}
	}
	return misses, nil
}

// spendState is one miss's row state, as [spendClassifySetPG] projects it.
type spendState struct {
	spentBy    []byte
	frozen     bool
	reservedBy sql.NullString
}

// classifySpendMisses turns every miss into its per-item verdict with ONE read,
// and returns the ops that look spendable again (the caller re-drives those).
// The taxonomy and its precedence are [Store.classifySpendFailure]'s, item for
// item — a recorded spend wins over a freeze, the spent_by arbiter rules in
// BOTH modes, and the freeze/reservation refusals exist only under a guard.
// The conformance suite pins the rulings; the two classifiers must stay in step
// with each other and with the aerostore twin.
func (s *Store) classifySpendMisses(ctx context.Context, x queryer, misses []*utxostore.SpendOp, force bool) ([]*utxostore.SpendOp, error) {
	ops := make([]utxostore.Outpoint, len(misses))
	for i, sp := range misses {
		ops[i] = sp.Outpoint
	}
	txids, vouts := pairArgs(ops)
	rows, err := x.QueryContext(ctx, spendClassifySetPG, txids, vouts)
	if err != nil {
		return nil, err
	}
	state, err := s.scanSpendStates(rows)
	if err != nil {
		return nil, err
	}

	var unresolved []*utxostore.SpendOp
	for _, sp := range misses {
		st, present := state[sp.Outpoint]
		if !present {
			sp.Err = &utxostore.NotFoundError{Op: sp.Outpoint}
			continue
		}
		switch {
		case len(st.spentBy) > 0:
			w, herr := decodeHash(st.spentBy)
			if herr != nil {
				return nil, herr
			}
			if *w == sp.SpendingTxID {
				continue // idempotent same-spender replay
			}
			sp.Err = &utxostore.SpentError{Op: sp.Outpoint, Winner: *w}
		case !force && st.frozen:
			sp.Err = &utxostore.FrozenError{Op: sp.Outpoint}
		case !force && st.reservedBy.String != sp.Reservation:
			sp.Err = &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: st.reservedBy.String}
		default:
			unresolved = append(unresolved, sp)
		}
	}
	return unresolved, nil
}

// scanSpendStates reads the classifying join into a per-outpoint state map.
// Outpoints with no entry are absent rows.
func (s *Store) scanSpendStates(rows *sql.Rows) (map[utxostore.Outpoint]spendState, error) {
	defer func() { _ = rows.Close() }()
	out := make(map[utxostore.Outpoint]spendState)
	for rows.Next() {
		var (
			txid   []byte
			vout   uint32
			st     spendState
			frozen boolScan
		)
		if err := rows.Scan(&txid, &vout, &st.spentBy, s.boolDest(&frozen), &st.reservedBy); err != nil {
			return nil, err
		}
		st.frozen = s.boolGet(frozen)
		var op utxostore.Outpoint
		copy(op.TxID[:], txid)
		op.Vout = vout
		out[op] = st
	}
	return out, rows.Err()
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
		if len(ops) == 0 {
			return nil
		}
		// PostgreSQL: one set-based UPDATE for the whole list. RowsAffected is
		// the release count directly — a duplicated outpoint updates its row
		// once, the same total the loop reached by having the second write
		// match nothing.
		if s.engine == EnginePostgres {
			txids, vouts := pairArgs(ops)
			res, err := x.ExecContext(ctx,
				`UPDATE utxos u SET spent_by=NULL, reserved_by=NULL, reserved_at=NULL, pinned=FALSE FROM `+pgKeyRel+
					` WHERE `+pgKeyMatch+` AND u.spent_by=$3`,
				txids, vouts, spendingTxID[:])
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			released = int(n)
			return nil
		}
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
		if len(ops) == 0 {
			return nil
		}
		// PostgreSQL: one set-based UPDATE. The tier<>$3 guard keeps the count
		// "rows whose tier actually changed" the interface promises, and a
		// duplicated outpoint is still counted once.
		if s.engine == EnginePostgres {
			txids, vouts := pairArgs(ops)
			res, err := x.ExecContext(ctx,
				`UPDATE utxos u SET tier=$3 FROM `+pgKeyRel+
					` WHERE `+pgKeyMatch+` AND u.tier<>$3`,
				txids, vouts, int64(to))
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			changed = int(n)
			return nil
		}
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
	if err := utxostore.ValidateMintOutpoints(mintTxID, ops); err != nil {
		return report, err
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
		if len(ops) == 0 {
			return nil
		}
		// PostgreSQL: one set-based UPDATE, whose RETURNING names the rows that
		// existed. The misses are the per-item NotFoundErrors, reported in the
		// caller's op order (and once per occurrence, as the loop did).
		if s.engine == EnginePostgres {
			txids, vouts := pairArgs(ops)
			rows, err := x.QueryContext(ctx,
				`UPDATE utxos u SET frozen=$3 FROM `+pgKeyRel+
					` WHERE `+pgKeyMatch+` RETURNING u.txid, u.vout`,
				txids, vouts, s.boolVal(frozen))
			if err != nil {
				return err
			}
			found, err := scanOutpointSet(rows)
			if err != nil {
				return err
			}
			for _, op := range ops {
				if _, ok := found[op]; !ok {
					itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
				}
			}
			return nil
		}
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
	return utxostore.JoinBatch(itemErrs)
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
