package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// The three PostgreSQL claim statements are the 1000-TPS hot path. They are
// package constants (not inlined) so claim_explain_test.go can EXPLAIN the
// exact production statements and fail if any plan stops using the
// idx_utxos_claim partial index (e.g. a dropped index or a predicate that no
// longer matches the index's WHERE clause). Each candidate CTE selects the best
// fit FOR UPDATE SKIP LOCKED and the outer UPDATE reserves it in the same round
// trip. The claim's WHERE and ORDER BY mirror the index's columns so it is a
// pure ordered index walk with no sort.
const (
	claimSmallestSufficientPG = `WITH candidate AS (
	SELECT txid, vout FROM utxos
	WHERE user_id=$1 AND basket=$2 AND tier=$3
	  AND reserved_by IS NULL AND spent_by IS NULL AND NOT frozen AND satoshis >= $4
	ORDER BY satoshis, seq LIMIT 1 FOR UPDATE SKIP LOCKED)
UPDATE utxos u SET reserved_by=$5, reserved_at=$6 FROM candidate c
WHERE u.txid=c.txid AND u.vout=c.vout
RETURNING ` + utxoColsU

	claimLargestInsufficientPG = `WITH candidate AS (
	SELECT txid, vout FROM utxos
	WHERE user_id=$1 AND basket=$2 AND tier=$3
	  AND reserved_by IS NULL AND spent_by IS NULL AND NOT frozen AND satoshis < $4
	ORDER BY satoshis DESC, seq DESC LIMIT $5 FOR UPDATE SKIP LOCKED)
UPDATE utxos u SET reserved_by=$6, reserved_at=$7 FROM candidate c
WHERE u.txid=c.txid AND u.vout=c.vout
RETURNING ` + utxoColsU

	claimExactPG = `WITH candidate AS (
	SELECT txid, vout FROM utxos
	WHERE user_id=$1 AND basket=$2 AND tier=$3
	  AND reserved_by IS NULL AND spent_by IS NULL AND NOT frozen AND satoshis = $4
	ORDER BY satoshis, seq LIMIT $5 FOR UPDATE SKIP LOCKED)
UPDATE utxos u SET reserved_by=$6, reserved_at=$7 FROM candidate c
WHERE u.txid=c.txid AND u.vout=c.vout
RETURNING ` + utxoColsU
)

// claimedRow pairs a claimed coin with its seq, so the returned slice can be
// ordered to match the memstore reference (largest-first / insertion-order)
// even though RETURNING itself is unordered.
type claimedRow struct {
	u   *utxostore.UTXO
	seq int64
}

// runClaim executes a claim statement that reserves rows in one round trip and
// scans the RETURNING set. An empty result means no claimable coin matched —
// or, under SKIP LOCKED, every match was momentarily locked by a concurrent
// claimer that is reserving it in the same atomic statement; either way the
// coin is not orphaned, so an empty set is reported as "none" (nil), never
// ErrContention. See the package doc, "Design notes".
//
// Retry mirrors [Store.withTx], and for the same reason. Claims used to be the
// one operation in this package that ran with no lock-error retry at all: every
// mutation goes through withTx -> [sqlkit.WithRetry], while a claim went
// straight to the pool. A 40001/40P01/55P03 on the hot path therefore surfaced
// raw to the funder, which retries only on [utxostore.ErrContention] — an error
// this backend never returns (see the package doc). Mode A masked it, because
// the metastore's own retry wraps the whole unit of work; a standalone store had
// nothing.
//
// A claim is a single atomic statement, so a failed attempt committed nothing
// and re-running it cannot double-allocate: either the UPDATE ... RETURNING
// committed and the coin is reserved, or it did not and the coin is untouched.
// That is what makes the retry safe here without a transaction.
//
// Under an ambient transaction the retry is deliberately skipped: the enclosing
// tx is already poisoned by the lock error and only the owner can restart it.
func (s *Store) runClaim(ctx context.Context, query string, args ...any) ([]claimedRow, error) {
	if _, ambient := sqltx.From(ctx, s.db); ambient {
		return s.claimOnce(ctx, query, args...)
	}
	var out []claimedRow
	err := sqlkit.WithRetry(ctx, func() error {
		var err error
		out, err = s.claimOnce(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// claimOnce is one attempt of a claim statement: execute and scan the RETURNING
// set. It holds no retry of its own so [Store.runClaim] owns that policy.
func (s *Store) claimOnce(ctx context.Context, query string, args ...any) ([]claimedRow, error) {
	rows, err := s.execer(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []claimedRow
	for rows.Next() {
		u, seq, err := s.scanUTXO(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, claimedRow{u: u, seq: seq})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ClaimSmallestSufficient implements [utxostore.Store]: the true minimum
// Satoshis >= minSats among claimable rows in scope, ties broken by insertion
// order (seq).
func (s *Store) ClaimSmallestSufficient(ctx context.Context, sc utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if s.isClosed() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}

	ts := s.now()
	var (
		query string
		args  []any
	)
	if s.engine == EnginePostgres {
		query = claimSmallestSufficientPG
		args = []any{sc.UserID, sc.Basket, int64(sc.Tier), minSats, reservation, s.encTime(ts)}
	} else {
		query = `UPDATE utxos SET reserved_by=?, reserved_at=?
		WHERE (txid, vout) IN (
			SELECT txid, vout FROM utxos
			WHERE user_id=? AND basket=? AND tier=?
			  AND reserved_by IS NULL AND spent_by IS NULL AND frozen = 0 AND satoshis >= ?
			ORDER BY satoshis, seq LIMIT 1)
		RETURNING ` + utxoCols
		args = []any{reservation, s.encTime(ts), sc.UserID, sc.Basket, int64(sc.Tier), minSats}
	}

	claimed, err := s.runClaim(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(claimed) == 0 {
		return nil, nil
	}
	return claimed[0].u, nil
}

// ClaimLargestInsufficient implements [utxostore.Store]: up to limit claimable
// coins with Satoshis < capSats, largest first (ties by insertion order).
func (s *Store) ClaimLargestInsufficient(ctx context.Context, sc utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if s.isClosed() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	ts := s.now()
	var (
		query string
		args  []any
	)
	if s.engine == EnginePostgres {
		query = claimLargestInsufficientPG
		args = []any{sc.UserID, sc.Basket, int64(sc.Tier), capSats, limit, reservation, s.encTime(ts)}
	} else {
		query = `UPDATE utxos SET reserved_by=?, reserved_at=?
		WHERE (txid, vout) IN (
			SELECT txid, vout FROM utxos
			WHERE user_id=? AND basket=? AND tier=?
			  AND reserved_by IS NULL AND spent_by IS NULL AND frozen = 0 AND satoshis < ?
			ORDER BY satoshis DESC, seq DESC LIMIT ?)
		RETURNING ` + utxoCols
		args = []any{reservation, s.encTime(ts), sc.UserID, sc.Basket, int64(sc.Tier), capSats, limit}
	}

	claimed, err := s.runClaim(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	// RETURNING is unordered; restore largest-first (ties by insertion order).
	sort.Slice(claimed, func(i, j int) bool {
		if claimed[i].u.Satoshis != claimed[j].u.Satoshis {
			return claimed[i].u.Satoshis > claimed[j].u.Satoshis
		}
		return claimed[i].seq > claimed[j].seq
	})
	return collect(claimed), nil
}

// ClaimExact implements [utxostore.Store]: up to count claimable coins with
// Satoshis == denomination, in insertion order.
func (s *Store) ClaimExact(ctx context.Context, sc utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if s.isClosed() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}

	ts := s.now()
	var (
		query string
		args  []any
	)
	if s.engine == EnginePostgres {
		query = claimExactPG
		args = []any{sc.UserID, sc.Basket, int64(sc.Tier), denomination, count, reservation, s.encTime(ts)}
	} else {
		query = `UPDATE utxos SET reserved_by=?, reserved_at=?
		WHERE (txid, vout) IN (
			SELECT txid, vout FROM utxos
			WHERE user_id=? AND basket=? AND tier=?
			  AND reserved_by IS NULL AND spent_by IS NULL AND frozen = 0 AND satoshis = ?
			ORDER BY satoshis, seq LIMIT ?)
		RETURNING ` + utxoCols
		args = []any{reservation, s.encTime(ts), sc.UserID, sc.Basket, int64(sc.Tier), denomination, count}
	}

	claimed, err := s.runClaim(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	// All rows share denomination; restore insertion order.
	sort.Slice(claimed, func(i, j int) bool { return claimed[i].seq < claimed[j].seq })
	return collect(claimed), nil
}

func collect(rows []claimedRow) []*utxostore.UTXO {
	out := make([]*utxostore.UTXO, len(rows))
	for i, r := range rows {
		out[i] = r.u
	}
	return out
}

// ReserveOutpoints implements [utxostore.Store]: an ALL-OR-NOTHING hold on the
// exact rows the caller named, so provided inputs get the same exclusivity a
// funded coin gets.
//
// # Why two phases and not one set-based UPDATE
//
// The obvious shape is a single CTE that locks the matching rows FOR UPDATE and
// updates them in the same statement, then compares the RETURNING count against
// len(ops) and fails the transaction on a shortfall — atomic for free, because
// the rollback un-reserves whatever the UPDATE touched. That is correct for a
// store that owns its transaction, and WRONG in Mode A: under an AMBIENT
// transaction (internal/sqltx) [Store.withTx] neither commits nor rolls back,
// so there is nothing to undo the mutation with. The statement would have
// already reserved the good rows before it discovered the bad one, and the
// caller's enclosing unit of work may well go on to commit.
//
// So the guard runs BEFORE any write, in both modes: phase one locks every
// target row (FOR UPDATE on PostgreSQL; SQLite's single writer connection
// already serializes) and classifies it; phase two stamps only if the whole set
// passed. Holding the locks across the two phases is what keeps it race-free —
// a concurrent claimer either got there first (and phase one refuses it) or
// blocks until this transaction ends.
//
// The cost is real and worth naming: N locking reads plus N stamps where the
// compact shape needed one statement. That is affordable because this runs once
// per CreateAction over a handful of named inputs, not per claim on the
// 1000-TPS path — and it is the same per-op posture [Store.Spend] already takes.
// Phase two can collapse to a single IN-list UPDATE later without changing the
// guarantee, since the guarantee comes from phase one's locks, not from the
// write's shape.
//
// Rows already held by exactly this reservation are satisfied WITHOUT being
// re-stamped: a replay must not slide ReservedAt forward and hide the
// reservation from the stale reaper.
func (s *Store) ReserveOutpoints(ctx context.Context, userID int64, reservation string, ops []utxostore.Outpoint) error {
	if s.isClosed() {
		return errClosed
	}
	if err := validateReserveOutpoints(reservation, ops); err != nil {
		return err
	}

	// Lock in a deterministic order so two callers whose input sets overlap
	// queue behind each other instead of deadlocking on a lock cycle.
	targets := sortedDistinct(ops)

	var itemErrs []error
	err := s.withTx(ctx, func(x queryer) error {
		itemErrs = itemErrs[:0]
		var toStamp []utxostore.Outpoint

		for _, op := range targets {
			needsStamp, itemErr, fatal := s.classifyForReserve(ctx, x, op, userID, reservation)
			if fatal != nil {
				return fatal
			}
			if itemErr != nil {
				itemErrs = append(itemErrs, itemErr)
				continue
			}
			if needsStamp {
				toStamp = append(toStamp, op)
			}
		}
		if len(itemErrs) > 0 {
			return nil // phase two never runs: nothing was mutated
		}

		// ONE timestamp for the whole set, read once outside the loop: the
		// named inputs were reserved by a single decision, so they must age as
		// a unit under the stale reaper rather than by however long the loop
		// took.
		ts := s.encTime(s.now())
		for _, op := range toStamp {
			// Phase one already holds this row's lock and proved it unreserved,
			// unspent and ours, so these predicates cannot change the outcome —
			// they are a belt-and-braces assertion that the lock did its job,
			// checked below via RowsAffected. reserved_by IS NULL is safe
			// because rows already held by this token never reach toStamp.
			res, err := x.ExecContext(ctx, s.rebind(
				`UPDATE utxos SET reserved_by=?, reserved_at=?
				 WHERE txid=? AND vout=? AND user_id=?
				   AND reserved_by IS NULL AND spent_by IS NULL`),
				reservation, ts, op.TxID[:], op.Vout, userID)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n != 1 {
				// Unreachable while the row lock holds. If it ever fires, the
				// isolation assumption this method rests on is broken, and
				// failing the transaction is the only safe answer.
				return fmt.Errorf("sqlstore: reserve outpoints: %s changed under its own row lock (%d rows stamped)", op, n)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return joinBatch(itemErrs)
}

// classifyForReserve locks one target row and decides its fate. needsStamp is
// true only for a row that is claimable and not already held by this token;
// a row already held by it is satisfied and reports needsStamp false with no
// error, so a replay re-stamps nothing.
//
// Refusal precedence matches [Store.Remove]: spent > reserved-by-another >
// frozen. A row owned by a DIFFERENT user reports NotFound rather than
// Reserved — the caller named the outpoint, so the reply must not confirm that
// somebody else's coin exists. The aerostore twin of this classifier makes the
// identical rulings; the two must stay in step.
func (s *Store) classifyForReserve(ctx context.Context, x queryer, op utxostore.Outpoint, userID int64, reservation string) (needsStamp bool, itemErr, fatal error) {
	var (
		rowUser    int64
		spentBy    []byte
		reservedBy sql.NullString
		frozen     boolScan
	)
	q := s.rebind("SELECT user_id, spent_by, reserved_by, frozen FROM utxos WHERE txid=? AND vout=?" + s.forUpdate())
	err := x.QueryRowContext(ctx, q, op.TxID[:], op.Vout).
		Scan(&rowUser, &spentBy, &reservedBy, s.boolDest(&frozen))
	if errors.Is(err, sql.ErrNoRows) {
		return false, &utxostore.NotFoundError{Op: op}, nil
	}
	if err != nil {
		return false, nil, err
	}
	if rowUser != userID {
		return false, &utxostore.NotFoundError{Op: op}, nil
	}

	switch {
	case len(spentBy) > 0:
		w, herr := decodeHash(spentBy)
		if herr != nil {
			return false, nil, herr
		}
		return false, &utxostore.SpentError{Op: op, Winner: *w}, nil
	case reservedBy.String != "" && reservedBy.String != reservation:
		return false, &utxostore.ReservedError{Op: op, HeldBy: reservedBy.String}, nil
	case s.boolGet(frozen):
		return false, &utxostore.FrozenError{Op: op}, nil
	case reservedBy.String == reservation:
		return false, nil, nil // already ours: satisfied, not re-stamped
	}
	return true, nil, nil
}

// sortedDistinct returns ops deduplicated and ordered by (txid, vout), the
// canonical lock order [Store.ReserveOutpoints] acquires row locks in.
func sortedDistinct(ops []utxostore.Outpoint) []utxostore.Outpoint {
	seen := make(map[utxostore.Outpoint]struct{}, len(ops))
	out := make([]utxostore.Outpoint, 0, len(ops))
	for _, op := range ops {
		if _, dup := seen[op]; dup {
			continue
		}
		seen[op] = struct{}{}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(out[i].TxID[:], out[j].TxID[:]); c != 0 {
			return c < 0
		}
		return out[i].Vout < out[j].Vout
	})
	return out
}

// ReleaseReservation implements [utxostore.Store]: frees every unspent,
// UNPINNED row held by (userID, reservation). Idempotent; never touches spent
// rows, and never frees the inputs of an in-flight send (see [Store.Pin]).
func (s *Store) ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error) {
	if s.isClosed() {
		return 0, errClosed
	}
	if reservation == "" {
		return 0, errors.New("sqlstore: reservation must be non-empty")
	}

	var released int
	err := s.withTx(ctx, func(x queryer) error {
		res, err := x.ExecContext(ctx, s.rebind(
			`UPDATE utxos SET reserved_by=NULL, reserved_at=NULL
			 WHERE user_id=? AND reserved_by=? AND spent_by IS NULL AND `+notPinned),
			userID, reservation)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		released = int(n)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return released, nil
}

// ReleaseOutpoints implements [utxostore.Store]: frees the listed rows iff each
// is currently reserved by exactly this reservation and unspent. Guard
// mismatches, unreserved/spent rows, and missing outpoints are per-item skips.
// A matching token OVERRIDES the pin (and clears it): the callers are the
// funder, whose rows are never pinned, and the reconciler's verified-dead
// release, which must be able to reclaim a transaction proven never to live.
func (s *Store) ReleaseOutpoints(ctx context.Context, reservation string, ops []utxostore.Outpoint) error {
	if s.isClosed() {
		return errClosed
	}
	if reservation == "" {
		return errors.New("sqlstore: reservation must be non-empty")
	}

	return s.withTx(ctx, func(x queryer) error {
		for _, op := range ops {
			if _, err := x.ExecContext(ctx, s.rebind(
				`UPDATE utxos SET reserved_by=NULL, reserved_at=NULL, pinned=`+s.boolLit(false)+`
				 WHERE txid=? AND vout=? AND reserved_by=? AND spent_by IS NULL`),
				op.TxID[:], op.Vout, reservation); err != nil {
				return err
			}
		}
		return nil
	})
}

// Pin implements [utxostore.Store]: marks every unspent row held by (userID,
// reservation) as pinned and returns how many it NEWLY pinned. The
// already-pinned guard in the WHERE clause is what makes the count idempotent —
// a replay updates no rows and reports 0.
func (s *Store) Pin(ctx context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(ctx, userID, reservation, true)
}

// Unpin implements [utxostore.Store]: clears the pin on every unspent row held
// by (userID, reservation), leaving them RESERVED. Idempotent.
func (s *Store) Unpin(ctx context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(ctx, userID, reservation, false)
}

// setPinned drives Pin/Unpin: one guarded UPDATE over the reservation's live
// (unspent) membership, whose RowsAffected is the count of rows that actually
// changed. Requiring reserved_by=? is what upholds the invariant — a pin can
// only ever be written onto a reserved, unspent row.
func (s *Store) setPinned(ctx context.Context, userID int64, reservation string, pinned bool) (int, error) {
	if s.isClosed() {
		return 0, errClosed
	}
	if reservation == "" {
		return 0, errors.New("sqlstore: reservation must be non-empty")
	}

	// Guard on the CURRENT pin state so only real transitions are counted:
	// pinning skips already-pinned rows, unpinning skips unpinned ones.
	guard := notPinned
	if !pinned {
		guard = "NOT (" + guard + ")"
	}

	var changed int
	err := s.withTx(ctx, func(x queryer) error {
		res, err := x.ExecContext(ctx, s.rebind(
			`UPDATE utxos SET pinned=`+s.boolLit(pinned)+`
			 WHERE user_id=? AND reserved_by=? AND spent_by IS NULL AND `+guard),
			userID, reservation)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		changed = int(n)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return changed, nil
}

// staleReservationsSQL is the stale-reservation statement, bound to two
// parameters: the staleness cutoff and the group limit, in that order.
//
// It is a method rather than an inlined string for the same reason the three
// claim statements are package constants: so a test can plan the EXACT
// production text. The stale scan's predicates and idx_utxos_reserved_at's
// partial WHERE must stay in step — SQLite matches partial indexes by predicate
// text — and TestStaleScanIsIndexDriven fails if they ever drift far enough to
// cost the sweep a table scan.
//
// The subquery picks the oldest `limit` stale reservation groups; the outer
// join expands each to its unspent, unpinned reserved outpoints, ordered so a
// group's rows are contiguous and dated by their minimum reserved_at.
func (s *Store) staleReservationsSQL() string {
	return s.rebind(`
		SELECT u.user_id, u.reserved_by, g.oldest, u.txid, u.vout
		FROM utxos u
		JOIN (
			SELECT user_id, reserved_by, MIN(reserved_at) AS oldest, MIN(seq) AS min_seq
			FROM utxos
			WHERE reserved_by IS NOT NULL AND spent_by IS NULL AND ` + notPinned + `
			GROUP BY user_id, reserved_by
			HAVING MIN(reserved_at) < ?
			ORDER BY oldest, min_seq
			LIMIT ?
		) g ON u.user_id = g.user_id AND u.reserved_by = g.reserved_by
		WHERE u.reserved_by IS NOT NULL AND u.spent_by IS NULL AND ` + notPinnedOn("u") + `
		ORDER BY g.oldest, g.min_seq, u.seq`)
}

// FindStaleReservations implements [utxostore.Store]: reservations (grouped by
// user and token) of unspent, UNPINNED reserved rows whose OLDEST reserved_at
// is before olderThan, oldest first, up to limit reservations. Pinned rows are
// excluded from both the grouping and the expansion, so a returned ref can
// never name a coin an in-flight transaction spends; both predicates use the
// same notPinned text the 00002 migrations declare idx_utxos_reserved_at with,
// so the scan stays index-driven (see [Store.staleReservationsSQL]).
func (s *Store) FindStaleReservations(ctx context.Context, olderThan time.Time, limit int) ([]utxostore.ReservationRef, error) {
	if s.isClosed() {
		return nil, errClosed
	}
	if limit <= 0 {
		return nil, nil
	}

	rows, err := s.execer(ctx).QueryContext(ctx, s.staleReservationsSQL(), s.encTime(olderThan), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var (
		refs []utxostore.ReservationRef
		cur  *utxostore.ReservationRef
	)
	for rows.Next() {
		var (
			userID     int64
			reservedBy string
			oldest     tsScan
			txid       []byte
			vout       uint32
		)
		if err := rows.Scan(&userID, &reservedBy, s.tsDest(&oldest), &txid, &vout); err != nil {
			return nil, err
		}
		if cur == nil || cur.UserID != userID || cur.Reservation != reservedBy {
			refs = append(refs, utxostore.ReservationRef{
				Reservation: reservedBy,
				UserID:      userID,
				ReservedAt:  s.tsTime(oldest),
			})
			cur = &refs[len(refs)-1]
		}
		var op utxostore.Outpoint
		copy(op.TxID[:], txid)
		op.Vout = vout
		cur.Outpoints = append(cur.Outpoints, op)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return refs, nil
}
