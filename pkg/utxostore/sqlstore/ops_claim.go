package sqlstore

import (
	"context"
	"errors"
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
