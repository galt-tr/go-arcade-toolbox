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
//
// That is also why the three statements do not break satoshi ties the same
// way: seq ASC for the two ascending walks, seq DESC for the descending one.
// A descending walk that asked for seq ASC could not be served by
// idx_utxos_claim without a sort node — on an equal-value fuel pool that is a
// sort of the WHOLE pool per claim — so the tie direction follows the index,
// and [utxostore.Store] documents it that way (memstore matches this, not the
// reverse).
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
// restored to the order the candidate CTE selected in — RETURNING itself is
// unordered — and thereby match the ordering [utxostore.Store] documents:
// largest-first with NEWEST-first ties for ClaimLargestInsufficient,
// insertion order for the other two.
type claimedRow struct {
	u   *utxostore.UTXO
	seq int64
}

// runClaim executes a claim statement that reserves rows in one round trip and
// scans the RETURNING set. An EMPTY result is reported as "none" (nil): it is
// ambiguous — no claimable coin matched, or every match was skipped by FOR
// UPDATE SKIP LOCKED — but resolving that ambiguity costs a second query, and
// only the caller knows whether the answer still matters. [Store.ClaimableExists]
// is that second query; the funder runs it ONCE, at the point where it would
// otherwise report insufficient funds. See the package doc, "Design notes".
//
// Retry mirrors [Store.withTx], and for the same reason. Claims used to be the
// one operation in this package that ran with no lock-error retry at all: every
// mutation goes through withTx -> [sqlkit.WithRetry], while a claim went
// straight to the pool. A 40001/40P01/55P03 on the hot path therefore surfaced
// raw to the funder, which retries only on [utxostore.ErrContention], and a raw
// driver lock error is not that (see the package doc). Mode A masked it, because
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

// claimShape carries the ONE part of a claim's candidate predicate that differs
// between the three claim statements: the satoshi comparison, in both dialects.
// Pairing the spellings in a single value is what lets [Store.ClaimableExists]
// ask exactly the question a claim asked, whichever engine ran it.
//
// Only the sufficient shape is wired to a caller today (it is the one that
// answers "is there ANY claimable coin here", which is what the funder needs);
// the other two exist so claim_predicate_test.go can pin their claim
// statements' predicates too.
type claimShape struct {
	pg     string
	sqlite string
}

// The satoshi comparisons themselves are constants so the SQLite claim
// statements below stay compile-time constant expressions (no per-claim string
// building on the hot path); the claimShape values just pair them up.
const (
	satsSufficientPG       = `satoshis >= $4`
	satsSufficientSQLite   = `satoshis >= ?`
	satsInsufficientPG     = `satoshis < $4`
	satsInsufficientSQLite = `satoshis < ?`
	satsExactPG            = `satoshis = $4`
	satsExactSQLite        = `satoshis = ?`
)

var (
	shapeSmallestSufficient  = claimShape{pg: satsSufficientPG, sqlite: satsSufficientSQLite}
	shapeLargestInsufficient = claimShape{pg: satsInsufficientPG, sqlite: satsInsufficientSQLite}
	shapeExact               = claimShape{pg: satsExactPG, sqlite: satsExactSQLite}
)

// claimCandidatePG and claimCandidateSQLite are the scope-and-claimability half
// of a claim's candidate predicate — the claim statements' own words, minus the
// per-shape satoshi comparison — in each dialect. They are also, verbatim, the
// terms idx_utxos_claim's partial WHERE is declared with, so the probe below is
// a lookup that index can answer.
//
// The three SQLite claim statements are BUILT from claimCandidateSQLite (see
// the claim methods below), so on that engine drift is impossible by
// construction. The three PostgreSQL ones are not: claim_explain_test.go
// EXPLAINs those constants byte-for-byte through their exported twins, so they
// stay literal, and claim_predicate_test.go is their drift guard — it slices
// each statement's WHERE…ORDER BY segment and requires it to EQUAL this
// predicate plus the shape's comparison, so a term appended anywhere in the
// clause fails. Drift in either direction breaks the probe: a looser predicate
// invents contention, a tighter one misses it.
const (
	claimCandidatePG     = `user_id=$1 AND basket=$2 AND tier=$3 AND reserved_by IS NULL AND spent_by IS NULL AND NOT frozen`
	claimCandidateSQLite = `user_id=? AND basket=? AND tier=? AND reserved_by IS NULL AND spent_by IS NULL AND frozen = 0`
)

// claimCandidateExistsSQL renders the contention probe for one claim shape: the
// claim's candidate predicate with no ORDER BY, no LIMIT, no UPDATE and no FOR
// UPDATE. It takes no locks and stops at the first matching row.
func (s *Store) claimCandidateExistsSQL(shape claimShape) string {
	if s.engine == EnginePostgres {
		return `SELECT EXISTS(SELECT 1 FROM utxos WHERE ` + claimCandidatePG + ` AND ` + shape.pg + `)`
	}
	return `SELECT EXISTS(SELECT 1 FROM utxos WHERE ` + claimCandidateSQLite + ` AND ` + shape.sqlite + `)`
}

// ClaimableExists reports whether sc holds at least one CLAIMABLE row
// (unreserved, unspent, unfrozen) with Satoshis >= minSats — pass 0 to ask
// "any claimable coin at all". It reserves nothing, takes no locks, and is NOT
// part of [utxostore.Store]: callers reach it through a type assertion, so
// backends that cannot answer it simply do not offer it.
//
// It exists to answer one question, for one caller: the funder, at the moment
// it is about to report ErrNotEnoughFunds. Audit finding P2-4 (see the package
// doc, "Design notes").
//
// # Why an empty claim is not proof of an empty pool
//
// The old argument — a row skipped by FOR UPDATE SKIP LOCKED is, by
// construction, being reserved by the peer holding its lock inside that peer's
// own atomic statement, so no coin is orphaned and "empty" is honest — holds
// only while a claim's locks live for the length of the claim statement. In
// Mode A they do not: the claim runs on the caller's ambient transaction, so
// its locks live until the whole CreateAction commits. A concurrent
// CreateAction skips those rows for as long as that takes, sees a false-empty
// pool, and the funder turns it into a user-facing ErrNotEnoughFunds — which it
// does NOT retry — while the coins are still there and may yet be released by a
// rollback.
//
// # Why this sees what the claim could not
//
// It is a plain non-locking read, so under READ COMMITTED — PostgreSQL's
// default, and this module never overrides it (no store passes TxOptions when
// it opens a transaction) — it reads the last COMMITTED version of each row: a
// row a competitor has locked and reserved but not committed still reads as
// unreserved. That is exactly the row SKIP LOCKED had to skip, and exactly the
// row that comes back if the competitor rolls back. Under a stricter isolation
// level (REPEATABLE READ/SERIALIZABLE) the caller's snapshot would predate the
// competitor's write anyway, so the answer would be the same; only the
// competitor's COMMIT becoming visible mid-transaction depends on READ
// COMMITTED.
//
// Taking no locks is also what makes it safe to run on the ambient transaction
// (via execer): it cannot queue behind the very locks it is diagnosing. Running
// there rather than on a separate connection is load-bearing, not incidental —
// inside the caller's own transaction its OWN uncommitted reservations read as
// reserved, so they are correctly excluded. A probe on a separate connection
// would see the caller's own claims as still claimable and report contention
// after every failed pass, turning each honest insufficient-funds into the full
// retry budget.
//
// # Why the caller's answer converges
//
// A true here means "retry may help", and the funder turns it into
// [utxostore.ErrContention]: it releases its whole token and retries after a
// jittered backoff. In Mode A that release unwinds only the caller's own
// uncommitted claims — it cannot touch the competitor's rows, which are held
// under a different token. The retried claim is a NEW statement with a NEW
// snapshot, so it sees whatever the competitor committed: either the coins are
// free (claim succeeds) or they are genuinely gone (claim empty, probe now
// false, honest ErrNotEnoughFunds). Persistent contention is bounded by the
// funder's attempt budget and surfaces as funder.ErrUTXOContention — a
// retryable answer, which a false insufficient-funds is not.
func (s *Store) ClaimableExists(ctx context.Context, sc utxostore.Scope, minSats uint64) (bool, error) {
	if s.isClosed() {
		return false, errClosed
	}
	if err := validateScope(sc); err != nil {
		return false, err
	}
	return s.claimableExists(ctx, sc, shapeSmallestSufficient, minSats)
}

// claimableExists runs the probe for one claim shape. It is the shape-generic
// half of [Store.ClaimableExists], kept separate so every claim statement's
// predicate has a probe twin the drift guard can pin.
func (s *Store) claimableExists(ctx context.Context, sc utxostore.Scope, shape claimShape, bound uint64) (bool, error) {
	var exists boolScan
	err := s.execer(ctx).
		QueryRowContext(ctx, s.claimCandidateExistsSQL(shape), sc.UserID, sc.Basket, int64(sc.Tier), bound).
		Scan(s.boolDest(&exists))
	if err != nil {
		return false, fmt.Errorf("sqlstore: claimable probe: %w", err)
	}
	return s.boolGet(exists), nil
}

// validateScope rejects an underspecified scope. It is [validateClaim] minus
// the reservation token, which a probe does not take.
func validateScope(sc utxostore.Scope) error {
	switch {
	case sc.UserID <= 0:
		return errors.New("sqlstore: scope user id must be positive")
	case sc.Basket == "":
		return errors.New("sqlstore: scope basket must be non-empty")
	case !sc.Tier.Valid():
		return fmt.Errorf("sqlstore: invalid scope tier %d", sc.Tier)
	}
	return nil
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
			WHERE ` + claimCandidateSQLite + ` AND ` + satsSufficientSQLite + `
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
// coins with Satoshis < capSats, largest first, ties broken NEWEST first
// (reverse insertion order) — the direction the descending index walk yields.
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
			WHERE ` + claimCandidateSQLite + ` AND ` + satsInsufficientSQLite + `
			ORDER BY satoshis DESC, seq DESC LIMIT ?)
		RETURNING ` + utxoCols
		args = []any{reservation, s.encTime(ts), sc.UserID, sc.Basket, int64(sc.Tier), capSats, limit}
	}

	claimed, err := s.runClaim(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	// RETURNING is unordered; restore what the CTE selected — largest first,
	// ties NEWEST first, mirroring the ORDER BY satoshis DESC, seq DESC above.
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
			WHERE ` + claimCandidateSQLite + ` AND ` + satsExactSQLite + `
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

// releaseReservationSQL is the whole-reservation release statement, bound to
// two parameters: the user id and the reservation token, in that order.
//
// It is a builder rather than an inlined string for the same reason
// [Store.staleReservationsSQL] is: so a test can plan the EXACT production
// text. Its WHERE is the shape idx_utxos_reserved is designed around —
// (reserved_by, user_id) as the seek, spent_by IS NULL as the index's own
// partial predicate, and NOT pinned answered from the covering columns — and
// sweep_explain_test.go fails if the release ever stops resolving through it.
func (s *Store) releaseReservationSQL() string {
	return s.rebind(`UPDATE utxos SET reserved_by=NULL, reserved_at=NULL
		WHERE user_id=? AND reserved_by=? AND spent_by IS NULL AND ` + notPinned)
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
		res, err := x.ExecContext(ctx, s.releaseReservationSQL(), userID, reservation)
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
//
// On PostgreSQL the whole list is ONE statement. This is the funder's rollback
// tail — a drainBatch that comes up short releases everything it holds — so at
// 1000 TPS the loop it replaces was a round trip per coin on a path that is
// already conceding.
func (s *Store) ReleaseOutpoints(ctx context.Context, reservation string, ops []utxostore.Outpoint) error {
	if s.isClosed() {
		return errClosed
	}
	if reservation == "" {
		return errors.New("sqlstore: reservation must be non-empty")
	}

	return s.withTx(ctx, func(x queryer) error {
		if len(ops) == 0 {
			return nil
		}
		if s.engine == EnginePostgres {
			txids, vouts := pairArgs(ops)
			_, err := x.ExecContext(ctx,
				`UPDATE utxos u SET reserved_by=NULL, reserved_at=NULL, pinned=FALSE FROM `+pgKeyRel+
					` WHERE `+pgKeyMatch+` AND u.reserved_by=$3 AND u.spent_by IS NULL`,
				txids, vouts, reservation)
			return err
		}
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
// The INNER aggregate is what the 00003 migration's covering index is shaped
// for: every utxos column it touches — user_id, reserved_by, reserved_at, seq,
// pinned, and spent_by as the index's own partial predicate — lives in
// idx_utxos_reserved, so on PostgreSQL it runs index-only and never visits the
// heap. Adding a column to it that the index does not carry silently demotes
// the sweep to a heap scan per tick; sweep_explain_test.go is that guard.
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
