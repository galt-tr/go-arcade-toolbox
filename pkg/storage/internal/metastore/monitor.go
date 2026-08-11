package metastore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// This file gathers the metadata-store operations the background monitor
// (pkg/monitor) drives: the distributed job lease, plus the finder/mutator
// helpers the provider's async status hooks need but that the synchronous
// write path did not.

// --- distributed job lease -------------------------------------------------

// LeaseRepo is the monitor's distributed job lease, one row per gocron job name
// in the shared monitor_job_locks table. Ownership of a job is holding an
// unexpired lease (owner = this instance AND lease_until in the future); a
// crashed owner is reclaimed automatically once lease_until passes. lease_until
// is stored as Unix NANOSECONDS so the CAS is dialect-neutral.
type LeaseRepo struct{ s *Store }

// Leases returns the monitor job-lease repository.
func (s *Store) Leases() LeaseRepo { return LeaseRepo{s} }

// Acquire tries to take or renew the lease for job under owner until leaseUntil
// (Unix nanoseconds), evaluating expiry against now (Unix nanoseconds). It
// succeeds — returns true — when no row exists, when this instance already owns
// the row, or when the existing lease has expired; it returns false when
// another instance holds an unexpired lease.
//
// The claim is a portable two-step: an INSERT ... ON CONFLICT DO NOTHING to
// ensure the row exists, then a CAS UPDATE guarded by
// `owner = me OR lease_until < now`. RowsAffected on the UPDATE is the sole
// arbiter, so it is correct even when two instances INSERT concurrently (the
// loser's CAS matches 0 rows).
func (r LeaseRepo) Acquire(ctx context.Context, job, owner string, leaseUntil, now int64) (bool, error) {
	ins := r.s.rebind(
		`INSERT INTO monitor_job_locks (job_name, owner, lease_until)
		 VALUES (?, ?, ?)
		 ON CONFLICT (job_name) DO NOTHING`,
	)
	if _, err := r.s.execer(ctx).ExecContext(ctx, ins, job, owner, leaseUntil); err != nil {
		return false, fmt.Errorf("metastore: lease ensure-row %s: %w", job, err)
	}

	upd := r.s.rebind(
		`UPDATE monitor_job_locks SET owner = ?, lease_until = ?
		 WHERE job_name = ? AND (owner = ? OR lease_until < ?)`,
	)
	res, err := r.s.execer(ctx).ExecContext(ctx, upd, owner, leaseUntil, job, owner, now)
	if err != nil {
		return false, fmt.Errorf("metastore: lease claim %s: %w", job, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Release expires the lease for job early, but only if this instance still owns
// it (a slow run whose lease already expired and was reclaimed must not stomp
// the new owner). now is Unix nanoseconds. Releasing an unheld lease is a
// no-op success.
func (r LeaseRepo) Release(ctx context.Context, job, owner string, now int64) error {
	q := r.s.rebind("UPDATE monitor_job_locks SET lease_until = ? WHERE job_name = ? AND owner = ?")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, now, job, owner); err != nil {
		return fmt.Errorf("metastore: lease release %s: %w", job, err)
	}
	return nil
}

// --- known-tx monitor helpers ----------------------------------------------

// SetArcadeStatus records the latest arcade wire status on a known tx (and
// touches updated_at). The monitor persists it after every applied status
// event so the arcade lattice guard (arcade.Status.CanSupersede) survives
// restarts and SSE replays. Returns [ErrNotFound] when the txid is unknown.
func (r KnownTxRepo) SetArcadeStatus(ctx context.Context, txid, arcadeStatus string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE known_txs SET arcade_status = ?, updated_at = ? WHERE txid = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q, arcadeStatus, r.s.encTime(r.s.now()), raw)
	if err != nil {
		return fmt.Errorf("metastore: set arcade status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: set arcade status: %s: %w", txid, ErrNotFound)
	}
	return nil
}

// BulkSetArcadeStatus records arcadeStatus on every known tx whose txid is in
// txids in ONE UPDATE. The async batch applier groups txids by their (identical)
// arcade status value and calls this once per distinct value, mirroring the
// single-row [KnownTxRepo.SetArcadeStatus] — a missing txid is simply not
// updated (the benign ErrNotFound the per-row writer tolerates).
//
// The txids are bound (and, on PostgreSQL, locked) in ascending storage order —
// see lockorder.go: without it two concurrent apply shards with overlapping
// txid sets could take the same rows in opposite orders and deadlock.
func (r KnownTxRepo) BulkSetArcadeStatus(ctx context.Context, txids []string, arcadeStatus string) error {
	if len(txids) == 0 {
		return nil
	}
	ids, err := txidArgs(txids)
	if err != nil {
		return err
	}
	args := append([]any{arcadeStatus, r.s.encTime(r.s.now())}, ids...)
	q := r.s.rebind("UPDATE known_txs SET arcade_status = ?, updated_at = ? WHERE " +
		r.s.txidLockOrderedIn("known_txs", "txid", len(ids)))
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: bulk set arcade status: %w", err)
	}
	return nil
}

// MarkPolled stamps last_polled_at = now on every known tx in txids WITHOUT
// touching updated_at. It is how the poll fallbacks record that they ATTEMPTED
// a row, whatever the outcome.
//
// This is the anti-starvation half of the poll: the work-list finders order by
// last_polled_at, so a row the poll selected but could not apply (an
// unverifiable proof, a GetTx failure, a batch apply error) moves to the BACK of
// the queue instead of pinning its head and hiding the entire backlog behind it.
// The invariant it encodes: a row can never be re-selected forever without the
// poll making progress past it.
//
// updated_at is deliberately left alone — it means "when the row's state last
// changed", and the resend grace / staleness cutoffs read it as such.
func (r KnownTxRepo) MarkPolled(ctx context.Context, txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	ids, err := txidArgs(txids)
	if err != nil {
		return err
	}
	args := append([]any{r.s.encTime(r.s.now())}, ids...)
	q := r.s.rebind("UPDATE known_txs SET last_polled_at = ? WHERE " +
		r.s.txidLockOrderedIn("known_txs", "txid", len(ids)))
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: mark polled: %w", err)
	}
	return nil
}

// MarkSuspect moves a known tx into the suspect-failed grace window recording
// everything the reject reconciler will read back via FindSuspectFailed:
// status = suspectFailed, suspect_since, the competing txids, the arcade wire
// status, and arcade's own reason for the refusal. It resets
// verified_rejected_at to NULL so the reconciler's two-pass guard starts fresh
// for this suspect cycle (a stale stamp from an earlier cycle must never let a
// re-suspected tx skip the second pass). This is the ASYNC-reject writer;
// unlike the synchronous 4xx path it does NOT release the reserved inputs (that
// is the reconciler's job). It leaves raw_tx / input_beef intact so the
// reconciler can re-verify.
//
// rejectReason is the event's extraInfo. It is persisted HERE, at first sight,
// because it is not re-fetchable: arcade drops a rejected transaction's record
// on its own schedule, and a REJECTED event whose GET /tx already 404s leaves
// no other way to learn the cause. An empty reason binds NULL and so preserves
// any reason recorded earlier in this suspect cycle.
func (r KnownTxRepo) MarkSuspect(ctx context.Context, txid string, suspectSince time.Time, competingTxs []string, arcadeStatus, rejectReason string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	var competing any
	if len(competingTxs) > 0 {
		b, mErr := json.Marshal(competingTxs)
		if mErr != nil {
			return fmt.Errorf("metastore: encode competing_txs: %w", mErr)
		}
		competing = string(b)
	}
	q := r.s.rebind(
		`UPDATE known_txs
		 SET status = ?, arcade_status = ?, competing_txs = ?, suspect_since = ?,
		     verified_rejected_at = NULL, reject_reason = COALESCE(?, reject_reason), updated_at = ?
		 WHERE txid = ?`,
	)
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(KnownTxStatusSuspectFailed), arcadeStatus, competing,
		r.s.encTime(suspectSince), rejectReasonArg(rejectReason), r.s.encTime(r.s.now()), raw)
	if err != nil {
		return fmt.Errorf("metastore: mark suspect: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: mark suspect: %s: %w", txid, ErrNotFound)
	}
	return nil
}

// ClearProof demotes a previously-proven known tx after a reorg: it drops the
// stored proof (block height/hash, merkle path/root, arcade_status) and resets
// the status to newStatus (typically unconfirmed) so the proof pipeline
// re-checks it. Returns [ErrNotFound] when the txid is unknown.
func (r KnownTxRepo) ClearProof(ctx context.Context, txid string, newStatus wdk.ProvenTxReqStatus) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind(
		`UPDATE known_txs
		 SET status = ?, arcade_status = NULL, block_height = NULL, block_hash = NULL,
		     merkle_path = NULL, merkle_root = NULL, updated_at = ?
		 WHERE txid = ?`,
	)
	res, err := r.s.execer(ctx).ExecContext(ctx, q, string(newStatus), r.s.encTime(r.s.now()), raw)
	if err != nil {
		return fmt.Errorf("metastore: clear proof: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: clear proof: %s: %w", txid, ErrNotFound)
	}
	return nil
}

// FindUnsent returns known txs queued for delayed broadcast (status = unsent),
// oldest first, up to limit — the SendWaiting sweep's work list.
func (r KnownTxRepo) FindUnsent(ctx context.Context, limit int) ([]KnownTx, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs WHERE status = ? " +
		"ORDER BY created_at ASC, txid ASC" + clause)
	args := append([]any{string(wdk.ProvenTxStatusUnsent)}, pageArgs...)
	return r.queryList(ctx, q, args)
}

// FindResendable returns known txs that hold a stored raw tx, were never
// broadcast (was_broadcast=0), and are re-drivable: the delayed queue (status
// 'unsent') PLUS txs stranded pre-broadcast (status 'unprocessed') — e.g. a
// broadcast the arcade circuit breaker short-circuited while open. Only rows
// older than grace are returned, so a freshly-created 'unprocessed' tx whose
// own ProcessAction is about to broadcast it is not stolen by the sweep.
// Oldest first, up to limit. This is the SendWaiting sweep's work list; it
// makes stranded transactions self-heal once arcade is reachable again.
func (r KnownTxRepo) FindResendable(ctx context.Context, grace time.Duration, limit int) ([]KnownTx, error) {
	cutoff := r.s.encTime(r.s.now().Add(-grace))
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	// The delayed queue ('unsent') sends promptly; the stranded set ('unprocessed')
	// is gated by grace so a freshly-created tx whose own ProcessAction is about
	// to broadcast it is not double-sent by the sweep.
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"WHERE was_broadcast = ? AND raw_tx IS NOT NULL " +
		"AND (status = ? OR (status = ? AND updated_at <= ?)) " +
		"ORDER BY created_at ASC, txid ASC" + clause)
	args := append([]any{
		r.s.boolVal(false),
		string(wdk.ProvenTxStatusUnsent),
		string(wdk.ProvenTxStatusUnprocessed), cutoff,
	}, pageArgs...)
	return r.queryList(ctx, q, args)
}

// pollOrder is the work-list ordering shared by every poll finder:
// LEAST-RECENTLY-ATTEMPTED first (last_polled_at, epoch = never polled), then
// oldest state change, then txid for a total order. Ordering by the ATTEMPT
// clock rather than by updated_at is what stops a row the poll cannot apply from
// being re-selected at the head of every tick forever — see the 00004 migration
// and [KnownTxRepo.MarkPolled].
const pollOrder = " ORDER BY last_polled_at ASC, updated_at ASC, txid ASC"

// FindByStatusOlderThan returns known txs whose status is in statuses and whose
// updated_at is at or before cutoff, least-recently-POLLED first, up to limit.
// It powers the poll fallbacks (stale in-flight txs to re-poll; unproven txs to
// re-check for proofs). An empty statuses set returns nothing.
//
// The caller MUST record the attempt via [KnownTxRepo.MarkPolled] for every row
// it takes, or the same head rows are handed back every tick.
func (r KnownTxRepo) FindByStatusOlderThan(ctx context.Context, cutoff time.Time, limit int, statuses ...wdk.ProvenTxReqStatus) ([]KnownTx, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"WHERE status IN (" + inPlaceholders(len(statuses)) + ") AND updated_at <= ?" +
		pollOrder + clause)
	args := make([]any, 0, len(statuses)+1+len(pageArgs))
	for _, st := range statuses {
		args = append(args, string(st))
	}
	args = append(args, r.s.encTime(cutoff))
	args = append(args, pageArgs...)
	return r.queryList(ctx, q, args)
}

// FindMissingArcadeStatus returns known txs in one of statuses that carry NO
// arcade status at all (NULL or the empty string), untouched since cutoff,
// least-recently-polled first, up to limit. It is the REPAIR work list.
//
// Arcade is the source of truth for a transaction's status, so a row the wallet
// knows about with an empty arcade_status is a row on which the local ledger has
// DIVERGED from arcade — it was never delivered (arcade drops events for a slow
// SSE client), or its apply failed, or its SSE batch was skipped. Those rows
// must be reachable no matter where they happen to sort inside the general poll
// list, which is why this is its own predicate (backed by its own partial index)
// rather than a filter bolted onto [FindByStatusOlderThan]: at 1000 TPS the
// general list is far larger than any batch limit, and a stranded row that sorts
// behind it would never be SELECTed. This query is what makes the "23,745 rows
// with an empty arcade_status, count frozen" state self-heal.
func (r KnownTxRepo) FindMissingArcadeStatus(ctx context.Context, cutoff time.Time, limit int, statuses ...wdk.ProvenTxReqStatus) ([]KnownTx, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"WHERE status IN (" + inPlaceholders(len(statuses)) + ") AND updated_at <= ? " +
		"AND (arcade_status IS NULL OR arcade_status = '')" +
		pollOrder + clause)
	args := make([]any, 0, len(statuses)+1+len(pageArgs))
	for _, st := range statuses {
		args = append(args, string(st))
	}
	args = append(args, r.s.encTime(cutoff))
	args = append(args, pageArgs...)
	return r.queryList(ctx, q, args)
}

// CountMissingArcadeStatus returns how many known txs in one of statuses carry
// no arcade status at all — the size of the divergence-from-arcade backlog. It
// is the counter the repair sweep logs so a frozen (never-draining) backlog is
// visible without querying the database by hand.
func (r KnownTxRepo) CountMissingArcadeStatus(ctx context.Context, statuses ...wdk.ProvenTxReqStatus) (int, error) {
	if len(statuses) == 0 {
		return 0, nil
	}
	q := r.s.rebind("SELECT COUNT(*) FROM known_txs " +
		"WHERE status IN (" + inPlaceholders(len(statuses)) + ") " +
		"AND (arcade_status IS NULL OR arcade_status = '')")
	args := make([]any, 0, len(statuses))
	for _, st := range statuses {
		args = append(args, string(st))
	}
	var n int
	if err := r.s.execer(ctx).QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("metastore: count known txs missing arcade status: %w", err)
	}
	return n, nil
}

// FindProvenAtOrAbove returns completed (proven) known txs whose block_height is
// at or above height, lowest first, up to limit — the reorg-demote work list.
func (r KnownTxRepo) FindProvenAtOrAbove(ctx context.Context, height uint32, limit int) ([]KnownTx, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"WHERE status = ? AND block_height IS NOT NULL AND block_height >= ? " +
		"ORDER BY block_height ASC, txid ASC" + clause)
	args := append([]any{string(wdk.ProvenTxStatusCompleted), int64(height)}, pageArgs...)
	return r.queryList(ctx, q, args)
}

// queryList runs a knownTxCols SELECT and scans the rows.
func (r KnownTxRepo) queryList(ctx context.Context, q string, args []any) ([]KnownTx, error) {
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: list known txs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []KnownTx
	for rows.Next() {
		kt, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *kt)
	}
	return out, rows.Err()
}

// --- transaction monitor helpers -------------------------------------------

// FindByTxIDAllUsers returns every transaction row carrying txid (display hex)
// regardless of user, newest first. The monitor works by txid (arcade knows
// nothing about wallet users), so this is how the async hooks resolve the
// user/reference/transaction-id they need to spend inputs and promote change.
func (r TransactionsRepo) FindByTxIDAllUsers(ctx context.Context, txid string) ([]wdk.TableTransaction, error) {
	raw, err := encTxID(txid)
	if err != nil {
		return nil, err
	}
	q := r.s.rebind("SELECT " + txCols + " FROM transactions WHERE txid = ? ORDER BY transaction_id DESC")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, raw)
	if err != nil {
		return nil, fmt.Errorf("metastore: find transaction by txid (all users): %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []wdk.TableTransaction
	for rows.Next() {
		t, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// FindAbandonedBefore returns transactions eligible for the abort sweep — those
// in a build-but-never-broadcast status (unsigned/nosend) created at or before
// cutoff — oldest first, up to limit. It is the absolute-cutoff form of
// [TransactionsRepo.FindAbandoned] (which takes a grace duration against the
// store clock); the monitor computes cutoff from its own clock.
func (r TransactionsRepo) FindAbandonedBefore(ctx context.Context, cutoff time.Time, limit int) ([]wdk.TableTransaction, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + txCols + " FROM transactions " +
		"WHERE status IN ('unsigned', 'nosend') AND created_at <= ? " +
		"ORDER BY created_at ASC, transaction_id ASC" + clause)
	args := append([]any{r.s.encTime(cutoff)}, pageArgs...)
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find abandoned before: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []wdk.TableTransaction
	for rows.Next() {
		t, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}
