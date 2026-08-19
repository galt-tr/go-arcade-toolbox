package metastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// KnownTxStatusSuspectFailed is an arcade-specific ProvenTxReqStatus (not part
// of the frozen wdk enum) marking a transaction the monitor suspects has failed
// but is holding in a grace window before treating as terminally failed. The
// suspect_since column records when it entered the window; FindSuspectFailed
// returns rows whose window has elapsed.
const KnownTxStatusSuspectFailed wdk.ProvenTxReqStatus = "suspectFailed"

// KnownTxStatusStuck is an arcade-specific terminal ProvenTxReqStatus the
// reject→release reconciler (Task 19) escalates a suspect to once it has sat
// unresolved past the max-quarantine window without ever becoming provably dead
// (e.g. a double spend whose competitor never won, or a tx GetTx could never
// re-verify). A stuck tx is operator-visible and, unlike a provably-dead tx, its
// inputs are NEVER auto-released — resurrecting a possibly-live input is the one
// thing the reconciler must never do. It is deliberately outside the suspect
// scan, so the reconciler never re-processes it.
const KnownTxStatusStuck wdk.ProvenTxReqStatus = "stuck"

// KnownTxStatusAborted is an arcade-specific ProvenTxReqStatus marking a known
// tx whose wallet transaction was aborted BEFORE any broadcast evidence
// existed. Two fences hold it here unconditionally:
// [KnownTxRepo.FindResendable] never selects it, and [KnownTxRepo.ClaimForSend]
// refuses to claim it.
//
// The third fence is an OBLIGATION ON CALLERS, not a property of this constant:
// every requeue-shaped write must pass [KnownTxNeverRequeueStatuses] as its
// skip set. An unguarded Upsert or UpdateStatus will happily move an aborted
// row back to 'unsent', from where the sweep would broadcast it.
//
// Wiring status: processNewTx's Upsert passes the set and treats a skip as a
// hard error, so a re-drive can no longer resurrect an aborted row that way.
// The provider's other requeue-shaped write, queueDelayed's UpdateStatus, is
// still unguarded and is owned by the next task in the sequence; until it
// lands, a delayed send can still walk an aborted row back to 'unsent'.
const KnownTxStatusAborted wdk.ProvenTxReqStatus = "aborted"

// knownTxPreBroadcastStatuses are the statuses from which a known tx has
// certainly not been handed to the network yet, and from which the one-row
// arbiter ([KnownTxRepo.TransitionToAborted] and [KnownTxRepo.ClaimForSend])
// may still move it. Both halves share this from-set exactly, which is what
// makes them mutually exclusive.
//
// The two CAS TARGETS — 'aborted' and 'sending' — must stay OUTSIDE this set.
// In particular, do not "simplify" it to [wdk.ProvenTxReqStatus.IsInFlight],
// which contains 'sending': that would let an abort take a live send.
//
// It is exactly the pre-broadcast statuses this codebase writes; a new
// pre-broadcast writer must extend this set, or its rows are silently neither
// abortable nor claimable.
var knownTxPreBroadcastStatuses = []wdk.ProvenTxReqStatus{
	wdk.ProvenTxStatusUnsent,
	wdk.ProvenTxStatusUnprocessed,
	wdk.ProvenTxStatusNoSend,
}

// KnownTxNeverRequeueStatuses are the known-tx statuses from which a raw tx
// must never be RE-QUEUED for broadcast: everything past the broadcast stage,
// plus the fenced/terminal states. The direction is in the name, and it is the
// whole contract — see the warning below before reaching for this set.
//
// It is derived from [wdk.ProvenTxReqBeyondBroadcastStageStatuses], so a future
// beyond-broadcast status is fenced automatically.
//
// USE IT ONLY as the skip set of a BACKWARD transition, i.e. one whose target
// status would put the bytes back on a broadcast work list ('unsent' or
// 'unprocessed'): queueDelayed-style UpdateStatus calls and processNewTx's
// Upsert, so a re-drive or a SendWith batch cannot resurrect an
// aborted/suspect/terminal row into the delayed queue.
//
// NEVER use it as the guard of a FORWARD transition. It contains the very
// statuses forward progress moves OFF, so it would block that progress and
// then leave the row where the sweep finds it again. That is why 'sending' is
// in the set: a re-drive must not regress a row a broadcaster is mid-POST on,
// but guarding the FORWARD write with this set is the mirror-image bug — a
// claimed send advances to unconfirmed when its 202/SEEN lands, and refusing
// that advance would leave the row at 'sending' to be re-POSTed forever. A
// claimant that genuinely died is recovered by [KnownTxRepo.FindResendable]'s
// graced 'sending' arm plus [KnownTxRepo.ReclaimStaleSend] — a read-side sweep
// and a clock re-stamp, never a status regression. The existing forward writers
// (applyAcceptedBroadcast, applySeen, applySeenBatch) must keep passing
// [wdk.ProvenTxReqBeyondBroadcastStageStatuses]; only the backward guard in
// queueDelayed is a candidate for this set.
var KnownTxNeverRequeueStatuses = append(
	append([]wdk.ProvenTxReqStatus(nil), wdk.ProvenTxReqBeyondBroadcastStageStatuses...),
	KnownTxStatusAborted,
	KnownTxStatusSuspectFailed,
	KnownTxStatusStuck,
	wdk.ProvenTxStatusInvalid,
	wdk.ProvenTxStatusDoubleSpend,
	wdk.ProvenTxStatusSending,
)

// KnownTx is the storage-wide record for a transaction the wallet knows about,
// keyed by txid. It merges the wdk ProvenTxReq processing state, the retained
// rawtx / input-BEEF / proof material, and arcade-specific broadcast bookkeeping
// (arcade_status, competing_txs, suspect_since). Byte fields hold raw bytes; the
// provider converts to/from the wdk display types.
type KnownTx struct {
	TxID                string // display hex
	Status              wdk.ProvenTxReqStatus
	ArcadeStatus        *string
	Attempts            uint64
	RebroadcastAttempts uint64
	WasBroadcast        bool
	Notified            bool
	Batch               *string
	Notify              string // opaque JSON, round-tripped unchanged (default "{}")
	RawTx               []byte
	InputBEEF           []byte
	BlockHeight         *uint32
	BlockHash           []byte
	MerklePath          []byte
	MerkleRoot          []byte
	CompetingTxs        []string
	SuspectSince        *time.Time
	VerifiedRejectedAt  *time.Time
	// RejectReason is arcade's own prose explaining why it refused this
	// transaction (the 4xx body on the synchronous path, extraInfo on the async
	// one), captured the first time it is learned. nil means no reason has ever
	// been recorded — which for a REJECTED transaction is itself a finding, not
	// a default. See the 00007 migration.
	RejectReason *string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// KnownTxRepo persists the known-transaction state.
type KnownTxRepo struct{ s *Store }

// KnownTx returns the known-transactions repository.
func (s *Store) KnownTx() KnownTxRepo { return KnownTxRepo{s} }

// ListByArcadeStatus returns known txs whose arcade status equals arcadeStatus,
// most-recently-updated first, up to limit. An empty arcadeStatus matches rows
// with no arcade status yet (NULL or ""). Deployment-wide; for UI drill-down.
func (r KnownTxRepo) ListByArcadeStatus(ctx context.Context, arcadeStatus string, limit int) ([]KnownTx, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	if arcadeStatus == "" {
		q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
			"WHERE arcade_status IS NULL OR arcade_status = '' " +
			"ORDER BY updated_at DESC, txid ASC" + clause)
		return r.queryList(ctx, q, pageArgs)
	}
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs WHERE arcade_status = ? " +
		"ORDER BY updated_at DESC, txid ASC" + clause)
	return r.queryList(ctx, q, append([]any{arcadeStatus}, pageArgs...))
}

// ListRecent returns the most-recently-updated known txs regardless of status,
// up to limit — the "all in-flight / recent transactions" drill-down (during a
// blast this is the flood of fuel-funded txs, newest first). Deployment-wide.
func (r KnownTxRepo) ListRecent(ctx context.Context, limit int) ([]KnownTx, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"ORDER BY updated_at DESC, txid ASC" + clause)
	return r.queryList(ctx, q, pageArgs)
}

// ListByStatus returns known txs in the given wallet-side status, most-recently-
// updated first, up to limit. Deployment-wide; for UI drill-down.
func (r KnownTxRepo) ListByStatus(ctx context.Context, status wdk.ProvenTxReqStatus, limit int) ([]KnownTx, error) {
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs WHERE status = ? " +
		"ORDER BY updated_at DESC, txid ASC" + clause)
	return r.queryList(ctx, q, append([]any{string(status)}, pageArgs...))
}

// CountByArcadeStatus returns the number of known transactions in each
// arcade-reported status (SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES / MINED /
// REJECTED / …). Rows with no arcade status yet are grouped under "" (not yet
// broadcast / no status received). known_txs is not user-scoped, so this is a
// deployment-wide count. For observability.
func (r KnownTxRepo) CountByArcadeStatus(ctx context.Context) (map[string]int, error) {
	q := r.s.rebind("SELECT COALESCE(arcade_status, ''), COUNT(*) FROM known_txs GROUP BY arcade_status")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("metastore: count known txs by arcade status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int)
	for rows.Next() {
		var (
			status string
			n      int
		)
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("metastore: scan known tx arcade status count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

const knownTxCols = "txid, status, arcade_status, attempts, rebroadcast_attempts, " +
	"was_broadcast, notified, batch, notify, raw_tx, input_beef, block_height, " +
	"block_hash, merkle_path, merkle_root, competing_txs, suspect_since, created_at, updated_at, " +
	"verified_rejected_at, reject_reason"

func (r KnownTxRepo) scan(sc rowScanner) (*KnownTx, error) {
	var (
		kt           KnownTx
		txidBytes    []byte
		status       string
		arcadeStatus sql.NullString
		attempts     int64
		rebroadcast  int64
		wasBroadcast boolScan
		notified     boolScan
		batch        sql.NullString
		blockHeight  sql.NullInt64
		competing    []byte
		suspectSince tsScan
		createdAt    tsScan
		updatedAt    tsScan
		verifiedRej  tsScan
		rejectReason sql.NullString
	)
	dest := []any{
		&txidBytes, &status, &arcadeStatus, &attempts, &rebroadcast,
		r.s.boolDest(&wasBroadcast), r.s.boolDest(&notified), &batch, &kt.Notify,
		&kt.RawTx, &kt.InputBEEF, &blockHeight, &kt.BlockHash, &kt.MerklePath,
		&kt.MerkleRoot, &competing, r.s.tsDest(&suspectSince),
		r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt), r.s.tsDest(&verifiedRej),
		&rejectReason,
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	if txid := decTxIDPtr(txidBytes); txid != nil {
		kt.TxID = *txid
	}
	kt.Status = wdk.ProvenTxReqStatus(status)
	kt.ArcadeStatus = nullStr(arcadeStatus)
	kt.Attempts = uint64(attempts)               //nolint:gosec // non-negative counter.
	kt.RebroadcastAttempts = uint64(rebroadcast) //nolint:gosec // non-negative counter.
	// WasBroadcast is reported sticky: the column OR the status' own evidence.
	kt.WasBroadcast = r.s.boolGet(wasBroadcast) || kt.Status.WasBroadcastStatus()
	kt.Notified = r.s.boolGet(notified)
	kt.Batch = nullStr(batch)
	if blockHeight.Valid {
		v := uint32(blockHeight.Int64) //nolint:gosec // block height fits uint32.
		kt.BlockHeight = &v
	}
	if len(competing) > 0 {
		if err := json.Unmarshal(competing, &kt.CompetingTxs); err != nil {
			return nil, fmt.Errorf("metastore: decode competing_txs: %w", err)
		}
	}
	kt.SuspectSince = r.s.tsTimePtr(suspectSince)
	kt.CreatedAt = r.s.tsTime(createdAt)
	kt.UpdatedAt = r.s.tsTime(updatedAt)
	kt.VerifiedRejectedAt = r.s.tsTimePtr(verifiedRej)
	kt.RejectReason = nullStr(rejectReason)
	return &kt, nil
}

// stickyOr renders the dialect-specific boolean-OR of the existing column and
// the proposed EXCLUDED value, so was_broadcast is never cleared by a later
// upsert that carries a non-broadcast status.
func (s *Store) stickyOr(col, excl string) string {
	if s.engine == EnginePostgres {
		return "(" + col + " OR " + excl + ")"
	}
	return "(" + col + " != 0 OR " + excl + " != 0)"
}

// Upsert inserts or updates a known-transaction row. When skipForStatuses is
// non-empty the update half is guarded: an existing row whose current status is
// in that set is left untouched (applied=false) — the mechanism that stops a
// tx regressing once it has moved beyond the broadcast stage. A fresh insert,
// or an applied update, it returns nil. On a guarded skip (an existing row whose
// status is in skipForStatuses) it returns [ErrStatusUpdateSkipped], matching
// the package-wide guarded-transition convention. was_broadcast is sticky.
func (r KnownTxRepo) Upsert(ctx context.Context, kt KnownTx, skipForStatuses ...wdk.ProvenTxReqStatus) error {
	raw, err := encTxID(kt.TxID)
	if err != nil {
		return err
	}
	now := r.s.encTime(r.s.now())
	wasBroadcast := kt.WasBroadcast || kt.Status.WasBroadcastStatus()
	notify := kt.Notify
	if notify == "" {
		notify = "{}"
	}
	var competing any
	if len(kt.CompetingTxs) > 0 {
		b, mErr := json.Marshal(kt.CompetingTxs)
		if mErr != nil {
			return fmt.Errorf("metastore: encode competing_txs: %w", mErr)
		}
		competing = string(b)
	}
	var blockHeight any
	if kt.BlockHeight != nil {
		blockHeight = int64(*kt.BlockHeight)
	}

	conflict := "ON CONFLICT (txid) DO UPDATE SET " +
		"status = EXCLUDED.status, arcade_status = EXCLUDED.arcade_status, " +
		"attempts = EXCLUDED.attempts, rebroadcast_attempts = EXCLUDED.rebroadcast_attempts, " +
		"was_broadcast = " + r.s.stickyOr("known_txs.was_broadcast", "EXCLUDED.was_broadcast") + ", " +
		"notified = EXCLUDED.notified, batch = EXCLUDED.batch, notify = EXCLUDED.notify, " +
		"raw_tx = EXCLUDED.raw_tx, input_beef = EXCLUDED.input_beef, " +
		"block_height = EXCLUDED.block_height, block_hash = EXCLUDED.block_hash, " +
		"merkle_path = EXCLUDED.merkle_path, merkle_root = EXCLUDED.merkle_root, " +
		"competing_txs = EXCLUDED.competing_txs, suspect_since = EXCLUDED.suspect_since, " +
		"updated_at = EXCLUDED.updated_at, verified_rejected_at = EXCLUDED.verified_rejected_at, " +
		// Sticky, like was_broadcast: an upsert that carries no reason must never
		// erase one already recorded. Only the rejection writers set this column,
		// and they are the only ones that ever have a reason to give; every other
		// upsert (the freshly-signed-tx insert, an internalize) passes nil and
		// would otherwise blank a diagnosis we cannot re-fetch.
		"reject_reason = COALESCE(EXCLUDED.reject_reason, known_txs.reject_reason)"
	var guardArgs []any
	if len(skipForStatuses) > 0 {
		conflict += " WHERE known_txs.status NOT IN (" + inPlaceholders(len(skipForStatuses)) + ")"
		for _, st := range skipForStatuses {
			guardArgs = append(guardArgs, string(st))
		}
	}

	q := r.s.rebind(
		`INSERT INTO known_txs (` + knownTxCols + `)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ` + conflict,
	)
	args := []any{
		//nolint:gosec // attempt counters are small, well within int64.
		raw, string(kt.Status), strPtrArg(kt.ArcadeStatus), int64(kt.Attempts), int64(kt.RebroadcastAttempts),
		r.s.boolVal(wasBroadcast), r.s.boolVal(kt.Notified), strPtrArg(kt.Batch), notify,
		bytesArg(kt.RawTx), bytesArg(kt.InputBEEF), blockHeight, bytesArg(kt.BlockHash),
		bytesArg(kt.MerklePath), bytesArg(kt.MerkleRoot), competing, r.s.encTimePtr(kt.SuspectSince),
		now, now, r.s.encTimePtr(kt.VerifiedRejectedAt), strPtrArg(kt.RejectReason),
	}
	args = append(args, guardArgs...)

	res, err := r.s.execer(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: upsert known tx: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	// A fresh insert or an applied update affects one row; a guarded conflict
	// whose WHERE failed affects none. There is no ErrNotFound case for an
	// upsert (it inserts when absent).
	if n == 0 && len(skipForStatuses) > 0 {
		return ErrStatusUpdateSkipped
	}
	return nil
}

// UpdateStatus transitions a known tx's status, optionally guarded by
// skipForStatuses (the transition is skipped when the current status is in that
// set). It returns nil when applied, [ErrStatusUpdateSkipped] when the row
// exists but the guard blocked the transition, and [ErrNotFound] when no such
// txid exists — the same convention as [TransactionsRepo.UpdateStatus], with
// the not-found and guard-skipped cases always distinguishable. was_broadcast
// is set sticky when the new status is broadcast evidence.
func (r KnownTxRepo) UpdateStatus(ctx context.Context, txid string, status wdk.ProvenTxReqStatus, skipForStatuses ...wdk.ProvenTxReqStatus) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	conds := []string{"txid = ?"}
	args := []any{string(status)}
	setWasBroadcast := ""
	if status.WasBroadcastStatus() {
		setWasBroadcast = ", was_broadcast = " + r.s.boolTrueLiteral()
	}
	args = append(args, r.s.encTime(r.s.now()), raw)
	if len(skipForStatuses) > 0 {
		conds = append(conds, "status NOT IN ("+inPlaceholders(len(skipForStatuses))+")")
		for _, st := range skipForStatuses {
			args = append(args, string(st))
		}
	}
	q := r.s.rebind("UPDATE known_txs SET status = ?" + setWasBroadcast + ", updated_at = ? WHERE " + joinAnd(conds))
	res, err := r.s.execer(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: update known tx status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	// Zero rows: distinguish "no such txid" from "guard blocked an existing row".
	exists, err := r.existsByTxID(ctx, raw)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("metastore: update known tx status: %s: %w", txid, ErrNotFound)
	}
	return ErrStatusUpdateSkipped
}

// BulkUpdateStatus transitions every known tx whose txid is in txids to status,
// guarded by skipForStatuses (rows whose current status is in that set are left
// untouched) exactly like the single-row [KnownTxRepo.UpdateStatus] — but as ONE
// set-based UPDATE with no per-row skip/not-found bookkeeping: a row the guard
// blocks, or a txid that does not exist, is simply not updated, which is
// precisely how the async batch applier tolerates ErrStatusUpdateSkipped /
// ErrNotFound from the per-row writer. was_broadcast is set sticky when the new
// status is broadcast evidence, matching UpdateStatus.
//
// The txids are bound (and, on PostgreSQL, locked) in ascending storage order —
// see lockorder.go.
func (r KnownTxRepo) BulkUpdateStatus(ctx context.Context, txids []string, status wdk.ProvenTxReqStatus, skipForStatuses ...wdk.ProvenTxReqStatus) error {
	if len(txids) == 0 {
		return nil
	}
	setWasBroadcast := ""
	if status.WasBroadcastStatus() {
		setWasBroadcast = ", was_broadcast = " + r.s.boolTrueLiteral()
	}
	ids, err := txidArgs(txids)
	if err != nil {
		return err
	}
	args := append([]any{string(status), r.s.encTime(r.s.now())}, ids...)
	conds := []string{r.s.txidLockOrderedIn("known_txs", "txid", len(ids))}
	if len(skipForStatuses) > 0 {
		conds = append(conds, "status NOT IN ("+inPlaceholders(len(skipForStatuses))+")")
		for _, st := range skipForStatuses {
			args = append(args, string(st))
		}
	}
	q := r.s.rebind("UPDATE known_txs SET status = ?" + setWasBroadcast + ", updated_at = ? WHERE " + joinAnd(conds))
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: bulk update known tx status: %w", err)
	}
	return nil
}

// existsByTxID reports whether a known-tx row with the given raw txid exists.
func (r KnownTxRepo) existsByTxID(ctx context.Context, raw []byte) (bool, error) {
	q := r.s.rebind("SELECT 1 FROM known_txs WHERE txid = ?")
	var one int
	err := r.s.execer(ctx).QueryRowContext(ctx, q, raw).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("metastore: known tx exists: %w", err)
	}
	return true, nil
}

// rejectReasonArg binds a rejection reason for a `COALESCE(?, reject_reason)`
// assignment: a non-empty reason is written, an empty one binds NULL so the
// COALESCE keeps whatever was recorded before. Arcade legitimately emits a
// REJECTED record with no reason at all, and that must not erase the reason an
// earlier event (or the synchronous 4xx body) already supplied.
func rejectReasonArg(reason string) any {
	if reason == "" {
		return nil
	}
	return reason
}

// boolTrueLiteral renders a literal true for the engine (postgres TRUE, sqlite
// 1). Used where a value, not a bound parameter, is wanted.
func (s *Store) boolTrueLiteral() string {
	if s.engine == EnginePostgres {
		return "TRUE"
	}
	return "1"
}

// MarkSuspectFailed moves a known tx into the suspect-failed grace window,
// recording suspectSince and resetting verified_rejected_at to NULL so the
// reconciler's two-pass guard starts fresh for this suspect cycle.
//
// rejectReason is arcade's own explanation of the refusal (the 4xx body on the
// synchronous broadcast path). It is recorded here because this is the only
// moment it exists: arcade may drop the transaction's record entirely — a
// REJECTED event followed by a 404 on GET /tx is something we have actually
// observed — after which no amount of polling can recover the cause. An empty
// reason leaves any previously-recorded one intact rather than blanking it.
func (r KnownTxRepo) MarkSuspectFailed(ctx context.Context, txid string, suspectSince time.Time, rejectReason string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE known_txs SET status = ?, suspect_since = ?, verified_rejected_at = NULL, " +
		"reject_reason = COALESCE(?, reject_reason), updated_at = ? WHERE txid = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(KnownTxStatusSuspectFailed), r.s.encTime(suspectSince),
		rejectReasonArg(rejectReason), r.s.encTime(r.s.now()), raw)
	if err != nil {
		return fmt.Errorf("metastore: mark suspect failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: mark suspect failed: %s: %w", txid, ErrNotFound)
	}
	return nil
}

// StampVerifiedRejected records the FIRST authoritative re-verification that a
// suspect tx is still REJECTED (the reconciler's two-pass guard, pass 1). It
// only stamps while the row is still suspectFailed and has no stamp yet, so it
// is idempotent (a re-run does not move the timestamp) and never resurrects a
// stamp on a tx that has since recovered or been finalized. A no-op (already
// stamped, no longer suspect, or unknown txid) returns nil — the caller reads
// the current VerifiedRejectedAt to decide pass 1 vs pass 2.
func (r KnownTxRepo) StampVerifiedRejected(ctx context.Context, txid string, at time.Time) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind(
		`UPDATE known_txs SET verified_rejected_at = ?, updated_at = ?
		 WHERE txid = ? AND status = ? AND verified_rejected_at IS NULL`,
	)
	if _, err := r.s.execer(ctx).ExecContext(ctx, q,
		r.s.encTime(at), r.s.encTime(r.s.now()), raw, string(KnownTxStatusSuspectFailed)); err != nil {
		return fmt.Errorf("metastore: stamp verified rejected: %w", err)
	}
	return nil
}

// TransitionSuspect performs the positive-CAS terminal transition of a
// suspect-failed known tx to newStatus, applying ONLY while the row is currently
// suspectFailed. This is the reconciler's no-double-release gate: the first
// release wins and flips the row off suspectFailed; any re-entry (a crash re-run
// or a racing pass) sees a non-suspect status and is refused with
// [ErrStatusUpdateSkipped]. [ErrNotFound] means the txid vanished. It is used
// for both the provably-dead terminal transition (invalidTx / doubleSpend) and
// the stuck escalation.
func (r KnownTxRepo) TransitionSuspect(ctx context.Context, txid string, newStatus wdk.ProvenTxReqStatus) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE known_txs SET status = ?, updated_at = ? WHERE txid = ? AND status = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(newStatus), r.s.encTime(r.s.now()), raw, string(KnownTxStatusSuspectFailed))
	if err != nil {
		return fmt.Errorf("metastore: transition suspect: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	exists, err := r.existsByTxID(ctx, raw)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("metastore: transition suspect: %s: %w", txid, ErrNotFound)
	}
	return ErrStatusUpdateSkipped
}

// TransitionToAborted is the ABORT half of the one-row broadcast arbiter: it
// fences a known tx at [KnownTxStatusAborted], applying ONLY while the row is
// still in one of the three pre-broadcast statuses (unsent / unprocessed /
// nosend) AND its was_broadcast column is still false.
//
// Aborting a wallet transaction used never to touch known_txs at all, so an
// aborted tx stayed fully broadcastable — [KnownTxRepo.FindResendable] keys on
// known_txs alone, and would happily re-POST the raw bytes of a transaction the
// wallet had already released the inputs of. That is the double-spend the audit
// records as P0-3. Landing the abort on the SAME row the broadcaster claims,
// with the SAME CAS, makes "aborted but still broadcastable" unrepresentable:
// a claimed row refuses the abort, and an aborted row refuses the claim.
//
// Caller guidance — the two non-nil returns mean different things and must be
// handled differently:
//
//   - [ErrStatusUpdateSkipped] means NOT ABORTABLE: the row is claimed,
//     broadcast, or already terminal. The abort must FAIL and its transaction
//     roll back; the caller must NOT re-read the status and treat "already
//     aborted" as success. That read is not atomic with this CAS — between the
//     two statements the row can be claimed and the bytes sent, and the caller
//     would then release inputs for a live transaction, which is the very
//     double-spend this fence exists to prevent. Losing the arbiter is a
//     legitimate outcome: the send won, so the abort did not happen.
//   - [ErrNotFound] is the NORMAL unsigned case, not an error: a transaction
//     aborted before it was ever signed has no known_txs row at all, so there
//     are no bytes to fence. The caller proceeds with the rest of the abort.
//
// Like [TransitionSuspect], the positive CAS excludes its own target, so
// re-aborting an already-aborted row reports a skip rather than a second apply.
func (r KnownTxRepo) TransitionToAborted(ctx context.Context, txid string) error {
	return r.casFromPreBroadcast(ctx, txid, KnownTxStatusAborted, "transition to aborted")
}

// ClaimForSend is the SEND half of the same arbiter: it claims a known tx for a
// broadcast attempt by moving it to [wdk.ProvenTxStatusSending] under exactly
// the CAS [TransitionToAborted] uses — from {unsent, unprocessed, nosend} and
// only while was_broadcast is false. Abort and broadcast therefore contend on
// ONE row, and exactly one of them can win: a claimed row refuses
// TransitionToAborted, and an aborted row refuses ClaimForSend.
//
// 'sending' was previously written nowhere in production (it appeared only as a
// poll filter), so the claim gives it a precise meaning: a broadcast attempt is
// in flight for these bytes. A claimant that dies mid-POST leaves the row in
// 'sending', where [KnownTxRepo.FindResendable]'s graced arm picks it up once
// the grace has elapsed; the recovery path then takes the row with
// [KnownTxRepo.ReclaimStaleSend] rather than this method, because the row is
// already in the fenced state and only needs its clock re-stamped.
//
// Returns nil on a successful claim, [ErrStatusUpdateSkipped] when the row
// exists but is aborted / already claimed / past the broadcast stage, and
// [ErrNotFound] when the txid is unknown.
func (r KnownTxRepo) ClaimForSend(ctx context.Context, txid string) error {
	return r.casFromPreBroadcast(ctx, txid, wdk.ProvenTxStatusSending, "claim for send")
}

// ReclaimStaleSend re-claims a stranded broadcast attempt: a row still at
// [wdk.ProvenTxStatusSending] whose updated_at has aged past grace. It takes
// the same grace [KnownTxRepo.FindResendable] does and derives the cutoff from
// the SAME store clock, so the selector and the taker cannot disagree about
// what "stranded" means — pass both the one configured resend grace. The CAS is
// `status='sending' AND was_broadcast=false AND updated_at <= now-grace`, and
// it writes only the clock: the status stays 'sending', which is already the
// fenced state the original claimant put the row in.
//
// It is the recovery-path counterpart to [KnownTxRepo.ClaimForSend], and it
// buys two things. Exactly-one-winner: two sweep instances that both see the
// same stranded row race this CAS, and only one re-drives it. And a full grace
// of backoff: the re-stamp pushes updated_at forward, so the row drops out of
// FindResendable's graced 'sending' arm for another whole grace period instead
// of being re-POSTed on every tick while it stays stuck.
//
// The presumption of death is a PRESUMPTION: a hung claimant that later wakes
// can still write alongside the re-driver, because nothing revokes its right to
// proceed. That is tolerable only because the network side is
// idempotent-by-txid — both POST identical bytes — and because the status they
// contend over is the same 'sending'. A real fencing token (a claim epoch the
// stale claimant would fail to match) is deliberately out of scope here.
//
// [ErrStatusUpdateSkipped] means the row exists but was not stale-and-sending —
// it is fresh (another instance just re-drove it, or the original send is still
// live), or it has already moved on; [ErrNotFound] means the txid is unknown.
// The abort fence is unaffected: a 'sending' row still refuses
// [KnownTxRepo.TransitionToAborted] at any age, so re-claiming never opens a
// window in which the bytes could be both aborted and in flight.
func (r KnownTxRepo) ReclaimStaleSend(ctx context.Context, txid string, grace time.Duration) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	now := r.s.now()
	q := r.s.rebind("UPDATE known_txs SET updated_at = ? WHERE txid = ? AND status = ? " +
		"AND was_broadcast = ? AND updated_at <= ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		r.s.encTime(now), raw, string(wdk.ProvenTxStatusSending),
		r.s.boolVal(false), r.s.encTime(now.Add(-grace)))
	if err != nil {
		return fmt.Errorf("metastore: reclaim stale send: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	exists, err := r.existsByTxID(ctx, raw)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("metastore: reclaim stale send: %s: %w", txid, ErrNotFound)
	}
	return ErrStatusUpdateSkipped
}

// casFromPreBroadcast is the shared positive CAS behind the abort/claim
// arbiter: it moves txid to newStatus only from a pre-broadcast status with no
// broadcast evidence, following [TransitionSuspect]'s zero-rows disambiguation
// (row missing ⇒ ErrNotFound, row present ⇒ ErrStatusUpdateSkipped).
//
// newStatus must NOT be broadcast evidence: unlike [KnownTxRepo.UpdateStatus]
// this helper does not carry the sticky was_broadcast write, so a
// broadcast-evidence status routed through here would land with was_broadcast
// still false and stay eligible for the resend sweep. Both current targets
// ('aborted', 'sending') are correctly non-evidence.
func (r KnownTxRepo) casFromPreBroadcast(ctx context.Context, txid string, newStatus wdk.ProvenTxReqStatus, op string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	args := []any{string(newStatus), r.s.encTime(r.s.now()), raw}
	for _, st := range knownTxPreBroadcastStatuses {
		args = append(args, string(st))
	}
	args = append(args, r.s.boolVal(false))
	q := r.s.rebind("UPDATE known_txs SET status = ?, updated_at = ? WHERE txid = ? " +
		"AND status IN (" + inPlaceholders(len(knownTxPreBroadcastStatuses)) + ") " +
		"AND was_broadcast = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	exists, err := r.existsByTxID(ctx, raw)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("metastore: %s: %s: %w", op, txid, ErrNotFound)
	}
	return ErrStatusUpdateSkipped
}

// SetProof records a mined transaction's proof and marks it completed/notified.
// It also drops input_beef: once the merkle proof anchors the tx, its input
// ancestry is no longer needed to build a descendant's BEEF (raw_tx + this proof
// suffice), so clearing it here bounds the known_txs blob growth under sustained
// load. raw_tx is kept (needed to reconstruct the tx when spending its outputs).
func (r KnownTxRepo) SetProof(ctx context.Context, txid string, height uint32, blockHash, merklePath, merkleRoot []byte) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind(
		`UPDATE known_txs
		 SET status = ?, was_broadcast = ` + r.s.boolTrueLiteral() + `, notified = ` + r.s.boolTrueLiteral() + `,
		     block_height = ?, block_hash = ?, merkle_path = ?, merkle_root = ?, input_beef = NULL, updated_at = ?
		 WHERE txid = ?`,
	)
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(wdk.ProvenTxStatusCompleted), int64(height),
		bytesArg(blockHash), bytesArg(merklePath), bytesArg(merkleRoot),
		r.s.encTime(r.s.now()), raw)
	if err != nil {
		return fmt.Errorf("metastore: set proof: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: set proof: %s: %w", txid, ErrNotFound)
	}
	return nil
}

// FindByTxID returns the known tx for txid, or found=false when absent.
func (r KnownTxRepo) FindByTxID(ctx context.Context, txid string) (*KnownTx, bool, error) {
	raw, err := encTxID(txid)
	if err != nil {
		return nil, false, err
	}
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs WHERE txid = ?")
	kt, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, raw))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find known tx: %w", err)
	}
	return kt, true, nil
}

// FindByTxIDs bulk-loads the known txs for the given display-hex txids in ONE
// query, returning only the rows that exist (a missing txid is simply absent
// from the result — the async batch applier treats that as "not ours", the same
// as FindByTxID's found=false). It mirrors FindByTxID's scan; result order is
// unspecified, so the caller maps by txid.
func (r KnownTxRepo) FindByTxIDs(ctx context.Context, txids []string) ([]KnownTx, error) {
	if len(txids) == 0 {
		return nil, nil
	}
	ph := make([]string, 0, len(txids))
	args := make([]any, 0, len(txids))
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return nil, err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs WHERE txid IN (" + joinComma(ph) + ")")
	return r.queryList(ctx, q, args)
}

// FindSuspectFailed returns known txs that have sat in the suspect-failed grace
// window at least grace long (suspect_since <= now-grace), oldest first, up to
// limit. It is the arcade equivalent of the old failure-review scan.
func (r KnownTxRepo) FindSuspectFailed(ctx context.Context, grace time.Duration, limit int) ([]KnownTx, error) {
	cutoff := r.s.encTime(r.s.now().Add(-grace))
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + knownTxCols + " FROM known_txs " +
		"WHERE status = ? AND suspect_since IS NOT NULL AND suspect_since <= ? " +
		"ORDER BY suspect_since ASC" + clause)
	args := append([]any{string(KnownTxStatusSuspectFailed), cutoff}, pageArgs...)

	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find suspect failed: %w", err)
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

// GetRawTx returns the retained raw transaction bytes for txid, or found=false
// when the row is absent or carries no raw tx.
func (r KnownTxRepo) GetRawTx(ctx context.Context, txid string) ([]byte, bool, error) {
	raw, err := encTxID(txid)
	if err != nil {
		return nil, false, err
	}
	q := r.s.rebind("SELECT raw_tx FROM known_txs WHERE txid = ?")
	var rawTx []byte
	err = r.s.execer(ctx).QueryRowContext(ctx, q, raw).Scan(&rawTx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: get raw tx: %w", err)
	}
	if len(rawTx) == 0 {
		return nil, false, nil
	}
	return rawTx, true, nil
}

// FindStatuses returns the status of each requested txid that exists, keyed by
// display-hex txid.
func (r KnownTxRepo) FindStatuses(ctx context.Context, txids []string) (map[string]wdk.ProvenTxReqStatus, error) {
	out := make(map[string]wdk.ProvenTxReqStatus, len(txids))
	if len(txids) == 0 {
		return out, nil
	}
	ph := make([]string, 0, len(txids))
	args := make([]any, 0, len(txids))
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return nil, err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	q := r.s.rebind("SELECT txid, status FROM known_txs WHERE txid IN (" + joinComma(ph) + ")")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find statuses: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			txidBytes []byte
			status    string
		)
		if err := rows.Scan(&txidBytes, &status); err != nil {
			return nil, err
		}
		if txid := decTxIDPtr(txidBytes); txid != nil {
			out[*txid] = wdk.ProvenTxReqStatus(status)
		}
	}
	return out, rows.Err()
}

// IncreaseAttempts bumps the attempts counter for each of the given txids.
func (r KnownTxRepo) IncreaseAttempts(ctx context.Context, txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	ph := make([]string, 0, len(txids))
	args := []any{r.s.encTime(r.s.now())}
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	q := r.s.rebind("UPDATE known_txs SET attempts = attempts + 1, updated_at = ? WHERE txid IN (" + joinComma(ph) + ")")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: increase attempts: %w", err)
	}
	return nil
}

// SetBatch assigns a broadcast batch label to each of the given txids.
func (r KnownTxRepo) SetBatch(ctx context.Context, txids []string, batch string) error {
	if len(txids) == 0 {
		return nil
	}
	ph := make([]string, 0, len(txids))
	args := []any{batch, r.s.encTime(r.s.now())}
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	q := r.s.rebind("UPDATE known_txs SET batch = ?, updated_at = ? WHERE txid IN (" + joinComma(ph) + ")")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: set batch: %w", err)
	}
	return nil
}
