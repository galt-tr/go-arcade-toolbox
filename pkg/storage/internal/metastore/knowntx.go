package metastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
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
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// KnownTxRepo persists the known-transaction state.
type KnownTxRepo struct{ s *Store }

// KnownTx returns the known-transactions repository.
func (s *Store) KnownTx() KnownTxRepo { return KnownTxRepo{s} }

const knownTxCols = "txid, status, arcade_status, attempts, rebroadcast_attempts, " +
	"was_broadcast, notified, batch, notify, raw_tx, input_beef, block_height, " +
	"block_hash, merkle_path, merkle_root, competing_txs, suspect_since, created_at, updated_at, " +
	"verified_rejected_at"

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
	)
	dest := []any{
		&txidBytes, &status, &arcadeStatus, &attempts, &rebroadcast,
		r.s.boolDest(&wasBroadcast), r.s.boolDest(&notified), &batch, &kt.Notify,
		&kt.RawTx, &kt.InputBEEF, &blockHeight, &kt.BlockHash, &kt.MerklePath,
		&kt.MerkleRoot, &competing, r.s.tsDest(&suspectSince),
		r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt), r.s.tsDest(&verifiedRej),
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
		"updated_at = EXCLUDED.updated_at, verified_rejected_at = EXCLUDED.verified_rejected_at"
	var guardArgs []any
	if len(skipForStatuses) > 0 {
		conflict += " WHERE known_txs.status NOT IN (" + inPlaceholders(len(skipForStatuses)) + ")"
		for _, st := range skipForStatuses {
			guardArgs = append(guardArgs, string(st))
		}
	}

	q := r.s.rebind(
		`INSERT INTO known_txs (` + knownTxCols + `)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ` + conflict,
	)
	args := []any{
		//nolint:gosec // attempt counters are small, well within int64.
		raw, string(kt.Status), strPtrArg(kt.ArcadeStatus), int64(kt.Attempts), int64(kt.RebroadcastAttempts),
		r.s.boolVal(wasBroadcast), r.s.boolVal(kt.Notified), strPtrArg(kt.Batch), notify,
		bytesArg(kt.RawTx), bytesArg(kt.InputBEEF), blockHeight, bytesArg(kt.BlockHash),
		bytesArg(kt.MerklePath), bytesArg(kt.MerkleRoot), competing, r.s.encTimePtr(kt.SuspectSince),
		now, now, r.s.encTimePtr(kt.VerifiedRejectedAt),
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
func (r KnownTxRepo) MarkSuspectFailed(ctx context.Context, txid string, suspectSince time.Time) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE known_txs SET status = ?, suspect_since = ?, verified_rejected_at = NULL, updated_at = ? WHERE txid = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(KnownTxStatusSuspectFailed), r.s.encTime(suspectSince), r.s.encTime(r.s.now()), raw)
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

// SetProof records a mined transaction's proof and marks it completed/notified.
func (r KnownTxRepo) SetProof(ctx context.Context, txid string, height uint32, blockHash, merklePath, merkleRoot []byte) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind(
		`UPDATE known_txs
		 SET status = ?, was_broadcast = ` + r.s.boolTrueLiteral() + `, notified = ` + r.s.boolTrueLiteral() + `,
		     block_height = ?, block_hash = ?, merkle_path = ?, merkle_root = ?, updated_at = ?
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
