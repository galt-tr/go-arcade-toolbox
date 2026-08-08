package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// TransactionsRepo persists wallet transactions (BRC-100 "actions") and their
// label associations.
type TransactionsRepo struct{ s *Store }

// Transactions returns the transactions repository.
func (s *Store) Transactions() TransactionsRepo { return TransactionsRepo{s} }

// CountByStatus returns the number of the user's transactions in each status
// (the wallet-side lifecycle: unsigned/unprocessed/sending/nosend/unproven/
// completed/failed/…). Absent statuses are omitted. For observability.
func (r TransactionsRepo) CountByStatus(ctx context.Context, userID int) (map[string]int, error) {
	q := r.s.rebind("SELECT status, COUNT(*) FROM transactions WHERE user_id = ? GROUP BY status")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("metastore: count transactions by status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int)
	for rows.Next() {
		var (
			status string
			n      int
		)
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("metastore: scan transaction status count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

const txCols = "transaction_id, user_id, proven_tx_id, status, reference, txid, " +
	"is_outgoing, satoshis, description, version, lock_time, input_beef, created_at, updated_at"

// NewTx is the input to [TransactionsRepo.Insert]: a fresh transaction plus the
// labels to attach. Insert fills the generated transaction id.
type NewTx struct {
	UserID      int
	Status      wdk.TxStatus
	Reference   string
	IsOutgoing  bool
	Satoshis    int64
	Description string
	Version     *uint32
	LockTime    *uint32
	InputBEEF   []byte
	Labels      []string
}

// ListActionsFilter selects a page of actions. It carries the concrete filter
// set the repository evaluates; mapping BRC-100 args (e.g. the default status
// set, or the "unfail" label special-case) onto it is the provider's job.
type ListActionsFilter struct {
	UserID         int
	Statuses       []wdk.TxStatus
	Labels         []string
	LabelQueryMode *defs.QueryMode
	Limit          int
	Offset         int
}

// ListTransactionsFilter selects a page of transactions, additionally
// filterable by explicit txids.
type ListTransactionsFilter struct {
	UserID         int
	Statuses       []wdk.TxStatus
	TxIDs          []string
	Labels         []string
	LabelQueryMode *defs.QueryMode
	Limit          int
	Offset         int
}

func (r TransactionsRepo) scan(sc rowScanner) (*wdk.TableTransaction, error) {
	var (
		t          wdk.TableTransaction
		txID       int64
		userID     int64
		provenTxID sql.NullInt64
		status     string
		reference  string
		txidBytes  []byte
		isOutgoing boolScan
		version    sql.NullInt64
		lockTime   sql.NullInt64
		inputBEEF  []byte
		createdAt  tsScan
		updatedAt  tsScan
	)
	dest := []any{
		&txID, &userID, &provenTxID, &status, &reference, &txidBytes,
		r.s.boolDest(&isOutgoing), &t.Satoshis, &t.Description, &version, &lockTime,
		&inputBEEF, r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt),
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	t.TransactionID = uint(txID) //nolint:gosec // DB identity id, always positive.
	t.UserID = int(userID)
	if provenTxID.Valid {
		v := int(provenTxID.Int64)
		t.ProvenTxID = &v
	}
	t.Status = wdk.TxStatus(status)
	t.Reference = primitives.Base64String(reference)
	t.IsOutgoing = r.s.boolGet(isOutgoing)
	t.TxID = decTxIDPtr(txidBytes)
	if version.Valid {
		v := uint32(version.Int64) //nolint:gosec // tx version fits uint32.
		t.Version = &v
	}
	if lockTime.Valid {
		v := uint32(lockTime.Int64) //nolint:gosec // lock_time fits uint32.
		t.LockTime = &v
	}
	t.InputBEEF = inputBEEF
	t.CreatedAt = r.s.tsTime(createdAt)
	t.UpdatedAt = r.s.tsTime(updatedAt)
	return &t, nil
}

// Insert creates a transaction (typically in TxStatusUnsigned with a fresh
// reference token) and attaches its labels, returning the generated id. It runs
// in a single transaction so the row and its label edges commit together.
func (r TransactionsRepo) Insert(ctx context.Context, tx NewTx) (uint, error) {
	var id uint
	err := r.s.inTx(ctx, func(ctx context.Context) error {
		now := r.s.encTime(r.s.now())
		var version, lockTime any
		if tx.Version != nil {
			version = int64(*tx.Version)
		}
		if tx.LockTime != nil {
			lockTime = int64(*tx.LockTime)
		}
		var inputBEEF any
		if len(tx.InputBEEF) > 0 {
			inputBEEF = tx.InputBEEF
		}

		q := r.s.rebind(
			`INSERT INTO transactions
			 (user_id, status, reference, is_outgoing, satoshis, description, version, lock_time, input_beef, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 RETURNING transaction_id`)
		var got int64
		if err := r.s.execer(ctx).QueryRowContext(ctx, q,
			tx.UserID, string(tx.Status), tx.Reference, r.s.boolVal(tx.IsOutgoing),
			tx.Satoshis, tx.Description, version, lockTime, inputBEEF, now, now,
		).Scan(&got); err != nil {
			return fmt.Errorf("metastore: insert transaction: %w", err)
		}
		id = uint(got) //nolint:gosec // DB identity id, always positive.

		for _, name := range tx.Labels {
			if err := r.s.Labels().attach(ctx, tx.UserID, id, name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// FindByID returns the transaction for id, or found=false when absent.
func (r TransactionsRepo) FindByID(ctx context.Context, id uint) (*wdk.TableTransaction, bool, error) {
	q := r.s.rebind("SELECT " + txCols + " FROM transactions WHERE transaction_id = ?")
	t, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find transaction: %w", err)
	}
	return t, true, nil
}

// FindByReference returns the transaction with reference for userID, or
// found=false when absent. The reference doubles as the UTXO reservation token.
func (r TransactionsRepo) FindByReference(ctx context.Context, userID int, reference string) (*wdk.TableTransaction, bool, error) {
	q := r.s.rebind("SELECT " + txCols + " FROM transactions WHERE user_id = ? AND reference = ?")
	t, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, userID, reference))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find transaction by reference: %w", err)
	}
	return t, true, nil
}

// FindByTxID returns the transactions for userID carrying txid (display hex).
// A txid is not unique — the same on-chain transaction can back several rows —
// so this returns every match, newest first.
func (r TransactionsRepo) FindByTxID(ctx context.Context, userID int, txid string) ([]wdk.TableTransaction, error) {
	raw, err := encTxID(txid)
	if err != nil {
		return nil, err
	}
	q := r.s.rebind("SELECT " + txCols + " FROM transactions WHERE user_id = ? AND txid = ? ORDER BY transaction_id DESC")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, userID, raw)
	if err != nil {
		return nil, fmt.Errorf("metastore: find transaction by txid: %w", err)
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

// SetTxID records the on-chain txid (display hex) on a transaction row.
func (r TransactionsRepo) SetTxID(ctx context.Context, transactionID uint, txid string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE transactions SET txid = ?, updated_at = ? WHERE transaction_id = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q, raw, r.s.encTime(r.s.now()), transactionID)
	if err != nil {
		return fmt.Errorf("metastore: set txid: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: set txid: transaction %d: %w", transactionID, ErrNotFound)
	}
	return nil
}

// UpdateStatus transitions a transaction's status. When expectedCurrent is
// non-empty it is a compare-and-set precondition (status IN expectedCurrent);
// a zero-row result then yields [ErrStatusUpdateSkipped] rather than a silent
// no-op.
func (r TransactionsRepo) UpdateStatus(ctx context.Context, transactionID uint, status wdk.TxStatus, expectedCurrent ...wdk.TxStatus) error {
	conds := []string{"transaction_id = ?"}
	args := []any{string(status), r.s.encTime(r.s.now()), transactionID}
	if len(expectedCurrent) > 0 {
		conds = append(conds, "status IN ("+inPlaceholders(len(expectedCurrent))+")")
		for _, st := range expectedCurrent {
			args = append(args, string(st))
		}
	}
	q := r.s.rebind("UPDATE transactions SET status = ?, updated_at = ? WHERE " + joinAnd(conds))
	return r.applyStatusUpdate(ctx, q, args, len(expectedCurrent) > 0)
}

// UpdateStatusByTxID transitions every transaction row carrying txid (display
// hex), with the same optional CAS precondition as UpdateStatus.
func (r TransactionsRepo) UpdateStatusByTxID(ctx context.Context, txid string, status wdk.TxStatus, expectedCurrent ...wdk.TxStatus) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	conds := []string{"txid = ?"}
	args := []any{string(status), r.s.encTime(r.s.now()), raw}
	if len(expectedCurrent) > 0 {
		conds = append(conds, "status IN ("+inPlaceholders(len(expectedCurrent))+")")
		for _, st := range expectedCurrent {
			args = append(args, string(st))
		}
	}
	q := r.s.rebind("UPDATE transactions SET status = ?, updated_at = ? WHERE " + joinAnd(conds))
	return r.applyStatusUpdate(ctx, q, args, len(expectedCurrent) > 0)
}

// ClearInputBEEFByTxID drops the stored input BEEF for every row carrying txid
// (display hex). Called once a tx is mined+proven: the merkle proof anchors it,
// so the input ancestry is no longer needed to build a descendant's BEEF, and
// clearing it bounds the transactions blob growth under sustained load. The
// `input_beef IS NOT NULL` guard makes a re-apply a cheap no-op.
func (r TransactionsRepo) ClearInputBEEFByTxID(ctx context.Context, txid string) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	q := r.s.rebind("UPDATE transactions SET input_beef = NULL, updated_at = ? WHERE txid = ? AND input_beef IS NOT NULL")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, r.s.encTime(r.s.now()), raw); err != nil {
		return fmt.Errorf("metastore: clear input beef: %w", err)
	}
	return nil
}

// BulkUpdateStatusByTxIDs transitions every transaction row whose txid is in
// txids to status. When expectedCurrent is non-empty it is a compare-and-set
// precondition (status IN expectedCurrent) exactly like
// [TransactionsRepo.UpdateStatusByTxID] — but as ONE set-based UPDATE with no
// ErrStatusUpdateSkipped bookkeeping: rows that fail the precondition are simply
// not updated, which is how the async batch applier tolerates the skip.
func (r TransactionsRepo) BulkUpdateStatusByTxIDs(ctx context.Context, txids []string, status wdk.TxStatus, expectedCurrent ...wdk.TxStatus) error {
	if len(txids) == 0 {
		return nil
	}
	args := []any{string(status), r.s.encTime(r.s.now())}
	ph := make([]string, 0, len(txids))
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	conds := []string{"txid IN (" + joinComma(ph) + ")"}
	if len(expectedCurrent) > 0 {
		conds = append(conds, "status IN ("+inPlaceholders(len(expectedCurrent))+")")
		for _, st := range expectedCurrent {
			args = append(args, string(st))
		}
	}
	q := r.s.rebind("UPDATE transactions SET status = ?, updated_at = ? WHERE " + joinAnd(conds))
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: bulk update transaction status: %w", err)
	}
	return nil
}

// BulkClearInputBEEFByTxIDs drops the stored input BEEF for every transaction
// row whose txid is in txids (the `input_beef IS NOT NULL` guard makes it a
// cheap no-op for rows already cleared). It is the set-based form of
// [TransactionsRepo.ClearInputBEEFByTxID] used by the mined batch writer.
func (r TransactionsRepo) BulkClearInputBEEFByTxIDs(ctx context.Context, txids []string) error {
	if len(txids) == 0 {
		return nil
	}
	args := []any{r.s.encTime(r.s.now())}
	ph := make([]string, 0, len(txids))
	for _, t := range txids {
		raw, err := encTxID(t)
		if err != nil {
			return err
		}
		ph = append(ph, "?")
		args = append(args, raw)
	}
	q := r.s.rebind("UPDATE transactions SET input_beef = NULL, updated_at = ? WHERE txid IN (" +
		joinComma(ph) + ") AND input_beef IS NOT NULL")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: bulk clear input beef: %w", err)
	}
	return nil
}

func (r TransactionsRepo) applyStatusUpdate(ctx context.Context, q string, args []any, guarded bool) error {
	res, err := r.s.execer(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: update transaction status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 && guarded {
		return ErrStatusUpdateSkipped
	}
	return nil
}

// FindAbandoned returns transactions eligible for the abort sweep: those in a
// build-but-never-broadcast status (unsigned/nosend) older than grace, oldest
// first, up to limit. It rides the idx_transactions_abandoned partial index.
func (r TransactionsRepo) FindAbandoned(ctx context.Context, grace time.Duration, limit int) ([]wdk.TableTransaction, error) {
	cutoff := r.s.encTime(r.s.now().Add(-grace))
	clause, pageArgs := r.s.limitOffsetClause(limit, 0)
	q := r.s.rebind("SELECT " + txCols + " FROM transactions " +
		"WHERE status IN ('unsigned', 'nosend') AND created_at <= ? " +
		"ORDER BY created_at ASC, transaction_id ASC" + clause)
	args := append([]any{cutoff}, pageArgs...)

	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find abandoned: %w", err)
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

// labelFilterSubquery builds the "transaction_id IN (...)" label filter and its
// args, or ("", nil) when no labels are requested. ANY (default) matches rows
// carrying any of the labels; ALL requires every label (GROUP BY ... HAVING
// COUNT(DISTINCT name) = N). Labels are resolved through the join to the labels
// table, scoped to the user, ignoring soft-deleted edges.
func (r TransactionsRepo) labelFilterSubquery(userID int, labels []string, mode *defs.QueryMode) (string, []any) {
	if len(labels) == 0 {
		return "", nil
	}
	args := []any{userID}
	args = append(args, anyArgs(labels)...)
	args = append(args, r.s.boolVal(false), r.s.boolVal(false))
	sub := `transaction_id IN (
		SELECT tl.transaction_id
		FROM transaction_labels tl
		JOIN labels l ON l.label_id = tl.label_id
		WHERE l.user_id = ? AND l.name IN (` + inPlaceholders(len(labels)) + `)
		  AND l.is_deleted = ? AND tl.is_deleted = ?`
	if isAllMode(mode) {
		sub += " GROUP BY tl.transaction_id HAVING COUNT(DISTINCT l.name) = ?"
		args = append(args, len(labels))
	}
	sub += ")"
	return sub, args
}

// ListActions returns a page of actions matching the filter plus the total
// number matching (ignoring pagination). Rows are ordered by transaction_id
// ASC, matching the old repo's stable ordering.
func (r TransactionsRepo) ListActions(ctx context.Context, filter ListActionsFilter) ([]wdk.TableTransaction, int64, error) {
	where, whereArgs := r.buildTxWhere(filter.UserID, filter.Statuses, nil, filter.Labels, filter.LabelQueryMode)
	return r.listAndCount(ctx, where, whereArgs, filter.Limit, filter.Offset)
}

// ListTransactions returns a page of transactions matching the filter (status,
// txids and labels) plus the total matching, ordered by transaction_id ASC.
func (r TransactionsRepo) ListTransactions(ctx context.Context, filter ListTransactionsFilter) ([]wdk.TableTransaction, int64, error) {
	where, whereArgs := r.buildTxWhere(filter.UserID, filter.Statuses, filter.TxIDs, filter.Labels, filter.LabelQueryMode)
	return r.listAndCount(ctx, where, whereArgs, filter.Limit, filter.Offset)
}

// buildTxWhere assembles the shared WHERE clause and args for the action /
// transaction list queries.
func (r TransactionsRepo) buildTxWhere(userID int, statuses []wdk.TxStatus, txids, labels []string, mode *defs.QueryMode) (string, []any) {
	conds := []string{"user_id = ?"}
	args := []any{userID}

	if len(statuses) > 0 {
		conds = append(conds, "status IN ("+inPlaceholders(len(statuses))+")")
		for _, st := range statuses {
			args = append(args, string(st))
		}
	}
	if len(txids) > 0 {
		ph := make([]string, 0, len(txids))
		for _, t := range txids {
			raw, err := encTxID(t)
			if err != nil {
				continue // skip malformed txids rather than fail the whole page
			}
			ph = append(ph, "?")
			args = append(args, raw)
		}
		if len(ph) > 0 {
			conds = append(conds, "txid IN ("+joinComma(ph)+")")
		}
	}
	if sub, subArgs := r.labelFilterSubquery(userID, labels, mode); sub != "" {
		conds = append(conds, sub)
		args = append(args, subArgs...)
	}
	return joinAnd(conds), args
}

func (r TransactionsRepo) listAndCount(ctx context.Context, where string, whereArgs []any, limit, offset int) ([]wdk.TableTransaction, int64, error) {
	var total int64
	countQ := r.s.rebind("SELECT COUNT(*) FROM transactions WHERE " + where)
	if err := r.s.execer(ctx).QueryRowContext(ctx, countQ, whereArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("metastore: count actions: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	clause, pageArgs := r.s.limitOffsetClause(limit, offset)
	selQ := r.s.rebind("SELECT " + txCols + " FROM transactions WHERE " + where +
		" ORDER BY transaction_id ASC" + clause)
	args := append(append([]any{}, whereArgs...), pageArgs...)

	rows, err := r.s.execer(ctx).QueryContext(ctx, selQ, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("metastore: list actions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []wdk.TableTransaction
	for rows.Next() {
		t, err := r.scan(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, *t)
	}
	return out, total, rows.Err()
}

// GetLabelsForTransactions returns the (non-deleted) label names attached to
// each of the given transaction ids, keyed by transaction id.
func (r TransactionsRepo) GetLabelsForTransactions(ctx context.Context, txIDs []uint) (map[uint][]string, error) {
	out := make(map[uint][]string, len(txIDs))
	if len(txIDs) == 0 {
		return out, nil
	}
	ph := make([]string, len(txIDs))
	args := make([]any, 0, len(txIDs)+2)
	for i, id := range txIDs {
		ph[i] = "?"
		args = append(args, id)
	}
	args = append(args, r.s.boolVal(false), r.s.boolVal(false))
	q := r.s.rebind(
		`SELECT tl.transaction_id, l.name
		 FROM transaction_labels tl
		 JOIN labels l ON l.label_id = tl.label_id
		 WHERE tl.transaction_id IN (` + joinComma(ph) + `)
		   AND tl.is_deleted = ? AND l.is_deleted = ?
		 ORDER BY l.name ASC`)
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("metastore: labels for transactions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			txID int64
			name string
		)
		if err := rows.Scan(&txID, &name); err != nil {
			return nil, err
		}
		out[uint(txID)] = append(out[uint(txID)], name) //nolint:gosec // DB identity id, always positive.
	}
	return out, rows.Err()
}
