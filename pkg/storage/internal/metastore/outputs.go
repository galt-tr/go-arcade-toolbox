package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// OutputsRepo persists descriptive output rows and their spend history.
//
// Spendability is NOT stored here (see the package doc): there is no spendable
// column. ListOutputs / FindOutputs apply only the descriptive filters this
// store owns and return each row WITH its outpoint; the provider intersects
// those with the utxostore's live inventory to decide spendability and balance.
type OutputsRepo struct{ s *Store }

// Outputs returns the outputs repository.
func (s *Store) Outputs() OutputsRepo { return OutputsRepo{s} }

// DefaultListOutputsStatuses is the parent-transaction status set the old store
// implicitly required for an output to be listable (it excludes failed and
// aborted transactions). The provider passes this into ListOutputsFilter unless
// a caller narrows it.
var DefaultListOutputsStatuses = []wdk.TxStatus{
	wdk.TxStatusCompleted,
	wdk.TxStatusUnprocessed,
	wdk.TxStatusSending,
	wdk.TxStatusUnproven,
	wdk.TxStatusUnsigned,
	wdk.TxStatusNoSend,
	wdk.TxStatusNonFinal,
}

// NewOutput is the input to [OutputsRepo.Insert].
type NewOutput struct {
	UserID             int
	TransactionID      uint
	Vout               uint32
	Satoshis           int64
	LockingScript      []byte
	Basket             *string // basket NAME; nil = not in a basket
	Change             bool
	Type               string
	ProvidedBy         string
	Purpose            string
	Description        string
	DerivationPrefix   *string
	DerivationSuffix   *string
	SenderIdentityKey  *string
	CustomInstructions *string
	Tags               []string
}

// OutputRow is a descriptive output plus the pieces the provider needs to
// compose the BRC-100 view: the parent transaction's txid (already set on
// TableOutput.TxID), the basket NAME (which TableOutput has no field for — its
// BasketID is a legacy numeric artifact left nil here), and the attached tags.
// TableOutput.Spendable is always false: spendability is external.
type OutputRow struct {
	wdk.TableOutput

	Basket *string
	Tags   []string
}

// ListOutputsFilter selects a page of outputs. Statuses restricts the parent
// transaction status (pass DefaultListOutputsStatuses for the old default).
type ListOutputsFilter struct {
	UserID       int
	Basket       string // "" = all baskets
	Tags         []string
	TagQueryMode *defs.QueryMode
	Statuses     []wdk.TxStatus
	Limit        int
	Offset       int
	IncludeTags  bool
}

const outputCols = "o.output_id, o.user_id, o.transaction_id, o.vout, o.satoshis, " +
	"o.locking_script, o.basket, o.spent_by, o.change, o.output_type, o.provided_by, " +
	"o.purpose, o.description, o.derivation_prefix, o.derivation_suffix, " +
	"o.sender_identity_key, o.custom_instructions, o.created_at, o.updated_at, t.txid"

func (r OutputsRepo) scan(sc rowScanner) (*OutputRow, error) {
	var (
		row       OutputRow
		outputID  int64
		userID    int64
		txnID     int64
		locking   []byte
		basket    sql.NullString
		spentBy   sql.NullInt64
		change    boolScan
		derivPfx  sql.NullString
		derivSfx  sql.NullString
		sender    sql.NullString
		custom    sql.NullString
		createdAt tsScan
		updatedAt tsScan
		parentTx  []byte
	)
	dest := []any{
		&outputID, &userID, &txnID, &row.Vout, &row.Satoshis,
		&locking, &basket, &spentBy, r.s.boolDest(&change), &row.Type, &row.ProvidedBy,
		&row.Purpose, &row.OutputDescription, &derivPfx, &derivSfx,
		&sender, &custom, r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt), &parentTx,
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	row.OutputID = uint(outputID) //nolint:gosec // DB identity id, always positive.
	row.UserID = int(userID)
	row.TransactionID = uint(txnID) //nolint:gosec // DB identity id, always positive.
	row.LockingScript = locking
	row.Basket = nullStr(basket)
	if spentBy.Valid {
		v := uint(spentBy.Int64) //nolint:gosec // DB identity id, always positive.
		row.SpentBy = &v
	}
	row.Change = r.s.boolGet(change)
	row.DerivationPrefix = nullStr(derivPfx)
	row.DerivationSuffix = nullStr(derivSfx)
	row.SenderIdentityKey = nullStr(sender)
	row.CustomInstructions = nullStr(custom)
	row.CreatedAt = r.s.tsTime(createdAt)
	row.UpdatedAt = r.s.tsTime(updatedAt)
	row.TxID = decTxIDPtr(parentTx)
	return &row, nil
}

// Insert creates one descriptive output row and attaches its tags, returning
// the generated id. Runs in one transaction.
func (r OutputsRepo) Insert(ctx context.Context, o NewOutput) (uint, error) {
	var id uint
	err := r.s.inTx(ctx, func(ctx context.Context) error {
		now := r.s.encTime(r.s.now())
		var locking any
		if len(o.LockingScript) > 0 {
			locking = o.LockingScript
		}
		q := r.s.rebind(
			`INSERT INTO outputs
			 (user_id, transaction_id, vout, satoshis, locking_script, basket, change,
			  output_type, provided_by, purpose, description,
			  derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions,
			  created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 RETURNING output_id`)
		var got int64
		if err := r.s.execer(ctx).QueryRowContext(ctx, q,
			o.UserID, o.TransactionID, o.Vout, o.Satoshis, locking, strPtrArg(o.Basket),
			r.s.boolVal(o.Change), o.Type, o.ProvidedBy, o.Purpose, o.Description,
			strPtrArg(o.DerivationPrefix), strPtrArg(o.DerivationSuffix),
			strPtrArg(o.SenderIdentityKey), strPtrArg(o.CustomInstructions), now, now,
		).Scan(&got); err != nil {
			return fmt.Errorf("metastore: insert output: %w", err)
		}
		id = uint(got) //nolint:gosec // DB identity id, always positive.

		for _, tag := range o.Tags {
			if err := r.s.Tags().attach(ctx, o.UserID, id, tag); err != nil {
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

// tagFilterSubquery builds the "o.output_id IN (...)" tag filter, mirroring the
// label filter: ANY = IN, ALL = GROUP BY ... HAVING COUNT(DISTINCT name) = N.
func (r OutputsRepo) tagFilterSubquery(userID int, tags []string, mode *defs.QueryMode) (string, []any) {
	if len(tags) == 0 {
		return "", nil
	}
	args := []any{userID}
	args = append(args, anyArgs(tags)...)
	args = append(args, r.s.boolVal(false), r.s.boolVal(false))
	sub := `o.output_id IN (
		SELECT ot.output_id
		FROM output_tags ot
		JOIN tags tg ON tg.tag_id = ot.tag_id
		WHERE tg.user_id = ? AND tg.name IN (` + inPlaceholders(len(tags)) + `)
		  AND tg.is_deleted = ? AND ot.is_deleted = ?`
	if isAllMode(mode) {
		sub += " GROUP BY ot.output_id HAVING COUNT(DISTINCT tg.name) = ?"
		args = append(args, len(tags))
	}
	sub += ")"
	return sub, args
}

// ListOutputs returns a page of descriptive output rows matching the filter
// plus the total matching (ignoring pagination). Rows are ordered by output_id
// ASC. Spendability is NOT applied — the provider composes it from the
// utxostore. When IncludeTags is set the returned rows carry their tag names.
func (r OutputsRepo) ListOutputs(ctx context.Context, filter ListOutputsFilter) ([]OutputRow, int64, error) {
	conds := []string{"o.user_id = ?"}
	args := []any{filter.UserID}

	if filter.Basket != "" {
		conds = append(conds, "o.basket = ?")
		args = append(args, filter.Basket)
	}
	if len(filter.Statuses) > 0 {
		conds = append(conds, "t.status IN ("+inPlaceholders(len(filter.Statuses))+")")
		for _, st := range filter.Statuses {
			args = append(args, string(st))
		}
	}
	if sub, subArgs := r.tagFilterSubquery(filter.UserID, filter.Tags, filter.TagQueryMode); sub != "" {
		conds = append(conds, sub)
		args = append(args, subArgs...)
	}
	where := joinAnd(conds)

	var total int64
	countQ := r.s.rebind("SELECT COUNT(*) FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id WHERE " + where)
	if err := r.s.execer(ctx).QueryRowContext(ctx, countQ, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("metastore: count outputs: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	clause, pageArgs := r.s.limitOffsetClause(filter.Limit, filter.Offset)
	selQ := r.s.rebind("SELECT " + outputCols +
		" FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id WHERE " + where +
		" ORDER BY o.output_id ASC" + clause)
	selArgs := append(append([]any{}, args...), pageArgs...)

	rows, err := r.s.execer(ctx).QueryContext(ctx, selQ, selArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("metastore: list outputs: %w", err)
	}
	out, err := r.collect(rows)
	if err != nil {
		return nil, 0, err
	}
	if filter.IncludeTags {
		if err := r.attachTags(ctx, out); err != nil {
			return nil, 0, err
		}
	}
	return out, total, nil
}

// FindOutputs returns the outputs matching args, ordered by output_id ASC. It
// honors every filter it can evaluate locally and DELIBERATELY IGNORES
// args.Spendable — spendability is external (see the package doc). args.TxID
// filters on the parent transaction's txid.
func (r OutputsRepo) FindOutputs(ctx context.Context, args wdk.FindOutputsArgs) ([]OutputRow, error) {
	conds := []string{}
	vals := []any{}
	if args.UserID != nil {
		conds = append(conds, "o.user_id = ?")
		vals = append(vals, *args.UserID)
	}
	if args.OutputID != nil {
		conds = append(conds, "o.output_id = ?")
		vals = append(vals, *args.OutputID)
	}
	if args.TransactionID != nil {
		conds = append(conds, "o.transaction_id = ?")
		vals = append(vals, *args.TransactionID)
	}
	if args.Satoshis != nil {
		conds = append(conds, "o.satoshis = ?")
		vals = append(vals, *args.Satoshis)
	}
	if args.Vout != nil {
		conds = append(conds, "o.vout = ?")
		vals = append(vals, *args.Vout)
	}
	if args.Change != nil {
		conds = append(conds, "o.change = ?")
		vals = append(vals, r.s.boolVal(*args.Change))
	}
	if args.TxID != nil {
		raw, err := encTxID(*args.TxID)
		if err != nil {
			return nil, err
		}
		conds = append(conds, "t.txid = ?")
		vals = append(vals, raw)
	}
	if len(args.TxStatus) > 0 {
		conds = append(conds, "t.status IN ("+inPlaceholders(len(args.TxStatus))+")")
		for _, st := range args.TxStatus {
			vals = append(vals, string(st))
		}
	}

	where := "1 = 1"
	if len(conds) > 0 {
		where = joinAnd(conds)
	}
	q := r.s.rebind("SELECT " + outputCols +
		" FROM outputs o JOIN transactions t ON t.transaction_id = o.transaction_id WHERE " + where +
		" ORDER BY o.output_id ASC")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, vals...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find outputs: %w", err)
	}
	return r.collect(rows)
}

func (r OutputsRepo) collect(rows *sql.Rows) ([]OutputRow, error) {
	defer func() { _ = rows.Close() }()
	var out []OutputRow
	for rows.Next() {
		row, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *row)
	}
	return out, rows.Err()
}

// attachTags fills each row's Tags from output_tags/tags for the collected
// output ids.
func (r OutputsRepo) attachTags(ctx context.Context, rows []OutputRow) error {
	if len(rows) == 0 {
		return nil
	}
	ph := make([]string, len(rows))
	args := make([]any, 0, len(rows)+2)
	idx := make(map[uint]int, len(rows))
	for i := range rows {
		ph[i] = "?"
		args = append(args, rows[i].OutputID)
		idx[rows[i].OutputID] = i
	}
	args = append(args, r.s.boolVal(false), r.s.boolVal(false))
	q := r.s.rebind(
		`SELECT ot.output_id, tg.name
		 FROM output_tags ot
		 JOIN tags tg ON tg.tag_id = ot.tag_id
		 WHERE ot.output_id IN (` + joinComma(ph) + `)
		   AND ot.is_deleted = ? AND tg.is_deleted = ?
		 ORDER BY tg.name ASC`)
	res, err := r.s.execer(ctx).QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: tags for outputs: %w", err)
	}
	defer func() { _ = res.Close() }()
	for res.Next() {
		var (
			outputID int64
			name     string
		)
		if err := res.Scan(&outputID, &name); err != nil {
			return err
		}
		if i, ok := idx[uint(outputID)]; ok { //nolint:gosec // DB identity id, always positive.
			rows[i].Tags = append(rows[i].Tags, name)
		}
	}
	return res.Err()
}

// RelinquishOutput removes an output from its basket (sets basket = NULL) for
// the output identified by (txid, vout) under userID. When basket is non-empty
// the output must currently be in that basket. It returns [ErrNotFound] when no
// output with that outpoint exists for the user, and [ErrOutputBasketMismatch]
// when the outpoint exists but is not in the requested basket. Removing the
// corresponding live utxostore inventory row (the spendability side) is the
// provider's responsibility.
func (r OutputsRepo) RelinquishOutput(ctx context.Context, userID int, basket, txid string, vout uint32) error {
	raw, err := encTxID(txid)
	if err != nil {
		return err
	}
	// The outpoint predicate (user + parent-tx txid + vout), shared by the
	// UPDATE and the disambiguating existence check.
	outpointCond := "user_id = ? AND vout = ? AND " +
		"transaction_id IN (SELECT transaction_id FROM transactions WHERE user_id = ? AND txid = ?)"
	outpointArgs := []any{userID, vout, userID, raw}

	conds := outpointCond
	args := append([]any{r.s.encTime(r.s.now())}, outpointArgs...)
	if basket != "" {
		conds += " AND basket = ?"
		args = append(args, basket)
	}
	q := r.s.rebind("UPDATE outputs SET basket = NULL, updated_at = ? WHERE " + conds)
	res, err := r.s.execer(ctx).ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("metastore: relinquish output: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	// Zero rows: distinguish "no such outpoint" from "wrong basket".
	existsQ := r.s.rebind("SELECT 1 FROM outputs WHERE " + outpointCond)
	var one int
	switch err := r.s.execer(ctx).QueryRowContext(ctx, existsQ, outpointArgs...).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("metastore: relinquish output: %s:%d: %w", txid, vout, ErrNotFound)
	case err != nil:
		return fmt.Errorf("metastore: relinquish output existence: %w", err)
	default:
		return fmt.Errorf("metastore: relinquish output: %s:%d not in basket %q: %w", txid, vout, basket, ErrOutputBasketMismatch)
	}
}

// MarkSpent records that spendingTransactionID consumed the given outputs
// (sets outputs.spent_by). History only; it does not touch spendability.
func (r OutputsRepo) MarkSpent(ctx context.Context, spendingTransactionID uint, outputIDs []uint) error {
	if len(outputIDs) == 0 {
		return nil
	}
	ph := make([]string, len(outputIDs))
	args := []any{spendingTransactionID, r.s.encTime(r.s.now())}
	for i, id := range outputIDs {
		ph[i] = "?"
		args = append(args, id)
	}
	q := r.s.rebind("UPDATE outputs SET spent_by = ?, updated_at = ? WHERE output_id IN (" + joinComma(ph) + ")")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("metastore: mark spent: %w", err)
	}
	return nil
}

// ClearSpentBy reverses MarkSpent for every output consumed by
// spendingTransactionID (sets spent_by back to NULL). Used when an unbroadcast
// transaction is aborted and its inputs are restored.
func (r OutputsRepo) ClearSpentBy(ctx context.Context, spendingTransactionID uint) error {
	q := r.s.rebind("UPDATE outputs SET spent_by = NULL, updated_at = ? WHERE spent_by = ?")
	if _, err := r.s.execer(ctx).ExecContext(ctx, q, r.s.encTime(r.s.now()), spendingTransactionID); err != nil {
		return fmt.Errorf("metastore: clear spent_by: %w", err)
	}
	return nil
}

// BRC29Material is the key-derivation material retained on an output for BRC-29
// (peer-to-peer payment) unlocking.
type BRC29Material struct {
	DerivationPrefix   *string
	DerivationSuffix   *string
	SenderIdentityKey  *string
	CustomInstructions *string
}

// GetBRC29Material returns the derivation material for an output, or
// found=false when the output does not exist.
func (r OutputsRepo) GetBRC29Material(ctx context.Context, outputID uint) (BRC29Material, bool, error) {
	q := r.s.rebind(
		`SELECT derivation_prefix, derivation_suffix, sender_identity_key, custom_instructions
		 FROM outputs WHERE output_id = ?`)
	var pfx, sfx, sender, custom sql.NullString
	err := r.s.execer(ctx).QueryRowContext(ctx, q, outputID).Scan(&pfx, &sfx, &sender, &custom)
	if errors.Is(err, sql.ErrNoRows) {
		return BRC29Material{}, false, nil
	}
	if err != nil {
		return BRC29Material{}, false, fmt.Errorf("metastore: brc29 material: %w", err)
	}
	return BRC29Material{
		DerivationPrefix:   nullStr(pfx),
		DerivationSuffix:   nullStr(sfx),
		SenderIdentityKey:  nullStr(sender),
		CustomInstructions: nullStr(custom),
	}, true, nil
}
