package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// ListActions returns a page of wallet actions (transactions) for the user.
//
// Simplification: BRC-100 spec-op label routing (e.g. the "unfail" pseudo-label
// and ListFailedActions) is not implemented; ordinary label/status filtering is.
// Inputs are not expanded (IncludeInputs is accepted but yields no input rows);
// outputs and labels are expanded when requested.
func (p *Provider) ListActions(ctx context.Context, auth wdk.AuthID, args wdk.ListActionsArgs) (*wdk.ListActionsResult, error) {
	p.trace(ctx, "ListActions")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	rows, total, err := p.meta.Transactions().ListActions(ctx, metastore.ListActionsFilter{
		UserID:         userID,
		Labels:         stringsFrom(args.Labels),
		LabelQueryMode: args.LabelQueryMode,
		Limit:          int(args.Limit),
		Offset:         int(args.Offset), //nolint:gosec // pagination offset within int range
	})
	if err != nil {
		return nil, fmt.Errorf("storage: list actions: %w", err)
	}

	includeLabels := args.IncludeLabels != nil && bool(*args.IncludeLabels)
	includeOutputs := args.IncludeOutputs != nil && bool(*args.IncludeOutputs)

	var labelsByTx map[uint][]string
	if includeLabels {
		ids := make([]uint, len(rows))
		for i := range rows {
			ids[i] = rows[i].TransactionID
		}
		labelsByTx, err = p.meta.Transactions().GetLabelsForTransactions(ctx, ids)
		if err != nil {
			return nil, fmt.Errorf("storage: labels for actions: %w", err)
		}
	}

	actions := make([]wdk.WalletAction, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		action := wdk.WalletAction{
			TxID:        derefStr(row.TxID),
			Satoshis:    row.Satoshis,
			Status:      string(row.Status),
			IsOutgoing:  row.IsOutgoing,
			Description: row.Description,
			Version:     derefU32(row.Version),
			LockTime:    derefU32(row.LockTime),
		}
		if includeLabels {
			action.Labels = labelsByTx[row.TransactionID]
		}
		if includeOutputs {
			outs, oerr := p.actionOutputs(ctx, userID, row.TransactionID)
			if oerr != nil {
				return nil, oerr
			}
			action.Outputs = outs
		}
		actions = append(actions, action)
	}

	return &wdk.ListActionsResult{
		TotalActions: primitives.PositiveInteger(total), //nolint:gosec // non-negative
		Actions:      actions,
	}, nil
}

// actionOutputs expands a transaction's outputs for ListActions.
func (p *Provider) actionOutputs(ctx context.Context, userID int, transactionID uint) ([]wdk.WalletActionOutput, error) {
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		UserID:        &userID,
		TransactionID: &transactionID,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: action outputs: %w", err)
	}
	out := make([]wdk.WalletActionOutput, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		spendable, err := p.outpointSpendable(ctx, row.TxID, row.Vout)
		if err != nil {
			return nil, err
		}
		wao := wdk.WalletActionOutput{
			Satoshis:          uint64(row.Satoshis), //nolint:gosec // non-negative
			Spendable:         spendable,
			OutputIndex:       row.Vout,
			OutputDescription: row.OutputDescription,
		}
		if row.Basket != nil {
			wao.Basket = *row.Basket
		}
		if row.CustomInstructions != nil {
			wao.CustomInstructions = *row.CustomInstructions
		}
		if len(row.LockingScript) > 0 {
			wao.LockingScript = fmt.Sprintf("%x", row.LockingScript)
		}
		wao.Tags = row.Tags
		out = append(out, wao)
	}
	return out, nil
}

// ListTransactions returns a page of transactions with their standardized
// status updates for the user.
func (p *Provider) ListTransactions(ctx context.Context, auth wdk.AuthID, args wdk.ListTransactionsArgs) (*wdk.ListTransactionsResult, error) {
	p.trace(ctx, "ListTransactions")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	mode := args.LabelQueryMode
	rows, total, err := p.meta.Transactions().ListTransactions(ctx, metastore.ListTransactionsFilter{
		UserID:         userID,
		TxIDs:          args.TxIDs,
		Labels:         args.Labels,
		LabelQueryMode: &mode,
		Limit:          int(args.Limit),
		Offset:         int(args.Offset), //nolint:gosec // pagination offset within int range
	})
	if err != nil {
		return nil, fmt.Errorf("storage: list transactions: %w", err)
	}

	ids := make([]uint, len(rows))
	for i := range rows {
		ids[i] = rows[i].TransactionID
	}
	labelsByTx, err := p.meta.Transactions().GetLabelsForTransactions(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("storage: labels for transactions: %w", err)
	}

	out := make([]wdk.CurrentTxStatus, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		status := row.Status.ToStandardizedStatus()
		if args.Status != nil && status != *args.Status {
			continue
		}
		cur := wdk.CurrentTxStatus{
			TxID:      derefStr(row.TxID),
			Status:    status,
			Reference: string(row.Reference),
			Labels:    labelsByTx[row.TransactionID],
		}
		p.enrichTxStatus(ctx, row.TxID, &cur)
		out = append(out, cur)
	}

	return &wdk.ListTransactionsResult{
		TotalTransactions: primitives.PositiveInteger(total), //nolint:gosec // non-negative
		Transactions:      out,
	}, nil
}

// enrichTxStatus fills block/merkle fields from the known-tx row when present.
func (p *Provider) enrichTxStatus(ctx context.Context, txid *string, cur *wdk.CurrentTxStatus) {
	if txid == nil {
		return
	}
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, *txid)
	if err != nil || !found {
		return
	}
	if kt.BlockHeight != nil {
		cur.BlockHeight = *kt.BlockHeight
	}
	if len(kt.BlockHash) > 0 {
		cur.BlockHash = fmt.Sprintf("%x", kt.BlockHash)
	}
	if len(kt.MerkleRoot) > 0 {
		cur.MerkleRoot = fmt.Sprintf("%x", kt.MerkleRoot)
	}
	if len(kt.MerklePath) > 0 {
		if mp, perr := transaction.NewMerklePathFromBinary(kt.MerklePath); perr == nil {
			cur.MerklePath = mp
		}
	}
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefU32(v *uint32) uint32 {
	if v == nil {
		return 0
	}
	return *v
}
