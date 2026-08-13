package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// abortableStatuses are the pre-broadcast transaction statuses a caller may
// abort. Anything with broadcast/network evidence (sending, unproven,
// completed, failed, aborted) is refused.
var abortableStatuses = map[wdk.TxStatus]bool{
	wdk.TxStatusUnsigned:    true,
	wdk.TxStatusNoSend:      true,
	wdk.TxStatusUnprocessed: true,
	wdk.TxStatusNonFinal:    true,
}

// AbortAction aborts a pre-broadcast transaction: it releases the funding
// reservation held under the transaction Reference, removes any minted change,
// and marks the transaction aborted. Post-broadcast transactions are refused
// with [wdk.ErrNotAbortableAction].
func (p *Provider) AbortAction(ctx context.Context, auth wdk.AuthID, args wdk.AbortActionArgs) (*wdk.AbortActionResult, error) {
	p.trace(ctx, "AbortAction")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}
	reference := string(args.Reference)

	txRow, found, err := p.meta.Transactions().FindByReference(ctx, userID, reference)
	if err != nil {
		return nil, fmt.Errorf("storage: find transaction: %w", err)
	}
	if !found && len(reference) == 64 {
		rows, ferr := p.meta.Transactions().FindByTxID(ctx, userID, reference)
		if ferr != nil {
			return nil, fmt.Errorf("storage: find transaction by txid: %w", ferr)
		}
		if len(rows) > 0 {
			txRow = &rows[0]
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("storage: no transaction for reference %q: %w", reference, wdk.ErrNotAbortableAction)
	}
	if !txRow.IsOutgoing {
		return nil, fmt.Errorf("storage: transaction %q is not outgoing: %w", reference, wdk.ErrNotAbortableAction)
	}
	if !abortableStatuses[txRow.Status] {
		return nil, fmt.Errorf("storage: transaction %q has status %q: %w", reference, txRow.Status, wdk.ErrNotAbortableAction)
	}

	if err := p.abortTxRow(ctx, userID, txRow); err != nil {
		if errors.Is(err, wdk.ErrNotAbortableAction) {
			return nil, err
		}
		return nil, fmt.Errorf("storage: abort action: %w", err)
	}
	return &wdk.AbortActionResult{Aborted: true}, nil
}

// abortTxRow is the transactional core of aborting a pre-broadcast
// transaction, shared by [Provider.AbortAction] (user-initiated) and the
// monitor's AbortAbandoned sweep. It CAS-transitions the row to aborted (from
// an abortable status), releases the funding reservation, removes any minted
// change, and restores the spend-history flag on its inputs — all atomically.
// It returns [wdk.ErrNotAbortableAction] when the CAS finds the row already
// past an abortable status (a concurrent transition won).
func (p *Provider) abortTxRow(ctx context.Context, userID int, txRow *wdk.TableTransaction) error {
	// Compute the change outpoints (to remove) before the write, using the
	// reservation token stored on the row.
	var txid string
	if txRow.TxID != nil {
		txid = *txRow.TxID
	}

	return p.meta.Do(ctx, func(ctx context.Context) error {
		// CAS the status first so a concurrent transition wins and rolls back.
		if err := p.meta.Transactions().UpdateStatus(ctx, txRow.TransactionID, wdk.TxStatusAborted,
			wdk.TxStatusUnsigned, wdk.TxStatusNoSend, wdk.TxStatusUnprocessed, wdk.TxStatusNonFinal); err != nil {
			if errors.Is(err, metastore.ErrStatusUpdateSkipped) {
				return wdk.ErrNotAbortableAction
			}
			return fmt.Errorf("storage: mark aborted: %w", err)
		}
		// Release the funding reservation (frees reserved inputs).
		if _, err := p.utxo.ReleaseReservation(ctx, int64(userID), string(txRow.Reference)); err != nil {
			return fmt.Errorf("storage: release reservation: %w", err)
		}
		// Remove any minted change (no-op when the tx was never processed).
		if txid != "" {
			ops, err := p.changeOutpoints(ctx, userID, txRow.TransactionID, txid)
			if err != nil {
				return err
			}
			if len(ops) > 0 {
				hash, herr := chainhash.NewHashFromHex(txid)
				if herr != nil {
					return fmt.Errorf("storage: parse txid: %w", herr)
				}
				if _, err := p.utxo.RemoveByMintTx(ctx, *hash, ops); err != nil {
					return fmt.Errorf("storage: remove minted change: %w", err)
				}
			}
			// Restore the spend-history flag on any inputs marked spent.
			if err := p.meta.Outputs().ClearSpentBy(ctx, txRow.TransactionID); err != nil {
				return fmt.Errorf("storage: clear spent_by: %w", err)
			}
		}
		return nil
	})
}
