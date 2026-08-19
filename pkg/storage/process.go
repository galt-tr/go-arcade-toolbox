package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/txutils"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// ProcessAction persists a signed transaction created by CreateAction and,
// unless it is delayed or no-send, broadcasts it through the arcade oracle.
func (p *Provider) ProcessAction(ctx context.Context, auth wdk.AuthID, args wdk.ProcessActionArgs) (*wdk.ProcessActionResult, error) {
	p.trace(ctx, "ProcessAction")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}
	result := &wdk.ProcessActionResult{}

	// Immediate-broadcast mode overrides the delayed default so the send happens
	// inline now instead of via the monitor. Applied before persisting/status
	// selection so every downstream branch treats the tx as non-delayed. An
	// explicit no-send is a distinct caller choice and is left untouched.
	if p.immediateBroadcast {
		args.IsDelayed = false
	}

	if args.IsNewTx {
		if err := p.processNewTx(ctx, userID, args); err != nil {
			return nil, err
		}
	}

	// noSend without a sendWith batch: committed locally, nothing to broadcast.
	if args.IsNoSend && len(args.SendWith) == 0 {
		return result, nil
	}

	txids := broadcastTxIDs(args)
	if len(txids) == 0 {
		return result, nil
	}

	// Delayed: do not broadcast now — the monitor sends later. Mark the tx as
	// waiting-to-send; change is already minted (spendable).
	if args.IsDelayed {
		for _, txid := range txids {
			if err := p.queueDelayed(ctx, txid); err != nil {
				return nil, err
			}
			result.SendWithResults = append(result.SendWithResults, wdk.SendWithResult{
				TxID:   primitives.TXIDHexString(txid),
				Status: wdk.SendWithResultStatusSending,
			})
		}
		return result, nil
	}

	// Non-delayed: broadcast each transaction.
	for _, txid := range txids {
		swr, rar, err := p.broadcastOne(ctx, userID, txid)
		if err != nil {
			return nil, err
		}
		result.SendWithResults = append(result.SendWithResults, swr)
		if rar != nil {
			result.NotDelayedResults = append(result.NotDelayedResults, *rar)
		}
	}
	return result, nil
}

// processNewTx validates and persists a freshly-signed transaction: it PINS the
// funding reservation, sets the txid, records the known-tx (raw tx + input
// BEEF), transitions the transaction out of unsigned, and mints its change into
// the change basket at TierSending (the txid is now known). Inputs stay
// reserved until broadcast acceptance — and, from here on, pinned, so no
// janitor can free them while the signed bytes are broadcastable.
func (p *Provider) processNewTx(ctx context.Context, userID int, args wdk.ProcessActionArgs) error {
	if args.Reference == nil {
		return fmt.Errorf("storage: process new tx requires a reference")
	}
	if args.TxID == nil {
		return fmt.Errorf("storage: process new tx requires a txid")
	}
	tx, err := transaction.NewTransactionFromBytes(args.RawTx)
	if err != nil {
		return fmt.Errorf("storage: parse raw tx: %w", err)
	}
	txid := tx.TxID().String()
	if txid != string(*args.TxID) {
		return fmt.Errorf("storage: raw tx id %s does not match provided txid %s", txid, string(*args.TxID))
	}

	txRow, found, err := p.meta.Transactions().FindByReference(ctx, userID, *args.Reference)
	if err != nil {
		return fmt.Errorf("storage: find transaction by reference: %w", err)
	}
	if !found {
		return fmt.Errorf("storage: no transaction for reference %q", *args.Reference)
	}
	if !txRow.IsOutgoing {
		return fmt.Errorf("storage: transaction %q is not outgoing", *args.Reference)
	}
	switch txRow.Status {
	case wdk.TxStatusUnsigned, wdk.TxStatusUnprocessed:
	default:
		return fmt.Errorf("storage: transaction %q has status %q, not signable", *args.Reference, txRow.Status)
	}
	// A re-drive that carries DIFFERENT bytes is refused outright (audit P1-4).
	// Repointing the row would orphan the raw tx already stored under the old
	// txid: it stays resendable in known_txs with nothing in transactions
	// pointing at it, so the wallet can broadcast bytes it no longer believes
	// it owns. The same bytes arriving twice is the benign case and proceeds.
	//
	// Deliberately BELOW the status switch. Of the two statuses that admit a
	// signer, only 'unprocessed' can legitimately already carry a txid (a
	// delayed transaction an earlier call persisted); an 'unsigned' row has
	// none by definition. Every OTHER txid-bearing row — aborted, failed,
	// sending, completed — is described far better by "not signable" than by a
	// divergence report, so the switch must get first refusal on it.
	//
	// This read is not atomic with the write; it exists for the message. The
	// actual fence is [metastore.TransactionsRepo.SetTxID]'s CAS below, which
	// closes the TOCTOU this check cannot.
	if txRow.TxID != nil && *txRow.TxID != txid {
		return divergentReDriveErr(*args.Reference, *txRow.TxID, txid)
	}

	// Hydrate input source transactions from the stored input BEEF so scripts
	// can be verified and the EF can be built.
	if err := p.hydrateInputs(tx, txRow.InputBEEF); err != nil {
		return err
	}
	if ok, verr := p.scripts.VerifyScripts(ctx, tx); verr != nil || !ok {
		return fmt.Errorf("storage: script verification failed: %w", verr)
	}

	// Validate provided (ProvidedByYou) outputs match the signed tx.
	if err := p.validateSignedOutputs(ctx, txRow.TransactionID, tx); err != nil {
		return err
	}

	// Serialize once: the fee check measures these exact bytes and the known-tx
	// row stores them, so there is no second serialization and no chance of the
	// two disagreeing about what was priced.
	rawTx := tx.Bytes()
	if err := p.checkBroadcastFeeRate(tx, len(rawTx)); err != nil {
		return err
	}

	txStatus, ktxStatus := newTxStatuses(args)

	return p.meta.Do(ctx, func(ctx context.Context) error {
		// PIN FIRST — before anything that makes the transaction broadcastable
		// (audit P0-4, provider half). A pin is the committed statement "a
		// signed transaction spends these coins": pinned rows stay reserved and
		// refuse claims exactly as before, but ReleaseReservation and
		// FindStaleReservations skip them, so no janitor can free the inputs of
		// an in-flight send. See [utxostore.Store.Pin].
		//
		// The ORDER is the point, and it differs per deployment mode (see the
		// package doc):
		//
		//   Mode A (shared database): this statement runs inside the very
		//   transaction that stores the raw tx, so the pin and the bytes commit
		//   or roll back together — atomic, no window at all.
		//
		//   Mode B (split stores): the utxostore cannot join the metastore's
		//   transaction, so the pin commits HERE, before the metadata does.
		//   That asymmetry is deliberate and fail-safe. A meta half that rolls
		//   back leaves the row at whichever status the switch above admitted,
		//   and the two arms differ:
		//
		//     'unsigned' — no raw tx exists anywhere, so the pin is a genuine
		//     orphan holding coins for nothing. The existing AbortAbandoned
		//     sweep reclaims it, but only once the row has aged past
		//     failAbandonedAge, so those coins are unavailable until then.
		//
		//     'unprocessed' — an earlier call already stored broadcastable bytes
		//     under this reference. The pin backs a real transaction and is not
		//     an orphan at all; continuing to hold those coins is correct.
		//
		//   Either way the failure mode is availability. The reverse order would
		//   leave a stored, broadcastable raw tx whose inputs are still
		//   sweepable: a double-spend window.
		//
		// Pin returns how many rows it NEWLY pinned; 0 is not a failure. Both
		// zero cases are legitimate here — an identical re-drive whose rows are
		// already pinned, and a reservation whose rows have since been spent or
		// released. The count is not load-bearing, so it is discarded.
		if _, err := p.utxo.Pin(ctx, int64(userID), *args.Reference); err != nil {
			return fmt.Errorf("storage: pin reserved inputs: %w", err)
		}
		if err := p.meta.Transactions().SetTxID(ctx, txRow.TransactionID, txid); err != nil {
			// The CAS refused: the row was bound to different bytes, possibly
			// by a racer since the pre-check above read it. Fail the action —
			// never re-read and overwrite, which is the very repointing this
			// CAS exists to prevent (see SetTxID's caller guidance).
			if errors.Is(err, metastore.ErrTxIDMismatch) {
				return divergentReDriveCASErr(*args.Reference, txid, err)
			}
			return fmt.Errorf("storage: set txid: %w", err)
		}
		if err := p.meta.Transactions().UpdateStatus(ctx, txRow.TransactionID, txStatus, wdk.TxStatusUnsigned, wdk.TxStatusUnprocessed); err != nil {
			return fmt.Errorf("storage: update tx status: %w", err)
		}
		// The skip set makes this Upsert a guarded BACKWARD transition: it
		// writes a pre-broadcast status, so it must not apply to a known tx
		// that has been fenced (aborted), has gone terminal
		// (suspectFailed/stuck/invalid/doubleSpend), is mid-POST (sending), or
		// is already past the broadcast stage. Without the guard a re-drive
		// would reset such a row to 'unsent' and hand its bytes straight back
		// to the send sweep. See [metastore.KnownTxNeverRequeueStatuses].
		if err := p.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
			TxID:         txid,
			Status:       ktxStatus,
			WasBroadcast: false,
			RawTx:        rawTx,
			// InputBEEF deliberately omitted: the ancestry stays on the
			// transactions row this function already read it from. Storing it
			// twice cost a second ~3.4kB TOASTed write per transaction plus a
			// second clearing UPDATE at MINED, on the WAL-bound hot path. See
			// Provider.inputBEEFFor for the read side.
		}, metastore.KnownTxNeverRequeueStatuses...); err != nil {
			// A skip is not a benign no-op here: the caller asked to make these
			// bytes broadcastable and storage is refusing, so the action must
			// fail rather than report a success it did not deliver. This is a
			// cold path whose unit of work is about to roll back anyway, so it
			// spends one more read to name the status that did the refusing —
			// "aborted" and "completed" call for very different operator
			// responses, and the guard alone cannot tell them apart.
			if errors.Is(err, metastore.ErrStatusUpdateSkipped) {
				return fmt.Errorf("storage: known tx %s is at status %q, which never re-queues "+
					"for broadcast: %w", txid, p.knownTxStatusFor(ctx, txid), err)
			}
			return fmt.Errorf("storage: upsert known tx: %w", err)
		}
		return p.mintChange(ctx, userID, txRow.TransactionID, txid)
	})
}

// divergentReDriveErr is the refusal for audit P1-4 as [Provider.processNewTx]'s
// pre-check sees it: the row it read is already bound to another signing, and a
// reference bound to one signing may never be repointed at another.
//
// Both this and [divergentReDriveCASErr] wrap [metastore.ErrTxIDMismatch], so
// the two in-tree call sites are matchable as one condition. That is the whole
// of the claim: there is no storage-level exported sentinel for this yet, the
// way [ErrFeeBelowFloor] exists for the fee floor, so out-of-tree callers have
// nothing stable to match on. Re-exporting one belongs with C3's abort work,
// which gives the divergence a documented resolution to point at.
func divergentReDriveErr(reference, boundTxID, newTxID string) error {
	return fmt.Errorf("storage: reference %q is already bound to txid %s; refusing re-drive with %s: %w",
		reference, boundTxID, newTxID, metastore.ErrTxIDMismatch)
}

// divergentReDriveCASErr is the same refusal detected one layer down, by
// [metastore.TransactionsRepo.SetTxID]'s CAS, after a racer bound the row
// between the pre-check's read and this write.
//
// It wraps cause rather than the bare sentinel: cause carries the transaction
// id the CAS was operating on, which is the only identifying detail available
// here. The winning txid is deliberately absent — learning it would mean
// re-reading the row, and SetTxID's caller guidance forbids exactly that,
// because the re-read is the first half of the overwrite this CAS exists to
// prevent.
func divergentReDriveCASErr(reference, newTxID string, cause error) error {
	return fmt.Errorf("storage: reference %q was bound to a different txid concurrently; "+
		"refusing re-drive with %s: %w", reference, newTxID, cause)
}

// knownTxStatusFor reports a known tx's current status for use in an error
// message. It is best-effort by design: the caller is already failing, and a
// lookup that cannot answer must not replace the real error with its own.
func (p *Provider) knownTxStatusFor(ctx context.Context, txid string) wdk.ProvenTxReqStatus {
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, txid)
	if err != nil || !found {
		return "unknown"
	}
	return kt.Status
}

// ErrFeeBelowFloor is returned when a signed transaction's actual fee is below
// the locally-configured broadcast floor. It is a FINAL, local verdict: the
// transaction bytes are underpriced and no retry of the same bytes will change
// that. Callers match it with errors.Is to distinguish "this transaction is
// wrong" from "the network is busy".
var ErrFeeBelowFloor = errors.New("storage: transaction fee is below the configured broadcast floor")

// checkBroadcastFeeRate refuses a signed transaction whose real fee is below the
// configured sat/kB floor, so the wallet never offers the network something it
// has already computed will be refused. Disabled (and free) when no floor is
// configured — see [WithMinBroadcastFeeRate].
//
// It is deliberately measured, not estimated. The fee is the difference between
// what the inputs bring and what the outputs take, read from the source outputs
// the caller has already had to supply for script verification; the size is the
// serialized length of the very bytes that will be broadcast. Both are facts
// about the finished transaction, so this cannot drift from what the node sees
// the way a pre-signing size estimate can.
//
// An input whose source output is unavailable makes the fee unknowable. That is
// not a fee failure and is not reported as one: the check declines to judge and
// lets the transaction through, because a wallet must not invent a verdict it
// has no evidence for. Such a transaction cannot be EF-encoded anyway and will
// fail later, with an accurate message.
func (p *Provider) checkBroadcastFeeRate(tx *transaction.Transaction, sizeBytes int) error {
	if p.minBroadcastFeeRate <= 0 {
		return nil
	}
	var inSats, outSats uint64
	for _, in := range tx.Inputs {
		src := in.SourceTxOutput()
		if src == nil {
			return nil // fee not knowable; see the doc comment
		}
		inSats += src.Satoshis
	}
	for _, out := range tx.Outputs {
		outSats += out.Satoshis
	}
	if outSats > inSats {
		// Outputs exceeding inputs is a malformed transaction, not an underpaid
		// one; report it as the (larger) problem it is rather than as a shortfall.
		return fmt.Errorf("%w: outputs (%d sat) exceed inputs (%d sat)", ErrFeeBelowFloor, outSats, inSats)
	}
	fee := inSats - outSats
	//nolint:gosec // a serialized length is non-negative.
	required := txutils.MinRequiredFee(uint64(sizeBytes), p.minBroadcastFeeRate)
	if fee >= required {
		return nil
	}
	return fmt.Errorf("%w: %d sat over %d bytes is %d sat short of the %d sat/kB floor "+
		"(the committed fee was computed from a size estimate that the signed transaction exceeded — "+
		"check every input's declared unlockingScriptLength)",
		ErrFeeBelowFloor, fee, sizeBytes, required-fee, p.minBroadcastFeeRate)
}

// mintChange mints the change outputs of a transaction into the change basket
// at TierSending. Idempotent (utxostore.Mint is a no-op for identical rows).
func (p *Provider) mintChange(ctx context.Context, userID int, transactionID uint, txid string) error {
	change := true
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		UserID:        &userID,
		TransactionID: &transactionID,
		Change:        &change,
	})
	if err != nil {
		return fmt.Errorf("storage: find change outputs: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: parse txid %s: %w", txid, err)
	}
	mints := make([]*utxostore.Mint, 0, len(rows))
	for i := range rows {
		// Mint each self-owned (change-purpose) output into ITS OWN basket: the
		// default change basket for ordinary change, and the pool/reserve basket
		// for throughput fuel fan-out outputs (which are also emitted as change).
		if rows[i].Basket == nil {
			continue
		}
		mints = append(mints, &utxostore.Mint{
			Outpoint:  utxostore.Outpoint{TxID: *hash, Vout: rows[i].Vout},
			UserID:    int64(userID),
			Basket:    *rows[i].Basket,
			Satoshis:  uint64(rows[i].Satoshis), //nolint:gosec // change value non-negative
			InputSize: utxostore.DefaultP2PKHInputSize,
			Tier:      utxostore.TierSending,
		})
	}
	if len(mints) == 0 {
		return nil
	}
	if err := p.utxo.Mint(ctx, mints); err != nil {
		return fmt.Errorf("storage: mint change: %w", err)
	}
	for _, m := range mints {
		if m.Err != nil {
			return fmt.Errorf("storage: mint change %s: %w", m.Outpoint, m.Err)
		}
	}
	return nil
}

// queueDelayed marks a transaction as waiting-to-send (the monitor broadcasts
// later). It does not spend inputs or broadcast; the transaction stays at a
// pre-broadcast (abortable) status and the known-tx at "unsent".
func (p *Provider) queueDelayed(ctx context.Context, txid string) error {
	if err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnsent, wdk.ProvenTxReqBeyondBroadcastStageStatuses...); err != nil &&
		!errors.Is(err, metastore.ErrStatusUpdateSkipped) && !errors.Is(err, metastore.ErrNotFound) {
		return fmt.Errorf("storage: mark unsent: %w", err)
	}
	return nil
}

// broadcastOne EF-encodes and broadcasts one transaction, then commits the
// outcome: on 202-accept it spends the reserved inputs, promotes the change to
// TierUnproven, and marks the tx unproven; on a 4xx rejection (final) it marks
// the tx failed / suspectFailed WITHOUT releasing inputs (the reconciler's job);
// on 503 backpressure it honors Retry-After once, then returns a service error.
func (p *Provider) broadcastOne(ctx context.Context, userID int, txid string) (wdk.SendWithResult, *wdk.ReviewActionResult, error) {
	swr := wdk.SendWithResult{TxID: primitives.TXIDHexString(txid)}

	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, txid)
	if err != nil {
		return swr, nil, fmt.Errorf("storage: load known tx: %w", err)
	}
	if !found || len(kt.RawTx) == 0 {
		return swr, nil, fmt.Errorf("storage: no stored raw tx for %s", txid)
	}
	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		return swr, nil, fmt.Errorf("storage: parse stored raw tx %s: %w", txid, err)
	}
	stored, err := p.inputBEEFFor(ctx, txid, kt.InputBEEF)
	if err != nil {
		return swr, nil, err
	}
	if err := p.hydrateInputs(tx, stored); err != nil {
		return swr, nil, err
	}
	if ok, verr := p.scripts.VerifyScripts(ctx, tx); verr != nil || !ok {
		return swr, nil, fmt.Errorf("storage: script verification failed for %s: %w", txid, verr)
	}
	ef, err := tx.EF()
	if err != nil {
		return swr, nil, fmt.Errorf("storage: EF-encode %s: %w", txid, err)
	}

	res, berr := p.broadcastWithBackpressure(ctx, txid, ef)
	switch {
	case berr != nil && isBackpressure(berr):
		// Exhausted the single retry: leave the tx in-flight, report service error.
		swr.Status = wdk.SendWithResultStatusSending
		return swr, &wdk.ReviewActionResult{
			TxID:   primitives.TXIDHexString(txid),
			Status: wdk.ReviewActionResultStatusServiceError,
			Errors: wdk.ReviewActionErrors{"arcade": berr},
		}, nil
	case berr != nil:
		// Opaque/transport failure: unknown fate; report service error, keep in-flight.
		swr.Status = wdk.SendWithResultStatusSending
		return swr, &wdk.ReviewActionResult{
			TxID:   primitives.TXIDHexString(txid),
			Status: wdk.ReviewActionResultStatusServiceError,
			Errors: wdk.ReviewActionErrors{"arcade": berr},
		}, nil
	}

	if res.Rejected {
		return p.commitRejected(ctx, userID, txid, res)
	}
	return p.commitAccepted(ctx, userID, txid, tx)
}

// broadcastWithBackpressure calls the oracle, honoring a single Retry-After on a
// 503 backpressure response.
func (p *Provider) broadcastWithBackpressure(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	res, err := p.oracle.Broadcast(ctx, txid, ef)
	if err == nil {
		return res, nil
	}
	var bp *arcade.BackpressureError
	if !errors.As(err, &bp) {
		return nil, err
	}
	// Honor Retry-After once.
	if waitErr := sleepCtx(ctx, bp.RetryAfter); waitErr != nil {
		return nil, waitErr
	}
	return p.oracle.Broadcast(ctx, txid, ef)
}

// commitAccepted commits a 202-accepted broadcast: spend reserved inputs,
// promote change to TierUnproven, transition statuses.
func (p *Provider) commitAccepted(ctx context.Context, userID int, txid string, tx *transaction.Transaction) (wdk.SendWithResult, *wdk.ReviewActionResult, error) {
	swr := wdk.SendWithResult{TxID: primitives.TXIDHexString(txid), Status: wdk.SendWithResultStatusUnproven}
	rar := &wdk.ReviewActionResult{TxID: primitives.TXIDHexString(txid), Status: wdk.ReviewActionResultStatusSuccess}

	txRow := p.firstTxByTxID(ctx, userID, txid)

	if err := p.applyAcceptedBroadcast(ctx, userID, txid, tx, txRow); err != nil {
		return swr, nil, err
	}
	return swr, rar, nil
}

// applyAcceptedBroadcast commits the wallet-state transition for a
// broadcast-accepted transaction, atomically: transaction rows → unproven,
// known tx → unconfirmed, reserved inputs → spent, change coins → TierUnproven,
// and the local input spend-history recorded. It is shared by the synchronous
// broadcast path ([Provider.commitAccepted]) and the monitor's SendWaiting
// sweep. txRow (the transaction row carrying userID/reference/transaction-id)
// may be nil, in which case only the status transitions are applied.
func (p *Provider) applyAcceptedBroadcast(ctx context.Context, userID int, txid string, tx *transaction.Transaction, txRow *wdk.TableTransaction) error {
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusUnproven,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: mark unproven: %w", err)
		}
		if err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnconfirmed,
			wdk.ProvenTxReqBeyondBroadcastStageStatuses...); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) && !errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: mark known tx unconfirmed: %w", err)
		}
		// Spend the reserved inputs (reserved→spent).
		if txRow != nil {
			if err := p.spendReservedInputs(ctx, tx, txid, string(txRow.Reference)); err != nil {
				return err
			}
			// Change stays at TierSending here and is NOT promoted to claimable
			// on the 202. A 202 is "accepted for processing", not validated: the
			// tx can still fail validation, and until arcade has SEEN it a child
			// that spends this change can reach arcade's validation before the
			// parent is in its mempool — an unknown-input rejection that cascades
			// down a self-payment chain. Promotion to TierUnproven happens only on
			// the actual SEEN status ([Provider.applySeen] / the poll fallback), or
			// directly to TierMined on proof — so a coin is spendable only once the
			// network has accepted the tx that created it.
			if err := p.markInputsSpent(ctx, userID, txRow.TransactionID, tx); err != nil {
				return err
			}
		}
		return nil
	})
}

// commitRejected commits a final 4xx rejection: mark the tx failed and the
// known-tx suspectFailed, and REMOVE the change minted in this same
// ProcessAction. A synchronous POST /tx 4xx is a final tx-level rejection
// (arcade contract): that change output never reached the chain, so leaving it
// live at TierSending would inflate the balance and be selectable — and the tx
// is now Failed (non-abortable), so the user cannot self-heal. The reserved
// INPUTS are deliberately NOT released here (the false-positive guard: the M4
// async-reject reconciler owns input release).
func (p *Provider) commitRejected(ctx context.Context, userID int, txid string, res *arcade.BroadcastResult) (wdk.SendWithResult, *wdk.ReviewActionResult, error) {
	swr := wdk.SendWithResult{TxID: primitives.TXIDHexString(txid), Status: wdk.SendWithResultStatusFailed}
	status := wdk.ReviewActionResultStatusInvalidTx
	if len(res.ExtraInfo) > 0 && res.Status == arcade.StatusDoubleSpendAttempted {
		status = wdk.ReviewActionResultStatusDoubleSpend
	}
	rar := &wdk.ReviewActionResult{
		TxID:   primitives.TXIDHexString(txid),
		Status: status,
		Errors: wdk.ReviewActionErrors{"arcade": errors.New(res.ExtraInfo)},
	}

	txRow := p.firstTxByTxID(ctx, userID, txid)

	// Record the reason before anything else: the 4xx body is the only statement
	// of cause that will ever exist for this transaction, and arcade may have
	// forgotten the transaction by the time anyone asks it again.
	p.logRejection(ctx, txid, string(res.Status), res.ExtraInfo, "broadcast_4xx")

	err := p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusFailed,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: mark failed: %w", err)
		}
		if err := p.meta.KnownTx().MarkSuspectFailed(ctx, txid, p.now(), res.ExtraInfo); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: mark suspect failed: %w", err)
		}
		// Remove the phantom change coins minted in this ProcessAction (final
		// rejection: they never reached the chain). Inputs are left reserved.
		if txRow != nil {
			ops, oerr := p.changeOutpoints(ctx, userID, txRow.TransactionID, txid)
			if oerr != nil {
				return oerr
			}
			if len(ops) > 0 {
				hash, herr := chainhash.NewHashFromHex(txid)
				if herr != nil {
					return fmt.Errorf("storage: parse txid: %w", herr)
				}
				if _, err := p.utxo.RemoveByMintTx(ctx, *hash, ops); err != nil {
					return fmt.Errorf("storage: remove rejected change: %w", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return swr, nil, err
	}
	return swr, rar, nil
}

// spendReservedInputs transitions the transaction's own reserved inputs to
// spent. Inputs not in the utxostore (external) are ignored.
func (p *Provider) spendReservedInputs(ctx context.Context, tx *transaction.Transaction, txid, reference string) error {
	spendingTxID, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: parse spending txid: %w", err)
	}
	ops := make([]*utxostore.SpendOp, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		ops = append(ops, &utxostore.SpendOp{
			Outpoint:     utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex},
			Reservation:  reference,
			SpendingTxID: *spendingTxID,
		})
	}
	if len(ops) == 0 {
		return nil
	}
	if err := p.utxo.Spend(ctx, ops, false); err != nil {
		// Tolerate not-found (external inputs) and unreserved rows; surface any
		// spent-by-another (real double spend) or other hard failures.
		for _, op := range ops {
			if op.Err == nil {
				continue
			}
			if errors.Is(op.Err, &utxostore.NotFoundError{}) {
				continue
			}
			var re *utxostore.ReservedError
			if errors.As(op.Err, &re) && re.HeldBy == "" {
				// KNOWN BUG (audit P0-2): HeldBy == "" means "in the inventory,
				// currently unreserved", NOT "external" — swallowing it turns a
				// release race into a silent double spend. Rewired to fact mode
				// (force=true) in a follow-up task.
				continue // not our reserved coin (external input)
			}
			return fmt.Errorf("storage: spend input %s: %w", op.Outpoint, op.Err)
		}
	}
	return nil
}

// changeOutpoints returns the outpoints of a transaction's change coins.
func (p *Provider) changeOutpoints(ctx context.Context, userID int, transactionID uint, txid string) ([]utxostore.Outpoint, error) {
	change := true
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		UserID:        &userID,
		TransactionID: &transactionID,
		Change:        &change,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: find change outputs: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return nil, fmt.Errorf("storage: parse txid %s: %w", txid, err)
	}
	ops := make([]utxostore.Outpoint, 0, len(rows))
	for i := range rows {
		// Every self-owned (change-purpose) output regardless of basket: the
		// default change basket AND the throughput pool/reserve baskets (fuel
		// fan-out outputs). Promotion on broadcast-accept and change removal on
		// abort must cover them all, or fuel coins would stay stranded at
		// TierSending (never spendable) or linger after an abort.
		if rows[i].Basket == nil {
			continue
		}
		ops = append(ops, utxostore.Outpoint{TxID: *hash, Vout: rows[i].Vout})
	}
	return ops, nil
}

// markInputsSpent records the spend history on the local input output rows.
func (p *Provider) markInputsSpent(ctx context.Context, userID int, spendingTxID uint, tx *transaction.Transaction) error {
	var ids []uint
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		row, err := p.findLocalOutput(ctx, userID, in.SourceTXID.String(), in.SourceTxOutIndex)
		if err != nil {
			return err
		}
		if row != nil {
			ids = append(ids, row.OutputID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := p.meta.Outputs().MarkSpent(ctx, spendingTxID, ids); err != nil {
		return fmt.Errorf("storage: mark inputs spent: %w", err)
	}
	return nil
}

// hydrateInputs attaches source transactions from beefBytes to tx's inputs so
// scripts can be verified and the EF can be built.
func (p *Provider) hydrateInputs(tx *transaction.Transaction, beefBytes []byte) error {
	if len(beefBytes) == 0 {
		return nil
	}
	beef, err := transaction.NewBeefFromBytes(beefBytes)
	if err != nil {
		return fmt.Errorf("storage: parse input beef: %w", err)
	}
	for _, in := range tx.Inputs {
		if in.SourceTransaction != nil || in.SourceTXID == nil {
			continue
		}
		if src := beef.FindTransactionByHash(in.SourceTXID); src != nil {
			in.SourceTransaction = src
		}
	}
	return nil
}

// validateSignedOutputs checks that each stored provided (ProvidedByYou) output
// matches the signed transaction's locking script at that vout. Storage-derived
// change outputs (whose scripts the wallet derives) are not checked here.
func (p *Provider) validateSignedOutputs(ctx context.Context, transactionID uint, tx *transaction.Transaction) error {
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		TransactionID: &transactionID,
	})
	if err != nil {
		return fmt.Errorf("storage: load outputs for validation: %w", err)
	}
	for i := range rows {
		row := &rows[i]
		if row.ProvidedBy != string(wdk.ProvidedByYou) || len(row.LockingScript) == 0 {
			continue
		}
		if int(row.Vout) >= len(tx.Outputs) {
			return fmt.Errorf("storage: signed tx missing output at vout %d", row.Vout)
		}
		got := tx.Outputs[row.Vout].LockingScript
		if got == nil || !bytesEqual(got.Bytes(), row.LockingScript) {
			return fmt.Errorf("storage: signed tx output %d locking script mismatch", row.Vout)
		}
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstTxByTxID returns the first transaction row for txid under userID, or nil.
func (p *Provider) firstTxByTxID(ctx context.Context, userID int, txid string) *wdk.TableTransaction {
	rows, err := p.meta.Transactions().FindByTxID(ctx, userID, txid)
	if err != nil || len(rows) == 0 {
		return nil
	}
	return &rows[0]
}

// --- small helpers -------------------------------------------------------

func broadcastTxIDs(args wdk.ProcessActionArgs) []string {
	seen := make(map[string]struct{})
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if args.TxID != nil {
		add(string(*args.TxID))
	}
	for _, t := range args.SendWith {
		add(string(t))
	}
	return out
}

// newTxStatuses maps the process args to the (transaction, known-tx) statuses
// set when persisting a freshly-signed transaction.
func newTxStatuses(args wdk.ProcessActionArgs) (wdk.TxStatus, wdk.ProvenTxReqStatus) {
	switch {
	case args.IsNoSend:
		return wdk.TxStatusNoSend, wdk.ProvenTxStatusNoSend
	case args.IsDelayed:
		// Delayed: not yet broadcast; kept at a pre-broadcast (abortable) status
		// until the monitor sends it.
		return wdk.TxStatusUnprocessed, wdk.ProvenTxStatusUnsent
	default:
		return wdk.TxStatusSending, wdk.ProvenTxStatusUnprocessed
	}
}

func isBackpressure(err error) bool {
	var bp *arcade.BackpressureError
	return errors.As(err, &bp)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
