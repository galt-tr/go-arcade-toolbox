package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
//
// This is a requeue-shaped write — its target status puts the bytes back on
// [metastore.KnownTxRepo.FindResendable]'s ungraced arm — so it carries
// [metastore.KnownTxNeverRequeueStatuses] as its guard, not the narrower
// beyond-broadcast set. That difference IS the fence: with only the narrow
// guard, a ProcessAction{IsDelayed, SendWith: [aborted txid]} walked an aborted
// row back to 'unsent' and handed the sweep bytes whose inputs the abort had
// already released. It also keeps a delayed re-request from yanking a row out
// from under a live broadcaster ('sending' is in the set).
//
// A skip is read, not swallowed. The two kinds mean opposite things: a row past
// the broadcast stage (or mid-POST) is an idempotent no-op — a retrying client
// is entitled to re-request a send that already happened — while a fenced or
// terminal row means storage is refusing to deliver what the caller asked for,
// and reporting success for that would be a lie the caller cannot detect.
func (p *Provider) queueDelayed(ctx context.Context, txid string) error {
	err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnsent, metastore.KnownTxNeverRequeueStatuses...)
	switch {
	case err == nil, errors.Is(err, metastore.ErrNotFound):
		return nil
	case !errors.Is(err, metastore.ErrStatusUpdateSkipped):
		return fmt.Errorf("storage: mark unsent: %w", err)
	}
	// Guarded out. One extra read (cold path only — the guard blocks nothing on
	// the ordinary delayed flow) buys an error that names the status, because
	// "aborted" and "completed" call for very different operator responses.
	st, rerr := p.statusBehindRefusal(ctx, txid)
	if rerr != nil {
		return rerr
	}
	if st.WasBroadcastStatus() || st == wdk.ProvenTxStatusSending {
		return nil
	}
	return notBroadcastableErr(txid, st)
}

// notBroadcastableError refuses a send for a known tx whose status the
// broadcast arbiter will not claim and which carries no evidence of ever having
// reached the network: fenced (aborted) or terminal (suspectFailed / stuck /
// invalidTx / doubleSpend).
//
// It is a type rather than a wrapped sentinel so callers can recover the STATUS
// without re-reading the row or scraping the message — the sweep logs it as a
// structured field. It still unwraps to [metastore.ErrStatusUpdateSkipped], so
// every refusal from the gate stays matchable as one condition.
//
// The distinction it carries is load-bearing for the sweep: this is the one
// refusal that is PERMANENT for the current row state, as opposed to the
// transient read failures below, which are worth retrying.
type notBroadcastableError struct {
	txid   string
	status wdk.ProvenTxReqStatus
}

func (e *notBroadcastableError) Error() string {
	return fmt.Sprintf("storage: transaction %s is %s; not broadcastable", e.txid, e.status)
}

func (e *notBroadcastableError) Unwrap() error { return metastore.ErrStatusUpdateSkipped }

func notBroadcastableErr(txid string, st wdk.ProvenTxReqStatus) error {
	return &notBroadcastableError{txid: txid, status: st}
}

// statusBehindRefusal re-reads the status that made a guard refuse. Its whole
// job is to keep three outcomes distinguishable, because the verdicts they
// justify are not the same:
//
//   - a status — the row really is fenced, terminal or already sent, and the
//     caller may name it in a permanent-sounding refusal, which is what it is.
//   - the row is GONE — it existed one statement ago (the guard's own zero-rows
//     disambiguation proved that), so something removed it concurrently.
//   - the read FAILED — we know nothing at all. This is the case that must not
//     be collapsed into the other two: a transient database blip reported as
//     "transaction X is unknown; not broadcastable" is a permanent-sounding
//     verdict about a transaction that may be perfectly sendable a second later,
//     and an operator reading it would go looking for the wrong problem.
//
// All three fail CLOSED — no confirmed status, no broadcast — but they say
// which they are, so "this transaction is dead" is never confused with "ask
// again". That is the difference from [Provider.knownTxStatusFor], which is
// deliberately best-effort: its caller already holds a real error and is only
// decorating it, whereas this one's callers are DECIDING on the answer.
func (p *Provider) statusBehindRefusal(ctx context.Context, txid string) (wdk.ProvenTxReqStatus, error) {
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, txid)
	switch {
	case err != nil:
		return "", fmt.Errorf("storage: transaction %s: status could not be read (transient); "+
			"refusing to broadcast this attempt: %w", txid, err)
	case !found:
		return "", fmt.Errorf("storage: transaction %s: known tx row vanished while it was being claimed; "+
			"refusing to broadcast this attempt", txid)
	}
	return kt.Status, nil
}

// sendClaim is the verdict of the broadcast gate: who, if anyone, owns the
// right to POST these bytes right now.
type sendClaim int

const (
	// sendClaimOwned: the row is ours, at 'sending'. POST it.
	sendClaimOwned sendClaim = iota
	// sendClaimInFlight: another claimant holds a live send. Do not POST.
	sendClaimInFlight
	// sendClaimAlreadySent: the network already has these bytes. Do not POST.
	sendClaimAlreadySent
)

// claimForBroadcast is the SEND half of the P0-3 one-row arbiter, shared by the
// synchronous broadcast path ([Provider.broadcastOne]) and the sweep
// ([Provider.sendOneWaiting]). No caller may hand bytes to the network without
// first winning here.
//
// known is the status the caller already read for this row, and it selects the
// arm: a row the caller found at 'sending' came off FindResendable's crash-
// recovery arm and is taken with [metastore.KnownTxRepo.ReclaimStaleSend] (the
// row is already in the fenced state; only its clock needs re-stamping), while
// anything else is taken with [metastore.KnownTxRepo.ClaimForSend]. Both are
// CAS writes, so a stale `known` costs at most a lost race, never a wrong write.
//
// THE CUTOFF: every ReclaimStaleSend here passes [resendGrace] — the same
// constant [metastore.KnownTxRepo.FindResendable] selects with. Sourcing the
// taker's cutoff anywhere else would let the selector and the taker disagree
// about what "stranded" means, which is how a live send gets stolen (taker too
// eager) or a dead one never recovered (taker too patient).
//
// A row at 'sending' can never be aborted: [metastore.KnownTxRepo.TransitionToAborted]
// refuses that status at any age, deliberately, so a claim cannot be
// snatched out from under a POST that may already be on the wire. The
// consequence is that a PERMANENTLY stuck send has no abort escape. What it has
// instead: the graced arm re-drives it once per grace forever, and it leaves
// 'sending' only by succeeding (→ unconfirmed) or by drawing a tx-level
// rejection (→ suspectFailed), at which point the reject→release reconciler
// owns it and can take it terminal. There is no operator path that fences a
// stuck 'sending' row directly, and adding one would mean re-opening the very
// window this CAS closes.
func (p *Provider) claimForBroadcast(ctx context.Context, txid string, known wdk.ProvenTxReqStatus) (sendClaim, error) {
	kt := p.meta.KnownTx()
	recovering := known == wdk.ProvenTxStatusSending

	var err error
	if recovering {
		err = kt.ReclaimStaleSend(ctx, txid, resendGrace)
	} else {
		err = kt.ClaimForSend(ctx, txid)
	}
	switch {
	case err == nil:
		return sendClaimOwned, nil
	case errors.Is(err, metastore.ErrNotFound):
		return 0, vanishedWhileClaimingErr(txid, err)
	case !errors.Is(err, metastore.ErrStatusUpdateSkipped):
		return 0, fmt.Errorf("storage: claim %s for broadcast: %w", txid, err)
	}

	// The CAS refused. Re-read rather than trusting `known`: the refusal itself
	// says the row is not where the caller last saw it.
	st, rerr := p.statusBehindRefusal(ctx, txid)
	if rerr != nil {
		return 0, rerr
	}
	switch {
	case st.WasBroadcastStatus():
		return sendClaimAlreadySent, nil
	case st != wdk.ProvenTxStatusSending:
		return 0, notBroadcastableErr(txid, st)
	case recovering:
		// Already attempted the takeover above and lost it: either the claim is
		// live, or another sweep re-drove it this window.
		return sendClaimInFlight, nil
	}

	// The row was claimed by someone else. If that claim is stranded past the
	// grace, its owner is presumed dead and this caller may take the send over —
	// the same recovery the sweep performs, available to a client re-driving
	// ProcessAction{SendWith} after the claiming process died.
	switch err := kt.ReclaimStaleSend(ctx, txid, resendGrace); {
	case err == nil:
		return sendClaimOwned, nil
	case errors.Is(err, metastore.ErrStatusUpdateSkipped):
		return sendClaimInFlight, nil
	case errors.Is(err, metastore.ErrNotFound):
		return 0, vanishedWhileClaimingErr(txid, err)
	default:
		return 0, fmt.Errorf("storage: reclaim stranded send %s: %w", txid, err)
	}
}

// vanishedWhileClaimingErr reports a claim CAS that found no row at all. This
// is NOT the ordinary "this transaction has no stored bytes" case — the caller
// read the row, and its raw tx, moments earlier — so it must not borrow that
// message. It means the known_txs row was removed between the read and the CAS,
// which nothing on the send path does; it is worth surfacing as its own
// anomaly rather than as a missing-bytes error an operator would chase.
func vanishedWhileClaimingErr(txid string, cause error) error {
	return fmt.Errorf("storage: known tx row for %s vanished between load and claim: %w", txid, cause)
}

// broadcastOne EF-encodes and broadcasts one transaction, then commits the
// outcome: on 202-accept it spends the reserved inputs, promotes the change to
// TierUnproven, and marks the tx unproven; on a 4xx rejection (final) it marks
// the tx failed / suspectFailed WITHOUT releasing inputs (the reconciler's job);
// on 503 backpressure it honors Retry-After once, then returns a service error.
//
// It CLAIMS the known-tx row before any of that — see [Provider.claimForBroadcast].
// Until that gate existed this function would POST any SendWith txid that had
// stored bytes, with no look at the row's status at all, which is what made the
// abort fence unenforceable: aborting releases the inputs, so a SendWith
// arriving afterwards broadcast a transaction the wallet had already spent the
// coins of. The claim also makes the two no-POST outcomes representable —
// another instance already sending, and the network already holding the bytes.
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

	// The gate. Everything above is local work that can be redone for free; from
	// here on the bytes may leave the process, so the row must be ours first.
	claim, err := p.claimForBroadcast(ctx, txid, kt.Status)
	if err != nil {
		return swr, nil, err
	}
	switch claim {
	case sendClaimAlreadySent:
		// Idempotent success: the network already accepted these bytes, so the
		// caller gets exactly what the original 202 gave it. Re-POSTing would be
		// harmless at arcade (duplicates are idempotent by txid) but it is a
		// round trip spent to learn something the row already knows.
		swr.Status = wdk.SendWithResultStatusUnproven
		return swr, &wdk.ReviewActionResult{
			TxID:   primitives.TXIDHexString(txid),
			Status: wdk.ReviewActionResultStatusSuccess,
		}, nil
	case sendClaimInFlight:
		// A live claim elsewhere owns this send: not this caller's error, and
		// nothing was POSTed. It still gets a review entry, because a non-delayed
		// batch keeps the invariant that every SendWithResult which is not
		// 'unproven' has one explaining itself.
		//
		// That invariant is what the wallet-side validator's error is built from
		// (see validate.NotDelayedProcessActionResult, which fails a batch whose
		// send results are not all unproven, and pkgerrors.ProcessActionError,
		// which carries BOTH lists to the caller). A mixed batch — one tx
		// accepted, one in flight elsewhere — fails that validation either way;
		// the entry is what stops it failing while saying nothing about the
		// transaction that caused it.
		//
		// serviceError is the established status for "handed off, fate not yet
		// known", which the backpressure and transport arms below already use;
		// the message is what distinguishes this cause from those.
		swr.Status = wdk.SendWithResultStatusSending
		return swr, &wdk.ReviewActionResult{
			TxID:   primitives.TXIDHexString(txid),
			Status: wdk.ReviewActionResultStatusServiceError,
			Errors: wdk.ReviewActionErrors{
				"storage": errors.New("a concurrent broadcast attempt holds this transaction; " +
					"it is in flight and will be re-driven by the send sweep if that attempt dies"),
			},
		}, nil
	case sendClaimOwned:
		// The row is ours and now reads 'sending'. Fall through to the POST.
	}

	res, berr := p.broadcastWithBackpressure(ctx, txid, ef)
	switch {
	case berr != nil && isBackpressure(berr):
		// Exhausted the single retry: leave the tx in-flight, report service
		// error. "In-flight" is now literal — the row stays at the 'sending' this
		// call's claim put it in, which is exactly where FindResendable's graced
		// arm looks for stranded sends. The row therefore backs off one full
		// grace and is then re-driven, instead of being retried on every tick.
		swr.Status = wdk.SendWithResultStatusSending
		return swr, &wdk.ReviewActionResult{
			TxID:   primitives.TXIDHexString(txid),
			Status: wdk.ReviewActionResultStatusServiceError,
			Errors: wdk.ReviewActionErrors{"arcade": berr},
		}, nil
	case berr != nil:
		// Opaque/transport failure: unknown fate; report service error, keep
		// in-flight at 'sending' for the graced recovery arm (as above).
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
	return p.commitAccepted(ctx, txid, tx)
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
// transition statuses, record the input spend-history.
//
// It does not resolve the transaction row itself. It used to, USER-SCOPED, and
// that was half of audit P1-3: a SendWith carrying another user's txid found no
// row under the caller and silently committed the statuses without the spend.
// [Provider.applyAcceptedBroadcast] owns the resolution now, and does it across
// users.
func (p *Provider) commitAccepted(ctx context.Context, txid string, tx *transaction.Transaction) (wdk.SendWithResult, *wdk.ReviewActionResult, error) {
	swr := wdk.SendWithResult{TxID: primitives.TXIDHexString(txid), Status: wdk.SendWithResultStatusUnproven}
	rar := &wdk.ReviewActionResult{TxID: primitives.TXIDHexString(txid), Status: wdk.ReviewActionResultStatusSuccess}

	if err := p.applyAcceptedBroadcast(ctx, txid, tx); err != nil {
		return swr, nil, err
	}
	return swr, rar, nil
}

// applyAcceptedBroadcast commits the wallet-state transition for a
// broadcast-accepted transaction: reserved inputs → spent (as a FACT), the
// transaction rows → unproven, the known tx → unconfirmed, and the local input
// spend-history recorded. It is shared by the synchronous broadcast path
// ([Provider.commitAccepted]) and the monitor's SendWaiting sweep
// ([Provider.sendOneWaiting]); both hand it only a txid and the parsed bytes.
//
// It resolves its OWN transaction row, CROSS-USER
// ([metastore.TransactionsRepo.FindByTxIDAllUsers]), because the monitor works
// by txid and a SendWith may carry a txid whose row belongs to another user.
// Two failures that used to be swallowed are now refusals:
//
//   - a query ERROR propagates. "The row could not be read" and "there is no
//     row" are different facts and only one of them is safe to act on.
//   - ZERO rows is a HARD error, raised BEFORE any status write. The old code
//     advanced the statuses anyway (stamping was_broadcast sticky) and skipped
//     the spend entirely, leaving the inputs reserved-and-unspent while the row
//     moved past the broadcast stage where no sweep re-drives it — audit P1-3,
//     a permanently lost spend. There is no legitimate flow that broadcasts
//     bytes with no transaction row behind them, so this is corruption, and
//     refusing keeps the known tx at 'sending' for the graced re-drive.
//
// ORDER INSIDE THE COMMIT: the fact-spend runs FIRST, then the statuses, then
// the spend-history. In Mode A everything is one transaction and the order is
// immaterial. In Mode B the utxostore is a different database and its write
// commits when the statement runs, so the order picks which half a crash can
// strand — and only one direction converges:
//
//   - spend first: a crash before the status commit leaves the known tx at
//     'sending', which is exactly [metastore.KnownTxRepo.FindResendable]'s
//     graced recovery arm. The re-drive re-POSTs (arcade is idempotent by
//     txid) and re-applies; the fact-mode spend of the SAME spender is an
//     idempotent success, so the second pass completes the statuses.
//   - statuses first: a crash before the spend leaves the known tx at
//     'unconfirmed' — beyond the broadcast stage, off every work list — with
//     its inputs still claimable. Nothing ever comes back for them.
//
// The same reasoning makes a returned error safe: the metadata half rolls back,
// the row stays at 'sending', and the next sweep tick redoes the whole apply.
func (p *Provider) applyAcceptedBroadcast(ctx context.Context, txid string, tx *transaction.Transaction) error {
	rows, err := p.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	if err != nil {
		return fmt.Errorf("storage: resolve transaction row for accepted broadcast %s: %w", txid, err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("storage: accepted broadcast for %s has no transaction row; "+
			"refusing to commit without recording the spend", txid)
	}
	// A txid can back several rows — a self-payment is recorded by BOTH sides —
	// and only the SPENDER's row carries the reservation that funded it and the
	// transaction-id the spend-history hangs off. Prefer the outgoing row; the
	// newest (rows are ordered transaction_id DESC) is the fallback when nothing
	// is marked outgoing.
	//
	// Fact mode already makes the reservation half forgiving — it is no longer a
	// guard there, only a non-empty programmer-error check — but markInputsSpent
	// is NOT: it looks the input rows up scoped to txRow.UserID, so picking the
	// recipient's row would silently record no spend-history at all.
	txRow := &rows[0]
	for i := range rows {
		if rows[i].IsOutgoing {
			txRow = &rows[i]
			break
		}
	}

	return p.meta.Do(ctx, func(ctx context.Context) error {
		// SPEND FIRST — see the ordering note above.
		if err := p.spendReservedInputs(ctx, tx, txid, string(txRow.Reference)); err != nil {
			return err
		}
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusUnproven,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: mark unproven: %w", err)
		}
		// The skip set stays the beyond-broadcast one and must: the row is at
		// 'sending' when this runs (the broadcast gate put it there), and
		// 'sending' is deliberately NOT in that set, so the advance applies. The
		// wider [metastore.KnownTxNeverRequeueStatuses] is the guard for BACKWARD,
		// requeue-shaped writes and would block this forward one forever.
		if err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnconfirmed,
			wdk.ProvenTxReqBeyondBroadcastStageStatuses...); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) && !errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: mark known tx unconfirmed: %w", err)
		}
		// Change stays at TierSending here and is NOT promoted to claimable on
		// the 202. A 202 is "accepted for processing", not validated: the tx can
		// still fail validation, and until arcade has SEEN it a child that spends
		// this change can reach arcade's validation before the parent is in its
		// mempool — an unknown-input rejection that cascades down a self-payment
		// chain. Promotion to TierUnproven happens only on the actual SEEN status
		// ([Provider.applySeen] / the poll fallback), or directly to TierMined on
		// proof — so a coin is spendable only once the network has accepted the
		// tx that created it.
		return p.markInputsSpent(ctx, txRow.UserID, txRow.TransactionID, tx)
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

	// Record the reason before anything else — including before the row lookup
	// below, which can fail: the 4xx body is the only statement of cause that
	// will ever exist for this transaction, and arcade may have forgotten the
	// transaction by the time anyone asks it again.
	p.logRejection(ctx, txid, string(res.Status), res.ExtraInfo, "broadcast_4xx")

	txRow, terr := p.firstTxByTxID(ctx, userID, txid)
	if terr != nil {
		return swr, nil, terr
	}

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

// spendReservedInputs RECORDS, in FACT MODE, that an accepted broadcast has
// spent this transaction's inputs.
//
// It runs only after arcade has accepted the bytes, and that is what selects
// the mode. A guarded spend asks "is this coin still reserved by me?", and the
// honest answer no longer matters: the network has the transaction, so no local
// row state can un-happen the spend. Fact mode
// ([utxostore.Store.Spend] with force=true) therefore drops the reservation and
// freeze guards and keeps exactly one arbiter — spent_by — because two spend
// facts cannot both hold.
//
// The guarded call this replaces is audit P0-2's amplifier. It had to tolerate
// [utxostore.ReservedError] with HeldBy == "" to work at all, and that error
// does NOT mean "external input": it means the row IS in the inventory and is
// currently CLAIMABLE — a reservation lost to a release race. Skipping it left
// the accepted transaction's own input free for the funder to lend to the next
// action, which is how one lost reservation became a double spend the wallet
// authored itself. Fact mode makes that state a recorded spend instead.
//
// The tolerance collapses to a single case:
//
//   - [utxostore.NotFoundError] — no row at all, so the input is genuinely
//     external (somebody else's coin funding this transaction). The ONLY skip,
//     and only when the top-level error is [utxostore.ErrBatch], which is the
//     store's promise that the per-item verdicts are the WHOLE story. A
//     top-level error of any other kind is never explained away by them: the
//     batch may have written nothing (see the switch below).
//   - [utxostore.ErrContention] — the row would not hold still under the
//     backend's bounded retries. Transient, and NOT a double-spend alert:
//     returned as-is so the caller's re-drive retries the whole apply (see
//     [Provider.applyAcceptedBroadcast] on why the known tx is still 'sending'
//     when that happens). Both error SHAPES are handled: a per-item verdict
//     (every backend's Spend reports it on SpendOp.Err) and a whole-call
//     failure carrying the sentinel directly.
//   - [utxostore.SpentError] — a DIFFERENT transaction already spent this coin
//     and the network has now accepted ours too. That is a materialized double
//     spend, the one thing this function exists to make visible: it logs at
//     ERROR and fails hard.
//   - anything else — hard error.
//
// A SECOND COPY OF THIS CLASSIFICATION EXISTS, deliberately:
// [Provider.factSpendMinedInputs] records the same fact for a mined transaction
// the wallet had written off. The two agree on every verdict except
// [utxostore.SpentError], where they are OPPOSITE — fatal here, an alert it
// continues past there — because here the caller can still refuse and there the
// transaction is already on chain with nothing left to refuse. That single
// divergence is why they were not folded into one helper. HARDEN BOTH OR
// NEITHER: a new verdict, a new tolerance, or a change to the ErrBatch
// reasoning belongs in both places, and a fix applied to only one of them
// leaves the other silently wrong.
func (p *Provider) spendReservedInputs(ctx context.Context, tx *transaction.Transaction, txid, reference string) error {
	// Fact mode still refuses an empty reservation per item, in every backend:
	// it is a programmer error, not a row state. Every caller resolves the
	// reference from a transaction row this path has already proven exists, so
	// an empty one means the row itself is malformed — say that once here rather
	// than letting the store say it once per input.
	if reference == "" {
		return fmt.Errorf("storage: accepted broadcast %s: transaction row carries no reference; "+
			"refusing to record its spend", txid)
	}
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

	serr := p.utxo.Spend(ctx, ops, true)
	if serr == nil {
		return nil
	}

	// Walk every item before deciding, so each materialized double spend gets
	// its own ERROR line rather than only the first one being visible.
	var (
		failed    int   // items that reported an error
		tolerated int   // …of which were NotFound, the only skippable one
		fatal     error // first hard verdict; outranks contention
		contended error // first retryable verdict
	)
	for _, op := range ops {
		if op.Err == nil {
			continue
		}
		failed++
		var spent *utxostore.SpentError
		switch {
		case errors.Is(op.Err, &utxostore.NotFoundError{}):
			tolerated++ // genuinely external input: the only skip fact mode allows
		case errors.As(op.Err, &spent):
			// THE ALERT BOUNDARY. Two transactions the network accepted spend the
			// same coin. Nothing local can repair that, so it is said out loud and
			// the apply fails rather than overwriting the recorded winner.
			// Both the log and the error name the store's OWN report of the
			// outpoint and winner, not our re-derivation of them.
			p.logger.ErrorContext(ctx, "double spend: an accepted broadcast spends a coin another transaction already spent",
				slog.String("txid", txid),
				slog.String("outpoint", spent.Op.String()),
				slog.String("winner", spent.Winner.String()))
			if fatal == nil {
				fatal = fmt.Errorf("storage: accepted broadcast %s spends %s, already spent by %s: %w",
					txid, spent.Op, spent.Winner, op.Err)
			}
		case errors.Is(op.Err, utxostore.ErrContention):
			if contended == nil {
				contended = fmt.Errorf("storage: record spend of %s for accepted broadcast %s: %w",
					op.Outpoint, txid, op.Err)
			}
		default:
			if fatal == nil {
				fatal = fmt.Errorf("storage: record spend of %s for accepted broadcast %s: %w",
					op.Outpoint, txid, op.Err)
			}
		}
	}
	switch {
	case fatal != nil:
		return fatal
	case contended != nil:
		return contended
	case errors.Is(serr, utxostore.ErrBatch) && failed > 0 && failed == tolerated:
		// The ONLY tolerable failure: the store reported per-item verdicts
		// ([utxostore.ErrBatch] is its promise that the top-level error is exactly
		// the sum of them) and every one was an external input. Everything the
		// call could write, it wrote.
		return nil
	default:
		// Anything else means the top-level error is NOT explained by the items,
		// so the batch may have written nothing at all — sqlstore runs its items
		// inside one transaction, and a driver error on a LATER item rolls back
		// the earlier ones while their per-item Err survives in memory. Reading
		// that residue as "a couple of external inputs, carry on" would commit
		// the statuses over a spend the store rolled back: reserved-and-unspent
		// inputs under a row moved past the broadcast stage, which no sweep
		// re-drives. Wrapping keeps the cause — including
		// [utxostore.ErrContention] — matchable by the caller.
		return fmt.Errorf("storage: record spends for accepted broadcast %s: %w", txid, serr)
	}
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
//
// The input rows are resolved in ONE batched lookup rather than a keyed query
// per input: this runs inside the broadcast-accept commit, for every accepted
// transaction, so the round trips it used to spend scaled with the input count
// of every send.
func (p *Provider) markInputsSpent(ctx context.Context, userID int, spendingTxID uint, tx *transaction.Transaction) error {
	ops := make([]wdk.OutPoint, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		ops = append(ops, wdk.OutPoint{TxID: in.SourceTXID.String(), Vout: in.SourceTxOutIndex})
	}
	rows, err := p.localOutputsByOutpoint(ctx, userID, ops)
	if err != nil {
		return err
	}
	// Walk the inputs, not the result: an input with no local row is skipped
	// (it is somebody else's coin), and the id order stays the input order.
	var ids []uint
	for _, op := range ops {
		if row := rows[op]; row != nil {
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

// firstTxByTxID returns the newest transaction row for txid under userID, or
// (nil, nil) when the user genuinely has none.
//
// The query error is RETURNED, never folded into the nil row. It used to be
// swallowed, which made "this user has no row for that txid" and "the database
// did not answer" the same value — and the sole remaining caller,
// [Provider.commitRejected], reads that value as "there is no change to
// remove". A blip would therefore leave the phantom change of a finally
// rejected transaction live and selectable, with no trace that anything was
// skipped.
func (p *Provider) firstTxByTxID(ctx context.Context, userID int, txid string) (*wdk.TableTransaction, error) {
	rows, err := p.meta.Transactions().FindByTxID(ctx, userID, txid)
	if err != nil {
		return nil, fmt.Errorf("storage: resolve transaction row for %s: %w", txid, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
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
