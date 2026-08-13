package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// This file is the reject→release reconciler (Task 19): the automatic, verified
// loop that replaces the old manual `unfail`. Phase 1 (ApplyStatusUpdate, done
// elsewhere) marks an async-rejected tx suspectFailed WITHOUT releasing its
// inputs; this file is Phase 2 — a leased pass that re-verifies each suspect
// against arcade and, ONLY when a tx is provably dead, releases its inputs back
// to the claimable pool. Under the arcade async model a rejection can be a
// transient false positive, so releasing eagerly risks resurrecting a still-live
// input into a real double spend; the whole design is built around never doing
// that.
//
// Safety cores:
//
//   - Two-pass false-positive guard (pure REJECTED): a suspect is released only
//     after GetTx has returned REJECTED on TWO passes separated by the grace
//     window (verified_rejected_at stamped on the first, checked on the second).
//     A transient REJECTED that recovers in between is caught by the recovered
//     branch and never released. "Pure" means ExtraInfo is NOT a spend-conflict
//     class (fee/script/policy) — every input is assumed still free on chain.
//   - Spend-conflict REJECTED (UTXO_SPENT / TX_CONFLICTING / CompetingTxs / ARC
//     466): same two-pass, but release is PARTIAL — winner-union style. Inputs
//     Arcade asserts already spent (ExtraInfo outpoints, or inputs of confirmed
//     competing spenders) stay spent; only the residual inputs are returned to
//     the pool. If conflict is detected but no spent set can be proven, release
//     is deferred (safe bias → stuck), never "free everything".
//   - Double-spend winner rule (DOUBLE_SPEND_ATTEMPTED): inputs are released only
//     once some competing tx is itself terminal-successful (MINED/IMMUTABLE) — i.e.
//     our inputs are provably consumed by the winner. Inputs the winner consumed
//     are left recorded as spent (out of the pool); only the inputs the winner did
//     NOT take are released.
//   - No-double-release: the terminal status flip is a positive CAS
//     (suspectFailed → invalidTx/doubleSpend) so a crash re-run or a racing pass
//     is refused; the release ops themselves are idempotent (Unspend's
//     spent_by==txid guard, ReleaseOutpoints' reservation guard, RemoveByMintTx's
//     already-gone skip), so even if two passes both got past the gate no input is
//     released twice — and critically, an input another tx has since reclaimed is
//     never stomped (Unspend's guard no longer matches it).
//
// Mode A (shared DB) applies the release atomically in one metastore.Do. Mode B
// (split stores) writes the terminal statuses + enqueues the utxo ops to the
// crash-recovery outbox in one Do, then inline-executes them; a crash between the
// two is healed by DrainOutbox, which replays the pending rows idempotently.

const (
	// outbox op_type discriminators for the release path.
	opUnspend       = "UNSPEND"        // release inputs recorded spent_by the dead tx
	opReleaseResv   = "RELEASE"        // release inputs still reserved by the dead tx
	opRemovePhantom = "REMOVE_PHANTOM" // remove inputs whose source tx is itself dead
	opRemoveMinted  = "REMOVE_MINTED"  // remove the dead tx's phantom change + cascade
)

// errAlreadyHandled is returned internally by the release gate when the suspect
// is no longer suspectFailed (a concurrent pass or crash re-run already
// finalized it). It rolls back the gate's (empty) transaction and signals the
// caller to treat the release as a no-op — the no-double-release short-circuit.
var errAlreadyHandled = errors.New("storage: suspect already finalized")

// txStatusesToFail are the pre-terminal transaction statuses a release CAS-flips
// to failed. A suspect whose transaction is already failed (the synchronous 4xx
// path did that) is tolerated as an idempotent skip.
var txStatusesToFail = []wdk.TxStatus{
	wdk.TxStatusUnproven, wdk.TxStatusSending, wdk.TxStatusUnprocessed, wdk.TxStatusNoSend,
}

// VerifyAndReleaseSuspects is the reconciler pass. It scans up to limit suspect-
// failed known txs past the grace window (oldest first), re-verifies each against
// the arcade oracle, and releases the inputs of the provably-dead ones. grace is
// both the minimum age before a suspect is considered and the minimum separation
// between the two rejected-verification passes; maxQuarantine is the age past
// which an unresolved suspect is escalated to the terminal stuck state (never
// auto-released). It returns a [defs.ReconcilerReport] of the pass's outcomes.
//
// It is idempotent and safe to re-run (a crashed pass re-runs cleanly), and is
// meant to be driven by the monitor's leased reject_release task (exactly one
// instance at a time).
func (p *Provider) VerifyAndReleaseSuspects(ctx context.Context, grace, maxQuarantine time.Duration, limit int) (defs.ReconcilerReport, error) {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	var report defs.ReconcilerReport
	if p.oracle == nil {
		return report, fmt.Errorf("storage: reconciler requires an arcade oracle")
	}

	suspects, err := p.meta.KnownTx().FindSuspectFailed(ctx, grace, limit)
	if err != nil {
		return report, fmt.Errorf("storage: find suspect failed: %w", err)
	}
	for i := range suspects {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.Scanned++
		sub, serr := p.reconcileSuspect(ctx, &suspects[i], grace, maxQuarantine)
		if serr != nil {
			// One suspect's failure never aborts the pass; the next pass retries it.
			p.logger.WarnContext(ctx, "reconciler: suspect reconcile failed",
				slog.String("txid", suspects[i].TxID), slog.String("error", serr.Error()))
			continue
		}
		report.Add(sub)
	}
	return report, nil
}

// reconcileSuspect re-verifies one suspect and branches per the design. It
// returns a single-suspect report (the counters it incremented). Every branch
// builds its report by value, so there is no shared accumulator to alias.
func (p *Provider) reconcileSuspect(ctx context.Context, kt *metastore.KnownTx, grace, maxQuarantine time.Duration) (defs.ReconcilerReport, error) {
	txid := kt.TxID

	rec, err := p.oracle.GetTx(ctx, txid)
	switch {
	case errors.Is(err, arcade.ErrTxNotFound):
		// Arcade has no verdict: indistinguishable from propagation lag or a
		// genuinely-gone tx. SAFE: leave suspect; escalate only past quarantine.
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "not_found")
	case err != nil:
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "getx_error")
	case rec == nil:
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "nil_record")
	}
	if rec.TxID == "" {
		rec.TxID = txid
	}

	switch {
	case isRecovered(rec.Status):
		return p.handleRecovered(ctx, kt, rec)
	case rec.Status == arcade.StatusRejected:
		return p.handleRejected(ctx, kt, rec, grace, maxQuarantine)
	case rec.Status == arcade.StatusDoubleSpendAttempted:
		return p.handleDoubleSpend(ctx, kt, rec, maxQuarantine)
	default:
		// PENDING_RETRY / RECEIVED / SENT_TO_NETWORK / STUMP_PROCESSING / UNKNOWN /
		// any future non-terminal status: still ambiguous, re-check next pass.
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, string(rec.Status))
	}
}

// isRecovered reports whether an arcade status means the suspect actually made
// it onto the network after all (a false positive) — a broadcast-accepted or
// mined state.
func isRecovered(s arcade.Status) bool {
	switch s {
	case arcade.StatusSeenOnNetwork, arcade.StatusSeenMultipleNodes,
		arcade.StatusAcceptedByNetwork, arcade.StatusMined, arcade.StatusImmutable:
		return true
	default:
		return false
	}
}

// handleRecovered clears the quarantine hold and routes the recovered record
// back through the normal lifecycle (ApplyStatusUpdate) — a MINED gets its
// header-verified proof, a SEEN goes unproven. Nothing was ever released.
func (p *Provider) handleRecovered(ctx context.Context, kt *metastore.KnownTx, rec *arcade.TxRecord) (defs.ReconcilerReport, error) {
	// Unfreeze BEFORE routing: applySeen/applyMined only Promote the change
	// (which leaves a frozen coin frozen and invisible to claims), so the hold
	// must be lifted here for the recovered change to become spendable again.
	p.unfreezeChange(ctx, kt.TxID)
	if err := p.ApplyStatusUpdate(ctx, *rec); err != nil {
		return defs.ReconcilerReport{}, fmt.Errorf("storage: reconciler route recovered %s: %w", kt.TxID, err)
	}
	p.logger.InfoContext(ctx, "reconciler: suspect recovered (false positive), no release",
		slog.String("txid", kt.TxID), slog.String("status", string(rec.Status)))
	return defs.ReconcilerReport{FalsePositive: 1}, nil
}

// handleRejected runs the two-pass false-positive guard for a still-REJECTED
// suspect. Pass 1 (no prior stamp) records verified_rejected_at and freezes the
// change; pass 2 (stamp older than grace) releases. In between it stays suspect.
//
// On pass 2 the release set depends on whether rec is a spend-conflict class
// (UTXO_SPENT / CompetingTxs / …): pure rejects free every funding input;
// conflicts free only inputs not proven already spent (winner-union style).
func (p *Provider) handleRejected(ctx context.Context, kt *metastore.KnownTx, rec *arcade.TxRecord, grace, maxQuarantine time.Duration) (defs.ReconcilerReport, error) {
	switch {
	case kt.VerifiedRejectedAt == nil:
		// Pass 1: first authoritative REJECTED re-verification.
		if err := p.meta.KnownTx().StampVerifiedRejected(ctx, kt.TxID, p.now()); err != nil {
			return defs.ReconcilerReport{}, err
		}
		p.freezeChange(ctx, kt.TxID)
		p.logger.DebugContext(ctx, "reconciler: rejected pass 1, stamped verified_rejected_at",
			slog.String("txid", kt.TxID),
			slog.Bool("spendConflict", isSpendConflictRecord(rec)))
		return defs.ReconcilerReport{Ambiguous: 1}, nil
	case p.now().Sub(*kt.VerifiedRejectedAt) >= grace:
		// Pass 2: still rejected after the grace separation — provably dead
		// (or spend-conflict with a proven residual release set).
		if isSpendConflictRecord(rec) {
			return p.releaseSpendConflict(ctx, kt, rec, maxQuarantine, "rejected_spend_conflict")
		}
		inputs, err := p.txInputs(kt)
		if err != nil {
			return defs.ReconcilerReport{}, err
		}
		plan := p.buildReleasePlan(ctx, kt, wdk.ProvenTxStatusInvalid, inputs)
		cascaded, released, err := p.releaseVerifiedDead(ctx, plan)
		if err != nil {
			return defs.ReconcilerReport{}, err
		}
		if !released {
			return defs.ReconcilerReport{}, nil // already finalized (no-double-release)
		}
		p.logger.InfoContext(ctx, "reconciler: released provably-dead rejected tx",
			slog.String("txid", kt.TxID), slog.Int("inputs", len(inputs)), slog.Int("cascaded", cascaded))
		return defs.ReconcilerReport{Released: 1, Cascaded: cascaded}, nil
	default:
		// Stamped, but the two passes are not yet grace apart: wait.
		p.freezeChange(ctx, kt.TxID)
		return defs.ReconcilerReport{Ambiguous: 1}, nil
	}
}

// handleDoubleSpend applies the winner rule for a DOUBLE_SPEND_ATTEMPTED suspect.
// It releases inputs only once some competing tx is provably terminal-successful,
// and then releases only the inputs the winner did NOT consume. With no winner
// yet it stays suspect (escalating past quarantine).
func (p *Provider) handleDoubleSpend(ctx context.Context, kt *metastore.KnownTx, rec *arcade.TxRecord, maxQuarantine time.Duration) (defs.ReconcilerReport, error) {
	return p.releaseSpendConflict(ctx, kt, rec, maxQuarantine, "double_spend_no_winner")
}

// releaseSpendConflict is the shared partial-release path for DOUBLE_SPEND_ATTEMPTED
// and for REJECTED records that carry spend-conflict evidence (ExtraInfo UTXO_SPENT,
// CompetingTxs, ARC 466, …). It unions:
//
//  1. outpoints Arcade's ExtraInfo asserts are already spent (trust the oracle
//     text — does not require the spender to be MINED in our view), and
//  2. inputs of every CompetingTxs / ExtraInfo spender that is itself
//     terminal-successful with a readable rawTx (classic winner-union).
//
// Only inputs in neither set are released. If the conflict class is known but
// neither source yields a non-empty spent set, release is deferred (safe bias).
// A confirmed winner with unreadable rawTx also defers (cannot prove residual
// inputs safe), matching the classic double-spend unreadable-winner rule.
func (p *Provider) releaseSpendConflict(ctx context.Context, kt *metastore.KnownTx, rec *arcade.TxRecord, maxQuarantine time.Duration, deferReason string) (defs.ReconcilerReport, error) {
	extraInfo := ""
	var competing []string
	if rec != nil {
		extraInfo = rec.ExtraInfo
		competing = rec.CompetingTxs
	}
	// Prefer known_tx stored competitors when the live record omits them
	// (status apply may have persisted them on first reject).
	if len(competing) == 0 && len(kt.CompetingTxs) > 0 {
		competing = kt.CompetingTxs
	}
	extra := parseSpendConflictExtra(extraInfo)
	spenders := mergeSpenders(competing, extra.Spenders)

	spent := make(map[utxostore.Outpoint]struct{}, len(extra.Spent)+8)
	for op := range extra.Spent {
		spent[op] = struct{}{}
	}

	// Confirmed-but-unreadable winner: never free residuals (consumption set
	// unknown). Applies even when ExtraInfo named some spent outpoints — another
	// of our inputs might still belong to the unreadable winner.
	if p.hasConfirmedUnreadableWinner(ctx, spenders) {
		p.freezeChange(ctx, kt.TxID)
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "double_spend_unreadable_winner")
	}

	winnerResolved := false
	if winnerInputs, ok := p.findWinnerInputs(ctx, spenders); ok {
		winnerResolved = true
		for op := range winnerInputs {
			spent[op] = struct{}{}
		}
	}
	// findWinnerInputs only accepts a MINED/IMMUTABLE winner, but in practice the
	// winner is one of OUR OWN earlier txs that is merely SEEN — measured on the
	// scale cluster, 8 of 8 sampled UTXO_SPENT rejections named a spender already
	// in the local ledger, created before the loser. Its raw tx is therefore on
	// hand, so resolve the consumption set locally: no oracle round-trip, no
	// confirmation wait, and it is strictly hold-biased (it can only ADD outpoints
	// to the spent set, never release one). Without this the winner stays
	// unresolved in the common case and Guard 2 below would defer every conflict.
	if localInputs, ok := p.localWinnerInputs(ctx, spenders); ok {
		winnerResolved = true
		for op := range localInputs {
			spent[op] = struct{}{}
		}
	}

	// Nothing proven spent yet (competitors still SEEN, ExtraInfo had no
	// outpoints): do NOT fall back to release-all.
	if len(spent) == 0 {
		p.freezeChange(ctx, kt.TxID)
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, deferReason)
	}

	allInputs, err := p.txInputs(kt)
	if err != nil {
		return defs.ReconcilerReport{}, err
	}
	releaseInputs, held := filterReleaseInputs(allInputs, spent)

	// Guard 1 — the spent set must intersect OUR inputs before it authorizes
	// anything. len(spent) > 0 only means "we parsed an outpoint somewhere in
	// ExtraInfo", not "we hold an outpoint that is provably gone". Arcade can
	// legitimately attach a conflict line naming an outpoint this tx never spent:
	// for a size-1 propagation batch it assigns the best unattributable ("alien")
	// line to the only tx in the batch WITHOUT verifying the outpoint belongs to
	// it (arcade services/propagation/propagator.go, the len(batch)==1 backstop,
	// whose own comment flags it as unverified), and every Teranode conflict line
	// additionally names the competing SPENDER txid. A foreign outpoint therefore
	// passes the len(spent) check, intersects nothing, and every input is released
	// — reinstating the exact release-all this function exists to prevent. Hold
	// nothing of ours ⇒ we have learned nothing about our inputs ⇒ defer.
	if held == 0 {
		p.logger.InfoContext(ctx, "reconciler: spend conflict names no outpoint of this tx; deferring",
			slog.String("txid", kt.TxID),
			slog.Int("parsedSpent", len(spent)),
			slog.Int("inputs", len(allInputs)))
		p.freezeChange(ctx, kt.TxID)
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "spend_conflict_foreign_outpoint")
	}

	// Guard 2 — releasing the RESIDUAL inputs (the ones the conflict did not
	// name) claims they are still ours. That is only knowable from the winner's
	// consumption set: the winner may have spent several of our inputs while
	// ExtraInfo named just one. ExtraInfo text alone does not license it.
	// hasConfirmedUnreadableWinner above only catches a MINED/IMMUTABLE winner we
	// cannot parse; a winner that is merely unresolvable — still SEEN, or GetTx
	// errored (both swallowed by findWinnerInputs' `continue`) — reaches here with
	// winnerResolved false. In that state hold everything and retry later rather
	// than free inputs the winner may already own. When nothing is released there
	// is no residual to get wrong, so this only guards the partial-release case.
	if !winnerResolved && len(releaseInputs) > 0 {
		p.logger.InfoContext(ctx, "reconciler: spend conflict winner unresolved; holding residual inputs",
			slog.String("txid", kt.TxID),
			slog.Int("heldSpent", held),
			slog.Int("wouldRelease", len(releaseInputs)),
			slog.Int("spenders", len(spenders)))
		p.freezeChange(ctx, kt.TxID)
		return p.leaveOrEscalate(ctx, kt, maxQuarantine, "spend_conflict_unresolved_winner")
	}

	plan := p.buildReleasePlan(ctx, kt, wdk.ProvenTxStatusDoubleSpend, releaseInputs)
	cascaded, released, err := p.releaseVerifiedDead(ctx, plan)
	if err != nil {
		return defs.ReconcilerReport{}, err
	}
	if !released {
		return defs.ReconcilerReport{}, nil // already finalized (no-double-release)
	}
	p.logger.InfoContext(ctx, "reconciler: partial release after spend conflict",
		slog.String("txid", kt.TxID),
		slog.Int("releasedInputs", len(releaseInputs)),
		slog.Int("heldSpent", held),
		slog.Int("cascaded", cascaded),
		slog.Int("spenders", len(spenders)),
		slog.Int("extraSpent", len(extra.Spent)))
	return defs.ReconcilerReport{Released: 1, Cascaded: cascaded}, nil
}

// hasConfirmedUnreadableWinner reports whether any competing tx is
// MINED/IMMUTABLE but lacks a parseable rawTx — the case that forces a full
// release defer under the winner-union safety rule.
// localWinnerInputs resolves a competing spender's consumption set from the
// LOCAL ledger. The spender in a Teranode UTXO_SPENT line is very often one of
// our own earlier transactions (the wallet re-spent an outpoint it had already
// consumed), so its raw bytes are already in known_txs and no oracle call is
// needed. Unlike findWinnerInputs this does not require the winner to be
// MINED/IMMUTABLE: a SEEN winner has still taken the outpoint as far as the
// node's UTXO set is concerned, which is exactly what the reject told us.
//
// It is safe in the only direction that matters: every outpoint it returns is
// ADDED to the spent set, so it can only ever cause MORE inputs to be held, and
// a miss simply leaves the winner unresolved for the caller's guard to handle.
// Errors and unparseable bodies are treated as "not resolved" for the same
// reason. Returns ok=true only when at least one spender was genuinely read.
func (p *Provider) localWinnerInputs(ctx context.Context, competingTxs []string) (map[utxostore.Outpoint]struct{}, bool) {
	consumed := make(map[utxostore.Outpoint]struct{})
	anyWinner := false
	for _, competitor := range competingTxs {
		if competitor == "" {
			continue
		}
		kt, found, err := p.meta.KnownTx().FindByTxID(ctx, competitor)
		if err != nil || !found || kt == nil || len(kt.RawTx) == 0 {
			continue
		}
		wtx, perr := transaction.NewTransactionFromBytes(kt.RawTx)
		if perr != nil {
			p.logger.DebugContext(ctx, "reconciler: local winner rawTx unparseable",
				slog.String("winner", competitor), slog.String("error", perr.Error()))
			continue
		}
		anyWinner = true
		for _, in := range wtx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			consumed[utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex}] = struct{}{}
		}
	}
	return consumed, anyWinner
}

func (p *Provider) hasConfirmedUnreadableWinner(ctx context.Context, competingTxs []string) bool {
	for _, competitor := range competingTxs {
		if competitor == "" {
			continue
		}
		crec, err := p.oracle.GetTx(ctx, competitor)
		if err != nil || crec == nil {
			continue
		}
		if crec.Status != arcade.StatusMined && crec.Status != arcade.StatusImmutable {
			continue
		}
		if len(crec.RawTx) == 0 {
			return true
		}
		if _, perr := transaction.NewTransactionFromBytes(crec.RawTx); perr != nil {
			return true
		}
	}
	return false
}

// leaveOrEscalate leaves an unresolved suspect for the next pass, unless it has
// sat past maxQuarantine, in which case it is escalated to the terminal stuck
// state (operator-visible, never auto-released).
func (p *Provider) leaveOrEscalate(ctx context.Context, kt *metastore.KnownTx, maxQuarantine time.Duration, reason string) (defs.ReconcilerReport, error) {
	if kt.SuspectSince != nil && p.now().Sub(*kt.SuspectSince) >= maxQuarantine {
		if err := p.escalateStuck(ctx, kt.TxID); err != nil {
			if errors.Is(err, metastore.ErrStatusUpdateSkipped) || errors.Is(err, metastore.ErrNotFound) {
				return defs.ReconcilerReport{}, nil // already finalized/vanished
			}
			return defs.ReconcilerReport{}, err
		}
		p.logger.WarnContext(ctx, "reconciler: suspect escalated to stuck (max quarantine, never auto-released)",
			slog.String("txid", kt.TxID), slog.String("reason", reason))
		return defs.ReconcilerReport{Stuck: 1}, nil
	}
	p.logger.DebugContext(ctx, "reconciler: suspect left in quarantine",
		slog.String("txid", kt.TxID), slog.String("reason", reason))
	return defs.ReconcilerReport{Ambiguous: 1}, nil
}

// failTransactions CAS-flips a tx's pre-terminal transaction rows to failed,
// tolerating an already-failed row (the synchronous 4xx path already failed it)
// as a benign skip. Shared by the release finalizer and the stuck escalation.
func (p *Provider) failTransactions(ctx context.Context, txid string) error {
	if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusFailed, txStatusesToFail...); err != nil &&
		!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
		return fmt.Errorf("storage: mark tx failed %s: %w", txid, err)
	}
	return nil
}

// escalateStuck flips a suspect to the terminal stuck known-tx status and fails
// its transaction rows. It NEVER touches the inputs.
func (p *Provider) escalateStuck(ctx context.Context, txid string) error {
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.KnownTx().TransitionSuspect(ctx, txid, metastore.KnownTxStatusStuck); err != nil {
			return err
		}
		return p.failTransactions(ctx, txid)
	})
}

// --- release plan ----------------------------------------------------------

// refUser pairs a transaction's reservation token with its owner.
type refUser struct {
	reference string
	userID    int
}

// releasePlan is everything the release path needs, gathered once outside the
// write transaction. The inputs to release are partitioned:
//
//   - realInputs: coins whose SOURCE tx is not (yet) known-dead — returned to
//     the claimable pool (funding coins that genuinely predate the dead tx).
//   - phantomInputs: coins whose SOURCE tx is itself terminal-dead (invalidTx /
//     doubleSpend / stuck) — these coins never existed on-chain, so they are
//     REMOVED from inventory, never resurrected into Claimable. This is the
//     multi-hop-cascade case (a dead parent's change spent by a now-dying child)
//     and restores the old MarkCreatedOutputsAsNotSpendable "remove phantom
//     change" invariant.
type releasePlan struct {
	txid          string
	spendingTxID  chainhash.Hash
	terminalKnown wdk.ProvenTxReqStatus
	refs          []refUser
	realInputs    []utxostore.Outpoint
	phantomInputs []utxostore.Outpoint
	changeOps     []utxostore.Outpoint
}

// buildReleasePlan assembles the plan for a provably-dead suspect: the inputs to
// release (partitioned real vs. phantom), the change to remove, and the
// references whose reservations to lift.
func (p *Provider) buildReleasePlan(ctx context.Context, kt *metastore.KnownTx, terminal wdk.ProvenTxReqStatus, releaseInputs []utxostore.Outpoint) releasePlan {
	plan := releasePlan{txid: kt.TxID, terminalKnown: terminal}
	if h, err := chainhash.NewHashFromHex(kt.TxID); err == nil {
		plan.spendingTxID = *h
	}
	if rows, err := p.meta.Transactions().FindByTxIDAllUsers(ctx, kt.TxID); err == nil {
		for i := range rows {
			plan.refs = append(plan.refs, refUser{reference: string(rows[i].Reference), userID: rows[i].UserID})
		}
	}
	for _, op := range releaseInputs {
		if p.sourceTxDead(ctx, op) {
			plan.phantomInputs = append(plan.phantomInputs, op)
		} else {
			plan.realInputs = append(plan.realInputs, op)
		}
	}
	if ops, err := p.changeOutpointsByTxID(ctx, kt.TxID); err == nil {
		plan.changeOps = ops
	}
	return plan
}

// sourceTxDead reports whether an input's SOURCE transaction is itself a known
// terminal-dead tx (invalidTx / doubleSpend / stuck). Such a coin is phantom —
// created by a tx that provably never confirmed — so on release it must be
// REMOVED from inventory, not returned to the claimable pool. A source that is
// merely still-suspect (not yet terminal) is treated as real for now; if it
// later dies, its own release/cascade cleans up any coins it created.
func (p *Provider) sourceTxDead(ctx context.Context, op utxostore.Outpoint) bool {
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, op.TxID.String())
	if err != nil || !found {
		return false
	}
	return kt.Status.IsTerminalFailure() || kt.Status == metastore.KnownTxStatusStuck
}

// releaseVerifiedDead applies a release plan. It returns the number of descendant
// txs cascaded into the suspect queue and whether the release actually ran (false
// when the suspect was already finalized — the no-double-release short-circuit).
func (p *Provider) releaseVerifiedDead(ctx context.Context, plan releasePlan) (cascaded int, released bool, err error) {
	if p.modeA {
		return p.releaseDirect(ctx, plan)
	}
	return p.releaseViaOutbox(ctx, plan)
}

// releaseDirect (Mode A) flips the statuses and applies every utxo op inside a
// single metastore.Do — atomic across both stores over the shared database.
func (p *Provider) releaseDirect(ctx context.Context, plan releasePlan) (cascaded int, released bool, err error) {
	err = p.meta.Do(ctx, func(ctx context.Context) error {
		if gerr := p.gateAndFinalizeStatuses(ctx, plan); gerr != nil {
			return gerr
		}
		c, aerr := p.applyReleaseOpsDirect(ctx, plan)
		cascaded = c
		return aerr
	})
	if errors.Is(err, errAlreadyHandled) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return cascaded, true, nil
}

// releaseViaOutbox (Mode B) commits the terminal statuses and durably enqueues
// the utxo ops in one Do, then inline-executes them. A crash between the commit
// and the inline execution is healed by DrainOutbox replaying the pending rows.
func (p *Provider) releaseViaOutbox(ctx context.Context, plan releasePlan) (cascaded int, released bool, err error) {
	rawTxid, herr := encodeTxidToRaw(plan.txid)
	if herr != nil {
		return 0, false, herr
	}
	ops := plan.outboxOps()

	// Phase 1: durable intent (statuses + enqueue), atomic on the metadata side.
	err = p.meta.Do(ctx, func(ctx context.Context) error {
		if gerr := p.gateAndFinalizeStatuses(ctx, plan); gerr != nil {
			return gerr
		}
		for _, op := range ops {
			if eerr := p.meta.Outbox().Enqueue(ctx, rawTxid, op.opType, op.chunk, op.payload); eerr != nil {
				return eerr
			}
		}
		return nil
	})
	if errors.Is(err, errAlreadyHandled) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	// Phase 2: inline execute (best-effort; the drain worker heals any that fail).
	_ = p.meta.Do(ctx, func(ctx context.Context) error {
		for _, op := range ops {
			c, xerr := p.executeOutboxOp(ctx, metastore.OutboxEntry{TxID: rawTxid, OpType: op.opType, Chunk: op.chunk, Payload: op.payload})
			if xerr != nil {
				_ = p.meta.Outbox().RecordError(ctx, rawTxid, op.opType, op.chunk, xerr.Error())
				p.logger.WarnContext(ctx, "reconciler: inline outbox op failed, left for drain worker",
					slog.String("txid", plan.txid), slog.String("op", op.opType), slog.String("error", xerr.Error()))
				continue
			}
			cascaded += c
			if derr := p.meta.Outbox().MarkDone(ctx, rawTxid, op.opType, op.chunk); derr != nil {
				return derr
			}
		}
		return nil
	})
	return cascaded, true, nil
}

// gateAndFinalizeStatuses is the no-double-release gate plus the terminal status
// flips. The known-tx CAS (suspectFailed → terminal) is the arbiter: a second
// entry finds a non-suspect row and returns errAlreadyHandled.
func (p *Provider) gateAndFinalizeStatuses(ctx context.Context, plan releasePlan) error {
	if err := p.meta.KnownTx().TransitionSuspect(ctx, plan.txid, plan.terminalKnown); err != nil {
		if errors.Is(err, metastore.ErrStatusUpdateSkipped) || errors.Is(err, metastore.ErrNotFound) {
			return errAlreadyHandled
		}
		return fmt.Errorf("storage: release gate %s: %w", plan.txid, err)
	}
	return p.failTransactions(ctx, plan.txid)
}

// applyReleaseOpsDirect applies the utxo half of a release directly (Mode A, in
// the ambient transaction). Every op is idempotent.
func (p *Provider) applyReleaseOpsDirect(ctx context.Context, plan releasePlan) (cascaded int, err error) {
	if err = p.doUnspend(ctx, plan.spendingTxID, plan.realInputs); err != nil {
		return 0, err
	}
	for _, r := range plan.refs {
		if err = p.doReleaseOutpoints(ctx, r.reference, plan.realInputs); err != nil {
			return 0, err
		}
	}
	if err = p.doRemovePhantom(ctx, plan.phantomInputs); err != nil {
		return 0, err
	}
	return p.doRemoveMinted(ctx, plan.spendingTxID, plan.changeOps)
}

// --- utxo op primitives (shared by direct + outbox execution) --------------

func (p *Provider) doUnspend(ctx context.Context, spendingTxID chainhash.Hash, ops []utxostore.Outpoint) error {
	if len(ops) == 0 {
		return nil
	}
	if _, err := p.utxo.Unspend(ctx, spendingTxID, ops); err != nil {
		return fmt.Errorf("storage: unspend inputs: %w", err)
	}
	// A released input must return FULLY to the claimable pool. When the input
	// was itself a parent suspect's change (the cascade case), the parent's
	// quarantine freeze is still on it — Unspend clears spent+reserved but not the
	// hold — so lift it here (idempotent; a NotFound/not-frozen op is a no-op).
	if err := p.utxo.Unfreeze(ctx, ops); err != nil && !errors.Is(err, &utxostore.NotFoundError{}) {
		p.logger.DebugContext(ctx, "reconciler: unfreeze released inputs (best-effort)",
			slog.String("error", err.Error()))
	}
	return nil
}

func (p *Provider) doReleaseOutpoints(ctx context.Context, reservation string, ops []utxostore.Outpoint) error {
	if reservation == "" || len(ops) == 0 {
		return nil
	}
	if err := p.utxo.ReleaseOutpoints(ctx, reservation, ops); err != nil {
		return fmt.Errorf("storage: release outpoints: %w", err)
	}
	return nil
}

// doRemovePhantom force-removes inputs whose source tx is itself terminal-dead:
// their coins never existed on-chain, so they must leave inventory rather than
// return to the claimable pool. force=true so a currently-spent (spent_by the
// dying tx) or frozen phantom coin is still removed; a missing coin is an
// idempotent no-op.
func (p *Provider) doRemovePhantom(ctx context.Context, ops []utxostore.Outpoint) error {
	if len(ops) == 0 {
		return nil
	}
	if err := p.utxo.Remove(ctx, ops, true); err != nil {
		return fmt.Errorf("storage: remove phantom inputs: %w", err)
	}
	return nil
}

// doRemoveMinted removes the dead tx's phantom change and cascades: any child tx
// that had spent that change is re-marked suspect so the next pass re-verifies
// it (the iterative cascade that converges without a conflicting-children graph).
func (p *Provider) doRemoveMinted(ctx context.Context, mintTxID chainhash.Hash, ops []utxostore.Outpoint) (cascaded int, err error) {
	if len(ops) == 0 {
		return 0, nil
	}
	report, err := p.utxo.RemoveByMintTx(ctx, mintTxID, ops)
	if err != nil {
		return 0, fmt.Errorf("storage: remove minted change: %w", err)
	}
	return p.cascadeChildren(ctx, mintTxID, report.AlreadySpentBy)
}

// cascadeChildren re-marks each child tx (that spent a now-removed phantom
// change coin) suspect, unless it is already terminal or proven — never
// clobbering a completed tx. Returns how many it (re-)marked.
//
// The recorded reason names the dead parent. A cascade is the case where the
// child's OWN rejection reason is least informative — arcade will say the input
// is unknown or the parent was rejected, which explains nothing on its own — so
// the one fact worth keeping is which ancestor actually died.
func (p *Provider) cascadeChildren(ctx context.Context, parent chainhash.Hash, children []chainhash.Hash) (int, error) {
	reason := "cascade: parent " + parent.String() + " is terminally dead"
	n := 0
	for i := range children {
		childTxid := children[i].String()
		kt, found, err := p.meta.KnownTx().FindByTxID(ctx, childTxid)
		if err != nil {
			return n, fmt.Errorf("storage: cascade load child %s: %w", childTxid, err)
		}
		if !found {
			continue // not ours / arcade-only
		}
		switch kt.Status {
		case wdk.ProvenTxStatusCompleted, metastore.KnownTxStatusSuspectFailed, metastore.KnownTxStatusStuck:
			continue // proven, already queued, or already terminal-stuck
		}
		if kt.Status.IsTerminalFailure() {
			continue // already invalidTx/doubleSpend
		}
		if err := p.meta.KnownTx().MarkSuspect(ctx, childTxid, p.now(), nil, "cascade", reason); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return n, fmt.Errorf("storage: cascade mark child suspect %s: %w", childTxid, err)
		}
		n++
	}
	return n, nil
}

// --- outbox executor + drain worker ----------------------------------------

// outboxOp is one enqueued release op, pre-serialized.
type outboxOp struct {
	opType  string
	chunk   int
	payload []byte
}

// outboxOps serializes a plan into its outbox ops (Mode B). Empty sub-ops are
// skipped so the outbox only carries work that exists.
func (plan releasePlan) outboxOps() []outboxOp {
	var ops []outboxOp
	if len(plan.realInputs) > 0 {
		ops = append(ops, outboxOp{opType: opUnspend, payload: mustJSON(unspendPayload{
			SpendingTxID: plan.spendingTxID.String(),
			Ops:          encodeOutpoints(plan.realInputs),
		})})
		for i, r := range plan.refs {
			ops = append(ops, outboxOp{opType: opReleaseResv, chunk: i, payload: mustJSON(releasePayload{
				Reservation: r.reference,
				Ops:         encodeOutpoints(plan.realInputs),
			})})
		}
	}
	if len(plan.phantomInputs) > 0 {
		ops = append(ops, outboxOp{opType: opRemovePhantom, payload: mustJSON(removePhantomPayload{
			Ops: encodeOutpoints(plan.phantomInputs),
		})})
	}
	if len(plan.changeOps) > 0 {
		ops = append(ops, outboxOp{opType: opRemoveMinted, payload: mustJSON(removeMintedPayload{
			MintTxID: plan.spendingTxID.String(),
			Ops:      encodeOutpoints(plan.changeOps),
		})})
	}
	return ops
}

type unspendPayload struct {
	SpendingTxID string   `json:"spendingTxId"`
	Ops          []string `json:"ops"`
}
type releasePayload struct {
	Reservation string   `json:"reservation"`
	Ops         []string `json:"ops"`
}
type removePhantomPayload struct {
	Ops []string `json:"ops"`
}
type removeMintedPayload struct {
	MintTxID string   `json:"mintTxId"`
	Ops      []string `json:"ops"`
}

// DrainOutbox replays pending utxo_ops_outbox rows against the utxostore, oldest
// first, idempotently — the Mode B crash-recovery half of the release. Each op
// that succeeds is MarkDone-d; each that fails has its attempt counter bumped and
// is left for a later pass, parking once it crosses the attempts ceiling. It is a
// no-op in Mode A (that path never enqueues). Fetch + execute + mark share one
// transaction so the SKIP-LOCKED row locks are held across the batch.
func (p *Provider) DrainOutbox(ctx context.Context, limit int) (defs.OutboxDrainReport, error) {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	var rep defs.OutboxDrainReport
	err := p.meta.Do(ctx, func(ctx context.Context) error {
		pending, err := p.meta.Outbox().FetchPending(ctx, limit)
		if err != nil {
			return err
		}
		for i := range pending {
			if err := ctx.Err(); err != nil {
				return err
			}
			e := pending[i]
			if _, xerr := p.executeOutboxOp(ctx, e); xerr != nil {
				if rerr := p.meta.Outbox().RecordError(ctx, e.TxID, e.OpType, e.Chunk, xerr.Error()); rerr != nil {
					return rerr
				}
				rep.Failed++
				if e.Attempts+1 >= metastore.MaxOutboxAttempts {
					rep.Parked++
				}
				continue
			}
			if derr := p.meta.Outbox().MarkDone(ctx, e.TxID, e.OpType, e.Chunk); derr != nil {
				return derr
			}
			rep.Drained++
		}
		return nil
	})
	if err != nil {
		return rep, fmt.Errorf("storage: drain outbox: %w", err)
	}
	return rep, nil
}

// executeOutboxOp applies one outbox op idempotently against the utxostore (and,
// for REMOVE_MINTED, cascades children in the ambient metadata transaction). It
// returns the number of children cascaded. Shared by the inline Mode B release
// and the background drain worker so both heal identically.
func (p *Provider) executeOutboxOp(ctx context.Context, e metastore.OutboxEntry) (cascaded int, err error) {
	switch e.OpType {
	case opUnspend:
		var pl unspendPayload
		if err := json.Unmarshal(e.Payload, &pl); err != nil {
			return 0, fmt.Errorf("storage: decode unspend op: %w", err)
		}
		spendingTxID, herr := chainhash.NewHashFromHex(pl.SpendingTxID)
		if herr != nil {
			return 0, fmt.Errorf("storage: decode unspend spendingTxId: %w", herr)
		}
		ops, derr := decodeOutpoints(pl.Ops)
		if derr != nil {
			return 0, derr
		}
		return 0, p.doUnspend(ctx, *spendingTxID, ops)
	case opReleaseResv:
		var pl releasePayload
		if err := json.Unmarshal(e.Payload, &pl); err != nil {
			return 0, fmt.Errorf("storage: decode release op: %w", err)
		}
		ops, derr := decodeOutpoints(pl.Ops)
		if derr != nil {
			return 0, derr
		}
		return 0, p.doReleaseOutpoints(ctx, pl.Reservation, ops)
	case opRemovePhantom:
		var pl removePhantomPayload
		if err := json.Unmarshal(e.Payload, &pl); err != nil {
			return 0, fmt.Errorf("storage: decode remove-phantom op: %w", err)
		}
		ops, derr := decodeOutpoints(pl.Ops)
		if derr != nil {
			return 0, derr
		}
		return 0, p.doRemovePhantom(ctx, ops)
	case opRemoveMinted:
		var pl removeMintedPayload
		if err := json.Unmarshal(e.Payload, &pl); err != nil {
			return 0, fmt.Errorf("storage: decode remove-minted op: %w", err)
		}
		mintTxID, herr := chainhash.NewHashFromHex(pl.MintTxID)
		if herr != nil {
			return 0, fmt.Errorf("storage: decode remove-minted mintTxId: %w", herr)
		}
		ops, derr := decodeOutpoints(pl.Ops)
		if derr != nil {
			return 0, derr
		}
		return p.doRemoveMinted(ctx, *mintTxID, ops)
	default:
		return 0, fmt.Errorf("storage: unknown outbox op type %q", e.OpType)
	}
}

// --- winner rule + input parsing -------------------------------------------

// findWinnerInputs unions the inputs consumed by EVERY competing tx that is
// itself terminal-successful (MINED/IMMUTABLE) with a readable raw tx, and
// returns that union. found is true when at least one competitor has provably
// won AND the consumption set of every confirmed winner is known.
//
// Unioning ALL winners is load-bearing, not an optimization: two competitors can
// each mine a DISJOINT subset of our inputs (they conflict with us but not with
// each other) — e.g. our tx spends {X,Y}, c1 mines {X}, c2 mines {Y}. Stopping
// at the first winner would leave the other winner's input outside the consumed
// set and release it back to Claimable, resurrecting a coin provably spent
// on-chain. Only an input taken by NONE of the confirmed winners is safe to
// release.
//
// If ANY confirmed winner's inputs are unreadable (missing or unparseable
// rawTx) we cannot prove which of our inputs it consumed, so no input can be
// proven safe: DEFER the whole release (return found=false → the suspect is left
// for a later pass / eventually stuck). This is the SAFE choice — never release
// on an unknown winner consumption set.
func (p *Provider) findWinnerInputs(ctx context.Context, competingTxs []string) (map[utxostore.Outpoint]bool, bool) {
	consumed := make(map[utxostore.Outpoint]bool)
	anyWinner := false
	for _, competitor := range competingTxs {
		if competitor == "" {
			continue
		}
		crec, err := p.oracle.GetTx(ctx, competitor)
		if err != nil || crec == nil {
			continue
		}
		if crec.Status != arcade.StatusMined && crec.Status != arcade.StatusImmutable {
			continue // not a winner yet
		}
		// A confirmed winner: its consumption set MUST be known, or we cannot prove
		// any input safe to release — defer the entire release.
		if len(crec.RawTx) == 0 {
			p.logger.DebugContext(ctx, "reconciler: winner confirmed but rawTx unavailable; deferring release",
				slog.String("winner", competitor))
			return nil, false
		}
		wtx, perr := transaction.NewTransactionFromBytes(crec.RawTx)
		if perr != nil {
			p.logger.DebugContext(ctx, "reconciler: winner rawTx unparseable; deferring release",
				slog.String("winner", competitor), slog.String("error", perr.Error()))
			return nil, false
		}
		anyWinner = true
		for _, in := range wtx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			consumed[utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex}] = true
		}
	}
	return consumed, anyWinner
}

// txInputs parses the suspect's retained raw tx into its input outpoints. An
// absent raw tx yields no inputs (nothing to release via Unspend/ReleaseOutpoints)
// — logged, not an error.
func (p *Provider) txInputs(kt *metastore.KnownTx) ([]utxostore.Outpoint, error) {
	if len(kt.RawTx) == 0 {
		p.logger.Warn("reconciler: suspect has no retained raw tx; cannot enumerate inputs to release",
			slog.String("txid", kt.TxID))
		return nil, nil
	}
	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		return nil, fmt.Errorf("storage: parse suspect raw tx %s: %w", kt.TxID, err)
	}
	ops := make([]utxostore.Outpoint, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		ops = append(ops, utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex})
	}
	return ops, nil
}

// --- freeze holds ----------------------------------------------------------

// freezeChange places a reversible hold on a confirmed-bad suspect's change so
// no new funding selects a coin that is about to be removed. Best-effort: a
// missing coin (no change, or already removed) is not an error.
func (p *Provider) freezeChange(ctx context.Context, txid string) {
	ops, err := p.changeOutpointsByTxID(ctx, txid)
	if err != nil || len(ops) == 0 {
		return
	}
	if err := p.utxo.Freeze(ctx, ops); err != nil {
		p.logger.DebugContext(ctx, "reconciler: freeze change (best-effort) failed",
			slog.String("txid", txid), slog.String("error", err.Error()))
	}
}

// unfreezeChange lifts the freezeChange hold (recovered branch).
func (p *Provider) unfreezeChange(ctx context.Context, txid string) {
	ops, err := p.changeOutpointsByTxID(ctx, txid)
	if err != nil || len(ops) == 0 {
		return
	}
	if err := p.utxo.Unfreeze(ctx, ops); err != nil {
		p.logger.DebugContext(ctx, "reconciler: unfreeze change (best-effort) failed",
			slog.String("txid", txid), slog.String("error", err.Error()))
	}
}

// --- small codecs ----------------------------------------------------------

func encodeOutpoints(ops []utxostore.Outpoint) []string {
	out := make([]string, len(ops))
	for i, op := range ops {
		out[i] = op.String()
	}
	return out
}

func decodeOutpoints(ss []string) ([]utxostore.Outpoint, error) {
	out := make([]utxostore.Outpoint, 0, len(ss))
	for _, s := range ss {
		idx := strings.LastIndex(s, ":")
		if idx < 0 {
			return nil, fmt.Errorf("storage: bad outpoint %q", s)
		}
		h, err := chainhash.NewHashFromHex(s[:idx])
		if err != nil {
			return nil, fmt.Errorf("storage: bad outpoint txid %q: %w", s, err)
		}
		vout, err := strconv.ParseUint(s[idx+1:], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("storage: bad outpoint vout %q: %w", s, err)
		}
		out = append(out, utxostore.Outpoint{TxID: *h, Vout: uint32(vout)})
	}
	return out, nil
}

func encodeTxidToRaw(txid string) ([]byte, error) {
	// Match the metastore's txid encoding (a plain hex decode of the display
	// string) so the outbox PK txid equals the known_txs txid byte-for-byte.
	b, err := hex.DecodeString(txid)
	if err != nil {
		return nil, fmt.Errorf("storage: decode txid %s: %w", txid, err)
	}
	return b, nil
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// The payloads are all plain structs of strings; marshal cannot fail.
		panic(fmt.Sprintf("storage: marshal outbox payload: %v", err))
	}
	return b
}
