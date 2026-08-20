package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"golang.org/x/sync/errgroup"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// This file holds the provider-side async status hooks the background monitor
// (pkg/monitor) drives: the single idempotent ApplyStatusUpdate entry point the
// SSE apply pool calls per event, the sweep methods (SendWaiting / AbortAbandoned
// / SynchronizeTransactionStatuses / CheckProofs), the reorg demote, and the
// key-value + job-lease seams. Together they turn the arcade-only async model
// on: a broadcast returns early, and these hooks later promote the tx to
// unproven/mined (with header-verified proofs) or mark it suspect.
//
// Everything here is IDEMPOTENT (the SSE pool delivers at-least-once and the
// polls re-apply) and Mode-A/B correct (multi-step writes wrap in metastore.Do,
// which under Mode A shares one transaction across the metastore and utxostore).

const (
	// defaultMonitorBatchLimit caps how many rows a sweep processes per run when
	// the caller passes a non-positive limit.
	defaultMonitorBatchLimit = 256

	// syncStaleness is how long a non-terminal known tx must have sat untouched
	// before SynchronizeTransactionStatuses re-polls it via GetTx. The cron
	// interval is the coarse cadence; this filters out just-touched rows.
	syncStaleness = 1 * time.Minute

	// maxRepairPages caps how many EXTRA pages of the repair list one sweep
	// drains beyond its first. The repair rate used to be flat — one page per
	// tick whatever the backlog — so a 269k divergence needed ~65 minutes at the
	// observed 4,000 rows/≈60s ≈ 67/s, and only a lucky block catchup rescued
	// it. Recovery has to scale with the hole it is filling, so the sweep pages
	// while the backlog is deeper than one page: 16 pages of 4,000 is ~1,000/s,
	// which drains that same 269k in ~4 minutes.
	//
	// The cap exists because the sweep must not become the load: each page is
	// `limit` GetTx calls (pollPeers at a time) plus a bulk apply, and gocron
	// runs this task in singleton/reschedule mode, so an overlong sweep simply
	// eats its own next tick.
	maxRepairPages = 16

	// repairSweepBudget bounds the wall clock the extra repair paging may take,
	// so the sweep yields well inside a poll interval — the shortest configured
	// default is 5 minutes, and the deployment that produced the 269k backlog
	// ran it at ≈60s — however slowly arcade is answering. It is the second half
	// of the maxRepairPages bound: pages cap the work, the budget caps the time.
	repairSweepBudget = 30 * time.Second
)

// pollableStatuses are the in-flight / unproven known-tx statuses the poll
// fallback re-polls (via GetTx → ApplyStatusUpdate) when they go stale. It
// excludes the terminal states, the delayed-send queue (unsent, owned by
// SendWaiting), and the never-broadcast nosend.
var pollableStatuses = []wdk.ProvenTxReqStatus{
	wdk.ProvenTxStatusSending,
	wdk.ProvenTxStatusUnprocessed,
	wdk.ProvenTxStatusUnconfirmed,
	wdk.ProvenTxStatusUnmined,
	wdk.ProvenTxStatusCallback,
	wdk.ProvenTxStatusReorg,
}

// unprovenStatuses are the broadcast-accepted-but-unproven known-tx statuses
// CheckProofs re-polls looking for a merkle proof.
var unprovenStatuses = []wdk.ProvenTxReqStatus{
	wdk.ProvenTxStatusUnconfirmed,
	wdk.ProvenTxStatusUnmined,
	wdk.ProvenTxStatusCallback,
}

// seenFencedStatuses are the known-tx statuses a SEEN-class event must NOT
// advance. Both are states in which the wallet has already ACTED on the
// transaction being dead, so letting arcade's word walk the row forward would
// contradict a decision already applied to the coins:
//
//   - 'aborted': the raw tx is fenced and the funding coins were unpinned and
//     handed back. Advancing to unconfirmed would present a transaction as
//     in-flight whose inputs another action may already have taken.
//   - 'stuck': the reconciler's terminal escalation for a suspect it could
//     never resolve. Its inputs are deliberately NEVER auto-released and it is
//     operator-visible on purpose; a SEEN silently clearing that flag would
//     retire the escalation without anyone deciding to.
//
// 'suspectFailed' is deliberately ABSENT, and that absence is load-bearing. The
// reject→release reconciler's false-positive branch
// ([Provider.handleRecovered]) recovers a suspect precisely BY routing the
// recovered record back through [Provider.ApplyStatusUpdate], whose SEEN arm
// then advances the row to unconfirmed. Fencing suspectFailed here would strand
// every false positive the reconciler exists to rescue. The other terminal
// failures ('invalidTx', 'doubleSpend') need no entry: the callers' own
// [wdk.ProvenTxReqStatus.IsTerminalFailure] guard already drops those events.
var seenFencedStatuses = []wdk.ProvenTxReqStatus{
	metastore.KnownTxStatusAborted,
	metastore.KnownTxStatusStuck,
}

// seenAdvanceSkipStatuses is the negative-CAS guard the SEEN appliers pass to
// the known-tx status write: the beyond-broadcast set they have always carried
// (a SEEN never regresses a row that is already further along) PLUS the fenced
// statuses above.
//
// It is NOT [metastore.KnownTxNeverRequeueStatuses], which contains 'sending'
// and 'suspectFailed' and is documented as the guard of BACKWARD, requeue-shaped
// writes only. A SEEN applier is a FORWARD writer; guarding it with that set
// would refuse the very advances it exists to make.
//
// The classifiers in [Provider.ApplyStatusUpdate] and
// [Provider.ApplyStatusBatch] already divert fenced rows before they reach an
// applier, so this guard is not the primary fence — it is the ATOMIC one. Both
// classifiers read the known-tx row OUTSIDE the write transaction, and an abort
// landing in that window is an ordinary concurrent event; only a predicate
// carried in the write itself can refuse it.
var seenAdvanceSkipStatuses = append(
	append([]wdk.ProvenTxReqStatus(nil), wdk.ProvenTxReqBeyondBroadcastStageStatuses...),
	seenFencedStatuses...)

// isSeenFenced reports whether a SEEN-class event must be refused for a row at
// this status. See [seenFencedStatuses].
func isSeenFenced(st wdk.ProvenTxReqStatus) bool {
	for _, fenced := range seenFencedStatuses {
		if st == fenced {
			return true
		}
	}
	return false
}

// minedRepairStatuses are the known-tx statuses on which a header-verified
// MINED/IMMUTABLE proof must be REPAIRED ([Provider.applyMinedRepair]) rather
// than either refused or applied normally. They are the four states in which
// the wallet has DECIDED THE TRANSACTION IS DEAD AND ACTED ON IT — the funding
// coins were released, or are held pending an operator — so a proof arriving
// afterwards is not a status update, it is the discovery that the wallet was
// wrong about coins it has already re-lent.
//
// This is audit P1-6 and the aborted door C5's probe found, in one set, because
// they are one hazard reached through two different guards:
//
//   - 'invalidTx' / 'doubleSpend' — the reject reconciler's verified release
//     already handed the inputs back. These are [wdk.ProvenTxReqStatus.IsTerminalFailure],
//     so the terminal-wallet-state guard swallowed the MINED and the false
//     positive could never be repaired.
//   - 'stuck' — the max-quarantine escalation. Its inputs are deliberately
//     never auto-released, so nothing is loose; but the transaction really is
//     on chain, and completing it retires an operator escalation that has been
//     answered.
//   - 'aborted' — the wallet fenced the bytes and released the reservation
//     before any broadcast, and the client broadcast them out of band anyway.
//     'aborted' is NOT a terminal failure, so no guard saw it at all: the MINED
//     walked into the ordinary apply, whose SetProof silently completed the
//     known tx with no fact-spend and no change re-mint. That is why this set
//     is written out explicitly instead of being derived from IsTerminalFailure.
//
// 'suspectFailed' is deliberately ABSENT, and for the same reason it is absent
// from [seenFencedStatuses]: nothing has been released yet, so there is nothing
// to repair, and the reconciler's false-positive branch
// ([Provider.handleRecovered]) recovers such a row by routing it through the
// ORDINARY apply. Adding it here would reroute every recovered suspect.
var minedRepairStatuses = []wdk.ProvenTxReqStatus{
	wdk.ProvenTxStatusInvalid,
	wdk.ProvenTxStatusDoubleSpend,
	metastore.KnownTxStatusStuck,
	metastore.KnownTxStatusAborted,
}

// isMinedRepairStatus reports whether a known tx at this status needs the
// repair apply rather than the ordinary one. See [minedRepairStatuses].
func isMinedRepairStatus(st wdk.ProvenTxReqStatus) bool {
	for _, s := range minedRepairStatuses {
		if st == s {
			return true
		}
	}
	return false
}

// isMinedClass reports whether an arcade status asserts the transaction is on
// chain — the only evidence that may drive a repair. A SEEN-class event on a
// fenced row keeps its existing treatment (a no-op for the terminal failures, an
// ERROR-logged divert for 'aborted'/'stuck'): "some node has it in a mempool" is
// not proof the wallet's decision was wrong, and acting on it would re-spend
// coins on hearsay.
func isMinedClass(s arcade.Status) bool {
	return s == arcade.StatusMined || s == arcade.StatusImmutable
}

// minedArcadeStatuses is [isMinedClass] as the wire strings the backfill query
// matches against the persisted arcade_status column.
var minedArcadeStatuses = []string{string(arcade.StatusMined), string(arcade.StatusImmutable)}

// minedRepairTxStatuses is the transaction-row CAS from-set for the repair
// apply: [Provider.applyMined]'s set EXTENDED with the two written-off statuses
// ('failed', 'aborted'). The extension is scoped to this path on purpose — the
// ordinary mined apply must never resurrect a written-off transaction row,
// because outside a verified repair there is nothing that has re-established
// the coins behind it.
var minedRepairTxStatuses = []wdk.TxStatus{
	wdk.TxStatusSending,
	wdk.TxStatusNoSend,
	wdk.TxStatusUnproven,
	wdk.TxStatusUnprocessed,
	wdk.TxStatusCompleted,
	wdk.TxStatusFailed,
	wdk.TxStatusAborted,
}

// logSeenOnFenced reports the irreducible race: the wallet decided this
// transaction was dead and acted on it, and the network has it anyway.
//
// ERROR, not WARN. For an aborted transaction the funding coins were released
// back to the funder the moment the fence went up, so if a second action has
// since taken one of them, two transactions the network will see now spend the
// same coin — and the accepted-broadcast path has already RECORDED the earlier
// spend as a fact, which is what makes this diagnosable at all. Nothing
// automatic can repair it; a person has to look.
func (p *Provider) logSeenOnFenced(ctx context.Context, txid string, st wdk.ProvenTxReqStatus, status arcade.Status) {
	p.logger.ErrorContext(ctx, "arcade reports a fenced transaction on the network; refusing to advance it",
		slog.String("txid", txid),
		slog.String("knownTxStatus", string(st)),
		slog.String("arcadeStatus", string(status)))
}

// ApplyStatusUpdate applies one arcade status record to the wallet state. It is
// the single idempotent entry point the SSE apply pool calls per event (and the
// polls call per GetTx result). It routes by rec.Status:
//
//   - SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES / ACCEPTED_BY_NETWORK → advance the
//     known tx + transaction to unproven and promote change to TierUnproven.
//   - MINED / IMMUTABLE → VERIFY the BUMP merkle root against the header source
//     BEFORE storing anything (a proof that fails or cannot yet be verified is
//     never stored — the tx stays unproven for the proof poll to retry), then
//     store the proof, complete the tx, and promote change to TierMined.
//   - PENDING_RETRY → keep the tx in flight, touching its clock.
//   - REJECTED / DOUBLE_SPEND_ATTEMPTED → mark the known tx suspectFailed and
//     record the competing txids + suspect_since, but do NOT release the
//     reserved inputs. That verified release is the M4.2 reconciler's job (which
//     reads exactly what MarkSuspect writes). This is the deliberate asymmetry
//     with the SYNCHRONOUS 4xx path (commitRejected), which cleans change
//     eagerly because a sync 4xx is a final tx-level rejection the user can no
//     longer self-heal.
//
// Idempotency + terminal guards: an unknown txid is a no-op; a txid already in a
// terminal wallet state (completed / invalidTx / doubleSpend) is a no-op; and
// the arcade status lattice (arcade.Status.CanSupersede against the persisted
// arcade_status) blocks any late lower-priority frame from downgrading a more
// advanced state (e.g. a SEEN arriving after MINED).
func (p *Provider) ApplyStatusUpdate(ctx context.Context, rec arcade.TxRecord) error {
	if rec.TxID == "" {
		return nil
	}
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, rec.TxID)
	if err != nil {
		return fmt.Errorf("storage: load known tx %s: %w", rec.TxID, err)
	}
	if !found {
		// Arcade streams every tx; this one is not ours.
		return nil
	}

	// The MINED REPAIR exception, evaluated BEFORE the terminal guard because it
	// is the one case where that guard is the bug: a proof on a written-off row
	// is not a downgrade, it is the discovery that coins the wallet has already
	// handed back are consumed on chain. See [minedRepairStatuses].
	repair := isMinedClass(rec.Status) && isMinedRepairStatus(kt.Status)

	// Terminal wallet-state guard: a proven or terminally-failed tx is never
	// downgraded by a later event (also makes re-applying MINED a no-op — the
	// repair completes the row, so its own re-delivery lands here).
	if !repair && (kt.Status == wdk.ProvenTxStatusCompleted || kt.Status.IsTerminalFailure()) {
		return nil
	}

	// Arcade lattice guard: a frame may only advance the status, never regress
	// it (RECEIVED after SEEN, SEEN after MINED, …). Idempotent same-status
	// re-applies are allowed (CanSupersede returns true for prev == s). It
	// applies to the repair too, and costs it nothing: MINED may supersede every
	// status but IMMUTABLE, and IMMUTABLE may supersede everything.
	var prev arcade.Status
	if kt.ArcadeStatus != nil {
		prev = arcade.Status(*kt.ArcadeStatus)
	}
	if !rec.Status.CanSupersede(prev) {
		p.logger.DebugContext(ctx, "ignoring superseded status event",
			slog.String("txid", rec.TxID),
			slog.String("prev", string(prev)),
			slog.String("status", string(rec.Status)))
		return nil
	}

	switch rec.Status {
	case arcade.StatusSeenOnNetwork, arcade.StatusSeenMultipleNodes, arcade.StatusAcceptedByNetwork:
		if isSeenFenced(kt.Status) {
			// The wallet already acted on this transaction being dead. Record what
			// arcade said — an operator staring at the row needs it — but touch no
			// wallet state. See [seenFencedStatuses].
			p.logSeenOnFenced(ctx, rec.TxID, kt.Status, rec.Status)
			return p.recordArcadeStatus(ctx, rec.TxID, rec.Status)
		}
		return p.applySeen(ctx, rec)
	case arcade.StatusMined, arcade.StatusImmutable:
		if repair {
			_, rerr := p.applyMinedRepair(ctx, rec, kt)
			return rerr
		}
		return p.applyMined(ctx, rec)
	case arcade.StatusRejected, arcade.StatusDoubleSpendAttempted:
		return p.applyRejected(ctx, rec)
	case arcade.StatusPendingRetry:
		return p.applyPendingRetry(ctx, rec)
	default:
		// RECEIVED / SENT_TO_NETWORK / STUMP_PROCESSING / UNKNOWN and any future
		// arcade status: record the lattice progression without touching wallet
		// state.
		return p.recordArcadeStatus(ctx, rec.TxID, rec.Status)
	}
}

// applySeen advances a broadcast-accepted tx to unproven and makes its change
// spendable at TierUnproven. Every step is idempotent (guarded transitions skip
// when already advanced; Promote of an already-TierUnproven coin is a no-op).
//
// Its caller diverts fenced rows before they get here; the known-tx write
// carries [seenAdvanceSkipStatuses] as well, which is the guard that survives an
// abort landing between that read and this write.
func (p *Provider) applySeen(ctx context.Context, rec arcade.TxRecord) error {
	txid := rec.TxID
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusUnproven,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: seen: mark unproven: %w", err)
		}
		if err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnconfirmed,
			seenAdvanceSkipStatuses...); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) && !errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: seen: mark unconfirmed: %w", err)
		}
		if err := p.promoteChangeByTxID(ctx, txid, utxostore.TierUnproven); err != nil {
			return err
		}
		return p.setArcadeStatus(ctx, txid, rec.Status)
	})
}

// applyMined verifies the BUMP merkle root against the header source and, only
// on success, stores the proof, completes the tx, promotes change to TierMined,
// and removes the now-terminal tx's spent inputs from the hot inventory. A
// missing/unparseable BUMP, an unverifiable root (our header view lagging
// arcade), or a mismatched root all leave the tx unproven — never storing an
// unverified proof — for CheckProofs to retry via GetTx.
func (p *Provider) applyMined(ctx context.Context, rec arcade.TxRecord) error {
	txid := rec.TxID
	if len(rec.MerklePath) == 0 {
		p.logger.DebugContext(ctx, "mined event without merkle path; deferring to proof poll", slog.String("txid", txid))
		return nil
	}
	if p.hdrs == nil {
		return fmt.Errorf("storage: cannot verify merkle proof for %s: no headers source", txid)
	}
	txidHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: parse txid %s: %w", txid, err)
	}
	mp, err := transaction.NewMerklePathFromBinary(rec.MerklePath)
	if err != nil {
		p.logger.WarnContext(ctx, "mined event carries an unparseable merkle path; not storing",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil
	}
	if rec.BlockHeight != 0 && uint64(mp.BlockHeight) != rec.BlockHeight {
		p.logger.WarnContext(ctx, "mined event block height disagrees with merkle path; not storing",
			slog.String("txid", txid),
			slog.Uint64("eventHeight", rec.BlockHeight),
			slog.Uint64("pathHeight", uint64(mp.BlockHeight)))
		return nil
	}
	root, err := mp.ComputeRoot(txidHash)
	if err != nil {
		p.logger.WarnContext(ctx, "cannot compute merkle root from event path; not storing",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil
	}

	// TRUST ANCHOR: the root must match the header at this height BEFORE we store.
	ok, verr := p.hdrs.VerifyMerkleRoot(ctx, root, mp.BlockHeight)
	if verr != nil {
		// The header is not available yet (our chain view lags arcade). Leave the
		// tx unproven; the proof poll re-checks once the header lands.
		p.logger.DebugContext(ctx, "merkle root not yet verifiable; deferring to proof poll",
			slog.String("txid", txid), slog.String("error", verr.Error()))
		return nil
	}
	if !ok {
		// A genuinely bad proof: never stored.
		p.logger.WarnContext(ctx, "merkle root failed header verification; rejecting proof",
			slog.String("txid", txid), slog.Uint64("height", uint64(mp.BlockHeight)))
		return nil
	}

	blockHash := p.resolveBlockHash(ctx, rec.BlockHash, mp.BlockHeight)

	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.KnownTx().SetProof(ctx, txid, mp.BlockHeight, blockHash, mp.Bytes(), root[:]); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: set proof: %w", err)
		}
		if err := p.setArcadeStatus(ctx, txid, rec.Status); err != nil {
			return err
		}
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusCompleted,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed, wdk.TxStatusCompleted); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: mined: mark completed: %w", err)
		}
		// The proof now anchors the tx, so its input BEEF ancestry is dead weight
		// (see SetProof, which drops the known_txs copy). Drop the transactions
		// copy too — the dominant blob under sustained load.
		if err := p.meta.Transactions().ClearInputBEEFByTxID(ctx, txid); err != nil {
			return fmt.Errorf("storage: mined: clear input beef: %w", err)
		}
		if err := p.promoteChangeByTxID(ctx, txid, utxostore.TierMined); err != nil {
			return err
		}
		// The mined tx is terminal: its spent inputs are permanently consumed and
		// no longer live, so drop them from the hot inventory (their history stays
		// in the output ledger). Without this, spent rows linger forever and
		// inflate insert/index cost. Idempotent on a MINED re-apply.
		removed, err := p.utxo.RemoveSpentBy(ctx, *txidHash)
		if err != nil {
			return fmt.Errorf("storage: mined: remove spent inputs: %w", err)
		}
		if removed > 0 {
			p.logger.DebugContext(ctx, "removed spent inputs of mined tx",
				slog.String("txid", txid), slog.Int("removed", removed))
		}
		return nil
	})
}

// applyMinedRepair is [Provider.applyMined] for a transaction the wallet had
// already written off: audit P1-6, and the aborted door beside it.
//
// The situation it answers is narrow and dangerous. The wallet decided the
// transaction was dead — a rejection that survived two verified reconciler
// passes, an abort, a quarantine escalation — and ACTED on that decision, which
// for the first two means the funding coins went back to the funder. The
// network then mined the transaction. Nothing local un-mines it: those coins are
// consumed on chain, and every second the wallet keeps offering them is a second
// in which the next CreateAction can author a double spend against a settled
// payment. So a header-verified proof does not merely update a status here, it
// takes the coins back.
//
// It differs from applyMined in exactly three places, and each is forced by the
// state it is repairing rather than by the proof:
//
//  1. It FACT-SPENDS the inputs. applyMined does not need to — the accepted
//     broadcast already recorded them — but a released or never-spent input has
//     no such record, and re-establishing it is the whole point.
//  2. Its transaction-row CAS admits 'failed' and 'aborted' (see
//     [minedRepairTxStatuses]).
//  3. It RE-MINTS the change before promoting it. Both the abort
//     (RemoveByMintTx) and the release (doRemoveMinted) DELETE the coins, so
//     applyMined's Promote would have nothing to promote; and where the
//     reconciler only froze them, the freeze has to be lifted or the coins stay
//     invisible to claims.
//
// ORDER: arcade's verdict is recorded FIRST, in its own commit, before the BUMP
// is verified. That is what makes the repair converge across a crash or a
// header view that lags arcade — a deferred proof writes no wallet state, and
// the arcade_status it leaves behind is precisely the signature the backfill
// ([Provider.repairMinedFenced]) finds it by. Without it a deferred repair would
// be lost: none of the poll work lists can reach a fenced row.
//
// applied reports whether the wallet state was actually repaired, as opposed to
// deferred for want of a verifiable proof. The live callers ignore it; the
// backfill counts it, because a pass that deferred every row must not report
// itself as having healed them.
func (p *Provider) applyMinedRepair(ctx context.Context, rec arcade.TxRecord, kt *metastore.KnownTx) (applied bool, err error) {
	txid := rec.TxID
	if p.hdrs == nil {
		return false, fmt.Errorf("storage: cannot verify merkle proof for %s: no headers source", txid)
	}
	p.logger.WarnContext(ctx, "repair: arcade reports a written-off transaction on chain",
		slog.String("txid", txid),
		slog.String("knownTxStatus", string(kt.Status)),
		slog.String("arcadeStatus", string(rec.Status)))
	if err := p.recordArcadeStatus(ctx, txid, rec.Status); err != nil {
		return false, err
	}

	// The SAME trust anchor the ordinary apply uses, and the same refusals: a
	// missing, malformed, unverifiable or mismatched BUMP writes NOTHING. The
	// repair is a bigger write than an ordinary apply, so it may not clear a
	// lower bar — an unverified proof that could consume a wallet's coins is a
	// forgery oracle.
	proof, _ := p.verifyMinedOne(ctx, rec, func(vctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
		return p.hdrs.VerifyMerkleRoot(vctx, root, height)
	})
	if proof == nil {
		return false, nil
	}
	if cerr := p.commitMinedRepair(ctx, kt, proof); cerr != nil {
		return false, cerr
	}
	return true, nil
}

// commitMinedRepair applies a VERIFIED proof to a written-off transaction in one
// [metastore.Store.Do]. Every step is idempotent, so a Mode B re-run (or a
// re-delivered event, or the backfill re-reaching a half-applied row) converges:
// a fact-mode spend by the same spender is a success, Mint of an existing coin
// is a no-op, Unfreeze/Promote of a coin already in that state is uncounted, and
// RemoveSpentBy of already-removed rows removes nothing.
func (p *Provider) commitMinedRepair(ctx context.Context, kt *metastore.KnownTx, proof *minedProof) error {
	txid := kt.TxID
	txRow := p.repairTxRow(ctx, txid)
	return p.meta.Do(ctx, func(ctx context.Context) error {
		// The coins first: a crash after this leaves the spend recorded and the
		// row still written off, which the backfill re-reaches. The reverse
		// ordering would complete the transaction while leaving its inputs
		// claimable — off every work list, with nothing to come back for them.
		if err := p.factSpendMinedInputs(ctx, kt, proof.txidHash, repairReservation(txRow, txid)); err != nil {
			return err
		}
		// The descriptive half of the same fact. The abort cleared this history
		// (fenceAborted's ClearSpentBy) and the never-broadcast paths never wrote
		// it, so without this a completed transaction would list its own inputs as
		// unspent — the utxostore says otherwise and drives spendability, but the
		// two views must not contradict each other.
		if txRow != nil {
			if err := p.markInputsSpentFromRaw(ctx, txRow, kt.RawTx); err != nil {
				return err
			}
		}
		if err := p.meta.KnownTx().SetProof(ctx, txid, proof.height, proof.blockHash, proof.mpBytes, proof.root); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: repair: set proof: %w", err)
		}
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusCompleted,
			minedRepairTxStatuses...); err != nil && !errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: repair: mark completed: %w", err)
		}
		if err := p.meta.Transactions().ClearInputBEEFByTxID(ctx, txid); err != nil {
			return fmt.Errorf("storage: repair: clear input beef: %w", err)
		}
		if err := p.remintChangeByTxID(ctx, txid, utxostore.TierMined); err != nil {
			return err
		}
		// As in applyMined: a mined tx's inputs are permanently consumed, so they
		// leave the hot inventory. Only rows spent BY this txid go — a coin a
		// competing transaction won stays, still recorded to its winner.
		removed, err := p.utxo.RemoveSpentBy(ctx, proof.txidHash)
		if err != nil {
			return fmt.Errorf("storage: repair: remove spent inputs: %w", err)
		}
		p.logger.InfoContext(ctx, "repair: completed a written-off transaction the network mined",
			slog.String("txid", txid),
			slog.Uint64("height", uint64(proof.height)),
			slog.Int("prunedInputs", removed))
		return nil
	})
}

// repairTxRow resolves the wallet transaction behind a repair, CROSS-USER
// (the monitor works by txid) and preferring the OUTGOING row: a txid can back
// several rows — a self-payment is recorded by both sides — and only the
// spender's row carries the reservation that funded it and the transaction-id
// the input spend-history hangs off. Exactly [Provider.applyAcceptedBroadcast]'s
// selection, for exactly its reasons.
//
// nil is a legitimate answer here, unlike on the broadcast path where it is
// corruption: a repair may be handed a known tx whose wallet rows an operator
// has pruned, and the coin-side of the repair still has to run.
func (p *Provider) repairTxRow(ctx context.Context, txid string) *wdk.TableTransaction {
	rows, err := p.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	if err != nil {
		p.logger.WarnContext(ctx, "repair: could not resolve the transaction row",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil
	}
	if len(rows) == 0 {
		return nil
	}
	for i := range rows {
		if rows[i].IsOutgoing {
			return &rows[i]
		}
	}
	return &rows[0]
}

// repairReservation is the reservation token the repair's fact-mode spend
// carries. Fact mode does not GUARD on it (the spend is on chain; no local
// reservation state can refuse it) but every backend still refuses an EMPTY one
// as a programmer error, so the caller must always name the funding run it
// believes it is recording.
//
// The transaction row's reference is that name where one survives. With no row
// (or a row carrying no reference) the spend must STILL be recorded — refusing
// for want of a label would leave the coins claimable, which is the whole hazard
// — so a placeholder naming the repair is used: honest about its provenance, and
// never matched against by anything.
func repairReservation(txRow *wdk.TableTransaction, txid string) string {
	if txRow != nil && txRow.Reference != "" {
		return string(txRow.Reference)
	}
	return "mined-repair:" + txid
}

// markInputsSpentFromRaw records the input spend-history of a repaired
// transaction from its retained raw bytes. Unparseable or absent bytes are not
// an error: [Provider.factSpendMinedInputs] has already reported that at ERROR
// (it is the same read, and it is the one that matters), and the descriptive
// history is not worth failing a repair over.
func (p *Provider) markInputsSpentFromRaw(ctx context.Context, txRow *wdk.TableTransaction, rawTx []byte) error {
	if len(rawTx) == 0 {
		return nil
	}
	tx, err := transaction.NewTransactionFromBytes(rawTx)
	if err != nil {
		return nil //nolint:nilerr // already reported by the fact-spend; see the doc
	}
	return p.markInputsSpent(ctx, txRow.UserID, txRow.TransactionID, tx)
}

// factSpendMinedInputs records, in FACT MODE, that a mined transaction consumed
// the inputs its retained raw tx names. It is the counterpart of
// [Provider.spendReservedInputs] on the broadcast path, and it exists separately
// for ONE reason: the two have opposite policies on a competing recorded spend.
//
//   - On the broadcast path a [utxostore.SpentError] is FATAL. Nothing has been
//     committed yet, the caller can still refuse, and refusing preserves the
//     recorded winner while the apply is re-driven.
//   - Here it is an ALERT the repair continues past. The transaction is already
//     mined; there is nothing left to refuse. Failing would abandon everything
//     the repair CAN still fix — the other inputs, the proof, the change — in
//     exchange for nothing, and would leave the row diverged so the backfill
//     re-attempted it forever. The conflict is real and unrepairable either way:
//     two spend facts exist for one coin, the chain has already picked one, and
//     only the reconciler's competing-tx machinery plus a person can settle the
//     wallet side. So it is named out loud, the recorded winner is left
//     untouched (overwriting it would erase the evidence), and the repair
//     carries on.
//
// The other verdicts are classified exactly as the broadcast path classifies
// them: [utxostore.NotFoundError] is the only benign skip (a pruned or genuinely
// external input), [utxostore.ErrContention] is retryable and returned so the
// whole repair is re-driven, and a top-level error that is NOT
// [utxostore.ErrBatch] is never explained away by the per-item verdicts — the
// batch may have written nothing.
//
// A missing or unparseable raw tx is reported at ERROR and does NOT stop the
// repair. It is the one case where the inputs cannot be enumerated at all, and
// completing the transaction with its change re-minted is still strictly better
// than leaving the row diverged as well; the abort path deliberately retains
// raw_tx for exactly this reader (see [Provider.fenceAborted]), so it should not
// happen.
func (p *Provider) factSpendMinedInputs(
	ctx context.Context, kt *metastore.KnownTx, spendingTxID chainhash.Hash, reservation string,
) error {
	txid := kt.TxID
	if len(kt.RawTx) == 0 {
		p.logger.ErrorContext(ctx, "repair: no retained raw tx; cannot record the mined transaction's spends",
			slog.String("txid", txid))
		return nil
	}
	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		p.logger.ErrorContext(ctx, "repair: retained raw tx is unparseable; cannot record the mined transaction's spends",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil
	}
	ops := make([]*utxostore.SpendOp, 0, len(tx.Inputs))
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil {
			continue
		}
		ops = append(ops, &utxostore.SpendOp{
			Outpoint:     utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex},
			Reservation:  reservation,
			SpendingTxID: spendingTxID,
		})
	}
	if len(ops) == 0 {
		return nil
	}
	serr := p.utxo.Spend(ctx, ops, true)
	if serr == nil {
		return nil
	}

	var (
		failed    int
		tolerated int // NotFound (external) + SpentError (alerted): survivable here
		fatal     error
		contended error
	)
	for _, op := range ops {
		if op.Err == nil {
			continue
		}
		failed++
		var spent *utxostore.SpentError
		switch {
		case errors.Is(op.Err, &utxostore.NotFoundError{}):
			tolerated++
		case errors.As(op.Err, &spent):
			tolerated++
			// THE UNREPAIRABLE OUTCOME. Named from the store's own report of the
			// outpoint and the winner, not from our re-derivation of them.
			p.logger.ErrorContext(ctx,
				"repair: input consumed by a competing recorded spend — double spend materialized",
				slog.String("txid", txid),
				slog.String("outpoint", spent.Op.String()),
				slog.String("winner", spent.Winner.String()))
		case errors.Is(op.Err, utxostore.ErrContention):
			if contended == nil {
				contended = fmt.Errorf("storage: repair: record spend of %s for %s: %w",
					op.Outpoint, txid, op.Err)
			}
		default:
			if fatal == nil {
				fatal = fmt.Errorf("storage: repair: record spend of %s for %s: %w",
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
		return nil
	default:
		return fmt.Errorf("storage: repair: record spends for %s: %w", txid, serr)
	}
}

// remintChangeByTxID makes a repaired transaction's change spendable again at
// tier, whichever way the wallet's write-off had disposed of it.
//
// Three dispositions have to converge on one claimable coin, which is why this
// is Mint-then-Promote-then-Unfreeze and not simply a Promote:
//
//   - REMOVED (the abort's RemoveByMintTx, the release's doRemoveMinted): the
//     row is gone, so only a Mint brings it back — and it must, because the
//     mined transaction genuinely created that output.
//   - FROZEN (the reconciler's pass-1 hold): the row survives, so Mint is an
//     idempotent no-op and does NOT touch the tier; the Promote is what fixes
//     the tier and the Unfreeze is what makes the coin visible to claims again.
//   - UNTOUCHED (a 'stuck' escalation never removes anything): Promote alone.
//
// The descriptive output rows are the source of truth for what to re-mint. They
// outlive all three dispositions — none of them deletes wallet metadata — which
// is what makes the re-mint reconstructible at all.
//
// A per-item [utxostore.AlreadyExistsError] is tolerated with a warning rather
// than failing the repair: it means a row exists under a DIFFERENT coin identity
// than the ledger describes, which the Promote and Unfreeze below still put into
// a claimable state, and abandoning the whole repair over a satoshi-value
// disagreement would leave the mined transaction's inputs unrecorded.
func (p *Provider) remintChangeByTxID(ctx context.Context, txid string, tier utxostore.Tier) error {
	change := true
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{TxID: &txid, Change: &change})
	if err != nil {
		return fmt.Errorf("storage: repair: find change outputs %s: %w", txid, err)
	}
	if len(rows) == 0 {
		return nil
	}
	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: repair: parse txid %s: %w", txid, err)
	}
	mints := make([]*utxostore.Mint, 0, len(rows))
	ops := make([]utxostore.Outpoint, 0, len(rows))
	for i := range rows {
		// Every self-owned (change-purpose) output regardless of basket, matching
		// [Provider.mintChange] and [Provider.changeOutpointsByTxID].
		if rows[i].Basket == nil {
			continue
		}
		// …except one the ledger already records as SPENT by a descendant. That
		// coin's fate belongs to the descendant's own lifecycle, and re-minting it
		// claimable here would hand out a coin something else has taken. Skipping
		// is the conservative half of a genuinely ambiguous case: a coin left
		// absent is recoverable, a coin handed out twice is not.
		if rows[i].SpentBy != nil {
			p.logger.WarnContext(ctx, "repair: change already recorded spent; not re-minting",
				slog.String("txid", txid), slog.Int64("vout", int64(rows[i].Vout)))
			continue
		}
		op := utxostore.Outpoint{TxID: *hash, Vout: rows[i].Vout}
		ops = append(ops, op)
		mints = append(mints, &utxostore.Mint{
			Outpoint:  op,
			UserID:    int64(rows[i].UserID),
			Basket:    *rows[i].Basket,
			Satoshis:  uint64(rows[i].Satoshis), //nolint:gosec // change value non-negative
			InputSize: utxostore.DefaultP2PKHInputSize,
			Tier:      tier,
		})
	}
	if len(mints) == 0 {
		return nil
	}
	if err := p.utxo.Mint(ctx, mints); err != nil {
		for _, m := range mints {
			switch {
			case m.Err == nil:
			case errors.Is(m.Err, &utxostore.AlreadyExistsError{}):
				p.logger.WarnContext(ctx, "repair: change coin already exists with a different identity",
					slog.String("outpoint", m.String()), slog.String("error", m.Err.Error()))
			default:
				return fmt.Errorf("storage: repair: re-mint change %s: %w", m.Outpoint, m.Err)
			}
		}
		if !errors.Is(err, utxostore.ErrBatch) {
			return fmt.Errorf("storage: repair: re-mint change %s: %w", txid, err)
		}
	}
	if _, err := p.utxo.Promote(ctx, ops, tier); err != nil {
		return fmt.Errorf("storage: repair: promote change %s: %w", txid, err)
	}
	if err := p.utxo.Unfreeze(ctx, ops); err != nil && !errors.Is(err, &utxostore.NotFoundError{}) {
		return fmt.Errorf("storage: repair: unfreeze change %s: %w", txid, err)
	}
	return nil
}

// applyRejected marks a known tx suspectFailed and records the competing txids
// + suspect_since + arcade status + arcade's rejection reason for the M4.2
// reject reconciler. It does NOT release inputs or touch change (see the
// ApplyStatusUpdate doc's asymmetry note).
func (p *Provider) applyRejected(ctx context.Context, rec arcade.TxRecord) error {
	p.logRejection(ctx, rec.TxID, string(rec.Status), rec.ExtraInfo, "status_event")
	if err := p.meta.KnownTx().MarkSuspect(ctx, rec.TxID, p.now(), rec.CompetingTxs, string(rec.Status), rec.ExtraInfo); err != nil &&
		!errors.Is(err, metastore.ErrNotFound) {
		return fmt.Errorf("storage: mark suspect %s: %w", rec.TxID, err)
	}
	return nil
}

// logRejection emits the one log line that makes a rejection diagnosable
// without a database query, at the instant the reason is first known.
//
// It exists because the reason is perishable. Arcade removes a rejected
// transaction's record on its own schedule; we have seen a REJECTED event whose
// GET /tx returned 404 immediately afterwards, leaving teranode's pod logs as
// the only remaining source of truth. Logging at WARN — not DEBUG — is
// deliberate: a rejection is never routine, and a rejection nobody can explain
// is worse than the rejection itself.
//
// An empty reason is reported as such rather than omitted: "arcade rejected
// this and told us nothing" is a distinct and actionable observation.
func (p *Provider) logRejection(ctx context.Context, txid, status, reason, source string) {
	if reason == "" {
		reason = "(arcade supplied no reason)"
	}
	p.logger.WarnContext(ctx, "arcade rejected transaction",
		slog.String("txid", txid),
		slog.String("arcadeStatus", status),
		slog.String("reason", reason),
		slog.String("source", source))
}

// applyPendingRetry keeps a tx in flight: arcade's broadcast hit a retryable
// error and it will retry. It records the arcade status (which touches
// updated_at, resetting the poll-staleness clock). The abandoned sweep only
// targets never-broadcast unsigned/nosend txs, so there is no attempt counter
// to reset here.
func (p *Provider) applyPendingRetry(ctx context.Context, rec arcade.TxRecord) error {
	return p.recordArcadeStatus(ctx, rec.TxID, rec.Status)
}

// recordArcadeStatus persists the arcade wire status without any wallet-state
// change (tolerating an ErrNotFound if the row vanished concurrently).
func (p *Provider) recordArcadeStatus(ctx context.Context, txid string, status arcade.Status) error {
	if err := p.setArcadeStatus(ctx, txid, status); err != nil {
		return fmt.Errorf("storage: record arcade status %s: %w", txid, err)
	}
	return nil
}

// setArcadeStatus writes the arcade wire status, treating ErrNotFound as benign.
func (p *Provider) setArcadeStatus(ctx context.Context, txid string, status arcade.Status) error {
	if err := p.meta.KnownTx().SetArcadeStatus(ctx, txid, string(status)); err != nil && !errors.Is(err, metastore.ErrNotFound) {
		return err
	}
	return nil
}

// promoteChangeByTxID promotes the change coins of txid (across all users) to
// tier. Idempotent: an already-at-tier coin counts as unchanged.
func (p *Provider) promoteChangeByTxID(ctx context.Context, txid string, tier utxostore.Tier) error {
	ops, err := p.changeOutpointsByTxID(ctx, txid)
	if err != nil || len(ops) == 0 {
		return err
	}
	if _, err := p.utxo.Promote(ctx, ops, tier); err != nil {
		return fmt.Errorf("storage: promote change %s: %w", txid, err)
	}
	return nil
}

// changeOutpointsByTxID returns the change outpoints for txid without needing a
// userID — the monitor works purely by txid. It is the async analog of
// [Provider.changeOutpoints], and a one-element call of the bulk form: the
// single-txid query it used to issue selected the same rows in the same order
// as the one FindChangeOutputsByTxIDs builds for a one-element list — same
// join, same two conditions, same ORDER BY output_id, differing only in clause
// order and `txid = ?` against `txid IN (?)`. Keeping a second copy only
// created somewhere for the two to drift apart.
func (p *Provider) changeOutpointsByTxID(ctx context.Context, txid string) ([]utxostore.Outpoint, error) {
	return p.changeOutpointsByTxIDs(ctx, []string{txid})
}

// resolveBlockHash returns the block hash bytes to store with a proof: the
// event's when parseable, else the header at height. Returns nil when neither
// is available (the verified merkle root is the trust anchor; the hash is
// descriptive).
func (p *Provider) resolveBlockHash(ctx context.Context, blockHashHex string, height uint32) []byte {
	if blockHashHex != "" {
		if h, err := chainhash.NewHashFromHex(blockHashHex); err == nil {
			return h[:]
		}
		p.logger.WarnContext(ctx, "mined event carries an unparseable block hash; falling back to headers",
			slog.String("blockHash", blockHashHex))
	}
	if p.hdrs != nil {
		if hdr, err := p.hdrs.HeaderByHeight(ctx, height); err == nil && hdr != nil {
			return hdr.Hash[:]
		}
	}
	return nil
}

// minedVerifyConcurrency bounds the parallel header-verification of a batch's
// MINED proofs (pure reads against the header source, no writes) before the
// single batched write transaction.
const minedVerifyConcurrency = 8

// ApplyStatusBatch applies a whole SSE apply-batch of arcade status records to
// the wallet state with BATCHED database writes. It is the high-throughput entry
// point the monitor's SSE apply pool calls; the poll fallbacks
// (SynchronizeTransactionStatuses / CheckProofs) still call the per-tx
// ApplyStatusUpdate.
//
// It is EXACTLY EQUIVALENT to calling ApplyStatusUpdate on each rec in arrival
// order — same terminal + arcade-lattice guards, same per-txid outcome, same
// idempotency — but collapses the per-event (FindByTxID + a small write
// transaction) into: one bulk known_tx load, per-tx MINED verification (header
// reads, done in parallel outside any transaction, never storing an unverified
// proof), then a SINGLE Mode-A transaction that applies each status class in
// set-based form.
//
// Collapse rule (step 1): within one batch a txid almost always appears once —
// its SEEN and its MINED are minutes apart and so land in different batches.
// When a txid DOES repeat within a batch, if every record carries the SAME
// status they are idempotent duplicates and collapse to a single apply; if they
// carry DIFFERENT statuses (reachable on cursor-resume-after-outage replay,
// where a txid's SEEN and MINED co-batch) that txid FALLS BACK to a per-event
// ApplyStatusUpdate loop in arrival order. That preserves exact semantics for
// the rare intricate case (a SEEN's advancement that a subsequently-unverifiable
// MINED would otherwise mask) while still batching every single-status txid —
// i.e. the entire steady-state load.
func (p *Provider) ApplyStatusBatch(ctx context.Context, recs []arcade.TxRecord) error {
	if len(recs) == 0 {
		return nil
	}

	// 1. Group by txid preserving first-seen (arrival) order.
	perTxid := make(map[string][]arcade.TxRecord, len(recs))
	order := make([]string, 0, len(recs))
	for _, rec := range recs {
		if rec.TxID == "" {
			continue // empty txid: a no-op, exactly like ApplyStatusUpdate
		}
		if _, seen := perTxid[rec.TxID]; !seen {
			order = append(order, rec.TxID)
		}
		perTxid[rec.TxID] = append(perTxid[rec.TxID], rec)
	}
	if len(order) == 0 {
		return nil
	}

	// 2. Bulk-load all known txs in ONE query. A txid absent from the map is not
	//    ours — skip it, exactly like ApplyStatusUpdate's !found return.
	loaded, err := p.meta.KnownTx().FindByTxIDs(ctx, order)
	if err != nil {
		return fmt.Errorf("storage: batch load known txs: %w", err)
	}
	known := make(map[string]metastore.KnownTx, len(loaded))
	for i := range loaded {
		known[loaded[i].TxID] = loaded[i]
	}

	// 3 + 4. Guards (evaluated in memory per rec against the loaded row, identical
	//        to ApplyStatusUpdate) and classification of the single surviving
	//        record per txid. Multi-distinct-status txids defer to the fallback.
	var (
		seenRecs     []arcade.TxRecord
		minedRecs    []arcade.TxRecord
		rejectedRecs []arcade.TxRecord
		arcadeOnly   []arcade.TxRecord // PENDING_RETRY + default: record arcade status only
		fallback     []string
	)
	for _, txid := range order {
		kt, ok := known[txid]
		if !ok {
			continue // not ours
		}
		evs := perTxid[txid]
		// The MINED REPAIR divert, ahead of the terminal guard exactly as in
		// ApplyStatusUpdate. It sends the txid to the per-event fallback rather
		// than repairing inline, because the repair is per-transaction work by
		// nature (its own inputs' fact-spend, its own change re-mint) with nothing
		// to batch, and it is rare enough that the round trips do not matter.
		//
		// Diverting here is what stops the batch BURNING THE FENCE: 'aborted' and
		// 'stuck' are not terminal failures, so without this a fenced row fell
		// straight through to applyMinedBatch's bulk SetProof loop — completing the
		// known tx, writing the proof, and destroying the very state the repair
		// keys on, all before any poll could reach it.
		//
		// The divert is NOT atomic, and does not need to be. Like every other
		// guard in this classifier it reads the row OUTSIDE the write
		// transaction, so an abort landing between this read and applyMinedBatch's
		// SetProof still burns the fence. What makes that tolerable — and what
		// makes leaving SetProof unguarded tolerable at all — is the BACKFILL, not
		// this check: a burned fence lands squarely in its signature (arcade says
		// mined, the transaction row says aborted), so the next
		// [Provider.repairMinedFenced] tick repairs the coins the burn skipped. A
		// CAS-guarded SetProof would turn that race into a refusal instead, which
		// is a worse outcome: the proof would be dropped and only the poll — which
		// cannot see a fenced row — would be left to re-drive it.
		if minedRepairBatchRoute(kt.Status, evs) {
			fallback = append(fallback, txid)
			continue
		}
		// Terminal wallet-state guard: a proven or terminally-failed tx is never
		// downgraded by a later event (identical to ApplyStatusUpdate).
		if kt.Status == wdk.ProvenTxStatusCompleted || kt.Status.IsTerminalFailure() {
			continue
		}
		if multipleDistinctStatuses(evs) {
			fallback = append(fallback, txid)
			continue
		}
		rec := evs[len(evs)-1] // collapse duplicates: all share one status
		// Arcade lattice guard: a frame may only advance the status, never regress
		// it (identical to ApplyStatusUpdate).
		var prev arcade.Status
		if kt.ArcadeStatus != nil {
			prev = arcade.Status(*kt.ArcadeStatus)
		}
		if !rec.Status.CanSupersede(prev) {
			continue
		}
		switch rec.Status {
		case arcade.StatusSeenOnNetwork, arcade.StatusSeenMultipleNodes, arcade.StatusAcceptedByNetwork:
			if isSeenFenced(kt.Status) {
				// Identical to ApplyStatusUpdate's SEEN arm: log the race, record
				// arcade's word, advance no wallet state. See [seenFencedStatuses].
				p.logSeenOnFenced(ctx, txid, kt.Status, rec.Status)
				arcadeOnly = append(arcadeOnly, rec)
				continue
			}
			seenRecs = append(seenRecs, rec)
		case arcade.StatusMined, arcade.StatusImmutable:
			minedRecs = append(minedRecs, rec)
		case arcade.StatusRejected, arcade.StatusDoubleSpendAttempted:
			rejectedRecs = append(rejectedRecs, rec)
		case arcade.StatusPendingRetry:
			arcadeOnly = append(arcadeOnly, rec)
		default:
			arcadeOnly = append(arcadeOnly, rec)
		}
	}

	// 5a. MINED verification is per-tx and lives OUTSIDE the write transaction
	//     (header I/O), exactly like applyMined; only verified proofs are written.
	verified := p.verifyMinedBatch(ctx, minedRecs)

	// Resolve each verified proof's spent inputs from its local raw tx (also
	// OUTSIDE the write transaction — pure in-memory parsing) so applyMinedBatch
	// can prune the spent inputs with a batched delete-by-outpoint instead of a
	// per-tx full-set scan (RemoveSpentBy). A proof whose raw tx is absent or
	// unparseable keeps spentOps nil and falls back to the scan.
	for i := range verified {
		kt, ok := known[verified[i].rec.TxID]
		if !ok || len(kt.RawTx) == 0 {
			continue
		}
		tx, terr := transaction.NewTransactionFromBytes(kt.RawTx)
		if terr != nil {
			continue
		}
		ops := make([]utxostore.Outpoint, 0, len(tx.Inputs))
		for _, in := range tx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			ops = append(ops, utxostore.Outpoint{TxID: *in.SourceTXID, Vout: in.SourceTxOutIndex})
		}
		verified[i].spentOps = ops
	}

	// 5b. One Mode-A transaction for every batched write.
	if len(seenRecs) > 0 || len(verified) > 0 || len(rejectedRecs) > 0 || len(arcadeOnly) > 0 {
		if err := p.meta.Do(ctx, func(ctx context.Context) error {
			if err := p.applySeenBatch(ctx, seenRecs); err != nil {
				return err
			}
			if err := p.applyMinedBatch(ctx, verified); err != nil {
				return err
			}
			if err := p.applyRejectedBatch(ctx, rejectedRecs); err != nil {
				return err
			}
			return p.setArcadeStatusBatch(ctx, arcadeOnly)
		}); err != nil {
			return err
		}
	}

	// 6. Per-event fallback for the rare multi-distinct-status txids: exact
	//    ApplyStatusUpdate semantics, arrival order preserved. Distinct txids are
	//    independent, so ordering relative to the batched writes is irrelevant.
	for _, txid := range fallback {
		for _, rec := range perTxid[txid] {
			if err := p.ApplyStatusUpdate(ctx, rec); err != nil {
				return err
			}
		}
	}
	return nil
}

// minedRepairBatchRoute reports whether a batched txid must leave the set-based
// path for the per-event fallback because a MINED/IMMUTABLE record has arrived
// for a written-off row. See the call site and [minedRepairStatuses].
func minedRepairBatchRoute(st wdk.ProvenTxReqStatus, evs []arcade.TxRecord) bool {
	if !isMinedRepairStatus(st) {
		return false
	}
	for i := range evs {
		if isMinedClass(evs[i].Status) {
			return true
		}
	}
	return false
}

// multipleDistinctStatuses reports whether a txid's batch records carry more
// than one distinct arcade status (the trigger for the per-event fallback).
func multipleDistinctStatuses(evs []arcade.TxRecord) bool {
	if len(evs) <= 1 {
		return false
	}
	first := evs[0].Status
	for i := 1; i < len(evs); i++ {
		if evs[i].Status != first {
			return true
		}
	}
	return false
}

// minedProof is a MINED record whose merkle root has been header-verified,
// carrying the proof material to store.
type minedProof struct {
	rec       arcade.TxRecord
	txidHash  chainhash.Hash
	height    uint32
	blockHash []byte
	mpBytes   []byte
	root      []byte
	// spentOps are the tx's input outpoints, resolved from its local raw tx, so
	// the MINED apply can prune the spent inputs with a batched delete-by-outpoint
	// instead of a per-tx full-set scan. Nil when the raw tx was unavailable or
	// unparseable — the apply then falls back to the RemoveSpentBy scan.
	spentOps []utxostore.Outpoint
}

// verifyMinedBatch header-verifies each MINED record's merkle root (in parallel,
// bounded) and returns only the verified ones with their proof material. It
// replicates applyMined's pre-write verification EXACTLY; a proof that fails,
// cannot yet be verified, or is malformed is dropped (left unproven for the
// proof poll), never stored.
func (p *Provider) verifyMinedBatch(ctx context.Context, recs []arcade.TxRecord) []minedProof {
	if len(recs) == 0 {
		return nil
	}

	// Memoize header verification by (height, root) for the life of this batch.
	// Every MINED tx in a block computes the SAME merkle root at the SAME height,
	// so without this each tx re-runs VerifyMerkleRoot -> HeaderByHeight; recent
	// (tip) block headers are intentionally uncached by the headers client (reorg
	// safety), so a block's worth of MINED events would otherwise re-fetch the one
	// block header once per tx (the ~28 apply/s, ~16-minute-lag bottleneck seen at
	// 1500 TPS). Keying on the root (not just height) keeps a malformed BUMP's
	// divergent root honestly re-verified. The fetch runs under the lock so a
	// concurrent burst collapses to one HeaderByHeight per block, not one per
	// verify goroutine.
	type vKey struct {
		height uint32
		root   chainhash.Hash
	}
	type vRes struct {
		ok  bool
		err error
	}
	// entry is one memo slot. done is closed once the single in-flight fetch for
	// this key completes, so late arrivals wait on the CHANNEL, never on vmu.
	type entry struct {
		done chan struct{}
		res  vRes
	}
	var vmu sync.Mutex
	vcache := make(map[vKey]*entry)
	// verify still collapses a block's worth of concurrent verifications to ONE
	// HeaderByHeight per (height, root) — but it must not hold vmu across that
	// fetch. Holding the lock for the whole network call serializes the entire
	// errgroup behind one mutex: a big block's MINED burst then applies at
	// single-goroutine speed, the SSE hand-off channel fills, and the arcade
	// reader blocks in dispatchFrame — stalling the WHOLE event stream, not just
	// MINED. Observed 2026-08-11 at ~1000 TPS: 224 goroutines parked in
	// sync.Mutex.Lock here, every status frozen, and the local ledger diverging
	// from arcade at the full creation rate until the blast was stopped.
	//
	// Singleflight instead: the lock covers only the map lookup/insert. The first
	// caller for a key does the fetch and closes done; the rest release the lock
	// immediately and wait on done. Cache hits never block.
	verify := func(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
		key := vKey{height: height, root: *root}

		vmu.Lock()
		if e, ok := vcache[key]; ok {
			vmu.Unlock()
			select {
			case <-e.done:
				return e.res.ok, e.res.err
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		e := &entry{done: make(chan struct{})}
		vcache[key] = e
		vmu.Unlock()

		e.res.ok, e.res.err = p.hdrs.VerifyMerkleRoot(ctx, root, height)
		close(e.done)
		return e.res.ok, e.res.err
	}

	results := make([]*minedProof, len(recs))
	reasons := make([]string, len(recs))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(minedVerifyConcurrency)
	for i := range recs {
		g.Go(func() error {
			results[i], reasons[i] = p.verifyMinedOne(gctx, recs[i], verify)
			return nil
		})
	}
	_ = g.Wait()
	out := make([]minedProof, 0, len(recs))
	dropped := make(map[string]int)
	var sample []string
	for i, r := range results {
		if r != nil {
			out = append(out, *r)
			continue
		}
		dropped[reasons[i]]++
		if len(sample) < loggedTxIDSample {
			sample = append(sample, recs[i].TxID)
		}
	}
	// A dropped MINED proof leaves the tx WITHOUT an arcade status, i.e. locally
	// diverged from arcade until something re-drives it. The per-tx detail stays
	// at DEBUG (a lagging header view drops a whole block's worth at once and
	// would flood the log), but the batch total is a WARN so a persistent
	// divergence is visible rather than silent — this is exactly the signal that
	// was missing while 23,745 rows sat stranded.
	if len(dropped) > 0 {
		p.logger.WarnContext(ctx, "mined proofs not stored; left for the repair poll",
			slog.Int("dropped", len(recs)-len(out)),
			slog.Int("batch", len(recs)),
			slog.Any("reasons", dropped),
			slog.Any("sampleTxIDs", sample))
	}
	return out
}

// loggedTxIDSample caps how many txids a diagnostic log line carries: enough to
// go look one up, never enough to blow up the log at 1000 TPS.
const loggedTxIDSample = 5

// verifyMinedOne is the read-only pre-write half of applyMined for one record:
// it returns the proof material when the BUMP's root verifies against the header
// source, or nil (with the same warn/debug logging as applyMined) for every
// defer/skip/bad case. Unlike applyMined it never returns a hard error — a
// missing headers source is logged and deferred so a global misconfig cannot
// fail sibling txids' writes; hdrs is required in practice.
//
// The second return is a short, low-cardinality REASON for the drop (empty on
// success) that [Provider.verifyMinedBatch] tallies into one WARN per batch: a
// record dropped here writes NOTHING, so its tx keeps an empty arcade_status and
// only the repair poll will ever pick it up. That has to be countable.
func (p *Provider) verifyMinedOne(ctx context.Context, rec arcade.TxRecord, verify func(context.Context, *chainhash.Hash, uint32) (bool, error)) (*minedProof, string) {
	txid := rec.TxID
	if len(rec.MerklePath) == 0 {
		p.logger.DebugContext(ctx, "mined event without merkle path; deferring to proof poll", slog.String("txid", txid))
		return nil, "no_merkle_path"
	}
	if p.hdrs == nil {
		p.logger.WarnContext(ctx, "cannot verify merkle proof: no headers source; deferring", slog.String("txid", txid))
		return nil, "no_headers_source"
	}
	txidHash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		p.logger.WarnContext(ctx, "cannot parse mined txid; deferring", slog.String("txid", txid), slog.String("error", err.Error()))
		return nil, "unparseable_txid"
	}
	mp, err := transaction.NewMerklePathFromBinary(rec.MerklePath)
	if err != nil {
		p.logger.WarnContext(ctx, "mined event carries an unparseable merkle path; not storing",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil, "unparseable_merkle_path"
	}
	if rec.BlockHeight != 0 && uint64(mp.BlockHeight) != rec.BlockHeight {
		p.logger.WarnContext(ctx, "mined event block height disagrees with merkle path; not storing",
			slog.String("txid", txid),
			slog.Uint64("eventHeight", rec.BlockHeight),
			slog.Uint64("pathHeight", uint64(mp.BlockHeight)))
		return nil, "height_mismatch"
	}
	root, err := mp.ComputeRoot(txidHash)
	if err != nil {
		p.logger.WarnContext(ctx, "cannot compute merkle root from event path; not storing",
			slog.String("txid", txid), slog.String("error", err.Error()))
		return nil, "root_compute_failed"
	}
	ok, verr := verify(ctx, root, mp.BlockHeight)
	if verr != nil {
		p.logger.DebugContext(ctx, "merkle root not yet verifiable; deferring to proof poll",
			slog.String("txid", txid), slog.String("error", verr.Error()))
		return nil, "header_unavailable"
	}
	if !ok {
		p.logger.WarnContext(ctx, "merkle root failed header verification; rejecting proof",
			slog.String("txid", txid), slog.Uint64("height", uint64(mp.BlockHeight)))
		return nil, "root_mismatch"
	}
	return &minedProof{
		rec:       rec,
		txidHash:  *txidHash,
		height:    mp.BlockHeight,
		blockHash: p.resolveBlockHash(ctx, rec.BlockHash, mp.BlockHeight),
		mpBytes:   mp.Bytes(),
		root:      root[:],
	}, ""
}

// applySeenBatch advances every SEEN-class tx to unproven and promotes all their
// change to TierUnproven — the batched form of applySeen, fenced the same way
// (see [seenAdvanceSkipStatuses]). Must run inside p.meta.Do.
func (p *Provider) applySeenBatch(ctx context.Context, recs []arcade.TxRecord) error {
	if len(recs) == 0 {
		return nil
	}
	txids := recordTxIDs(recs)
	if err := p.meta.Transactions().BulkUpdateStatusByTxIDs(ctx, txids, wdk.TxStatusUnproven,
		wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed); err != nil {
		return fmt.Errorf("storage: batch seen: mark unproven: %w", err)
	}
	if err := p.meta.KnownTx().BulkUpdateStatus(ctx, txids, wdk.ProvenTxStatusUnconfirmed,
		seenAdvanceSkipStatuses...); err != nil {
		return fmt.Errorf("storage: batch seen: mark unconfirmed: %w", err)
	}
	ops, err := p.changeOutpointsByTxIDs(ctx, txids)
	if err != nil {
		return err
	}
	if len(ops) > 0 {
		if _, err := p.utxo.Promote(ctx, ops, utxostore.TierUnproven); err != nil {
			return fmt.Errorf("storage: batch seen: promote change: %w", err)
		}
	}
	return p.setArcadeStatusBatch(ctx, recs)
}

// applyMinedBatch stores each verified proof (per-tx, as the proof data differs)
// then batches the status/beef/change/spent writes — the batched form of
// applyMined. Must run inside p.meta.Do.
func (p *Provider) applyMinedBatch(ctx context.Context, proofs []minedProof) error {
	if len(proofs) == 0 {
		return nil
	}
	// LOCK ORDERING: sort the batch into ascending STORAGE txid order before any
	// write. The per-row SetProof loop below takes one row lock at a time, so in
	// arrival order two concurrent apply shards (or an apply racing the poll
	// sweep) whose txid sets overlap could grab the same two rows in opposite
	// orders and deadlock (SQLSTATE 40P01). This is the same order the set-based
	// bulk mutators lock in (see metastore/lockorder.go), so every writer in the
	// pipeline now agrees on one global order.
	sort.Slice(proofs, func(i, j int) bool {
		return metastore.LessTxID(proofs[i].rec.TxID, proofs[j].rec.TxID)
	})
	txids := make([]string, len(proofs))
	recs := make([]arcade.TxRecord, len(proofs))
	for i := range proofs {
		txids[i] = proofs[i].rec.TxID
		recs[i] = proofs[i].rec
	}
	// Per-tx proof write (each proof's data differs — acceptable per the batch
	// design). SetProof also flips known_txs → completed and drops its input_beef.
	for i := range proofs {
		pr := &proofs[i]
		if err := p.meta.KnownTx().SetProof(ctx, pr.rec.TxID, pr.height, pr.blockHash, pr.mpBytes, pr.root); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: batch mined: set proof: %w", err)
		}
	}
	if err := p.setArcadeStatusBatch(ctx, recs); err != nil {
		return err
	}
	if err := p.meta.Transactions().BulkUpdateStatusByTxIDs(ctx, txids, wdk.TxStatusCompleted,
		wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed, wdk.TxStatusCompleted); err != nil {
		return fmt.Errorf("storage: batch mined: mark completed: %w", err)
	}
	if err := p.meta.Transactions().BulkClearInputBEEFByTxIDs(ctx, txids); err != nil {
		return fmt.Errorf("storage: batch mined: clear input beef: %w", err)
	}
	ops, err := p.changeOutpointsByTxIDs(ctx, txids)
	if err != nil {
		return err
	}
	if len(ops) > 0 {
		if _, err := p.utxo.Promote(ctx, ops, utxostore.TierMined); err != nil {
			return fmt.Errorf("storage: batch mined: promote change: %w", err)
		}
	}
	// Remove the spent inputs of each mined (terminal) tx. Prefer ONE batched
	// delete-by-outpoint over the resolved inputs (O(inputs), single-record PK
	// deletes) — the old per-tx RemoveSpentBy runs a full aerospike set-scan per
	// tx (O(set-size)), which dominates MINED apply under a large block. Proofs
	// whose inputs could not be resolved (no local raw tx) fall back to the scan.
	// Remove(force) is idempotent (no-ops on already-removed outpoints), so a
	// re-apply is safe.
	allSpent := make([]utxostore.Outpoint, 0, len(proofs))
	var scanFallback []*minedProof
	for i := range proofs {
		if len(proofs[i].spentOps) == 0 {
			scanFallback = append(scanFallback, &proofs[i])
			continue
		}
		allSpent = append(allSpent, proofs[i].spentOps...)
	}
	if len(allSpent) > 0 {
		if err := p.utxo.Remove(ctx, allSpent, true); err != nil {
			return fmt.Errorf("storage: batch mined: remove spent inputs: %w", err)
		}
	}
	for _, pr := range scanFallback {
		if _, err := p.utxo.RemoveSpentBy(ctx, pr.txidHash); err != nil {
			return fmt.Errorf("storage: batch mined: remove spent inputs %s: %w", pr.rec.TxID, err)
		}
	}
	return nil
}

// applyRejectedBatch marks each REJECTED-class tx suspect — the batched form of
// applyRejected. Looping the existing per-tx MarkSuspect is acceptable (low
// frequency). Must run inside p.meta.Do.
func (p *Provider) applyRejectedBatch(ctx context.Context, recs []arcade.TxRecord) error {
	for _, rec := range recs {
		p.logRejection(ctx, rec.TxID, string(rec.Status), rec.ExtraInfo, "status_batch")
		if err := p.meta.KnownTx().MarkSuspect(ctx, rec.TxID, p.now(), rec.CompetingTxs, string(rec.Status), rec.ExtraInfo); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: batch mark suspect %s: %w", rec.TxID, err)
		}
	}
	return nil
}

// setArcadeStatusBatch records the arcade wire status for a set of records,
// grouping by status value so each distinct value is a single UPDATE. Must run
// inside p.meta.Do.
func (p *Provider) setArcadeStatusBatch(ctx context.Context, recs []arcade.TxRecord) error {
	if len(recs) == 0 {
		return nil
	}
	byStatus := make(map[arcade.Status][]string)
	for _, rec := range recs {
		byStatus[rec.Status] = append(byStatus[rec.Status], rec.TxID)
	}
	for status, txids := range byStatus {
		if err := p.meta.KnownTx().BulkSetArcadeStatus(ctx, txids, string(status)); err != nil {
			return fmt.Errorf("storage: batch set arcade status: %w", err)
		}
	}
	return nil
}

// changeOutpointsByTxIDs returns the change outpoints for all of txids in one
// bulk query. It is the ONE by-txid implementation:
// [Provider.changeOutpointsByTxID] is a one-element call of it, and
// [Provider.changeOutpoints] keeps its own query only because it is scoped to a
// (userID, transactionID) rather than to a txid — see that function.
func (p *Provider) changeOutpointsByTxIDs(ctx context.Context, txids []string) ([]utxostore.Outpoint, error) {
	if len(txids) == 0 {
		return nil, nil
	}
	rows, err := p.meta.Outputs().FindChangeOutputsByTxIDs(ctx, txids)
	if err != nil {
		return nil, fmt.Errorf("storage: find change outputs (bulk): %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	ops := make([]utxostore.Outpoint, 0, len(rows))
	for i := range rows {
		// All change baskets, not just the default (see changeOutpointsByTxID):
		// SEEN is now the only promotion point, so pool/reserve fuel coins must
		// be promoted here too or they stay stranded at TierSending.
		if rows[i].Basket == nil || rows[i].TxID == nil {
			continue
		}
		hash, err := chainhash.NewHashFromHex(*rows[i].TxID)
		if err != nil {
			return nil, fmt.Errorf("storage: parse change txid %s: %w", *rows[i].TxID, err)
		}
		ops = append(ops, utxostore.Outpoint{TxID: *hash, Vout: rows[i].Vout})
	}
	return ops, nil
}

// recordTxIDs extracts the txids of a record slice, preserving order.
func recordTxIDs(recs []arcade.TxRecord) []string {
	out := make([]string, len(recs))
	for i := range recs {
		out[i] = recs[i].TxID
	}
	return out
}

// resendGrace is how long a never-broadcast tx must sit before the SendWaiting
// sweep re-drives it. It protects a freshly-created 'unprocessed' tx (whose own
// ProcessAction is about to broadcast it) from being double-sent by the sweep;
// anything unbroadcast longer than this was stranded (e.g. an open arcade
// circuit breaker skipped the send) and is safe to re-broadcast.
//
// It is the SINGLE definition of "stranded" for this package, and both sides of
// the recovery path must read it from here: the SELECTOR
// ([metastore.KnownTxRepo.FindResendable], which decides what the sweep is
// handed) and the TAKER ([metastore.KnownTxRepo.ReclaimStaleSend], via
// [Provider.claimForBroadcast], which decides what may actually be re-driven).
// A second cutoff defined anywhere else would let the two disagree, and the
// disagreement has a direction either way: a taker more eager than the selector
// steals live sends, a taker more patient than it never recovers dead ones
// while the selector keeps re-offering them.
const resendGrace = 20 * time.Second

// SendWaitingTransactions broadcasts up to limit re-drivable known txs through
// the oracle and, on acceptance, commits the same wallet-state transition the
// synchronous 202 path does (inputs spent, change promoted to TierUnproven,
// statuses advanced). Its work list is both the delayed queue (status 'unsent')
// and transactions stranded pre-broadcast ('unprocessed', never sent) — so a
// tx the circuit breaker short-circuited while open self-heals on a later
// cycle once arcade is reachable. Backpressure/opaque failures leave the tx for
// the next cycle; a tx-level rejection marks it suspect.
func (p *Provider) SendWaitingTransactions(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	waiting, err := p.meta.KnownTx().FindResendable(ctx, resendGrace, limit)
	if err != nil {
		return fmt.Errorf("storage: find resendable: %w", err)
	}
	if len(waiting) == 0 {
		return nil
	}

	// Resolve the whole sweep's ancestry in ONE query. The blob now lives on the
	// transactions row (see Provider.inputBEEFFor); the loop below fans out at
	// sendConcurrency, so a per-transaction lookup here would trade the write
	// saving this indirection exists for against read latency on the same path.
	beefs, err := p.inputBEEFForBatch(ctx, waiting)
	if err != nil {
		return err
	}

	// Broadcast the batch with bounded concurrency. Each POST /tx is an arcade
	// round-trip; done sequentially the drainer cannot keep pace with a
	// high-throughput creation rate and the delayed queue grows without bound.
	// Delayed self-payments are independent (each spends already-broadcast
	// funding), so parallel broadcast is safe; per-tx errors are logged and the
	// tx is left for the next cycle rather than aborting the batch.
	conc := p.sendConcurrency
	if conc < 1 {
		conc = 1
	}
	var sent, failed atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(conc)
	for i := range waiting {
		kt := &waiting[i]
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			if serr := p.sendOneWaiting(gctx, kt, beefs[kt.TxID]); serr != nil {
				failed.Add(1)
				p.logger.WarnContext(gctx, "send waiting: broadcast failed, will retry next cycle",
					slog.String("txid", kt.TxID), slog.String("error", serr.Error()))
				return nil
			}
			sent.Add(1)
			return nil
		})
	}
	if werr := g.Wait(); werr != nil {
		return werr
	}
	p.logger.DebugContext(ctx, "send waiting batch complete",
		slog.Int64("broadcast", sent.Load()), slog.Int64("failed", failed.Load()),
		slog.Int("batch", len(waiting)), slog.Int("concurrency", conc))
	return nil
}

// SweepStaleReservations reclaims funding reservations older than olderThan
// whose transaction can no longer be sent — inputs leaked by never-completed
// CreateActions (failed fuel fan-outs, or payments built but never broadcast
// because the circuit breaker was open), plus the residue of aborts whose utxo
// half never completed.
//
// It is FENCE-FIRST, and that is audit P0-4's completion. The sweep used to
// release the reservation and leave the transaction row exactly where it found
// it — typically at 'unsigned', which is a status ProcessAction ADMITS. A
// signer arriving a second later would then be handed a live reference whose
// inputs the funder had already re-lent, and storage would happily store its
// bytes as broadcastable: a double spend the wallet authored itself, caused by
// its own janitor. The old guard against that — a "is this transaction still
// resendable" check, deleted with this change — could not close it, because it
// only asked whether broadcastable bytes ALREADY existed; the late signer's do
// not exist yet. (It was also a hand-rolled re-encoding of FindResendable that
// had drifted strictly broader, which pinned the inputs of terminal rows
// forever: audit P2-10.)
//
// So nothing is released until the transaction has been killed. Per ref, the
// disposition comes from the transaction row, not from the coin:
//
//   - NO transaction row — a funder-internal reservation (a fan-out that died
//     before any action existed). Nothing broadcastable can exist without a
//     row, so a direct release is safe and is the whole job.
//   - an ABANDONED status ([sweepAbortableStatuses]) — [Provider.abortTxRow],
//     the same fence-first core AbortAction and AbortAbandoned use: CAS to
//     'aborted' and fence the raw tx in ONE metadata commit, then unpin and
//     release. Losing that CAS ([wdk.ErrNotAbortableAction]) means a live
//     transition got there first, and skipping is then the correct outcome.
//   - already ABORTED — the healer arm (see below).
//   - anything else (unprocessed / nonfinal / sending / unproven / completed /
//     failed) — never touched. Those have network evidence or belong to another
//     owner: SendWaiting sends the delayed ones, the reconciler releases
//     provably-dead suspects, and the send path releases on rejection.
//
// The abortTxRow calls are made OUTSIDE any [metastore.Store.Do], which is that
// function's documented caller invariant — an ambient transaction would collapse
// its two phases and re-open P0-5 from the outside. This loop therefore holds no
// unit of work across iterations, which also keeps one bad ref from failing the
// batch.
//
// The listing is the pinned-INCLUSIVE one, and it has to be. A pinned
// reservation is one the store believes is in flight, and hiding those from
// janitors is what makes an ordinary sweep safe — but "pinned" is a PROXY for
// "still broadcastable" and this sweep establishes the real property itself, by
// fencing before it touches a coin. Without the inclusive listing the aborted
// arm below would be unreachable: those coins are pinned by definition, so the
// ordinary listing hides precisely the rows that need healing.
func (p *Provider) SweepStaleReservations(ctx context.Context, olderThan time.Time, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	refs, err := p.utxo.FindStaleReservationsIncludingPinned(ctx, olderThan, limit)
	if err != nil {
		return fmt.Errorf("storage: find stale reservations: %w", err)
	}
	// HEAD-OF-LINE NOTE. The listing is one oldest-first PAGE, and the inclusive
	// listing put rows in it that the ordinary one filtered out — pinned holds
	// belonging to live transactions, which this sweep skips on every tick and
	// will keep skipping. They are therefore capable of filling the page and
	// starving the work behind them: during an arcade outage longer than the
	// reservation TTL, more than `limit` queued delayed sends all age past the
	// cutoff, sort ahead of everything younger, and a funder orphan or a parked
	// abort residue created afterwards is not reached until they drain. This is
	// a NEW failure mode of the inclusive listing — the pin filter used to
	// remove those rows before the LIMIT applied.
	//
	// It is left as an observable condition rather than a pagination redesign,
	// on three grounds: it is self-healing (SendWaiting drains the queue and the
	// rows leave the listing by being spent), it costs availability only (the
	// starved work is coins coming back late, never a coin freed wrongly), and
	// the fix would be a cursor over a set that mutates under it. What the
	// summary below gives an operator is the number that distinguishes "quiet
	// tick" from "the page is full of work I am not allowed to do":
	// skipped_in_flight at or near the page limit, tick after tick, is this
	// condition.
	var fenced, healed, orphans, inFlight int
	for i := range refs {
		ref := &refs[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		switch outcome, err := p.sweepStaleReservation(ctx, ref); {
		case err != nil:
			p.logger.WarnContext(ctx, "sweep stale reservations: reclaim failed",
				slog.String("reservation", ref.Reservation),
				slog.Int64("user_id", ref.UserID), slog.String("error", err.Error()))
		case outcome == sweptByAbort:
			fenced++
		case outcome == sweptByHeal:
			healed++
		case outcome == sweptOrphan:
			orphans++
		case outcome == sweptSkippedInFlight:
			inFlight++
		}
	}
	// Reported when the page held ANY of the four, in-flight skips included: a
	// tick that reclaimed nothing because every slot was occupied by a live
	// transaction is precisely the tick worth seeing, and it would otherwise be
	// silent.
	if fenced+healed+orphans+inFlight > 0 {
		p.logger.DebugContext(ctx, "sweep stale reservations complete",
			slog.Int("scanned", len(refs)), slog.Int("limit", limit),
			slog.Int("aborted", fenced), slog.Int("healed", healed),
			slog.Int("funder_orphans", orphans), slog.Int("skipped_in_flight", inFlight))
	}
	return nil
}

// sweepAbortableStatuses are the transaction statuses the stale-reservation
// sweep may kill: the ones with NO other owner. It is deliberately narrower
// than [abortableStatuses], which is what a USER may abort — a user aborting
// their own delayed payment is a decision, while a janitor doing it on a timer
// is a canceled payment nobody asked for.
//
// It is exactly the set [metastore.TransactionsRepo.FindAbandonedBefore]
// selects, so the two janitors agree on what "abandoned" means and only differ
// in the side they approach it from (a transaction's age there, a coin's hold
// here) and in how long they wait. The two statuses it leaves out of the
// abortable four are both owned elsewhere at any age:
//
//   - 'unprocessed' is a DELAYED send. Its bytes are stored and SendWaiting
//     re-drives them, so a slow send — an open circuit breaker, an arcade
//     outage — must outlive the reservation TTL rather than be canceled by it.
//   - 'nonfinal' is waiting on an nLockTime it cannot control.
//
// Their coins are pinned, which is what kept them out of the ordinary listing;
// with the pinned-inclusive listing the sweep SEES them, so the refusal has to
// be explicit here instead of a side effect of the query.
var sweepAbortableStatuses = map[wdk.TxStatus]bool{
	wdk.TxStatusUnsigned: true,
	wdk.TxStatusNoSend:   true,
}

// sweepOutcome names what one stale ref cost the sweep, for the batch counters.
type sweepOutcome int

const (
	sweptSkipped         sweepOutcome = iota // left alone: terminal, unreadable, or lost a race
	sweptByAbort                             // fenced and released via abortTxRow
	sweptByHeal                              // already-fenced residue, unpinned and released
	sweptOrphan                              // no transaction row: released directly
	sweptSkippedInFlight                     // owned by a live transaction: the head-of-line population
)

// sweepStaleReservation reclaims ONE stale reservation, choosing its arm from
// the transaction that holds it. See [Provider.SweepStaleReservations] for why
// the transaction, and not the coin, is the thing consulted.
func (p *Provider) sweepStaleReservation(ctx context.Context, ref *utxostore.ReservationRef) (sweepOutcome, error) {
	userID := int(ref.UserID)
	txRow, found, err := p.meta.Transactions().FindByReference(ctx, userID, ref.Reservation)
	if err != nil {
		return sweptSkipped, fmt.Errorf("storage: find transaction for reservation: %w", err)
	}

	switch {
	case !found:
		// A reservation with no transaction row cannot back anything
		// broadcastable: the only path that stores sendable bytes is
		// processNewTx, and it starts by resolving this very reference to a row.
		// So this is a funder-internal hold whose action never came into
		// existence, and a plain release is both safe and sufficient.
		//
		// Except when it is PINNED, which is unrepresentable by the same
		// argument — the pin is written inside processNewTx, after the row was
		// found. Seeing one means the two stores disagree about a transaction
		// that exists on neither side of the disagreement, so the sweep reports
		// it and touches nothing: a stuck coin is recoverable by hand, a coin
		// freed on a false premise is not.
		switch state := p.refPinState(ctx, ref); state {
		case refPinned:
			p.logger.ErrorContext(ctx, "sweep stale reservations: pinned reservation has no transaction row",
				slog.String("reservation", ref.Reservation), slog.Int64("user_id", ref.UserID),
				slog.Int("outpoints", len(ref.Outpoints)))
			return sweptSkipped, nil
		case refPinUnreadable:
			// Could not establish either way. Leave it: the next tick re-reads,
			// and the reservation is not going anywhere in the meantime.
			p.logger.WarnContext(ctx, "sweep stale reservations: could not read the pin state of an orphan hold",
				slog.String("reservation", ref.Reservation), slog.Int64("user_id", ref.UserID))
			return sweptSkipped, nil
		case refUnpinned:
		}
		if _, rerr := p.utxo.ReleaseReservation(ctx, ref.UserID, ref.Reservation); rerr != nil {
			return sweptSkipped, fmt.Errorf("storage: release orphan reservation: %w", rerr)
		}
		return sweptOrphan, nil

	case sweepAbortableStatuses[txRow.Status]:
		// The fence. abortTxRow is shared with AbortAction and AbortAbandoned, so
		// a swept action dies exactly the way a user-aborted one does — including
		// the known-tx fence, which is what stops a late signer or a queued
		// broadcaster from using the coins this is about to hand back.
		if aerr := p.abortTxRow(ctx, userID, txRow); aerr != nil {
			if errors.Is(aerr, wdk.ErrNotAbortableAction) {
				// Lost to a live transition: a signer, a broadcaster's claim, or
				// another janitor. Whoever won owns the coins now.
				return sweptSkipped, nil
			}
			return sweptSkipped, fmt.Errorf("storage: abort stale reservation: %w", aerr)
		}
		return sweptByAbort, nil

	case txRow.Status == wdk.TxStatusAborted:
		// The healer, and the closure of abortViaOutbox's KNOWN RESIDUAL: an
		// abort whose utxo half never landed (a parked ABORT_RELEASE row, or a
		// crash between the unpin and the release) leaves coins pinned and
		// reserved behind a transaction that is already fenced. Nothing else
		// reclaims them — the outbox row is past MaxOutboxAttempts, and
		// re-aborting cannot help because the CAS refuses an aborted row.
		//
		// This runs DIRECTLY in both modes, and the Mode B reasoning is worth
		// stating because P0-5 makes direct utxo writes look suspect: what P0-5
		// forbids is committing a utxo write while the METADATA half it belongs
		// to is still provisional. There is no metadata half here. The fence is
		// already durable — it is why this arm was chosen — and doAbortRelease
		// writes nothing but the utxostore, so there is no atomicity to lose and
		// nothing an outbox row could make safer. Both of its ops are
		// token-guarded and idempotent, so a partial failure simply comes back
		// on the next tick.
		if rerr := p.doAbortRelease(ctx, ref.UserID, ref.Reservation); rerr != nil {
			return sweptSkipped, fmt.Errorf("storage: heal aborted reservation: %w", rerr)
		}
		// Retire the intent this just carried out, if there is one. The op has
		// been performed, which is precisely what MarkDone records, and doing it
		// AFTER the release means a failure above leaves the row exactly where it
		// was. Two reasons it matters beyond tidiness: a PENDING row would
		// otherwise replay the same two ops on the next drain for nothing, and a
		// PARKED one would keep the parked_total gauge reporting stranded funds
		// that are no longer stranded — an alert with no way to clear. Mode A has
		// no outbox row at all, so this is a no-op there.
		if derr := p.meta.Outbox().MarkDone(ctx, abortOutboxKey(ref.Reservation), opAbortRelease, 0); derr != nil {
			p.logger.WarnContext(ctx, "sweep stale reservations: could not retire the healed abort intent",
				slog.String("reservation", ref.Reservation), slog.String("error", derr.Error()))
		}
		p.logger.InfoContext(ctx, "sweep stale reservations: reclaimed the residue of an aborted action",
			slog.String("reservation", ref.Reservation), slog.Int64("user_id", ref.UserID),
			slog.Int("outpoints", len(ref.Outpoints)))
		return sweptByHeal, nil

	case txRow.Status == wdk.TxStatusCompleted:
		// A completed transaction's inputs are SPENT, so they should not be
		// holding a live reservation at all. Reaching here means a spend was
		// recorded against the wallet ledger but never against these coins (audit
		// P1-3's shape) — they are stuck, and the sweep is the wrong tool: their
		// transaction is on the network, so releasing them would offer the funder
		// coins that are already gone. Loudly, because nothing else will say it.
		p.logger.WarnContext(ctx, "sweep stale reservations: completed transaction still holds a live reservation",
			slog.String("reservation", ref.Reservation), slog.Int64("user_id", ref.UserID),
			slog.Int("outpoints", len(ref.Outpoints)))
		return sweptSkipped, nil

	default:
		// unprocessed / nonfinal / sending / unproven / failed: either the
		// network has the bytes, or a work list does, or another owner (the
		// reconciler, the send path) is responsible for the release. Never this
		// sweep — see [sweepAbortableStatuses] for why the delayed ones are on
		// this side of the line even though a user could abort them.
		//
		// This is also the arm that can crowd a page out; it is counted
		// separately for that reason (see the head-of-line note on the caller).
		p.logger.DebugContext(ctx, "sweep stale reservations: reservation belongs to a live transaction",
			slog.String("reservation", ref.Reservation), slog.String("status", string(txRow.Status)))
		return sweptSkippedInFlight, nil
	}
}

// refPinState is what [Provider.refPinState] could establish about a ref's
// rows. The third value is not a detail: the caller acts on this, and "I could
// not tell" must not be spelled the same way as either answer.
type refPinState int

const (
	refUnpinned      refPinState = iota // every row read, none pinned
	refPinned                           // at least one row is pinned
	refPinUnreadable                    // a row could not be read; nothing may be concluded
)

// refPinState reports whether any outpoint of the ref is pinned. It exists only
// for the impossible-orphan check in [Provider.sweepStaleReservation] and runs
// only on that arm, which is rare (a create that died between the funder's
// claim and its metadata) and holds a handful of outpoints.
//
// A MISSING row is benign and does not stop the release. Between the listing
// and this read the row can legitimately have gone: RemoveByMintTx, a mined
// tx's RemoveSpentBy, or an operator Remove. A vanished coin is not a pinned
// coin, and it is not evidence of the corruption the caller is looking for — so
// it is skipped rather than counted as a reason to refuse. The release that
// follows is idempotent and simply will not find it either.
//
// Any OTHER read error is unreadable, not unpinned: the caller must leave the
// coin alone when the store cannot say what state it is in.
func (p *Provider) refPinState(ctx context.Context, ref *utxostore.ReservationRef) refPinState {
	for _, op := range ref.Outpoints {
		u, err := p.utxo.Get(ctx, op)
		switch {
		case errors.Is(err, &utxostore.NotFoundError{}):
			p.logger.DebugContext(ctx, "sweep stale reservations: orphan hold row already gone",
				slog.String("reservation", ref.Reservation), slog.String("outpoint", op.String()))
			continue
		case err != nil:
			p.logger.WarnContext(ctx, "sweep stale reservations: pin read failed",
				slog.String("reservation", ref.Reservation), slog.String("outpoint", op.String()),
				slog.String("error", err.Error()))
			return refPinUnreadable
		case u.Pinned:
			return refPinned
		}
	}
	return refUnpinned
}

// sendOneWaiting EF-encodes and broadcasts one delayed tx, then commits the
// outcome.
//
// It CLAIMS the row before the POST — see [Provider.claimForBroadcast]. The
// work list it is driven from is a SELECT, so between that snapshot and this
// POST another instance (or the synchronous path) can take the same row; losing
// that race must cost one silent no-op, never a second POST. kt.Status is the
// snapshot's status and picks the arm: rows off the graced 'sending' arm are
// crash recoveries and are taken by re-claim, everything else by first claim.
func (p *Provider) sendOneWaiting(ctx context.Context, kt *metastore.KnownTx, inputBEEF []byte) error {
	txid := kt.TxID
	if len(kt.RawTx) == 0 {
		return fmt.Errorf("no stored raw tx for %s", txid)
	}
	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		return fmt.Errorf("parse stored raw tx %s: %w", txid, err)
	}
	if err := p.hydrateInputs(tx, inputBEEF); err != nil {
		return err
	}
	if ok, verr := p.scripts.VerifyScripts(ctx, tx); verr != nil || !ok {
		return fmt.Errorf("script verification failed for %s: %w", txid, verr)
	}
	ef, err := tx.EF()
	if err != nil {
		return fmt.Errorf("EF-encode %s: %w", txid, err)
	}
	claim, err := p.claimForBroadcast(ctx, txid, kt.Status)
	var fenced *notBroadcastableError
	switch {
	case errors.As(err, &fenced):
		// The row went fenced or terminal between FindResendable's SELECT and
		// this CAS — an abort landed, or a rejection did. This is NOT a failure
		// to report: the sweep's error path logs "will retry next cycle", and
		// there is no next cycle for these rows, because FindResendable excludes
		// every one of these statuses at any age. Counting it as a failed
		// broadcast would put a permanent, self-inflating number in front of an
		// operator for an outcome that is the fence working exactly as designed.
		p.logger.DebugContext(ctx, "send waiting: row left the broadcastable set mid-sweep",
			slog.String("txid", txid), slog.String("status", string(fenced.status)))
		return nil
	case err != nil:
		// Transient by contrast (an unreadable status, a vanished row): those DO
		// deserve the warning and the failed count.
		return err
	}
	if claim != sendClaimOwned {
		// Another instance claimed it this tick, or the bytes are already on the
		// network. Either way this sweep has nothing to do for this row, and that
		// is a normal outcome — reporting it as a failure would make a healthy
		// multi-instance deployment look like a broken one.
		return nil
	}
	res, berr := p.broadcastWithBackpressure(ctx, txid, ef)
	if berr != nil {
		// Backpressure exhausted or opaque/transport failure: the row stays at
		// the 'sending' the claim put it in, which is FindResendable's graced
		// recovery arm. The re-claim's clock re-stamp is what makes that a
		// once-per-grace retry rather than a once-per-tick POST loop.
		return berr
	}
	if res.Rejected {
		// A tx-level rejection: mark suspect (the reconciler owns input release),
		// matching the async-reject asymmetry. The 4xx body is the whole diagnosis
		// and it exists only here, so it goes to the log and to the row before
		// anything else happens.
		p.logRejection(ctx, txid, string(res.Status), res.ExtraInfo, "delayed_broadcast_4xx")
		if err := p.meta.KnownTx().MarkSuspect(ctx, txid, p.now(), nil, string(res.Status), res.ExtraInfo); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("mark suspect %s: %w", txid, err)
		}
		return nil
	}
	// The apply resolves its own rows (cross-user) and refuses to commit when
	// there are none. A hard error here is logged by SendWaitingTransactions and
	// retried next tick: nothing above rolled the status forward, so the known tx
	// is still at the 'sending' the claim put it in, which is FindResendable's
	// graced recovery arm.
	return p.applyAcceptedBroadcast(ctx, txid, tx)
}

// AbortAbandoned aborts up to limit never-broadcast unsigned/nosend txs created
// at or before olderThan: released reservation, removed change, restored input
// spend-history, status → aborted (reusing the AbortAction core). A tx that
// raced a concurrent transition out of an abortable status is skipped.
func (p *Provider) AbortAbandoned(ctx context.Context, olderThan time.Time, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	abandoned, err := p.meta.Transactions().FindAbandonedBefore(ctx, olderThan, limit)
	if err != nil {
		return fmt.Errorf("storage: find abandoned: %w", err)
	}
	for i := range abandoned {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := &abandoned[i]
		if err := p.abortTxRow(ctx, row.UserID, row); err != nil {
			if errors.Is(err, wdk.ErrNotAbortableAction) {
				continue // raced a concurrent transition
			}
			p.logger.WarnContext(ctx, "abort abandoned failed",
				slog.String("reference", string(row.Reference)), slog.String("error", err.Error()))
		}
	}
	return nil
}

// SynchronizeTransactionStatuses is the poll fallback: for non-terminal known
// txs untouched for at least syncStaleness, it fetches the authoritative record
// via GetTx and routes it through ApplyStatusUpdate. This covers an SSE outage
// and the cold-start terminal gap (a fresh SSE connect replays only
// non-terminal statuses, so a tx that went terminal while disconnected is only
// learned by polling).
//
// It polls TWO work lists and applies their union:
//
//   - the staleness list (least-recently-polled non-terminal txs), and
//   - the REPAIR list: txs with no arcade status at all, i.e. rows on which the
//     local ledger has provably diverged from arcade (arcade has a status for
//     every tx it has ever seen). Those get their own query because at high
//     throughput the staleness list is far longer than any batch limit, so a
//     stranded row that happens to sort behind it would never be selected. This
//     is the sweep that drains a "23,745 rows with an empty arcade_status" state
//     instead of letting it sit frozen forever.
//
// The repair list is the one that has to KEEP UP: when it does not fit in a
// single page the sweep keeps paging it (see drainRepairBacklog), so recovery
// scales with the size of the divergence instead of trickling at a fixed rate.
func (p *Provider) SynchronizeTransactionStatuses(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	cutoff := p.now().Add(-syncStaleness)
	stale, err := p.meta.KnownTx().FindByStatusOlderThan(ctx, cutoff, limit, pollableStatuses...)
	if err != nil {
		return fmt.Errorf("storage: find stale non-terminal: %w", err)
	}
	stranded, err := p.meta.KnownTx().FindMissingArcadeStatus(ctx, cutoff, limit, pollableStatuses...)
	if err != nil {
		return fmt.Errorf("storage: find txs missing an arcade status: %w", err)
	}

	if err := p.pollAndApply(ctx, mergeKnownTxs(stale, stranded)); err != nil {
		return err
	}
	if len(stranded) == 0 {
		return nil
	}
	return p.drainRepairBacklog(ctx, cutoff, limit, len(stranded))
}

// drainRepairBacklog reports the repair sweep and, when the first page came back
// FULL, keeps paging: a full page means the divergence is deeper than one page,
// and one page per tick will not close it. At the observed 4,000 rows per ≈60s
// tick a 269k backlog needs ~65 minutes, and the run that produced that number
// was rescued by a block catchup rather than by this path.
//
// It scales the page count with the measured backlog, bounded by maxRepairPages
// and repairSweepBudget so a deep backlog cannot turn the sweep into the load.
// Paging works because pollAndApply stamps last_polled_at on every row it takes
// BEFORE anything else, and the repair query orders by that stamp — so each page
// returns rows the previous one did not.
//
// firstPage is what the caller already repaired, counted here so the log line
// reports the sweep's whole effort.
func (p *Provider) drainRepairBacklog(ctx context.Context, cutoff time.Time, limit, firstPage int) error {
	// The backlog SIZE (not just this page of it) is what tells an operator
	// whether the repair is draining or frozen, and it is what sets the pace.
	total, err := p.meta.KnownTx().CountMissingArcadeStatus(ctx, pollableStatuses...)
	if err != nil {
		p.logger.WarnContext(ctx, "poll: count of txs missing an arcade status failed", slog.String("error", err.Error()))
		total = 0
	}

	// total is measured AFTER the caller's page applied, so it is what is still
	// owed; size the extra paging to exactly that, capped.
	pages := 0
	if firstPage >= limit && total > 0 {
		pages = min((total+limit-1)/limit, maxRepairPages)
	}

	deadline := p.now().Add(repairSweepBudget)
	repaired := firstPage
	for range pages {
		if ctx.Err() != nil || !p.now().Before(deadline) {
			break
		}
		page, ferr := p.meta.KnownTx().FindMissingArcadeStatus(ctx, cutoff, limit, pollableStatuses...)
		if ferr != nil {
			return fmt.Errorf("storage: find txs missing an arcade status: %w", ferr)
		}
		if len(page) == 0 {
			break
		}
		if aerr := p.pollAndApply(ctx, page); aerr != nil {
			return aerr
		}
		repaired += len(page)
		if len(page) < limit {
			break // the list is drained
		}
	}

	p.logger.WarnContext(ctx, "poll: repairing transactions with no arcade status",
		slog.Int("repairing", repaired),
		slog.Int("pages", pages+1),
		slog.Int("arcade_status_missing_total", total))
	return nil
}

// mergeKnownTxs concatenates two work lists, dropping txids already present in
// the first (the staleness list and the repair list overlap by construction —
// a stranded tx is usually stale too — and polling a txid twice in one sweep is
// pure waste).
func mergeKnownTxs(a, b []metastore.KnownTx) []metastore.KnownTx {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a))
	for i := range a {
		seen[a[i].TxID] = struct{}{}
	}
	out := a
	for i := range b {
		if _, dup := seen[b[i].TxID]; dup {
			continue
		}
		seen[b[i].TxID] = struct{}{}
		out = append(out, b[i])
	}
	return out
}

// CheckProofs is the proof poll fallback: for broadcast-accepted-but-unproven
// known txs it fetches the record via GetTx (which carries the full BUMP) and
// routes it through ApplyStatusUpdate, promoting any that have since mined.
//
// It also carries the MINED-REPAIR BACKFILL (see
// [Provider.repairMinedFenced]). The two ride together because they are the
// same job at two ends of the same pipe: this task exists to reconcile the
// wallet with proofs it has not applied, and a written-off transaction the
// network mined is exactly that — a proof the wallet refused. Sharing the task
// also means the backfill inherits its lease, its cadence and its batch limit
// rather than adding a fifth scheduled sweep for a condition that should be
// empty.
//
// The backfill's outcome never fails the proof poll: they are independent work
// lists, and a repair that cannot complete must not stop unproven transactions
// from being promoted.
func (p *Provider) CheckProofs(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	unproven, err := p.meta.KnownTx().FindByStatusOlderThan(ctx, p.now(), limit, unprovenStatuses...)
	if err != nil {
		return fmt.Errorf("storage: find unproven: %w", err)
	}
	perr := p.pollAndApply(ctx, unproven)
	if rerr := p.repairMinedFenced(ctx, limit); rerr != nil {
		p.logger.ErrorContext(ctx, "repair: mined-on-written-off backfill failed",
			slog.String("error", rerr.Error()))
	}
	return perr
}

// repairMinedFenced is the BACKFILL half of the mined repair: the healer for
// rows on which the divergence is already durable rather than arriving as an
// event.
//
// It exists because nothing else can reach them. Two populations sit in that
// state, and neither appears on any poll work list — every one of those is keyed
// on a non-terminal known-tx status:
//
//   - PRE-REPAIR ROWS. Before this change a MINED on an aborted row ran the
//     ordinary apply, whose SetProof completed the known tx and stored the proof
//     while the transaction row stayed 'aborted', no input was ever fact-spent
//     and the change stayed removed. A deployment carries one such row per
//     mined-after-abort that ever happened, and its known tx now reads
//     'completed' — the most thoroughly ignored status there is.
//   - DEFERRED REPAIRS. A live repair whose BUMP could not be verified yet (our
//     header view lagging arcade), or whose commit failed, writes no wallet
//     state but does leave arcade's verdict on the row.
//
// One signature covers both: arcade says MINED/IMMUTABLE, the wallet transaction
// says written off. It also picks up a case neither the guard nor the repair
// route reaches — a SYNCHRONOUS 4xx rejection (which fails the transaction row
// but leaves the known tx merely 'suspectFailed', so a later MINED takes the
// ORDINARY apply) whose transaction-row CAS then finds 'failed' outside its
// from-set and skips. The known tx completes, the row stays failed, and the
// inputs stay reserved: same divergence, same signature, healed here. Rows are repaired from their STORED proof where one exists —
// it was header-verified when it was written, and this re-verifies it — and
// otherwise from a fresh GetTx, which is what carries the BUMP for a deferred
// row that never got one.
//
// It SELF-QUIESCES: a successful repair moves the transaction row to
// 'completed', which is not in the finder's predicate, so a drained backlog
// costs one bounded index scan per tick and nothing else. A row that cannot be
// repaired (a permanently unverifiable proof) is retried on each tick and stays
// loudly visible, which for a transaction arcade claims is mined and the wallet
// has written off is the correct amount of noise.
func (p *Provider) repairMinedFenced(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	diverged, err := p.meta.KnownTx().FindMinedOnDeadTransactions(ctx, minedArcadeStatuses, limit)
	if err != nil {
		return fmt.Errorf("storage: find mined-on-written-off transactions: %w", err)
	}
	if len(diverged) == 0 {
		return nil
	}
	p.logger.WarnContext(ctx, "repair: transactions the wallet wrote off that arcade reports on chain",
		slog.Int("found", len(diverged)), slog.Int("limit", limit))

	// ANTI-STARVATION, and it is not optional here. Three of the four outcomes
	// below write NOTHING to the known tx — arcade has forgotten the
	// transaction, arcade has revised its verdict, or the row could not be
	// repaired — and the finder orders by this stamp. Without it such a row keeps
	// its place at the head of every page and hides the entire backlog behind
	// itself, which is the failure the poll fallbacks already learned the hard
	// way (see [metastore.KnownTxRepo.MarkPolled]). Recorded BEFORE any of the
	// work, so it holds however the pass ends.
	attempted := make([]string, len(diverged))
	for i := range diverged {
		attempted[i] = diverged[i].TxID
	}
	if merr := p.meta.KnownTx().MarkPolled(ctx, attempted); merr != nil {
		p.logger.WarnContext(ctx, "repair: failed to record the backfill attempt; the work list may not advance",
			slog.Int("batch", len(attempted)), slog.String("error", merr.Error()))
	}

	var repaired, deferredN, disagreed, failed int
	for i := range diverged {
		if err := ctx.Err(); err != nil {
			return err
		}
		kt := &diverged[i]
		outcome, rerr := p.repairOneMinedFenced(ctx, kt)
		switch {
		case rerr != nil:
			failed++
			p.logger.WarnContext(ctx, "repair: backfill of one transaction failed; retrying next tick",
				slog.String("txid", kt.TxID), slog.String("error", rerr.Error()))
		case outcome == repairApplied:
			repaired++
		case outcome == repairDeferred:
			deferredN++
		case outcome == repairDisagreed:
			disagreed++
		}
	}
	// The counters are split because they mean opposite things to an operator and
	// only one of them is progress. A single "repaired" that also counted the
	// deferred and the disagreed would report a healthy drain on a pass that
	// healed nothing at all — the same illusion the arcade-status repair sweep
	// was diagnosed with.
	p.logger.InfoContext(ctx, "repair: mined-on-written-off backfill pass complete",
		slog.Int("attempted", len(diverged)), slog.Int("repaired", repaired),
		slog.Int("deferred", deferredN), slog.Int("disagreed", disagreed), slog.Int("failed", failed))
	return nil
}

// repairOutcome names what one backfill row cost the pass. Only
// [repairApplied] is progress; the rest leave the row diverged for a later tick
// (or, for [repairDisagreed], for good).
type repairOutcome int

const (
	// repairApplied: the proof verified and the wallet state was repaired.
	repairApplied repairOutcome = iota
	// repairDeferred: nothing could be verified yet (a lagging header view, a
	// missing BUMP). No wallet state was written; the next tick retries.
	repairDeferred
	// repairDisagreed: arcade no longer says the transaction is mined — it has
	// forgotten the record, or revised the verdict. Nothing to repair.
	repairDisagreed
)

// repairOneMinedFenced re-drives [Provider.applyMinedRepair] for one diverged
// row, sourcing the proof material it needs.
//
// The STORED proof is preferred and is the common case: a pre-repair row already
// has a header-verified BUMP in known_txs, so re-verifying it locally costs one
// header read and no arcade round trip — which matters, because the alternative
// on a large backlog is a GetTx per row. The oracle is asked only when there is
// nothing stored, which is the deferred-repair case: the row is fenced precisely
// because no BUMP has ever verified for it.
func (p *Provider) repairOneMinedFenced(ctx context.Context, kt *metastore.KnownTx) (repairOutcome, error) {
	rec, ok := storedProofRecord(kt)
	if !ok {
		if p.oracle == nil {
			return repairDeferred, fmt.Errorf(
				"storage: repair: %s has no stored proof and there is no oracle to refetch it", kt.TxID)
		}
		fetched, gerr := p.oracle.GetTx(ctx, kt.TxID)
		switch {
		case errors.Is(gerr, arcade.ErrTxNotFound), fetched == nil && gerr == nil:
			// Arcade no longer has the record it once gave a verdict for (it drops
			// them on its own schedule). Nothing to verify, so nothing may be
			// repaired. The row keeps its recorded verdict — that is the only
			// remaining trace that this transaction was ever reported mined, and
			// erasing it would take the divergence out of the work list without
			// resolving it. The attempt stamp is what keeps it from blocking the
			// page.
			p.logger.WarnContext(ctx, "repair: arcade no longer knows a transaction it reported mined",
				slog.String("txid", kt.TxID))
			return repairDisagreed, nil
		case gerr != nil:
			return repairDeferred, fmt.Errorf("storage: repair: refetch %s: %w", kt.TxID, gerr)
		}
		if fetched.TxID == "" {
			fetched.TxID = kt.TxID
		}
		if !isMinedClass(fetched.Status) {
			// Arcade has revised its verdict since the status was recorded. The
			// repair is only ever driven by a mined verdict, so nothing is repaired
			// — but the REVISED verdict is recorded, which both keeps the row honest
			// and (because it is a real status write) drops it out of the backfill's
			// predicate for good.
			p.logger.WarnContext(ctx, "repair: arcade has revised its mined verdict",
				slog.String("txid", kt.TxID), slog.String("arcadeStatus", string(fetched.Status)))
			if serr := p.recordArcadeStatus(ctx, kt.TxID, fetched.Status); serr != nil {
				return repairDisagreed, serr
			}
			return repairDisagreed, nil
		}
		rec = *fetched
	}

	// applyMinedRepair keys the repair off the CURRENT known-tx status, and a
	// pre-repair row is already 'completed' — the divergence is on the
	// transaction row, not the known tx — so the backfill calls the repair
	// DIRECTLY rather than routing through ApplyStatusUpdate, whose terminal
	// guard would (correctly, for a live event) drop it.
	applied, err := p.applyMinedRepair(ctx, rec, kt)
	switch {
	case err != nil:
		return repairDeferred, err
	case applied:
		return repairApplied, nil
	default:
		return repairDeferred, nil
	}
}

// storedProofRecord rebuilds an arcade record from the BUMP already stored on a
// known tx, so the repair can re-verify and re-apply it without asking arcade.
// ok is false when the row carries no usable stored proof.
func storedProofRecord(kt *metastore.KnownTx) (arcade.TxRecord, bool) {
	if len(kt.MerklePath) == 0 || kt.BlockHeight == nil {
		return arcade.TxRecord{}, false
	}
	rec := arcade.TxRecord{
		TxID:        kt.TxID,
		Status:      arcade.StatusMined,
		BlockHeight: uint64(*kt.BlockHeight),
		MerklePath:  kt.MerklePath,
	}
	if len(kt.BlockHash) == chainhash.HashSize {
		var h chainhash.Hash
		copy(h[:], kt.BlockHash)
		rec.BlockHash = h.String()
	}
	return rec, true
}

// pollPeers bounds concurrent GetTx calls in the poll fallback. GetTx is a
// stateless HTTP read that arcade serves concurrently, so polling serially caps
// recovery near the per-request latency (a stuck-status backlog then never
// drains). Fanning the fetches out lets one sweep re-fetch its whole batch in
// roughly a single round-trip.
const pollPeers = 16

// pollApplyAttempts bounds the retry of a failed bulk poll-apply. The batch is
// idempotent, so an immediate retry is always safe and clears the common cause
// (transient DB contention).
const pollApplyAttempts = 3

// pollAndApply fetches each tx's authoritative record via GetTx — concurrently,
// bounded by pollPeers — then applies all successful results in one bulk
// ApplyStatusBatch. This is the recovery path for statuses the live SSE stream
// missed or that arcade dropped; polling one-at-a-time was far too slow to clear
// a backlog. A not-found tx is skipped; a transport error is counted and skipped
// (the next sweep retries). Order does not matter: the records are independent
// and ApplyStatusBatch is idempotent and guard-checked per txid.
//
// Two invariants this function OWNS, both learned the hard way:
//
//  1. PROGRESS. Every selected row is stamped via MarkPolled up front, whatever
//     happens next. The finders order by that stamp, so a row that cannot be
//     applied moves to the back of the queue instead of being re-selected at the
//     head of every tick and hiding the entire backlog behind it (the frozen
//     23,745-row count). A row can never be re-selected forever without the poll
//     making progress past it.
//
//  2. NO SILENT DROPS. A failed bulk apply is retried, then FALLS BACK to a
//     per-record apply so one poisoned record cannot take a whole batch of
//     updates down with it (a single `batch=4000` failure used to be logged at
//     WARN and discarded), and whatever still fails is reported at ERROR with a
//     txid sample and returned to the caller.
func (p *Provider) pollAndApply(ctx context.Context, kts []metastore.KnownTx) error {
	if len(kts) == 0 {
		return nil
	}

	// Invariant 1: record the ATTEMPT before doing anything that can fail.
	attempted := make([]string, len(kts))
	for i := range kts {
		attempted[i] = kts[i].TxID
	}
	if err := p.meta.KnownTx().MarkPolled(ctx, attempted); err != nil {
		// Not fatal for this sweep, but it is exactly the condition that lets a
		// batch pin the head of the work list, so it must not be quiet.
		p.logger.WarnContext(ctx, "poll: failed to record poll attempt; work list may not advance",
			slog.Int("batch", len(attempted)), slog.String("error", err.Error()))
	}

	recs := make([]arcade.TxRecord, len(kts)) // one slot per input; empty TxID = skip
	var fetchFailed, notFound atomic.Int64
	var g errgroup.Group
	g.SetLimit(pollPeers)
	for i := range kts {
		if ctx.Err() != nil {
			break
		}
		i := i
		g.Go(func() error {
			txid := kts[i].TxID
			rec, err := p.oracle.GetTx(ctx, txid)
			switch {
			case errors.Is(err, arcade.ErrTxNotFound), rec == nil:
				notFound.Add(1)
				return nil
			case err != nil:
				fetchFailed.Add(1)
				p.logger.DebugContext(ctx, "poll: GetTx failed", slog.String("txid", txid), slog.String("error", err.Error()))
				return nil
			}
			if rec.TxID == "" {
				rec.TxID = txid
			}
			recs[i] = *rec // distinct index per goroutine: no shared write
			return nil
		})
	}
	_ = g.Wait() // per-tx errors are counted and skipped inside; Wait never returns non-nil

	if n := fetchFailed.Load(); n > 0 {
		// Aggregated (not per-tx) so a wholesale arcade outage is one line, but at
		// WARN: every failed fetch is a tx whose divergence persists another cycle.
		p.logger.WarnContext(ctx, "poll: GetTx failed for part of the batch, retrying next cycle",
			slog.Int64("failed", n), slog.Int64("notFound", notFound.Load()), slog.Int("batch", len(kts)))
	} else if n := notFound.Load(); n > 0 {
		// Arcade does not know these txids (yet) — a freshly-broadcast tx is not
		// queryable for a few minutes. Benign, but it is why a repair pass can come
		// back empty-handed, so keep it visible at DEBUG.
		p.logger.DebugContext(ctx, "poll: arcade does not know part of the batch",
			slog.Int64("notFound", n), slog.Int("batch", len(kts)))
	}

	batch := make([]arcade.TxRecord, 0, len(recs))
	for i := range recs {
		if recs[i].TxID != "" {
			batch = append(batch, recs[i])
		}
	}
	if len(batch) == 0 {
		return nil
	}

	// Invariant 2: bounded retry, then per-record fallback, then a loud failure.
	var applyErr error
	for range pollApplyAttempts {
		applyErr = p.ApplyStatusBatch(ctx, batch)
		if applyErr == nil || ctx.Err() != nil {
			break
		}
	}
	if applyErr == nil {
		return nil
	}
	var failed []string
	for i := range batch {
		if err := p.ApplyStatusUpdate(ctx, batch[i]); err != nil {
			failed = append(failed, batch[i].TxID)
		}
	}
	if len(failed) == 0 {
		p.logger.WarnContext(ctx, "poll: bulk apply failed but the per-tx fallback applied every record",
			slog.Int("batch", len(batch)), slog.String("error", applyErr.Error()))
		return nil
	}
	sample := failed
	if len(sample) > loggedTxIDSample {
		sample = sample[:loggedTxIDSample]
	}
	p.logger.ErrorContext(ctx, "poll: apply failed, transactions left diverged from arcade",
		slog.Int("failed", len(failed)),
		slog.Int("batch", len(batch)),
		slog.Any("sampleTxIDs", sample),
		slog.String("error", applyErr.Error()))
	return fmt.Errorf("storage: poll apply failed for %d/%d transactions (e.g. %v): %w",
		len(failed), len(batch), sample, applyErr)
}

// DemoteReorgedProofs is the reorg handler: for stored proofs at or above
// forkHeight whose merkle root no longer matches the header at that height, it
// demotes the tx back to unproven, clears the proof, and re-promotes change to
// TierUnproven (change stays spendable — reorged txs usually re-mine, and the
// proof pipeline re-verifies). It is best-effort per the headers contract; the
// proof poll is the authority. Proofs that still verify on the new chain are
// left untouched.
func (p *Provider) DemoteReorgedProofs(ctx context.Context, forkHeight uint32) error {
	if p.hdrs == nil {
		return nil
	}
	proven, err := p.meta.KnownTx().FindProvenAtOrAbove(ctx, forkHeight, defaultMonitorBatchLimit)
	if err != nil {
		return fmt.Errorf("storage: find proven at/above %d: %w", forkHeight, err)
	}
	for i := range proven {
		if err := ctx.Err(); err != nil {
			return err
		}
		kt := &proven[i]
		if kt.BlockHeight == nil || len(kt.MerkleRoot) != chainhash.HashSize {
			continue
		}
		var root chainhash.Hash
		copy(root[:], kt.MerkleRoot)
		if ok, verr := p.hdrs.VerifyMerkleRoot(ctx, &root, *kt.BlockHeight); verr == nil && ok {
			continue // proof still valid on the post-reorg chain
		}
		if err := p.demoteProof(ctx, kt.TxID); err != nil {
			p.logger.WarnContext(ctx, "reorg demote failed", slog.String("txid", kt.TxID), slog.String("error", err.Error()))
		} else {
			p.logger.InfoContext(ctx, "reorg: demoted proof to unproven", slog.String("txid", kt.TxID))
		}
	}
	return nil
}

// demoteProof clears a tx's proof and re-enters it into the proof pipeline
// (known tx → unconfirmed, transaction → unproven, change → TierUnproven).
func (p *Provider) demoteProof(ctx context.Context, txid string) error {
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.KnownTx().ClearProof(ctx, txid, wdk.ProvenTxStatusUnconfirmed); err != nil &&
			!errors.Is(err, metastore.ErrNotFound) {
			return fmt.Errorf("storage: clear proof: %w", err)
		}
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusUnproven,
			wdk.TxStatusCompleted); err != nil && !errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: demote tx: %w", err)
		}
		return p.promoteChangeByTxID(ctx, txid, utxostore.TierUnproven)
	})
}

// GetKeyValue exposes the metastore key-value store (the SSE cursor lives here).
func (p *Provider) GetKeyValue(ctx context.Context, key string) ([]byte, bool, error) {
	return p.meta.KeyValue().Get(ctx, key)
}

// SetKeyValue upserts a value in the metastore key-value store.
func (p *Provider) SetKeyValue(ctx context.Context, key string, value []byte) error {
	return p.meta.KeyValue().Set(ctx, key, value)
}

// AcquireLease tries to take or renew the monitor job lease for job under owner
// for ttl. It returns true when this instance holds the lease afterward, false
// when another live instance holds it. See metastore.LeaseRepo.
func (p *Provider) AcquireLease(ctx context.Context, job, owner string, ttl time.Duration) (bool, error) {
	now := p.now().UnixNano()
	return p.meta.Leases().Acquire(ctx, job, owner, now+ttl.Nanoseconds(), now)
}

// ReleaseLease releases the monitor job lease for job if owner still holds it.
func (p *Provider) ReleaseLease(ctx context.Context, job, owner string) error {
	return p.meta.Leases().Release(ctx, job, owner, p.now().UnixNano())
}
