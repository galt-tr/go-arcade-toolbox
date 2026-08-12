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

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
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

	// Terminal wallet-state guard: a proven or terminally-failed tx is never
	// downgraded by a later event (also makes re-applying MINED a no-op).
	if kt.Status == wdk.ProvenTxStatusCompleted || kt.Status.IsTerminalFailure() {
		return nil
	}

	// Arcade lattice guard: a frame may only advance the status, never regress
	// it (RECEIVED after SEEN, SEEN after MINED, …). Idempotent same-status
	// re-applies are allowed (CanSupersede returns true for prev == s).
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
		return p.applySeen(ctx, rec)
	case arcade.StatusMined, arcade.StatusImmutable:
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
func (p *Provider) applySeen(ctx context.Context, rec arcade.TxRecord) error {
	txid := rec.TxID
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.Transactions().UpdateStatusByTxID(ctx, txid, wdk.TxStatusUnproven,
			wdk.TxStatusSending, wdk.TxStatusNoSend, wdk.TxStatusUnproven, wdk.TxStatusUnprocessed); err != nil &&
			!errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: seen: mark unproven: %w", err)
		}
		if err := p.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnconfirmed,
			wdk.ProvenTxReqBeyondBroadcastStageStatuses...); err != nil &&
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
// changeOutpoints.
func (p *Provider) changeOutpointsByTxID(ctx context.Context, txid string) ([]utxostore.Outpoint, error) {
	change := true
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{TxID: &txid, Change: &change})
	if err != nil {
		return nil, fmt.Errorf("storage: find change outputs %s: %w", txid, err)
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
		// Every self-owned (change-purpose) output regardless of basket — the
		// default change basket AND the throughput pool/reserve baskets. Since
		// promotion to claimable now happens ONLY here (on SEEN) and no longer on
		// the 202, a default-only filter would strand fuel/pool coins at
		// TierSending forever. Matches [Provider.changeOutpoints].
		if rows[i].Basket == nil {
			continue
		}
		ops = append(ops, utxostore.Outpoint{TxID: *hash, Vout: rows[i].Vout})
	}
	return ops, nil
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
		// Terminal wallet-state guard: a proven or terminally-failed tx is never
		// downgraded by a later event (identical to ApplyStatusUpdate).
		if kt.Status == wdk.ProvenTxStatusCompleted || kt.Status.IsTerminalFailure() {
			continue
		}
		evs := perTxid[txid]
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
// change to TierUnproven — the batched form of applySeen. Must run inside
// p.meta.Do.
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
		wdk.ProvenTxReqBeyondBroadcastStageStatuses...); err != nil {
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
// bulk query — the batched analog of changeOutpointsByTxID.
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
// because the circuit breaker was open). It deliberately SKIPS reservations
// whose tx is still re-drivable (a stored raw tx that SendWaiting will retry),
// so it never frees inputs out from under an in-flight re-broadcast. Releasing
// is idempotent, so a racing completion is harmless.
func (p *Provider) SweepStaleReservations(ctx context.Context, olderThan time.Time, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	refs, err := p.utxo.FindStaleReservations(ctx, olderThan, limit)
	if err != nil {
		return fmt.Errorf("storage: find stale reservations: %w", err)
	}
	for i := range refs {
		ref := &refs[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		resendable, err := p.reservationResendable(ctx, int(ref.UserID), ref.Reservation)
		if err != nil {
			p.logger.WarnContext(ctx, "sweep stale reservations: resendable check failed",
				slog.String("reservation", ref.Reservation), slog.String("error", err.Error()))
			continue
		}
		if resendable {
			continue // SendWaiting owns it; do not free its inputs
		}
		if _, err := p.utxo.ReleaseReservation(ctx, ref.UserID, ref.Reservation); err != nil {
			p.logger.WarnContext(ctx, "sweep stale reservations: release failed",
				slog.String("reservation", ref.Reservation), slog.String("error", err.Error()))
			continue
		}
	}
	return nil
}

// reservationResendable reports whether the transaction holding this reservation
// still has a stored raw tx and was never broadcast — i.e. SendWaiting will
// re-drive it, so the reservation must NOT be swept.
func (p *Provider) reservationResendable(ctx context.Context, userID int, reference string) (bool, error) {
	txRow, found, err := p.meta.Transactions().FindByReference(ctx, userID, reference)
	if err != nil {
		return false, err
	}
	if !found || txRow.TxID == nil {
		return false, nil
	}
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, *txRow.TxID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return len(kt.RawTx) > 0 && !kt.WasBroadcast, nil
}

// sendOneWaiting EF-encodes and broadcasts one delayed tx, then commits the
// outcome.
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
	res, berr := p.broadcastWithBackpressure(ctx, txid, ef)
	if berr != nil {
		// Backpressure exhausted or opaque/transport failure: leave unsent.
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
	rows, err := p.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	if err != nil {
		return fmt.Errorf("find tx rows %s: %w", txid, err)
	}
	var txRow *wdk.TableTransaction
	var userID int
	if len(rows) > 0 {
		txRow = &rows[0]
		userID = rows[0].UserID
	}
	return p.applyAcceptedBroadcast(ctx, userID, txid, tx, txRow)
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
func (p *Provider) CheckProofs(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	unproven, err := p.meta.KnownTx().FindByStatusOlderThan(ctx, p.now(), limit, unprovenStatuses...)
	if err != nil {
		return fmt.Errorf("storage: find unproven: %w", err)
	}
	return p.pollAndApply(ctx, unproven)
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
