package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// + suspect_since + arcade status for the M4.2 reject reconciler. It does NOT
// release inputs or touch change (see the ApplyStatusUpdate doc's asymmetry
// note).
func (p *Provider) applyRejected(ctx context.Context, rec arcade.TxRecord) error {
	if err := p.meta.KnownTx().MarkSuspect(ctx, rec.TxID, p.now(), rec.CompetingTxs, string(rec.Status)); err != nil &&
		!errors.Is(err, metastore.ErrNotFound) {
		return fmt.Errorf("storage: mark suspect %s: %w", rec.TxID, err)
	}
	return nil
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
		if rows[i].Basket == nil || *rows[i].Basket != p.changeBasketName() {
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
			if serr := p.sendOneWaiting(gctx, kt); serr != nil {
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
	kt, found, err := p.meta.KnownTx().FindByTxID(ctx, string(*txRow.TxID))
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
func (p *Provider) sendOneWaiting(ctx context.Context, kt *metastore.KnownTx) error {
	txid := kt.TxID
	if len(kt.RawTx) == 0 {
		return fmt.Errorf("no stored raw tx for %s", txid)
	}
	tx, err := transaction.NewTransactionFromBytes(kt.RawTx)
	if err != nil {
		return fmt.Errorf("parse stored raw tx %s: %w", txid, err)
	}
	if err := p.hydrateInputs(tx, kt.InputBEEF); err != nil {
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
		// matching the async-reject asymmetry.
		if err := p.meta.KnownTx().MarkSuspect(ctx, txid, p.now(), nil, string(res.Status)); err != nil &&
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
func (p *Provider) SynchronizeTransactionStatuses(ctx context.Context, limit int) error {
	if limit <= 0 {
		limit = defaultMonitorBatchLimit
	}
	cutoff := p.now().Add(-syncStaleness)
	stale, err := p.meta.KnownTx().FindByStatusOlderThan(ctx, cutoff, limit, pollableStatuses...)
	if err != nil {
		return fmt.Errorf("storage: find stale non-terminal: %w", err)
	}
	return p.pollAndApply(ctx, stale)
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

// pollAndApply fetches each tx's authoritative record via GetTx and applies it.
// A not-found tx is skipped; a transport error is logged and skipped (the next
// sweep retries).
func (p *Provider) pollAndApply(ctx context.Context, kts []metastore.KnownTx) error {
	for i := range kts {
		if err := ctx.Err(); err != nil {
			return err
		}
		txid := kts[i].TxID
		rec, err := p.oracle.GetTx(ctx, txid)
		switch {
		case errors.Is(err, arcade.ErrTxNotFound):
			continue
		case err != nil:
			p.logger.DebugContext(ctx, "poll: GetTx failed", slog.String("txid", txid), slog.String("error", err.Error()))
			continue
		case rec == nil:
			continue
		}
		if rec.TxID == "" {
			rec.TxID = txid
		}
		if err := p.ApplyStatusUpdate(ctx, *rec); err != nil {
			p.logger.WarnContext(ctx, "poll: apply failed", slog.String("txid", txid), slog.String("error", err.Error()))
		}
	}
	return nil
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
