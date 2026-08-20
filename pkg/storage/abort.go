package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// abortableStatuses are the pre-broadcast transaction statuses a caller may
// abort. Anything with broadcast/network evidence (sending, unproven,
// completed, failed, aborted) is refused.
//
// This is the WALLET-side vocabulary. Its known-tx counterpart is
// [metastore.KnownTxRepo.TransitionToAborted]'s from-set (unsent / unprocessed
// / nosend), and the two are deliberately not the same list: they name the same
// idea — "certainly not handed to the network yet" — in the two different state
// machines this package keeps. Only 'unprocessed' is spelled identically in
// both. An abort must clear BOTH gates (see [Provider.fenceAborted]).
var abortableStatuses = map[wdk.TxStatus]bool{
	wdk.TxStatusUnsigned:    true,
	wdk.TxStatusNoSend:      true,
	wdk.TxStatusUnprocessed: true,
	wdk.TxStatusNonFinal:    true,
}

// ErrAbortLostToSend narrows [wdk.ErrNotAbortableAction] to the one abort
// refusal a caller can respond to differently: the raw transaction was already
// CLAIMED for broadcast (or the network already has it), so the abort lost the
// race and the transaction is on its way out.
//
// It matters because the generic refusal invites the wrong reflex. "Not
// abortable" usually means the action never got far enough or has already
// settled, and rebuilding is reasonable; here it means the opposite — bytes
// spending these very inputs are in flight, and rebuilding would author a
// double spend of coins that are about to be gone. The correct response is to
// stop and watch the transaction's status: it will land, or it will fail and
// the reconciler will free its inputs.
//
// Do NOT read the ABSENCE of this sentinel as "the abort can be retried". The
// other refusal — the wallet-side status CAS losing to a concurrent transition
// — carries [wdk.ErrNotAbortableAction] alone, and is deliberately left
// unnamed: re-reading the action's status is the only sensible response to it,
// which is what the generic sentinel already says.
//
// It WRAPS [wdk.ErrNotAbortableAction] rather than sitting beside it, so a
// caller matching only the BRC-100 sentinel keeps working unchanged — including
// across the REST transport, where the client reconstructs the error from one
// wire code and matches whatever that code's sentinel wraps.
var ErrAbortLostToSend = fmt.Errorf("storage: abort lost to an in-flight broadcast: %w", wdk.ErrNotAbortableAction)

// AbortAction aborts a pre-broadcast transaction: it fences the raw transaction
// so it can never be broadcast, releases the funding reservation held under the
// transaction Reference, removes any minted change, and marks the transaction
// aborted. Post-broadcast transactions — and pre-broadcast ones a broadcaster
// has already claimed — are refused with [wdk.ErrNotAbortableAction].
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

// abortTxRow is the transactional core of aborting a pre-broadcast transaction,
// shared by [Provider.AbortAction] (user-initiated), the monitor's
// AbortAbandoned sweep and [Provider.SweepStaleReservations] (which aborts the
// transaction before it reclaims a stale reservation, so a swept action dies
// exactly the way an aborted one does). It returns [wdk.ErrNotAbortableAction]
// when either arm
// of the fence finds the transaction already past a pre-broadcast state (a
// concurrent transition, or a broadcaster, won).
//
// An abort is two halves that cannot be one write outside Mode A: a METADATA
// half (fence the raw tx, mark the transaction aborted, restore the inputs'
// spend history) and a UTXO half (unpin, release the reservation, remove the
// minted change). The dispatch below is entirely about how those two halves are
// ordered and committed — see [Provider.abortDirect] and
// [Provider.abortViaOutbox].
//
// CALLER INVARIANT: call this OUTSIDE any ambient [metastore.Store.Do]. All
// three callers do — AbortAction, AbortAbandoned and the fence-first
// [Provider.SweepStaleReservations] enter with no transaction on the context,
// and the sweep holds no unit of work across its refs. The reason is not
// hygiene: metastore.Do JOINS an ambient transaction instead of opening its own
// (see uow.go), so an outer Do would collapse abortViaOutbox's two phases into
// one — the enqueue would no longer be durable before the inline execution ran,
// and a rollback of the outer transaction would discard the fence and the intent
// while the utxostore writes had already committed. That is precisely P0-5,
// re-opened from the outside.
func (p *Provider) abortTxRow(ctx context.Context, userID int, txRow *wdk.TableTransaction) error {
	if p.modeA {
		return p.abortDirect(ctx, userID, txRow)
	}
	return p.abortViaOutbox(ctx, userID, txRow)
}

// fenceAborted is the METADATA half of an abort, and the whole of its safety.
// It runs inside the caller's [metastore.Store.Do] and does three things in one
// commit:
//
//  1. CAS the wallet transaction to 'aborted' from an abortable status. Losing
//     it means a concurrent transition already moved the row.
//  2. Fence the raw transaction at [metastore.KnownTxStatusAborted]. This is
//     the P0-3 fix: abort used never to touch known_txs, and known_txs is the
//     only table the send sweep reads, so an aborted transaction stayed fully
//     broadcastable while its inputs were being handed back to the funder.
//  3. Restore the spend-history flag on the inputs the transaction had marked
//     spent.
//
// Steps 1 and 2 are atomic with each other because transactions and known_txs
// always live in the SAME database (the metastore), in every deployment mode.
// That is what makes "aborted but still broadcastable" unrepresentable rather
// than merely unlikely, and it is why the fence — not the release — is the part
// that must commit first.
//
// THE FENCE IS THE STATUS, AND raw_tx IS DELIBERATELY LEFT INTACT. This looks
// like an omission and is not; do not "harden" it by adding `raw_tx = NULL`
// here. Two reasons, in order of importance:
//
//   - It would be a SECOND, WEAKER fence that masks regressions in the real
//     one. The status is what fences: 'aborted' appears in none of
//     [metastore.KnownTxRepo.FindResendable]'s three status arms at any age, and
//     [metastore.KnownTxRepo.ClaimForSend]'s CAS refuses it, so no work list
//     yields the row and no broadcaster can take it. A null raw_tx would ALSO
//     drop the row from FindResendable (its predicate includes `raw_tx IS NOT
//     NULL`) — which means a future bug that let an aborted row back into a
//     status arm would be silently absorbed instead of failing the tests that
//     watch the status fence. Belt and braces are good; braces that hide a
//     failing belt are not.
//   - The bytes have LIVE post-abort consumers. Abort is a wallet-side
//     decision, and the client already holds the signed transaction
//     ProcessAction returned to it; if it broadcasts them out of band, the
//     network can still confirm a transaction we call aborted. Reading its
//     inputs back is then the only way to reconcile, and three readers already
//     do exactly that against retained raw bytes: [Provider.txInputs],
//     [Provider.localWinnerInputs] (which resolves an ESCAPED competitor's
//     consumption set straight out of known_txs, aborted rows included), and
//     the upcoming mined-repair apply. Nulling the column would blind all
//     three to save nothing the status fence does not already guarantee.
//
// The known-tx arm follows [metastore.KnownTxRepo.TransitionToAborted]'s caller
// guidance exactly, and the two error cases mean opposite things:
//
//   - ErrStatusUpdateSkipped: a broadcaster has CLAIMED these bytes, or the
//     network already has them. The abort LOSES and the whole unit of work
//     rolls back, step 1 included. It deliberately does NOT re-read the row to
//     see whether it says 'aborted' already: that read is not atomic with the
//     CAS, and between the two a claim can be taken and the bytes POSTed — at
//     which point "already aborted, treat as success" would release the inputs
//     of a live transaction, which is the exact double spend the fence exists
//     to prevent. Losing to a send is a legitimate outcome; it means the send
//     happened and the abort did not.
//   - ErrNotFound: the ordinary unsigned case. A transaction aborted before it
//     was ever signed has no known_txs row, so there are no bytes to fence and
//     nothing to lose to. Proceed.
//
// Both refusals surface as [wdk.ErrNotAbortableAction], which is the only typed
// abort failure the BRC-100 surface has. ONE of the two is separated out on top
// of it: the known-tx arm additionally carries [ErrAbortLostToSend], because
// only that case tells the caller something it can act on differently —
// the transaction is going out, so do not rebuild it. The status-CAS arm gets
// no sentinel of its own; "the action is not abortable in its current state" is
// already exactly what [wdk.ErrNotAbortableAction] says, and a second name for
// it would export a distinction with no distinct response.
func (p *Provider) fenceAborted(ctx context.Context, txRow *wdk.TableTransaction, txid string) error {
	if err := p.meta.Transactions().UpdateStatus(ctx, txRow.TransactionID, wdk.TxStatusAborted,
		wdk.TxStatusUnsigned, wdk.TxStatusNoSend, wdk.TxStatusUnprocessed, wdk.TxStatusNonFinal); err != nil {
		if errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return wdk.ErrNotAbortableAction
		}
		return fmt.Errorf("storage: mark aborted: %w", err)
	}
	if txid == "" {
		return nil
	}
	switch err := p.meta.KnownTx().TransitionToAborted(ctx, txid); {
	case err == nil, errors.Is(err, metastore.ErrNotFound):
		// Fenced, or never signed and so nothing to fence.
	case errors.Is(err, metastore.ErrStatusUpdateSkipped):
		return fmt.Errorf("storage: transaction %s is claimed for broadcast or already sent: %w",
			txid, ErrAbortLostToSend)
	default:
		return fmt.Errorf("storage: fence raw tx %s: %w", txid, err)
	}
	// Restore the spend-history flag on any inputs marked spent.
	if err := p.meta.Outputs().ClearSpentBy(ctx, txRow.TransactionID); err != nil {
		return fmt.Errorf("storage: clear spent_by: %w", err)
	}
	return nil
}

// abortDirect (Mode A) commits the fence and every utxo op in ONE transaction —
// the metastore and the utxostore share a database, so there is no split to
// reconcile and no outbox to keep. Either the whole abort happened or none of
// it did.
func (p *Provider) abortDirect(ctx context.Context, userID int, txRow *wdk.TableTransaction) error {
	txid := abortTxID(txRow)
	reference := string(txRow.Reference)
	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.fenceAborted(ctx, txRow, txid); err != nil {
			return err
		}
		if err := p.doAbortRelease(ctx, int64(userID), reference); err != nil {
			return err
		}
		if txid == "" {
			return nil
		}
		ops, err := p.changeOutpoints(ctx, userID, txRow.TransactionID, txid)
		if err != nil {
			return err
		}
		return p.doAbortRemoveChange(ctx, txid, ops)
	})
}

// abortViaOutbox (Mode B) is the split-store abort, and the fix for audit P0-5.
//
// The old code did the whole abort inside one metastore.Do: the CAS, then the
// utxostore release. In Mode B those are different databases, so the release
// committed IMMEDIATELY and irrevocably while the metadata half was still
// provisional — and metastore.Do wraps its closure in sqlkit.WithRetry, so that
// half could both roll back AND RE-RUN. A retryable failure after the release
// therefore un-aborted a transaction whose coins were already free: bytes on a
// broadcast work list, inputs back in the claimable pool. That is a wallet
// double-spending its own coins, and no ordering of the two calls inside one Do
// can fix it, because one of the two stores is not in the transaction.
//
// So the Do commits NO utxo write at all. It commits the fence plus a durable
// INTENT (outbox rows), which is the [Provider.releaseViaOutbox] pattern:
//
//	phase 1 — fence + Enqueue, atomic on the metadata side. The instant this
//	          commits the raw tx is unbroadcastable, and the utxo work to do is
//	          recorded. A re-run of the closure re-enqueues the same keys, which
//	          [metastore.OutboxRepo.Enqueue] deduplicates (ON CONFLICT DO
//	          NOTHING), so WithRetry costs nothing.
//	phase 2 — inline execution, best effort. Failures are recorded on the row
//	          and left for DrainOutbox (the monitor's reject_release task).
//
// The dedupe is mostly belt and braces: a WithRetry re-run rolls the failed
// transaction back and opens a new one, so the closure starts from a clean
// slate and would insert cleanly anyway. Where ON CONFLICT actually earns its
// keep is the AMBIGUOUS COMMIT — the server committed but the client saw a
// connection error, so the retry re-runs against a database that already has
// both the fence and the row. There the re-run's transactions CAS loses (the
// row is already 'aborted') and the caller gets a spurious
// ErrNotAbortableAction, which is the safe direction: the fence is committed,
// the coins are still held, and the pending outbox row goes on to release them.
//
// The residue of a crash or a failure anywhere in phase 2 is always the SAFE
// one: a fenced transaction whose coins are still held. Never the reverse.
//
// Replay safety, since every op can run more than once:
//
//   - Unpin and ReleaseReservation are token-guarded by (userID, reservation).
//     If the coin has since been re-claimed by a NEW action it is held under a
//     different token, so a late replay matches no row and does nothing.
//   - A crash between the Unpin and the ReleaseReservation inside one op leaves
//     the rows reserved-but-unpinned. The pending outbox row is what recovers
//     that: the next drain re-runs both (the Unpin idempotently) and completes
//     the release. [Provider.SweepStaleReservations] is a second backstop rather
//     than the first one — it reclaims on the stale-reservation TTL, minutes
//     later, where the drain runs on the next tick — but it does now cover this
//     residue, through its aborted-status arm.
//   - RemoveByMintTx skips change that is already gone.
//
// A PARKED row outlives the recovery above. The pending outbox row stops being
// pending once it crosses [metastore.MaxOutboxAttempts]: ten failures at the
// drain's 60s cadence, i.e. roughly ten minutes of continuous utxostore
// unavailability, and the release intent drops out of FetchPending for good.
// The transaction is safely fenced — this never becomes a double spend — but
// its coins are left pinned AND reserved, and re-aborting cannot help because
// the transactions CAS refuses an already-aborted row, so the abort never
// reaches phase 1 to re-enqueue.
//
// [Provider.SweepStaleReservations] is what finally clears it. Its aborted-
// status arm reads a pinned-INCLUSIVE stale listing and, for a transaction
// already at 'aborted', runs [Provider.doAbortRelease] directly. Lifting a pin
// without its owning transaction's consent is legitimate in exactly that one
// condition: the fence has already proved those bytes can never be broadcast.
// Recovery is therefore on the stale-reservation TTL rather than the drain's
// tick, and until it runs the parked row remains operator-visible through the
// parked_total gauge on the reconciler's drain log line.
func (p *Provider) abortViaOutbox(ctx context.Context, userID int, txRow *wdk.TableTransaction) error {
	txid := abortTxID(txRow)
	reference := string(txRow.Reference)

	// Rebuilt from scratch on every attempt, because the closure below can be
	// re-run by WithRetry.
	var ops []abortOp
	err := p.meta.Do(ctx, func(ctx context.Context) error {
		ops = nil
		if err := p.fenceAborted(ctx, txRow, txid); err != nil {
			return err
		}
		ops = append(ops, abortOp{
			key:    abortOutboxKey(reference),
			opType: opAbortRelease,
			payload: mustJSON(abortReleasePayload{
				UserID:      int64(userID),
				Reservation: reference,
			}),
		})
		if txid != "" {
			change, cerr := p.changeOutpoints(ctx, userID, txRow.TransactionID, txid)
			if cerr != nil {
				return cerr
			}
			if len(change) > 0 {
				rawTxid, herr := encodeTxidToRaw(txid)
				if herr != nil {
					return herr
				}
				ops = append(ops, abortOp{
					key:    rawTxid,
					opType: opAbortRemoveChange,
					payload: mustJSON(abortRemoveChangePayload{
						MintTxID: txid,
						Ops:      encodeOutpoints(change),
					}),
				})
			}
		}
		for _, op := range ops {
			if eerr := p.meta.Outbox().Enqueue(ctx, op.key, op.opType, 0, op.payload); eerr != nil {
				return eerr
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Phase 2: inline execute (best-effort; the drain worker heals any that fail).
	_ = p.meta.Do(ctx, func(ctx context.Context) error {
		for _, op := range ops {
			entry := metastore.OutboxEntry{TxID: op.key, OpType: op.opType, Payload: op.payload}
			if _, xerr := p.executeOutboxOp(ctx, entry); xerr != nil {
				_ = p.meta.Outbox().RecordError(ctx, op.key, op.opType, 0, xerr.Error())
				// The two ops fail with different consequences, so they log at
				// different levels. A failed ABORT_RELEASE has STRANDED USER
				// FUNDS — the coins stay pinned and reserved until a drain
				// succeeds, and if the row parks (see the doc above) nothing
				// frees them at all — which is an operator action, not a note.
				// A failed ABORT_REMOVE_CHANGE leaves phantom change that is
				// unspendable anyway and costs only inventory tidiness.
				log := p.logger.WarnContext
				if op.opType == opAbortRelease {
					log = p.logger.ErrorContext
				}
				log(ctx, "abort: inline utxo op failed, left for drain worker",
					slog.String("reference", reference), slog.String("op", op.opType),
					slog.String("error", xerr.Error()))
				continue
			}
			if derr := p.meta.Outbox().MarkDone(ctx, op.key, op.opType, 0); derr != nil {
				return derr
			}
		}
		return nil
	})
	return nil
}

// --- abort utxo op primitives (shared by direct + outbox execution) ---------
//
// These are the arms [Provider.executeOutboxOp] dispatches opAbortRelease and
// opAbortRemoveChange to, and the same calls abortDirect makes inline. Both
// modes therefore apply the identical utxo semantics; only the commit boundary
// differs.

// doAbortRelease gives an aborted transaction's funding inputs back: it lifts
// the pre-broadcast pin, then releases the reservation.
//
// The Unpin is not optional and its ORDER is not incidental. processNewTx pins
// the reservation the moment it stores broadcastable bytes, and
// ReleaseReservation is structurally unable to free a pinned row (see
// [utxostore.Store.Unpin]) — so without the lift the release is silently
// refused and the funding coin is stranded, reserved forever. Abort is one of
// the few parties entitled to declare a transaction dead, and by the time this
// runs the fence has already proved these bytes can never be broadcast.
//
// It must also stay OUTSIDE any `txid != ""` guard the caller applies. A pin
// does not imply a txid: in Mode B processNewTx's pin commits before the
// metadata, so a rolled-back meta half leaves a pinned reservation on a row
// that never got a txid — and that row, still unsigned, is precisely what
// AbortAbandoned brings here.
//
// Unpin only lifts the pin; the rows stay reserved, and freeing them is the
// separate call below. A crash between the two degrades to a stale reservation
// (recovered by the pending outbox row), never to a free coin backing bytes
// someone could still broadcast.
func (p *Provider) doAbortRelease(ctx context.Context, userID int64, reservation string) error {
	if _, err := p.utxo.Unpin(ctx, userID, reservation); err != nil {
		return fmt.Errorf("storage: unpin reservation: %w", err)
	}
	if _, err := p.utxo.ReleaseReservation(ctx, userID, reservation); err != nil {
		return fmt.Errorf("storage: release reservation: %w", err)
	}
	return nil
}

// doAbortRemoveChange removes the change an aborted transaction minted. The
// coins were minted against a txid that will never reach the network, so they
// must leave inventory rather than return to the pool.
//
// The RemoveByMintTx report is deliberately discarded, unlike in the
// reject->release path, which uses it to cascade children. An aborted
// transaction was never broadcast, so anything that spent its change was built
// locally from a coin that never existed; those descendants surface through
// their own abort/rejection lifecycles rather than through a cascade rooted
// here.
func (p *Provider) doAbortRemoveChange(ctx context.Context, mintTxID string, ops []utxostore.Outpoint) error {
	if len(ops) == 0 {
		return nil
	}
	hash, err := chainhash.NewHashFromHex(mintTxID)
	if err != nil {
		return fmt.Errorf("storage: parse txid: %w", err)
	}
	if _, err := p.utxo.RemoveByMintTx(ctx, *hash, ops); err != nil {
		return fmt.Errorf("storage: remove minted change: %w", err)
	}
	return nil
}

// abortOp is one enqueued abort op, pre-serialized with its own outbox key.
// Unlike the reconciler's [outboxOp], the key cannot be hoisted out of the
// loop: an abort spreads its ops over TWO keys (see [abortOutboxKey]).
type abortOp struct {
	key     []byte
	opType  string
	payload []byte
}

// abortOutboxKey is the outbox key for the ops of an abort that may have no
// txid at all — the unsigned case, which is the common one. The outbox is keyed
// (txid, op_type, chunk) over a 32-byte column, so an abort needs a
// txid-shaped surrogate: sha256("abort:" + reference).
//
// Key uniqueness is a CONSTRAINT, not a probability argument. `reference` is
// `TEXT NOT NULL UNIQUE` on the transactions table (00001_init.sql) — unique
// GLOBALLY, not per user — so distinct actions cannot share a reference and
// therefore cannot share a key, whatever the generator does. (For the record it
// is 12 crypto/rand bytes, 96 bits, via [defaultRandomizer.Base64]; that is what
// makes a collision unlikely, but the UNIQUE index is what makes it
// impossible.)
//
// Against the release path's keys the separation is structural too: op_type is
// part of the primary key, so an abort op could only collide with a real
// transaction's op if a sha256 output equalled a txid AND both carried the same
// op_type, and no op_type here is shared with the release path's four.
//
// The change-removal op is keyed by the real txid instead — it only exists when
// there IS one, and keying it that way keeps an aborted transaction's minted
// change under the same key the rest of the system knows it by.
func abortOutboxKey(reference string) []byte {
	sum := sha256.Sum256([]byte("abort:" + reference))
	return sum[:]
}

// abortTxID returns the transaction's txid, or "" when it was never signed.
func abortTxID(txRow *wdk.TableTransaction) string {
	if txRow.TxID == nil {
		return ""
	}
	return *txRow.TxID
}
