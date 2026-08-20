package aerostore

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Mint implements [utxostore.Store]. Each coin is a CREATE_ONLY put; a
// same-identity replay (KEY_EXISTS) is a no-op success and an identity conflict
// fails that item with [utxostore.AlreadyExistsError].
func (s *Store) Mint(_ context.Context, mints []*utxostore.Mint) error {
	if s.closed.Load() {
		return errClosed
	}
	failed := 0
	for _, m := range mints {
		m.Err = s.mintOne(m)
		if m.Err != nil {
			failed++
		}
	}
	return utxostore.BatchCountErr(failed, len(mints))
}

func (s *Store) mintOne(m *utxostore.Mint) error {
	if err := utxostore.ValidateMint(m); err != nil {
		return err
	}

	key, err := s.keyFor(m.Outpoint)
	if err != nil {
		return err
	}

	now := s.nowMillis()
	bins := []*as.Bin{
		as.NewBin(binTxID, m.TxID[:]),
		as.NewBin(binVout, int(m.Vout)),
		as.NewBin(binUserID, m.UserID),
		as.NewBin(binBasket, m.Basket),
		as.NewBin(binTier, int(m.Tier)),
		as.NewBin(binSats, int64(m.Satoshis)), //nolint:gosec // satoshi amounts are < 2^63
		as.NewBin(binInpSize, int(m.InputSize)),
		as.NewBin(binCreated, now),
		as.NewBin(binClaimKey, claimKeyFor(m.UserID, m.Basket, m.Tier, bucketOf(m.Satoshis))),
		as.NewBin(binInvKey, invKeyFor(m.UserID, m.Basket)),
	}

	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.CREATE_ONLY
	aerr := s.client.PutBins(wp, key, bins...)
	if aerr == nil {
		s.noteClaimable(m.UserID, m.Basket, m.Tier, m.Satoshis)
		return nil
	}
	if !aerr.Matches(types.KEY_EXISTS_ERROR) {
		return fmt.Errorf("aerostore: mint %s: %w", m.Outpoint, aerr)
	}

	// Row exists: idempotent iff the immutable identity matches.
	rec, gerr := s.client.Get(nil, key)
	if gerr != nil {
		return fmt.Errorf("aerostore: mint %s: read existing: %w", m.Outpoint, gerr)
	}
	u, cerr := recordToUTXO(rec)
	if cerr != nil {
		return cerr
	}
	if u.UserID == m.UserID && u.Basket == m.Basket &&
		u.Satoshis == m.Satoshis && u.InputSize == m.InputSize {
		return nil // same identity: idempotent success
	}
	return &utxostore.AlreadyExistsError{Op: m.Outpoint}
}

// Get implements [utxostore.Store].
func (s *Store) Get(_ context.Context, op utxostore.Outpoint) (*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	rec, found, err := s.getRecord(op)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, &utxostore.NotFoundError{Op: op}
	}
	return recordToUTXO(rec)
}

// Remove implements [utxostore.Store]. Missing rows are no-ops; without force,
// reserved/spent/frozen rows are refused per item with typed errors joined
// under [utxostore.ErrBatch] — or [utxostore.ErrContention] when a row keeps
// flipping between removable and held, which is transient: retry the call.
func (s *Store) Remove(_ context.Context, ops []utxostore.Outpoint, force bool) error {
	if s.closed.Load() {
		return errClosed
	}
	var itemErrs []error
	for _, op := range ops {
		if err := s.removeOne(op, force); err != nil {
			itemErrs = append(itemErrs, err)
		}
	}
	return utxostore.JoinBatch(itemErrs)
}

// removableFilter re-asserts, server-side at delete time, the classification
// removeOne made from its snapshot: the row must still be unreserved, unspent
// and unfrozen — one conjunct per state Remove refuses. Without it a coin
// reserved or spent inside the snapshot→delete window is destroyed anyway,
// silently removing a live input of a broadcast transaction (audit P1-1).
//
// A pinned row is by construction a reserved row — the pin is only ever set on
// a row already reserved by the pinning token, and every release path clears
// both together — so binResBy covers pins and no pinned conjunct is needed.
//
// It guards one conjunct more than [phantomFilter]: an un-forced Remove refuses
// a frozen row, while RemoveByMintTx removes frozen phantom coins.
func removableFilter() *as.Expression {
	return as.ExpAnd(
		as.ExpNot(as.ExpBinExists(binResBy)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
		as.ExpNot(as.ExpBinExists(binFrozen)),
	)
}

func (s *Store) removeOne(op utxostore.Outpoint, force bool) error {
	if force {
		// Force: the caller asserts authority over the row whatever its state,
		// so there is nothing to classify and nothing to guard — the delete goes
		// straight out, saving the pre-read the classified path needs. The
		// trade-off is on already-absent outpoints (a re-applied MINED batch
		// re-forcing coins a previous apply removed): those now cost a replicated
		// write that no-ops server-side instead of a local read. Deleting an
		// absent key stays a success, so "missing = no-op" is unchanged.
		_, err := s.deleteRecordGuarded(op, nil)
		return err
	}
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, found, err := s.getRecord(op)
		if err != nil {
			return err
		}
		if !found {
			return nil // missing = no-op
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		switch {
		case u.SpentBy != nil:
			return &utxostore.SpentError{Op: op, Winner: *u.SpentBy}
		case u.ReservedBy != "":
			return &utxostore.ReservedError{Op: op, HeldBy: u.ReservedBy}
		case u.Frozen:
			return &utxostore.FrozenError{Op: op}
		}
		res, derr := s.deleteRecordGuarded(op, removableFilter())
		if derr != nil {
			return derr
		}
		if res != deleteGuardLost {
			return nil // deleteRemoved or deleteAbsent: either way, no row remains
		}
		// Lost the guard: a reserve/spend/freeze landed between the snapshot and
		// the delete. Re-read and re-classify — never silently succeed, and never
		// delete the row anyway.
	}
	// Budget exhausted: the row kept flipping between removable and held, so
	// neither a removal nor a refusal is true of it. It is still there; the
	// caller sees a transient error and may retry (see [utxostore.ErrContention]).
	return fmt.Errorf("aerostore: remove %s: %w", op, utxostore.ErrContention)
}

// Freeze implements [utxostore.Store]: sets the hold and removes claimKey so the
// row is invisible to claims. Missing outpoints are per-item
// [utxostore.NotFoundError]s under [utxostore.ErrBatch].
func (s *Store) Freeze(_ context.Context, ops []utxostore.Outpoint) error {
	if s.closed.Load() {
		return errClosed
	}
	var itemErrs []error
	for _, op := range ops {
		key, err := s.keyFor(op)
		if err != nil {
			itemErrs = append(itemErrs, err)
			continue
		}
		wp := as.NewWritePolicy(0, 0)
		wp.RecordExistsAction = as.UPDATE_ONLY
		_, aerr := s.client.Operate(
			wp, key,
			as.PutOp(as.NewBin(binFrozen, 1)),
			removeBinOp(binClaimKey),
		)
		if aerr != nil {
			if aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
				itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
				continue
			}
			itemErrs = append(itemErrs, fmt.Errorf("aerostore: freeze %s: %w", op, aerr))
		}
	}
	return utxostore.JoinBatch(itemErrs)
}

// Unfreeze implements [utxostore.Store]: lifts the hold and restores claimKey
// iff the row is otherwise claimable (unreserved and unspent).
func (s *Store) Unfreeze(_ context.Context, ops []utxostore.Outpoint) error {
	if s.closed.Load() {
		return errClosed
	}
	var itemErrs []error
	for _, op := range ops {
		rec, found, err := s.getRecord(op)
		if err != nil {
			itemErrs = append(itemErrs, err)
			continue
		}
		if !found {
			itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
			continue
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			itemErrs = append(itemErrs, cerr)
			continue
		}
		key, kerr := s.keyFor(op)
		if kerr != nil {
			itemErrs = append(itemErrs, kerr)
			continue
		}
		wp := as.NewWritePolicy(0, 0)
		wp.RecordExistsAction = as.UPDATE_ONLY
		s.fireRestoreRaceHook()
		// Lift the hold and restore claimKey iff the row is otherwise claimable
		// (unreserved AND unspent) — both the guard and the restored key's tier
		// are evaluated server-side against the live record, so a Promote or a
		// reserve/spend racing the snapshot read above can neither leave a
		// stale-tier key nor resurrect a now-reserved/-spent coin as claimable.
		_, aerr := s.client.Operate(
			wp, key,
			removeBinOp(binFrozen),
			restoreClaimKeyOp(as.ExpOr(as.ExpBinExists(binResBy), as.ExpBinExists(binSpentBy)), u),
		)
		switch {
		case aerr == nil:
			// The coin may now be claimable again; re-probe its bucket. As in
			// the other restore paths the live tier may have moved under the
			// snapshot, so mark every tier (see [Store.noteClaimableAllTiers]).
			s.noteClaimableAllTiers(u.UserID, u.Basket, u.Satoshis)
		case aerr.Matches(types.KEY_NOT_FOUND_ERROR):
			itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
		default:
			itemErrs = append(itemErrs, fmt.Errorf("aerostore: unfreeze %s: %w", op, aerr))
		}
	}
	return utxostore.JoinBatch(itemErrs)
}

// Promote implements [utxostore.Store]: retiers rows in either direction.
// Missing rows are skipped, rows already at the target tier are unchanged, and
// the transition applies regardless of reservation/spend/frozen state. When the
// row is claimable its claimKey is rewritten to the new tier atomically.
func (s *Store) Promote(_ context.Context, ops []utxostore.Outpoint, to utxostore.Tier) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	if !to.Valid() {
		return 0, fmt.Errorf("aerostore: invalid target tier %d", to)
	}
	changed := 0
	for _, op := range ops {
		rec, found, err := s.getRecord(op)
		if err != nil {
			return changed, err
		}
		if !found {
			continue // missing = skip
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return changed, cerr
		}
		if u.Tier == to {
			continue // already at target = unchanged
		}
		key, kerr := s.keyFor(op)
		if kerr != nil {
			return changed, kerr
		}
		newClaimKey := claimKeyFor(u.UserID, u.Basket, to, bucketOf(u.Satoshis))
		wp := as.NewWritePolicy(0, 0)
		wp.RecordExistsAction = as.UPDATE_ONLY
		// Rewrite claimKey to the new tier ONLY if the row is currently
		// claimable (claimKey present) — evaluated atomically so a concurrent
		// reserve/spend/freeze is never resurrected as claimable.
		_, aerr := s.client.Operate(
			wp, key,
			as.PutOp(as.NewBin(binTier, int(to))),
			as.ExpWriteOp(binClaimKey,
				as.ExpCond(as.ExpBinExists(binClaimKey), as.ExpStringVal(newClaimKey), as.ExpUnknown()),
				as.ExpWriteFlagEvalNoFail),
		)
		if aerr != nil {
			if aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue
			}
			return changed, fmt.Errorf("aerostore: promote %s: %w", op, aerr)
		}
		// If the row was claimable it is now claimable in the target tier's
		// bucket; re-probe there (harmless if it was reserved/spent).
		s.noteClaimable(u.UserID, u.Basket, to, u.Satoshis)
		changed++
	}
	return changed, nil
}

// RemoveByMintTx implements [utxostore.Store]: removes phantom coins of an
// invalidated mint transaction and classifies the survivors. It fails with
// [utxostore.ErrContention] when a coin keeps flipping between removable and
// held, which is transient: retry the call.
func (s *Store) RemoveByMintTx(_ context.Context, mintTxID chainhash.Hash, ops []utxostore.Outpoint) (utxostore.RemoveByMintReport, error) {
	var report utxostore.RemoveByMintReport
	if s.closed.Load() {
		return report, errClosed
	}
	if err := utxostore.ValidateMintOutpoints(mintTxID, ops); err != nil {
		return report, err
	}

	reservedRefs := make(map[string]*utxostore.ReservationRef)
	var refOrder []string
	seenSpenders := make(map[chainhash.Hash]bool)

	for _, op := range ops {
		survivor, removed, err := s.removeMintTxOp(op)
		if err != nil {
			return report, err
		}
		switch {
		case removed:
			// Was unreserved and unspent (frozen or not) at delete time too: a
			// phantom coin, now gone.
			report.Removed = append(report.Removed, op)
		case survivor == nil:
			// Missing when read, or gone by the time it was re-read: skipped.
		case survivor.SpentBy != nil:
			if w := *survivor.SpentBy; !seenSpenders[w] {
				seenSpenders[w] = true
				report.AlreadySpentBy = append(report.AlreadySpentBy, w)
			}
		case survivor.ReservedBy != "":
			// A descendant is in flight: group the coin under its reservation.
			ref, ok := reservedRefs[survivor.ReservedBy]
			if !ok {
				ref = &utxostore.ReservationRef{Reservation: survivor.ReservedBy, UserID: survivor.UserID, ReservedAt: survivor.ReservedAt}
				reservedRefs[survivor.ReservedBy] = ref
				refOrder = append(refOrder, survivor.ReservedBy)
			}
			if survivor.ReservedAt.Before(ref.ReservedAt) {
				ref.ReservedAt = survivor.ReservedAt
			}
			ref.Outpoints = append(ref.Outpoints, op)
		default:
			// Unreachable: removeMintTxOp only returns a survivor that is spent
			// or reserved. Fail loudly rather than drop the coin from the report.
			return report, fmt.Errorf("aerostore: remove-by-mint-tx %s: internal invariant: survivor is neither spent nor reserved", op)
		}
	}

	for _, token := range refOrder {
		report.AlreadyReserved = append(report.AlreadyReserved, *reservedRefs[token])
	}
	return report, nil
}

// phantomFilter re-asserts, server-side at delete time, the classification
// RemoveByMintTx made from its snapshot: the coin of an invalidated mint tx is
// removable only while unreserved AND unspent.
//
// It is [removableFilter] minus the frozen conjunct, and deliberately so:
// RemoveByMintTx removes frozen phantom coins (see the interface doc), because
// a freeze is a hold placed on a coin, not a claim staked on one, and this coin
// never existed. Only an in-flight descendant — a reservation or a recorded
// spend — can keep it.
func phantomFilter() *as.Expression {
	return as.ExpAnd(
		as.ExpNot(as.ExpBinExists(binResBy)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
	)
}

// removeMintTxOp settles one outpoint of an invalidated mint transaction:
// classify the row, and when it is a phantom coin delete it under a guard that
// re-asserts that classification. Losing the guard means a reserve or spend
// landed inside the snapshot→delete window, so the row is re-read and
// re-classified — it lands in AlreadyReserved/AlreadySpentBy instead of being
// destroyed while a descendant is in flight (audit P1-1).
//
// Exactly one of the results is meaningful: removed=true (the row was deleted),
// survivor != nil (the row is kept, reserved or spent), or both zero (the row is
// gone — never there, or removed by someone else).
func (s *Store) removeMintTxOp(op utxostore.Outpoint) (survivor *utxostore.UTXO, removed bool, err error) {
	for attempt := 0; attempt < casAttempts; attempt++ {
		rec, found, gerr := s.getRecord(op)
		if gerr != nil {
			return nil, false, gerr
		}
		if !found {
			return nil, false, nil // already gone
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return nil, false, cerr
		}
		if u.SpentBy != nil || u.ReservedBy != "" {
			return u, false, nil // kept, and classified by the caller
		}
		res, derr := s.deleteRecordGuarded(op, phantomFilter())
		if derr != nil {
			return nil, false, derr
		}
		switch res {
		case deleteRemoved:
			return nil, true, nil
		case deleteAbsent:
			return nil, false, nil // someone else removed it: not ours to report
		case deleteGuardLost:
			// Re-read and re-classify rather than report a removal that did not
			// happen.
		}
	}
	// Budget exhausted: the row kept flipping between phantom and held, so it
	// belongs in no report slot — it was neither removed nor is it reliably a
	// survivor. The caller sees a transient error and may retry the whole call
	// (see [utxostore.ErrContention]).
	return nil, false, fmt.Errorf("aerostore: remove-by-mint-tx %s: %w", op, utxostore.ErrContention)
}
