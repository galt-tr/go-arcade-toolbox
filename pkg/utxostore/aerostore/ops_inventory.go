package aerostore

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
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
	return batchCountErr(failed, len(mints))
}

func (s *Store) mintOne(m *utxostore.Mint) error {
	switch {
	case m.UserID <= 0:
		return fmt.Errorf("aerostore: mint %s: user id must be positive", m.Outpoint)
	case m.Basket == "":
		return fmt.Errorf("aerostore: mint %s: basket must be non-empty", m.Outpoint)
	case !m.Tier.Valid():
		return fmt.Errorf("aerostore: mint %s: invalid tier %d", m.Outpoint, m.Tier)
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
// under [utxostore.ErrBatch].
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
	return joinBatch(itemErrs)
}

func (s *Store) removeOne(op utxostore.Outpoint, force bool) error {
	rec, found, err := s.getRecord(op)
	if err != nil {
		return err
	}
	if !found {
		return nil // missing = no-op
	}
	if !force {
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
	}
	return s.deleteRecord(op)
}

func (s *Store) deleteRecord(op utxostore.Outpoint) error {
	key, err := s.keyFor(op)
	if err != nil {
		return err
	}
	if _, derr := s.client.Delete(s.deleteWritePolicy(), key); derr != nil {
		return fmt.Errorf("aerostore: delete %s: %w", op, derr)
	}
	return nil
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
	return joinBatch(itemErrs)
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
			// The coin may now be claimable again; re-probe its bucket.
			s.noteClaimable(u.UserID, u.Basket, u.Tier, u.Satoshis)
		case aerr.Matches(types.KEY_NOT_FOUND_ERROR):
			itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
		default:
			itemErrs = append(itemErrs, fmt.Errorf("aerostore: unfreeze %s: %w", op, aerr))
		}
	}
	return joinBatch(itemErrs)
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
// invalidated mint transaction and classifies the survivors.
func (s *Store) RemoveByMintTx(_ context.Context, mintTxID chainhash.Hash, ops []utxostore.Outpoint) (utxostore.RemoveByMintReport, error) {
	var report utxostore.RemoveByMintReport
	if s.closed.Load() {
		return report, errClosed
	}
	for _, op := range ops {
		if op.TxID != mintTxID {
			return report, fmt.Errorf("aerostore: outpoint %s is not an output of mint tx %s", op, mintTxID.String())
		}
	}

	reservedRefs := make(map[string]*utxostore.ReservationRef)
	var refOrder []string
	seenSpenders := make(map[chainhash.Hash]bool)

	for _, op := range ops {
		rec, found, err := s.getRecord(op)
		if err != nil {
			return report, err
		}
		if !found {
			continue // already gone
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return report, cerr
		}
		switch {
		case u.SpentBy != nil:
			if w := *u.SpentBy; !seenSpenders[w] {
				seenSpenders[w] = true
				report.AlreadySpentBy = append(report.AlreadySpentBy, w)
			}
		case u.ReservedBy != "":
			ref, ok := reservedRefs[u.ReservedBy]
			if !ok {
				ref = &utxostore.ReservationRef{Reservation: u.ReservedBy, UserID: u.UserID, ReservedAt: u.ReservedAt}
				reservedRefs[u.ReservedBy] = ref
				refOrder = append(refOrder, u.ReservedBy)
			}
			if u.ReservedAt.Before(ref.ReservedAt) {
				ref.ReservedAt = u.ReservedAt
			}
			ref.Outpoints = append(ref.Outpoints, op)
		default:
			// Unreserved and unspent (frozen or not): a phantom coin, removed.
			if derr := s.deleteRecord(op); derr != nil {
				return report, derr
			}
			report.Removed = append(report.Removed, op)
		}
	}

	for _, token := range refOrder {
		report.AlreadyReserved = append(report.AlreadyReserved, *reservedRefs[token])
	}
	return report, nil
}
