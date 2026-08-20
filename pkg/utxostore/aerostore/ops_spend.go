package aerostore

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Spend implements [utxostore.Store]: reserved(reservation) → spent(txid) as a
// single-record CAS. On a guard failure it classifies per the taxonomy — an
// already-recorded spend takes precedence over a freeze; a same-spender replay
// is an idempotent success. With force it records a spend the network has
// already accepted, dropping the reservation and freeze conjuncts from the CAS
// (see [utxostore.Store.Spend]).
//
// Refusals are per item under [utxostore.ErrBatch]. A row that will not hold
// still across the bounded re-CAS budget exits differently per mode, and this
// backend is the reason [utxostore.ErrContention]'s doc says not to assume
// parity: FACT mode reports ErrContention (transient — re-drive the spend, and
// a caller recording an accepted broadcast must not read it as a refused coin),
// while GUARDED mode reports [utxostore.ReservedError] naming the current
// holder, because under a guard a token mismatch is a refusal it can state.
// See [Store.spendFallbackError].
func (s *Store) Spend(_ context.Context, spends []*utxostore.SpendOp, force bool) error {
	if s.closed.Load() {
		return errClosed
	}
	failed := 0
	for _, sp := range spends {
		sp.Err = s.spendOne(sp, force)
		if sp.Err != nil {
			failed++
		}
	}
	return utxostore.BatchCountErr(failed, len(spends))
}

func (s *Store) spendOne(sp *utxostore.SpendOp, force bool) error {
	if err := utxostore.ValidateSpend(sp); err != nil {
		return err
	}
	key, err := s.keyFor(sp.Outpoint)
	if err != nil {
		return err
	}

	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	if force {
		// Fact mode: the only surviving precondition is the spent_by arbiter —
		// unspent, or already spent by this very transaction (an idempotent
		// replay). The reservation and freeze conjuncts are gone: the spend is
		// already on the network and neither can un-happen it. The second arm
		// is safe on an unspent row because comparing an absent bin yields
		// false, as the guarded resBy filter already relies on.
		wp.FilterExpression = as.ExpOr(
			as.ExpNot(as.ExpBinExists(binSpentBy)),
			as.ExpEq(as.ExpBlobBin(binSpentBy), as.ExpBlobVal(sp.SpendingTxID[:])),
		)
	} else {
		// Guarded: reserved by exactly this token, not yet spent, not frozen.
		wp.FilterExpression = as.ExpAnd(
			as.ExpEq(as.ExpStringBin(binResBy), as.ExpStringVal(sp.Reservation)),
			as.ExpNot(as.ExpBinExists(binSpentBy)),
			as.ExpNot(as.ExpBinExists(binFrozen)),
		)
	}

	// The spend consumes the coin, so any pre-broadcast pin goes with it.
	ops := []*as.Operation{
		as.PutOp(as.NewBin(binSpentBy, sp.SpendingTxID[:])),
		removeBinOp(binInvKey),
		removeBinOp(binPinned),
	}
	if force {
		// A guarded spend only ever lands on a reserved row, whose claimKey is
		// already gone, so the guarded op set never needed this. A fact-mode
		// spend CAN land on a still-claimable row (unreserved, or
		// frozen-then-thawed), and the claim path has no spentBy conjunct
		// anywhere: reserve's CAS guards ONLY claimKey equality. So a leftover
		// claimKey means the next claimer WINS the row and hands an
		// already-spent coin to a second transaction — precisely the double
		// spend this mode exists to prevent. Removing an absent bin is a no-op,
		// which is why the same op is harmless on rows that never had one.
		ops = append(ops, removeBinOp(binClaimKey))
	}

	// casAttempts: the guard-failure classification can, under a concurrent
	// release+re-reserve, observe a state that is spendable again; retry once.
	for attempt := 0; attempt < casAttempts; attempt++ {
		_, aerr := s.client.Operate(wp, key, ops...)
		if aerr == nil {
			return nil // spent
		}
		if !aerr.Matches(types.FILTERED_OUT) && !aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return fmt.Errorf("aerostore: spend %s: %w", sp.Outpoint, aerr)
		}
		classified, retry, cerr := s.classifySpendFailure(sp, force)
		if cerr != nil {
			return cerr
		}
		if !retry {
			return classified
		}
	}
	// Persistent ambiguity under contention.
	return s.spendFallbackError(sp, force)
}

// classifySpendFailure inspects the row after a failed spend CAS and returns the
// per-item error (or nil for an idempotent same-spender replay). retry is true
// when the row appears spendable again (a race) and the caller should re-CAS.
// force selects the mode: the missing-row and spent-by classifications hold in
// both, while the freeze and reservation refusals exist only under a guard.
func (s *Store) classifySpendFailure(sp *utxostore.SpendOp, force bool) (err error, retry bool, fatal error) {
	rec, found, ferr := s.getRecord(sp.Outpoint)
	if ferr != nil {
		return nil, false, ferr
	}
	if !found {
		return &utxostore.NotFoundError{Op: sp.Outpoint}, false, nil
	}
	u, cerr := recordToUTXO(rec)
	if cerr != nil {
		return nil, false, cerr
	}
	switch {
	case u.SpentBy != nil:
		if *u.SpentBy == sp.SpendingTxID {
			return nil, false, nil // idempotent: same spender
		}
		return &utxostore.SpentError{Op: sp.Outpoint, Winner: *u.SpentBy}, false, nil
	case !force && u.Frozen:
		return &utxostore.FrozenError{Op: sp.Outpoint}, false, nil
	case !force && u.ReservedBy != sp.Reservation:
		return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: u.ReservedBy}, false, nil
	default:
		// Row looks spendable again — a concurrent release+re-reserve (or, in
		// fact mode, a concurrent Unspend) raced the CAS. Signal a retry.
		return nil, true, nil
	}
}

// spendFallbackError is the best-effort report for a spend whose CAS kept
// failing while the row kept looking spendable — pure contention. Guarded mode
// names the current holder; fact mode reports the contention itself, since a
// reservation is not a refusal there.
func (s *Store) spendFallbackError(sp *utxostore.SpendOp, force bool) error {
	rec, found, err := s.getRecord(sp.Outpoint)
	if err != nil {
		return err
	}
	if !found {
		return &utxostore.NotFoundError{Op: sp.Outpoint}
	}
	if force {
		return fmt.Errorf("aerostore: spend %s: %w", sp.Outpoint, utxostore.ErrContention)
	}
	u, cerr := recordToUTXO(rec)
	if cerr != nil {
		return cerr
	}
	return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: u.ReservedBy}
}

// Unspend implements [utxostore.Store]: for each op whose row has SpentBy ==
// spendingTxID, clears the spend AND the reservation, returning the coin to the
// claimable pool (unless frozen). Guard mismatches and missing outpoints are
// skips. Returns the number of rows released.
func (s *Store) Unspend(_ context.Context, spendingTxID chainhash.Hash, ops []utxostore.Outpoint) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	released := 0
	for _, op := range ops {
		rec, found, err := s.getRecord(op)
		if err != nil {
			return released, err
		}
		if !found {
			continue // skip: missing
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return released, cerr
		}
		if u.SpentBy == nil || *u.SpentBy != spendingTxID {
			continue // skip: guard mismatch
		}

		key, kerr := s.keyFor(op)
		if kerr != nil {
			return released, kerr
		}
		wp := as.NewWritePolicy(0, 0)
		wp.RecordExistsAction = as.UPDATE_ONLY
		wp.FilterExpression = as.ExpEq(as.ExpBlobBin(binSpentBy), as.ExpBlobVal(spendingTxID[:]))
		s.fireRestoreRaceHook()
		// Restore claimKey unless the row is frozen, deriving its tier from the
		// live tier bin so a concurrent Promote cannot leave a stale-tier key.
		_, aerr := s.client.Operate(
			wp, key,
			removeBinOp(binSpentBy),
			removeBinOp(binResBy),
			removeBinOp(binResAt),
			removeBinOp(binPinned),
			as.PutOp(as.NewBin(binInvKey, invKeyFor(u.UserID, u.Basket))),
			restoreClaimKeyOp(as.ExpBinExists(binFrozen), u),
		)
		if aerr != nil {
			if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue // guard no longer holds: skip
			}
			return released, fmt.Errorf("aerostore: unspend %s: %w", op, aerr)
		}
		// The coin is claimable again (unless frozen); re-probe its bucket. The
		// tier came from the snapshot above, but the restored key's tier came
		// from the live bin, so mark every tier (see
		// [Store.noteClaimableAllTiers]).
		s.noteClaimableAllTiers(u.UserID, u.Basket, u.Satoshis)
		released++
	}
	return released, nil
}

// RemoveSpentBy implements [utxostore.Store]: deletes every row spent by
// spendingTxID, a now-terminal (mined) tx whose inputs are permanently
// consumed. There is no secondary index on spentBy, so this runs a filtered set
// scan and durably deletes each match under a delete-time guard re-asserting
// that same spentBy; a spentBy SI would optimize it if aerostore ever became a
// mined-heavy hot path, but mined-apply is not a hot path. Idempotent; returns
// the number of rows removed.
//
// Both halves honor ctx. The scan is a FULL SET scan — the most expensive walk
// in the package — so a caller's expired budget must be able to stop it, and the
// delete loop is checked per row for the same reason. Stopping early returns the
// rows removed so far alongside the error; the call is idempotent, so a later
// one finishes the job.
func (s *Store) RemoveSpentBy(ctx context.Context, spendingTxID chainhash.Hash) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	// The same predicate serves twice: as the scan's filter, and then as each
	// delete's guard, so a row that a concurrent Unspend (or a re-spend by
	// another transaction) took over between the scan and the delete is filtered
	// out instead of being destroyed (audit P1-1).
	spentByTx := as.ExpEq(as.ExpBlobBin(binSpentBy), as.ExpBlobVal(spendingTxID[:]))

	stmt := as.NewStatement(s.namespace, s.set)
	qp := as.NewQueryPolicy()
	qp.FilterExpression = spentByTx
	var ops []utxostore.Outpoint
	if err := s.streamQuery(ctx, qp, stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		ops = append(ops, u.Outpoint)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("aerostore: remove-spent-by query: %w", err)
	}
	removed := 0
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		res, derr := s.deleteRecordGuarded(op, spentByTx)
		if derr != nil {
			return removed, derr
		}
		// Only a row this call actually deleted counts: a guard loss means a
		// concurrent Unspend reclaimed it (it is alive and must be left alone),
		// and an absent row was removed by someone else between the scan and
		// here — neither is a removal by us.
		if res == deleteRemoved {
			removed++
		}
	}
	return removed, nil
}
