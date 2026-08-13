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
// is an idempotent success.
func (s *Store) Spend(_ context.Context, spends []*utxostore.SpendOp) error {
	if s.closed.Load() {
		return errClosed
	}
	failed := 0
	for _, sp := range spends {
		sp.Err = s.spendOne(sp)
		if sp.Err != nil {
			failed++
		}
	}
	return batchCountErr(failed, len(spends))
}

func (s *Store) spendOne(sp *utxostore.SpendOp) error {
	if sp.Reservation == "" {
		return fmt.Errorf("aerostore: spend %s: reservation must be non-empty", sp.Outpoint)
	}
	key, err := s.keyFor(sp.Outpoint)
	if err != nil {
		return err
	}

	// The CAS guard: reserved by exactly this token, not yet spent, not frozen.
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binResBy), as.ExpStringVal(sp.Reservation)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
		as.ExpNot(as.ExpBinExists(binFrozen)),
	)

	// Two attempts: the guard-failure classification can, under a concurrent
	// release+re-reserve, observe a state that is spendable again; retry once.
	for attempt := 0; attempt < 2; attempt++ {
		_, aerr := s.client.Operate(
			wp, key,
			as.PutOp(as.NewBin(binSpentBy, sp.SpendingTxID[:])),
			removeBinOp(binInvKey),
		)
		if aerr == nil {
			return nil // spent
		}
		if !aerr.Matches(types.FILTERED_OUT) && !aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return fmt.Errorf("aerostore: spend %s: %w", sp.Outpoint, aerr)
		}
		classified, retry, cerr := s.classifySpendFailure(sp)
		if cerr != nil {
			return cerr
		}
		if !retry {
			return classified
		}
	}
	// Persistent ambiguity under contention: report as a reservation-guard
	// failure naming the current holder (best effort).
	return s.spendGuardError(sp)
}

// classifySpendFailure inspects the row after a failed spend CAS and returns the
// per-item error (or nil for an idempotent same-spender replay). retry is true
// when the row appears spendable again (a race) and the caller should re-CAS.
func (s *Store) classifySpendFailure(sp *utxostore.SpendOp) (err error, retry bool, fatal error) {
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
	case u.Frozen:
		return &utxostore.FrozenError{Op: sp.Outpoint}, false, nil
	case u.ReservedBy != sp.Reservation:
		return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: u.ReservedBy}, false, nil
	default:
		// Row looks spendable again — a concurrent release+re-reserve raced the
		// CAS. Signal a retry.
		return nil, true, nil
	}
}

func (s *Store) spendGuardError(sp *utxostore.SpendOp) error {
	rec, found, err := s.getRecord(sp.Outpoint)
	if err != nil {
		return err
	}
	if !found {
		return &utxostore.NotFoundError{Op: sp.Outpoint}
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
			as.PutOp(as.NewBin(binInvKey, invKeyFor(u.UserID, u.Basket))),
			restoreClaimKeyOp(as.ExpBinExists(binFrozen), u),
		)
		if aerr != nil {
			if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
				continue // guard no longer holds: skip
			}
			return released, fmt.Errorf("aerostore: unspend %s: %w", op, aerr)
		}
		// The coin is claimable again (unless frozen); re-probe its bucket.
		s.noteClaimable(u.UserID, u.Basket, u.Tier, u.Satoshis)
		released++
	}
	return released, nil
}

// RemoveSpentBy implements [utxostore.Store]: deletes every row spent by
// spendingTxID, a now-terminal (mined) tx whose inputs are permanently
// consumed. There is no secondary index on spentBy, so this runs a filtered set
// scan and durably deletes each match; a spentBy SI would optimize it if
// aerostore ever became a mined-heavy hot path, but mined-apply is not a hot
// path. Idempotent; returns the number of rows removed.
func (s *Store) RemoveSpentBy(_ context.Context, spendingTxID chainhash.Hash) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	stmt := as.NewStatement(s.namespace, s.set)
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpEq(as.ExpBlobBin(binSpentBy), as.ExpBlobVal(spendingTxID[:]))
	rs, err := s.client.Query(qp, stmt)
	if err != nil {
		return 0, fmt.Errorf("aerostore: remove-spent-by query: %w", err)
	}
	var ops []utxostore.Outpoint
	for res := range rs.Results() {
		if res.Err != nil {
			return 0, fmt.Errorf("aerostore: remove-spent-by result: %w", res.Err)
		}
		u, cerr := recordToUTXO(res.Record)
		if cerr != nil {
			return 0, cerr
		}
		ops = append(ops, u.Outpoint)
	}
	removed := 0
	for _, op := range ops {
		if derr := s.deleteRecord(op); derr != nil {
			return removed, derr
		}
		removed++
	}
	return removed, nil
}
