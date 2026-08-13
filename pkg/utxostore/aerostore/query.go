package aerostore

import (
	"context"
	"fmt"
	"sort"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// streamQuery runs stmt and hands every record to visit, honoring ctx.
//
// The Aerospike Go client (v8) has NO context-aware API: Query hands back a
// Recordset whose producer goroutines run until it is drained or closed, and
// NewQueryPolicy defaults TotalTimeout to 0 — no time limit whatsoever. A caller
// that wrapped one of these queries in a budget therefore had no way to enforce
// it, which matters because these are index scans: the invKey sindex behind
// [Store.Balance] measured 303,264 entries per bval on the live cluster, so one
// call walks ~300k entries with nothing able to stop it.
//
// So the context is applied on both halves: its deadline bounds the server-side
// work through the policy, and its done channel is selected on alongside the
// results so cancellation takes effect immediately. The recordset is always
// closed — abandoning one mid-walk otherwise strands the client's producer
// goroutines until the GC finalizer happens to run.
func (s *Store) streamQuery(ctx context.Context, policy *as.QueryPolicy, stmt *as.Statement, visit func(*as.Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		policy.TotalTimeout = remaining
	}

	rs, err := s.client.Query(policy, stmt)
	if err != nil {
		return err
	}
	defer func() { _ = rs.Close() }() // idempotent; cancels producers on an early exit

	results := rs.Results()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-results:
			if !ok {
				return nil
			}
			if res.Err != nil {
				return res.Err
			}
			if verr := visit(res.Record); verr != nil {
				return verr
			}
		}
	}
}

// emptyBalance is the zero result returned alongside every Balance error, so a
// caller that ignores the error cannot mistake a partial sum for a real one.
func emptyBalance() utxostore.Balance {
	return utxostore.Balance{
		Claimable:      make(map[utxostore.Tier]uint64),
		ClaimableCount: make(map[utxostore.Tier]int),
	}
}

// Balance implements [utxostore.Store]: sums the (userID, basket) coins via the
// invKey index (which contains only unspent rows). Claimable is per tier
// (unspent, unreserved, not frozen); Reserved is reserved-but-unspent (frozen
// or not). Frozen unreserved rows and spent rows count in neither bucket.
//
// It honors ctx: canceling or expiring it abandons the index walk (see
// streamQuery).
func (s *Store) Balance(ctx context.Context, userID int64, basket string) (utxostore.Balance, error) {
	if s.closed.Load() {
		return emptyBalance(), errClosed
	}

	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binInvKey, invKeyFor(userID, basket))); err != nil {
		return emptyBalance(), fmt.Errorf("aerostore: set balance filter: %w", err)
	}

	b := emptyBalance()
	err := s.streamQuery(ctx, as.NewQueryPolicy(), stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		switch {
		case u.SpentBy != nil:
			return nil // defensive: invKey index should already exclude spent rows
		case u.ReservedBy != "":
			b.Reserved += u.Satoshis
			b.ReservedCount++
		case !u.Frozen:
			b.Claimable[u.Tier] += u.Satoshis
			b.ClaimableCount[u.Tier]++
		}
		return nil
	})
	if err != nil {
		return emptyBalance(), fmt.Errorf("aerostore: balance query: %w", err)
	}
	return b, nil
}

// FindStaleReservations implements [utxostore.Store]: reservations of unspent
// reserved rows whose oldest ReservedAt is before olderThan, oldest first
// (approximate ordering is acceptable). Backed by the numeric resAt index.
//
// It honors ctx across both phases — phase B fans out one query PER stale
// reservation, so an unhonored context there is unbounded in the number of index
// walks, not just in the length of one.
func (s *Store) FindStaleReservations(ctx context.Context, olderThan time.Time, limit int) ([]utxostore.ReservationRef, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if limit <= 0 {
		return nil, nil
	}

	cutoff := olderThan.UnixMilli()
	if cutoff <= 0 {
		return nil, nil
	}

	// Phase A: the resAt index yields rows reserved strictly before the cutoff.
	// A reservation is stale iff its OLDEST row is that old; because the oldest
	// row of a stale reservation necessarily has resAt < cutoff, phase A sees
	// it, so the per-token minimum here IS the reservation's true age. (A
	// reservation may also hold newer rows reserved after the cutoff — those
	// are fetched in phase B so the returned ref lists the full membership.)
	type token struct {
		userID int64
		res    string
	}
	oldest := make(map[token]int64) // true oldest resAt (ms) per stale reservation
	var order []token

	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewRangeFilter(binResAt, 0, cutoff-1)); err != nil {
		return nil, fmt.Errorf("aerostore: set stale filter: %w", err)
	}
	qpA := as.NewQueryPolicy()
	qpA.FilterExpression = as.ExpNot(as.ExpBinExists(binSpentBy)) // unspent only
	if err := s.streamQuery(ctx, qpA, stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		if u.ReservedBy == "" {
			return nil // defensive
		}
		k := token{userID: u.UserID, res: u.ReservedBy}
		at := u.ReservedAt.UnixMilli()
		if cur, ok := oldest[k]; !ok {
			oldest[k] = at
			order = append(order, k)
		} else if at < cur {
			oldest[k] = at
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("aerostore: stale query: %w", err)
	}

	// Oldest-first (approximate ordering is acceptable), then apply the limit
	// before the per-reservation membership fetch so phase B is bounded.
	sort.SliceStable(order, func(i, j int) bool { return oldest[order[i]] < oldest[order[j]] })
	if len(order) > limit {
		order = order[:limit]
	}

	// Phase B: fetch the full unspent membership of each stale reservation via
	// the resBy index (includes rows reserved after the cutoff).
	refs := make([]utxostore.ReservationRef, 0, len(order))
	for _, k := range order {
		ref, ferr := s.reservationMembership(ctx, k.userID, k.res)
		if ferr != nil {
			return nil, ferr
		}
		if len(ref.Outpoints) > 0 {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

// reservationMembership returns the full set of unspent rows held by
// (userID, reservation), dated by the oldest.
func (s *Store) reservationMembership(ctx context.Context, userID int64, reservation string) (utxostore.ReservationRef, error) {
	ref := utxostore.ReservationRef{Reservation: reservation, UserID: userID}
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binResBy, reservation)); err != nil {
		return ref, fmt.Errorf("aerostore: set membership filter: %w", err)
	}
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binUserID), as.ExpIntVal(userID)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
	)
	if err := s.streamQuery(ctx, qp, stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		if ref.ReservedAt.IsZero() || u.ReservedAt.Before(ref.ReservedAt) {
			ref.ReservedAt = u.ReservedAt
		}
		ref.Outpoints = append(ref.Outpoints, u.Outpoint)
		return nil
	}); err != nil {
		return ref, fmt.Errorf("aerostore: membership query: %w", err)
	}
	return ref, nil
}
