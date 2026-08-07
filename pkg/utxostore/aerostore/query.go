package aerostore

import (
	"context"
	"fmt"
	"sort"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

// Balance implements [utxostore.Store]: sums the (userID, basket) coins via the
// invKey index (which contains only unspent rows). Claimable is per tier
// (unspent, unreserved, not frozen); Reserved is reserved-but-unspent (frozen
// or not). Frozen unreserved rows and spent rows count in neither bucket.
func (s *Store) Balance(_ context.Context, userID int64, basket string) (utxostore.Balance, error) {
	b := utxostore.Balance{
		Claimable:      make(map[utxostore.Tier]uint64),
		ClaimableCount: make(map[utxostore.Tier]int),
	}
	if s.closed.Load() {
		return b, errClosed
	}

	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binInvKey, invKeyFor(userID, basket))); err != nil {
		return b, fmt.Errorf("aerostore: set balance filter: %w", err)
	}
	rs, err := s.client.Query(as.NewQueryPolicy(), stmt)
	if err != nil {
		return b, fmt.Errorf("aerostore: balance query: %w", err)
	}
	for res := range rs.Results() {
		if res.Err != nil {
			return utxostore.Balance{
					Claimable:      make(map[utxostore.Tier]uint64),
					ClaimableCount: make(map[utxostore.Tier]int),
				},
				fmt.Errorf("aerostore: balance query result: %w", res.Err)
		}
		u, cerr := recordToUTXO(res.Record)
		if cerr != nil {
			return b, cerr
		}
		switch {
		case u.SpentBy != nil:
			continue // defensive: invKey index should already exclude spent rows
		case u.ReservedBy != "":
			b.Reserved += u.Satoshis
			b.ReservedCount++
		case !u.Frozen:
			b.Claimable[u.Tier] += u.Satoshis
			b.ClaimableCount[u.Tier]++
		}
	}
	return b, nil
}

// FindStaleReservations implements [utxostore.Store]: reservations of unspent
// reserved rows whose oldest ReservedAt is before olderThan, oldest first
// (approximate ordering is acceptable). Backed by the numeric resAt index.
func (s *Store) FindStaleReservations(_ context.Context, olderThan time.Time, limit int) ([]utxostore.ReservationRef, error) {
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
	rsA, err := s.client.Query(qpA, stmt)
	if err != nil {
		return nil, fmt.Errorf("aerostore: stale query: %w", err)
	}
	for res := range rsA.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("aerostore: stale query result: %w", res.Err)
		}
		u, cerr := recordToUTXO(res.Record)
		if cerr != nil {
			return nil, cerr
		}
		if u.ReservedBy == "" {
			continue // defensive
		}
		k := token{userID: u.UserID, res: u.ReservedBy}
		at := u.ReservedAt.UnixMilli()
		if cur, ok := oldest[k]; !ok {
			oldest[k] = at
			order = append(order, k)
		} else if at < cur {
			oldest[k] = at
		}
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
		ref, ferr := s.reservationMembership(k.userID, k.res)
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
func (s *Store) reservationMembership(userID int64, reservation string) (utxostore.ReservationRef, error) {
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
	rs, err := s.client.Query(qp, stmt)
	if err != nil {
		return ref, fmt.Errorf("aerostore: membership query: %w", err)
	}
	for res := range rs.Results() {
		if res.Err != nil {
			return ref, fmt.Errorf("aerostore: membership query result: %w", res.Err)
		}
		u, cerr := recordToUTXO(res.Record)
		if cerr != nil {
			return ref, cerr
		}
		if ref.ReservedAt.IsZero() || u.ReservedAt.Before(ref.ReservedAt) {
			ref.ReservedAt = u.ReservedAt
		}
		ref.Outpoints = append(ref.Outpoints, u.Outpoint)
	}
	return ref, nil
}
