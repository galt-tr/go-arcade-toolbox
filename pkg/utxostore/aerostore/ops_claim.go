package aerostore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

const (
	// claimSample is the per-bucket best-of-N sample size for arbitrary-amount
	// claims. The client picks the sample's min (smallest) or max (largest) and
	// reserves it; a bigger sample sharpens the approximation at query cost.
	claimSample = 16
	// casBudget caps the number of failed CAS attempts a single claim call
	// tolerates before returning ErrContention (the funder's outer retry then
	// re-drives the claim). Bounds worst-case latency under heavy contention.
	casBudget = 8
)

// validateClaim rejects underspecified claim inputs (programmer errors).
func validateClaim(sc utxostore.Scope, reservation string) error {
	switch {
	case reservation == "":
		return errors.New("aerostore: reservation must be non-empty")
	case sc.UserID <= 0:
		return errors.New("aerostore: scope user id must be positive")
	case sc.Basket == "":
		return errors.New("aerostore: scope basket must be non-empty")
	case !sc.Tier.Valid():
		return fmt.Errorf("aerostore: invalid scope tier %d", sc.Tier)
	}
	return nil
}

// candidate is one row returned by a claimKey index probe.
type candidate struct {
	rec  *as.Record
	sats uint64
}

// probe runs a claimKey secondary-index query with an optional server-side
// value filter and returns up to maxRecords claimable candidates.
func (s *Store) probe(claimKey string, valueFilter *as.Expression, maxRecords int64) ([]candidate, error) {
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binClaimKey, claimKey)); err != nil {
		return nil, fmt.Errorf("aerostore: set claim filter: %w", err)
	}
	qp := as.NewQueryPolicy()
	qp.MaxRecords = maxRecords
	if valueFilter != nil {
		qp.FilterExpression = valueFilter
	}
	rs, err := s.client.Query(qp, stmt)
	if err != nil {
		return nil, fmt.Errorf("aerostore: claim query: %w", err)
	}
	var out []candidate
	for res := range rs.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("aerostore: claim query result: %w", res.Err)
		}
		sats, _ := asInt64(res.Record.Bins[binSats])
		out = append(out, candidate{rec: res.Record, sats: uint64(sats)}) //nolint:gosec // satoshis are non-negative
	}
	return out, nil
}

// reserve performs the single-record CAS that marks a candidate reserved: it
// sets resBy/resAt and removes claimKey iff the row still carries EXACTLY the
// claimKey it was probed under. Guarding the exact value (not merely its
// existence) is what makes a cached candidate safe: a coin reserved, spent,
// frozen, removed, or retiered by Promote since the probe (Promote rewrites
// claimKey to the new tier) no longer matches, so the CAS filters out and we
// treat it as a lost race rather than reserving a coin whose live tier has
// drifted from the snapshot we would return. Returns (utxo, true) on success,
// (nil, false) on a lost race (FILTERED_OUT, or KEY_NOT_FOUND if the record was
// durably deleted), and a non-nil error only for a genuine backend failure.
func (s *Store) reserve(c candidate, claimKey, reservation string, nowMs int64) (*utxostore.UTXO, bool, error) {
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpEq(as.ExpStringBin(binClaimKey), as.ExpStringVal(claimKey))

	_, aerr := s.client.Operate(
		wp, c.rec.Key,
		as.PutOp(as.NewBin(binResBy, reservation)),
		as.PutOp(as.NewBin(binResAt, nowMs)),
		removeBinOp(binClaimKey),
	)
	if aerr != nil {
		if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil, false, nil // lost the race
		}
		return nil, false, fmt.Errorf("aerostore: reserve: %w", aerr)
	}
	u, cerr := recordToUTXO(c.rec) // snapshot is claimable; overlay reservation
	if cerr != nil {
		return nil, false, cerr
	}
	u.ReservedBy = reservation
	u.ReservedAt = msToTime(nowMs)
	return u, true, nil
}

// ClaimSmallestSufficient implements [utxostore.Store]. It walks best-fit
// buckets upward from bucket(minSats), reserving an approximately-smallest
// sufficient coin. Returns (nil, nil) only when no sufficient claimable coin
// exists; if candidates were seen but all lost to concurrent claimers it
// returns [utxostore.ErrContention] (never a false negative).
func (s *Store) ClaimSmallestSufficient(_ context.Context, sc utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}

	sufficient := func(sats uint64) bool { return sats >= minSats }
	smallest := func(a, b uint64) bool { return a < b }
	losses := 0
	for b := bucketOf(minSats); b < bucketCount; b++ {
		claimKey := claimKeyFor(sc.UserID, sc.Basket, sc.Tier, b)
		for {
			cands, err := s.claimCands(claimKey, as.ExpGreaterEq(as.ExpIntBin(binSats), as.ExpIntVal(int64(minSats))), sufficient, smallest, 1) //nolint:gosec // satoshi amounts are < 2^63
			if err != nil {
				return nil, err
			}
			if len(cands) == 0 {
				break // bucket holds no more sufficient coins; walk up
			}
			for _, c := range cands {
				u, won, rerr := s.reserve(c, claimKey, reservation, s.nowMillis())
				if rerr != nil {
					return nil, rerr
				}
				if won {
					return u, nil
				}
				if losses++; losses >= casBudget {
					return nil, utxostore.ErrContention
				}
			}
		}
	}
	if losses > 0 {
		return nil, utxostore.ErrContention // saw candidates, lost every race
	}
	return nil, nil // pool exhausted for this predicate
}

// claimCands returns up to want claimable candidates for claimKey that satisfy
// pred, preferring by prefer (nil = any order). It draws from the amortised
// claim cache when enabled — one whole-bucket probe feeds many claims — and
// falls back to a direct value-filtered index probe (sorted by prefer) when the
// cache is disabled. Either way the caller must still win the reserve CAS: the
// candidates are only hints.
func (s *Store) claimCands(claimKey string, valueFilter *as.Expression, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) ([]candidate, error) {
	if s.claimCache != nil {
		return s.claimCache.take(s, claimKey, pred, prefer, want)
	}
	// Cache disabled: sample at least claimSample rows so best-fit stays a
	// best-of-sample, then return the preferred `want`.
	sample := int64(want)
	if sample < claimSample {
		sample = claimSample
	}
	cands, err := s.probe(claimKey, valueFilter, sample)
	if err != nil {
		return nil, err
	}
	if prefer != nil {
		sort.Slice(cands, func(i, j int) bool { return prefer(cands[i].sats, cands[j].sats) })
	}
	if len(cands) > want {
		cands = cands[:want]
	}
	return cands, nil
}

// ClaimLargestInsufficient implements [utxostore.Store]. It walks buckets
// downward from bucket(capSats-1), reserving up to limit approximately-largest
// coins strictly below capSats. A short result is not an error.
func (s *Store) ClaimLargestInsufficient(_ context.Context, sc utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if limit <= 0 || capSats == 0 {
		return nil, nil
	}

	insufficient := func(sats uint64) bool { return sats < capSats }
	largest := func(a, b uint64) bool { return a > b }
	claimed := make([]*utxostore.UTXO, 0, limit)
	losses := 0
	for b := bucketOf(capSats - 1); b >= 0; b-- {
		if len(claimed) >= limit {
			break
		}
		claimKey := claimKeyFor(sc.UserID, sc.Basket, sc.Tier, b)
		for len(claimed) < limit {
			cands, err := s.claimCands(claimKey, as.ExpLess(as.ExpIntBin(binSats), as.ExpIntVal(int64(capSats))), insufficient, largest, limit-len(claimed)) //nolint:gosec // satoshi amounts are < 2^63
			if err != nil {
				return nil, err
			}
			if len(cands) == 0 {
				break // bucket holds no more coins below the cap; walk down
			}
			progressed := false
			for _, c := range cands {
				if len(claimed) >= limit {
					break
				}
				u, won, rerr := s.reserve(c, claimKey, reservation, s.nowMillis())
				if rerr != nil {
					return nil, rerr
				}
				if won {
					claimed = append(claimed, u)
					progressed = true
					continue
				}
				if losses++; losses >= casBudget && len(claimed) == 0 {
					return nil, utxostore.ErrContention
				}
			}
			if !progressed {
				break // whole batch lost the CAS race; stop churning this bucket
			}
		}
	}
	if len(claimed) == 0 && losses > 0 {
		return nil, utxostore.ErrContention
	}
	return claimed, nil
}

// ClaimExact implements [utxostore.Store]. Every coin of a given denomination
// shares one bucket, so candidates come from that bucket filtered to sats ==
// denomination — served from the amortised claim cache (one whole-bucket probe
// per batch) when enabled, or a direct index probe otherwise. A result shorter
// than count is pool underflow, not an error.
func (s *Store) ClaimExact(_ context.Context, sc utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}

	valueFilter := as.ExpEq(as.ExpIntBin(binSats), as.ExpIntVal(int64(denomination))) //nolint:gosec // satoshi amounts are < 2^63
	claimKey := claimKeyFor(sc.UserID, sc.Basket, sc.Tier, bucketOf(denomination))
	exact := func(sats uint64) bool { return sats == denomination }

	claimed := make([]*utxostore.UTXO, 0, count)
	losses := 0
	for len(claimed) < count {
		// Exact matches are interchangeable, so no preference ordering — the
		// cache serves any denomination coin, amortising the index probe.
		cands, err := s.claimCands(claimKey, valueFilter, exact, nil, count-len(claimed))
		if err != nil {
			return nil, err
		}
		if len(cands) == 0 {
			break // pool underflow for this denomination
		}
		progressed := false
		for _, c := range cands {
			if len(claimed) >= count {
				break
			}
			u, won, rerr := s.reserve(c, claimKey, reservation, s.nowMillis())
			if rerr != nil {
				return nil, rerr
			}
			if won {
				claimed = append(claimed, u)
				progressed = true
				continue
			}
			losses++
		}
		if !progressed {
			break // whole batch lost the CAS race; stop (funder retries on ErrContention)
		}
	}
	// Zero reserved despite candidates lost to concurrent claimers: retryable.
	if len(claimed) == 0 && losses > 0 {
		return nil, utxostore.ErrContention
	}
	return claimed, nil
}

// ReleaseReservation implements [utxostore.Store]: frees every unspent row held
// by (userID, reservation) and returns the count freed. Idempotent; spent rows
// are never touched (their reservation is provenance).
func (s *Store) ReleaseReservation(_ context.Context, userID int64, reservation string) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	if reservation == "" {
		return 0, errors.New("aerostore: reservation must be non-empty")
	}

	// Probe the resBy index; restrict to this user and to unspent rows.
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binResBy, reservation)); err != nil {
		return 0, fmt.Errorf("aerostore: set release filter: %w", err)
	}
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binUserID), as.ExpIntVal(userID)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
	)
	rs, err := s.client.Query(qp, stmt)
	if err != nil {
		return 0, fmt.Errorf("aerostore: release query: %w", err)
	}

	var rows []*utxostore.UTXO
	for res := range rs.Results() {
		if res.Err != nil {
			return 0, fmt.Errorf("aerostore: release query result: %w", res.Err)
		}
		u, cerr := recordToUTXO(res.Record)
		if cerr != nil {
			return 0, cerr
		}
		rows = append(rows, u)
	}

	released := 0
	for _, u := range rows {
		ok, rerr := s.releaseRow(u, reservation)
		if rerr != nil {
			return released, rerr
		}
		if ok {
			released++
		}
	}
	return released, nil
}

// ReleaseOutpoints implements [utxostore.Store]: frees each listed row iff it is
// currently reserved by exactly this reservation and unspent. Guard mismatches,
// spent rows and missing outpoints are per-item skips.
func (s *Store) ReleaseOutpoints(_ context.Context, reservation string, ops []utxostore.Outpoint) error {
	if s.closed.Load() {
		return errClosed
	}
	if reservation == "" {
		return errors.New("aerostore: reservation must be non-empty")
	}
	for _, op := range ops {
		rec, found, err := s.getRecord(op)
		if err != nil {
			return err
		}
		if !found {
			continue // skip: missing
		}
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		if u.ReservedBy != reservation || u.SpentBy != nil {
			continue // skip: stale or foreign release intent
		}
		if _, rerr := s.releaseRow(u, reservation); rerr != nil {
			return rerr
		}
	}
	return nil
}

// releaseRow performs the CAS that returns a reserved row to the claimable
// pool: guarded by resBy == reservation and unspent, it removes resBy/resAt and
// restores claimKey (unless the row is frozen). The restored claimKey's tier is
// derived server-side from the live tier bin, so a Promote that raced the
// caller's snapshot read cannot leave a stale-tier key. Returns false when the
// guard no longer holds (FILTERED_OUT) — an idempotent skip.
func (s *Store) releaseRow(u *utxostore.UTXO, reservation string) (bool, error) {
	key, err := s.keyFor(u.Outpoint)
	if err != nil {
		return false, err
	}
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binResBy), as.ExpStringVal(reservation)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
	)
	s.fireRestoreRaceHook()
	_, aerr := s.client.Operate(
		wp, key,
		removeBinOp(binResBy),
		removeBinOp(binResAt),
		restoreClaimKeyOp(as.ExpBinExists(binFrozen), u),
	)
	if aerr != nil {
		if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return false, nil // guard no longer holds: skip
		}
		return false, fmt.Errorf("aerostore: release %s: %w", u.Outpoint, aerr)
	}
	return true, nil
}
