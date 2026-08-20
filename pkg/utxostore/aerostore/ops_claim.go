package aerostore

import (
	"context"
	"fmt"
	"sort"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

const (
	// claimSample is the per-bucket best-of-N sample size for arbitrary-amount
	// claims. The client picks the sample's min (smallest) or max (largest) and
	// reserves it; a bigger sample sharpens the approximation at query cost.
	claimSample = 16
	// casBudget caps the number of failed CAS attempts a single claim call
	// tolerates before returning ErrContention (the funder's outer retry then
	// re-drives the claim). Bounds worst-case latency under heavy contention.
	//
	// It is deliberately far below the claim cache's refill batch
	// ([defaultClaimCacheSize] = 512), and the arithmetic of that gap is worth
	// stating. A snapshot every one of whose candidates has gone stale burns 8
	// of them per claim before conceding, so ~64 consecutive calls report
	// ErrContention before the snapshot empties and a refill replaces it. That
	// is self-healing, not a stall: every one of those calls is retryable, the
	// funder retries them, and the refill that ends the run REPLACES the
	// snapshot unconditionally — including with an empty result, which is the
	// P2-6a fix (see claim_cache.go's refillAndPull) and the reason a poisoned
	// bucket can no longer wedge forever.
	//
	// Raising casBudget toward the refill size would trade that burst of cheap
	// retryable answers for one long claim call holding the caller's latency
	// budget while it walks the same corpses; lowering it would concede before
	// ordinary two- or three-way contention resolves. The gap is the point.
	casBudget = 8
)

// Pin handling for [Store.releaseRow], named so the call sites read as intent
// rather than as a bare bool: the janitor path keeps pins (and skips pinned
// rows), the token-guarded path overrides and clears them.
const (
	keepPins     = false
	overridePins = true
)

// candidate is one row returned by a claimKey index probe.
type candidate struct {
	rec  *as.Record
	sats uint64
}

// probe runs a claimKey secondary-index query with an optional server-side
// value filter and returns up to maxRecords claimable candidates. It satisfies
// the [prober] seam the claim cache probes through.
//
// The walk honors ctx (see [Store.streamQuery]): a claim is on the payment hot
// path, so a caller whose budget has expired must stop paying for index reads
// rather than finish a query nobody is waiting for.
func (s *Store) probe(ctx context.Context, claimKey string, valueFilter *as.Expression, maxRecords int64) ([]candidate, error) {
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binClaimKey, claimKey)); err != nil {
		return nil, fmt.Errorf("aerostore: set claim filter: %w", err)
	}
	qp := as.NewQueryPolicy()
	qp.MaxRecords = maxRecords
	if valueFilter != nil {
		qp.FilterExpression = valueFilter
	}
	var out []candidate
	if err := s.streamQuery(ctx, qp, stmt, func(rec *as.Record) error {
		sats, _ := asInt64(rec.Bins[binSats])
		out = append(out, candidate{rec: rec, sats: uint64(sats)}) //nolint:gosec // satoshis are non-negative
		return nil
	}); err != nil {
		return nil, fmt.Errorf("aerostore: claim query: %w", err)
	}
	return out, nil
}

// sampleSize is how many rows a value-filtered probe asks for when the caller
// wants `want`: never fewer than [claimSample], so best-fit stays a best-of-N
// sample rather than "whatever the index happened to return first".
func sampleSize(want int) int64 {
	if want < claimSample {
		return claimSample
	}
	return int64(want)
}

// preferredMatches keeps the candidates satisfying pred, orders them by prefer
// (nil = any order), and trims to want. The pred pass is defensive — a
// value-filtered probe already applied the equivalent expression server-side —
// but pred is the authority on what the caller asked for, and it costs one
// comparison per sampled row.
func preferredMatches(cands []candidate, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) []candidate {
	out := cands[:0:0]
	for _, c := range cands {
		if pred(c.sats) {
			out = append(out, c)
		}
	}
	if prefer != nil {
		sort.Slice(out, func(i, j int) bool { return prefer(out[i].sats, out[j].sats) })
	}
	if len(out) > want {
		out = out[:want]
	}
	return out
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
	_, aerr := s.client.Operate(
		reservePolicy(claimKey), c.rec.Key,
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

// reservePolicy is the write policy behind the claim-reserve CAS: UPDATE_ONLY
// (never resurrect a deleted coin) guarded on the row still carrying EXACTLY the
// claimKey it was probed under.
//
// # This CAS MUST NOT be auto-retried
//
// It is not idempotent from the client's point of view. The very write it
// performs — removing claimKey — falsifies its own guard, so a retry of a
// request that actually SUCCEEDED but whose reply was lost comes back
// FILTERED_OUT, which [Store.reserve] reads as "lost the race". The coin is then
// reserved on the server by a token the caller believes it never got: a phantom
// reservation, freed only by the stale-reservation sweep (audit P2-7).
//
// Nothing retries it today, and this constructor is where that stays true:
// [as.NewWritePolicy] explicitly sets MaxRetries = 0 for exactly this reason
// ("writes may not be idempotent"), and the store configures no dynamic config
// provider, which is the one other thing that can raise a write's retry count at
// command time (WritePolicy.patchDynamic honors Dynamic.Write.MaxRetries).
// TestReservePolicy_NoRetries pins both halves of that claim.
//
// If retries ever DO become desirable here, raising MaxRetries alone is not the
// fix. The reserve would need a per-call nonce: stamp resBy with a value unique
// to this attempt (or carry an attempt id in a bin), widen the guard to
// "claimKey == expected OR resBy == this attempt's nonce", and treat the second
// arm as a WIN rather than a loss — that makes the retry observe its own
// completed work instead of mistaking it for somebody else's. Deliberately not
// implemented: unreachable code guarding an unreachable path is a liability, and
// the guard above is only correct because nothing retries.
func reservePolicy(claimKey string) *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpEq(as.ExpStringBin(binClaimKey), as.ExpStringVal(claimKey))
	return wp
}

// ClaimSmallestSufficient implements [utxostore.Store]. It walks best-fit
// buckets upward from bucket(minSats), reserving an approximately-smallest
// sufficient coin. Returns (nil, nil) only when no sufficient claimable coin
// exists; if candidates were seen but all lost to concurrent claimers it
// returns [utxostore.ErrContention] (never a false negative).
//
// The bucket walk honors ctx: it spans up to 64 buckets, each of which can cost
// an index probe, so a caller whose budget expired mid-walk stops there.
func (s *Store) ClaimSmallestSufficient(ctx context.Context, sc utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := utxostore.ValidateClaim(sc, reservation); err != nil {
		return nil, err
	}

	sufficient := func(sats uint64) bool { return sats >= minSats }
	smallest := func(a, b uint64) bool { return a < b }
	losses := 0
	for b := bucketOf(minSats); b < bucketCount; b++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		claimKey := claimKeyFor(sc.UserID, sc.Basket, sc.Tier, b)
		for {
			cands, err := s.claimCands(ctx, claimKey, as.ExpGreaterEq(as.ExpIntBin(binSats), as.ExpIntVal(int64(minSats))), sufficient, smallest, 1) //nolint:gosec // satoshi amounts are < 2^63
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
// pred, preferring by prefer (nil = any order). It draws from the amortized
// claim cache when enabled — one whole-bucket probe feeds many claims, with
// valueFilter reserved for the cache's narrow fallback probe — and issues a
// direct value-filtered index probe when the cache is disabled. Either way the
// caller must still win the reserve CAS: the candidates are only hints.
func (s *Store) claimCands(ctx context.Context, claimKey string, valueFilter *as.Expression, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) ([]candidate, error) {
	if s.claimCache != nil {
		return s.claimCache.take(ctx, s.probe, claimKey, valueFilter, pred, prefer, want)
	}
	// Cache disabled: sample at least claimSample rows so best-fit stays a
	// best-of-sample, then return the preferred `want`.
	cands, err := s.probe(ctx, claimKey, valueFilter, sampleSize(want))
	if err != nil {
		return nil, err
	}
	return preferredMatches(cands, pred, prefer, want), nil
}

// ClaimLargestInsufficient implements [utxostore.Store]. It walks buckets
// downward from bucket(capSats-1), reserving up to limit approximately-largest
// coins strictly below capSats. A short result is not an error. The walk honors
// ctx, for the same reason [Store.ClaimSmallestSufficient]'s does.
func (s *Store) ClaimLargestInsufficient(ctx context.Context, sc utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := utxostore.ValidateClaim(sc, reservation); err != nil {
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		claimKey := claimKeyFor(sc.UserID, sc.Basket, sc.Tier, b)
		for len(claimed) < limit {
			cands, err := s.claimCands(ctx, claimKey, as.ExpLess(as.ExpIntBin(binSats), as.ExpIntVal(int64(capSats))), insufficient, largest, limit-len(claimed)) //nolint:gosec // satoshi amounts are < 2^63
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
// denomination — served from the amortized claim cache (one whole-bucket probe
// per batch) when enabled, or a direct index probe otherwise. A result shorter
// than count is pool underflow, not an error. Honors ctx per drain iteration.
func (s *Store) ClaimExact(ctx context.Context, sc utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := utxostore.ValidateClaim(sc, reservation); err != nil {
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Exact matches are interchangeable, so no preference ordering — the
		// cache serves any denomination coin, amortizing the index probe.
		cands, err := s.claimCands(ctx, claimKey, valueFilter, exact, nil, count-len(claimed))
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

// ReserveOutpoints implements [utxostore.Store]: an all-or-nothing hold on the
// exact rows the caller named, so provided inputs get the same exclusivity a
// funded coin gets. Each op is one guarded CAS, the same single-record
// transition a claim uses — the record's claimKey bin exists IFF the coin is
// claimable, so demanding its presence is the whole precondition, and removing
// it in the same Operate is what takes the coin out of the claim index.
//
// # Atomicity is BOUNDED here, not absolute
//
// Aerospike has no multi-record transaction, so "no row is left newly reserved"
// is enforced by COMPENSATION: rows won earlier in the call are handed back
// through the same token-guarded release path the funder's rollback uses
// ([Store.releaseRow] with [keepPins] — a fresh reservation is never pinned, so
// there is no pin to override). A crash between the failing op and the last
// compensating release leaves rows reserved by a token nobody will ever use:
// the ordinary stale-reservation sweep reclaims them, exactly as it does for a
// funder that died mid-run.
//
// That is the same CLASS of bounded window [Store.Pin] documents, but it fails
// the other way and the difference matters. A partial Pin leaves rows
// UNDER-pinned — sweepable when they should not be — while this residue leaves
// rows OVER-reserved: held when they should be free. Over-reserved is the safe
// direction (no second transaction can take those coins), but unlike a missing
// pin it does not self-correct at the next broadcast; it waits for the sweep.
// A deployment that needs the window closed rather than bounded wants a SQL
// backend, where one transaction reserves the whole set or none of it.
//
// Rows already held by exactly this reservation are satisfied WITHOUT a write:
// a replay must not slide resAt forward and hide the reservation from the
// stale reaper.
func (s *Store) ReserveOutpoints(_ context.Context, userID int64, reservation string, ops []utxostore.Outpoint) error {
	if s.closed.Load() {
		return errClosed
	}
	if err := utxostore.ValidateReserveOutpoints(reservation, ops); err != nil {
		return err
	}

	// ONE timestamp for the whole set: the named inputs were reserved by a
	// single decision, so they must age as a unit under the stale reaper rather
	// than by however long the per-record CAS loop took.
	nowMs := s.nowMillis()
	var (
		itemErrs []error
		won      []*utxostore.UTXO
	)
	for _, op := range distinctOutpoints(ops) {
		u, itemErr, fatal := s.reserveOutpointOne(op, userID, reservation, nowMs)
		if fatal != nil {
			s.undoReserveOutpoints(won, reservation)
			return fatal
		}
		if itemErr != nil {
			itemErrs = append(itemErrs, itemErr)
			continue
		}
		if u != nil {
			won = append(won, u)
		}
	}
	if len(itemErrs) > 0 {
		s.undoReserveOutpoints(won, reservation)
		return utxostore.JoinBatch(itemErrs)
	}
	return nil
}

// distinctOutpoints drops repeats, preserving first-seen order. Duplicates in
// one ReserveOutpoints call name the same row, so classifying it twice would
// report the same refusal twice for a single coin.
func distinctOutpoints(ops []utxostore.Outpoint) []utxostore.Outpoint {
	seen := make(map[utxostore.Outpoint]struct{}, len(ops))
	out := make([]utxostore.Outpoint, 0, len(ops))
	for _, op := range ops {
		if _, dup := seen[op]; dup {
			continue
		}
		seen[op] = struct{}{}
		out = append(out, op)
	}
	return out
}

// reserveOutpointOne reserves one named row. It returns a non-nil UTXO only for
// a row this CALL won (so the caller knows exactly what to hand back on
// failure); an already-ours row is a satisfied no-op and reports nothing.
func (s *Store) reserveOutpointOne(op utxostore.Outpoint, userID int64, reservation string, nowMs int64) (won *utxostore.UTXO, itemErr, fatal error) {
	u, satisfied, itemErr, fatal := s.classifyForReserve(op, userID, reservation)
	if fatal != nil || itemErr != nil || satisfied {
		return nil, itemErr, fatal
	}

	ok, err := s.casReserveOutpoint(u, userID, reservation, nowMs)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return u, nil, nil
	}

	// The snapshot went stale between the read and the CAS. Look again: the
	// live state usually names the refusal in the taxonomy (a concurrent
	// claimer took it, an operator froze it, a broadcast spent it).
	_, satisfied, itemErr, fatal = s.classifyForReserve(op, userID, reservation)
	switch {
	case fatal != nil:
		return nil, nil, fatal
	case itemErr != nil:
		return nil, itemErr, nil
	case satisfied:
		return nil, nil, nil
	}
	// It does not: the row read as claimable again, so the CAS lost a race that
	// has since resolved. Transient, and the caller's retry is the fix.
	return nil, nil, utxostore.ErrContention
}

// classifyForReserve reads a named row and decides its fate. satisfied marks a
// row already held by exactly this reservation, which needs no write at all.
//
// Refusal precedence matches [Store.Remove]: spent > reserved-by-another >
// frozen. A row owned by a DIFFERENT user reports NotFound rather than
// Reserved — the caller named the outpoint, so the reply must not confirm that
// somebody else's coin exists. It shares its name with sqlstore's classifier
// because it must make the identical rulings; the two stay in step by design.
//
// The read is only a snapshot; [Store.casReserveOutpoint] is the arbiter, and a
// stale snapshot costs a second look, never a wrong write.
func (s *Store) classifyForReserve(op utxostore.Outpoint, userID int64, reservation string) (u *utxostore.UTXO, satisfied bool, itemErr, fatal error) {
	rec, found, gerr := s.getRecord(op)
	if gerr != nil {
		return nil, false, nil, gerr
	}
	if !found {
		return nil, false, &utxostore.NotFoundError{Op: op}, nil
	}
	row, cerr := recordToUTXO(rec)
	if cerr != nil {
		return nil, false, nil, cerr
	}
	switch {
	case row.UserID != userID:
		return nil, false, &utxostore.NotFoundError{Op: op}, nil
	case row.SpentBy != nil:
		return nil, false, &utxostore.SpentError{Op: op, Winner: *row.SpentBy}, nil
	case row.ReservedBy != "" && row.ReservedBy != reservation:
		return nil, false, &utxostore.ReservedError{Op: op, HeldBy: row.ReservedBy}, nil
	case row.Frozen:
		return nil, false, &utxostore.FrozenError{Op: op}, nil
	case row.ReservedBy == reservation:
		return row, true, nil, nil
	}
	return row, false, nil, nil
}

// casReserveOutpoint is the single-record transition behind ReserveOutpoints:
// guarded by "still claimable AND owned by this user", it stamps resBy/resAt
// and removes claimKey so the coin leaves the claim index synchronously. The
// claimKey guard is what makes it exclusive against every concurrent claim
// shape — both write the same bin under the same precondition, so exactly one
// wins. Returns false on a lost race (FILTERED_OUT, or KEY_NOT_FOUND when the
// record was durably deleted).
func (s *Store) casReserveOutpoint(u *utxostore.UTXO, userID int64, reservation string, nowMs int64) (bool, error) {
	key, err := s.keyFor(u.Outpoint)
	if err != nil {
		return false, err
	}
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpAnd(
		as.ExpBinExists(binClaimKey),
		as.ExpEq(as.ExpIntBin(binUserID), as.ExpIntVal(userID)),
	)

	if _, aerr := s.client.Operate(wp, key,
		as.PutOp(as.NewBin(binResBy, reservation)),
		as.PutOp(as.NewBin(binResAt, nowMs)),
		removeBinOp(binClaimKey),
	); aerr != nil {
		if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return false, nil // lost the race
		}
		return false, fmt.Errorf("aerostore: reserve outpoint %s: %w", u.Outpoint, aerr)
	}
	u.ReservedBy = reservation
	u.ReservedAt = msToTime(nowMs)
	return true, nil
}

// undoReserveOutpoints hands back the rows a failed ReserveOutpoints won,
// restoring their claimKey (and telling the claim cache about it) through the
// same token-guarded release the funder's own rollback uses. keepPins is the
// right posture rather than a compromise: these reservations are seconds old
// and nothing has pinned them.
//
// Neither way of failing to compensate is returned — the caller already has the
// error that matters — but BOTH are logged, because both leave the same
// residue: a row still held by a token the caller has abandoned, waiting on the
// stale sweep. A backend error is one way. The other is a release the server
// simply refused: releaseRow reports ok=false when its guard no longer holds,
// which here means something pinned or spent the row in the moments after we
// won it. That is a silent skip unless it is logged, and a silent skip in a
// rollback is exactly the kind of thing that turns into an unexplained stuck
// coin weeks later.
func (s *Store) undoReserveOutpoints(won []*utxostore.UTXO, reservation string) {
	for _, u := range won {
		ok, err := s.releaseRow(u, reservation, keepPins)
		switch {
		case err != nil:
			s.logger.Error("rolling back a partial ReserveOutpoints failed; the row stays reserved by this token until the stale sweep frees it",
				"outpoint", u.String(), "reservation", reservation, "err", err)
		case !ok:
			s.logger.Warn("rolling back a partial ReserveOutpoints skipped a row: its release guard no longer holds (pinned or spent); it stays reserved by this token until the stale sweep frees it",
				"outpoint", u.String(), "reservation", reservation)
		}
	}
}

// ReleaseReservation implements [utxostore.Store]: frees every unspent,
// UNPINNED row held by (userID, reservation) and returns the count freed.
// Idempotent; spent rows are never touched (their reservation is provenance),
// and pinned rows are never touched (a signed transaction spends them — see
// [Store.Pin]).
//
// Both halves honor ctx — the membership scan and the per-row release loop — so
// a canceled sweep stops instead of walking an index and issuing writes for a
// caller that has gone away. Stopping early is safe: releasing is idempotent and
// per row, so the count returned with the error is exactly what was freed, and a
// later call finishes the rest.
func (s *Store) ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	if err := utxostore.ValidateReservation(reservation); err != nil {
		return 0, err
	}

	// Probe the resBy index; restrict to this user and to unspent, unpinned
	// rows. The pin guard is re-applied per record in releaseRow, so a row
	// pinned between this query and the CAS is still refused.
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binResBy, reservation)); err != nil {
		return 0, fmt.Errorf("aerostore: set release filter: %w", err)
	}
	qp := as.NewQueryPolicy()
	qp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binUserID), as.ExpIntVal(userID)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
		as.ExpNot(as.ExpBinExists(binPinned)),
	)

	var rows []*utxostore.UTXO
	if err := s.streamQuery(ctx, qp, stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		rows = append(rows, u)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("aerostore: release query: %w", err)
	}

	released := 0
	for _, u := range rows {
		if err := ctx.Err(); err != nil {
			return released, err
		}
		ok, rerr := s.releaseRow(u, reservation, keepPins)
		if rerr != nil {
			return released, rerr
		}
		if ok {
			released++
		}
	}
	return released, nil
}

// Pin implements [utxostore.Store]: marks every unspent row held by (userID,
// reservation) as pinned, returning how many it NEWLY pinned. The pin is
// presence-encoded in the pinned bin and written by a guarded per-record CAS
// (reserved by this token, unspent, not already pinned), so a replay pins
// nothing and reports 0.
//
// Unlike the SQL backends this is NOT one atomic multi-record transaction —
// Aerospike has none — so a crash mid-Pin leaves the token PARTLY pinned, and
// the stragglers are exactly as exposed as they were before Pin existed:
// FindStaleReservations still reports them and ReleaseReservation still frees
// them. Two things BOUND that window; neither closes it.
//
//   - The caller re-runs Pin, idempotently, until it succeeds before it
//     broadcasts. The exposure is therefore the client's retry gap, not the
//     broadcast round trip the pin exists to cover.
//   - The provider fences the transaction row before releasing anything: its
//     sweep aborts the owning transaction (CAS to 'aborted' plus the known-tx
//     fence) and only then touches a coin, so a straggler this backend still
//     reports is a coin whose transaction is already unbroadcastable.
//
// A deployment that needs this atomic rather than bounded wants a SQL backend,
// where one UPDATE pins the whole membership or none of it.
func (s *Store) Pin(ctx context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(ctx, userID, reservation, true)
}

// Unpin implements [utxostore.Store]: removes the pinned bin from every unspent
// row held by (userID, reservation), leaving them RESERVED. Idempotent.
func (s *Store) Unpin(ctx context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(ctx, userID, reservation, false)
}

// setPinned drives Pin/Unpin: a resBy index probe for the reservation's live
// membership, then one guarded CAS per record. Claimability is deliberately
// untouched — a pinned coin is reserved, so its claimKey is already absent and
// the claim cache has nothing to learn (no noteClaimable call here).
//
// Both halves honor ctx, exactly as [Store.ReleaseReservation]'s do: the
// membership scan through [Store.streamQuery] (which also guarantees the
// recordset is closed, so an early exit cannot strand the client's producer
// goroutines), and the per-row CAS loop through a per-iteration check. Stopping
// early is safe in either direction — pinning and unpinning are idempotent and
// per row, so the count returned with the error is exactly what changed, and
// the caller's retry (Pin's own, or the outbox drain's for Unpin) finishes the
// rest.
func (s *Store) setPinned(ctx context.Context, userID int64, reservation string, pinned bool) (int, error) {
	if s.closed.Load() {
		return 0, errClosed
	}
	if err := utxostore.ValidateReservation(reservation); err != nil {
		return 0, err
	}

	verb := pinVerb(pinned)
	stmt := as.NewStatement(s.namespace, s.set)
	if err := stmt.SetFilter(as.NewEqualFilter(binResBy, reservation)); err != nil {
		return 0, fmt.Errorf("aerostore: set %s filter: %w", verb, err)
	}
	qp := as.NewQueryPolicy()
	// Only rows already in the target's opposite state are worth visiting; the
	// same guard is re-applied per record, so the count stays exact under a race.
	pinState := as.ExpBinExists(binPinned)
	if pinned {
		pinState = as.ExpNot(pinState)
	}
	qp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpIntBin(binUserID), as.ExpIntVal(userID)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
		pinState,
	)

	var ops []utxostore.Outpoint
	if err := s.streamQuery(ctx, qp, stmt, func(rec *as.Record) error {
		u, cerr := recordToUTXO(rec)
		if cerr != nil {
			return cerr
		}
		ops = append(ops, u.Outpoint)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("aerostore: %s query: %w", verb, err)
	}

	changed := 0
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return changed, err
		}
		ok, perr := s.setPinnedRow(op, reservation, pinned)
		if perr != nil {
			return changed, perr
		}
		if ok {
			changed++
		}
	}
	return changed, nil
}

// pinVerb names the direction of a setPinned call, so an Unpin failure does not
// report itself as a pin failure.
func pinVerb(pinned bool) string {
	if pinned {
		return "pin"
	}
	return "unpin"
}

// setPinnedRow is the single-record CAS behind Pin/Unpin: guarded by "reserved
// by exactly this token, unspent, and not already in the target pin state", it
// writes or removes the pinned bin. A false guard (FILTERED_OUT) or a vanished
// record is an idempotent skip that does not count.
func (s *Store) setPinnedRow(op utxostore.Outpoint, reservation string, pinned bool) (bool, error) {
	key, err := s.keyFor(op)
	if err != nil {
		return false, err
	}

	pinState := as.ExpBinExists(binPinned)
	pinOp := removeBinOp(binPinned)
	if pinned {
		pinState = as.ExpNot(pinState)
		pinOp = as.PutOp(as.NewBin(binPinned, 1))
	}
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpAnd(
		as.ExpEq(as.ExpStringBin(binResBy), as.ExpStringVal(reservation)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
		pinState,
	)

	if _, aerr := s.client.Operate(wp, key, pinOp); aerr != nil {
		if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return false, nil // already in the target state, or no longer ours
		}
		return false, fmt.Errorf("aerostore: %s %s: %w", pinVerb(pinned), op, aerr)
	}
	return true, nil
}

// ReleaseOutpoints implements [utxostore.Store]: frees each listed row iff it is
// currently reserved by exactly this reservation and unspent. Guard mismatches,
// spent rows and missing outpoints are per-item skips. A matching token
// OVERRIDES the pin and clears it — the callers are the funder (whose rows are
// never pinned) and the reconciler's verified-dead release.
func (s *Store) ReleaseOutpoints(_ context.Context, reservation string, ops []utxostore.Outpoint) error {
	if s.closed.Load() {
		return errClosed
	}
	if err := utxostore.ValidateReservation(reservation); err != nil {
		return err
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
		if _, rerr := s.releaseRow(u, reservation, overridePins); rerr != nil {
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
//
// overridePin splits the two release paths; call it with [keepPins] or
// [overridePins] rather than a bare bool. keepPins is the janitor path
// (ReleaseReservation): the guard then also demands an unpinned row, so a
// pinned one FILTERED_OUTs and is skipped, and no sweep can free the inputs of
// an in-flight send. overridePins is the token-guarded path (ReleaseOutpoints —
// the funder's own rollback and the reconciler's verified-dead release): it
// overrides the pin and clears it in the same atomic Operate as the release.
func (s *Store) releaseRow(u *utxostore.UTXO, reservation string, overridePin bool) (bool, error) {
	key, err := s.keyFor(u.Outpoint)
	if err != nil {
		return false, err
	}
	guards := []*as.Expression{
		as.ExpEq(as.ExpStringBin(binResBy), as.ExpStringVal(reservation)),
		as.ExpNot(as.ExpBinExists(binSpentBy)),
	}
	ops := []*as.Operation{removeBinOp(binResBy), removeBinOp(binResAt)}
	if overridePin {
		ops = append(ops, removeBinOp(binPinned))
	} else {
		guards = append(guards, as.ExpNot(as.ExpBinExists(binPinned)))
	}
	ops = append(ops, restoreClaimKeyOp(as.ExpBinExists(binFrozen), u))

	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.UPDATE_ONLY
	wp.FilterExpression = as.ExpAnd(guards...)
	s.fireRestoreRaceHook()
	_, aerr := s.client.Operate(wp, key, ops...)
	if aerr != nil {
		if aerr.Matches(types.FILTERED_OUT) || aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return false, nil // guard no longer holds: skip
		}
		return false, fmt.Errorf("aerostore: release %s: %w", u.Outpoint, aerr)
	}
	// The coin is claimable again (unless frozen); re-probe its bucket. Which
	// TIER's bucket is not knowable here — the restore above derived the key
	// from the record's LIVE tier bin precisely because a Promote may have moved
	// it since u was read — so all three are marked (audit P2-6c).
	s.noteClaimableAllTiers(u.UserID, u.Basket, u.Satoshis)
	return true, nil
}
