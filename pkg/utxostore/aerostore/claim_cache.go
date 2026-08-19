package aerostore

import (
	"container/list"
	"context"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// claimCache amortizes the per-claim claimKey secondary-index probe that is the
// dominant cost of high-throughput claiming on Aerospike. Every claim used to
// issue its own index query, and on a single-node cluster the aerospike client
// serializes concurrent queries on a per-node routing lock — the bottleneck
// profiled at the hybrid throughput ceiling. The cache turns per-claim queries
// into per-batch queries: one whole-bucket probe fills a bounded snapshot that
// many concurrent claims then drain with direct single-record CAS reserves, and
// a per-bucket single-flight token collapses a thundering herd of concurrent
// empty-snapshot claims to a single in-flight probe.
//
// One snapshot per claimKey (user|basket|tier|bucket) serves ALL claim shapes
// for that bucket — ClaimExact, ClaimSmallestSufficient, ClaimLargestInsufficient
// — because they differ only in a value predicate applied client-side, not in
// which records the index returns. The refill fetches the whole bucket (no
// server-side value filter) so a single probe feeds every shape; when that
// unfiltered sample happens to contain nothing the caller's predicate accepts,
// [claimCache.filteredProbe] asks the server the narrow question once.
//
// Correctness rests on the reserve CAS, never on the cache. A cached candidate
// is only a hint: [Store.reserve] guards the exact claimKey, so any candidate
// reserved, spent, frozen, removed, or retiered by Promote since it was probed
// loses the CAS and is discarded. Refill REPLACES the snapshot rather than
// appending, so a coin can never be handed out twice, the buffer stays bounded
// by the refill size regardless of pool size, and a snapshot can never outlive
// the probe that produced it.
//
// # The one thing the cache decides on its own
//
// Skipping a probe. A bucket whose last whole-bucket probe came back empty is
// treated as empty — but only until emptyTTL elapses, and only until this
// process makes a coin claimable there ([claimCache.markClaimable], which every
// restore path calls after its write commits).
//
// The TTL is not a tuning knob, it is the correctness bound. markClaimable only
// ever hears about writes made by THIS process, so a coin minted (or released,
// or unfrozen) into the bucket by ANOTHER replica is invisible to the
// event-driven clear. Without the TTL that invisibility was permanent: one
// replica that happened to probe a bucket while it was empty refused payments
// on a funded wallet until somebody restarted it (audit P2-5). With it, the
// worst case is emptyTTL of staleness — while idle probing of a genuinely empty
// tier (the funder walks TierMined on every payment before anything settles)
// stays capped at one query per bucket per TTL, which is what the negative cache
// existed for in the first place.
type claimCache struct {
	refill     int
	maxBuckets int           // LRU cap; <= 0 means unbounded
	emptyTTL   time.Duration // how long an empty probe may suppress the next one; <= 0 never suppresses
	now        func() time.Time

	mu      sync.Mutex               // guards buckets and lru
	buckets map[string]*list.Element // claimKey -> element whose Value is *claimBucket
	lru     *list.List               // most-recently-used at the front, eviction victim at the back
}

// claimBucket is one claimKey's candidate snapshot. dataMu guards the snapshot;
// refillMu is the single-flight token ensuring only one goroutine probes when
// the snapshot cannot satisfy a request. Lock order is always refillMu → dataMu
// (the refiller holds refillMu across its dataMu section); no goroutine ever
// holds dataMu while acquiring refillMu, so the two never deadlock. Neither is
// ever held while cc.mu is held, so the cache-wide map lock never waits on a
// probe.
//
// probedEmpty records that the last whole-bucket probe returned zero records —
// the bucket holds no claimable coin at all — and probedAt records WHEN, so the
// suppression expires (see the emptyTTL rationale on [claimCache]). It is also
// cleared event-driven by [claimCache.markClaimable] whenever this process makes
// a coin claimable in this bucket (mint, unspend, unfreeze, release,
// promote-in), so an in-process restore is visible to the very next claim
// without waiting out the TTL. gen guards the refill/invalidation race: a
// refiller only trusts an empty probe (sets probedEmpty) if gen is unchanged
// across the probe, so a markClaimable that lands mid-probe forces a re-probe.
type claimBucket struct {
	// key is the claimKey this bucket snapshots. Immutable after construction:
	// it is what an eviction needs to find the bucket in cc.buckets, and what a
	// refill needs to re-probe.
	key string

	dataMu      sync.Mutex
	cands       []candidate
	probedEmpty bool
	probedAt    time.Time
	gen         uint64
	refillMu    sync.Mutex
}

// prober fetches up to maxRecords claimable candidates for claimKey, optionally
// narrowed by a server-side value filter. [Store.probe] is the production
// implementation; the seam exists so the cache's refill, negative-cache and
// eviction logic can be exercised by unit tests with no Aerospike server in
// sight. That matters because every availability bug audit P2-5/P2-6 found was
// a decision the cache made BEFORE it reached the network.
type prober func(ctx context.Context, claimKey string, valueFilter *as.Expression, maxRecords int64) ([]candidate, error)

func newClaimCache(refill, maxBuckets int, emptyTTL time.Duration, now func() time.Time) *claimCache {
	return &claimCache{
		refill:     refill,
		maxBuckets: maxBuckets,
		emptyTTL:   emptyTTL,
		now:        now,
		buckets:    make(map[string]*list.Element),
		lru:        list.New(),
	}
}

// bucket returns claimKey's snapshot, creating it if absent, and marks it as the
// most recently used. Creating one may EVICT the least recently used bucket (see
// [claimCache.evictLocked]).
func (cc *claimCache) bucket(claimKey string) *claimBucket {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if el, ok := cc.buckets[claimKey]; ok {
		cc.lru.MoveToFront(el)
		return el.Value.(*claimBucket) // the list holds only *claimBucket
	}
	b := &claimBucket{key: claimKey}
	cc.buckets[claimKey] = cc.lru.PushFront(b)
	cc.evictLocked()
	return b
}

// lookup returns claimKey's snapshot only if one already exists, WITHOUT
// creating it and without disturbing the LRU order. Recency is claim traffic; a
// bucket nobody claims from should not hold a slot open.
func (cc *claimCache) lookup(claimKey string) *claimBucket {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	el, ok := cc.buckets[claimKey]
	if !ok {
		return nil
	}
	return el.Value.(*claimBucket) // the list holds only *claimBucket
}

// evictLocked drops least-recently-used buckets until the cap is respected.
// Caller must hold cc.mu.
//
// The cap exists because claimKey embeds the user id: a multi-tenant pool with
// millions of wallets would otherwise accumulate one snapshot per (user, basket,
// tier, value-bucket) forever and grow without bound (audit P2-8). Bounding the
// MAP is what matters — each snapshot is already bounded to refill candidates.
//
// Evicting a bucket that another goroutine is mid-take on is safe, and
// deliberately unsynchronized with those holders: they finish against the
// orphaned struct, whose candidates are still just CAS-guarded hints, and
// whatever they write into it (a refilled snapshot, a probedEmpty flag) is
// simply dropped with it. The costs are a re-probe for the next caller and, in
// the narrow window where an orphan holder is still probing, a second in-flight
// probe for that key — never a wrong answer, because the only state an evicted
// bucket could have carried forward is a SUPPRESSION, and a fresh bucket
// suppresses nothing.
func (cc *claimCache) evictLocked() {
	for cc.maxBuckets > 0 && cc.lru.Len() > cc.maxBuckets {
		el := cc.lru.Back()
		if el == nil {
			return
		}
		cc.lru.Remove(el)
		delete(cc.buckets, el.Value.(*claimBucket).key) // the list holds only *claimBucket
	}
}

// markClaimable records that a coin was (or may have been) made claimable in
// claimKey's bucket, so the next claim re-probes even if the bucket was last
// seen empty. It bumps gen so an empty probe in flight cannot subsequently mark
// the bucket empty and hide the new coin. Called AFTER the backing write commits
// (the coin is visible to a probe), by every path that restores a claimKey:
// mint, unspend, unfreeze, release, promote-in.
//
// A claimKey with no snapshot is a no-op rather than a fresh bucket: there is no
// suppression to clear, since a bucket that does not exist cannot be hiding a
// probe, and the next take creates one that probes immediately. Not creating it
// keeps write paths (which mark three tier keys per restore, see
// [Store.noteClaimableAllTiers]) from filling the LRU with buckets no claim ever
// reads.
func (cc *claimCache) markClaimable(claimKey string) {
	b := cc.lookup(claimKey)
	if b == nil {
		return
	}
	b.dataMu.Lock()
	b.gen++
	b.probedEmpty = false
	b.dataMu.Unlock()
}

// take returns up to want cached candidates for claimKey that satisfy pred,
// choosing by prefer when non-nil (best-fit: pass a<b for smallest-first, a>b
// for largest-first; nil when any order is acceptable). valueFilter is the
// server-side expression equivalent to pred, used only for the narrow fallback
// probe; pass nil to forgo it.
//
// It performs at most two index probes — an unfiltered refill that REPLACES the
// snapshot, then at most one value-filtered probe when that sample turned out to
// hold nothing matching — so a caller that needs more loops over take, each call
// costing a bounded number of queries. A short result means the bucket currently
// holds no more matching claimable coins (pool underflow for this predicate),
// never an error on its own.
//
// ctx is honored on entry and across both probes; the single-flight wait is the
// one deliberate exception (see [claimCache.refillAndPull]).
func (cc *claimCache) take(ctx context.Context, p prober, claimKey string, valueFilter *as.Expression, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) ([]candidate, error) {
	if want <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := cc.bucket(claimKey)

	// Fast path: satisfy from the current snapshot without probing. A bucket
	// known to hold no claimable coin at all (probed empty recently, and nothing
	// made claimable here since) returns short without a wasted probe.
	b.dataMu.Lock()
	out := b.pullLocked(pred, prefer, want)
	knownEmpty := b.knownEmptyLocked(cc.now(), cc.emptyTTL)
	b.dataMu.Unlock()
	if len(out) >= want || knownEmpty {
		return out, nil
	}

	// Snapshot cannot satisfy the request: refill (single-flight) and retry.
	more, empty, err := cc.refillAndPull(ctx, p, b, pred, prefer, want-len(out))
	out = append(out, more...)
	if err != nil || len(out) > 0 || empty || valueFilter == nil {
		return out, err
	}

	// Nothing at all matched, yet the bucket is not empty: the unfiltered sample
	// simply missed. Ask the server the narrow question before the caller reads
	// this as underflow (audit P2-6b).
	//
	// Only when the result is EMPTY — which costs no coverage, because a PARTIAL
	// result is unreachable for one claim shape and self-correcting for the other
	// two. ClaimSmallestSufficient always passes want=1, so its result is empty
	// or complete, never partial. ClaimExact and ClaimLargestInsufficient loop
	// until satisfied, and the refill that produced the partial result also set
	// probedEmpty=false, so the next iteration refills again, that refill yields
	// nothing (the same sample still misses), and the fallback fires there
	// instead. The narrow probe is reached in exactly the case it was written
	// for; keeping it off the partial path keeps it off the hot path too.
	more, err = cc.filteredProbe(ctx, p, claimKey, valueFilter, pred, prefer, want)
	return more, err
}

// refillAndPull re-probes the whole bucket under the single-flight token and
// returns what the refreshed snapshot yields. empty reports that the bucket
// holds no claimable coin at all, which is the caller's signal that a narrower
// probe cannot help either.
func (cc *claimCache) refillAndPull(ctx context.Context, p prober, b *claimBucket, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) (cands []candidate, empty bool, err error) {
	if !b.refillMu.TryLock() {
		// Another goroutine is refilling; wait for it, then take what remains.
		// We do not initiate our own probe — that is what keeps refill
		// single-flight under a thundering herd of empty-snapshot claims.
		//
		// The wait is deliberately NOT ctx-interruptible: it is bounded by the
		// one probe in flight (which carries the refiller's own deadline), and
		// bailing out early would turn a collapsed herd back into one probe per
		// caller — the exact cost the token exists to avoid. ctx is re-checked
		// the moment the wait is over.
		b.refillMu.Lock()
		b.refillMu.Unlock() //nolint:staticcheck // intentional wait-then-release: gate on the in-flight refill
		if cerr := ctx.Err(); cerr != nil {
			return nil, false, cerr
		}
		//
		// A waiter reads emptiness from the shared verdict rather than from the
		// refiller's return value, so with the negative cache switched off
		// (emptyTTL <= 0, where knownEmptyLocked trusts nothing) it cannot tell
		// "the refill found nothing" from "we do not know", and each waiter pays
		// one narrow fallback probe the refiller's own caller would have skipped.
		// Deliberate: that configuration exists to buy freshness with queries, and
		// the error is in the safe direction — an extra probe, never a coin
		// declared missing.
		b.dataMu.Lock()
		out := b.pullLocked(pred, prefer, want)
		known := b.knownEmptyLocked(cc.now(), cc.emptyTTL)
		b.dataMu.Unlock()
		return out, known, nil
	}
	defer b.refillMu.Unlock()

	b.dataMu.Lock()
	g := b.gen
	b.dataMu.Unlock()

	probed, perr := p(ctx, b.key, nil, int64(cc.refill))

	b.dataMu.Lock()
	if perr == nil {
		// REPLACE unconditionally, including with an empty result. A probe of
		// the whole bucket that returns nothing PROVES every cached candidate is
		// no longer claimable under this claimKey (reserved, spent, frozen,
		// removed or retiered), and keeping those corpses meant pullLocked kept
		// handing them out, every reserve lost its CAS, and the caller reported
		// ErrContention on an empty pool — forever, since a non-empty snapshot
		// also defeats the knownEmpty short-circuit (audit P2-6a). The result
		// cannot be a truncation artifact: MaxRecords only ever caps a non-empty
		// result.
		//
		// A coin already handed out from the OLD snapshot can of course come back
		// in this one (it stays claimable until its reserve CAS lands), so two
		// callers can hold the same hint. That is the cache's standing bargain,
		// not a new hazard: the loser of the CAS wastes one round trip and moves
		// on, which is the same thing that happens to any stale candidate.
		b.cands = probed
		switch {
		case len(probed) > 0:
			b.probedEmpty = false
		case b.gen == g:
			// Empty probe and no coin was made claimable while we probed: trust
			// it, for emptyTTL. A racing markClaimable bumps gen, forcing a
			// re-probe instead.
			b.probedEmpty, b.probedAt = true, cc.now()
		}
	}
	out := b.pullLocked(pred, prefer, want)
	b.dataMu.Unlock()
	return out, perr == nil && len(probed) == 0, perr
}

// filteredProbe is the last resort on a cache miss: ONE index probe that pushes
// the caller's value predicate to the server, for a bucket the unfiltered refill
// just failed to satisfy.
//
// It exists because a refill is a SAMPLE — at most `refill` of the bucket's rows,
// chosen by the index with no value filter — so a bucket holding more than that
// many coins can hand back a full sample in which nothing matches, even though a
// matching coin sits just outside it. A bucket spans a whole power of two, so
// "matches" is a real question there: 512 dust coins and one 1000-sat coin share
// bucket 9, and before this fallback a request for >= 800 sat read that pool as
// empty and reported not-enough-funds on a wallet that had the coin (audit
// P2-6b).
//
// One probe, and the results are NOT folded into the snapshot: they are a
// value-biased subset of the bucket, so caching them would make every other
// claim shape's view of it wrong. The cost is bounded — it fires only when the
// unfiltered refill came back non-empty AND yielded the caller nothing at all,
// which for the bucket walks is at most the single partially-matching bucket per
// call (every higher bucket is entirely above minSats, every lower one entirely
// below the cap).
func (cc *claimCache) filteredProbe(ctx context.Context, p prober, claimKey string, valueFilter *as.Expression, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) ([]candidate, error) {
	cands, err := p(ctx, claimKey, valueFilter, sampleSize(want))
	if err != nil {
		return nil, err
	}
	return preferredMatches(cands, pred, prefer, want), nil
}

// knownEmptyLocked reports whether the bucket may be treated as holding no
// claimable coin WITHOUT probing: the last whole-bucket probe came back empty,
// the snapshot it produced is drained, nothing was made claimable here since,
// and that probe is younger than ttl. Caller must hold dataMu.
//
// ttl <= 0 disables the negative cache outright (every claim probes), which is
// the escape hatch for a deployment that would rather pay for the queries than
// tolerate any cross-process staleness at all.
func (b *claimBucket) knownEmptyLocked(now time.Time, ttl time.Duration) bool {
	return b.probedEmpty && len(b.cands) == 0 && ttl > 0 && now.Sub(b.probedAt) < ttl
}

// pullLocked removes and returns up to want candidates matching pred from the
// snapshot, choosing by prefer when non-nil. Caller must hold dataMu. Selection
// is over the cached snapshot only, so best-fit is approximate ("best within the
// sampled bucket") — the honest property Aerospike claiming documents.
func (b *claimBucket) pullLocked(pred func(uint64) bool, prefer func(a, b uint64) bool, want int) []candidate {
	if want <= 0 || len(b.cands) == 0 {
		return nil
	}
	out := make([]candidate, 0, want)
	for len(out) < want {
		best := -1
		for i := range b.cands {
			if !pred(b.cands[i].sats) {
				continue
			}
			if best == -1 {
				best = i
				if prefer == nil {
					break // any match is acceptable
				}
				continue
			}
			if prefer(b.cands[i].sats, b.cands[best].sats) {
				best = i
			}
		}
		if best == -1 {
			break // no matching candidate remains
		}
		out = append(out, b.cands[best])
		last := len(b.cands) - 1
		b.cands[best] = b.cands[last]
		b.cands[last] = candidate{} // drop the *as.Record reference for GC
		b.cands = b.cands[:last]
	}
	return out
}
