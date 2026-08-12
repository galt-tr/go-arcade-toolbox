package aerostore

import (
	"sync"
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
// server-side value filter) so a single probe feeds every shape.
//
// Correctness rests on the reserve CAS, never on the cache. A cached candidate
// is only a hint: [Store.reserve] guards the exact claimKey, so any candidate
// reserved, spent, frozen, removed, or retiered by Promote since it was probed
// loses the CAS and is discarded. Refill REPLACES the snapshot rather than
// appending, so a coin can never be handed out twice and the buffer stays
// bounded by the refill size regardless of pool size. The cache never suppresses
// a probe based on staleness: a coin made claimable again (unspend, unfreeze,
// release, mint, promote) is visible to the very next claim, because an empty
// snapshot always re-probes.
type claimCache struct {
	refill int

	mu      sync.Mutex // guards buckets
	buckets map[string]*claimBucket
}

// claimBucket is one claimKey's candidate snapshot. dataMu guards the snapshot;
// refillMu is the single-flight token ensuring only one goroutine probes when
// the snapshot cannot satisfy a request. Lock order is always refillMu → dataMu
// (the refiller holds refillMu across its dataMu section); no goroutine ever
// holds dataMu while acquiring refillMu, so the two never deadlock.
//
// probedEmpty records that the last whole-bucket probe returned zero records —
// the bucket holds no claimable coin at all — so subsequent claims skip the
// probe entirely (the funder's fast path walks always-empty tiers, e.g.
// TierMined before anything settles, on every payment). It is CLEARED
// event-driven by [claimCache.markClaimable] whenever a coin is made claimable
// in this bucket (mint, unspend, unfreeze, release, promote-in), so a restored
// coin is visible to the next claim. gen guards the refill/invalidation race: a
// refiller only trusts an empty probe (sets probedEmpty) if gen is unchanged
// across the probe, so a markClaimable that lands mid-probe forces a re-probe.
type claimBucket struct {
	dataMu      sync.Mutex
	cands       []candidate
	probedEmpty bool
	gen         uint64
	refillMu    sync.Mutex
}

func newClaimCache(refill int) *claimCache {
	return &claimCache{refill: refill, buckets: make(map[string]*claimBucket)}
}

func (cc *claimCache) bucket(claimKey string) *claimBucket {
	cc.mu.Lock()
	b := cc.buckets[claimKey]
	if b == nil {
		b = &claimBucket{}
		cc.buckets[claimKey] = b
	}
	cc.mu.Unlock()
	return b
}

// markClaimable records that a coin was (or may have been) made claimable in
// claimKey's bucket, so the next claim re-probes even if the bucket was last
// seen empty. It bumps gen so an empty probe in flight cannot subsequently mark
// the bucket empty and hide the new coin. Called AFTER the backing write commits
// (the coin is visible to a probe), by every path that restores a claimKey:
// mint, unspend, unfreeze, release, promote-in.
func (cc *claimCache) markClaimable(claimKey string) {
	b := cc.bucket(claimKey)
	b.dataMu.Lock()
	b.gen++
	b.probedEmpty = false
	b.dataMu.Unlock()
}

// take returns up to want cached candidates for claimKey that satisfy pred,
// choosing by prefer when non-nil (best-fit: pass a<b for smallest-first, a>b
// for largest-first; nil when any order is acceptable). It performs at most one
// index probe — refilling and REPLACING the snapshot when the current one has
// no matching candidate — so a caller that needs more loops over take, each
// call costing at most one query. A short result means the bucket currently
// holds no more matching claimable coins (pool underflow for this predicate),
// never an error on its own.
func (cc *claimCache) take(s *Store, claimKey string, pred func(uint64) bool, prefer func(a, b uint64) bool, want int) ([]candidate, error) {
	if want <= 0 {
		return nil, nil
	}
	b := cc.bucket(claimKey)

	// Fast path: satisfy from the current snapshot without probing. A bucket
	// known to hold no claimable coin at all (probedEmpty, cleared event-driven)
	// returns short without a wasted probe.
	b.dataMu.Lock()
	out := b.pullLocked(pred, prefer, want)
	knownEmpty := b.probedEmpty && len(b.cands) == 0
	b.dataMu.Unlock()
	if len(out) >= want || knownEmpty {
		return out, nil
	}

	// Snapshot cannot satisfy the request: refill (single-flight) and retry.
	if b.refillMu.TryLock() {
		b.dataMu.Lock()
		g := b.gen
		b.dataMu.Unlock()
		cands, err := s.probe(claimKey, nil, int64(cc.refill))
		b.dataMu.Lock()
		if err == nil {
			if len(cands) > 0 {
				b.cands = cands // REPLACE — bounded, no duplicate accumulation
				b.probedEmpty = false
			} else if b.gen == g {
				// Empty probe and no coin was made claimable while we probed:
				// trust it. A racing markClaimable bumps gen, forcing a re-probe.
				b.probedEmpty = true
			}
		}
		more := b.pullLocked(pred, prefer, want-len(out))
		b.dataMu.Unlock()
		b.refillMu.Unlock()
		if err != nil {
			return out, err
		}
		out = append(out, more...)
	} else {
		// Another goroutine is refilling; wait for it, then take what remains.
		// We do not initiate our own probe — that is what keeps refill
		// single-flight under a thundering herd of empty-snapshot claims.
		b.refillMu.Lock()
		b.refillMu.Unlock() //nolint:staticcheck // intentional wait-then-release: gate on the in-flight refill
		b.dataMu.Lock()
		more := b.pullLocked(pred, prefer, want-len(out))
		b.dataMu.Unlock()
		out = append(out, more...)
	}
	return out, nil
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
