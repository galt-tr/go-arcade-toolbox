package aerostore

import (
	"strconv"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// claimCache amortises the ClaimExact secondary-index probe. Denominated fuel
// pools are the primary high-throughput claim path, and each claim was issuing
// its own claimKey index query — on a single-node cluster the aerospike client
// serialises those queries on a per-node routing lock, which is the dominant
// cost profiled at the ~600 TPS hybrid ceiling. The cache turns per-claim
// queries into per-batch queries: one index probe fills a bounded queue of
// candidates that many concurrent ClaimExact calls then drain with direct
// single-record CAS reserves, collapsing the query rate — and that lock
// contention — by the refill batch factor.
//
// Correctness rests on the reserve CAS, never on the cache. A queued candidate
// is only a hint: [Store.reserve] guards the exact claimKey, so any candidate
// that was reserved, spent, frozen, removed, or retiered by Promote since it was
// probed simply loses the CAS and is discarded. The queue can therefore hold
// stale entries without ever handing out a coin twice, and — because the queue
// is keyed by the exact denomination, not merely the shared log2 bucket — never
// hands a coin of one denomination to a request for another that maps to the
// same bucket.
type claimCache struct {
	refill  int
	mu      sync.Mutex // guards buckets
	buckets map[string]*claimBucket
}

// claimBucket is one denomination's candidate queue plus its refill token. The
// channel gives lock-free multi-consumer draining; refillMu ensures exactly one
// goroutine performs the index probe when the queue empties.
type claimBucket struct {
	ch       chan candidate
	refillMu sync.Mutex
}

func newClaimCache(refill int) *claimCache {
	return &claimCache{refill: refill, buckets: make(map[string]*claimBucket)}
}

// exactCacheKey namespaces a queue by the exact denomination so that two
// denominations sharing a log2 bucket (hence one claimKey) never share a queue.
func exactCacheKey(claimKey string, denomination uint64) string {
	return claimKey + "|=" + strconv.FormatUint(denomination, 10)
}

func (cc *claimCache) bucket(cacheKey string) *claimBucket {
	cc.mu.Lock()
	b := cc.buckets[cacheKey]
	if b == nil {
		b = &claimBucket{ch: make(chan candidate, cc.refill)}
		cc.buckets[cacheKey] = b
	}
	cc.mu.Unlock()
	return b
}

// pop returns the next cached candidate for a denomination, refilling from a
// single claimKey index probe (server-side filtered by valueFilter to the exact
// denomination) when the queue is empty. ok is false only when the pool is
// genuinely empty for this predicate — a short/underflow result, never an error
// on its own. Exactly one goroutine refills at a time; the rest wait for it.
func (cc *claimCache) pop(s *Store, cacheKey, claimKey string, valueFilter *as.Expression) (candidate, bool, error) {
	b := cc.bucket(cacheKey)

	// Fast path: a candidate is already queued.
	select {
	case c := <-b.ch:
		return c, true, nil
	default:
	}

	if b.refillMu.TryLock() {
		// We own the refill. Re-check first: another refiller may have filled
		// the queue between our failed receive and acquiring the token.
		defer b.refillMu.Unlock()
		select {
		case c := <-b.ch:
			return c, true, nil
		default:
		}
		cands, err := s.probe(claimKey, valueFilter, int64(cc.refill))
		if err != nil {
			return candidate{}, false, err
		}
		for _, c := range cands {
			select {
			case b.ch <- c:
			default: // queue full (cap == refill); drop the surplus
			}
		}
	} else {
		// A refill is in flight; block until it completes, then take one if any
		// remain after faster consumers.
		b.refillMu.Lock()
		b.refillMu.Unlock() //nolint:staticcheck // intentional wait-then-release: gate on the in-flight refill
	}

	select {
	case c := <-b.ch:
		return c, true, nil
	default:
		return candidate{}, false, nil // pool empty for this predicate
	}
}
