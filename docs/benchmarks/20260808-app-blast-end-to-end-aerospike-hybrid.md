# End-to-end app-blast throughput — Aerospike hybrid (2026-08-08)

Measured with the `toolbox-app-arcade` self-payment blaster against **real arcade
(tstn)**, the **aerospike-hybrid** backend (aerostore UTXO hot path + PostgreSQL
metastore, Mode B). This is the full path — `wallet.CreateAction` → sign →
immediate broadcast → async SSE status apply — not the storage-direct
`perfrunner` numbers.

## Environment

- Buyer wallet, throughput strategy (`HIGH_THROUGHPUT=1`), `IMMEDIATE_BROADCAST=true`,
  `UTXO_BACKEND=aerospike-hybrid`, fuel pool denomination ~21,104 sat,
  `BLAST_AMOUNT_SATS=50`, `FEE_SAT_PER_KB=125`, `MAX_DB_CONNS=180`,
  `FUEL_MINT_CONCURRENCY=64`, `FUEL_RECYCLE_COUNT=8`, `FUEL_TARGET_POOL=60000`.
- Aerospike **Community Edition** single node (`aerospike/aerospike-server:8.1.2.4`,
  namespace `test`, `default-ttl=0`); PostgreSQL 17 (16 GB shared_buffers, tuned).
- Blast: `tps=1500 workers=256`. 62 GB host, rootless podman.

## The finding: the hybrid ceiling was the Aerospike client's query-routing lock

Profiling the stalled hybrid showed the binding constraint was **not** PostgreSQL
(idle), **not** the Aerospike server, and **not** our metadata code. It was the
`aerospike-client-go/v8` per-node query-routing lock: the client's
`internal/atomic.Int` is backed by a `sync.Mutex`, read by `Node.IsActive`
during per-partition query routing, so every claimKey secondary-index query
locked it once per partition. The aerostore hot claim path issued one such query
per claim, so ~168 concurrent claims on the single CE node serialised there
(~40–61 % of all CPU, ~281 of ~704 goroutines parked in the lock).

## Fix: amortise + suppress claim probes (two commits)

1. **Unified claim cache** — one whole-bucket index probe fills a bounded
   per-claimKey snapshot that all three claim shapes (`ClaimExact`,
   `ClaimSmallestSufficient`, `ClaimLargestInsufficient`) drain via direct
   single-record CAS reserves; the value predicate and best-fit preference are
   applied client-side. Per-bucket single-flight collapses a herd of concurrent
   empty-snapshot claims to one probe; refill replaces the snapshot (bounded).
2. **Event-driven empty-bucket suppression** — a whole-bucket probe that returns
   zero marks the bucket empty so later claims skip the probe; any op that makes
   a coin claimable there (mint/unspend/unfreeze/release/promote) clears it, with
   a generation counter closing the refill/invalidation race. This kills the
   re-probe churn from the funder fast path walking always-empty tiers
   (`TierMined` before anything settles) on every payment. No time-based
   throttle — a restored coin is visible to the very next claim.

Correctness rests on the reserve CAS (guards the exact claimKey, treats
`KEY_NOT_FOUND` as a lost race), never on the cache; validated by the aerostore
conformance, contention, promote-race, bucket-walk and underflow suites plus the
storage hybrid Mode-B conformance + status-apply e2e.

## Results

| stage | create (sustained) | backpressure | p95 | aerospike-mutex goroutines | top CPU |
|---|---|---|---|---|---|
| Before (uncached claims) | **~600 hard ceiling** | — | — | 281 / 704 | aerospike query lock |
| Unified cache only | ~1,400 for ~45 s, then **collapse → ~300** | climbed to 18,233 | 900 ms → 14 s | 55 | aerospike query lock 47 % |
| **+ empty-bucket suppression** | **~1,450–1,600, held 5+ min** | **0** | **~180 ms** | **1** | **ECDSA signing** (secp256k1) |

The claim path is off the critical path: the hybrid now sustains **~1,450 TPS
with zero backpressure**, bound by transaction signing (`ec.fieldVal.Square/Mul`,
`ScalarBaseMult`) and HTTP broadcast — real work, not lock contention.

## Remaining constraint (not the claim path, not PostgreSQL)

At ~1,450 TPS the **local ledger grows unbounded** (`buyer_utxos` and the fuel
pool grew ~1,450/s; the pool ballooned from its 60k target toward 780k). Each
payment consumes one fuel coin and the keeper mints a replacement, so the system
does ~2× create+broadcast per payment, and freshly-minted fuel sits at
`TierSending` (unclaimable) during the SEEN lag, so the keeper over-mints. Next
lever ("Fix B"): route each payment's change directly into the fuel pool so it
self-replenishes 1:1 (the keeper then only tops up the value bled to fees),
bounding the ledger and roughly halving the create+broadcast load. Arcade
tstn-dev also 503-backpressures the combined broadcast rate at the very top end;
a scaling-env arcade would lift that.

## CE caveat

Aerospike **Community Edition** single node: durable-delete is unavailable
(aerostore warns; deleted rows are not tombstoned and could reappear after a
cold restart) and a single node likely under-represents clustered/Enterprise
performance. The query-routing lock this fix targets is per-node, so a real
multi-node EE cluster would relieve it further on top of the cache.
