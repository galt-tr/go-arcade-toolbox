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

## Fix B: self-replenishing change — bounds the ledger, halves the load

The ~1,450 TPS plateau above still grew the local ledger unbounded: each payment
consumed one fuel coin but its change landed in the `default` basket, so the
keeper recycled `default→fuel` with a separate createAction+broadcast per payment
(~2× create+broadcast load) and over-minted the pool (60k target → ~780k),
because its claimable-inventory stop test excludes the large in-flight *reserved*
population.

Fix (`storage: self-replenish the fuel pool from payment change`): a throughput
payment's change is routed straight back into the fuel `PoolBasket`, so the pool
refills 1:1 (one coin spent, one minted back). The keeper drops to value top-up
only. Re-measured on the same hybrid + real arcade, `tps=1500 workers=256`:

| metric | before Fix B | after Fix B |
|---|---|---|
| Sustained create (clean 60 s window) | ~1,450 (then ledger-driven decline) | **1,501 TPS** |
| Backpressure | 0 | 0 |
| p95 | ~180 ms | ~174 ms |
| Fuel-pool claimable | grew 60k → 780k | **pinned ~43.5k** (self-replenishing) |
| `default` basket | accumulating | **flat (308)** |
| Unspent working set (`buyer_utxos`) | grew ~1,450/s | **flat** |
| Keeper mint events during blast | continuous (frantic) | **0** (pool never dropped below low-water) |

So the fuel keeper is idle under steady state — the per-payment recycle
create+broadcast is gone (~half the system load), and the unspent working set is
bounded. The remaining spent-row accumulation in the UTXO store is
mining-paced pruning (`RemoveSpentBy` on MINED); on tstn mining lags the blast
window so spent rows linger, but on a chain that mines at pace they prune.

## Combined result

Fix A (unified claim cache + empty-bucket suppression) + Fix B (self-replenishing
change) take the hybrid from a **~600 TPS hard ceiling** to **~1,500 TPS
sustained with zero backpressure, an idle fuel keeper, and a bounded unspent
working set** — bound by secp256k1 signing + HTTP broadcast, not lock contention
or fuel starvation.

Remaining headroom levers (not pursued here): arcade tstn-dev 503-backpressures
the combined broadcast rate at the very top end (a scaling-env arcade would lift
it), and PostgreSQL is not yet the binding constraint but would be profiled next
now that claiming and fuel are off the critical path.

## CE caveat

Aerospike **Community Edition** single node: durable-delete is unavailable
(aerostore warns; deleted rows are not tombstoned and could reappear after a
cold restart) and a single node likely under-represents clustered/Enterprise
performance. The query-routing lock this fix targets is per-node, so a real
multi-node EE cluster would relieve it further on top of the cache.
