# Benchmarks — write-path throughput

> **2026-08-18 — the optimistic create ceiling is ~1,118 TPS, durable.** With a
> 100,000-coin pool, one input, one output and NO change output, an instant 202
> and no monitor daemon, PostgreSQL Mode A sustains **1,118 TPS at 384 workers**
> with `synchronous_commit=on` — roughly double the realistic-payment figure
> below, on a curve that turns over at 512 workers. Zero contention retries at
> every worker count. Per-stage: `sign_process` (56.7 ms p50) costs about twice
> `create` (29.2 ms), which contradicts the gap analysis further down naming the
> create phase the wall. Two caveats worth reading before quoting the number:
> PostgreSQL's default `max_connections=100` caps every run above 64 workers and
> presents as connection errors, and a 1,000,000-coin pool measures ~5% lower
> with double the p99. Full report:
> [20260818-optimistic-create-ceiling.md](20260818-optimistic-create-ceiling.md).


> **Task 28 update — the per-op `COUNT(*)` is gone; the wall is now the durable
> commit.** `CreateAction` no longer runs the change-basket `SELECT COUNT(*)`
> on the throughput hot path (it is skipped entirely under the fuel-pool
> strategy, where clamping change fan-out on a fixed-denomination pool is
> degenerate), and on the tiered path the count is now a cheap indexed count
> over `idx_outputs_user_basket` with no join to `transactions`
> (`pkg/storage/create.go` `changeBasketCount`, `metastore.OutputsRepo.CountInBasket`).
>
> **Re-measured honestly on this box (32-core i9, containers via podman):**
>
> - **1000 TPS is NOT reached with strict per-op durability** (`fsync=on`,
>   `synchronous_commit=on`). The single-node ceiling is **~575 TPS (Postgres)**
>   and **~646 TPS (Aerospike hybrid)** at 256 workers — sub-linear scaling with
>   e2e p99 climbing past 1 s: **latency-bound on the per-`CreateAction` durable
>   ACID commit**, not on the claim (0 contention) and not on sign/broadcast.
> - **1000 TPS IS reached at just 64 workers with relaxed durability**
>   (`synchronous_commit=off`): **1379 TPS**, e2e p50 45 ms — a clean 3.5× over
>   the same-config durable run (393.8 TPS). This is the proof that the ceiling
>   is the durable commit floor, not CPU or the storage logic.
> - **The `COUNT` kill removes all pool-size sensitivity on the throughput path.**
>   Controlled SQLite A/B (single writer, the clean control): pool 2000→16000
>   dropped **98.0→74.7 TPS (−24%) before**, **108.5→93.5 TPS (−14%) after**
>   (+25 % at the large pool). On Postgres the count was already a cheap indexed
>   scan, so the same-container 64-worker A/B is a smaller **370.2→393.8 TPS
>   (+6.4 %)** — real but modest, because the durable commit dominates.
>
> See [Task 28 — optimized results](#task-28--optimized-results-count-kill--honest-re-measure).
> The section below this line is the **pre-optimization (Task 27) baseline**, kept
> for the before/after narrative.

---

> **Read this first.** Two funding paths are now measured side by side:
>
> - **Tiered (privacy)** — a plain `wallet.CreateAction` funding from the change
>   basket with `Denomination=0` via the bounded tiered `SKIP LOCKED` claim.
> - **Throughput (fuel-pool)** — `UTXOManagement.Strategy=throughput`, so
>   `CreateAction` funds via the funder's closed-form **`ClaimExact`** fast path
>   over a denominated pool (`FundArgs.Denomination>0`). This is the 1000-TPS
>   design's route, wired in Task 27 (`pkg/storage/create.go` `fundingSource()`).
>
> **Verdict: 1000 TPS is NOT reached on this box on either path.** The fuel-pool
> path sustains **~175–210 TPS single-call** (Postgres 209.8, hybrid 175.0). What
> it *does* deliver is **deterministic zero claim contention** — every throughput
> run shows `0` contention retries and ~0.5% op-failure, versus the tiered
> Postgres path's high-variance **0–117k** retries and up to **18%** op-failure.
> With claim contention gone, the bottleneck **moves to the `create` phase**
> (fund + reserve + persist DB round trips and their long tail), *not* to
> sign/broadcast. See [Fuel-pool path](#fuel-pool-path-claimexact--task-27) and
> [Gap analysis](#gap-analysis--path-to-1000-tps) below. A fully clean fuel-pool
> ceiling is blocked by a deeper gap (a dedicated, wallet-signable pool basket is
> unreachable through the public API — see the gap analysis).

Captured throughput of the full BRC-100 wallet write path
(`CreateAction → SignAction → ProcessAction/broadcast`), produced by the
performance harness (`internal/perf`, `cmd/perfrunner`, `test/perf`). Each
per-run report renders deterministically from its sibling JSON:

```sh
go run ./cmd/perfrunner render docs/benchmarks/20260807-postgres-twostep.json
```

## Tiered-path results (this box: i9-13900K, 32 logical cores, 62.6 GiB, Fedora 7.1.5, Go 1.26.3)

Bounded 60 s measured window (15 s warmup discarded), 64 workers, 4000-coin
pool, unbounded rate. Broadcasts hit the in-process mockarcade, so these measure
**storage + wallet** cost, not network. SQLite is an in-process baseline
(30 s / 8 workers), not a target.

| Backend | Mode | Sustained TPS | e2e p50 | e2e p95 | e2e p99 | create p50 | sign p50 | claim-contention retries | op-failure rate | maturation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PostgreSQL Mode A (shared SQL) | two-step | **203.7** | 250 ms | 673 ms | 1030 ms | 171 ms | 62 ms | 10,534 | 2.1% | 99.7% |
| PostgreSQL Mode A (shared SQL) | single-call | **211.2** | 97 ms | 156 ms | 248 ms | — | — | 117,585 | 18.2% | 100% |
| Aerospike + PostgreSQL Mode B (split) | two-step | **152.7** | 378 ms | 621 ms | 780 ms | 319 ms | 54 ms | 0 | 0.55% | 99.9% |
| Aerospike + PostgreSQL Mode B (split) | single-call | **153.6** | 388 ms | 631 ms | 800 ms | — | — | 0 | 0.55% | 100% |
| SQLite Mode A (baseline — not a target) | two-step | 62.2 | 33 ms | 346 ms | 760 ms | 21 ms | 4 ms | 3,042 | 4.5% | 81% |
| SQLite Mode A (baseline — not a target) | single-call | 56.9 | 27 ms | 331 ms | 774 ms | — | — | 3,799 | 6.2% | 82% |

Reports: postgres
[two-step](20260807-postgres-twostep.md) /
[single-call](20260807-postgres-signandprocess.md) ·
aerospike-hybrid
[two-step](20260807-aerospike-hybrid-twostep.md) /
[single-call](20260807-aerospike-hybrid-signandprocess.md) ·
sqlite
[two-step](20260807-sqlite-twostep.md) /
[single-call](20260807-sqlite-signandprocess.md).

`op-failure rate = (contentionFails + deadlockFails + otherErrors) / attempted`,
i.e. ops that exhausted all retries or hit a non-retryable error. (E.g. the
Postgres two-step run: (310 + 0 + 56) / 17230 ≈ **2.1%**.)

## Fuel-pool path (ClaimExact) — Task 27

Same harness, `-throughput` (`PERF_THROUGHPUT=1`): the provider runs
`Strategy=throughput`, so each `wallet.CreateAction` funds via the funder's
closed-form `ClaimExact` over a denominated pool instead of the tiered claim.
Bounded **60 s** window (5 s warmup), 64 workers, 72 DB conns, **36 000-coin**
pool, unbounded rate. (The larger pool is required because throughput change is
*not* recycled — the pool drains ~1 coin/op — so it must outlast the window; a
harness measurement choice, see the [gap analysis](#gap-analysis--path-to-1000-tps).)

| Backend | Mode | Sustained TPS | e2e p50 | e2e p95 | e2e p99 | create p50 | sign p50 | claim-contention retries | op-failure rate | maturation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| PostgreSQL Mode A (shared SQL) | two-step | **183.5** | 172 ms | 1282 ms | 1982 ms | 85 ms | 73 ms | **0** | 0.56% | 50% |
| PostgreSQL Mode A (shared SQL) | single-call | **209.8** | 173 ms | 1146 ms | 1918 ms | — | — | **0** | 0.49% | 56% |
| Aerospike + PostgreSQL Mode B (split) | two-step | **200.6** | 157 ms | 1164 ms | 1988 ms | 81 ms | 67 ms | **0** | 0.52% | 50% |
| Aerospike + PostgreSQL Mode B (split) | single-call | **175.0** | 164 ms | 1338 ms | 2311 ms | — | — | **0** | 0.59% | 49% |

Reports: postgres
[two-step](20260807-postgres-twostep-throughput.md) /
[single-call](20260807-postgres-signandprocess-throughput.md) ·
aerospike-hybrid
[two-step](20260807-aerospike-hybrid-twostep-throughput.md) /
[single-call](20260807-aerospike-hybrid-signandprocess-throughput.md).

### Tiered vs fuel-pool, side by side (single-call)

| Path | Postgres TPS | Postgres op-fail | Postgres claim-retries | Hybrid TPS | Hybrid op-fail |
|---|---:|---:|---:|---:|---:|
| Tiered (pool 4000) | 211.2 | **18.2%** | **117,585** | 153.6 | 0.55% |
| Fuel-pool (pool 36000) | 209.8 | 0.49% | **0** | 175.0 | 0.59% |

- **The `ClaimExact` wiring works and eliminates claim contention — deterministically.**
  Every fuel-pool run (5+ captured, both backends, both modes) shows **exactly 0**
  claim-contention retries and ~0.5% op-failure (the residual is the one in-flight
  op per worker cancelled at shutdown). The tiered Postgres single-call path, by
  contrast, collapsed to **117k retries and 18% op-failure** at pool 4000. This is
  the structural win: the uniform denomination makes every pool coin interchangeable,
  so the `FOR UPDATE SKIP LOCKED` claim never collides — 64 workers grab 64
  different coins instead of fighting over the smallest-sufficient one.
- **1000 TPS is not reached; the real ceiling here is ~175–210 TPS single-call.**
  With claim contention removed, raw sustained TPS is *create-phase bound* and lands
  in the same ~150–230 band as the tiered path.
- **Equal-pool isolation (pool 8000, single-call, 15 s):** tiered **219.8** vs
  fuel-pool **230.4 TPS** (both `0` retries in that pair — tiered contention is
  high-variance and happened not to fire). At equal pool the two paths are within
  ~5%: on this box both are limited by the same create-phase DB work, so removing
  claim contention shifts *reliability* (no 18% collapse) far more than it shifts
  median TPS.
- **Hybrid (Mode B) gains most:** fuel-pool cuts the `create` p50 from **319 ms**
  (tiered) to **81 ms** — the single `ClaimExact` replaces the multi-tier
  best-fit walk across the Aerospike inventory — lifting two-step from 152.7 → 200.6
  TPS (+31%).

## Task 28 — optimized results (COUNT kill + honest re-measure)

### What changed

`CreateAction` used to call `changeBasketCount` on **every** op, which ran a
`SELECT COUNT(*) FROM outputs o JOIN transactions t ON … WHERE o.user_id=? AND
o.basket=?` — an unconditional count with a join, executed as its own round trip
*before* the DB transaction opens, whose cost scaled with the basket's row count
(and on the throughput path the funding pool lives in that basket, so it scaled
with pool size on every op). Task 28:

- **Throughput path: skip the count entirely** (return `0` = "do not clamp on
  basket fullness"). Funding comes from a fixed-denomination pool whose
  closed-form `ClaimExact` leaves ~no change, so clamping change fan-out on how
  full the change basket is is degenerate. This removes the dominant scalable
  per-op cost on the 1000-TPS design's route.
- **Tiered (privacy) path: keep the clamp, make the count cheap.** A new
  `OutputsRepo.CountInBasket` runs `SELECT COUNT(*) FROM outputs WHERE user_id=?
  AND basket=?` — a single indexed range count over `idx_outputs_user_basket`,
  **no join to `transactions`**. The value is provably identical (the join was on
  a `NOT NULL` FK into the unique `transactions` PK — exactly one match per row,
  no fan-out, no drop) so change-clamping is unchanged. Guarded by
  `metastore.TestCountInBasket_*` (value parity + query-plan: index used, no
  `transactions` access) and `storage.TestChangeBasketCount_*` (throughput skips,
  privacy still counts).

### Before/after — the `COUNT` delta

**SQLite (self-contained, single writer — the clean control), throughput
single-call, 8 workers, 20 s window.** Two samples each; the count was the only
change:

| Pool | Before (join count) | After (count skipped) | After vs before |
|---|---:|---:|---:|
| 2 000 | 98.0 (97.5, 98.4) | 108.5 (110.2, 106.7) | +10.7 % |
| 16 000 | 74.7 (73.0, 76.3) | 93.5 (92.4, 94.5) | **+25.2 %** |
| **pool 2 000 → 16 000 drop** | **−24 %** | **−14 %** | — |

Killing the count removes ~40 % of the pool-size sensitivity and lifts
large-pool TPS ~25 %. The residual −14 % is *not* the count (it is skipped); it
is other pool-scaling work (the `ClaimExact` scan, input-BEEF assembly, the
balance sampler), left as follow-up.

**Postgres (durable, same container before vs after), throughput single-call,
64 workers, pool 36 000, 72 conns, 60 s:** **370.2 → 393.8 TPS (+6.4 %)**. Modest
on Postgres because its indexed count is cheap and the durable commit dominates —
the win there is structural (no per-op join, no pool-size scaling), not headline
TPS.

> The committed pre-optimization numbers above (209.8 / 175.0) came from the
> less-tuned `testenv` Postgres; the honest `COUNT` delta is the **same-container**
> before/after (+6.4 % PG, +25 % SQLite-large-pool), not 209.8→393.8 (which also
> folds in Postgres tuning: `shared_buffers=2GB`, `max_connections=400`, and an
> idle box).

### Worker / connection-pool sweep — the ceiling (durable, after-fix)

Throughput single-call, `fsync=on`, `synchronous_commit=on`, 60 s window (5 s
warmup), unbounded rate. Pools sized so the 1M-denominated coins never underflow
(clean `ClaimExact`; verified ops < pool, flat buckets, ~0 retries).

| Backend | Workers | Pool | Conns | Sustained TPS | e2e p50 | e2e p95 | e2e p99 | op-fail |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Postgres Mode A | 64 | 36 000 | 72 | 393.8 | 150 ms | 255 ms | 417 ms | 0.25 % |
| Postgres Mode A | 128 | 60 000 | 144 | 473.9 | 254 ms | 414 ms | 602 ms | 0.42 % |
| Postgres Mode A | 256 | 72 000 | 272 | **575.7** | 405 ms | 753 ms | 989 ms | 0.68 % |
| Aerospike hybrid Mode B | 64 | 36 000 | 72 | 382.7 | 149 ms | 260 ms | 474 ms | 0.22 % |
| Aerospike hybrid Mode B | 128 | 60 000 | 144 | 489.7 | 228 ms | 530 ms | 736 ms | 0.40 % |
| Aerospike hybrid Mode B | 256 | 72 000 | 272 | **645.6** | 349 ms | 711 ms | 1407 ms | 0.61 % |

Reports (optimized set): postgres
[64w](20260807-postgres-signandprocess-throughput-optimized-64w.md) /
[128w](20260807-postgres-signandprocess-throughput-optimized-128w.md) /
[256w](20260807-postgres-signandprocess-throughput-optimized-256w.md) ·
aerospike-hybrid
[64w](20260807-aerospike-hybrid-signandprocess-throughput-optimized-64w.md) /
[128w](20260807-aerospike-hybrid-signandprocess-throughput-optimized-128w.md) /
[256w](20260807-aerospike-hybrid-signandprocess-throughput-optimized-256w.md).

Both backends scale **sub-linearly** (PG +20 %/+22 % per doubling; hybrid
+28 %/+32 %) while e2e latency grows ~linearly with worker count — the signature
of a saturated resource where extra workers buy queueing, not throughput. Claim
contention stays at 0 (the `ClaimExact` win holds); the bottleneck is the
create-phase **durable commit**. Hybrid edges past PG at high concurrency
because its Aerospike utxostore offloads the claim, leaving the Postgres
metastore less per-op work.

### Proof the wall is the durable commit

Same 64-worker config, only `synchronous_commit`/`fsync` flipped (clean window,
no underflow):

| Postgres 64w | Sustained TPS | e2e p50 | e2e p99 |
|---|---:|---:|---:|
| Durable (`fsync=on`, `synchronous_commit=on`) | 393.8 | 150 ms | 417 ms |
| Relaxed (`synchronous_commit=off`, `fsync=off`) | **1379.0** | 45 ms | 108 ms |

A 3.5× jump at the same worker count and workload, from relaxing durability
alone. Report: [64w relaxed durability](20260807-postgres-signandprocess-throughput-optimized-64w-relaxed-durability.md).

### Verdict & remaining gap to 1000 TPS

- **Strict per-op durability: ~575 TPS (Postgres) / ~646 TPS (hybrid) per node**,
  latency-bound on the durable ACID commit. 1000 TPS is **not** reached on one
  node in a sane-latency regime; pushing workers past 256 only inflates the tail
  (hybrid p99 already 1.4 s).
- **Relaxed durability reaches 1000+ TPS at 64 workers** (1379 TPS). So the gap
  to 1000 is squarely the per-`CreateAction` durable commit, exactly the
  "irreducible ACID commit floor" the task anticipated. Closing it without
  weakening durability means **batching/group-commit** (amortize `fsync` across
  ops) or **horizontal scale-out** (N nodes × ~575–645 TPS). `synchronous_commit=off`
  is a legitimate middle ground (a bounded last-few-ms durability window) that
  already clears 1000 on a single node.
- The claim is no longer a bottleneck (0 contention on every run) and the
  `COUNT(*)` no longer scales with pool size. The next create-phase costs to
  attack are the metadata persist (tx + output rows + input-BEEF assembly) and
  the monitor daemon sharing the connection pool.

## Reading the numbers

- **Single-call is NOT ~2× on the tiered path — it is backend-dependent, and
  here it barely moves.** Fewer wallet round trips do lower per-op latency
  (Postgres e2e p50 drops 250 ms → 97 ms), but throughput rises only ~4% on
  Postgres, ~0.6% on the hybrid, and single-call is actually **slower** on
  write-serialized SQLite (56.9 vs 62.2 TPS). The "~2×" is only a *plausible
  estimate* for a claim path that is not the bottleneck; on the tiered path the
  bottleneck moves elsewhere (below), so the round-trip saving is eaten.
- **Postgres Mode A is a claim-contention hotspot, and cutting latency makes it
  worse.** 64 workers claiming the smallest sufficient coin collide on the
  shared-SQL `SKIP LOCKED` scan. In single-call mode the faster op cycle drives
  contention ~11× higher (117k vs 10k retries) and the failure rate to **18%** —
  so the lower latency buys almost no extra throughput. A larger pool-to-worker
  ratio, fewer workers, or the fuel-pool exact-claim path all reduce this.
- **The Aerospike hybrid shows zero claim contention** (its approximate bucket
  selection sidesteps the `SKIP LOCKED` collision) but pays higher per-op
  latency because Mode B cannot share a transaction across the Aerospike
  inventory and the PostgreSQL metadata — two coordinated writes per op. It is
  latency-bound, so single-call barely helps.
- **The bottleneck phase is `create` (fund + reserve + persist)** on every
  backend and on **both** funding paths; `sign_process` is 3–6× cheaper at the
  median and its tail is far tighter (create p95 ≈ 1.1–1.3 s vs sign p95 ≈ 0.18 s
  on the fuel-pool runs). The cost is DB round trips in the funding/claim/persist
  path, not signing or the (instant, in-process) broadcast. On the fuel-pool path
  the *claim* part of `create` is no longer the culprit (0 contention) — it is the
  change-basket `COUNT(*)` + persist + input-BEEF assembly. See the gap analysis.
- **Contention counts are HIGH-VARIANCE.** Across near-identical configs, the
  Postgres claim-retry count has been observed anywhere from ~0 to >100k
  (scheduling-dependent `SKIP LOCKED` collisions). Do not over-read a single
  run's contention figure; the shape (Postgres contends, hybrid does not) is the
  durable signal, not the exact count.
- **`otherErrors` is the residual bucket** — write-path errors not matched as
  contention (contention/conflict/not-enough-funds/insufficient) or deadlock
  (deadlock/serialization/40001/40P01). It is ~56–64 in *every* run regardless
  of backend or load, i.e. ≈ one per worker: the single in-flight op each worker
  is executing when the run's context is cancelled at shutdown, plus rare
  transient BEEF-assembly/reference errors. It is **not** a storage-capacity
  signal (<0.6% of ops).
- **The async loop is genuinely exercised.** The monitor's SSE apply pipeline
  consumed MINED frames from the auto-miner and promoted **99–100%** of sent
  transactions to `completed` on the containerized backends (change matured
  unproven → mined) under load. Maturation also bounds latency: mined coins are
  trust anchors that truncate the BEEF ancestry walk. (SQLite sits lower, ~81%,
  because the single writer serializes the monitor's promotion writes behind the
  workers.) The auto-miner drives the status-SSE MINED stream + proof headers
  only; it does **not** advance the chaintracks tip stream.

## Gap analysis — path to 1000+ TPS

Task 27 wired the fuel-pool `ClaimExact` route and it works, but **neither path
reaches 1000 TPS on this box** — the ceiling is ~175–210 TPS single-call. With
claim contention removed, the bottleneck has moved off the claim and onto the
`create` phase. What now stands between here and 1000, roughly in order of
leverage:

1. **The `create` phase is the wall, and it is not the claim anymore.** In the
   two-step split, `create` (fund + reserve + persist) still dominates
   `sign_process`, and its *tail* is what caps throughput: create p95 ≈ 1.1–1.3 s
   and p99 ≈ 1.9 s even at create p50 ≈ 85 ms and **0 claim contention**. The cost
   is DB round trips — the change-basket `COUNT(*)` (see #2), the metadata persist
   (tx + output rows + input-BEEF assembly), and the monitor daemon competing for
   the same connection pool — not signing and not the (instant, in-process)
   broadcast. The task's prior guess that the next bottleneck would be
   sign/commit/broadcast does **not** hold: it is squarely the funding/persist
   create path.
2. **Give the fuel pool a dedicated basket (this is the biggest single lever; it
   was blocked when these runs were captured and is now available — see
   [Closed gap](#closed-gap-the-dedicated-signable-fuel-basket)).**
   `CreateAction` calls `changeBasketCount` — a `SELECT COUNT(*)` over the funding
   basket — on *every* op (`create.go`). Because the harness pool lives in the
   `default` basket, that COUNT scans the whole pool per op, and its cost grows
   with pool size (measured A/B on SQLite: pool 2000→16000 dropped 116.8→79.2 TPS
   at identical workload). A dedicated `fuel` basket keeps the pool out of that
   scan entirely. Independently, making `changeBasketCount` O(1) (a cached/maintained
   basket counter instead of a per-op `COUNT(*)`) removes the cost on both paths.
3. **More workers + connection-pool tuning.** Throughput is now latency/round-trip
   bound with contention headroom (0 retries), so it should scale with concurrency
   until Postgres saturates; the shared 72-connection pool (workers + monitor)
   and the create tail are the current limiters to profile next.
4. **Delayed-broadcast / batched processing** to cut per-op synchronous DB work
   and shorten the create tail.
5. **Denomination sizing** so one `ClaimExact` claims exactly one coin per payment
   (it already does here: `n=1`), keeping change minimal.

### Closed gap: the dedicated, signable fuel basket

**This gap is closed. The runs below predate the fix and size their pool in the
`default` basket; read their numbers with that in mind.**

When these benchmarks were captured, no public wallet API could put a
wallet-*signable* coin into a non-default basket: `Options.FuelShape` was dead in
storage, so `FanOutFuel` minted ordinary change into `default` rather than shaped
denominations into a pool basket.

Storage now reads `FuelShape` on the create path — it sizes the fan-out outputs
(`pkg/storage/create.go:97-102`), adds their value to the funding target
(`:113-115`), resolves the fan-out's source basket (`:134`, `:455-467`) and emits
them as shaped change into the pool or reserve basket (`:357-377`). Those coins
carry derivation material like ordinary change, so they are wallet-signable and
`ClaimExact`-selectable. **Mint into a dedicated `fuel` basket; do not size around
the old caveat.**

One related limitation is still real: `InternalizeAction`'s **wallet-payment**
protocol (the only one recording the BRC-29 derivation material the signer needs)
hardcodes the `default` basket, while its **basket-insertion** protocol can target
any basket but records no derivation material — the assembler's fallback
derivation uses an *empty* key ID, which `brc29` rejects (`KeyID.Validate`). So
basket-insertion coins remain unspendable; `FanOutFuel`, not `InternalizeAction`,
is how you provision a dedicated basket.

Because the runs below pooled in `default`, they pay the `changeBasketCount` cost
of #2 and force a large, non-recycling pool. They remain a faithful end-to-end
exercise of `ClaimExact` — treat their absolute numbers as a **floor** for the
denominated path.

## How these were produced

```sh
# Container-backed capture (podman); PERF_MODES runs both modes reusing the
# containers. Writes JSON to ./perf-results/:
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true \
PERF_MODES="twostep signandprocess" \
PERF_DURATION=60s PERF_WARMUP=15s PERF_WORKERS=64 PERF_POOL=4000 PERF_MAX_DB_CONNS=72 \
  go test -tags perf -run TestPerf_PostgresModeA -timeout 25m -v ./test/perf/...

# Fuel-pool (ClaimExact) capture: add PERF_THROUGHPUT=1 and a pool large enough
# to outlast the window (throughput change is not recycled — the pool drains
# ~1 coin/op, so pool > peak_TPS × (warmup+duration); underflow shows up as a
# claim-contention/not-enough-funds spike):
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true \
PERF_THROUGHPUT=1 PERF_MODES="twostep signandprocess" \
PERF_DURATION=60s PERF_WARMUP=5s PERF_WORKERS=64 PERF_POOL=36000 PERF_MAX_DB_CONNS=72 \
  go test -tags perf -run TestPerf_PostgresModeA -timeout 30m -v ./test/perf/...

# Or a full-length run via the CLI (SQLite self-contained; PG/Aero via flags).
# Add -throughput for the fuel-pool path:
go run ./cmd/perfrunner -backend postgres -pg-dsn "postgres://..." \
  -workers 64 -duration 5m -warmup 30s -pool-size 36000 -max-db-conns 72 \
  -mode signandprocess -throughput

# Render any result JSON to Markdown:
go run ./cmd/perfrunner render -o report.md perf-results/<run>.json
```

The CI floor (a short bounded run, two-step only by default, conservative TPS
assertion) lives in `test/perf` behind the `perf` build tag; the headline
numbers above come from the longer manual runs.
