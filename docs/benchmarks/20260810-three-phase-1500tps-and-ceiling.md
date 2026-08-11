# Three-phase scaling run: 500 → 1500 → ceiling hunt (2026-08-10, fresh env)

End-to-end blast of go-arcade-toolbox → arcade v0.11.3 → teranode/merkle-service v0.5.2
on scale-ovh (`dev-ovh-1`, ns `arcade-v2` + `merkle-service`), fresh chain (reset at
height ~305) and wiped local stores. App: `facts-app` @ toolbox-app-arcade `26cbf60`,
toolbox `main@9a67085` (SSE apply sharding, MINED prune-by-outpoint `e225d6f`, verify
memoization `bca3514` all in). Aerospike-hybrid backend, `FUEL_DENOM_SATS=600`,
immediate broadcast, buyer-only. Blocks mined on demand every ~60s during blasts via
scale-1 RPC `generate`. Raw artifacts (CSVs, pprof profiles, timeline, logs) in the
session scratchpad `53a1326d-*/scratchpad`.

## TL;DR

- **Phase A (500 TPS × 3 min): clean.** Median 498/s, 0 failed / 0 rejected / 0
  backpressure, p50 125ms, p95 165ms. Conversion **99.995%** (91,557/91,562 mined).
- **Phase B (1500 TPS × 5 min): create averaged 1,360/s** (oscillating 1,009–1,670),
  0 failed / 0 backpressure, 4 rejected (0.001%). Conversion **99.998%** — but ~38% of
  MINED events were dropped from live SSE and took ~45 min of poll recovery to settle.
- **Phase C (ceiling): the system intake ceiling is ~1,600–1,900 TPS, and it is
  arcade-side (propagation), not the toolbox.** Peak sustained create 1,876/s (step
  2250). The old "toolbox tops out ~1.6–1.7k signing-bound" figure is obsolete: at the
  plateau the toolbox ran ~9 of 32 cores with 0 errors; throughput was capped by arcade
  POST /tx latency (closed-loop: 512 workers ÷ ~280ms RTT ≈ 1,830/s) while the
  single-active propagation pod pegged its 2-core limit and arcade accumulated a
  **560k-tx RECEIVED backlog**. Distinct from intake, the **settled** ceiling is
  teranode's 50k/block assembly cap × block frequency (~833/s at a 60s mine cadence):
  the backlog drained in exact 50k block-quantized steps with propagation idle.
- **The old P1 "tens of thousands never mined" gap did NOT reproduce**: teranode
  eventually included everything offered (all but 12 txs of ~1.63M across the day:
  7 REJECTED, ~5 stuck RECEIVED from Phase A/B tails).
- **Biggest live gap: arcade SSE MINED-burst delivery.** Blocks ≤ ~8.5k txs deliver
  100% of MINED events; blocks ≥ 20k deliver only 44–73% (buffer overflow). Run-wide
  live MINED delivery was ~57–62%; the toolbox poll fallback recovers the rest at only
  ~90/s.

## Setup

| item | value |
|---|---|
| arcade | v0.11.3 (#277 SSE buffer, #280–#285 reorg fixes) |
| merkle-service | v0.5.2 |
| propagation tuning | `merkle_concurrency=50 max_concurrent_batches=8 teranode_max_batch_size=25` (ConfigMap, survived redeploy) |
| toolbox app env | `HIGH_THROUGHPUT=1 IMMEDIATE_BROADCAST=true UTXO_BACKEND=aerospike-hybrid MAX_DB_CONNS=180 FUEL_DENOM_SATS=600 FUEL_TARGET_POOL=1500000 FUEL_MINT_CONCURRENCY=64 FUEL_STREAM_LEAF_CAP=2000 FUEL_STREAM_NO_YIELD=1 FEE_SAT_PER_KB=125 BLAST_AMOUNT_SATS=50` (`FUEL_RECYCLE_BASKET=off` for A/B; direct-recycle `FUEL_RECYCLE_COUNT=8` for C) |
| funding | 2 × 10 BSV mined EF txs (`3cda450d…`, `c1bf316d…`), imported via `POST /api/funding/import` |

## Phase A — 500 TPS × 180s (13:28–13:31Z)

- created 86,509 blast txs (+~5k keeper mints): per-5s median **498/s** (p25 492 / p75
  506), 0 failed, 0 backpressure, 0 rejected; create p50 125ms / p95 165ms.
- Arcade mined **91,557 of 91,562** known txs into blocks 310–312. Residue: 5 stuck
  RECEIVED on arcade (never propagated — P2 class, 0.005%).

Sampled tx lifecycle (`b31b6350…`, via pkg/txtrace):

| hop | Δ from create |
|---|---|
| POST /tx | +4ms |
| 202 RECEIVED | +118ms |
| ACCEPTED_BY_NETWORK (SSE, applied +13ms) | +256ms |
| SEEN_ON_NETWORK (SSE, applied +19ms) | +5.5s |
| MINED on arcade (block 311) | +77s |
| MINED SSE event | never delivered (dropped); recovered by poll |

Block→MINED (mine RPC call → toolbox):

| block | txs | mine→announce | mine→BUMP built | mine→first MINED recv | delivery window |
|---|---|---|---|---|---|
| 310 | 35,211 | +18.3s | +51.1s | +52.2s | 8.1s (19,052 live) |
| 311 | 40,005 | +6.1s | +23.5s | +24.9s | 3.9s (22,584 live) |
| 312 | 16,341 | +5.3s | +16.7s | +18.9s | 1.3s (11,623 live) |

MINED apply itself (post-`e225d6f`): 512-batches in ~60–200ms after warmup (first
batches ~1.5s) — the toolbox apply is off the critical path; the gate is delivery.

## Phase B — 1500 TPS × 300s (13:43–13:48Z)

- **408,152 created in 300s = 1,360/s avg** (5s-window median 1,448, p25 1,009, p75
  1,670, max 2,263). 0 failed, 0 backpressure. 4 REJECTED (teranode
  `ProcessTransaction (4): failed to validate`, no competing txs; 0.001%). 7 stuck
  RECEIVED (0.002%).
- **Create dips correlate with MINED-burst applies**: throughput cratered to 51/154/
  238/61 per-sec exactly in the windows where each block's MINED events were being
  applied (13:45:09, 13:46:21, 13:47:27, 13:48:41). Toolbox CPU was only ~8.5/32 cores
  (GC ~30%, ECDSA ~11%) → local **store contention** (apply vs create on aero+PG),
  not CPU.
- p95 spiked to 664ms and 2,439ms near mines; p50 held ~135ms otherwise.

Inclusion (arcade `transactions` per block):

| block | mine→BUMP | txs |
|---|---|---|
| 316 | +28.9s | 47,131 |
| 317 | +28.3s | 68,439 |
| 318 | +24.0s | 51,437 |
| 319 | +26.8s | **50,000 (exact)** |
| 320 | +25.8s | **50,000 (exact)** |
| 321 (drain mine) | +25.4s | 33,000 |
| 322 (timer block) | +21.8s | 108,145 |

- During the blast, inclusion ran ~700–970/s vs 1,360/s offered → ~141k backlog, fully
  swept within ~3 min of blast end. Exact-50,000 blocks = teranode block-assembly cap
  signature (seen again in Phase C: 339, 340, 344).
- Block→MINED-emit held steady ~25–30s per 50–68k-tx block; merkle-service kept pace at
  60s cadence (subtree-workers ~5–15 cores in bursts). No runaway, no crash-loops.
- **Live MINED delivery 61.5%** (250,983 of 408,153); the rest recovered by the 60s
  poll at ~90/s ≈ **45 minutes to converge**. Final conversion 99.998%.

## Phase C — ceiling steps (15:50–16:06Z, direct-recycle on)

| step (tps) | workers | created in 120s | % of target | avg/s | failed | bp |
|---|---|---|---|---|---|---|
| 1750 | 350 | 191,466 | 91% | 1,596 | 0 | 0 |
| 2000 | 400 | 192,614 | 80% | 1,605 | 0 | 0 |
| 2250 | 450 | 225,110 | 83% | **1,876** | 0 | 0 |
| 2500 | 500 | 216,570 | 72% | 1,805 | 0 | 0 |
| 2750 | 512 | 203,384 | 62% → stop | 1,695 | 0 | 0 |

- **Ceiling verdict: ~1,600–1,900 TPS sustained intake, bounded by arcade, not the
  toolbox.** Evidence:
  - propagation (single-active, `arcade.propagation` pinned to 1 partition) pegged at
    **2,001m = its 2-core limit** through C (1,968m in B);
  - arcade accumulated **~562k txs at status RECEIVED** by the end of C (arcade had
    ZERO rows at SEEN — everything validated was mined; the backlog is pure
    RECEIVED→propagation);
  - POST /tx latency rose with each step (p50 130→305ms, p95 spikes 2.6s): the
    closed-loop blaster caps at workers ÷ RTT (512 ÷ ~0.28s ≈ 1,830/s — the observed
    plateau);
  - the toolbox ran ~9/32 cores at the plateau (pprof: ECDSA ~19% flat, GC ~25%, no
    lock contention in top) with 0 failed / 0 backpressure — it followed arcade's
    latency rather than saturating. **The old ~1.6–1.7k "signing-bound toolbox ceiling"
    is obsolete.**
- 3 more REJECTED (7 run-total, 0.0004%).
- Fuel: direct-recycle sustained the pool (1.48M → ~900k floor, keeper recycling change
  at ~2.5k leaves/s when engaged).

### Post-blast: the backlog drain separates the two ceilings

Once the blast stopped, propagation went **idle (~28m CPU)** while the 562k RECEIVED
backlog drained in **exact ~50,000-tx block-quantized steps** (561,995 → 511,995 →
464,457 …), one step per mined block. So the backlogged txs were already in teranode's
mempool; what remained was purely block assembly. Two distinct ceilings:

1. **Intake ceiling (~1,600–1,900 TPS)** — binds *during* the blast: propagation pegged
   at its 2-core limit, POST /tx RTT rises, closed-loop create caps at workers ÷ RTT.
2. **Settlement ceiling (= 50,000 × block frequency)** — binds *after* intake: at a
   60s mine cadence that is ~833 tx/s settled; at the ~7-min natural cadence, ~120/s.

Also note: backlogged txs **skip SEEN entirely** (arcade jumps them RECEIVED→MINED when
a block sweeps them). Any consumer gating on SEEN (e.g. promote-on-SEEN change
management) sees those coins as unpropagated right up until they are MINED.

## Findings (ranked)

1. **arcade SSE MINED-burst overflow — the dominant live-status gap.** bump-builder
   publishes a block's whole MINED set at once (e.g. 40,005 events in 41 Kafka chunks
   within ~1ms); each SSE connection has an 8,192-slot buffer drained at ~2.4–6k
   events/s; everything beyond silently drops. Measured: blocks ≤8.5k txs → **100%**
   delivered (block 334); blocks ≥20k → **44–73%**. Run-wide live MINED delivery
   ~57–62%. No `slow_client` log lines on any sse pod (drop accounting invisible at
   info level). Fix directions: pace/chunk the MINED unfan to consumer drain rate,
   block-or-catchup instead of silent drop, or mid-stream Last-Event-ID replay.
2. **arcade propagation single-active is the intake ceiling (~1,600–1,900 TPS), and
   past it the backlog enters a claim-revocation requeue spiral.** Root-caused
   end-to-end (corrected after code exploration — the canceler is the **Kafka
   consumer-group claim**, NOT the k8s reaper lease): the active pod is CPU-throttled
   at its 2-core limit; the dispatcher drains pending in macro-batches capped by
   `propagation.max_pending` (default **50,000**, `config.go:420`); a throttled 50k
   cycle overruns Sarama's session/poll window → the broker revokes the partition →
   `claim.Context()` cancels and propagates into the DETACHED batch goroutine
   (propagator.go:696-705 → dispatcher.go:258) mid-`/watch` registration (log:
   `merkle-service /watch partial/all failure … context canceled`, 39,189/53,269
   failed) → the failed subset requeues under the same dead ctx and retries derive
   born-canceled contexts (`accepted=0, success_endpoints=[]` within 12ms) → the
   backlog crawls at ~50k per fresh claim. **merkle-service is exonerated**: `/watch`
   served every call at 3–50ms / HTTP 200 throughout. Stuck txs are 404 on teranode
   (never delivered) and skip SEEN when finally swept. Nearly all 466k stuck txs were
   created after 15:59Z — steps 2500/2750, offered load beyond the intake ceiling.
   **Three-layer recovery observed:** (1) small cycles complete (`requeued=0`); (2) a
   pod roll's rebalance replays whatever Kafka can still re-read (466k→178k); (3) the
   residue that lost Kafka replayability drains ONLY via the reaper's durable
   rebroadcast, hardcoded 200 txs/tick (~6.7/s at 30s ticks — hours). Tuning + fixes:
   see "Propagation tuning" below.
3. **teranode block assembly caps ~50,000 txs/block** (5+ exact-50,000 blocks across
   B/C, and the post-blast backlog drained in exact 50k steps per block with
   propagation idle) → settled throughput ≡ 50,000 × block frequency (~833/s at 60s
   cadence, ~120/s at the natural ~7-min timer). At sustained >800/s *settled*, either
   the cap or the cadence must rise. Corollary: overload txs skip SEEN entirely
   (RECEIVED→MINED), which breaks SEEN-gated consumers.
4. **Toolbox poll fallback is far too slow as the drop backstop: ~90/s.** 157k dropped
   MINED (Phase B) took ~45 min to reconcile; Phase C's ~243k will take ~45–90 min.
   Toolbox fix: raise poll batch/concurrency and add backoff+alerting; ideally arcade
   fixes delivery so the poll is a true fallback.
5. **Toolbox MINED-apply bursts starve create** (dips to 51–238/s during block-apply
   windows at 1500 TPS; store contention on aero+PG, CPU idle). Candidate fix:
   rate-limit/yield the apply pipeline while a blast is active, or isolate apply I/O
   (separate aero connection pool / PG session budget).
6. **Keeper cannot rebuild fuel from a fragmented basket with recycle off.** After A+B
   consumed the pool, 331k × 525-sat change coins sat in `default`; the chunk fan-out
   path failed every round (`create action failed` aggregating thousands of tiny
   inputs) until direct-recycle (`FUEL_RECYCLE_COUNT=8`) was enabled — which then
   rebuilt 1.48M leaves in ~10 min. Also: the `/api/status` `fuel_pool` gauge goes
   stale when the keeper idles (showed 499,500 while the real pool was 5,039).
7. **merkle-service subtree-fetchers burn ~12 cores at idle** ingesting 2^20-txid
   subtree announcements from *other* scale nodes (scale-2 asset-cache observed) —
   wasted work for an arcade bound to scale-1.
8. **bump-builder logs the full txid array per "transactions mined" chunk line** —
   megabytes of log per block (41 lines × 40k txids each for block 311).
9. Minor: teranode `lastblocks` reports garbage `transactionCount`/`size` on this
   chain; count inclusion via arcade PG instead. Blocks with zero tracked txs get
   `bump_built_at=NULL` + a zero-STUMP warning (benign, but indistinguishable at a
   glance from the merkle#208 dropped-callback failure mode).

## Propagation tuning (for the arcade operator)

ConfigMap (`arcade-v2-config`, under `propagation:`) + deployment changes, ranked:

1. **`max_pending: 5000`** (from default 50000) — the macro-cycle must complete within
   the lease. At ~1.5–3k registrations+broadcasts/s per cycle observed, 5k finishes in
   seconds; 50k on a throttled pod cannot finish in 90s.
2. **Raise the propagation CPU limit to 4 (request 2)** — the pod sat pinned at
   exactly 2,001m during intake; throttling is what stretches cycles past the lease.
3. **`lease_ttl_ms: 300000`** (5 min) as a belt-and-braces guard so a slow cycle
   degrades gracefully instead of cancel-spiraling.
4. Keep `merkle_concurrency: 50`, `max_concurrent_batches: 8`,
   `teranode_max_batch_size: 25` (validated); with more CPU, `max_concurrent_batches:
   16` is worth an A/B.
5. **More propagation replicas do NOT help** — the topic is pinned to 1 partition
   (single-active by design); replicas are failover only. Real scale-out past ~2k TPS
   intake needs partition-safe sharding of `arcade.propagation`.
6. `lease_ttl_ms` correction: it tunes only the reaper's k8s leader lease, not the
   batch canceler (that's the Kafka claim) — harmless, not load-bearing.
7. Post-incident drain: residue that lost Kafka replayability drains only via the
   reaper backstop (200/tick hardcoded). Interim: `reaper_interval_ms: 2000` (argocd
   #204, ~13× faster incl. ~2s scan cost/tick). Proper: `reaper_rebroadcast_batch`
   knob (arcade #289) → set ~2000 and relax the interval.

**Arcade code fixes shipped from this run's findings (all PR'd 2026-08-10):**
- **#287** `fix(propagation)`: fail fast on revoked claim (no dead-ctx register/
  broadcast/requeue; one Info + counter; offsets left for replay) + inflight-depth
  gauge + periodic reaper backlog-depth log. (Merged.)
- **#288** `fix(sse)`: mid-stream catchup on per-conn overflow (per-client delivered
  watermark + drop flag → store-backed replay; aggregated drop logging; also fixes
  bulk MINED event ids to carry the store transition timestamp — makes Last-Event-ID
  resume exact instead of silently lossy).
- **#286** `chore(bump-builder)`: one bounded INFO per mined block, full txid chunks
  at Debug.
- **#289** `feat(propagation)`: `reaper_rebroadcast_batch` config knob (default 200).

## pprof (toolbox)

- Phase A (~5 cores): GC ~25–30% (allocation-heavy; `Transaction.ShallowClone` 8.3%
  cum), ECDSA field ops ~11%, syscalls ~6%.
- Phase B (~8.5 cores) and C steps (~9 cores): same shape scaled; ECDSA ~19% flat at
  2250/2750; no mutex/lock stalls in the top; nothing new binding. The toolbox has
  ~3.5× CPU headroom at the system ceiling. Micro-opt candidates (not binding):
  allocation reduction around ShallowClone / per-tx buffers to cut GC share.

## Conversion accounting (settled state)

- Phase A: 99.995% mined (5 RECEIVED-stuck).
- Phase B: 99.998% mined (6 RECEIVED-stuck, 4 REJECTED).
- Phase C: inclusion still draining at report time through the RECEIVED backlog +
  timer blocks (blocks 333–346 mined 571k so far incl. more exact-50k blocks); local
  convergence gated by the ~90/s poll on the dropped MINED share.
- Run total ~1.63M txs, 7 REJECTED (0.0004%), ~11 RECEIVED-stuck — **no mass-REJECT
  event and no never-mined tail like the 2026-08-09 run.**

## Repro / operational notes

- Funding import needs the EF tx *mined first* (`merkle_proof` 404 otherwise) — mine 1
  block and retry.
- `pkill -f 'facts-app serve'` from an agent shell kills the agent's own wrapper too —
  use `pgrep -x facts-app`.
- kubectl port-forward to the services PG is flaky; `kubectl exec` psql in the CNPG
  primary (`postgres-dev-ovh-1-services-cluster-5`) is reliable.
- The natural miner fires every ~5–10 min regardless — attribute blocks by hash
  (`blocks.csv` has the mapping).
