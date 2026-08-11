# Post-fix validation + the two single-partition ceilings (2026-08-10, PM)

Follow-up to `20260810-three-phase-1500tps-and-ceiling.md`, after the day's arcade fixes
shipped (v0.11.4 = #286/#287/#288/#289; propagation CPU 4→8; reaper/census tuning). Goal
was a sustained 2k-TPS/30-min metrics run to show the fixes improved results. What we
actually found: **the toolbox is not the bottleneck at any rate tried; two independent
arcade/merkle single-partition Kafka topics cap the pipeline**, and a clean sustained
run lands at **~1000 TPS settled** with arcade RECEIVED held flat.

## TL;DR

- **2000 TPS: RECEIVED grows unbounded.** Arcade's `propagation` topic is one partition,
  single-active; even at an 8-core limit the leader oscillates 2–7.7 cores (never
  pegged) and sustains only ~1.5–1.8k/s. More CPU doesn't help — the serial
  single-partition dispatch is the wall.
- **1500 TPS: still backs up** (RECEIVED→127k, ACCEPTED→70k) — worse than the AM run
  because the cluster was loaded/degraded after a day of heavy testing.
- **1000 TPS: clean and sustained.** Over 30 min: **arcade RECEIVED stayed flat —
  median 119, p95 322, max 552** (vs 1.5k→127k, 2k→75k+). 1.49M created, 5 failed, 0
  backpressure. Propagation peaked 5.5 of 8 cores (headroom).
- **The remaining latency limit is ACCEPTED→SEEN, and it's the *merkle-service* callback
  topic** — also one partition, serial, and shared with heavy STUMP/BLOCK_PROCESSED
  callbacks. SEEN latency was median ~3.2s but spiked to p95 ~12s / worst ~55s exactly
  during block processing (STUMP floods head-of-line-block the small SEEN callbacks).

## The two structural single-partition ceilings

Both are the same anti-pattern: a Kafka topic pinned to 1 partition for ordering
correctness, delivered serially, and **shared between light latency-sensitive traffic
and heavy bulk traffic** — so bulk traffic head-of-line-blocks the latency-sensitive
kind. More CPU/replicas cannot fix a serial single-partition consumer.

1. **arcade `propagation` topic → intake ceiling ~1.8k/s.** Single-active leader
   (`services/propagation`). At 2k the leader ran 2–7.7 cores (8-core limit, never
   pegged) and RECEIVED grew. Fix = shard the topic (partition by a correctness-safe key
   preserving parent-before-child), an arcade design change.

2. **merkle-service `callback` topic → ACCEPTED→SEEN latency spikes.** `callback.DeliveryService`
   (`internal/callback/delivery.go`) consumes ONE partition serially and it carries both
   the small `SEEN_ON_NETWORK`/`SEEN_MULTIPLE_NODES` callbacks *and* the ~545 KB
   STUMP/BLOCK_PROCESSED block-processing callbacks (`internal/block/subtree_worker.go`).
   When a block processes, STUMP messages flood the partition and the SEEN callbacks
   queue behind them — arcade keeps reaching ACCEPTED (synchronous from teranode's submit
   response) while SEEN stalls. The topic is hard-fixed at 1 partition
   ("until a cross-partition BLOCK_PROCESSED barrier exists"). Fix = decouple SEEN
   callbacks from block-processing callbacks (separate topic, or the deferred
   partition-widening), a merkle-service change.

ACCEPTED_BY_NETWORK is set synchronously from teranode's `POST /tx(s)` response
(`services/propagation/propagator.go`), which is why ACCEPTED never stalls — only the
callback-driven SEEN transition does.

## 1000-TPS run — metrics (30 min, sparse mining ~every 4 min)

| metric | value |
|---|---|
| created | 1,488,259 (~827/s avg, med 880, dips to 0 during MINED-apply bursts) |
| failed / backpressure | **5 / 0** |
| **arcade RECEIVED** | **med 119, p75 174, p95 322, max 552 — FLAT** |
| arcade ACCEPTED backlog | med 2,691, p95 10,420, max 48,396 (spikes during block bursts) |
| broadcast → 202 RECEIVED | ~113 ms |
| RECEIVED → ACCEPTED | ~94 ms |
| **ACCEPTED → SEEN** | **med ~3.2s, p75 4.3s, p95 11.8s, worst ~55s (during STUMP floods)** |
| propagation CPU peak | 5,555m of 8,000m (headroom — 1000 is below the ceiling) |
| conversion (final) | ~97% reached SEEN/MINED; ~1.3% no-status, ~1.3% ACCEPTED-stuck (callback residue), 0.17% REJECTED |

REJECTED dropped from 22,853 (the backed-up 1.5k run — direct-recycle payment chains
outran teranode's mempool ancestor limit while nothing confirmed) to **2,922 (~0.17%)**
at 1000, because under the ceiling parents confirm fast and the recycle chains stay
shallow.

## Toolbox assessment — not the bottleneck

- Create held target minus MINED-burst dips (1,050 `mined_batch` apply events over the
  run; each big MINED batch briefly monopolizes the apply pipeline and pauses create via
  local store contention — the next toolbox-side lever if tighter is wanted).
- SSE apply kept pace at ~1000/s with a bounded in-flight buffer (~78k mid-run, inflated
  by timer-block MINED bursts, held steady — not runaway). It drains post-blast.
- `#288` SSE mid-stream catchup is deployed and working; the residual no-status is apply
  buffer + merkle-callback SEEN lag, not SSE drops.

## Environment notes / debris

- The arcade `transactions` table was truncated mid-day (1.89M→0) to remove per-tx
  status-write drag; this orphaned the in-flight txs of an aborted run in the toolbox
  ledger (arcade returned not-found → permanent local no-status). A fresh local wipe +
  re-fund before the 1000 run cleaned this so the dashboard reads true.
- merkle subtree-fetchers idle at ~7.6 cores (continuously ingesting other scale nodes'
  1M-txid subtree announcements) — baseline churn, not our load.

## The 23,745 stranded transactions — root cause and fix (the important part)

The 1000-TPS run left **23,745 local txs with an empty `arcade_status`, and the count
was frozen**. This was NOT a throughput artifact and NOT txs stuck in RECEIVED:
**40/40 sampled returned `MINED` from arcade with valid merkle proofs** (2,242-char
path at height 392, chaintracks synced at 401). The local ledger simply diverged from
the source of truth and could not self-heal.

Four defects compounded — **three of them were features that existed, were tested, and
were documented, but were never actually wired**:

1. **Apply concurrency was dead code.** `applyShards = 8` was a hardcoded const;
   `WithApplyConcurrency` / `APPLY_CONCURRENCY` was stored and never read, despite its
   own doc saying ~1000 TPS needs more than 8. That capped the apply pipeline, saturated
   the 16384-slot queue, blocked the SSE reader, and made us the slow client arcade drops.
2. **The callback token was never derived.** `wdk.DeriveArcadeCallbackToken` shipped with
   unit tests, and `defs.Arcade` documented "when empty it is derived from the wallet
   identity key at wiring time" — nothing called it. So we connected **tokenless**, and
   arcade only offers mid-stream catchup (the #288 fix shipped the same day) to
   token-scoped clients: `catchup_eligible=false`. Arcade logged **136 lines of
   "dropped events for slow SSE client", ~25,000 events/s**, unrecoverable.
   Tokenless also means receiving every event on the instance — most of the drop pressure.
3. **The SSE cursor advanced past failed batches**, so nothing was redelivered.
4. **The poll backstop could not reach stranded rows**: it never predicated on
   `arcade_status`, and records it failed to apply were dropped with **no write at all**,
   so their `updated_at` never moved and they pinned the head of the
   `ORDER BY updated_at ASC LIMIT n` queue forever. The rest of the backlog was never
   even SELECTed. One 4,000-row batch was additionally lost to a Postgres deadlock
   (unordered bulk UPDATE vs 8 concurrent SSE shards) that logged WARN and `return nil`.

**Fixes** (toolbox PR galt-tr/go-arcade-toolbox#4, app commit `b7d54db`): wire the apply
knob; derive the callback token; hold the SSE cursor at the last fully-applied event; add
a `FindMissingArcadeStatus` repair query plus a dedicated `last_polled_at` column and
`ORDER BY last_polled_at ASC` so **a row can never be re-selected forever without the
poll making progress past it**; bounded retry then per-record fallback then a loud ERROR;
deterministic lock ordering (`ORDER BY txid FOR UPDATE` sub-select — sorting the bind
list alone does not fix it, Postgres still picks its own lock order).

**Validated against the real stranded data**, not just unit tests: on the first repair
tick `(none)` went **20,426 → 16,427 (−3,999 = exactly one batch)**, and polled rows now
carry `MINED`/`completed` matching arcade. Pre-fix that number sat frozen for hours.

Known limitation: the repair rides the 10-minute `un_fail` task at 4,000/tick (~6.7/s),
so a large backlog converges slowly (16k ≈ 40 min). Correctness (eventual convergence)
is guaranteed; **cadence should scale with `arcade_status_missing_total`** if fast
convergence after an incident matters.

## Ranked follow-ups

1. **merkle-service: decouple SEEN callbacks from STUMP/BLOCK_PROCESSED** (separate topic
   or widen the callback partition). This is the ACCEPTED→SEEN latency fix and the
   highest-value remaining item for "status maps to SEEN quickly."
2. **arcade: shard the `propagation` topic** for sustained intake >1.8k (correctness-safe
   partition key).
3. **toolbox: keep create flat during MINED-apply bursts** (rate-limit/yield the apply
   pipeline or isolate its store I/O) so create doesn't dip on big blocks.
4. Deploy the queued fixes: arcade #290/#291 (reaper census) → v0.11.5, then argocd #207
   (reaper batch back to 2000).
