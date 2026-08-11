# Transition timing, arcade lag, zero rejections, block→MINED (2026-08-11)

A measured 1000-TPS run instrumented with per-hop `pkg/txtrace` sampling
(`TXTRACE_SAMPLE_EVERY=500`), designed to answer four questions that no previous run could
answer because **per-status timestamps are persisted nowhere** — `known_txs` keeps one
`arcade_status` + one `updated_at`, overwritten on every transition, and arcade's
`transactions` table has the same shape.

The headline result is a **clean pass on rejections and proofs** and one **new, precisely
located root cause** for the status lag that has been blamed on the toolbox for three runs:

> **arcade's SSE fan-out runs on a single goroutine and issues one synchronous Postgres
> query per event per token-scoped client** (`services/sse/manager.go:212` →
> `store/postgres/postgres.go:1363`). Measured at 0.583 ms/probe against a 4,522,695-row
> `submissions` table, that caps live SSE delivery at **~1,500–1,700 events/s**. At 1000 TPS
> the pipeline generates ~4,000 status events/s. Arcade then logs the shortfall as
> **"dropped events for slow SSE client"** — but the client was idle (6 of 32 cores, apply
> batches averaging 15 of a 512 cap, a 16,384-slot queue that never filled). **The name is
> wrong: the slow party is arcade's fan-out, not the toolbox.**

## Run log and an honest disclosure

The measured window was interrupted. Mid-run instructions arrived to ramp to 3000 TPS, then
a retraction. Four blast segments resulted. **All are reported separately; nothing is
blended.**

| segment | window (UTC) | requested | workers | **true rate** (from `succeeded`/wall-clock) |
|---|---|---|---|---|
| **P1 — the clean measurement** | 12:50:01.417 → 12:57:36.149 (455 s) | 1000 | 200 | **1,069/s** (957/s blast + ~112/s fuel mints) |
| GAP-a — off-plan, excluded | 12:58:09.249 → 13:03:28.927 (314 s) | 3000 | 200 | 1,182/s (worker-bound, never reached 3000) |
| GAP-b — off-plan, excluded | 13:03:40.933 → 13:07:15.550 (197 s) | 3000 | 700 | **1,891/s** |
| P1b — 1000 TPS, contaminated | 13:07:27.603 → 13:11:07.912 (220 s) | 1000 | 200 | 894/s |

**P1 is the only segment comparable to the 2026-08-10 1000-TPS baseline.** P1b ran on top of
a 105k arcade RECEIVED backlog and a 500k local apply backlog left by GAP-b, so its
distributions are recovery data, not steady-state. The run was stopped early at 13:11:07.912Z
at the user's request. Settle and convergence were then observed to completion.

Environment re-verified live before starting (not trusted from git): callback-delivery
`limits.memory 4Gi`, `GOMEMLIMIT=3GiB`, `timeoutSec: 120`, `deliveryWorkers: 64`, **0
restarts**; `callback-dlq` HWM **82**; `callback-seen` 12 partitions / `callback` 1 partition;
arcade v0.11.6, merkle v0.5.4; chain tip 374 on scale-1.

**One prep defect found and fixed before the run:** the wallet had been funded while the app
ran *without* `HIGH_THROUGHPUT=1`. `AeroHybrid` is only wired when
`cfg.UseThroughput() && name == "buyer"` (`cmd/facts-app/main.go:170`), so the funding UTXO
existed in Postgres but **not in Aerospike** (`test:utxos` = 0 objects) and the throughput
funder reported `not enough funds` forever. Local stores were re-truncated and the EF
re-imported under the throughput profile. **Fund the wallet with the same profile you will
run it with**, or the UTXO is invisible to the funder.

---

## Q1 — Transition timing end-to-end

881 fully-traced lifecycles in P1; **100% reached MINED** (881/881 received *and* applied for
every stage). All 881 broadcasts returned **HTTP 202 / `RECEIVED`**; zero broadcast errors.

### P1 per-stage latency (ms)

| stage | source | p50 | p95 | max |
|---|---|---|---|---|
| create → POST /tx | toolbox | **9.3** | 57.5 | 288 |
| POST → 202 RECEIVED | toolbox↔arcade | **116.5** | 132.1 | 1,228 |
| create total (incl. sign+persist) | toolbox | **134.7** | 209.7 | 1,308 |
| create → arcade ACCEPTED_BY_NETWORK | arcade ts | **154.1** | 389.2 | 4,138 |
| arcade ACCEPTED → SEEN_ON_NETWORK | arcade ts | **8,513** | 18,989 | 32,150 |
| arcade SEEN_ON → SEEN_MULTIPLE_NODES | arcade ts | **23,671** | 35,842 | 46,171 |
| arcade SEEN_MULTIPLE → MINED | arcade ts | **298,231** | 505,928 | 527,329 |
| **create → MINED applied locally** | end-to-end | **502,615** (8.4 min) | 649,367 | 714,611 |

`SEEN_MULTIPLE → MINED` p50 of ~5 min is the natural block cadence, not a defect.

### The headline metric — arcade emit → toolbox applied

| status | SSE delivery (`recv−arcade_ts`) p50 / p95 | apply lag (`applied−recv`) p50 / p95 | **arcade emit → applied** p50 / p95 |
|---|---|---|---|
| ACCEPTED_BY_NETWORK | **86.6 / 21,610** | 28.8 / 547 | **215 / 21,665** |
| SEEN_ON_NETWORK | **327 / 21,433** | 29.5 / 483 | **420 / 21,496** |
| SEEN_MULTIPLE_NODES | **1,291 / 15,542** | 34.6 / 787 | **1,442 / 15,591** |
| MINED | 105,399 / 346,573 | 2,107 / 19,360 | 110,356 / 353,936 |

**The toolbox's own apply step is fast and stable: p50 29–35 ms, p95 under 800 ms, for every
status.** Essentially all of the emit→applied time is transport (`recv − arcade_ts`), i.e.
arcade-side.

The p50/p95 spread (86 ms → 21.6 s, a 250× ratio) is **entirely block-burst head-of-line
blocking**, and it splits cleanly by 5-minute bucket:

| stage | bucket 0 (12:50–12:55, contains block 376) | bucket 1 (12:55–12:57) |
|---|---|---|
| `sse_delivery_ACCEPTED` p50 / p95 | 104.6 / **26,777** | 77.4 / **2,269** |
| `arcade_emit_to_applied_ACCEPTED` p50 / p95 | 228.8 / **26,796** | 164.7 / **2,456** |
| `arcade_emit_to_applied_SEEN_ON` p50 / p95 | 430.2 / **27,110** | 384.7 / **2,585** |
| `post_to_202` p50 / p95 | 116.4 / 132.1 | 116.9 / 132.1 |

A block's MINED wave and the live ACCEPTED/SEEN stream share **one SSE connection and one
fan-out goroutine**, so a 122,640-tx block delays live status by ~27 s at p95. `post_to_202`
is flat across both buckets, which proves the delay is on the status path, not ingestion.

**Measurement note:** the SSE payload's `timestamp` field is RFC3339 at **second**
resolution, so payload-derived sub-second deltas are ±1 s (they produced a nonsensical
−291 ms p50 before correction). The frame **`id` is the same `status.Timestamp` at
nanosecond precision** (`services/sse/manager.go:684`). All figures above use the frame id.
*Any consumer measuring latency from the payload timestamp is getting ±1 s of noise* —
that is a small arcade API defect worth fixing (emit RFC3339Nano).

---

## Q2 — Arcade lag, attributed to the owning hop

### Intake is healthy and better than baseline

| metric | 2026-08-10 baseline (1000 TPS, 30 min) | **this run, P1** |
|---|---|---|
| arcade RECEIVED backlog | med 119 / p95 322 / max 552 | **med 89 / p95 203 / max 244** |
| broadcast → 202 RECEIVED | ~113 ms | **116.5 ms** |
| RECEIVED → ACCEPTED | ~94 ms | **~38 ms** (154 ms from create, minus 116 ms POST) |
| propagation CPU | 5,555m of 8,000m | 3,300–5,000m of 8,000m |
| failed / backpressure | 5 / 0 | **0 / 0** |

Arcade's *ingestion* path keeps up with 1000 TPS comfortably. RECEIVED stays flat.

### Where the backlog actually forms — and the proof

RECEIVED only grows once offered load exceeds the known single-partition propagation ceiling:

| segment | true rate | arcade RECEIVED med / p95 / max |
|---|---|---|
| P1 | 1,069/s | **89 / 203 / 244** |
| GAP-a | 1,182/s | 135 / 8,780 / 12,888 |
| GAP-b | **1,891/s** | **55,769 / 105,089 / 139,344** |

This is the documented ~1.6–1.9k propagation ceiling and is **not** reported as a new finding.

### The status path — the real lag, and it is arcade's

`arcade RECEIVED` stayed flat while the *toolbox's* view fell up to 577,725 transactions
behind. That is not the toolbox failing to apply. The chain, with evidence:

1. **arcade fan-out is one goroutine doing one DB query per event per token client.**
   `services/sse/manager.go:212`:
   ```go
   if c.Token != "" && !m.txBelongsToToken(ctx, status.TxID, c.Token) { continue }
   ```
   → `store/postgres/postgres.go:1363`:
   ```sql
   SELECT EXISTS(SELECT 1 FROM submissions WHERE txid = $1 AND callback_token = $2)
   ```
   `EXPLAIN ANALYZE` on the live table: **Execution Time 0.583 ms, `Buffers: shared read=5`
   (reads, not hits)**, over **4,522,695** rows. Plus network RTT ⇒ **~0.8–1.0 ms/event**
   on a single goroutine ⇒ **~1,000–1,700 events/s ceiling**.
   Measured toolbox receive rate: **~1,566 events/s.** Match.

2. **Demand exceeds it even at 1000 TPS.** Every transaction emits four transitions
   (ACCEPTED → SEEN_ON → SEEN_MULTIPLE → MINED) ⇒ **~4,000 events/s at 1000 TPS**, ~2.5×
   the fan-out ceiling. P1 showed no drops only because SEEN_MULTIPLE (+24 s) and MINED
   (+5 min) had not yet materialised inside a 7.6-minute window — the deficit was
   *accumulating*, not absent. At GAP-b's 1,891/s demand reached ~7,600 events/s and the
   per-client channel overflowed **15 seconds later** (first drop 13:03:55, GAP-b started
   13:03:40).

3. **The toolbox was demonstrably idle, not slow.**
   - toolbox CPU **~6 of 32 cores** at peak;
   - goroutine dump during drain: **3 `applyShard` goroutines busy** of 32 configured;
   - `mined_batch` traces show apply batches averaging **15.3 records against a 512 cap**
     (block 378) — the 16,384-slot reader→applier queue was near **empty**;
   - the toolbox's own apply lag stayed at **p50 29–35 ms / p95 <800 ms** throughout.
   - arcade `sse` pods peaked at **581 millicores across 3 pods** — idle, blocked on the DB.

4. **Recovery livelocks while load continues.** Arcade's mid-stream catchup (`catchup_eligible:
   true`, correctly enabled) replays the dropped window **down the same saturated
   connection**. Observed **with the blast fully stopped and zero new transactions**:
   ```
   msg":"sse mid-stream catchup round","client_id":2,"frames":11818,"capped":true
   msg":"dropped events for slow SSE client","client_id":2,"dropped":3919
   msg":"dropped events for slow SSE client","client_id":2,"dropped":3876
   ```
   570 drop lines and 34 catchup rounds in one 10-minute window with no live traffic.
   Total dropped across the run: **1,605,909 events**, 13:03:55 → 13:11:35.

**Attribution: the transition lag is owned by arcade's SSE fan-out, not by the toolbox
apply pipeline and not by merkle-service.** The SEEN/STUMP topic split did its job — SEEN
delivery never head-of-line-blocked on STUMPs.

### It does converge

Once load stopped, the system healed **without intervention**. Block 379 (13:17:44) and the
catchup rounds cleared the backlog:

| time | local no-status | local MINED |
|---|---|---|
| 13:11:33 (blast stopped) | 567,002 | 602,937 |
| 13:17:54 | 269,532 | 1,176,906 |
| 13:20:48 | **22,253** | **1,517,056** |

Final residual: **23,791 no-status, of which 23,360 were created in the last 3 minutes**
(fuel-keeper mints still in flight) and **only 67 predate the blast window**. Convergence
took ~10 minutes after load stopped.

---

## Q3 — Zero rejected transactions: **PASS**

| check | result |
|---|---|
| arcade `REJECTED`, run-scoped (`created_at >= 12:50:01Z`) | **0** |
| local `known_txs.arcade_status='REJECTED'` | **0** |
| local `competing_txs` non-null (double-spend evidence) | **0** |
| local `verified_rejected_at` non-null | **0** |
| local `suspect_since` non-null | **0** |
| local `status='stuck'` | **0** |
| local `status='suspectFailed'` | **0** |
| `spend_conflict_foreign_outpoint` / `spend_conflict_unresolved_winner` in app log | 0 / 0 |
| `UTXO_SPENT` in app log | 0 |

**Zero rejections across 1.54 M transactions, including the 1,891/s overload segment.** No
double-spends, nothing propagated out of order (no missing-inputs or ancestor-limit
rejections at all). PR #3's guards were never triggered because no conflict arose — the
1.27 % UTXO_SPENT churn of the previous run did not recur. Issue #6's `stuck` fund sink
gained **zero** members.

Final arcade run-scoped state: **MINED 1,508,029 / SEEN_MULTIPLE 23,528 / SEEN_ON 196 /
RECEIVED 0 / ACCEPTED 0 / REJECTED 0.**

### The 1,520 `failed` counter entries are an artefact, not rejections

`/api/status` ends at `failed: 1520`. All of them cluster at the three blast **stops**
(12:57:36, 13:03:28, 13:07:15). `metrics.RecordResult` counts any non-nil error, and the
blaster's own counter explicitly excludes `context.Canceled`
(`internal/blaster/blaster.go:233`) while the metrics counter does not. With 200–700
in-flight workers, each stop books its in-flight ops as failures. **Zero failures occurred
during steady-state blasting.** (This is only visible because the run was interrupted; it
would not appear in an uninterrupted run.)

---

## Q4 — Block → MINED with valid proofs: **PASS**

Pre-flight gate held for the entire run: **`callback-dlq` HWM flat at 82** (every sample),
**`stumps-dlq` 0**, **callback-delivery restarts 0**, peak memory **1,371 MiB of 4,096 MiB
(34 %)**. argocd #212 + #213 are validated: the STUMP path that collapsed on 2026-08-11
carried 1.5 M transactions' worth of blocks with zero dead letters and zero OOMs.

### Per block

| height | txs (arcade) | header_seen → processed | header_seen → **bump_built** | txs/s BUMP | toolbox `mined_batch`: batches / mean size | arcade_ts_min → first recv | delivery window | arcade_ts_min → apply done |
|---|---|---|---|---|---|---|---|---|
| 375 | 9,179 | 0.61 s | **0.61 s** | 15,000 | 19 / 483.1 | 1.43 s | 2.95 s | **19.1 s** |
| 376 | 122,640 | 11.79 s | **11.78 s** | 10,410 | 1,565 / 78.4 | 9.43 s | 32.0 s | **41.8 s** |
| 377 | 471,197 | 95.61 s | **95.57 s** | 4,930 | 5,192 / 157.5 | 75.3 s | 309.9 s | **385.2 s** |
| 378 | 573,976 | 122.43 s | **122.35 s** | 4,691 | 72,773 / **15.3** | 150.8 s | 203.1 s | **353.9 s** |
| 379 | 340,216 | 48.96 s | **48.93 s** | 6,949 | — | — | — | — |

`processed_at` and `bump_built_at` are within ~50 ms of each other on every block — **BUMP
construction is not a bottleneck**; it scales at ~4.7–15 k txs/s.

### Proofs actually landed

| check | result |
|---|---|
| local `arcade_status='MINED'` | **1,517,056** |
| of those, `merkle_path IS NOT NULL` | **1,517,056 (100 %)** |
| of those, `merkle_root IS NOT NULL` | **1,517,056 (100 %)** |
| mean `merkle_path` size | 733 bytes |
| local↔arcade status agreement, random 40-tx sample | **39/40** (the 1 miss is a fuel tx created seconds earlier, still in flight) |

The toolbox only persists a proof after verifying its merkle root against the chaintracks
header for that height (memoized per `(height, root)`, commit `bca3514`), so 100 % proof
presence means 100 % **verified** proofs.

**Correction to the plan's acceptance criterion:** it required `merkle_path IS NOT NULL` on
*arcade's* `transactions` table. Arcade stores **0** merkle paths there — all 1,167,813
MINED rows have `merkle_path` NULL but `merkle_registered_at` set. Arcade does not persist
the proof in that column; it enriches at fan-out time from the merkle store
(`enrichMerklePath`). **Checking arcade's `merkle_path` column would have produced a false
100 % failure.** The local column is the authoritative check.

**Environment change worth recording: the 50,000-tx/block assembly cap is gone.** Blocks in
this run were 122,640 / 471,197 / 573,976 / 340,216 transactions. Every prior benchmark's
"settled throughput = 50k × block frequency" reasoning is obsolete.

---

## Performance gaps and inefficiencies

Ranked by impact, each with the evidence that supports it.

### 1. arcade: SSE fan-out does one synchronous DB query per event per token client — **~1,600 events/s ceiling**
`services/sse/manager.go:212` → `store/postgres/postgres.go:1363`. 0.583 ms measured
execution, 5 buffer *reads*, 4,522,695-row table, on the manager's **single** fan-out
goroutine. Demand at 1000 TPS is ~4,000 events/s. sse pods sat at 581 millicores — idle.
**Fix:** cache token↔txid membership in memory (a submission's token never changes), or
attach the token set to the status event at publish time, or shard fan-out per client.
This one change removes the drops, the catchup livelock, and the p95 blowups in Q1.

### 2. arcade: mid-stream catchup replays into the same saturated connection — livelock
Observed with **zero** live traffic: 34 catchup rounds and 570 drop lines in 10 minutes,
alternating `catchup round frames:11818 capped:true` → `dropped:3919`. Catchup uses the
efficient bulk query (`IterateStatusesByToken`) but re-enters the same slow fan-out.
**Fix:** rate-match catchup to the client's observed drain rate, or deliver catchup on a
side channel that bypasses the per-event probe. Without #1 this is a positive feedback loop.

### 3. arcade: the "slow SSE client" log line misattributes the fault
1,605,909 events logged as dropped because of a "slow client" that was running at 6 of 32
cores with an empty queue. This log line has cost three runs' worth of misdirected
toolbox investigation. **Fix:** log the fan-out's own dispatch rate and per-event probe
latency alongside the drop count.

### 4. toolbox: `/api/status` and `/api/series` overstate throughput by a variable 5–15×
**This is a real defect and every rate number in this document deliberately avoids it.**
`internal/metrics/metrics.go:180-197`: `RunSampler` uses `time.NewTicker(time.Second)` and
computes `rate := float64(succeeded - m.prevSucceeded)` — a raw delta **labelled per-second**.
But `sampleOnce` performs gauge queries against Postgres/Aerospike inline, so it takes
seconds, and `time.Ticker` *drops* missed ticks. Measured directly:

| snapshot interval | succeeded delta | **reported `rate_per_sec`** | **true rate** |
|---|---|---|---|
| 3,799 ms | 3,749 | 3,749 | **987/s** |
| 4,390 ms | 4,262 | 4,262 | **971/s** |
| 2,771 ms | 2,833 | 2,833 | **1,022/s** |
| 2,796 ms | 2,745 | 2,745 | **982/s** |
| 2,427 ms | 2,425 | 2,425 | **999/s** |

`/api/series` over the run: `tps` med 1,671 / p95 8,953 / **max 15,831**; `smooth_tps` med
2,076 / p95 8,238 / max 11,289 — against a true peak of 1,891/s. The inflation is the
sampling interval, so it is **not a constant factor**; it worsens exactly when the app is
busy. The 600-point series is also mislabelled on the x-axis: 600 "seconds" of ring buffer
actually spans ~30–40 minutes.
**Fix:** divide by real elapsed time (`t.Sub(prevSampleAt).Seconds()`) and move the gauge
reads off the rate-sampling path.

### 5. toolbox: `context.Canceled` trips the arcade circuit breaker, blocking healthy traffic
`pkg/arcade/client.go:186-193` feeds the breaker from a `switch` whose `default` arm counts
**any** non-backpressure error as an opaque outage, and `wrapTransportError`
(`client.go:317`) does not distinguish `ctx.Err()`. Evidence: **538 `arcade broadcast
short-circuited by open circuit breaker` warnings with zero transport errors, zero
`hop=broadcast_error` traces, and zero arcade 5xx responses.** The largest cluster (306) is
at the 12:57:36 blast stop, when 200 workers cancelled at once and blew past the
`failure_threshold: 10`; 102 more landed in the **next** blast's first minute, i.e. the
breaker was still open and refusing healthy broadcasts. Fuel-keeper `errgroup` cancellation
(`mintRecycleLeaves`) does the same thing and cross-contaminates the payment path — they
share one client.
**Fix:** treat `errors.Is(err, context.Canceled)`/`DeadlineExceeded` as neither success nor
failure for the breaker.

### 6. toolbox: the applier degenerates to tiny batches under a slow reader
`pkg/monitor/status_events.go:120-140` — one applier goroutine drains whatever is *ready*
(`default: break drain`) with no linger and no minimum batch, then blocks until the whole
batch commits across 32 shards **and writes the replay cursor**. Block 378 produced
**72,773 batches averaging 15.3 records** against `applyBatchMax = 512`: ~72k fan-out
setups and ~72k cursor writes to move 1.1 M records. When the reader is starved (gap #1),
the fixed per-batch cost dominates.
**Fix:** add a short linger (e.g. 5–10 ms) or a minimum batch size before applying, and
coalesce the cursor write.

### 7. toolbox: one SSE connection means a MINED block burst head-of-line-blocks live status
p50 86 ms → p95 21.6 s for ACCEPTED delivery in P1, and the split is entirely bucket 0 (block
376) vs bucket 1 (no block): 26,777 ms vs 2,269 ms p95. This mirrors the merkle
`callback`-topic anti-pattern that was already fixed upstream by the SEEN/STUMP split.
**Fix:** a second SSE subscription (or an arcade-side status filter) so bulk MINED does not
share a connection with live ACCEPTED/SEEN.

### 8. toolbox: `arcade_status` recovery rides a slow poll when SSE has dropped
Once events are dropped and the cursor has advanced past them, the only convergent path is
the repair poll. Log evidence: `poll: repairing transactions with no arcade status
repairing=4000 arcade_status_missing_total=280307`, ticking at 4,000 rows/≈60 s ≈ **67/s**.
The 269 k backlog would have needed ~65 minutes on that path alone; it was actually rescued
by block 379's catchup. It works, but **recovery rate should scale with
`arcade_status_missing_total`**, not be a fixed 4,000.

### 9. toolbox: fuel-keeper `not enough funds` error spam during normal operation
35 `ERROR fuel top-up round failed` and 534 `chunk fan-out did not fund, halving ask` lines
during a run whose pool built to 1.55 M leaves without trouble. These are ordinary
contention between concurrent mint rounds, logged at ERROR. They also drive gap #5.
**Fix:** demote to DEBUG or make the round serialise on the reserve.

### 10. arcade: SSE payload timestamps are second-resolution
The `data` payload's `timestamp` is RFC3339 without fractional seconds while the frame `id`
carries nanoseconds. Any consumer measuring latency from the payload sees ±1 s error
(it produced a −291 ms p50 in the first pass of this analysis). **Fix:** emit RFC3339Nano.

### Non-findings (do not re-report)
- arcade `propagation` = 1 partition ⇒ ~1.6–1.9 k intake ceiling. Reconfirmed exactly at
  GAP-b (1,891/s, RECEIVED → 139,344).
- merkle `callback` = 1 partition, STUMP-only. It behaved: DLQ flat, no head-of-line blocking.
- **The 50k/block teranode assembly cap no longer exists** — see Q4. Prior runs' settled-
  throughput arithmetic should be discarded.

---

## Acceptance criteria

| criterion | verdict |
|---|---|
| Pre-flight gate: DLQ flat at 82, callback-delivery restarts 0 | **PASS** — flat for the entire run; peak mem 34 % of 4 Gi |
| Q1: arcade-emit → applied p95 low and stable across buckets | **FAIL** — p50 is excellent (215–1,442 ms) but p95 reaches 21.6 s during block bursts, and is not stable bucket-to-bucket (26.8 s → 2.5 s). The toolbox's own apply step passes cleanly (p95 < 800 ms); the failure is transport. |
| Q2: arcade RECEIVED flat at baseline **and zero SSE drops** | **SPLIT — RECEIVED PASS, drops FAIL.** RECEIVED beat baseline (med 89 vs 119, max 244 vs 552). But 1,605,909 events were dropped. Root cause located to arcade `manager.go:212`. |
| Q3: **0** REJECTED | **PASS** — 0 rejections, 0 double-spends, 0 out-of-order, 0 `stuck`, 0 `suspectFailed`, across 1.54 M txs |
| Q4: every block gets `bump_built_at`; txs reach MINED with non-null `merkle_path` | **PASS** — 5/5 blocks built BUMPs; 1,517,056/1,517,056 MINED carry verified proofs |
| Cross-check: ~40 local txids match arcade status-for-status | **PASS** — 39/40 (the 1 miss was created seconds before sampling) |
| 45 minutes at 1000 TPS, uninterrupted | **FAIL** — 455 s clean at 1000 TPS; interrupted by an off-plan ramp and stopped early on request |

## Ranked list for the next run

1. **Fix arcade's per-event token probe** (gap #1). Nothing else in the status path matters
   until fan-out can exceed ~1,600 events/s. Cache the token↔txid membership or carry the
   token set on the event. Re-run and confirm zero `slow SSE client` drops at 1000 TPS.
2. **Rate-match or bypass mid-stream catchup** (gap #2) so recovery cannot livelock.
3. **Fix the dashboard rate calculation** (gap #4) — divide by real elapsed time and move
   gauge reads off the sampler. Until then, no TPS number from the UI is usable.
4. **Stop counting `context.Canceled` against the circuit breaker** (gap #5) — a one-line
   guard that today lets a blast stop, or a fuel-keeper hiccup, block real broadcasts.
5. **Add a linger/min-batch to the applier and coalesce the cursor write** (gap #6).
6. **Re-run the full uninterrupted 45 minutes at 1000 TPS** once #1 lands, and only then
   judge Q1's p95 criterion. This run's clean window was 7.6 minutes.
7. **Split the SSE subscription** so MINED bursts stop blocking live status (gap #7).
8. **Scale repair-poll cadence with `arcade_status_missing_total`** (gap #8).
9. Re-baseline settled-throughput expectations now that the 50k/block cap is gone.
