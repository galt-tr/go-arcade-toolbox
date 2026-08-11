# The MINED collapse: 5 oversized STUMPs, a DLQ, and 1.65M txs parked at SEEN (2026-08-11)

Follow-up to `20260810-postfix-validation-and-partition-ceilings.md`. This run validated the
toolbox status-recovery work (the 23,745-stranded bug is **fixed and proven**) and, in doing
so, exposed the next binding constraint clearly: **arcade never learned that 5 blocks were
mined**, because the merkle-service STUMP callbacks for those blocks died and landed in a
dead-letter topic.

The headline number looks alarming and is not a toolbox defect: **MINED = 58,417 of
1,726,729 (3.4%)**, with **1,646,318 (95.3%) parked at `SEEN_MULTIPLE_NODES`**. The toolbox
is faithfully mirroring arcade. Arcade is the one that never got the proofs.

## TL;DR

- **The status-recovery fix works.** Local `arcade_status` empty count went **23,745 → 1**,
  and a random 40-tx sample matched arcade **40/40**. The toolbox no longer diverges from the
  source of truth. PRs #3 and #4 are merged to `main`.
- **The MINED collapse is upstream and precisely localized.** `callback-dlq` holds **82
  messages, 100% `type: STUMP`**, which dedupe to **exactly 5 distinct
  `(blockHash, subtreeHash)` pairs = 5 distinct `stumpRef`s**. Those 5 blocks' worth of
  transactions are the ~1.65M stuck at SEEN.
- **The delivery process was OOMKilled 19×** (exit 137, 1 GiB limit, 1 replica). The last log
  line before each death is a `type:"STUMP"` message being reprocessed with dedup bypassed —
  a poison-message crash loop on the 1-partition `callback` topic.
- **Correction to an earlier diagnosis: there was no 413.** Zero payload-too-large responses
  across all three arcade api-server pods in 11h. arcade's inbound cap is already 128 MiB.
  Grepping logs for `413` matches hex inside txids — that false positive is what produced the
  earlier "413 problem" framing.
- **The SEEN/STUMP topic split is now live** and it worked. `callback-seen` carried real
  traffic across 12 partitions while `callback` (1 partition) carried only STUMP /
  BLOCK_PROCESSED. That is exactly why SEEN delivery stayed healthy (1.65M reached
  `SEEN_MULTIPLE_NODES`) while MINED collapsed — the two failure domains are now separated,
  which is what made this diagnosis clean.

## What actually happened

Arcade's own status counts, bucketed by hour, show the inflection precisely:

| hour (UTC) | MINED | SEEN_MULTIPLE_NODES | REJECTED |
|---|---|---|---|
| 01:00 | 204,381 | — | — |
| 02:00 | 734,656 | 313,319 | 285 |
| **03:00** | **55,271** | **1,646,422** | 1,521 |
| 04:00 | 134 | — | 1,897 |
| 05:00–08:00 | 101–1,965/hr | — | 858–6,672 |

Mining converted normally through 02:00 (939,037 MINED). From 03:00 onward it essentially
stopped, and 1,646,422 transactions banked up at SEEN — matching the toolbox's 1,646,318
almost exactly.

The DLQ explains the gap. Its 82 messages carry `nextRetryAt` timestamps spanning
**02:42:22Z → 05:29:17Z**, and every one is a STUMP:

```
distinct (block,subtree) pairs: 5
  block 5d7df985ec49d3b2… subtree 8d6c0eb6266d035a…  -> 39 DLQ copies
  block 21e29792c4ffd5a8… subtree 7ed4bd3a54e2d3aa…  -> 24 DLQ copies
  block 03c4b4bcfbe21359… subtree c34c5bc4b5bfb155…  -> 17 DLQ copies
  block 46352979d4c54380… subtree 9b90a1a13f6060ff…  ->  1 DLQ copy
  block 41f1f3a60b3c729c… subtree 307f2dda38029934…  ->  1 DLQ copy
```

Block `46352979…` is the one flagged during the run as "the BUMP was built but the txs never
went MINED." That is now fully explained: the BUMP *was* built, but the STUMP callback that
tells arcade about it never landed, so arcade never wrote MINED, so the toolbox never saw it.
The toolbox was correct the whole time.

### Why the STUMPs are oversized

`block_processor` reports **`subtreeCount: 1`** for heights 319, 320 and 321 (verified
directly in its logs). The rebuilt teranode on this env emits **one subtree per block** where
it previously emitted dozens to hundreds. So a single STUMP now carries an entire block:
~48.7 MB, which merkle hex-encodes (2×) into a **97,344,002-byte JSON body**.

### Why it died

The Kafka message is already a claim-check — it carries a `stumpRef`, not the blob. The blow-up
is on the delivery side: fetch ~48.7 MB from the cache, hex-encode to a 97 MB string, marshal
into JSON, and hold all of it while POSTing. That is several hundred MB of transient
allocation per message against a **1 GiB limit on a single replica**, and it repeats on every
retry.

Both failure modes are real and they compound:
1. **Timeout** — a 10s `http.Client.Timeout` must cover connect + 97 MB upload + arcade's
   fully-synchronous `handleStump` + response. 10s demands ≥78 Mbit/s sustained for the
   upload alone. Measured: `callback http transport error` at `durationMs 10001`.
2. **OOM** — 19 restarts, `exitCode 137`, `reason: OOMKilled`.

The 82-copies-from-5-messages amplification is the crash loop itself: each OOM restart
redelivers the uncommitted message, republishes it with an incremented `retryCount`, and dies
again before committing.

### The pending config change is necessary but not sufficient

argocd **#212** raises `callback.timeoutSec` 10 → 120. Its own note says *"WATCH MEMORY.
callback-delivery is 1 replica at a 1Gi limit… If the pod OOMs, raise the limit."* **That "if"
has already resolved — the pod OOMed 19 times.** Merging #212 alone holds a 97 MB body in
memory for up to 120s across 6 attempts (~19.5 min per message), which makes the OOM more
likely, not less. **#212 must be amended to raise the memory limit in the same change**, or it
will trade a fast failure for a slower one.

## Toolbox: what was validated

| check | result |
|---|---|
| local `arcade_status` empty | **1** (was 23,745) |
| local vs arcade, random sample | **40/40 match** |
| created / failed / backpressure | 1,534,433 / **0** / **0** |
| p50 / p95 create latency | 135 ms / 7,041 ms |
| SSE apply stall (singleflight fix) | no recurrence |

The one remaining `(none)` row is a single transaction, not a systemic gap. The
`FindMissingArcadeStatus` repair query plus `last_polled_at` ordering did what it was designed
to do: no row can be re-selected forever without the poll making progress past it.

**21,865 REJECTED (1.27%)** were previously proven to be UTXO_SPENT churn with earlier
self-owned winners — the release-all bug that PR #3 closes. PR #3 is merged but **this run
predates it**, so the next run is the first real test of those guards.

## What to interrogate on the next run

Ranked. The first three are the ones that decide whether the run is meaningful at all.

1. **Does any STUMP land in `callback-dlq`?** This is the single highest-signal check and it
   is cheap: `rpk topic describe callback-dlq -p` — high-watermark should stay flat. If it
   moves, MINED conversion is already broken and the rest of the run's MINED numbers are
   meaningless. Check it *before* trusting any conversion percentage.
2. **Does `callback-delivery` restart?** `kubectl -n merkle-service get pod -l app=callback-delivery`
   — any non-zero restart count is an OOM until proven otherwise. Watch `kubectl top pod`
   against the 1 GiB limit during block processing, and confirm whether the memory limit was
   raised alongside #212's timeout.
3. **What is `subtreeCount` per block under load?** If it is still 1, STUMPs stay
   block-sized and the problem is structural rather than tuning. Dozens-to-hundreds means
   teranode's sizing is restored and the pressure is off. This is the root cause; everything
   else is mitigation.
4. **Do PR #3's guards behave?** New log reasons `spend_conflict_foreign_outpoint` and
   `spend_conflict_unresolved_winner` should appear instead of blanket releases, and REJECTED
   should fall well below this run's 1.27%. Watch for the flip side: transactions accumulating
   in `suspectFailed`/`stuck`, which is issue #6 (stuck has no exit path and is a permanent
   fund sink). Count them; don't let them silently grow.
5. **Why do 26 of 82 DLQ messages have `retryCount: 0`?** A message that has never been
   retried should not be in a dead-letter topic. Either the retry counter resets across the
   crash loop or some messages bypass retry entirely. Worth resolving because it affects
   whether the DLQ is a trustworthy signal.
6. **Does the toolbox recover once STUMPs are delivered?** The 1.65M at SEEN are a natural
   experiment: if the 5 blocks' STUMPs are ever redelivered from the DLQ, those transactions
   should transition to MINED without intervention. That end-to-end path has not been
   exercised.
7. **Intake ceiling** (unchanged from 2026-08-10): arcade's `propagation` topic is still 1
   partition, ~1.6–1.9k TPS. Do not read a plateau near that range as a toolbox limit.

## Environment state after this run

- **Toolbox reset and rebuilt.** Local Postgres (`arcade_buyer`, `arcade_seller`) truncated;
  Aerospike `test` namespace truncated (4,026,438 → 0 objects); `buyer.key` / `seller.key`
  preserved. Rebuilt from `main` (now containing PRs #3 and #4) and restarted clean: 0 txs, 0
  balance, 0 errors on startup.
- **Funding address:** `n2TGvu3w6YDyr8bHNfT23pjSvesqMDXfdj` (unchanged; the EF must be mined
  before import).
- **Arcade retains 2,981,235 transactions** from prior runs. This is *not* wiped. Consequence:
  a fresh token-scoped SSE connect triggers a catchup that arcade truncates at its 10,000-frame
  cap (`sse catchup truncated at frame cap`). It caused no local corruption — the toolbox
  ignored txids it does not know, and the ledger stayed at 0 rows — but arcade-side status
  distributions will mix old and new transactions until that table is truncated.
- `callback-dlq` still holds the 82 STUMP messages. Leaving them is useful: they are the
  before/after evidence for whether a memory-limit bump actually fixes delivery.
