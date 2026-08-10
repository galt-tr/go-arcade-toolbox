# Scaling / blast summary + next-run plan (2026-08-10)

**Read this first when picking the effort back up.** Self-contained re-orientation:
what we've been testing, where the numbers landed, every bottleneck found and its
fix/deploy status, and the specific performance gaps to watch on the next run.

---

## 1. TL;DR

- **The toolbox is no longer the binding constraint at 1500 TPS.** Both toolbox-side
  bottlenecks from the first runs are fixed and validated: the SSE-delivery loss and
  the MINED-apply lag. It sustains 1500 TPS broadcast with 0 failures and applies
  SEEN/MINED in single-digit ms.
- **The remaining ceiling is entirely downstream (cluster-side):** merkle-service
  block processing, teranode block-assembly capacity, and arcade's single-active
  propagation path. These are what the next run should measure.
- **Reorg recovery is now fixed** (arcade #280→#283→#284→#285 + merkle v0.5.2) after a
  same-height competition (height 764) left ~92.7k txs anchored to an orphan. The
  incident was healed; the reconciler now re-anchors from the stored canonical BUMP.
- **A real correctness/throughput gap surfaced:** of ~212.8k txs broadcast, a large
  tail (tens of thousands) never got mined at all — teranode didn't include them in a
  block and they were eventually dropped. This is the biggest open question for the
  next run.

---

## 2. System under test

```
go-arcade-toolbox (buyer wallet, aerospike-hybrid)
   │  async broadcast: POST /tx (Extended Format) → 202 RECEIVED
   │  SSE status stream: RECEIVED → SEEN_ON_NETWORK → SEEN_MULTIPLE_NODES → MINED → IMMUTABLE
   │  BUMP (BRC-74 merkle proof) verified LOCALLY vs chaintracks headers
   ▼
arcade (scale-ovh, ns arcade-v2)  ──►  teranode + merkle-service (block/subtree processing)
   └─ chaintracks (headers)              └─ STUMPs → BLOCK_PROCESSED → compound BUMP
```

- Cluster: `dev-ovh-1` on scale-ovh (`KUBECONFIG=~/.kube/scale-ovh`), **regtest**
  difficulty (trivial PoW → same-height competitions are easy; blocks mined on demand).
- Toolbox stores (local, podman): PG `arcade_buyer` on `127.0.0.1:5433`
  (`go-wallet-toolbox_db_1`), aerospike `test:utxos` on `127.0.0.1:3000-3002`
  (`arcade-aerospike`). Backend = **aerospike-hybrid** (aerospike hot-path UTXOs, PG
  metadata).
- Local truth model: the wallet's local ledger is the source of truth for spendable
  UTXOs; arcade is the tx-status/proof oracle; chaintracks supplies headers; merkle-root
  verification is local.

---

## 3. Test methodology (the repro)

- **App:** buyer-only (`SELLER_MONITOR=off`), instrumented build `facts-app-txtrace`
  (in the session scratchpad). `IMMEDIATE_BROADCAST=true`, `HIGH_THROUGHPUT=1`,
  `UTXO_BACKEND=aerospike-hybrid`, `DB_ENGINE=postgres`, `MAX_DB_CONNS=180`.
- **Fuel pool:** `FUEL_DENOM_SATS=600`, `FUEL_TARGET_POOL=350000` (keeper settles
  ~223k above low-water), `FUEL_STREAM_LEAF_CAP=2000`, recycle + self-replenish **off**.
  Denominated fuel pool is the 1000+ TPS funding path (`ClaimExact`).
- **Blast:** `tps=1500 workers=256`, ~140 s, **212,800 txs**, shallow-ancestry fuel
  (no self-payment chaining). `BLAST_AMOUNT_SATS=50`, `FEE_SAT_PER_KB=125`.
- **Fee floor:** 100 sat/kB == the arcade GoBDK min-fee floor (zero margin). Static
  sat/kB must satisfy it or txs 4xx-reject as final.
- **Instrumentation:** `pkg/txtrace` (opt-in per-tx lifecycle tracing — ctx sample-flag
  + txid mark-set + `msg=txtrace` slog lines). Trace one sampled tx create→202→each SSE
  status→SEEN_MULTIPLE_NODES; `mined_batch` traces summarize MINED apply/lag per height.
- **Reset (fresh env):** truncate PG `arcade_buyer` + `asinfo truncate:namespace=test;set=utxos`.
  (Done 2026-08-10 — see §8.) Buyer deposit address `n2TGvu3w6YDyr8bHNfT23pjSvesqMDXfdj`.

---

## 4. Results — where we ended up

### Toolbox (upstream) — healthy
| metric | value |
|---|---|
| Broadcast throughput | **1500 TPS, 0 failed, 0 backpressure** (212,800 txs) |
| create + sign | ~5 ms |
| POST /tx → 202 RECEIVED | ~113–131 ms (arcade RTT) |
| SEEN apply (per event) | ~35–42 ms |
| MINED apply (per batch, post-fix) | **~1–3 ms** |

### The two toolbox bottlenecks — found, fixed, validated
| # | bottleneck | before | after fix |
|---|---|---|---|
| A | **Arcade SSE delivery** — per-conn send channel was a hardcoded 64-slot non-blocking buffer that silently dropped on overflow; producers coalesced ~50 txids/msg into bursts that overflowed it | ~52% of SEEN events dropped (~111k stuck, never recovered) | **5 / 214,957 (~0.002%)** — nothing dropped |
| B | **Toolbox MINED apply** — `RemoveSpentBy` did a full aerospike SET SCAN per mined tx (O(set-size × mined-txs)); + re-fetched the same tip header per tx | ~28/s, 512-batch ~18–20s, **recv→apply lag ~16 min** on a 200k-tx block | **~1–3 ms/batch, lag → ms, ~6,500/s burst** |

Fix A = arcade **#277** (`SSEConfig.ClientBufferSize` default 8192 + coalesced frame
writes), shipped **v0.10.6**. Fix B = toolbox `e225d6f` (prune spent inputs by outpoint
from local raw tx, one batched `utxo.Remove`) + `bca3514` (memoize merkle-root verify
per `(height,root)` within a batch). Both in toolbox `main`.

**Bottom line:** at 1500 TPS the toolbox is off the critical path for both SEEN and
MINED. The next levers are all cluster-side.

---

## 5. The new gate is downstream (this is what to profile next)

- **merkle-service block processing** — a 215k-tx block ran subtree-workers at **~87
  cores (16–19 each) for ~30–45 s** before arcade could emit MINED. This is the dominant
  MINED-latency gate.
- **teranode block assembly** — put only **~101.5k of the 215k** txs into one block; the
  rest stay SEEN for the next block (or never get mined — see §7).
- **arcade propagation is single-active** — `arcade.propagation` topic pinned to 1
  partition for correctness, so one propagation pod is the throughput ceiling. Tuned in
  the ConfigMap for ~10× load: `merkle_concurrency=50`, `max_concurrent_batches=8`,
  `teranode_max_batch_size=25`. On defaults it does big serial ~7s broadcast batches and
  the RECEIVED backlog grows unbounded at 1000 TPS.

---

## 6. The reorg incident (height 764) — resolved

Regtest trivial PoW produced a **same-height competition at 764**: canonical
`12114cd0…` (merkleRoot `04e3eb54…`) vs orphan `5a55997b…` (root `7eede453…`). Arcade
kept ~**92.7k** txs anchored to the orphan and served orphan proofs; the toolbox
**correctly rejected** them (local verify against canonical chaintracks headers). Not a
toolbox/chaintracks bug — an arcade post-competition tx→block index that didn't re-point.

Fix line (all merged): arcade **#280** (re-anchor after competition) → **#283** (bound the
startup full-scan) → **#284** (gate the startup scan on chaintracks readiness — v0.11.2)
→ **#285** (reconcile tick prioritizes *re-anchorable* orphans; v0.11.3, pending argo #202)
→ merkle **#209** (`/reprocess` dedup-bypass; **v0.5.2**).

Why it kept "not healing" through the iterations (lessons):
1. **v0.11.1:** bounded scan was defeated because the bump-builder's embedded chaintracks
   is **ephemeral and resyncs from genesis on every deploy** — the startup scan ran at
   height 0 and fail-opened (→ #284 readiness gate).
2. **v0.11.2:** the scan was a no-op (764 already marked orphaned); the real blocker was
   the **reconcile tick draining 4,356 orphaned rows oldest-first** (regtest spawned
   thousands of same-height competitions), burying 764 behind ancient un-re-anchorable
   orphans (→ #285 prioritize re-anchorable).
3. **Healed** via a one-row PG nudge (`orphaned_at → epoch`) moving 764 to the front;
   the reconciler re-anchored **214,954 txs** to canonical `12114cd0` and re-emitted
   MINED; the toolbox drained SEEN 113,401 → 32,758 / MINED → 182,214. `00f143…` →
   `MINED@12114cd0`. With #285 deployed, no manual nudge is needed next time.

Key facts for next time: arcade `block_processing` keeps **multiple rows per height**
after a competition (winner `active`, losers `orphaned`); `GetTx` returns both the
canonical and former-orphan blockHash for re-anchored txs. Do **not** trust arcade
`GetTx.blockHash` as canonical — compare teranode `/api/v1/lastblocks` vs chaintracks
`/header/height/N` block-for-block.

---

## 7. Performance gaps / open questions to watch on the NEXT run

Ranked by importance:

1. **[BIGGEST] What fraction of broadcast txs actually reach MINED?** In the last run
   ~**32.7k** txs (of 212.8k) ended stuck `SEEN` forever — no local proof, and arcade
   returned "not found" for them. They were **never mined into any block** (teranode
   didn't include them; evicted/dropped from mempool). Plus ~**13** stuck at `RECEIVED`
   (teranode never propagated them past the 202). **Measure:** broadcast→MINED
   conversion rate, SEEN-that-never-mines count, mempool eviction, per-block inclusion
   counts. This is the difference between "1500 TPS accepted" and "1500 TPS *settled*".

2. **merkle-service block-processing throughput** — the MINED-latency gate. **Measure:**
   subtree-worker core-hours per block, time from block-found → arcade emits MINED,
   MINED emit rate (last run ~6,500/s bounded by arcade's emit, not the toolbox apply).

3. **teranode block-assembly capacity** — how many txs land per block vs how many spill.
   **Measure:** txs/block, SEEN backlog growth between blocks, block cadence vs broadcast
   rate. Last run: ~101.5k/215k in one block.

4. **arcade propagation single-active ceiling** — **Measure:** RECEIVED→ACCEPTED latency
   and RECEIVED backlog depth as TPS climbs; whether the single propagation pod
   saturates. Confirm the tuned `propagation` knobs hold at the target rate.

5. **Reorg recovery under sustained load** — now that #280–#285 are in, confirm
   same-height competitions **self-heal (re-anchor) without a manual nudge**, and the
   reconcile tick keeps up (watch `txs_reanchored`, orphaned-row backlog size). Note the
   embedded-chaintracks resync-on-deploy behavior (§6.1) — expect a few minutes of
   header catch-up after any arcade roll before proofs verify.

6. **Toolbox robustness follow-ups** (filed, not root-cause; verify they don't bite):
   - proof-poll should **back off + alert** on proofs that persistently fail verification
     against a present/stable header (last run emitted ~92,700 warnings/round during the
     fork) instead of hot-retrying.
   - `FailAbandoned` should **reap never-SEEN txs** after the reservation TTL so a
     perpetually-`RECEIVED` tx can't leak its reserved inputs (relevant to the ~13 and
     any RECEIVED-stuck set at scale).

---

## 8. Version / deploy matrix going into the next test

| component | version / state |
|---|---|
| **toolbox** | `main` — SSE-delivery fix + MINED-apply-by-outpoint fix + `pkg/txtrace` all in |
| **arcade** | **v0.11.2 deployed** (chaintracks-readiness gate, #284). **v0.11.3** = tick prioritization (#285) — pending **argo PR #202** merge (also drops the temporary `full_scan 764` target). Merge #202 → ArgoCD rolls v0.11.3. |
| **merkle-service** | **v0.5.2 deployed** (`/reprocess` dedup-bypass, #209) |
| **argo config** | `arcade-v2-config` ConfigMap — propagation tuned (see §5); after #202 merges, remove-target is done and reconciler runs on the depth-144 horizon |

Deploy gotchas seen (all cluster-side, expect them again): merging a version bump ahead
of the GHCR image publish → brief `ImagePullBackOff` (self-resolves); chaintracks
(standalone **and** bump-builder embedded) resyncs from genesis on every re-roll (ephemeral
storage) → a few minutes at height 0 before headers/proofs work; sse/api-server can
crash-loop on `apply schema: timeout` on cold boot.

---

## 9. Running the next test on the fresh env

Environment was **wiped clean 2026-08-10**: PG `arcade_buyer` truncated (schema/goose
kept), aerospike `test:utxos` truncated to 0 objects (`default-ttl=0` intact), PG
container restarted (memory 11.9 GB → 401 MB). App is **stopped**.

Steps:
1. **(Optional) Merge argo #202** first so the run is against arcade **v0.11.3** (tick
   prioritization) rather than v0.11.2.
2. **Restart the app** — `facts-app-txtrace serve` (scratchpad) with the §3 env config.
3. **Re-fund** — wallet has 0 UTXOs; internalize the buyer deposit
   (`n2TGvu3w6YDyr8bHNfT23pjSvesqMDXfdj`) and let the fuel keeper build the pool.
4. **Blast** `tps=1500 workers=256` (or step the rate to find the settled-TPS knee).
5. **Capture** (the §7 gaps): broadcast→MINED conversion (not just acceptance),
   per-block inclusion counts, time-to-MINED, RECEIVED/SEEN backlog curves, subtree-worker
   core usage, and one full txtrace (create→SEEN_MULTIPLE_NODES) + `mined_batch` lag.
6. Mine blocks on demand; for a MINED measurement, trigger a block once the SEEN backlog
   is large.

Note: only the **local** toolbox stores were wiped. The **arcade cluster chain was not
reset** — new txids process fresh, so a cluster reset isn't required. If a clean *chain*
is wanted (empty blocks/mempool, no leftover competitions), that's a separate
teranode/merkle reset.

---

## Reference: related docs & memory
- `docs/benchmarks/20260809-sse-delivery-and-mined-apply-fix-validation.md` — the
  before/after validation of fixes A + B.
- Memory: `throughput-blast-arcade-pipeline`, `sse-mined-apply-bottlenecks`,
  `arcade-fee-floor-and-delayed-broadcast`.
