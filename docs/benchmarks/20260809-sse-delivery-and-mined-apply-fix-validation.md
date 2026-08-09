# SSE-delivery + MINED-apply fix validation — Aerospike hybrid (2026-08-09)

Second end-to-end instrumented blast (`toolbox-app-arcade` self-payment blaster
against **real arcade** on the scaling cluster, **aerospike-hybrid** backend,
Mode B). This run validates the two fixes that came out of the [first run's
whole-system diagnosis](20260808-app-blast-end-to-end-aerospike-hybrid.md):

- **Arcade SSE status delivery** — `bsv-blockchain/arcade#277` (deployed as
  `v0.10.6`): the per-connection SSE send channel was a hardcoded 64-slot,
  non-blocking buffer that silently dropped on overflow; the fix makes it
  configurable (default 8192) and coalesces frame writes (one flush per drained
  burst instead of one per frame).
- **Toolbox MINED apply** — `e225d6f`: `applyMinedBatch` pruned each mined tx's
  spent inputs with a per-tx `RemoveSpentBy`, which runs a full aerospike
  set-scan (O(set-size) per tx); the fix resolves the inputs from the local raw
  tx and issues one batched `utxo.Remove(ops, force)` (O(inputs)). Plus `bca3514`:
  memoize MINED merkle-root verification per `(height, root)` within a batch so a
  block's worth of MINED events fetches the (uncached, tip) block header once
  instead of per tx.

Instrumentation: `pkg/txtrace` (opt-in per-tx lifecycle tracing) + a buyer-only
app run (`SELLER_MONITOR=off`) for unambiguous traces.

## Environment

- Buyer wallet, `HIGH_THROUGHPUT=1`, `IMMEDIATE_BROADCAST=true`,
  `UTXO_BACKEND=aerospike-hybrid`, `DB_ENGINE=postgres`; `FUEL_DENOM_SATS=600`,
  `FUEL_TARGET_POOL=350000` (keeper settled ~213k above the low-water mark),
  `FUEL_STREAM_LEAF_CAP=2000`, recycle + self-replenish **off**,
  `FEE_SAT_PER_KB=125`, `BLAST_AMOUNT_SATS=50`, `MAX_DB_CONNS=180`.
- Blast: `tps=1500 workers=256`, ~140 s (pool-limited). 212,800 txs, an
  independent shallow-ancestry fuel pool (no self-payment chaining).
- Stores reset clean before the run (PG `arcade_buyer` truncated, aerospike
  `test:utxos` truncated to 0).

## Results — both fixes validated

| metric | before (first run, old arcade) | after (this run) |
|---|---|---|
| Blast | ~1,450–1,467 TPS | **1,500 TPS, 0 failed, 0 backpressure** (212,800 txs) |
| SEEN-delivery gap (post-blast) | **~52 % stuck**, flat forever (dropped) | **5 / 214,957 (~0.002 %)**, fully drained |
| Trace 1 (create → SEEN_MULTIPLE_NODES) | stalled at the 202 — SEEN never delivered | **complete** end-to-end |
| MINED apply — recv→apply lag | **~16 minutes** | **~1–3 ms** |
| MINED apply — per batch | 512 txs / **~18–20 s** (~28/s, scan-bound) | ~50 txs / **~1–3 ms** |
| MINED burst throughput | ~28/s (toolbox-bound) | **~6,500/s** (bound by arcade's emit rate, not the apply) |

**SEEN delivery.** During the blast `no_status` rode ~40 % (in-flight apply lag
under the 1500-TPS × 3-status firehose), then drained `50,005 → 14,244 → 5`
within ~60 s of the blast winding down and held. Nothing was dropped — vs the
first run where ~52 % (~111k) were dropped by arcade's 64-slot buffer and never
recovered. The keeper's fan-out also no longer stalled on promote-on-SEEN.

**Trace 1** (sampled tx, mid-run):

| hop | Δ |
|---|---|
| broadcast POST → 202 RECEIVED | ~131 ms (arcade RTT) |
| SEEN_ON_NETWORK — recv → applied | apply ~42 ms |
| SEEN_MULTIPLE_NODES — recv → applied | apply ~35 ms |

**MINED apply.** After one block mined ~101,555 of the backlog, the `mined_batch`
traces show batches applying in **~1–3 ms** (`recv_min ≈ apply_start ≈
apply_done`) vs the old ~18–20 s per 512-batch — a ~1000× per-tx improvement,
recv→apply lag ~16 min → ~1–3 ms. The apply is no longer on the critical path
for either SEEN or MINED.

## The new gate is downstream (arcade / merkle-service / chaintracks)

With the toolbox off the critical path, the remaining limits are all cluster-side:

- **merkle-service block processing.** A 215k-tx block ran the subtree-workers at
  ~87 cores (16–19 each) for ~30–45 s before arcade could emit MINED, and
  teranode's block assembly put only ~101.5k of the 215k in one block (the rest
  stay SEEN for the next block). Block-processing throughput, not the toolbox.

- **chaintracks ↔ teranode fork at height 764 (found this run).** chaintracks's
  active 764 (`12114cd0…`) diverged from teranode/arcade's canonical 764
  (`5a55997b…`, which chaintracks holds only as an orphan) — a regtest
  competing-block artifact after chaintracks's rough from-genesis re-sync. Arcade
  builds every 764 proof against `5a55997b`, so the toolbox — correctly — refuses
  to store proofs it cannot verify against its trusted headers (chaintracks),
  leaving ~92,700 mined txs stuck at SEEN locally and the proof-poll re-verifying
  them futilely. This is the SPV trust anchor working as designed; it auto-heals
  the instant chaintracks converges to teranode's chain.

- **~13 txs stuck at RECEIVED in arcade** (0.006 %) — teranode never advanced
  them past the 202 (never SEEN/propagated). The toolbox has no status to apply.
  Upstream intake → mempool drop.

## Toolbox follow-ups (robustness, not correctness)

- **Proof poll should not hot-retry unverifiable proofs indefinitely.** When a
  proof consistently fails verification against a *present, stable* header at its
  height (a fork / headers-source disagreement), back off and surface an operator
  alert instead of re-verifying every cycle (92,700 warnings/round observed
  during the 764 fork).
- **`FailAbandoned` should reap never-SEEN txs** after the reservation TTL so a
  perpetually-RECEIVED tx (P2) cannot leak its reserved inputs.

## Bottom line

Fix A (arcade SSE buffer + coalesce) and Fix B (toolbox MINED apply by outpoint)
together close the two bottlenecks from the first run: the ~52 % SEEN-delivery
loss is gone, and MINED apply drops from a ~16-minute lag to milliseconds. At
1500 TPS with 0 failures the toolbox is no longer the binding constraint on
either the SEEN or the MINED path — the remaining levers are all cluster-side
(merkle-service block processing; chaintracks fork-choice / sync fidelity;
arcade intake→mempool delivery).
