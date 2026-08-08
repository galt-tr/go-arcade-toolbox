# End-to-end app-blast throughput — PostgreSQL (2026-08-08)

Measured with the `toolbox-app-arcade` self-payment blaster against **real arcade
(tstn)** + real ChainTracks, PostgreSQL 17 storage (metadata + `utxostore`
hot path in the same DB, Mode A). This is the full path — `wallet.CreateAction`
→ sign → immediate broadcast → async SSE status apply — not the storage-direct
`perfrunner` numbers (those measure the storage layer alone at ~2000–2500 TPS).

## Environment

- PostgreSQL tuned: `shared_buffers=16GB`, `effective_cache_size=48GB`,
  `max_wal_size=32GB`, `wal_compression=on`, `checkpoint_timeout=15min`,
  `synchronous_commit=off`, aggressive autovacuum, `max_parallel_workers_per_gather=0`.
  62 GB host, rootless podman `postgres:17-alpine`.
- Buyer wallet, `IMMEDIATE_BROADCAST=true`, doubled fuel keeper
  (`FUEL_MINT_CONCURRENCY=64`, `FUEL_STREAM_NO_YIELD=1`), `MAX_DB_CONNS=180`,
  `FEE_SAT_PER_KB=125`, `BLAST_AMOUNT_SATS=50`.

## Results

| metric | result |
|---|---|
| **CREATE (create→sign→broadcast→202)** | **~1000 TPS sustained**, 0 failures, 0 backpressure, on a clean tuned DB (940–1032/s across a 10-min run as the ledger grew 54 MB→4.5 GB) |
| REJECTED | 0 (fee margin clears the EF-basis GoBDK floor) |
| **STATUS-APPLY (SSE `SEEN`/`MINED` → local state)** | **the ceiling** — see below |

### The apply ceiling is the binding constraint for *sustained* throughput

At 1000 TPS, arcade emits **~2000 status events/s** (a `SEEN` and later a
`MINED` per tx). Applying them is where sustained throughput breaks:

- Before the header cache: **~40 TPS** apply (every mined proof re-fetched its
  block header from ChainTracks, ~200 ms, and ~1000 proofs share one block).
- After caching recent headers (`headers.WithCacheDepth(0)`, reorg-evicted):
  a **~353 TPS burst** — now DB-op-bound, not header-fetch-bound.
- With `APPLY_CONCURRENCY=32`: no higher in practice, because the apply pool
  then **contends with `create` and the fuel keeper for the same 180-connection
  pool**. Each event costs a `FindByTxID` SELECT + a transaction of ~5–7
  single-row UPDATEs; at ~2000 events/s that is ~10–14k DB round-trips/s on top
  of the create + recycle load. Something starves — in one run the keeper lost,
  fuel drained, and the blast stalled on backpressure.

Consequence: apply falls behind creation, so `known_txs`/`transactions` and the
`utxos` hot table (spent rows) grow unbounded, and pruning (`RemoveSpentBy`,
`input_beef` drop on mine) — which is mining/apply-paced — cannot keep the
ledger bounded. **create@1000 is real; hours@1000 with a bounded ledger is not,
on PG-only.**

### SSE "slow consumption" — root cause

The SSE path is a single-threaded, synchronous pipeline: `sse.Reader.Run` scans
lines → `dispatchFrame` (json.Unmarshal) → `onEvent`, which does a **blocking
send** on a 1024-slot channel. When the applier can't drain, the channel fills,
`onEvent` blocks, and the reader stops reading the socket. So observed "slow SSE
consumption" is the **downstream apply back-pressuring the reader** — the reader
and JSON parse are not the independent bottleneck; broadcast is fine.

## Fixes shipped (this pass)

- Recent-header cache (`headers` `maybeCache` + `WithCacheDepth`), SPV-safe via
  the existing reorg-evict-before-forward + `DemoteReorgedProofs`.
- `WithApplyConcurrency` (monitor) / `APPLY_CONCURRENCY` (app).
- Drop `input_beef` on mine (both `known_txs` and `transactions`).
- `RemoveSpentBy` — remove a mined tx's spent inputs from the hot store.

## Update — apply-batching shipped (commit 650063f): measured PG ceiling

The status-apply DB round-trips are now batched (`storage.ApplyStatusBatch`):
one bulk `known_txs` load + one Mode-A write transaction of bulk statements per
apply-batch, guards preserved. Re-measured end-to-end (fresh 16 GB-tuned DB,
`BLAST_AMOUNT_SATS=50`):

| metric | before batching | after batching |
|---|---|---|
| CREATE (sustained) | ~1000/s (holds even at 5 GB DB, 0 fail/bp) | ~1000/s (unchanged — not the ceiling) |
| **STATUS-APPLY (SEEN)** | ~40/s | **~500–750/s** (~15× lift) |

**The apply pipeline is still the sustained ceiling.** Even batched, apply
(~500–750 events/s) trails create (~1000/s), so the SEEN backlog grows ~250–500/s
and — because mining-paced pruning cannot fire within a run (tstn mining lag
exceeds the window, `mined=0`) — the ledger grows unbounded, and apply itself
degrades as it grows (~750/s at ~2 GB → ~467/s at ~5 GB). So:

- **Create ceiling: ~1000 TPS** (robust; PG create path + 16 GB buffers handle it).
- **Sustained-with-bounded-ledger ceiling: apply-bound, ~500–750 TPS** (SEEN
  keeping pace; lower once MINED apply + pruning must also keep pace at steady
  state). Batching lifted this ~15× but did not reach create's 1000, because
  create + fuel-keeper + apply all contend on one PostgreSQL.

The single-threaded applier with 64-event batches is the remaining structural
limit; larger batches were inconclusive here (DB-growth confound — needs a fresh
DB per config to isolate). Batch-size tuning is left as a future config knob.

## Next: Aerospike hot-path

Move the UTXO hot store off PostgreSQL (design's intended 1000+ path): create +
apply + fuel-recycle stop contending on one database, freeing PG capacity for
the metadata + status-apply writes. Expected to raise the apply-bound ceiling
toward create's ~1000/s. Re-run this same end-to-end measurement on the hybrid
and record it alongside.
