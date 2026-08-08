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

## Next (to raise / find the true PG ceiling)

1. **Batch the status-apply DB round-trips** — bulk-load the batch's `known_txs`
   in one query and collapse the per-event UPDATEs into per-batch bulk
   statements (all `SEEN` → one status update + one promote; all `MINED` → bulk
   proof/complete/promote/remove). Cuts ~10k round-trips/s to hundreds and
   unblocks the SSE reader. This is the "squeeze PG" lever.
2. Then re-measure the sustained ceiling on a fresh DB and record it here.
3. Then the **Aerospike hot-path** (design's intended 1000+ path): move UTXO ops
   off PG so create + apply + recycle stop contending on one database.
