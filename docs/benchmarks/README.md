# Benchmarks — write-path throughput

> **Read this first.** These numbers measure the **TIERED (privacy) funding
> path** — the only path a plain `wallet.CreateAction` takes today, funding from
> the change basket with `Denomination=0` via the bounded tiered claim. **The
> denominated fuel-pool `ClaimExact` fast path that the 1000-TPS design targets
> is NOT YET wired to `CreateAction`** (`pkg/storage/create.go` never sets
> `FundArgs.Denomination`); that wiring is a tracked follow-up. So **~200 TPS is
> not the design ceiling** — the fuel-pool path is expected to be substantially
> higher (the Aerospike hybrid already shows *zero* claim contention here, i.e.
> its ceiling is latency, not the tiered-claim hotspot). Do not read these
> figures as the toolbox's throughput limit.

Captured throughput of the full BRC-100 wallet write path
(`CreateAction → SignAction → ProcessAction/broadcast`), produced by the
performance harness (`internal/perf`, `cmd/perfrunner`, `test/perf`). Each
per-run report renders deterministically from its sibling JSON:

```sh
go run ./cmd/perfrunner render docs/benchmarks/20260807-postgres-twostep.json
```

## Results (this box: i9-13900K, 32 logical cores, 62.6 GiB, Fedora 7.1.5, Go 1.26.3)

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
  backend; `sign_process` is 3–6× cheaper. The cost is DB round trips in the
  funding/claim/persist path, not signing or the (instant, in-process) broadcast.
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

Neither backend reaches 1000 TPS on the tiered path. What closes the gap,
roughly in order of leverage:

1. **Wire the denominated fuel-pool `ClaimExact` funding path to `CreateAction`
   (tracked follow-up).** This is the *designed* high-throughput route: a single
   closed-form exact claim over a denominated pool, with no tiered `SKIP LOCKED`
   scan — which is exactly the Postgres contention hotspot above. This is the
   structural win for both TPS and contention and is why ~200 is not the ceiling.
2. **More workers + connection-pool tuning + a larger pool-to-worker coin
   ratio.** Latency-bound throughput scales with concurrency until the store
   saturates; the hybrid has contention headroom to add workers now, and
   Postgres needs a bigger pool relative to workers to stop the claim collisions
   from worsening.
3. **Delayed-broadcast / batched processing** to cut per-op synchronous DB work.
4. **Denomination sizing** so one claim funds one payment (minimal change, no
   multi-input gather), shortening the funding path.
5. **Single-call mode** helps only where the claim path is *not* the bottleneck
   — a modest lever on the tiered path (see above), potentially larger once the
   fuel-pool path removes the contention ceiling.

## How these were produced

```sh
# Container-backed capture (podman); PERF_MODES runs both modes reusing the
# containers. Writes JSON to ./perf-results/:
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true \
PERF_MODES="twostep signandprocess" \
PERF_DURATION=60s PERF_WARMUP=15s PERF_WORKERS=64 PERF_POOL=4000 PERF_MAX_DB_CONNS=72 \
  go test -tags perf -run TestPerf_PostgresModeA -timeout 25m -v ./test/perf/...

# Or a full-length run via the CLI (SQLite self-contained; PG/Aero via flags):
go run ./cmd/perfrunner -backend postgres -pg-dsn "postgres://..." \
  -workers 64 -duration 5m -warmup 30s -pool-size 4000 -max-db-conns 72 \
  -mode signandprocess

# Render any result JSON to Markdown:
go run ./cmd/perfrunner render -o report.md perf-results/<run>.json
```

The CI floor (a short bounded run, two-step only by default, conservative TPS
assertion) lives in `test/perf` behind the `perf` build tag; the headline
numbers above come from the longer manual runs.
