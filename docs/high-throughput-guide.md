# High-throughput guide

This is the guide to pushing write-path throughput on `go-arcade-toolbox`. It is
honest about what the toolbox delivers today, and every number here is
**measured** (see [`docs/benchmarks`](benchmarks/README.md)) — nothing is
aspirational.

## The headline, stated honestly

On a 32-core i9-13900K with containerized PostgreSQL/Aerospike (rootless podman):

- **Strict per-op durability (`fsync=on`, `synchronous_commit=on`): ~575 TPS
  (PostgreSQL) / ~646 TPS (Aerospike hybrid) per node**, at 256 workers. This is
  the single-node durable ceiling. It is **latency-bound on the per-`CreateAction`
  ACID commit** — not on the claim (0 contention on the fuel-pool path) and not on
  sign/broadcast.
- **Relaxed durability (`synchronous_commit=off`): 1379 TPS at just 64 workers** —
  a clean 3.5× over the same-config durable run (393.8 TPS). This is the proof
  that the ceiling is the durable commit floor, not CPU or the storage logic.
- **To exceed 1000 TPS *durably*: scale out (N nodes × ~575-645 TPS) or add
  group-commit** (amortize `fsync` across ops). `synchronous_commit=off` is a
  legitimate middle ground — a bounded, last-few-milliseconds durability window —
  that already clears 1000 TPS on a single node.

So: **do not claim an unqualified 1000 TPS.** The toolbox reaches 1000+ TPS under
relaxed durability or via horizontal scale-out; it does ~600 TPS/node with strict
durability.

### The worker / connection sweep (durable, fuel-pool path)

| Backend | Workers | Pool | Conns | Sustained TPS | e2e p50 | e2e p99 |
|---|---:|---:|---:|---:|---:|---:|
| PostgreSQL Mode A | 64 | 36,000 | 72 | 393.8 | 150 ms | 417 ms |
| PostgreSQL Mode A | 128 | 60,000 | 144 | 473.9 | 254 ms | 602 ms |
| PostgreSQL Mode A | 256 | 72,000 | 272 | **575.7** | 405 ms | 989 ms |
| Aerospike hybrid Mode B | 64 | 36,000 | 72 | 382.7 | 149 ms | 474 ms |
| Aerospike hybrid Mode B | 128 | 60,000 | 144 | 489.7 | 228 ms | 736 ms |
| Aerospike hybrid Mode B | 256 | 72,000 | 272 | **645.6** | 349 ms | 1407 ms |

Both scale **sub-linearly** while e2e latency grows ~linearly with worker count —
the signature of a saturated resource (the durable commit) where extra workers
buy queueing, not throughput. Pushing past 256 workers only inflates the tail.

## Two funding paths

`CreateAction` funds a payment one of two ways, selected by the storage
provider's `UTXOManagement` strategy (`pkg/defs/utxo_management.go`):

- **Tiered (privacy)** — `Strategy=privacy` (the default). A bounded best-fit
  claim from the change basket (`FOR UPDATE SKIP LOCKED` on SQL). Randomized
  change for privacy. **Contention-bound** under high concurrency: on the
  PostgreSQL single-call path at a small pool it collapsed to **117,585
  claim-contention retries and an 18.2% op-failure rate** — 64 workers fighting
  over the smallest-sufficient coin.
- **Throughput (fuel-pool)** — `Strategy=throughput`. `CreateAction` funds via
  the funder's closed-form **`ClaimExact`** over a pool of fixed-denomination
  coins. Because every pool coin is interchangeable, the `SKIP LOCKED` claim never
  collides: **every throughput run shows exactly 0 claim-contention retries and
  ~0.5% op-failure**, deterministically, on both backends. This is the high-TPS
  route.

The structural win of the fuel pool is **reliability, not raw median TPS**: at an
equal pool size both paths land in the same ~150-230 TPS band on this box (both
are create-phase bound), but the fuel pool removes the 18% collapse. The Aerospike
hybrid gains most from it — `ClaimExact` cuts the hybrid's `create` p50 from
319 ms (tiered) to 81 ms.

> Where is the bottleneck once claim contention is gone? The **`create` phase**
> (fund + reserve + persist), on every backend and both paths — its *tail* caps
> throughput (create p95 ≈ 1.1-1.3 s even at create p50 ≈ 85 ms). `sign_process`
> is 3-6× cheaper at the median with a far tighter tail. The cost is DB round
> trips in the funding/persist path — **not** signing and not the (in-process,
> instant) broadcast.

## The fuel-pool workflow

1. **Size the pool.** From your workload the config derives the coin denomination
   and the pool target:

   ```go
   um := defs.DefaultUTXOManagement()
   um.Strategy = defs.StrategyThroughput
   um.Throughput.ExpectedTxSizeBytes = 400        // signed size incl. the fuel input
   um.Throughput.ExpectedOutputSatoshis = 0       // pure fee/data action
   um.Throughput.TargetTPS = 1000
   um.Throughput.ExpectedConfirmationSeconds = 300
   um.Throughput.SpendPolicy = defs.SpendPolicyPreferMined

   denom, _ := um.Throughput.Denomination(defs.DefaultFeeModel(), defs.Commission{})
   target := um.Throughput.TargetPool() // ceil(target_tps × confirmation × headroom)
   ```

   - **Denomination** = the fee for the expected signed size + the expected output
     satoshis (floored at the marginal fuel-input fee). One `ClaimExact` should
     claim exactly one coin per payment (`n=1`), keeping change minimal.
   - **Pool target** = `target_tps × expected_confirmation_seconds ×
     pool_headroom_factor` (default headroom 1.5). The pool must be big enough to
     absorb every claim during the window a freshly-minted coin takes to confirm.
   - **`SpendPolicy`** controls which tiers the funder may claim from:
     `mined_only`, `prefer_mined` (default), or `any` (also spend still-sending
     coins).

2. **Wire it into storage** via `storage.WithUTXOManagement(um)` (through
   `perfprovider.Config.ExtraOptions`, or `storage.New(...)` directly).

3. **Provision the pool** with `FanOutFuel`, which mints a batch of equal-value
   fuel coins in one transaction, and keep it topped up with the **fuel keeper**:

   ```go
   cfg := fuelkeeper.FromThroughput(um.Throughput, denom)
   cfg.Originator = "my-app.example.com"
   keeper, _ := fuelkeeper.New(w, cfg, logger)
   go keeper.Run(ctx) // refills when the pool drops below the low-water mark
   ```

   The keeper holds the operator's keys and runs **client-side**, not in the
   server monitor. See [`examples/highthroughput`](../examples/highthroughput).

### Follow-up caveat: the dedicated fuel basket

The `ClaimExact` funding path is wired and benchmarked, but **provisioning a
dedicated, wallet-signable pool basket through the public wallet API is not
finished.** Storage does not yet honor `Options.FuelShape`, so `FanOutFuel`
currently mints its outputs as ordinary change into the `default` basket rather
than into a separate pool basket. The benchmarks size the pool in the `default`
basket as a result, which is a faithful end-to-end exercise of `ClaimExact` but
pays a per-op cost the dedicated basket would remove. Closing this — shaped change
carrying derivation material into the pool basket — is a documented follow-up
(see the gap analysis in [the benchmarks README](benchmarks/README.md)).

## Config tuning knobs

| Knob | Where | Effect |
|---|---|---|
| **Funding strategy** | `defs.UTXOManagement.Strategy` | `throughput` removes claim contention (0 retries); `privacy` randomizes change but contends under load. |
| **Denomination** | `Throughput.Denomination` (derived, or `DenominationSatoshis` to override) | Right-size so one `ClaimExact` claims one coin per payment. |
| **Pool size** | `Throughput.TargetPool` (derived, or `TargetPoolSize`) | Must outlast the confirmation window under load; throughput change is *not* recycled, so the pool drains ~1 coin/op. |
| **Workers** | your concurrency | Throughput scales sub-linearly to the durable-commit ceiling; extra workers past ~256 buy tail latency, not TPS. |
| **Connection pool** | `perfprovider.Config.MaxDBConns` | The shared SQL pool serves workers **and** the monitor. Too low → workers queue on connections; too high → PostgreSQL context-switches. Benchmarks used `conns ≈ workers + a margin`. |
| **Mode A vs B** | how the stores are wired | Mode A (shared SQL) commits both stores atomically; Mode B (Aerospike hybrid) offloads the claim to Aerospike (0 contention) at the cost of two coordinated writes per op — it edges past PostgreSQL at high concurrency. |
| **Durability** | PostgreSQL `synchronous_commit` / `fsync` | The single biggest lever: relaxing it is a 3.5× jump. See below. |
| **Delayed broadcast** | `sdk.CreateActionOptions.AcceptDelayedBroadcast` | Decouples the synchronous broadcast from the create call to shorten the per-op critical path (a lever for cutting the create tail). |

### The durability tradeoff (the biggest lever)

Same 64-worker config, only `synchronous_commit`/`fsync` flipped:

| PostgreSQL 64w | Sustained TPS | e2e p50 | e2e p99 |
|---|---:|---:|---:|
| Durable (`fsync=on`, `synchronous_commit=on`) | 393.8 | 150 ms | 417 ms |
| Relaxed (`synchronous_commit=off`, `fsync=off`) | **1379.0** | 45 ms | 108 ms |

`synchronous_commit=off` keeps crash-safety of the database structure but opens a
bounded window (a few hundred ms) in which the last committed transactions can be
lost on an OS/hardware crash. For a wallet where the DB is the only record of your
coins, weigh this against your backup posture (see [operations](operations.md)).
The durable path to 1000+ TPS is **group-commit** (amortize the `fsync` across
concurrent commits — a follow-up) or **horizontal scale-out**.

## Hardware notes

The measured numbers were produced on an **i9-13900K (32 logical cores),
62.6 GiB RAM, Fedora, Go 1.26.3**, with PostgreSQL/Aerospike in rootless podman
containers on the same box, PostgreSQL tuned with `shared_buffers=2GB`,
`max_connections=400`. Broadcasts hit an in-process mock Arcade, so the numbers
measure **storage + wallet** cost, not network round trips to a real Arcade —
plan additional headroom for real broadcast latency and its backpressure. The
claim path is index-bound, not CPU-bound.

## Failure-mode playbook

| Symptom | Likely cause | What to do |
|---|---|---|
| `ErrNotEnoughFunds` in throughput mode despite deposits | The denominated pool is empty or underflowing (throughput change is not recycled — the pool drains ~1 coin/op). | Ensure the fuel keeper is running and the pool target outlasts the confirmation window; size the pool `> peak_TPS × (mint-confirmation time)`. |
| Op-failure rate spikes; claim-contention retries in the tens of thousands | You are on the **tiered (privacy)** path under high concurrency, colliding on the smallest-sufficient coin. | Switch to `Strategy=throughput`, or raise the pool-to-worker ratio / lower worker count. |
| Throughput flat while e2e latency climbs with every added worker | You have hit the **durable-commit ceiling** (create-phase bound). | Relax durability (`synchronous_commit=off`), scale out to more nodes, or wait for group-commit. Adding workers past ~256 only grows the tail. |
| Change/coins slow to become spendable; low maturation % | The monitor's SSE apply pipeline is lagging (it promotes coins to `TierMined` as proofs arrive). | Check the monitor is running and consuming the status SSE stream; check the shared connection pool isn't starving the monitor behind the workers. |
| Reconciler backlog / rising `reconciler_stuck_total` | Suspects can't be verified (Arcade unreachable, or ambiguous competitors with unreadable rawTx). | Check Arcade connectivity/circuit-breaker state; `stuck` suspects are operator-visible and never auto-released — investigate them. See [arcade-integration](arcade-integration.md#the-rejectrelease-reconciler). |

## Reproduce the numbers

```sh
DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true \
PERF_THROUGHPUT=1 PERF_MODES="signandprocess" \
PERF_DURATION=60s PERF_WARMUP=5s PERF_WORKERS=64 PERF_POOL=36000 PERF_MAX_DB_CONNS=72 \
  go test -tags perf -run TestPerf_PostgresModeA -timeout 30m -v ./test/perf/...
```

Or via the CLI (`cmd/perfrunner`, add `-throughput` for the fuel-pool path). Full
commands and the complete result set are in [the benchmarks
README](benchmarks/README.md).
