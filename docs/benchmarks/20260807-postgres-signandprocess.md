# Benchmark: PostgreSQL (Mode A) (postgres, signandprocess mode)

_Postgres Mode A (shared SQL) — bounded perf run_

Generated: 2026-08-07 03:53:49 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **211.2** |
| Target TPS | 1000 |
| % of target | 21.1% |
| Total ops (measured) | 12672 |
| Measured window | 60.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| end-to-end | 12672 | 97.47 | 155.51 | 248.34 | 434.29 | 105.12 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 23699 |
| Ops succeeded | 19393 |
| Claim-contention retries | 117585 |
| Deadlock retries | 0 |
| Contention failures (retries exhausted) | 4242 |
| Deadlock failures (retries exhausted) | 0 |
| Other errors | 63 |

## Async loop (monitor + SSE maturation)

| Metric | Value |
|---|---:|
| Monitor enabled | true |
| Auto-miner enabled | true |
| MINED frames emitted | 19393 |
| Actions total (sampled) | 19424 |
| Actions completed (mined) | 10000 |
| Maturation % | 100.0% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 231.5 |
| 10 | 212.0 |
| 20 | 203.5 |
| 30 | 210.6 |
| 40 | 202.3 |
| 50 | 207.3 |
| 60 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 9 | 3957236175 |
| 19 | 3984268847 |
| 28 | 3982074940 |
| 38 | 3980079738 |
| 47 | 3979132429 |
| 56 | 3976165759 |
| 66 | 3974232716 |

## Environment

| Field | Value |
|---|---|
| Host | fedora |
| CPU | 13th Gen Intel(R) Core(TM) i9-13900K |
| Logical cores | 32 |
| RAM | 62.6 GiB |
| OS / Arch | linux/amd64 |
| Kernel | 7.1.5-100.fc43.x86_64 |
| Go | go1.26.3 |
| Podman | podman version 5.8.4 |

## Run configuration

| Knob | Value |
|---|---|
| Workers | 64 |
| Target TPS | unbounded (max) |
| Duration | 60s (+15s warmup) |
| Pool size | 4000 coins |
| Denomination | 1000000 sats |
| Payment | 1000 sats |
| Max DB conns | 72 |
| Network | test |

## Notes

- TIERED PATH ONLY: these numbers measure the bounded tiered (privacy) funding path. Plain wallet.CreateAction always funds from the change basket with Denomination=0; the denominated fuel-pool ClaimExact fast path that the 1000-TPS design targets is NOT YET wired to CreateAction (tracked as a follow-up) and is expected to be substantially higher. Do not read this sustained TPS as the design ceiling — the Aerospike hybrid here already shows 0 claim contention.
- Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.
- Recycling is implicit: each payment's change re-enters the change basket ('default') and is re-selected by subsequent claims (mined-first, then unproven). This exercises real claim contention.
- Contention counts are HIGH-VARIANCE run to run (SKIP-LOCKED collisions depend on scheduling); observed anywhere from ~0 to tens of thousands of retries at near-identical config. Do not over-read a single run's contention figure.
- otherErrors is the residual bucket: write-path errors not matched as contention (contention/conflict/not-enough-funds/insufficient) or deadlock (deadlock/serialization/40001/40P01) — e.g. transient BEEF-assembly or reference/timeout errors, including ops interrupted at shutdown. Typically <0.5% of ops here and not individually root-caused.
- The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.
- Single-call mode: one CreateAction does create+sign+process+broadcast; only the end-to-end phase is timed (fewest round trips). Fewer round trips is only plausibly ~2x on the SQL/hybrid backends; on write-serialized SQLite it can be marginally slower.

