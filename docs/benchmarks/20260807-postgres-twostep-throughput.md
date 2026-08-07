# Benchmark: PostgreSQL (Mode A) (postgres, twostep mode)

**Funding path: throughput (ClaimExact fuel pool)**

_Postgres Mode A (shared SQL) — bounded perf run_

Generated: 2026-08-07 04:34:48 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **183.5** |
| Target TPS | 1000 |
| % of target | 18.3% |
| Total ops (measured) | 11008 |
| Measured window | 60.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| create (fund+reserve+persist) | 11008 | 85.26 | 1178.83 | 1857.80 | 3613.68 | 262.46 |
| sign_process (sign+broadcast+commit) | 11008 | 72.52 | 176.86 | 331.79 | 4072.96 | 89.40 |
| end-to-end | 11008 | 172.11 | 1282.15 | 1981.74 | 4928.28 | 351.85 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 11389 |
| Ops succeeded | 11325 |
| Claim-contention retries | 0 |
| Deadlock retries | 0 |
| Contention failures (retries exhausted) | 0 |
| Deadlock failures (retries exhausted) | 0 |
| Other errors | 64 |

## Async loop (monitor + SSE maturation)

| Metric | Value |
|---|---:|
| Monitor enabled | true |
| Auto-miner enabled | true |
| MINED frames emitted | 11325 |
| Actions total (sampled) | 11434 |
| Actions completed (mined) | 5021 |
| Maturation % | 50.2% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 66.2 |
| 10 | 64.5 |
| 20 | 64.7 |
| 30 | 147.2 |
| 40 | 408.8 |
| 50 | 349.4 |
| 60 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 8 | 35999462987 |
| 16 | 35997916803 |
| 24 | 35994385904 |
| 33 | 35986859081 |
| 41 | 35992395436 |
| 49 | 35934461735 |
| 57 | 35935261056 |

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
| Funding path | throughput (ClaimExact fuel pool) |
| Workers | 64 |
| Target TPS | unbounded (max) |
| Duration | 60s (+5s warmup) |
| Pool size | 36000 coins |
| Denomination | 1000000 sats |
| Payment | 1000 sats |
| Max DB conns | 72 |
| Network | test |

## Notes

- FUEL-POOL PATH: the provider runs with UTXOManagement.Strategy=throughput, so each worker's wallet.CreateAction funds via the funder's closed-form ClaimExact fast path (FundArgs.Denomination>0) over a denominated pool — no tiered SKIP-LOCKED best-fit scan. This is the 1000-TPS design's funding route (Task 27 wiring).
- POOL BASKET = 'default' (a measurement choice, not the production layout): the pool must hold wallet-SIGNABLE coins so every op can sign+broadcast through the real wallet, and the only public API that mints BRC-29-signable coins (InternalizeAction wallet-payment) books them into the default basket. ClaimExact selects strictly by (basket, tier, satoshis==denomination), so the non-denominated change that also lands in 'default' is invisible to the fast path. A dedicated 'fuel' basket would need shaped-change minting (FanOutFuel/FuelShape) which storage does not yet implement — see the benchmark README gap analysis.
- NO RECYCLING: unlike the tiered path, each op's change is NOT re-claimed (it is not denomination-sized), so the pool strictly drains ~1 coin per op. The pool is sized to outlast warmup+duration; if ClaimExact ever underflowed it would fall back to the tiered walk over 'default' (visible as a contention/not-enough-funds spike). A clean run shows ~0 contention and ~0 not-enough-funds.
- Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.
- otherErrors is the residual bucket: write-path errors not matched as contention or deadlock — e.g. transient BEEF-assembly or reference/timeout errors, including the one in-flight op per worker interrupted at shutdown. Typically <0.5% of ops and not individually root-caused.
- The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.
- Two-step mode: 'create' = CreateAction (fund+reserve+persist); 'sign_process' = SignAction (sign+broadcast+commit). Finer broadcast/commit split is not separable at the wallet API boundary.

