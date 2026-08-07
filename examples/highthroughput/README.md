# highthroughput

Demonstrates the throughput building blocks: the denominated fuel-pool
configuration, the `FanOutFuel` minting primitive, and the fuel-keeper top-up
loop.

```sh
go run ./examples/highthroughput
```

It is **illustrative**: it runs against the in-process mocks to show the APIs
and print the real derived sizing numbers, but a real high-TPS deployment needs
a tuned PostgreSQL or Aerospike backend and a pre-provisioned pool. See
[`../../docs/high-throughput-guide.md`](../../docs/high-throughput-guide.md).

What it shows:

- **Pool sizing** — `defs.UTXOManagement` with `Strategy=throughput` derives the
  fuel-coin denomination and the target pool size from your expected transaction
  size, target TPS, and confirmation time.
- **`FanOutFuel`** — mints a batch of equal-value fuel coins in one transaction.
- **Fuel keeper** — `fuelkeeper.FromThroughput` + `fuelkeeper.New` build the
  client-side loop that refills the pool when it drops below the low-water mark.

## Honest limitations

- **~575-646 TPS per node with strict per-op durability**; **1379 TPS with
  relaxed durability** (`synchronous_commit=off`). Exceeding 1000 TPS durably is
  a scale-out (N nodes) or group-commit story. Numbers are measured — see
  [`../../docs/benchmarks`](../../docs/benchmarks).
- **The dedicated, wallet-signable fuel basket is a follow-up.** Storage does not
  yet honor `Options.FuelShape`, so `FanOutFuel` currently mints its outputs as
  ordinary change into the default basket rather than into a separate pool
  basket. The denominated `ClaimExact` funding path itself is wired and
  benchmarked; provisioning the pool through the public wallet API is the
  remaining piece (see the gap analysis in the benchmarks README).
