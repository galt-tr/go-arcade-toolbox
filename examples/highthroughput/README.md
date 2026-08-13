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
- **The 2026-08-07 benchmark numbers predate the dedicated fuel basket.** Those
  runs sized the pool in the `default` basket because `FanOutFuel` could not yet
  provision a separate one. Storage now honors `Options.FuelShape`
  (`pkg/storage/create.go:357-377`), minting wallet-signable denominated coins
  straight into the pool basket, so those figures are a floor for the denominated
  path rather than a ceiling.
