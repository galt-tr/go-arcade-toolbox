# go-arcade-toolbox

`go-arcade-toolbox` is a from-zero, **arcade-only**, [BRC-100](https://brc.dev/100)-compatible
BSV wallet toolbox built for high throughput, with pluggable UTXO-storage
backends (SQLite, PostgreSQL, Aerospike). It is a rewrite of
[go-wallet-toolbox](https://github.com/bsv-blockchain/go-wallet-toolbox) whose
public API is kept compatible so an existing user migrates by **changing the
import path** (see [the migration guide](docs/migration-from-go-wallet-toolbox.md)).

"Arcade-only" means the toolbox talks to exactly one transaction-truth oracle —
an [Arcade](https://github.com/bsv-blockchain/arcade) node — plus a ChainTracks
headers service, and nothing else. There is no block explorer, no exchange-rate
feed, no script-history lookup, and **no UTXO discovery**. A wallet learns about
its coins only from transactions it created and from `InternalizeAction`. The
practical consequence is stated loudly up front: **operational backup of the
wallet database is a correctness requirement — there is no restore-from-seed.**
See [operations](docs/operations.md).

> **New here?** Read the [**Getting started guide**](GETTING_STARTED.md) — wallet
> creation, baskets, dependencies, BRC-100, the public Arcade instances for each
> network, and example applications. Building with a coding agent? Point it at
> [**AGENTS.md**](AGENTS.md).

## Three sources of truth

The toolbox composes three independent sources of truth; none is trusted to do
another's job:

| Source | Package | Answers |
|---|---|---|
| **Local UTXO ledger** | `pkg/storage` + `pkg/utxostore` | "Which coins do I control, and are they spendable?" |
| **Arcade tx-oracle** | `pkg/arcade` | "Did this transaction broadcast, and what is its lifecycle status?" |
| **ChainTracks headers** | `pkg/headers` | "What is the chain of block headers?" — used to verify merkle proofs **locally** |

Merkle-root verification (SPV) is performed locally against ChainTracks headers;
the header service is trusted only to return the correct header for a height, and
the "does this proof's root match the chain?" decision stays on our side of the
wire.

## Quickstart

The runnable, tested version of this snippet is
[`examples/quickstart`](examples/quickstart) — run it with no external services:

```sh
go run ./examples/quickstart
```

The shape of building and using a wallet (see
[`examples/internal/demoenv`](examples/internal/demoenv/demoenv.go) for the full,
compiling wiring):

```go
// 1. Wire the two external services. In production these URLs point at a real
//    Arcade deployment; here they are in-process mocks.
oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arcadeURL, EventsURL: arcadeURL})
hdrs, err := headers.New(logger, defs.ChainTracks{Enabled: true, URL: chaintracksURL})

// 2. Build a storage provider over SQLite (Mode A: metastore + utxostore share
//    one file), then migrate it once.
provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
    Backend:     perfprovider.BackendSQLite,
    SQLitePath:  "./wallet.db",
    Network:     defs.NetworkTestnet,
    StorageName: "my-wallet",
}, oracle, hdrs)
_, err = provider.Migrate(ctx, "my-wallet", storageIdentityKeyHex)

// 3. Build the BRC-100 wallet. The key source may be a hex string, a WIF, an
//    *ec.PrivateKey, or a *sdk.KeyDeriver.
w, err := wallet.New(defs.NetworkTestnet, walletPrivateKeyHex, provider,
    wallet.WithServices(services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(defs.NetworkTestnet))),
    wallet.WithLogger(logger),
)

// 4. Send a payment: fund + sign (real BRC-29) + broadcast in one call.
res, err := w.CreateAction(ctx, sdk.CreateActionArgs{
    Description: "a payment",
    Outputs: []sdk.CreateActionOutput{{
        LockingScript:     lockingScript, // []byte
        Satoshis:          1000,
        OutputDescription: "to a counterparty",
    }},
    Options: &sdk.CreateActionOptions{SignAndProcess: boolPtr(true)},
}, "my-app.example.com")

balance, err := w.Balance(ctx) // spendable satoshis in the change basket
```

`CreateAction`, `SignAction`, `InternalizeAction`, `ListOutputs`, and the rest
take the **go-sdk** `github.com/bsv-blockchain/go-sdk/wallet` argument types
(imported as `sdk`), which is what keeps the public surface byte-compatible with
go-wallet-toolbox.

A fresh wallet has **zero** spendable balance; fund it by receiving a payment via
`InternalizeAction` — see [`examples/internalize`](examples/internalize).

## Minimal config (remote storage server)

`cmd/storage-server` hosts a provider behind the REST `/storage/v1` API. A minimal
`config.yaml` (full surface in
[`cmd/storage-server/config.example.yaml`](cmd/storage-server/config.example.yaml)):

```yaml
network: test                 # main | test | ttn | tstn
http_address: ":8100"
backend: sqlite               # sqlite | postgres | aerospike-hybrid
sqlite_path: ./storage.db
arcade:
  enabled: true
  url: "https://arcade.example.com"
chaintracks:
  enabled: true
  url: "https://chaintracks.example.com"
monitor_enabled: true         # run the SSE apply pipeline + reject→release reconciler
```

## Backend matrix

| Backend | Package | Selection | Deployment mode | Role |
|---|---|---|---|---|
| **SQLite** | `pkg/utxostore/sqlstore` | exact best-fit | Mode A (shared DB) | dev / single-node baseline |
| **PostgreSQL** | `pkg/utxostore/sqlstore` | exact best-fit (`FOR UPDATE SKIP LOCKED`) | Mode A (shared DB) | durable single-node production |
| **Aerospike (hybrid)** | `pkg/utxostore/aerostore` | approximate bucket | Mode B (split stores) | highest-concurrency inventory + PostgreSQL metadata |

All three pass the same `utxostore` conformance suite. The metadata always lives
in a SQL metastore (SQLite or PostgreSQL); Aerospike backs only the hot-path UTXO
inventory. See [storage](docs/storage.md).

## Documentation

- [**Getting started**](GETTING_STARTED.md) — the front door: wallet creation, baskets, dependencies, BRC-100, Arcade and the public instances per network, and example applications.
- [**AGENTS.md**](AGENTS.md) — the same ground for coding agents: canonical wiring, API contracts, the traps, and verification commands.
- [Architecture](docs/architecture.md) — the three sources of truth, package map, write-path and async status lifecycle, trust model, what was removed and why.
- [Storage](docs/storage.md) — the `WalletStorageProvider` contract, multi-user model, the pluggable `utxostore`, Mode A/B, the spendability seam, and how to add a backend.
- [Arcade integration](docs/arcade-integration.md) — status lifecycle, EF broadcast semantics, the SSE contract, and the reject→release reconciler.
- [Reject→release vs unfail](docs/reject-release-vs-unfail.md) — why the automatic verified reconciler replaces manual unfail, and the guards that make it safe.
- [High-throughput guide](docs/high-throughput-guide.md) — the fuel-pool workflow, config tuning, the **measured** throughput/durability tradeoff, and a failure-mode playbook.
- [Application throughput playbook](docs/application-throughput-playbook.md) — the application author's companion: what limits you and in what order, and how to measure honestly.
- [Rejection hardening audit](docs/rejection-hardening-audit.md) — every way a client can get a transaction rejected, and what the library can prevent or explain locally.
- [Aerospike value review](docs/aerospike-value-review.md) — whether Mode B is earning its keep.
- [Migration from go-wallet-toolbox](docs/migration-from-go-wallet-toolbox.md) — import-path rewrite, behavior deltas, dropped features, config migration.
- [Operations](docs/operations.md) — **backup is a correctness requirement**, per-backend backup/restore, monitoring, and running the storage server.
- [Benchmarks](docs/benchmarks/README.md) — the raw measured numbers.
- [Examples](examples/) — runnable programs.

## Status

`v0.1.0`: API-compatible with go-wallet-toolbox; three storage backends; the
automatic verified reject→release reconciler; a measured single-node write-path
throughput of ~575-646 TPS durable (1000+ TPS with relaxed durability or
scale-out). Documented follow-ups: multi-Arcade HA, BRC-103/104 mutual auth, a
dedicated wallet-signable fuel basket, the streaming storage-to-storage sync, and
group-commit.
