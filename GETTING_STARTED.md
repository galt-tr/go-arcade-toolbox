# Getting started with go-arcade-toolbox

Build a Bitcoin SV application in Go that creates, signs and broadcasts transactions —
with a real wallet, real SPV verification, and a throughput ceiling measured in hundreds
of transactions per second.

All you need to get started is an Arcade instance, and we host those for you.

```sh
go get github.com/galt-tr/go-arcade-toolbox
```

---

## What this is

`go-arcade-toolbox` is a [BRC-100](https://brc.dev/100) wallet you embed in your Go
program as a library. It is **arcade-only**: it talks to exactly one transaction-truth
oracle — an [Arcade](https://github.com/bsv-blockchain/arcade) node — plus a ChainTracks
headers service, and nothing else. No block explorer, no exchange-rate feed, no
script-history lookup.

That buys you a small, fast, predictable system. It also means one thing you must
internalise before you write a line of code:

> **There is no UTXO discovery and no restore-from-seed.** A wallet learns about its
> coins only from transactions it created and from `InternalizeAction`. Backing up the
> wallet database is a *correctness* requirement, not an ops nicety.

If you lose the database, you lose the coins. See [operations](docs/operations.md) for
per-backend backup and restore.

Three independent sources of truth, none trusted to do another's job:

| Source | Answers |
|---|---|
| Local UTXO ledger (`pkg/storage`, `pkg/utxostore`) | "Which coins do I control, and are they spendable?" |
| Arcade tx-oracle (`pkg/arcade`) | "Did this broadcast, and what is its status?" |
| ChainTracks headers (`pkg/headers`) | "What is the chain of block headers?" |

Merkle proofs are verified **locally** against those headers. Arcade is trusted to
deliver a proof, never to tell you it's valid.

---

## Building with an agent

We actively encourage building on this toolbox with a coding agent. The API has sharp
edges that are cheap to avoid and expensive to discover — status names that don't read
the way you'd guess, a broadcast error taxonomy where retrying the wrong class corrupts
your throughput, funding modes that behave differently under load.

**[`AGENTS.md`](AGENTS.md) is written for that.** It's a dense, directive-style companion
to this guide: canonical wiring, API contracts, the traps with file references, and the
commands to verify the result. Point your agent at it:

```
Read AGENTS.md in this repo, then build me a service that ...
```

It works with Claude Code, Cursor, Copilot, or anything else that reads repository
context. The rest of *this* document is for you, the human.

---

## Run something in 60 seconds

The quickstart runs end to end against in-process Arcade and ChainTracks mocks — no
external services, no configuration, no funding:

```sh
git clone https://github.com/galt-tr/go-arcade-toolbox
cd go-arcade-toolbox
go run ./examples/quickstart
```

```
wallet identity (receive) key: 0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798
balance before funding: 0 sat
funded with 100000 sat via tx 448e64fe44678a82b38e68163deccbe8f6f3f9b1f2fb7d8dd1dc9ae4e372c2c4
balance after funding: 100000 sat
sent payment in tx c00a9f46faec506929f6b45e06048bb822f59076a7749432fa4ded360207d1d5
arcade received 1 broadcast(s)
balance after payment: 98953 sat (change from seed - payment - fee)
wallet has 10 spendable output(s):
  ...
```

Two more runnable programs sit alongside it — [`examples/internalize`](examples/internalize)
(receiving an external payment, with real SPV verification) and
[`examples/highthroughput`](examples/highthroughput) (fuel-pool sizing). All three build
with `go build ./examples/...`.

---

## Creating a wallet

Four steps: wire the two services, build storage, migrate once, construct the wallet.
[`examples/internal/demoenv`](examples/internal/demoenv/demoenv.go) is the full compiling
version of this.

```go
// 1. Point at a network. DefaultServicesConfig fills in the Arcade and ChainTracks
//    URLs for you — see the network table below.
cfg := defs.DefaultServicesConfig(defs.NetworkTTN)

oracle := arcade.New(logger, nil, cfg.Arcade)
hdrs, err := headers.New(logger, cfg.ChainTracks)

// 2. Build a storage provider. SQLite here; use PostgreSQL for anything real.
provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
    Backend:     perfprovider.BackendSQLite,
    SQLitePath:  "./wallet.db",
    Network:     defs.NetworkTTN,
    StorageName: "my-app",
}, oracle, hdrs)
defer closeProv(ctx)

// 3. Create the schema. Once per storage instance.
_, err = provider.Migrate(ctx, "my-app", storageIdentityKeyHex)

// 4. Build the BRC-100 wallet.
w, err := wallet.New(defs.NetworkTTN, walletPrivateKeyHex, provider,
    wallet.WithServices(services.New(logger, oracle, hdrs, cfg)),
    wallet.WithLogger(logger),
)
```

The key can be a DER hex string, a `wallet.WIF`, an `*ec.PrivateKey`, or a
`*sdk.KeyDeriver` — whichever your key management already produces.

Every wallet call takes an `originator`: a non-empty, FQDN-shaped string identifying the
calling application (`"my-app.example.com"`). BRC-100 requires it and the toolbox
validates it.

The wallet binds storage lazily. To force the bind and get the wallet's receive key:

```go
pub, err := w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, originator)
receiveKey := pub.PublicKey.ToDERHex()
```

**A fresh wallet has zero balance.** Fund it by receiving a payment — see
[BRC-100 and funding](#brc-100-and-funding) below.

### Sending

```go
res, err := w.CreateAction(ctx, sdk.CreateActionArgs{
    Description: "a payment",
    Outputs: []sdk.CreateActionOutput{{
        LockingScript:     lockingScript, // []byte
        Satoshis:          1000,
        OutputDescription: "to a counterparty",
    }},
    Options: &sdk.CreateActionOptions{SignAndProcess: ptr(true)},
}, originator)
```

`SignAndProcess` funds, signs (real BRC-29 signatures) and broadcasts in one call. Split
it into `CreateAction` + `SignAction` when you need to inspect or externally sign the
transaction in between.

`CreateAction`, `SignAction`, `InternalizeAction`, `ListOutputs` and the rest take
**go-sdk** argument types from `github.com/bsv-blockchain/go-sdk/wallet` — that's what
keeps this surface compatible with `go-wallet-toolbox`.

---

## Baskets

A basket is a named bucket that outputs are booked into. It's how you keep your
application's coins separated by purpose — change here, fuel there, your NFTs somewhere
else.

There is **no `CreateBasket`**. A basket exists as soon as you name one:

```go
Outputs: []sdk.CreateActionOutput{{
    LockingScript: script,
    Satoshis:      1,
    Basket:        "my-tokens",
    Tags:          []string{"series-a"},
}}
```

Three names are well known: `default` (where change lands), `fuel` and `reserve` (used by
the high-throughput funding path).

Reading state:

```go
w.Balance(ctx)                              // spendable sats in "default"
w.BasketBalance(ctx, "my-tokens")           // spendable sats in a named basket
w.BasketClaimableCount(ctx, "fuel")         // how many coins are claimable
w.ListOutputs(ctx, sdk.ListOutputsArgs{Basket: "my-tokens"}, originator)
```

> **One caveat worth knowing up front.** `InternalizeAction` has two protocols. The
> *wallet payment* protocol records BRC-29 derivation material, so the coin is
> spendable. The *basket insertion* protocol can tag any output into a named basket but
> records no derivation material — **those coins are not wallet-signable today**. Use
> basket insertion for tracking, wallet payment for anything you intend to spend.

---

## Dependencies

| Backend | Use it for | Setup |
|---|---|---|
| **SQLite** | development, single-node experiments | none — one file |
| **PostgreSQL** | **anything real** | a database and a DSN |
| Aerospike (hybrid) | highest-concurrency inventory | Aerospike + PostgreSQL |

**Most applications want PostgreSQL.** It's the durable single-node production backend and
the one the benchmarks characterise most thoroughly.

```go
perfprovider.Config{
    Backend:     perfprovider.BackendPostgres,
    PostgresDSN: "postgres://user:pass@localhost:5432/wallet?sslmode=disable",
    MaxDBConns:  workers + 16, // size for your workers PLUS the monitor
    Network:     defs.NetworkTTN,
    StorageName: "my-app",
}
```

Metadata always lives in a SQL metastore; Aerospike backs only the hot UTXO inventory.
Before reaching for it, read [`docs/aerospike-value-review.md`](docs/aerospike-value-review.md) —
a 2026-08-12 review concluded Aerospike is not earning its keep and recommends
PostgreSQL-only. MySQL is parsed but explicitly rejected.

`Migrate` is idempotent and runs once per storage instance. Schema changes ship as
embedded goose migrations; you don't manage SQL files yourself.

---

## BRC-100 and funding

**This toolbox is a BRC-100 wallet.** `wallet.Wallet` satisfies the go-sdk interface
directly — `var _ sdk.Interface = (*Wallet)(nil)` — across actions (`CreateAction`,
`SignAction`, `AbortAction`, `InternalizeAction`, `ListActions`, `ListOutputs`,
`RelinquishOutput`), crypto (`Encrypt`/`Decrypt`, `CreateSignature`/`VerifySignature`,
`CreateHMAC`/`VerifyHMAC`, key linkage), certificates and identity discovery, and chain
queries.

**Because it speaks BRC-100, anyone with a BRC-100 wallet can fund your application.**
That's the practical on-ramp: a user opens [BSV Desktop](https://bsvblockchain.org),
sends a payment to your app's receive key, and your app credits it:

```go
_, err := w.InternalizeAction(ctx, sdk.InternalizeActionArgs{
    Tx: beefBytes,
    Outputs: []sdk.InternalizeOutput{{
        OutputIndex: 0,
        Protocol:    sdk.InternalizeProtocolWalletPayment,
        PaymentRemittance: &sdk.Payment{
            DerivationPrefix:  prefix,
            DerivationSuffix:  suffix,
            SenderIdentityKey: senderPubKey,
        },
    }},
    Description: "incoming payment",
}, originator)
```

The toolbox re-derives the locking script from the remittance, verifies the merkle proof
against ChainTracks headers, and only then records the coin.
[`examples/internalize`](examples/internalize) plays both sides of this exchange.

One honest limitation: BSV Desktop can *pay* your application, but it cannot *drive* a
toolbox wallet — there is no BRC-100 transport shim in this repo. Remoting today is the
storage-layer REST API (`storage.NewClient` against `cmd/storage-server`), which lets
many app processes share one storage backend.

---

## Arcade, and how status reaches your app

Arcade is the only external service you need. One host serves the whole gateway by path:

```
<arcade-base>                     broadcast + status API
<arcade-base>/events              SSE status stream
<arcade-base>/chaintracks/v2      block headers
```

Give the toolbox the base URL and the other two are derived automatically.

### The callback token

Arcade pushes status over Server-Sent Events. The **callback token** scopes that stream to
your wallet — arcade filters to your transactions and can replay what you missed while
disconnected. Derive it from your identity key:

```go
cfg.Arcade.CallbackToken = wdk.DeriveArcadeCallbackToken(identityKeyHex)
```

**The library does not wire this for you, and skipping it is not a small mistake.** A
measured run without it produced ~25,000 events/s of *"dropped events for slow SSE
client"* and left 23,745 transactions with no status at all.

### The statuses that matter

| Status | What it means for you |
|---|---|
| `SEEN_ON_NETWORK` | **The coin is now spendable.** Change lands at the unproven tier. |
| `SEEN_MULTIPLE_NODES` | Same. Note the name — it is *not* `SEEN_ON_MULTIPLE_NODES`. |
| `MINED` | Merkle proof verified locally against headers; the transaction is complete. |
| `DOUBLE_SPEND_ATTEMPTED` | Terminal. Inputs stay held until the reconciler proves the outcome. |
| `REJECTED` | Terminal, but recoverable — a later `SEEN`/`ACCEPTED` can supersede it. |

Most applications care about exactly two: `SEEN_ON_NETWORK` to go, `MINED` to settle.

### Watching status in your application

Run the monitor daemon. It owns the single SSE connection, applies status to storage, and
runs the reconciliation sweeps. It is not optional:

```go
monCfg := defs.DefaultMonitorConfig()
mon, err := monitor.NewDaemon(logger, provider, hdrs, oracle, monCfg,
    monitor.WithApplyConcurrency(32),
    monitor.WithStatusObserver(func(recs []arcade.TxRecord) {
        ui.Apply(recs) // your app sees every applied status
    }),
)
if err := mon.Start(ctx, monCfg.Tasks.EnabledTasks()); err != nil { ... }
defer mon.Stop()
```

`WithStatusObserver` is how your application follows transactions. **Do not open a second
SSE stream** — arcade's `/events` has no per-client filter, so a second connection is a
full duplicate against a fan-out that is already the system's bottleneck.

Your observer must not block (it runs inline on the applier goroutine), must not panic,
and must be idempotent — delivery is at-least-once. Don't retain or mutate the slice.

---

## Networks

The BSV Association hosts public Arcade instances for every network, in two regions. The
US instance is the built-in default; the EU instance is equivalent — set `arcade.url`
explicitly to use it.

| Network | `defs.BSVNetwork` | US (default) | EU |
|---|---|---|---|
| **Mainnet** | `NetworkMainnet` (`main`) | `arcade-v2-us-1.bsvblockchain.tech` | `arcade-v2-eu-1.bsvblockchain.tech` |
| **Teratestnet** | `NetworkTTN` (`ttn`) | `arcade-v2-ttn-us-1.bsvblockchain.tech` | `arcade-v2-ttn-eu-1.bsvblockchain.tech` |
| **Testnet** | `NetworkTestnet` (`test`) | `arcade-v2-testnet-us-1.bsvblockchain.tech` | `arcade-v2-testnet-eu-1.bsvblockchain.tech` |

All are `https://`. There's a fourth network, `NetworkTSTN` (`tstn`), for private scaling
deployments — it reads its endpoint from `TSTN_ARCADE_URL`.

`defs.DefaultServicesConfig(chain)` resolves all of this. To use a EU instance:

```go
cfg := defs.DefaultServicesConfig(defs.NetworkTTN)
cfg.Arcade.URL = defs.ArcadeTTNEUURL
cfg.Arcade.EventsURL = ""   // re-derived from the new base
cfg.ChainTracks.URL = ""
```

**Teratestnet (`ttn`) is the best place to start.** It's the public Teranode scaling test
network — real infrastructure, worthless coins, and the throughput headroom to actually
exercise a high-volume design.

### Getting coins: BSV Desktop

Install [**BSV Desktop**](https://bsvblockchain.org) — the reference BRC-100 wallet. It
creates wallets on **mainnet, testnet and teratestnet**, so you can hold spendable coins
on whichever network you're developing against and fund your application from a real
wallet over a real network.

That's the intended loop: create a wallet in BSV Desktop on `ttn`, send a payment to your
app's receive key, internalize it, and you're running.

---

## Going to production

A short checklist. Each line links to the detail.

- **Run the monitor daemon.** Without it, transactions never get their status applied.
- **Back up the database.** [It is a correctness requirement.](docs/operations.md)
- **Set the fee rate to 125 sat/kB, not the default 100.** Arcade's validator prices
  differently than the toolbox's fee arithmetic; 100 leaves no margin.
- **Never retry a 4xx.** A rejection is a verdict, not a transport failure. Always retry a
  503 — that's backpressure and the transaction was never queued.
- **Treat `ErrNotEnoughFunds` in throughput mode as backpressure**, not an error. Retry it.
- **Derive the callback token.** See above.

### What to expect

Measured on one 32-core node against PostgreSQL:

| Configuration | Sustained TPS |
|---|---|
| Strict durability (`synchronous_commit=on`) | ~575 |
| Relaxed durability (`synchronous_commit=off`) | ~1379 |

So: **do not claim an unqualified 1000 TPS.** The honest number depends on what you're
willing to give up. Above roughly 1.6k TPS the binding constraint stops being the toolbox
and becomes arcade's own propagation and SSE fan-out.

Two documents go deeper, and they're the best material in this repo:

- [**Application throughput playbook**](docs/application-throughput-playbook.md) — written
  for you, the application author. A 16-item ordered checklist, workload shaping, the fuel
  pool end to end, and how to measure honestly.
- [**High-throughput guide**](docs/high-throughput-guide.md) — the library view: tuning
  knobs, the durability tradeoff, and a failure-mode playbook.

The single highest-leverage idea in both: **prefer many independent coins over one long
chain**, and never pass accumulated ancestry as `InputBEEF` (one measured case ballooned a
4 kB transaction to 1,194 kB of ancestry).

---

## Example applications

**[rule-110-arcade](https://github.com/galt-tr/rule-110-arcade)** — a Rule 110 cellular
automaton whose transition rule is enforced in native Bitcoin Script. 128 cells run as
independent UTXO chains, each transaction proving one bit of the next generation is
correct. It uses `WithChronicleOpcodes()`, the `ClaimExact` fuel-pool funding path, and
SSE status subscription — a genuine high-throughput design rather than a toy.

Live on teratestnet: **https://rule110-ttn-us-1.bsvblockchain.tech/**

More examples will be linked here as they land.

---

## Complex scripts: Rúnar

If your application needs non-trivial locking scripts, don't hand-assemble opcodes. Use
[**Rúnar**](https://github.com/icellan/runar):

> Write Bitcoin smart contracts in TypeScript, Go, Rust, Java, Ruby, Python, Zig, Solidity,
> or Move. Compile to Bitcoin Script.

```sh
go get github.com/icellan/runar/packages/runar-go
```

All nine source languages compile to byte-identical Bitcoin Script. The compiled locking
script drops straight into `sdk.CreateActionOutput.LockingScript`, so Rúnar and this
toolbox compose without glue — Rúnar decides what the coin means, the toolbox decides how
it gets funded, signed and broadcast.

---

## Where to go next

| Question | Document |
|---|---|
| Building with an agent | [AGENTS.md](AGENTS.md) |
| How does it work internally? | [docs/architecture.md](docs/architecture.md) |
| Storage contract, adding a backend | [docs/storage.md](docs/storage.md) |
| Status lifecycle, SSE, the reconciler | [docs/arcade-integration.md](docs/arcade-integration.md) |
| Making it fast | [docs/application-throughput-playbook.md](docs/application-throughput-playbook.md) |
| Backup, monitoring, running the server | [docs/operations.md](docs/operations.md) |
| Coming from go-wallet-toolbox | [docs/migration-from-go-wallet-toolbox.md](docs/migration-from-go-wallet-toolbox.md) |
| The raw measured numbers | [docs/benchmarks/README.md](docs/benchmarks/README.md) |
