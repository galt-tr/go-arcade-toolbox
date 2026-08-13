# AGENTS.md — building applications on go-arcade-toolbox

Instructions for a coding agent writing an application that **uses** this toolbox as a
library. If you are modifying the toolbox itself, read [docs/architecture.md](docs/architecture.md)
instead.

The human-facing companion is [GETTING_STARTED.md](GETTING_STARTED.md). This file is the
dense version: contracts, traps, and the commands that prove you got it right.

---

## 0. Orientation — read this before writing code

`go-arcade-toolbox` is an **arcade-only BRC-100 wallet library**, embedded in-process.

Module path: `github.com/galt-tr/go-arcade-toolbox` — use it in every `import`.

Four facts that determine most design decisions:

1. **There is no UTXO discovery and no restore-from-seed.** A wallet knows only about
   transactions it created and coins passed to `InternalizeAction`. The database *is* the
   wallet. Design for backup.
2. **Arcade is the only transaction-truth oracle.** Broadcast, status, and merkle proofs
   all come from one service. There is no fallback and no multi-arcade HA.
3. **Merkle proofs are verified locally** against ChainTracks headers before anything is
   stored. Never bypass that.
4. **Status arrives asynchronously over SSE.** A successful `CreateAction` does not mean
   confirmed. Your application must handle the status lifecycle.

Do not invent abstractions over `wallet.Wallet`. It already implements the go-sdk
`sdk.Interface`; wrapping it usually means you missed an existing method.

---

## 1. Canonical wiring

Start from this. It is the production shape — PostgreSQL, real network, monitor running.
Adapted from `examples/internal/demoenv/demoenv.go` and the throughput playbook §7.6.

```go
import (
    "github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
    "github.com/galt-tr/go-arcade-toolbox/pkg/defs"
    "github.com/galt-tr/go-arcade-toolbox/pkg/headers"
    "github.com/galt-tr/go-arcade-toolbox/pkg/monitor"
    "github.com/galt-tr/go-arcade-toolbox/pkg/services"
    "github.com/galt-tr/go-arcade-toolbox/pkg/storage"
    "github.com/galt-tr/go-arcade-toolbox/pkg/storage/perfprovider"
    "github.com/galt-tr/go-arcade-toolbox/pkg/wallet"
    "github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
    sdk "github.com/bsv-blockchain/go-sdk/wallet"
)

// 1. Network + services. DefaultServicesConfig resolves arcade, SSE and ChainTracks
//    URLs for main | test | ttn; tstn reads $TSTN_ARCADE_URL.
cfg := defs.DefaultServicesConfig(defs.NetworkTTN)

// 2. REQUIRED: derive the callback token. The library will NOT do this for you.
cfg.Arcade.CallbackToken = wdk.DeriveArcadeCallbackToken(identityKeyHex)

oracle := arcade.New(logger, nil, cfg.Arcade)
hdrs, err := headers.New(logger, cfg.ChainTracks, headers.WithCacheDepth(0))

// 3. Storage.
provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
    Backend:     perfprovider.BackendPostgres,
    PostgresDSN: dsn,
    Network:     defs.NetworkTTN,
    StorageName: "my-app",
    MaxDBConns:  workers + 16,                                  // workers PLUS the monitor
    FeeModel:    defs.FeeModel{Type: defs.SatPerKB, Value: 125}, // 125, not the default 100
    ExtraOptions: []storage.Option{
        storage.WithImmediateBroadcast(),
        storage.WithDirectInputBEEF(),
        storage.WithMinBroadcastFeeRate(100),
    },
}, oracle, hdrs)
defer closeProv(ctx)

_, err = provider.Migrate(ctx, "my-app", identityKeyHex) // once per storage instance

// 4. Wallet.
w, err := wallet.New(defs.NetworkTTN, keySource, provider,
    wallet.WithServices(services.New(logger, oracle, hdrs, cfg)),
    wallet.WithLogger(logger),
)

// 5. Monitor daemon. NOT OPTIONAL — without it nothing ever gets a status.
monCfg := defs.DefaultMonitorConfig()
mon, err := monitor.NewDaemon(logger, provider, hdrs, oracle, monCfg,
    monitor.WithApplyConcurrency(32),
    monitor.WithStatusObserver(func(recs []arcade.TxRecord) { app.OnStatus(recs) }),
)
if err := mon.Start(ctx, monCfg.Tasks.EnabledTasks()); err != nil { return err }
defer mon.Stop()
```

`keySource` may be a DER hex `string`, a `wallet.WIF`, an `*ec.PrivateKey`, or a
`*sdk.KeyDeriver`.

For local development substitute `perfprovider.BackendSQLite` + `SQLitePath`, and see
`examples/internal/demoenv` for running against in-process mocks with zero external
services.

---

## 2. API surface

`wallet.Wallet` satisfies `sdk.Interface` (`pkg/wallet/wallet.go:46`). All argument and
result types come from `github.com/bsv-blockchain/go-sdk/wallet`, imported as `sdk`.

**Actions:** `CreateAction`, `SignAction`, `AbortAction`, `InternalizeAction`,
`ListActions`, `ListOutputs`, `RelinquishOutput`

**Crypto:** `GetPublicKey`, `Encrypt`, `Decrypt`, `CreateHMAC`, `VerifyHMAC`,
`CreateSignature`, `VerifySignature`, `RevealCounterpartyKeyLinkage`,
`RevealSpecificKeyLinkage`

**Certificates:** `AcquireCertificate`, `ListCertificates`, `ProveCertificate`,
`RelinquishCertificate`, `DiscoverByIdentityKey`, `DiscoverByAttributes`

**Chain / auth:** `GetHeight`, `GetHeaderForHeight`, `GetNetwork`, `GetVersion`,
`IsAuthenticated`, `WaitForAuthentication`

**Toolbox extensions (not BRC-100):**

| Method | Use |
|---|---|
| `Balance(ctx)` | spendable sats in the `default` basket |
| `BasketBalance(ctx, basket)` | spendable sats in a named basket |
| `BasketClaimableCount(ctx, basket)` | claimable coin count — use for fuel-pool health |
| `FanOutFuel(ctx, shape, originator)` | mint denominated coins into a basket |
| `ListFailedActions(ctx, args, unfail, originator)` | inspect/recover failed actions |
| `ListTransactions(ctx, args, originator)` | richer than `ListActions` |
| `GetBeefParty()`, `Close()`, `Destroy()` | lifecycle |

Every call takes an `originator` string: non-empty and FQDN-shaped, validated.

---

## 3. Traps

These cost real debugging time. Each is verified against the code at the cited path.

### 3.1 Broadcast error taxonomy — retrying the wrong class is destructive
`pkg/arcade/client.go:239-293`

| Outcome | Return | What to do |
|---|---|---|
| 2xx / 202 | `*BroadcastResult`, `err == nil` | Accepted, **not final**. Wait for status. |
| **4xx** | `BroadcastResult{Rejected: true}`, **`err == nil`** | A verdict. **NEVER retry.** |
| 503 | `*arcade.BackpressureError` | Never queued. **Safe to retry** after `RetryAfter`. |
| ≥500 / transport | plain `error` | Fate unknown. Reconcile via `GetTx`, don't blind-retry. |

A 4xx returns **`err == nil`** with `Rejected: true`. Code that only checks `err != nil`
will treat a rejection as success.

### 3.2 Status name
It is `SEEN_MULTIPLE_NODES` (`arcade.StatusSeenMultipleNodes`), **not**
`SEEN_ON_MULTIPLE_NODES`. `pkg/arcade/wire.go:68-96`

### 3.3 The callback token is not auto-wired
`cfg.Arcade.CallbackToken` is empty unless you set it. Derive with
`wdk.DeriveArcadeCallbackToken(identityKeyHex)` (`pkg/wdk/arcade_token.go:12`). Skipping
it measured ~25,000 events/s of "dropped events for slow SSE client" and 23,745
transactions stranded with empty `arcade_status`.

### 3.4 Basket-inserted coins are not spendable
`sdk.InternalizeProtocolBasketInsertion` records no BRC-29 derivation material, so the
coin **cannot be signed by the wallet**. Use `sdk.InternalizeProtocolWalletPayment` for
anything you intend to spend. `examples/internalize/main.go:113`

### 3.5 Never pass accumulated ancestry as `InputBEEF`
Passing your own transaction chain's BEEF grows without bound — one measured case reached
**1,194 kB of ancestry for a 4 kB transaction**. Use `storage.WithDirectInputBEEF()` and
let arcade validate EF from inline prevouts. Playbook §2.1, §2.4.

### 3.6 `GetNetwork` returns internal names
Returns `"main"` / `"test"`, not BRC-100's `"mainnet"` / `"testnet"`. Translate at your
API boundary if you expose it.

### 3.7 A fresh wallet has zero balance, permanently, unless funded
No discovery, no seed recovery. `InternalizeAction` or a transaction you created — those
are the only two ways a coin enters the wallet.

### 3.8 Declare `unlockingScriptLength` on caller-provided inputs
Undeclared inputs are assumed 107-byte P2PKH. Under-declaring underpays the fee and earns
a 4xx you cannot retry. Pair with `storage.WithMinBroadcastFeeRate(100)` to turn
underpayment into a local error instead of a remote rejection.

### 3.9 Do not open a second SSE stream
Arcade's `GET /events` accepts one `callbackToken` and no per-client filter, so a second
connection is a full duplicate against a fan-out that is already the system bottleneck.
Use `monitor.WithStatusObserver`. `pkg/arcade/sse.go:65-82`

---

## 4. Status lifecycle

`pkg/arcade/wire.go`. Open enum — treat unknown statuses as non-fatal.

| Status | Terminal | Application meaning |
|---|---|---|
| `RECEIVED`, `SENT_TO_NETWORK` | no | in arcade's pipeline, no verdict |
| `ACCEPTED_BY_NETWORK`, `SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES` | no | **coin is spendable**, `TierUnproven` |
| `PENDING_RETRY`, `STUMP_PROCESSING` | no | still in flight |
| `DOUBLE_SPEND_ATTEMPTED` | yes | `suspectFailed`; inputs held until the reconciler proves death |
| `REJECTED` | yes | may be a transient false positive; a later `SEEN`/`ACCEPTED` can recover it |
| `MINED` | yes | BUMP verified against headers, proof stored, `TierMined` |
| `IMMUTABLE` | yes | buried deep enough to be final |

`Status.CanSupersede(prev)` guards regressions. `MINED → IMMUTABLE` is the only transition
out of a terminal state.

**Status observer contract** (`pkg/monitor/options.go:74`) — violating any of these
degrades the whole pipeline:

- **Must not block.** Runs inline on the applier goroutine; blocking fills the
  16,384-slot queue, which blocks the SSE reader, which makes arcade drop your events.
- **Must not panic.** There is no recover on this path.
- **At-least-once** — be idempotent.
- Applied batches only; a failed batch is held for redelivery.
- Records arrive unfiltered and in arrival order, including txids the wallet has no row
  for. **Do not retain or mutate the slice.**
- Poll fallbacks do not route through the observer. Reconcile via `GetTx` if you need a
  convergence guarantee.

Cold start is lossy by design: a fresh connection replays only non-terminal statuses. The
monitor's poll sweeps cover the gap — another reason the daemon is mandatory.

---

## 5. Throughput

Apply in this order. Full detail: [docs/application-throughput-playbook.md](docs/application-throughput-playbook.md).

| # | Action | Why |
|---|---|---|
| 0 | Run the monitor daemon | Not optional. Nothing gets status without it. |
| 1 | `monitor.WithApplyConcurrency(32)` | Default is **8** and is not enough past a few hundred TPS |
| 2 | Fee rate **125 sat/kB** | Arcade's validator prices differently; 100 leaves zero margin |
| 3 | `storage.WithDirectInputBEEF()` | Ancestry stays O(inputs), not O(chain length) |
| 4 | Many independent coins, not one chain | Chain depth serialises everything downstream |
| 5 | `headers.WithCacheDepth(0)` | ~1000 proofs in one block share a single header fetch |
| 6 | `storage.WithImmediateBroadcast()` | Every benchmark past 1000 TPS used it |
| 7 | `MaxDBConns ≈ workers + margin` | Size for workers **plus** the monitor |
| 8 | `wallet.WithThroughputMode(true)` | Removes two `BeefParty` mutex serialisation points. **Pair with #3** |
| 9 | Fuel pool (below) | Removes claim contention entirely |
| 10 | `cfg.Arcade.FullStatusUpdates = false` | At 1000 TPS full updates demand ~4,000 events/s vs arcade's ~1,600/s ceiling |
| 11 | `synchronous_commit=off` | Measured 3.5×. **Understand the durability tradeoff first.** |

Barrier-free worker pool plus a token bucket. **Never** a per-batch `WaitGroup.Wait()` —
it converts your throughput into the latency of the slowest item in every batch.

### Fuel pool

Use when sustaining high TPS. Denominated coins remove UTXO-selection contention.

```go
um := defs.DefaultUTXOManagement()
um.Strategy = defs.StrategyThroughput
um.Throughput.ExpectedTxSizeBytes = 400   // signed size INCLUDING the fuel input
um.Throughput.ExpectedOutputSatoshis = 0  // pure fee/data action
um.Throughput.TargetTPS = 1000
um.Throughput.ExpectedConfirmationSeconds = 300
um.Throughput.SpendPolicy = defs.SpendPolicyPreferMined

if err := um.Validate(defs.DefaultFeeModel(), defs.Commission{}); err != nil { return err }
denom, err := um.Throughput.Denomination(defs.DefaultFeeModel(), defs.Commission{})
target := um.Throughput.TargetPool()  // ceil(TPS × confirmSeconds × 1.5)

// Pass storage.WithUTXOManagement(um) in ExtraOptions, then keep the pool topped up:
keeper, err := fuelkeeper.New(w, fuelkeeper.FromThroughput(um.Throughput, denom), logger)
go keeper.Run(ctx)
```

`Validate` enforces the sustained-throughput identity
`fanout_outputs_per_tx × fanout_max_txs_per_round >= target_tps × top_up_interval × 1.2`.
If it errors, your pool cannot refill as fast as you drain it — fix the shape, don't skip
validation.

**`ErrNotEnoughFunds` in throughput mode is backpressure, not failure. Retry it.**

Keeper knobs worth setting at scale: `MintConcurrency` (default serial — benchmarks used
64), `StreamLeafCap` (default 10 — benchmarks used 2000), `RecycleBasket`/`RecycleCount`
when change accumulates as fuel-sized coins.

### Ceilings you cannot engineer around

| Limit | Value |
|---|---|
| Arcade intake | ~1.6–1.9k TPS (single-partition propagation topic) |
| Arcade SSE fan-out | ~1,600 events/s |
| Repair sweep | 4,000 rows/tick, fixed |
| Multi-arcade HA | does not exist |

Measured single-node write path: **~575 TPS** durable PostgreSQL, **~1379 TPS** with
`synchronous_commit=off`. Do not claim an unqualified 1000 TPS. Past ~1.6k TPS the toolbox
measures idle and arcade is the constraint.

---

## 6. Verifying your work

Run these before reporting an application complete. Do not claim success without output.

```sh
go build ./...                       # compiles
go vet ./...                         # vets
go test ./...                        # your tests pass
```

Against this repo, if you changed it:

```sh
make test          # go test ./...
make test-race     # go test -race ./...
make lint          # golangci-lint
go build ./examples/...
go run ./examples/quickstart         # end-to-end, no external services
go test ./pkg/wallet/... -run BRC100Conformance -v
```

**Check a live arcade before blaming your code:**

```sh
curl -s https://arcade-v2-ttn-us-1.bsvblockchain.tech/health
# {"healthy":true,"version":"v0.12.1","status":"ok","blockHeight":29497,...}
```

Debugging checklist when transactions stall with no status:

1. Is the monitor daemon running and `Start`ed?
2. Is `cfg.Arcade.CallbackToken` set?
3. Is `WithApplyConcurrency` above the default 8?
4. Does the status observer block, panic, or retain the slice?
5. Is a second SSE stream open anywhere?

---

## 7. Networks

| Network | Constant | Default URL |
|---|---|---|
| Mainnet | `defs.NetworkMainnet` | `defs.ArcadeURL` (`arcade-v2-us-1`) / `defs.ArcadeEUURL` |
| Testnet | `defs.NetworkTestnet` | `defs.ArcadeTestnetURL` (`arcade-v2-testnet-us-1`) / `defs.ArcadeTestnetEUURL` |
| Teratestnet | `defs.NetworkTTN` | `defs.ArcadeTTNURL` (`arcade-v2-ttn-us-1`) / `defs.ArcadeTTNEUURL` |
| Scaling testnet | `defs.NetworkTSTN` | `$TSTN_ARCADE_URL` |

All hosts are under `.bsvblockchain.tech` and serve broadcast at the base, SSE at
`/events`, ChainTracks at `/chaintracks/v2`. `DefaultServicesConfig` derives the latter
two — set only `cfg.Arcade.URL` and blank the derived fields to switch region.

**Default to `NetworkTTN` for development** unless the user specified otherwise.

An unknown network name yields no endpoints at all and fails validation by name — it never
silently inherits mainnet.

---

## 8. Complex locking scripts

Do not hand-assemble opcodes. Use [Rúnar](https://github.com/icellan/runar) — it compiles
contracts written in TypeScript, Go, Rust, Java, Ruby, Python, Zig, Solidity or Move to
byte-identical Bitcoin Script.

```sh
go get github.com/icellan/runar/packages/runar-go
```

The compiled script goes directly into `sdk.CreateActionOutput.LockingScript`. Rúnar
defines what the coin means; the toolbox funds, signs and broadcasts it.

Working example of script-enforced application logic:
[rule-110-arcade](https://github.com/galt-tr/rule-110-arcade).

---

## 9. Doc map — read one file, not all of them

| Question | File |
|---|---|
| How does the write path work? Trust model? | [docs/architecture.md](docs/architecture.md) |
| Storage contract, Mode A/B, adding a backend | [docs/storage.md](docs/storage.md) |
| Status lifecycle, SSE contract, reconciler | [docs/arcade-integration.md](docs/arcade-integration.md) |
| **Making my application fast** | [docs/application-throughput-playbook.md](docs/application-throughput-playbook.md) |
| Library tuning knobs, durability tradeoff | [docs/high-throughput-guide.md](docs/high-throughput-guide.md) |
| Backup, monitoring, running storage-server | [docs/operations.md](docs/operations.md) |
| Why did my transaction get rejected? | [docs/rejection-hardening-audit.md](docs/rejection-hardening-audit.md) |
| Migrating from go-wallet-toolbox | [docs/migration-from-go-wallet-toolbox.md](docs/migration-from-go-wallet-toolbox.md) |
| Raw measured numbers | [docs/benchmarks/README.md](docs/benchmarks/README.md) |

Runnable code: `examples/quickstart`, `examples/internalize`, `examples/highthroughput`,
and `examples/internal/demoenv` for the full wiring.
