# Examples

Runnable, self-contained programs that exercise the `go-arcade-toolbox` wallet
against **in-process Arcade + ChainTracks mocks**, so they run with no external
services:

```sh
go run ./examples/quickstart      # fund a wallet, send a payment, read balance
go run ./examples/internalize     # receive an external payment
go run ./examples/highthroughput  # denominated fuel-pool config + FanOutFuel
```

All three build with `go build ./examples/...`.

| Example | What it shows | Runnable? |
|---|---|---|
| [`quickstart`](quickstart/) | The 5-minute tour: SQLite wallet, seed funds, `CreateAction`, `Balance`, `ListOutputs` | Yes, end to end |
| [`internalize`](internalize/) | Funding a wallet by receiving an external BRC-29 payment via `InternalizeAction` | Yes, end to end |
| [`highthroughput`](highthroughput/) | Throughput config sizing, `FanOutFuel`, and the fuel-keeper wiring | Yes (illustrative — real 1000 TPS needs a tuned backend) |

## The mock environment

The shared wiring lives in [`internal/demoenv`](internal/demoenv/demoenv.go): it
builds a real `wallet.Wallet` over a real SQLite `storage.Provider`, with the
production `arcade.Client` and `headers.Client` pointed at the in-process
[`mockarcade`](../internal/testenv/mockarcade) doubles. Read `demoenv.go` to see
exactly how a wallet is assembled from config.

**In production** you change two things: point `defs.Arcade.URL` /
`defs.ChainTracks.URL` at a real Arcade deployment
(see [`../docs/arcade-integration.md`](../docs/arcade-integration.md)), and fund
the wallet by receiving real payments rather than with the demo faucet — a fresh
arcade-only wallet has **zero** spendable balance and there is **no
restore-from-seed** (see [`../docs/operations.md`](../docs/operations.md)).
