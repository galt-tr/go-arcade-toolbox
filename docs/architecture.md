# Architecture

`go-arcade-toolbox` is a BRC-100 wallet toolbox that composes **three
independent sources of truth**. The whole design follows from keeping them
separate and never letting one adjudicate another's question.

## The three sources of truth

| Source | Package(s) | Authoritative question | Trust posture |
|---|---|---|---|
| **Local UTXO ledger** | `pkg/storage`, `pkg/utxostore` | Which outputs does this wallet control, and which are spendable right now? | The wallet is the sole writer of its own rows; it records what it *believes*. |
| **Arcade transaction oracle** | `pkg/arcade` | Was a transaction accepted, and what is its lifecycle status (broadcast → seen → mined)? | The single broadcast target and the only conflict/double-spend adjudicator. |
| **ChainTracks headers** | `pkg/headers` | What is the block-header chain? | Trusted only to return the correct header for a height; the SPV decision is made locally. |

The UTXO store "records what the wallet believes (reserved, spent-by, tier) and
the reconciler corrects that belief from oracle verdicts. The store itself never
adjudicates double-spends." (`pkg/utxostore/doc.go`.)

## Package map

```
pkg/wallet      BRC-100 Wallet: New[KeySource](chain, key, storage, opts...),
                CreateAction / SignAction / InternalizeAction / AbortAction /
                ListActions / ListOutputs / GetPublicKey / Balance / FanOutFuel /
                ListTransactions / certificates. Public surface = go-sdk wallet types.
  pkg/wallet/fuelkeeper   client-side fuel-pool top-up loop (throughput mode)

pkg/wdk         The wallet data-kernel: the WalletStorageProvider contract, the
                Services interface, arg/result/row types, shared enums/errors.
  pkg/wdk/primitives      length/format-validated value types (satoshis, outpoints, …)

pkg/storage     The Provider satisfying wdk.WalletStorageProvider by orchestrating
                the metastore (metadata) + utxostore (inventory) + funder (coin
                selection) + arcade oracle + headers source. Mode A vs Mode B.
                Also the REST /storage/v1 Server + Client and the reject→release
                reconciler.
  pkg/storage/perfprovider    public builder: assembles a Provider over a backend
  pkg/storage/conformance     provider-level conformance suite

pkg/utxostore   The Store interface for the hot-path UTXO inventory + its error
                taxonomy. Every backend implements it; the funder is written
                against it.
  pkg/utxostore/sqlstore      SQLite + PostgreSQL (exact best-fit claims)
  pkg/utxostore/aerostore     Aerospike (approximate bucket selection)
  pkg/utxostore/memstore      in-memory reference implementation
  pkg/utxostore/utxostoretest backend conformance suite
  pkg/utxostore/factory       DSN-scheme registry (aerospike://) + blank-import register

pkg/arcade      Arcade tx-oracle client: EF broadcast, GetTx poll, SSE status
                stream, circuit breaker, the wire status lattice.
pkg/headers     ChainTracks headers client + LOCAL merkle-root verification.
pkg/monitor     Background daemon: SSE apply pipeline, tip/reorg consumers, the
                reject→release reconciler, scheduled tasks (gocron).
pkg/services    wdk.Services compatibility shim over arcade + headers.
pkg/brc29       BRC-29 key derivation (P2PKH between sender and recipient).
pkg/certificates, pkg/defs, pkg/errors, pkg/logging, pkg/tracing, pkg/randomizer
```

`pkg/storage` depends on `pkg/utxostore`, `pkg/arcade`, `pkg/headers`; the wallet
depends on `pkg/storage` (via the `wdk.WalletStorageProvider` interface) and
`pkg/services`. Nothing depends back on the wallet.

## The write path

A wallet-created transaction's txid is only known once it has been **signed**
(BSV txids hash the unlocking scripts too), so the toolbox splits the write path
carefully around that fact.

1. **`CreateAction`** reserves the funding inputs (under the transaction
   `Reference` as the reservation token) and persists the *unsigned* metadata
   (transaction row, change output rows, commission). It **cannot** yet mint the
   change inventory — the change outpoints have no txid. With `SignAndProcess`
   the wallet immediately runs steps 2–3 internally; the two-step form returns a
   signable transaction and stops here.
2. **`SignAction`** produces the signed raw transaction (real BRC-29 signatures),
   which fixes the txid.
3. **`ProcessAction`** (internal) mints the change inventory into the change
   basket at `TierSending`, broadcasts the Extended Format transaction to Arcade,
   and on a 202 accept promotes the change to `TierUnproven` and flips the
   reserved funding inputs `reserved → spent`.

`AbortAction` before broadcast is correct precisely because of this ordering: it
releases the reservation by the tx `Reference` and removes any minted change.

### Coin tiers

A coin's tier tracks how settled its minting transaction is
(`pkg/utxostore/doc.go`):

- `TierSending` (1) — the mint transaction's broadcast is in flight.
- `TierUnproven` (2) — Arcade accepted it (a `SEEN_*` status) but no proof is verified yet.
- `TierMined` (3) — a header-verified merkle proof exists.

The funder walks tiers explicitly (one tier per micro-query), so every claim is a
single index walk on every backend. `Promote` moves coins between tiers in both
directions: up as evidence arrives, down on reorg.

## The async status lifecycle

Arcade is asynchronous: **a broadcast is an intake, not a verdict.** The
authoritative lifecycle arrives out of band. The monitor daemon
(`pkg/monitor`) consumes it and drives the local ledger forward:

```
CreateAction ── sign ── broadcast (POST /tx) ──▶ 202 accepted (early status, e.g. RECEIVED)
                                                     │  change minted TierSending → TierUnproven
                                                     ▼
   Arcade status SSE stream ──▶ SEEN_ON_NETWORK / SEEN_MULTIPLE_NODES ──▶ MINED
                                                     │
                                                     ▼  monitor fetches the BUMP (GET /tx),
                                                        verifies its merkle root against local
                                                        ChainTracks headers, then Promote → TierMined
                                                     ▼
                                            change matured (mined trust anchor)

   Rejection path (async): a broadcast can be accepted then later REJECTED, or a
   4xx makes it terminal-REJECTED immediately. Phase 1 marks the tx suspect
   WITHOUT releasing its inputs; the reject→release reconciler (Phase 2)
   re-verifies and, only when the tx is provably dead, releases the inputs:

     REJECTED ──▶ suspectFailed ──(two verified passes, grace apart)──▶ release inputs
                       │
                       └──▶ recovered (SEEN_*/ACCEPTED/MINED) ──▶ false positive, nothing released
```

The reject→release reconciler is the headline correctness fix over the old manual
`unfail`: async-rejected transactions have their inputs auto-released **after
verification**, with a two-pass false-positive guard and a double-spend
winner-union rule, so there is no UTXO leak and no manual intervention. See
[reject-release-vs-unfail](reject-release-vs-unfail.md) for why this model is
safer than unfail, and
[arcade-integration](arcade-integration.md#the-rejectrelease-reconciler) for the
full guard rules.

## The trust model: BUMP verified against local headers

Arcade delivers merkle proofs as BRC-74 BUMPs. The toolbox does **not** ask
Arcade or ChainTracks "is this root valid?" — ChainTracks exposes no
`isValidRootForHeight` route by design. Instead
`headers.VerifyMerkleRoot(root, height)` fetches the header for `height` and
byte-compares its merkle root against the candidate (`pkg/headers/doc.go`):

> This keeps the toolbox in control of the comparison: the header service is
> trusted only to return the correct header for a height, and the SPV decision
> (does this BUMP's computed root match the chain?) stays on our side of the wire.

`InternalizeAction` of a mined payment exercises exactly this: the wallet
re-derives the BRC-29 locking script, then verifies the BUMP against the local
headers client before recording the coin — a bad root is rejected and nothing is
persisted.

The reorg SSE stream is treated as **advisory only**; authoritative reorg safety
comes from re-verifying stored proofs against the headers client (the poll path),
never from relying on a stream that can miss events across a reconnect gap.

## What was removed vs go-wallet-toolbox, and why

The rewrite deletes everything the old toolbox needed a general-purpose services
layer for, because an arcade-only wallet does not need it:

| Removed | Why |
|---|---|
| WhatsOnChain / Bitails / GorillaPool ARC / TAAL integrations | Arcade is the single transaction-truth oracle; there is no multi-provider fallback queue or broadcast failover chain (only the arcade circuit breaker + backpressure remain). |
| Exchange-rate feed (`BsvExchangeRate`) | Fees are a static `sat/kb` config model, never fetched. |
| Script-hash history (`GetScriptHashHistory`) | No core-flow consumer; needs an indexer the arcade-only posture drops. |
| UTXO status / `GetUtxoStatus` / `IsUtxo` | No UTXO discovery — the local ledger is the only record of a wallet's coins. |
| GORM | Replaced by raw `database/sql` over `pgx` + pure-Go `modernc.org/sqlite`, with `goose` migrations. |
| MySQL | Retained only as an enum for config compile-compat; `Database.Validate` rejects it. Supported metastore engines are SQLite and PostgreSQL. |
| JSON-RPC transport | Replaced by the REST `/storage/v1` server + client for remote/multi-user deployment. |

None of these are part of the `wdk.Services` interface, so dropping the
implementations costs nothing on the compatibility promise — the interface is
byte-identical and `pkg/services` re-implements it as a thin adapter over
`arcade.TxOracle` + `headers.Headers`. See
[migration](migration-from-go-wallet-toolbox.md).

## The hard limitation

There is **no UTXO discovery and no restore-from-seed.** A wallet learns of its
outputs only from transactions it created (change) and from `InternalizeAction`.
Lose the local database and the funds are unspendable-in-practice even though the
keys are intact. Operational backup of the wallet database is therefore a
**correctness requirement**, not a convenience — see [operations](operations.md).
