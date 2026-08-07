# Operations

## Backup is a correctness requirement

**Read this first. It is the single most important operational fact about this
toolbox.**

An arcade-only wallet has **no UTXO discovery and no restore-from-seed.** It
learns about its outputs only from:

- transactions **it created** (the change it minted), and
- transactions **handed to it** via `InternalizeAction`.

There is no way to recover a wallet's UTXO set from the network — no indexer, no
rescan. **Lose the local database and the funds are unspendable-in-practice even
though the keys are intact**, until/unless the outputs are re-internalized from an
external record. This is the deliberate price of dropping the indexer
(`go-wallet-toolbox/docs/arcade-only-design.md` §3.6).

Consequences you must design for:

- **Operational backup of the wallet database is a correctness requirement, not a
  convenience.** Treat it like you would treat the private keys.
- Back up **atomically and consistently** — the metastore (metadata) and the
  utxostore (spendability) together are the wallet's state. In Mode A they are one
  database, so a single consistent snapshot suffices. In Mode B (Aerospike +
  PostgreSQL) they are two stores; back both up and understand that a
  point-in-time skew between them is healed by the transactional outbox +
  reconciler, but a *lost* store is not recoverable.
- The keys alone are **not** a backup. There is no rescan to rebuild balances.

### Per-backend backup / restore

- **SQLite (Mode A).** The whole wallet is one file. Use the SQLite online backup
  API or `VACUUM INTO` for a consistent snapshot (do not just `cp` a live file).
  Restore = put the file back and start the process.
- **PostgreSQL (Mode A, and the metastore for the hybrid).** Use continuous
  archiving / PITR (`pg_basebackup` + WAL archiving) or consistent logical dumps
  (`pg_dump`). If you run with `synchronous_commit=off` for throughput
  (see the [high-throughput guide](high-throughput-guide.md)), understand the
  bounded last-few-ms loss window on a crash and size your durability posture
  accordingly.
- **Aerospike (hybrid inventory, Mode B).** Back up the UTXO namespace
  (`asbackup`/`asrestore`) *and* the PostgreSQL metastore. See the Aerospike
  fund-safety requirements next.

## Aerospike fund-safety requirements

The Aerospike-backed utxostore enforces two things because a wallet's UTXO rows
must never silently vanish (`pkg/utxostore/aerostore/aerostore.go`):

- **The namespace `default-ttl` must be 0.** The store **refuses to start**
  otherwise: it reads `namespace/<ns>` info and errors with `REFUSING TO START:
  namespace ... has default-ttl=<n> (must be 0 so UTXO rows never silently
  expire)`. A non-zero TTL would let coins expire out of the inventory — data loss
  indistinguishable from losing funds.
- **Durable deletes require Enterprise Edition.** Deletes carry a tombstone only
  when the server supports durable deletes; the store auto-detects the edition
  (Community is detected and durable deletes are unavailable there). Without
  durable deletes, a deleted record can reappear after a cold restart — a
  fund-safety hazard for a wallet. Run **Aerospike Enterprise** for production
  fund-safety; `aerostore.WithDurableDeletes(true/false)` overrides the
  auto-detection if you must.

Deletion in the utxostore is explicit only — there is **no** TTL- or
height-based background expiry anywhere in the design.

## Monitoring

Run the monitor daemon (`pkg/monitor`, `monitor_enabled: true` on the storage
server). It runs the SSE apply pipeline, the tip/reorg consumers, the scheduled
tasks, and the reject→release reconciler. Watch these signals:

- **Reconciler metrics**, logged per pass (`reject_release` task):
  - `reconciler_released_total` — suspects whose inputs were released as provably
    dead (the leak the old design hid is now countable).
  - `reconciler_false_positive_total` — suspects that recovered (transient
    rejections). A healthy number here means the two-pass guard is doing its job.
  - `reconciler_stuck_total` — suspects escalated past the max-quarantine ceiling
    (default 24h). **These are never auto-released and need operator attention** —
    a rising count means suspects can't be verified (Arcade unreachable, or
    ambiguous double-spend competitors with unreadable rawTx).
  - plus `scanned`, `ambiguous`, `cascaded`, and the outbox drain report
    (`drained` / `failed` / `parked`).
- **SSE apply lag / maturation.** The monitor promotes coins to `TierMined` as
  header-verified proofs arrive off the status SSE stream. If coins are slow to
  become spendable, check the monitor is consuming the stream and that the shared
  connection pool isn't starving it behind the write workers.
- **Circuit-breaker state.** Repeated `ErrCircuitOpen` on broadcast means Arcade
  is unreachable (opaque failures); the breaker probes `GET /health` to recover.

## Running `cmd/storage-server`

The storage server hosts a provider behind the REST `/storage/v1` API so remote
wallets use it as a drop-in `wdk.WalletStorageProvider` (via
`storage.NewClient`). It wires the Arcade oracle + ChainTracks source, builds and
migrates the provider over the configured backend, and optionally runs the
monitor alongside the HTTP server.

```sh
storage-server -config config.yaml
```

Config surface: [`cmd/storage-server/config.example.yaml`](../cmd/storage-server/config.example.yaml).
Key fields: `network` (`main | test | ttn | tstn`), `backend`
(`sqlite | postgres | aerospike-hybrid`), `arcade.url`, `chaintracks.url`,
`monitor_enabled`, `max_db_conns`. A few settings can be overridden by
environment variables (they win over the file): `STORAGE_HTTP_ADDRESS`,
`STORAGE_SQLITE_PATH`, `STORAGE_POSTGRES_DSN`, `STORAGE_IDENTITY_KEY`,
`STORAGE_ARCADE_URL`, `STORAGE_CHAINTRACKS_URL`.

### Auth caveat

The **default server authenticator trusts an `X-Identity-Key` header**
(`storage.HeaderAuthenticator`). That is appropriate for a trusted network or
behind a gateway that terminates real authentication — **not** for direct
exposure to an untrusted network. The 5 storage-level routes (`Migrate`,
`MakeAvailable`, `FindOrInsertUser`, `GetSyncChunk`, `ProcessSyncChunk`) are
anonymous by default unless you configure an admin authenticator. Full
BRC-103/104 mutual auth is a documented follow-up; `storage.WithAuthenticator` /
`storage.WithAdminAuthenticator` are the seams.

## Integration tests: rootless podman

The integration and performance suites use testcontainers. On a rootless-podman
box:

```sh
# Verify the podman socket is active and print the DOCKER_HOST to export.
make check-podman
# (starts it if needed: systemctl --user enable --now podman.socket)

# Point testcontainers at the rootless podman socket, and disable the ryuk
# resource-reaper (it does not run rootless):
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true

make test-integration      # go test -tags integration ./...
make test-conformance      # the conformance subset
make clean-containers      # remove leftover testcontainers containers
```

The same two environment variables front every containerized benchmark run — see
[the benchmarks README](benchmarks/README.md) for the full commands.
