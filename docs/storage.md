# Storage

The storage layer is a `storage.Provider` (`pkg/storage`) that satisfies the
BRC-100 `wdk.WalletStorageProvider` contract by orchestrating three subsystems
plus the two external services:

- the **metastore** — wallet metadata: users, baskets, transactions/actions,
  labels + tags, outputs (descriptive rows + spend history), known transactions
  (proven-tx state + retained rawtx/BEEF), certificates, sync state, key-values,
  and the crash-recovery outbox. SQLite or PostgreSQL.
- the **utxostore** — the hot-path UTXO inventory keyed by outpoint. SQLite,
  PostgreSQL, or Aerospike.
- the **funder** — coin selection + change/fee computation, written against the
  `utxostore.Store` interface so it is correct on every backend.
- an **`arcade.TxOracle`** for broadcast and a **`headers.Headers`** source for
  merkle-root verification.

## The `WalletStorageProvider` contract

`wdk.WalletStorageProvider` (`pkg/wdk/storage.interface.go`) is a 21-method
interface. The write and read methods that a wallet drives:

```
Migrate, MakeAvailable, SetActive
FindOrInsertUser
CreateAction, ProcessAction, AbortAction, InternalizeAction
ListActions, ListOutputs, ListTransactions
FindOutputBasketsAuth, FindOutputsAuth
GetBalance
RelinquishOutput
InsertCertificateAuth, RelinquishCertificate, ListCertificates
GetSyncChunk, FindOrInsertSyncStateAuth, ProcessSyncChunk
```

Three implementations satisfy it: the in-process `storage.Provider`, the REST
`storage.Client` (the same interface, over HTTP), and any third-party
implementation. The provider-level conformance suite drives all of them through
this surface only.

## Multi-user model (`AuthID`)

Every user-scoped method takes an `AuthID`:

```go
type AuthID struct {
	IdentityKey string
	UserID      *int
	IsActive    *bool
}
```

Users are keyed by `IdentityKey` (the public identity key), which the storage
layer resolves to an authoritative numeric `UserID` via `FindOrInsertUser`. In
the REST server the `UserID` is derived server-side from the authenticated
identity and never trusted from the request body, so a caller cannot act as
another user. The provider refuses any user-scoped call whose `AuthID.UserID` is
nil with an authorization error. Every metastore and utxostore query is scoped to
that numeric `UserID` — no operation ever crosses a user boundary implicitly.

## The spendability seam

This is the key cross-store design decision. **The metastore has no `spendable`
column.** In the old GORM store a boolean `outputs.spendable` was the source of
truth; here that truth moves out of the metadata store entirely
(`pkg/storage/doc.go`, `pkg/storage/internal/metastore/doc.go`):

> The metastore has no "spendable" column: an output is spendable iff a live
> inventory row exists for its outpoint in the utxostore. ListOutputs and
> GetBalance compose spendability by intersecting the metastore's descriptive
> rows with the utxostore's live set.

So the **utxostore is the single source of spendability truth**. `ListOutputs`
and `GetBalance` intersect the metastore's descriptive rows with the utxostore's
live set; the metastore never decides spendability.

## Mode A vs Mode B consistency

The provider auto-detects its consistency mode from how the two stores are wired
— there is no explicit flag:

- **Mode A ("shared database")** — the metastore and utxostore run over one
  `*sql.DB`. The provider detects this (`metastore.SharesDatabase(utxo.DB())`)
  and wraps each `CreateAction` / `ProcessAction` / `AbortAction` write core in a
  single `metastore.Do` unit of work, so the metadata write and the inventory
  write commit or roll back **atomically**. SQLite and single-database PostgreSQL
  deployments are Mode A.

- **Mode B ("split stores")** — the utxostore is a different backend (Aerospike,
  or a separate SQL database), so the two stores cannot share a transaction. The
  inventory ops are applied directly against the utxostore and the metadata half
  commits in its own transaction. `CreateAction` reserves inputs via the funder,
  persists metadata, and **compensates** (a whole-token reservation release) if
  the metadata write fails. A process that crashes *between* the two direct
  writes is healed by the **transactional outbox**: the durable `utxo_ops_outbox`
  table records the pending inventory ops, and the monitor's `reject_release`
  task drains it (`DrainOutbox`) by replaying the rows idempotently
  (oldest-first, `FOR UPDATE SKIP LOCKED`, parked after
  `MaxOutboxAttempts = 10`). The Aerospike + PostgreSQL hybrid is Mode B.

  > Note: `pkg/storage/doc.go` still describes the outbox drain worker as
  > "deferred to M4"; that comment predates the reconciler. The drain is wired
  > today via the monitor's `reject_release` task calling `DrainOutbox`.

Every utxostore operation an outbox replays is idempotent by construction (a
same-data mint is a no-op success, a same-spender spend is a success, releasing
something already free is a skip, etc.), which is what makes replay safe.

## The pluggable `utxostore`

`utxostore.Store` (`pkg/utxostore/store.go`) is the contract every backend
implements. It answers a wallet's question ("claim any suitable coin") rather
than a node's ("spend this exact outpoint"):

```
Health
Mint, Get, Remove, RemoveByMintTx
ClaimSmallestSufficient, ClaimLargestInsufficient, ClaimExact
ReleaseReservation, ReleaseOutpoints
Spend, Unspend, Promote
Freeze, Unfreeze
Balance, FindStaleReservations
Close
```

The hot operation is an atomic reserve-by-predicate. `ClaimExact` is the
throughput fast path:

```go
ClaimExact(ctx, s Scope, reservation string, denomination uint64, count int) ([]*UTXO, error)
```

It reserves up to `count` claimable coins of exactly `denomination` in one atomic
round trip; `len(result) < count` signals pool underflow and is not an error.

Six atomicity contracts are the core of the interface (every method is atomic
per item, claims are single-round-trip transitions, guards are exact-match
preconditions, every replayed op is idempotent, frozen rows are invisible to
claims and refuse `Spend` without force, and pinned rows are reserved rows no
janitor may free). The conformance suite enforces all six.

There is **no TTL- or height-based deletion, ever.** A node can rebuild its UTXO
set from the chain; a wallet cannot, so silently expiring a row is
indistinguishable from losing funds. Deletion is explicit only (`Remove` on
operator intent, `RemoveByMintTx` when the reconciler learns a minting
transaction is invalid).

### Backend matrix

| Backend | Package | Selection | `ErrContention`? | Constructed via |
|---|---|---|---|---|
| **SQLite / PostgreSQL** | `pkg/utxostore/sqlstore` | **exact** best-fit | never (the candidate SELECT and reserving UPDATE are the same statement) | `sqlstore.New` (Mode A, wrap `*sql.DB`), `OpenPostgres`, `OpenSQLite`, `Open(defs.Database)` |
| **Aerospike** | `pkg/utxostore/aerostore` | **approximate** (log2 buckets + best-of-16 sample; single-record CAS) | yes, under CAS exhaustion | `aerostore.Open(ctx, dsn)` |
| **in-memory** | `pkg/utxostore/memstore` | exact (reference impl) | never | `memstore.New()` |

**Approximate selection (Aerospike).** Aerospike keys coins into 64 best-fit
buckets by `floor(log2(sats))`. A denominated pool shares one bucket, so
`ClaimExact` is a single index probe. Arbitrary amounts are served by walking
buckets with a bounded best-of-sample approximation — "never over/under-spending,
never double-claiming, only relaxing selection optimality"
(`pkg/utxostore/aerostore/aerostore.go`). This is why the Aerospike hybrid shows
**zero** claim contention under load where the SQL `SKIP LOCKED` path can collide.

### Factory and DSN schemes

`utxostore.Open(ctx, dsn)` dispatches on the DSN scheme. **Only `aerospike://` is
registered today**, via a blank-import register package so a binary links the
Aerospike client only when it opts in — no build tags:

```go
import _ "github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/aerostore/register"
// then:
store, err := utxostore.Open(ctx, "aerospike://host:3000/namespace?set=utxos")
```

The SQL and in-memory backends are wired with their direct constructors
(`sqlstore.Open`, `memstore.New`), not through the factory. (The `perfprovider`
builder used in the examples handles all of this wiring for you.)

## The metastore

One implementation drives PostgreSQL and SQLite through a dialect switch over raw
`database/sql` (no ORM); the differences are confined to the embedded `goose`
migration set, the `?`→`$N` placeholder rewrite, the boolean spelling, and the
timestamp encoding (PostgreSQL `TIMESTAMPTZ` vs SQLite integer Unix
microseconds). It exposes one repository per aggregate and a `Do` unit-of-work
that opens one `*sql.Tx` and stashes it on the context — the Mode-A
shared-transaction seam a co-located `sqlstore` enlists in. The two packages use
distinct goose version tables (`goose_db_version_meta` /
`goose_db_version_utxo`) so their migration chains never collide in a shared
database.

## Remote storage: REST `/storage/v1`

`storage.NewServer(logger, provider, opts...)` mounts the provider behind
`/storage/v1/*`, one route per method. `storage.NewClient(baseURL, opts...)`
returns a full `wdk.WalletStorageProvider` that POSTs each call and reconstructs
typed errors across the wire (so `errors.Is` against the funder/wdk sentinels
still matches). Two auth tiers:

- 16 user-scoped routes go through an `Authenticator` (default
  `HeaderAuthenticator`, which trusts an `X-Identity-Key` header — suitable
  behind a gateway, not for direct exposure to an untrusted network).
- 5 storage-level routes (`Migrate`, `MakeAvailable`, `FindOrInsertUser`,
  `GetSyncChunk`, `ProcessSyncChunk`) are anonymous by default and gated only if
  an admin authenticator is configured.

Full BRC-103/104 mutual auth is a documented follow-up
(`storage.WithAuthenticator` is the seam). See [operations](operations.md) for
running the server.

## Adding a backend

There are two conformance suites, at two layers. Both hand your constructor a
fresh, isolated instance per subtest, run their subtests in **parallel** (each
gets its own schema/file/set, so concurrent unrelated transactions are part of
what is tested), and run under `-race` — including the container-backed legs, via
`make test-integration-race`.

Scale any of the concurrency subtests for a soak run with `ARCADE_STRESS`, which
multiplies worker and round counts without changing the code under test:

```console
$ ARCADE_STRESS=20 go test -race -tags integration -run Conformance ./...
```

**A new UTXO backend** implements `utxostore.Store` and runs:

```go
func TestMyStoreConformance(t *testing.T) {
	utxostoretest.RunStoreSuite(t, func(t *testing.T) utxostore.Store {
		return newMyStore(t) // register cleanup on t
	}) // add utxostoretest.WithExactSelection() for exact best-fit backends
}
```

Exact backends (SQL, memstore) opt **in** to the strict best-fit/ordering
assertions with `WithExactSelection()`; approximate backends (Aerospike) omit it.
If you want factory dispatch, register a DSN scheme from a blank-import `register`
subpackage.

**A new provider** implements `wdk.WalletStorageProvider` and runs:

```go
func TestMyProviderConformance(t *testing.T) {
	conformance.RunProviderSuite(t, func(t *testing.T) wdk.WalletStorageProvider {
		return newMyProvider(t) // fresh, unmigrated; the suite calls Migrate
	}) // add conformance.WithApproximateSelection() for approximate backends
}
```

Here the polarity is inverted: the default is **strict**, and an approximate
backend opts **out** with `WithApproximateSelection()` — a backend that forgets
the flag fails loudly rather than silently under-testing itself.
`conformance.NewInMemoryProvider(t, net, oracle, hdrs)` is a ready wiring helper
(SQLite metastore + memstore utxostore + funder + Provider). The repo's own
`conformance_sqlite_test.go`, `conformance_pg_test.go`, and
`conformance_remote_test.go` (which drives the REST client end to end) are worked
examples.

**Supply `WithRejectReleaseEnv` too.** Two subtests — `RejectRelease` and
`ConcurrentLifecycle` — cover the reject→release reconciler: that a verified
rejection returns its inputs to the pool, that a rejection the network revises
returns nothing, and that both hold while other workers concurrently fund from
the same basket. They need more than the provider, because two of the three
things they drive are not reachable through `wdk.WalletStorageProvider`:

```go
conformance.WithRejectReleaseEnv(func(t *testing.T) conformance.RejectReleaseEnv {
	clock := newTestClock() // pass clock.Now to metastore/utxostore/storage WithClock
	oracle := &conformance.FakeOracle{}
	p := newMyProviderClocked(t, oracle, clock) // MIGRATED by you, not the suite
	return conformance.RejectReleaseEnv{Provider: p, Oracle: oracle, Advance: clock.Advance}
})
```

The oracle is needed because the subtests must script arcade's verdict for a
txid that does not exist until signing; the clock is needed because the release
is gated on a grace period, and the alternative is a test that sleeps for it.
Every backend can supply both — `metastore`, `memstore`, `sqlstore` and
`aerostore` all expose `WithClock`.

Omit the option and both subtests **skip rather than fail**, on the same
principle as `WithRejectingHeadersProvider`. That is the right answer for a
provider that genuinely cannot reconcile — `storage.Client` cannot, because
`/storage/v1` has no reconciler route: the monitor runs in-process with the
`Provider`. It is the wrong answer for a real backend that just has not wired it,
so check the skip lines rather than the summary. This property went unproven for
four backends while the suite reported green, which is the whole reason it is now
a suite subtest rather than one backend's test file.
