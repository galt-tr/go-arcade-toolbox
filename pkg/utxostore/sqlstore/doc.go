// Package sqlstore is the PostgreSQL + SQLite implementation of
// [utxostore.Store]. One implementation drives both engines through a dialect
// switch over raw database/sql (no ORM); the differences are confined to the
// embedded migration set, the placeholder syntax, the claim-statement shape,
// and the timestamp encoding.
//
// It passes the utxostoretest conformance suite with exact selection enabled
// (true best-fit claims) on both engines. Construct it with [New] (wrap an
// existing *sql.DB), [OpenPostgres], [OpenSQLite], or [Open] (from a
// [defs.Database] config).
//
// # The claim hot path
//
// A claim reserves coins in a single atomic round trip. On PostgreSQL it is a
// CTE that selects the best-fit candidate(s) FOR UPDATE SKIP LOCKED and, in the
// same statement, UPDATEs them to reserved and RETURNs them. The partial index
// idx_utxos_claim — over (user_id, basket, tier, satoshis, seq) WHERE the row
// is claimable — makes this a pure index walk with no sort, flat in cost as the
// pool grows to hundreds of thousands of rows (see claim_explain_test.go). On
// SQLite the same shape is an UPDATE ... WHERE (txid,vout) IN (SELECT ... ORDER
// BY ... LIMIT ?) RETURNING, exclusive by virtue of the single writer
// connection.
//
// # Lock discipline
//
// Two teranode rationales are preserved deliberately:
//
//   - CLAIMS use FOR UPDATE SKIP LOCKED (PostgreSQL): any suitable coin is
//     acceptable, so a claimer skips rows a peer is mid-reserving rather than
//     forming a lock convoy behind them.
//   - Spend, Unspend, Promote, Remove, RemoveByMintTx, Freeze, the releases and
//     the pin toggles have nothing to skip to (they name a fixed outpoint, or a
//     reservation's whole membership), so they use plain
//     guarded statements that WAIT on the row lock, wrapped in a bounded
//     lock-error retry (isLockError classifies PostgreSQL 40001/40P01/55P03 and
//     SQLite BUSY/LOCKED; backoff 100ms << attempt, up to three retries).
//
// Both paths carry that retry. Claims went without one for a while, which read
// as a deliberate asymmetry and was not: SKIP LOCKED removes the common reason a
// claim would block, but it does not make 40001/40P01/55P03 impossible, and the
// funder above only retries [utxostore.ErrContention], and a raw driver lock
// error is not that. The result was a lock error on the hottest path failing the
// call outright while the same error on a cold path was retried three times. Retrying
// a claim is safe without a transaction because the statement is atomic: a
// failed attempt committed nothing, so re-running it cannot double-allocate.
// Under an ambient (Mode A) transaction the retry is skipped and the enclosing
// unit of work owns recovery.
//
// # Guarded mutations
//
// Every conditional mutation writes FIRST and carries its preconditions in its
// own WHERE — Spend re-asserts the reservation token, the unrecorded spend and
// the absence of a freeze; Remove re-asserts unreserved/unspent/unfrozen;
// RemoveByMintTx's delete re-asserts unreserved/unspent. They used to SELECT
// (FOR UPDATE), branch in Go, then write an UNGUARDED statement, which was safe
// only because of the lock the read had taken: correct on PostgreSQL, and on
// SQLite correct only through the single writer connection — which [New] could
// not install on a pool it does not own. That gap (audit P1-7) is closed from
// both ends: the guards make the write self-defending on any engine, and the
// constructors now VALIDATE the SQLite pin instead of assuming it.
//
// The inversion also removes a round trip from the accept path: the common
// outcome — one row updated — is now a single statement, and only a write that
// matched nothing pays for a classifying read. That read is deliberately plain
// (no FOR UPDATE): the write matched no row, so it holds no lock worth
// extending, and its answer is confirmed by the next attempt's guarded write
// anyway. Because a classification can race, each guarded mutation runs a
// two-attempt loop (guardAttempts) whose second pass exists for one outcome
// only — the row looked eligible again, meaning a peer released and re-reserved
// it mid-flight. See the ErrContention note below for what happens after that.
//
// [Store.classifyForReserve] is the one read that keeps FOR UPDATE: its
// all-or-nothing multi-outpoint hold acquires row locks in a canonical order
// and must keep them for the life of the transaction, so its decision cannot be
// folded into a single write's WHERE.
//
// # Set-based batches (PostgreSQL)
//
// The guarded mutations take a LIST of outpoints, and on PostgreSQL each one is
// a single statement over the whole list rather than a statement per item.
// Spend, ReleaseOutpoints, Unspend, Promote and Freeze/Unfreeze join their
// target rows against a two-parameter key relation (pgKeyRel: the two-argument
// unnest of a bytea[] of txids and a bigint[] of vouts), carrying exactly the
// guard conjuncts the per-op write carried. Two arrays rather than N tuples
// keeps the statement text — and so the cached plan — constant across batch
// sizes, and keeps a wide batch clear of the 65535-parameter ceiling. The
// two-ARGUMENT spelling is deliberate: it pairs the arrays positionally by
// definition, where two unnests in a target list would leave that to a planner
// rule that has differed between major versions, and a mis-pairing here spends
// the wrong coin. See pgKeyRel.
//
// This is where the round trips were. Spend runs on the broadcast-accept path
// of every accepted transaction, once per input; ReleaseOutpoints is the
// funder's rollback tail. At 1000 TPS those were per-coin round trips on paths
// that already know everything they need in one go.
//
// Nothing about the guarded-mutation contract changes. Spend still writes
// first, still classifies only what the write missed, and still runs the
// two-attempt guardAttempts loop — all of it lifted to the batch, so a mixed
// batch costs one write plus one classifying read and re-drives only the items
// that are actually contended. Where a statement's result is per item (Spend's
// verdicts, Freeze's NotFoundErrors) the write RETURNS its keys, and the misses
// are the input minus the returned set, reported in the caller's order. A
// duplicated outpoint behaves as it did under the loop: PostgreSQL applies at
// most one UPDATE per target row per statement, so the second occurrence
// resolves against the first's result rather than writing again.
//
// SQLite deliberately keeps the loops. Its pool is pinned to one local writer
// connection, so a statement per op costs no network round trip, and the
// dialect has neither unnest nor a derived-table column-alias list to express
// the batch with. The conformance suite runs both arms and pins that they rule
// identically; guarded_stmt_test.go and set_based_pg_test.go pin the statement
// COUNTS, which is the only way the difference is visible at all.
//
// # Mode A: shared database
//
// The store exposes [Store.SharesDatabase] and honors an ambient transaction
// carried in the context under the internal/sqltx key IFF it was opened over
// this store's own *sql.DB, so a co-located store (the metastore, later) can
// enlist the utxostore in one caller-owned transaction over a shared *sql.DB.
// Every statement runs against execer(ctx)/withTx, which use the ambient
// transaction when present and owned by this store's db, and the pool
// otherwise. The goose version table is named goose_db_version_utxo so the
// two packages' migration chains never collide in a shared database.
//
// # Design notes
//
// Decisions the plan left to this package, made explicitly:
//
//   - ErrContention vs (nil, nil) on a claim. An empty claim result is not by
//     itself evidence of contention: the candidate SELECT and the reserving
//     UPDATE are the SAME statement, so any row a claimer skips under SKIP
//     LOCKED is being reserved by the peer holding its lock — the coin is never
//     orphaned. Whether an empty result is reported as
//     [utxostore.ErrContention] is therefore a question about what else the
//     store knows, not about the claim statement's semantics. The claim itself
//     still reports "none" (nil) on both engines, and the conformance suite's
//     concurrent-exclusivity subtest still confirms every coin is claimed
//     exactly once along that path.
//
//     What the store knows how to answer WHEN ASKED is now
//     [Store.ClaimableExists]: a non-locking SELECT EXISTS over the claim's own
//     candidate predicate. The "the peer holding the lock is taking it anyway"
//     argument above is scoped to the life of a claim STATEMENT; in Mode A the
//     claim runs on the caller's ambient transaction, so its locks live until
//     that whole CreateAction commits and the peer may yet roll back and hand
//     the coin back. A concurrent CreateAction that reported ErrNotEnoughFunds
//     over rows in that state would be lying to the user (audit finding P2-4).
//
//     The funder is the only caller, and it asks at exactly one point: after a
//     whole funding pass has allocated nothing, in place of reporting
//     insufficient funds. Never on the allocating path — an empty claim
//     mid-walk is ordinary — and never mid-walk, so locked rows in one tier
//     cannot pre-empt a fund another tier could cover. idx_utxos_claim's
//     partial WHERE is exactly the probe's claimability terms, so it answers in
//     one index descent, and under READ COMMITTED (PostgreSQL's default, which
//     nothing in this module overrides) it reads the last COMMITTED row
//     version — precisely why it sees what SKIP LOCKED had to skip.
//     claim_predicate_test.go pins its predicate to the claim statements' so
//     the two cannot drift, and claim_explain_test.go pins its plan.
//
//     The guarded mutations have their own ErrContention exit, from an
//     unrelated cause: Spend (both modes) and Remove report it per item when a
//     row survives guardAttempts of "guarded write matched nothing, follow-up
//     read says it is eligible again", and RemoveByMintTx fails the whole call
//     on the same condition. That is a peer cycling this exact coin — transient
//     by definition, and to be re-driven rather than recorded as a refusal. It
//     is unreachable under a correctly pinned SQLite handle (one writer, no
//     interleaving) and vanishingly rare on PostgreSQL, where the guarded write
//     already serializes on the row lock.
//
//   - SQLite's single writer is validated, not assumed. [OpenSQLite] pins
//     SetMaxOpenConns(1) on the pool it opens. [New] is handed a pool it does
//     not own, so it REFUSES a SQLite handle that is not already pinned rather
//     than resizing a caller's pool behind their back — the same handle is
//     typically the metastore's, which makes the identical check. Widening a
//     SQLite pool buys no write parallelism SQLite can use; it only converts
//     serialization into SQLITE_BUSY.
//
//     The PostgreSQL counterpart cannot be validated, only reported: a pool
//     this store did not open carries whoever sized it, and in Mode A that is
//     the metastore's metadata-shaped pool (defaults 10/5) rather than this
//     package's claim-shaped one (defaultPool, 25/10). Claims past the ceiling
//     queue inside database/sql, so the wait appears as store latency and in no
//     query plan; [New] therefore logs MaxOpenConnections at Debug on
//     construction. An owned pool is sized from [WithConnPool] over defaultPool.
//
//   - Recommended PostgreSQL lock_timeout: about 2s. Set it on the ROLE
//     (ALTER ROLE ... SET lock_timeout = '2s'), which is the only route the
//     config-driven constructors offer — [sqlkit.PostgresDSN] renders a fixed
//     set of connection fields and cannot express it, so a per-connection
//     setting means hand-building a DSN (options=-c%20lock_timeout%3D2s) and
//     passing it to [OpenPostgres] directly.
//
//     Every guarded mutation WAITS on its row lock by design — it names a
//     fixed outpoint or a whole reservation, so there is nothing to skip to —
//     which means a pathological holder (a stuck Mode A CreateAction, a long
//     ad-hoc transaction) parks those statements indefinitely and, with them, a
//     pool connection each. A lock_timeout converts that unbounded wait into
//     SQLSTATE 55P03 (lock_not_available), which [sqlkit.WithRetry] already
//     classifies as a lock error and retries with backoff: the caller gets a
//     bounded retry against a transient stall, or an honest error, instead of a
//     hung request. Do NOT confuse it with statement_timeout, which would also
//     kill the long-running sweep. Claims are unaffected either way — SKIP
//     LOCKED means they never wait.
//
//     One interaction to state rather than discover: a ROLE-level lock_timeout
//     also applies to the startup migration, which runs on the same role from
//     newStore -> migrate. Migration 00003 rebuilds idx_utxos_reserved under
//     ACCESS EXCLUSIVE (see its own comment), so on a busy database that DDL
//     can be the thing that times out — and goose Up is NOT wrapped in
//     [sqlkit.WithRetry], so the 55P03 surfaces as a store-CONSTRUCTION
//     failure rather than a retried statement. Failing to start beats
//     deadlocking the deploy behind a lock nobody can see, but it is a startup
//     failure mode worth knowing about before it happens.
//
//   - SQLite seq. The primary key is composite (txid, vout), so seq cannot be
//     an INTEGER-PRIMARY-KEY rowid alias. PostgreSQL uses a GENERATED IDENTITY
//     column; SQLite draws seq from a single-row counter table (utxo_seq)
//     bumped once per mint attempt under the single writer connection. seq only
//     needs to be monotonic and gap-tolerant (it breaks ties in claim ordering
//     and dates reservations), never contiguous, so a bump wasted on an
//     idempotent no-op mint is harmless.
//
//   - SQLite timestamps. reserved_at and created_at are stored as INTEGER
//     microseconds since the Unix epoch (t.UnixMicro), not TEXT. Integer
//     comparison over the reserved_at index is exact and cheap, matches
//     PostgreSQL TIMESTAMPTZ's microsecond resolution (so both engines round
//     trip a claim's reserved_at identically — the conformance suite compares a
//     claim's timestamp to the one FindStaleReservations reports), and avoids
//     RFC3339 parsing and timezone ambiguity. Sub-millisecond fidelity, which
//     the exact-selection ordering assertions require, follows directly.
//
//   - Driver choice: modernc.org/sqlite (CGo-free) over mattn/go-sqlite3. It
//     needs no C toolchain, cross-compiles cleanly, bundles a recent SQLite
//     with partial indexes and RETURNING, and keeps `go test ./...` buildable
//     everywhere with no build-tag gymnastics. The claim path is index-bound,
//     not CPU-bound, so mattn's marginal raw-speed edge does not matter here.
//
//   - Migrations: embedded goose (pressly/goose/v3) in library mode with an
//     embed.FS and one directory per engine, per the storage-agent
//     recommendation. goose gives versioned, idempotent, transactional
//     migrations and a custom version-table name for Mode A; the library import
//     pulls in no database drivers of its own (only the CLI does), so the
//     dependency cost is small.
//
//   - The pin and the indexes. The pinned column is deliberately absent from
//     idx_utxos_claim: a pinned row is reserved, so that index's partial WHERE
//     (reserved_by IS NULL …) already excludes it and the hot path's plan is
//     untouched — claim_explain_test.go still EXPLAINs the byte-identical claim
//     statements. idx_utxos_reserved_at, which exists for the sweep, DOES fold
//     the pin into its predicate: pinned rows never enter it, and its WHERE
//     stays a superset match for the sweep's own terms. One predicate text
//     serves both engines ([notPinned] — "NOT pinned" is correct over SQLite's
//     INTEGER 0/1, exactly as "NOT frozen" already is in Balance), and that
//     single spelling is what keeps SQLite's literal partial-index matching
//     working. TestStaleScanIsIndexDriven pins both halves: the production
//     stale statement never table-scans, and the index still matches the pin
//     predicate. Note the planner satisfies the grouped stale scan from
//     idx_utxos_reserved instead, because staleness is a HAVING over
//     MIN(reserved_at) rather than a WHERE range on it — which is what the
//     next note is about.
//
//   - idx_utxos_reserved covers the sweep (migration 00003). As first written
//     it was (reserved_by, user_id) WHERE reserved_by IS NOT NULL, and it had
//     two faults, both on the sweep rather than the claim. It never shrank: a
//     Spend leaves reserved_by set as provenance, so every coin ever handed
//     out stayed in it forever while the live hold set did not — and fact-mode
//     spends make that the common case. And its predicate had fallen behind
//     its queries, which all filter spent_by IS NULL AND NOT pinned, so each of
//     them re-checked both per heap tuple across that dead weight. The grouped
//     stale scan above paid the worst of it, reading the entire live hold set
//     every tick: PostgreSQL could not use this index for it at all and fell
//     back to a hashed aggregate over a bitmap heap scan of ~100k rows, driven
//     off idx_utxos_reserved_at.
//
//     00003 rewrites it as (reserved_by, user_id) INCLUDE (reserved_at, seq,
//     pinned) WHERE reserved_by IS NOT NULL AND spent_by IS NULL: spent rows
//     leave the index at Spend time, and the INCLUDEd columns are exactly the
//     rest of what [Store.staleReservationsSQL]'s inner aggregate reads, so on
//     PostgreSQL it runs index-only. pinned is INCLUDEd rather than folded into
//     the predicate — unlike idx_utxos_reserved_at, which only the sweep uses —
//     because Pin/Unpin look rows up BY their current pin state through this
//     index, so pinned rows must stay in it and merely be filterable inside it.
//     txid/vout are left out on purpose: only the sweep's outer expansion
//     projects them, once per returned group rather than per pool row, and a
//     32-byte txid per entry would spend the size win.
//
//     SQLite has no INCLUDE, so the same columns are trailing key columns
//     there — plus one PostgreSQL does not need at all: spent_by. Both engines
//     use spent_by only as the partial predicate, but only PostgreSQL
//     DISCHARGES it, recognizing that the index's own WHERE already proves the
//     query's "spent_by IS NULL" and dropping the term. SQLite matches the
//     partial index and then re-evaluates the term anyway, so without the
//     column in the index it reads the table row to check a condition that
//     cannot be false — and the stale scan loses its covering property for
//     nothing. The column is always NULL for every row in this index, so
//     carrying it is nearly free. TestStaleScanIsIndexDriven asserts the
//     resulting "USING COVERING INDEX" line; sweep_explain_test.go is the
//     PostgreSQL guard, over 100k live holds against 150k spent ones.
//
// The in-memory reference implementation is [memstore]; the conformance suite
// is [utxostoretest]. This package satisfies both.
package sqlstore
