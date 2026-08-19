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
//     orphaned, and no retry by this claimer would find it. Whether an empty
//     result is reported as [utxostore.ErrContention] is therefore a question
//     about what else the store knows, not about the claim statement's
//     semantics. Today it knows nothing more, so it reports "none" (nil); the
//     conformance suite's concurrent-exclusivity subtest confirms every coin is
//     claimed exactly once along that path.
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
//     predicate. Note the planner currently satisfies the grouped stale scan
//     from idx_utxos_reserved instead, because staleness is a HAVING over
//     MIN(reserved_at) rather than a WHERE range on it.
//
// The in-memory reference implementation is [memstore]; the conformance suite
// is [utxostoretest]. This package satisfies both.
package sqlstore
