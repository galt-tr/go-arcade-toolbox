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
//   - Spend, Unspend, Promote, Remove, RemoveByMintTx, Freeze and the releases
//     operate on FIXED outpoints with nothing to skip to, so they use plain
//     guarded statements that WAIT on the row lock, wrapped in a bounded
//     lock-error retry (isLockError classifies PostgreSQL 40001/40P01/55P03 and
//     SQLite BUSY/LOCKED; backoff 100ms << attempt, up to three retries).
//
// Both paths carry that retry. Claims went without one for a while, which read
// as a deliberate asymmetry and was not: SKIP LOCKED removes the common reason a
// claim would block, but it does not make 40001/40P01/55P03 impossible, and the
// funder above only retries [utxostore.ErrContention] — which this backend never
// returns. The result was a lock error on the hottest path failing the call
// outright while the same error on a cold path was retried three times. Retrying
// a claim is safe without a transaction because the statement is atomic: a
// failed attempt committed nothing, so re-running it cannot double-allocate.
// Under an ambient (Mode A) transaction the retry is skipped and the enclosing
// unit of work owns recovery.
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
//   - ErrContention vs (nil, nil). This is a LOCK-based provider, so per the
//     [utxostore] error taxonomy it NEVER returns [utxostore.ErrContention]. An
//     empty claim result is always reported as "nothing available" (nil). This
//     is correct even under SKIP LOCKED contention: the candidate SELECT and
//     the reserving UPDATE are the SAME statement, so any row a claimer skips
//     because it is locked is, by construction, being reserved by the peer that
//     holds its lock in that peer's own atomic statement — the coin is never
//     orphaned. A claimer that momentarily sees an empty pool therefore loses
//     no coin by concluding "none": every skipped coin is claimed by someone.
//     The conformance suite's concurrent-exclusivity subtest confirms every
//     coin is claimed exactly once with no ErrContention path taken.
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
// The in-memory reference implementation is [memstore]; the conformance suite
// is [utxostoretest]. This package satisfies both.
package sqlstore
