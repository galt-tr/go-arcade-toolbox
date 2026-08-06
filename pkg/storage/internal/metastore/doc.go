// Package metastore is the wallet metadata store over PostgreSQL and SQLite. It
// holds everything the UTXO inventory ([utxostore]) does not: users, output
// baskets, transactions/actions, labels + tags, outputs (descriptive data plus
// spend history), known transactions (proven-tx state plus retained
// rawtx/BEEF material), certificates, sync state, key-values and the
// utxo_ops_outbox.
//
// One implementation drives both engines through a dialect switch over raw
// database/sql (no ORM); the differences are confined to the embedded goose
// migration set, the '?'→$N placeholder rewrite, the boolean spelling and the
// timestamp encoding (PostgreSQL TIMESTAMPTZ vs SQLite INTEGER Unix
// microseconds). That dialect scaffolding is shared with pkg/utxostore/sqlstore
// through internal/sqlkit; see "Shared dialect scaffolding" below.
//
// # Repositories and the Unit of Work
//
// The [Store] exposes one repository per aggregate — [Store.Users],
// [Store.Baskets], [Store.Transactions], [Store.Outputs], [Store.Labels],
// [Store.Tags], [Store.KnownTx], [Store.Certificates], [Store.SyncState],
// [Store.KeyValue], [Store.Outbox]. Every repository method runs against
// execer(ctx): the ambient *sql.Tx carried by ctx when present, otherwise the
// pool. [Store.Do] is the Unit of Work: it opens ONE *sql.Tx, stashes it on the
// context via internal/sqltx, and commits or rolls back around the callback. A
// repository call — and any co-located sqlstore call over the same *sql.DB —
// made with the Do-provided context therefore runs inside that one transaction.
// This is the Mode-A shared-transaction seam; [Store.SharesDatabase] lets the
// provider detect it.
//
// # The spendability seam (ListOutputs / GetBalance)
//
// This is the key cross-store design decision, made explicitly here:
//
// The metastore has NO "spendable" column on outputs. In the old GORM store a
// boolean outputs.spendable was the source of truth for both ListOutputs
// (WHERE spendable = true) and GetBalance (Σ satoshis WHERE spendable). In the
// hybrid design that truth moves OUT of the metadata store: an output is
// spendable if — and only if — a corresponding live inventory row exists in the
// [utxostore]. Spendability is therefore EXTERNAL to the metastore, and the
// metastore never decides it.
//
// Concretely:
//
//   - [OutputsRepo.ListOutputs] applies only the descriptive filters it owns —
//     basket, tag set (ANY/ALL), allowed parent-transaction status, and
//     pagination — and returns each matching row WITH its outpoint
//     (txid:vout). It does NOT filter by, or populate, spendability. The
//     provider (a later task) intersects each row's outpoint with the
//     utxostore's live set to set WalletOutput.Spendable and, when the caller
//     asked for spendable-only, to drop the rest.
//
//   - There is deliberately NO metastore GetBalance. A spendable balance is
//     composed by the provider from utxostore.Balance (which already sums the
//     claimable coins per tier) — optionally joined with this store's
//     descriptive data — not from a metastore sum over a spendable column that
//     does not exist.
//
//   - [OutputsRepo.FindOutputs] honors every wdk.FindOutputsArgs filter it can
//     evaluate locally (user, output id, transaction id, txid, vout, change,
//     satoshis, parent tx status) and IGNORES the Spendable filter, which is
//     external for the same reason; callers that need it apply it after the
//     utxostore join.
//
// The upshot: metastore.Outputs exposes queries keyed by (user, basket, tags,
// outpoints) returning descriptive rows; the provider composes spendability and
// balance from the utxostore. This keeps the two stores' responsibilities
// disjoint and is what makes one funder correct over both a shared SQL database
// and a split SQL-metadata / Aerospike-inventory deployment.
//
// # Shared dialect scaffolding (internal/sqlkit)
//
// The dialect scaffolding is shared with pkg/utxostore/sqlstore through
// internal/sqlkit rather than copy-adapted: the '?'→$N rebind, the IsLockError
// classifier + bounded WithRetry, the EncTime/BoolVal encoders and the
// TsScan/BoolScan scan helpers, the SQLite DSN pragmas and the Postgres DSN
// builder, the connection-pool sizing, the ambient-transaction resolver
// (Execer), and the goose embed.FS-per-engine runner. Single-sourcing these
// matters in Mode A: the two stores share one *sql.DB, so their pragmas must
// match and one retry classifier must govern both statement chains. The
// store-specific pieces stay here — the repository scans, the SQL strings, the
// FOR UPDATE SKIP LOCKED clause, the pagination clause. The migration version
// tables keep deliberately different names (goose_db_version_meta here,
// goose_db_version_utxo in sqlstore) so the two chains stay independent in a
// shared Mode-A database.
package metastore
