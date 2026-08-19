// Package storage is the wallet storage backend: the [Provider] that satisfies
// the BRC-100 [wdk.WalletStorageProvider] by orchestrating three subsystems —
// the metastore (wallet metadata: users, transactions, outputs, baskets,
// labels/tags, known transactions, certificates, sync state), the utxostore
// (the hot-path UTXO inventory keyed by outpoint), and the funder (coin
// selection + change/fee computation) — plus an arcade [arcade.TxOracle] for
// broadcast and a [headers.Headers] source for merkle-root verification.
//
// # Two deployment modes
//
// Mode A ("shared database"): the metastore and utxostore run over ONE *sql.DB,
// so a single database transaction spans both stores. The provider detects this
// with metastore.SharesDatabase(utxo.DB()) and wraps the create/process/abort
// write cores in one metastore.Do unit of work — the metadata write and the
// inventory write commit or roll back atomically.
//
// Mode B ("split stores"): the utxostore is a different backend (e.g. Aerospike,
// or a separate SQL database), so the two stores cannot share a transaction. The
// inventory ops are applied DIRECTLY against the utxostore and the metadata half
// is committed in its own transaction: CreateAction reserves inputs via the
// funder, then persists metadata and COMPENSATES (a whole-token
// ReleaseReservation) if that metadata write fails; ProcessAction applies the
// Spend / Promote / Mint directly. A process that crashes between the two direct
// writes is healed by the outbox-backed crash-recovery drain: the monitor's
// reject_release task calls DrainOutbox, which replays the durable
// utxo_ops_outbox rows idempotently (oldest-first, parked after
// MaxOutboxAttempts).
//
// Two write paths do NOT apply their inventory ops directly, because for them a
// half-applied write is a double spend rather than a stale row: the reconciler's
// verified-dead release and AbortAction. Both commit the METADATA decision plus
// a durable outbox INTENT in one transaction and execute the utxo half
// afterwards, so the only residue a crash can leave is a transaction nobody can
// broadcast whose coins are still held — never freed coins behind sendable
// bytes. See [Provider.abortViaOutbox] and [Provider.releaseViaOutbox].
//
// # The txid timing decision (change minting)
//
// The utxostore is keyed by outpoint (txid:vout). A wallet-created transaction's
// txid is only known once the wallet has SIGNED it — i.e. at ProcessAction, when
// the signed raw transaction arrives — because for BSV the txid hashes the
// unlocking scripts too. CreateAction therefore CANNOT mint change inventory: it
// only reserves the funding inputs (under the transaction Reference as the
// reservation token) and persists the unsigned metadata (transaction row, change
// output rows, commission). Change inventory is minted at ProcessAction into the
// change basket at [utxostore.TierSending] and promoted to
// [utxostore.TierUnproven] once arcade accepts the broadcast (HTTP 202). The
// reserved funding inputs stay reserved through the pre-broadcast states
// (unsigned / nosend / unsent) and transition reserved→spent only on broadcast
// acceptance. This is what makes AbortAction correct: a pre-broadcast abort
// releases the reservation by the tx Reference and removes any minted change.
//
// # The spendability seam
//
// The metastore has no "spendable" column: an output is spendable iff a live
// inventory row exists for its outpoint in the utxostore. ListOutputs and
// GetBalance compose spendability by intersecting the metastore's descriptive
// rows with the utxostore's live set. See the metastore package doc.
package storage
