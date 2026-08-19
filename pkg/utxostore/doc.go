// Package utxostore defines the storage contract for the wallet's UTXO
// inventory: the [Store] interface, its types, and its error taxonomy. Every
// storage provider (SQLite, PostgreSQL, Aerospike) implements this interface,
// and the funder and reconciler are written against it — the contracts below
// are what make one funder correct on every backend.
//
// The in-memory reference implementation lives in the memstore subpackage; the
// backend-agnostic conformance suite that every provider must pass lives in
// the utxostoretest subpackage.
//
// # Wallet semantics, not node semantics
//
// The design is inspired by teranode's stores/utxo (per-item batch errors,
// typed error taxonomy, a shared conformance suite), but a wallet's UTXO store
// answers a different question than a node's:
//
//   - Ownership: every output belongs to a (userID, basket) pair. All queries
//     are scoped; no operation ever crosses a user boundary implicitly.
//   - The hot operation is "claim any suitable coin" — an atomic
//     reserve-by-predicate (smallest sufficient, largest insufficient, exact
//     denomination) — not "spend this exact outpoint". A node validates spends
//     it is given; a wallet selects coins to fund transactions it is building.
//   - Conflict detection is external: Arcade is the transaction-truth oracle.
//     The store records what the wallet believes (reserved, spent-by, tier)
//     and the reconciler corrects that belief from oracle verdicts. The store
//     itself never adjudicates double-spends.
//
// # Deliberately dropped from teranode
//
// The following teranode features are intentionally absent:
//
//   - Alert-system reassignment (ReAssignUTXO): wallets do not enforce
//     court-ordered reassignment; coins are frozen/unfrozen explicitly instead.
//   - Conflicting-children graph (SetConflicting, GetConflictingChildren):
//     Arcade is the conflict oracle; the wallet has no mempool graph to walk.
//   - Field masks (fields.FieldName): wallet rows are small and fixed; partial
//     reads buy nothing and complicate every provider.
//   - UTXO-hash checksums: teranode hashes (txid, vout, script, satoshis) to
//     detect corrupted spends from untrusted peers; a wallet is the sole
//     writer of its own rows.
//   - Block-height push (SetBlockHeight/SetMedianBlockTime): confirmation
//     state arrives via the reconciler as explicit [Store.Promote] calls, not
//     via a store-internal height clock.
//   - Per-tx pagination records (unmined iterators, per-tx metadata pages):
//     wallet queries are per-(user, basket), never chain-wide scans.
//   - ANY TTL- or height-based deletion (DeleteAtHeight, PreserveUntil,
//     cleanup services): see below.
//
// # No TTL deletion — ever
//
// A node can delete aggressively because it can rebuild its UTXO set from the
// chain. A wallet cannot: its UTXO set is the only record of which outputs it
// controls, and silently expiring a row is indistinguishable from losing
// funds. Deletion is therefore explicit only — [Store.Remove] (operator
// intent) and [Store.RemoveByMintTx] (the reconciler reacting to an oracle
// verdict that the minting transaction is invalid). No implementation may
// add background expiry.
//
// # Tier model
//
// A coin's [Tier] tracks how settled its minting transaction is:
//
//   - [TierSending] (1): the mint transaction's broadcast is in flight.
//   - [TierUnproven] (2): the oracle accepted it (SEEN_* status) but the
//     wallet has not verified a proof.
//   - [TierMined] (3): a header-verified merkle proof exists.
//
// Tiers order risk, and the funder walks them explicitly — one tier per
// micro-query ([Scope] makes the tier mandatory) — so that every claim is a
// single index walk on every backend. [Store.Promote] moves coins between
// tiers in both directions: up as evidence arrives, down on reorg.
//
// # Pre-broadcast pin
//
// A plain reservation is a SOFT hold: the stale-reservation reaper frees one
// that has aged out. That is right while the wallet is still choosing coins,
// and wrong the moment it has SIGNED a transaction spending them — freeing
// those inputs lets a later funding run hand the same coins to a second
// transaction, and the two race on-chain as a double spend.
//
// [Store.Pin] closes that window. It sets a COMMITTED bit, written in the same
// transaction that stores the signed raw tx, which turns the reservation into a
// hard hold: pinned rows are reserved rows no janitor may free.
// [Store.ReleaseReservation] skips them and [Store.FindStaleReservations]
// cannot see them, so only the transaction's own lifecycle ends a pin —
// [Store.Spend], [Store.Unspend], a token-guarded [Store.ReleaseOutpoints] (the
// funder's own rollback and the reconciler's verified-dead release),
// [Store.Unpin], or row deletion.
//
// The invariant every provider upholds: Pinned implies ReservedBy != "" AND
// SpentBy == nil. Claims are unaffected — a pinned coin is reserved, so it was
// already invisible to them — which is why pinning costs the hot path nothing.
//
// # Spend: a guarded transition and a recorded fact
//
// [Store.Spend] serves two callers whose relationship to the network is
// opposite, and its force parameter says which one is calling.
//
// While the wallet is still AUTHORING a spend, the reservation is the
// authority: the coin may only move to spent if this funding run still holds
// it. That is the guarded mode (force=false), and every guard failure —
// frozen, held by another token, held by nobody — is a real refusal that must
// stop the caller.
//
// Once the broadcast has been ACCEPTED, the spend is a fact of the network.
// The transaction is in mempools; the inputs are consumed whatever the local
// row says. A reservation that has since been released (a reaper that fired
// mid-broadcast, an operator release, a crash-recovery path) does not make the
// spend un-happen — it only means the store's view is stale. Refusing to
// record the spend there is precisely how a lost reservation becomes a silent
// double spend: the coin stays claimable, a later funding run hands it to a
// second transaction, and the two race on-chain. So fact mode (force=true)
// records the spend over a reservation mismatch and over a freeze: no row
// STATE refuses a fact.
//
// Two STATE errors survive fact mode, for opposite reasons:
//
//   - [NotFoundError] — there is no row, so the input was never the wallet's
//     to record (a genuinely external input). This is the ONLY error a
//     fact-mode caller may skip.
//   - [SpentError] — a DIFFERENT transaction is already recorded as spending
//     the coin. Two spend facts cannot both be true; one of them is a real
//     double spend and the caller must alert, never overwrite. (A replay by
//     the SAME spender is an idempotent success, as always.)
//
// Beyond these two, a fact-mode item can still fail for reasons that are not
// row state: an empty op.Reservation (a programmer error) and transient
// backend errors — an optimistic backend may report [ErrContention], which is
// retryable, not skippable and not a double spend. A caller that skips
// everything except [SpentError] would silently drop those, which is why the
// skip list is [NotFoundError] alone and not "everything but SpentError".
//
// # Atomicity contracts
//
// These six rules are the core of the interface; the conformance suite
// enforces them and every provider must honor them:
//
//  1. Every method is atomic PER ITEM, never across items. Batch methods may
//     partially succeed; they report failures per item (via the op's Err
//     field where one exists, otherwise via typed errors joined into the
//     returned error) plus the [ErrBatch] sentinel at the top level.
//  2. Claim methods are single-round-trip atomic transitions. No interface
//     method ever requires the caller to hold a lock across calls — this is
//     what makes one funder correct on both SQL and Aerospike.
//  3. Guards are exact-match state preconditions carried in the op (a spend
//     carries its reservation; an unspend carries its spending txid). A failed
//     guard is per-item data, not corruption.
//  4. Idempotency table — every operation a crash-recovery outbox replays is
//     idempotent by construction:
//     Mint: same data again = no-op success.
//     Spend: same spender again = success (guarded and fact mode alike).
//     Unspend: guard mismatch = skip.
//     Release: already free = skip.
//     Remove: missing = no-op.
//     Promote: already at the target tier = counts as unchanged.
//     Pin/Unpin: already in that state = uncounted skip.
//  5. Frozen rows are invisible to claims and refuse a GUARDED Spend
//     ([FrozenError]), but Release, Unspend, Promote, and a forced
//     (fact-mode) Spend still apply to them. Precedence, in both spend modes:
//     an already-recorded spend wins over the freeze — a same-spender Spend
//     replay on a since-frozen row is an idempotent success, and a competing
//     spender gets [SpentError], not [FrozenError].
//  6. Pinned rows are reserved rows no janitor may free: ReleaseReservation
//     leaves them alone and FindStaleReservations never reports them (see the
//     pre-broadcast pin above). Pinned always implies reserved and unspent.
//
// # Design notes
//
// Decisions the interface commits to where the contract left latitude:
//
//   - ReleaseOutpoints guard mismatch is a per-item SKIP, not an error,
//     consistent with the idempotency table: releasing something you no
//     longer hold is a stale intent, and replays must be harmless.
//   - Spend keeps ReservedBy on the row after the transition. After a GUARDED
//     spend that is true provenance — the guard proved the token is the
//     funding run that spent the coin — and it drives the
//     AlreadyReserved/AlreadySpentBy classification in [RemoveByMintReport].
//     In fact mode the kept reservation is simply whatever the row last
//     carried — possibly a foreign token, possibly none — so it is the row's
//     last reservation, not necessarily the run that broadcast. Neither mode
//     writes op.Reservation onto the row. Unspend clears both SpentBy and the
//     reservation: the coin returns to the claimable pool.
//   - [ReservedError] is the general reservation-guard failure: HeldBy names
//     the current holder, and HeldBy == "" means the row is IN the inventory
//     but not reserved at all (e.g. a guarded Spend of a released row). It
//     never means "external input" — only [NotFoundError] means that, which
//     is why fact mode keeps NotFound and drops the reservation guard.
//   - Spend's force parameter, like Remove's, picks preconditions and not
//     effects: a forced and a guarded spend that both succeed write exactly
//     the same row state (spent_by set, pin cleared, reservation kept).
//   - Mint idempotency compares the immutable coin identity only (UserID,
//     Basket, Satoshis, InputSize). Tier is a starting state, not identity: a
//     replayed mint whose row has since been promoted, reserved, or spent is
//     still a no-op success. A mismatch in identity is [AlreadyExistsError].
//   - Remove refuses frozen rows without force, like reserved/spent ones: a
//     freeze is an explicit hold and silent deletion would defeat it.
//     RemoveByMintTx, by contrast, removes frozen-but-unreserved rows: if the
//     minting transaction is invalid the coin is phantom, and a frozen
//     phantom is strictly worse than no row.
//   - Batch methods whose ops are plain []Outpoint (Remove, Freeze, Unfreeze)
//     have no per-item Err slot; they report failures as typed per-item
//     errors joined into the returned error together with [ErrBatch]
//     (errors.Is finds ErrBatch, errors.As finds the item errors).
//   - Balance buckets: a row is Claimable (per tier) when unspent, unreserved
//     and not frozen; Reserved when reserved and unspent (frozen or not).
//     Frozen unreserved rows appear in neither bucket — a freeze removes
//     value from the spendable balance by design.
//   - The pin is scoped by (userID, reservation) like ReleaseReservation, not
//     by outpoint: the caller fences or unfences a send with the token it
//     already holds. The one release path that OVERRIDES a pin is
//     ReleaseOutpoints, which is token-guarded — its callers are the funder's
//     own rollback (whose rows are never pinned) and the reconciler's
//     verified-dead release, the two parties entitled to declare a transaction
//     dead. Unpin, by contrast, only lifts the pin: it leaves the rows
//     reserved, so a crash between Unpin and the release degrades to a stale
//     reservation the reaper handles, never to a free coin.
//   - FindStaleReservations dates a reservation by its OLDEST row (the
//     funder may claim several times under one token) and should return
//     oldest-first; approximate backends may relax the ordering (the
//     conformance suite gates the ordering assertion behind the
//     exact-selection option).
//   - Claim inputs are validated: reservation must be non-empty and the
//     [Scope] fully specified (UserID > 0, Basket != "", valid Tier). An
//     empty reservation would make reserved rows indistinguishable from free
//     ones, so it is rejected as a programmer error (plain error, not part
//     of the state-outcome taxonomy).
package utxostore
