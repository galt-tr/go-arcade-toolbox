package utxostore

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Store is the storage contract for the wallet's UTXO inventory.
// Implementations must be safe for concurrent use.
//
// Atomicity contracts (normative — the conformance suite enforces them):
//
//  1. Every method is atomic PER ITEM, never across items. Batch methods may
//     partially succeed and report per-item Err (plus the [ErrBatch]
//     sentinel at the top level). [Store.ReserveOutpoints] is the ONE
//     documented exception: it is all-or-nothing across its ops, because a
//     partially reserved input set is not a usable transaction.
//  2. Claim methods are single-round-trip atomic transitions; no interface
//     method ever requires the caller to hold a lock across calls. This is
//     what makes one funder correct on both SQL and Aerospike.
//  3. Guards are exact-match state preconditions carried in the op; a failed
//     guard is per-item data, not corruption.
//  4. Idempotency: Mint (same data = no-op success), Spend (same spender =
//     success, in either mode), Unspend (guard mismatch = skip), Release
//     (already free = skip), Remove (missing = no-op), Promote (already there
//     = counts as unchanged), Pin/Unpin (already in that state = uncounted
//     skip), ReserveOutpoints (rows already held by exactly this token count
//     as satisfied and are not re-stamped). Every op a crash-recovery outbox
//     replays is idempotent by construction.
//  5. Frozen rows are invisible to claims and refuse a GUARDED Spend
//     ([FrozenError]), but Release, Unspend, Promote, and a forced
//     (fact-mode) Spend still apply to them.
//  6. Pinned rows are reserved rows that no janitor may free: they are
//     invisible to FindStaleReservations and untouched by ReleaseReservation.
//     pinned = TRUE implies reserved and unspent, always. See [Store.Pin].
//  7. FOUR operations create a reservation — the three claim shapes and
//     [Store.ReserveOutpoints] — and they all produce the SAME thing: a row
//     held by (userID, reservation), unpinned, sweepable, released by token.
//     Nothing downstream distinguishes a coin the funder selected from one the
//     caller named.
type Store interface {
	// Health reports the store's health: an HTTP-convention status code
	// (200 healthy, 503 unavailable), a human-readable message, and an error
	// when unhealthy. If checkLiveness is true, the store also exercises its
	// backend (e.g. pings the database) rather than only checking local state.
	// After Close, Health MUST report unhealthy: a non-2xx status (503
	// suggested) and a non-nil error.
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// --- inventory ---

	// Mint creates coins. Idempotent per item: an existing row with the same
	// identity (UserID, Basket, Satoshis, InputSize) is a no-op success
	// regardless of its current tier/reservation/spend state; an existing row
	// with different identity fails that item with [AlreadyExistsError].
	// Failures are reported per item via Mint.Err plus a top-level error
	// matching [ErrBatch]; successful items in the same batch are committed.
	Mint(ctx context.Context, mints []*Mint) error

	// Get returns a copy of the row for op, or a [NotFoundError] when absent.
	Get(ctx context.Context, op Outpoint) (*UTXO, error)

	// Remove deletes rows explicitly — the ONLY deletion path besides
	// RemoveByMintTx; no implementation may expire rows in the background.
	// Missing outpoints are no-ops. Without force, reserved, spent, or frozen
	// rows are refused; refusals are reported as per-item typed errors
	// ([ReservedError], [SpentError], [FrozenError]) joined into the returned
	// error together with [ErrBatch], while the remaining items are still
	// removed. With force, rows are removed regardless of state.
	Remove(ctx context.Context, ops []Outpoint, force bool) error

	// --- claim/reserve: THE HOT PATH ---
	// Each claim is a single atomic transition: select rows matching the
	// predicate within s that are claimable (unreserved, unspent, not
	// frozen), mark them reserved by the given token, and return copies.
	// reservation must be non-empty and s fully specified (UserID > 0,
	// Basket != "", valid Tier); violations are plain errors.
	//
	// Ties (several claimable coins of the SAME Satoshis) break
	// deterministically, but NOT in the same direction for all three shapes:
	// insertion order (oldest first) for ClaimSmallestSufficient and
	// ClaimExact; REVERSE insertion order (newest first) for
	// ClaimLargestInsufficient — the order a DESCENDING index walk yields, so
	// the hot path stays sort-free. The consequence on a pool of identical
	// coins is that ClaimLargestInsufficient drains it LIFO; that is accepted,
	// since equal coins are equally spendable. Approximate backends (see
	// utxostoretest's exact-selection option) are held to none of this.

	// ClaimSmallestSufficient reserves the claimable coin with the smallest
	// Satoshis >= minSats within s. Returns (nil, nil) when none qualifies.
	// Selection SHOULD be the true minimum; approximate backends may return
	// any sufficient claimable coin (see utxostoretest's exact-selection
	// option). Ties break by insertion order (oldest first).
	ClaimSmallestSufficient(ctx context.Context, s Scope, reservation string, minSats uint64) (*UTXO, error)

	// ClaimLargestInsufficient reserves up to limit claimable coins with
	// Satoshis < capSats within s, largest first, ties broken NEWEST first
	// (reverse insertion order — see above). Returns fewer (possibly none)
	// when the pool is short; that is not an error. limit <= 0 returns
	// (nil, nil).
	ClaimLargestInsufficient(ctx context.Context, s Scope, reservation string, capSats uint64, limit int) ([]*UTXO, error)

	// ClaimExact reserves up to count claimable coins with Satoshis ==
	// denomination within s, in insertion order (oldest first — every row is
	// a tie by construction). len(result) < count signals pool underflow and
	// is NOT an error. count <= 0 returns (nil, nil).
	ClaimExact(ctx context.Context, s Scope, reservation string, denomination uint64, count int) ([]*UTXO, error)

	// ReserveOutpoints reserves the EXACT listed rows under (userID,
	// reservation) — the outpoint-targeted analog of the claim methods, used
	// for caller-provided wallet-owned inputs so they get the same exclusivity
	// as funded coins. ALL-OR-NOTHING: every op must exist, belong to userID,
	// and be claimable (unreserved, unspent, not frozen); if ANY op fails, NO
	// row is left newly reserved, and the error joins per-item causes
	// ([NotFoundError], [ReservedError], [SpentError], [FrozenError]) under
	// [ErrBatch]. A row that belongs to a DIFFERENT user reports
	// [NotFoundError], never [ReservedError]: the answer must not leak whether
	// another user's coin exists. Refusal precedence per row matches Remove:
	// spent > reserved-by-another > frozen. Duplicate outpoints are collapsed;
	// a refusal is reported once per DISTINCT outpoint.
	//
	// On non-transactional backends (Aerospike) the all-or-nothing guarantee is
	// enforced by compensation, so a crash on a refused reserve can leave rows
	// under the caller's token until the stale sweep reclaims them; callers may
	// ReleaseReservation on the token as insurance.
	//
	// An optimistic backend may return [ErrContention] (nothing was reserved);
	// it is retryable, not a refusal.
	//
	// Idempotent for a replay under the same reservation: rows already reserved
	// by exactly this token count as satisfied (and are not re-stamped, so
	// ReservedAt keeps naming when the hold began; a row already ours that has
	// since been FROZEN still refuses with [FrozenError] — the freeze outranks
	// the replay). reservation must be non-empty; ops must be non-empty.
	//
	// The rows it reserves are ordinary UNPINNED reservations — rule 7.
	ReserveOutpoints(ctx context.Context, userID int64, reservation string, ops []Outpoint) error

	// ReleaseReservation frees every unspent, UNPINNED row held by (userID,
	// reservation) and returns how many it freed. Idempotent: an unknown or
	// already-released reservation returns 0, nil. Spent rows are never
	// touched — their reservation is provenance — and neither are pinned rows,
	// which a signed transaction is already spending (see [Store.Pin]).
	ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error)

	// ReleaseOutpoints frees the given rows if — and only if — each is
	// currently reserved by exactly this reservation and unspent. Guard
	// mismatches, unreserved rows, spent rows, and missing outpoints are
	// per-item SKIPS, not errors (idempotency: a stale release replays
	// harmlessly).
	ReleaseOutpoints(ctx context.Context, reservation string, ops []Outpoint) error

	// --- pre-broadcast pin ---
	// A pin is the committed "a broadcastable transaction spends these coins"
	// state. Pinned rows are still reserved (a pin never exists without a
	// reservation) and refuse claims exactly as reserved rows do; what a pin
	// changes is the RELEASE side: ReleaseReservation and FindStaleReservations
	// skip pinned rows entirely, so no janitor can free the inputs of an
	// in-flight send. Only the transaction's own lifecycle clears a pin:
	// Spend, Unspend, ReleaseOutpoints (the funder's own rollback or the
	// reconciler's verified-dead release), Unpin (an abort that has already
	// fenced the raw tx), or row deletion.

	// Pin marks every unspent row reserved by (userID, reservation) as pinned
	// and returns how many rows it newly pinned. Idempotent: already-pinned
	// rows are left as-is and not counted. An unknown or fully-spent
	// reservation returns 0, nil. reservation must be non-empty.
	Pin(ctx context.Context, userID int64, reservation string) (int, error)

	// Unpin clears the pin on every unspent row reserved by (userID,
	// reservation), leaving the rows RESERVED — release is a separate step, so
	// a crash between Unpin and ReleaseReservation leaves coins merely
	// reserved (reclaimed by the stale sweep), never double-spendable.
	// Idempotent. Returns how many rows it unpinned.
	Unpin(ctx context.Context, userID int64, reservation string) (int, error)

	// --- spend lifecycle ---

	// Spend marks rows spent by op.SpendingTxID. force selects between two
	// modes that differ ONLY in which preconditions they enforce; both write
	// the same row state.
	//
	// Guarded mode (force=false) is a state TRANSITION
	// reserved(op.Reservation) → spent(op.SpendingTxID), for a spend the
	// wallet is still authoring. Guards, per item: the row must exist
	// ([NotFoundError]), must not be frozen ([FrozenError]), and must be
	// reserved by exactly op.Reservation ([ReservedError] with HeldBy naming
	// the actual holder — HeldBy == "" means the row IS in the inventory but
	// currently UNRESERVED, never that the input is external). Idempotent: a
	// row already spent by the SAME SpendingTxID succeeds; a row spent by a
	// different transaction fails with [SpentError] carrying the winner.
	//
	// Fact mode (force=true) RECORDS a spend that has already happened on the
	// network — the broadcast was accepted, so no local state may un-happen
	// it. Per item: a MISSING row still fails with [NotFoundError] (a
	// genuinely external input, and the ONLY error a caller may skip); a row
	// already spent by the SAME SpendingTxID is an idempotent success; a row
	// spent by a DIFFERENT transaction still fails with [SpentError], because
	// two spend facts cannot both hold and the caller must alert rather than
	// overwrite. No OTHER row state refuses a fact: reserved by another token,
	// unreserved, or frozen, it is RECORDED ANYWAY.
	//
	// Beyond those two, a fact-mode item can still fail for reasons that are
	// not row state: an empty op.Reservation (a programmer error) and
	// transient backend errors — an optimistic backend may report
	// [ErrContention], which is retryable, not skippable and not a double
	// spend.
	//
	// Precedence (both modes): an already-recorded spend wins over a freeze —
	// the spent-state check comes BEFORE the frozen check, so a same-spender
	// replay (e.g. an outbox replaying after a crash) on a since-frozen row
	// is an idempotent success, and a different spender receives
	// [SpentError], never [FrozenError].
	//
	// In both modes a recorded spend clears the pin — the coin is consumed,
	// not in flight — and leaves ReservedBy/ReservedAt untouched: neither mode
	// writes op.Reservation onto the row. After a guarded spend that is
	// provenance (the guard proved the token spent the coin); after a forced
	// one it is only whatever the row last carried, possibly a foreign token
	// and possibly none.
	//
	// Failures are reported per item via SpendOp.Err plus a top-level error
	// matching [ErrBatch].
	Spend(ctx context.Context, spends []*SpendOp, force bool) error

	// Unspend reverses recorded spends: for each op whose row has SpentBy ==
	// spendingTxID, it clears the spend AND the reservation, returning the
	// coin to the claimable pool. Guard mismatches (unspent rows, rows spent
	// by another transaction) and missing outpoints are skips, not errors.
	// Returns the number of rows released.
	Unspend(ctx context.Context, spendingTxID chainhash.Hash, ops []Outpoint) (int, error)

	// RemoveSpentBy deletes every row currently marked spent by spendingTxID.
	// Called when spendingTxID reaches a terminal on-chain state (mined): its
	// inputs are permanently consumed and no longer live, so they leave the hot
	// inventory (their history remains in the wallet's output ledger). Unlike
	// Unspend it restores nothing — a mined tx's inputs cannot be contested — and
	// unlike Remove it needs no explicit outpoints: every row whose SpentBy ==
	// spendingTxID goes. Idempotent (a second call removes nothing). Returns the
	// number of rows removed.
	RemoveSpentBy(ctx context.Context, spendingTxID chainhash.Hash) (int, error)

	// Promote sets the tier of each row to `to`. Downgrades are allowed
	// (reorg). Missing outpoints are skipped; rows already at the target tier
	// count as unchanged. Applies regardless of reservation, spend, or frozen
	// state. Returns the number of rows whose tier actually changed.
	Promote(ctx context.Context, ops []Outpoint, to Tier) (int, error)

	// RemoveByMintTx reacts to an oracle verdict that mintTxID is invalid:
	// its outputs are phantom coins. Every op must be an output of mintTxID
	// (op.TxID == mintTxID); a mismatch is a caller bug and fails the whole
	// call with a plain error. Each op is classified per item:
	//   - unreserved and unspent (frozen or not): removed → Removed;
	//   - reserved: kept, grouped by reservation → AlreadyReserved
	//     (descendants in flight — cascade candidates);
	//   - spent: kept, its spender recorded (deduplicated) → AlreadySpentBy
	//     (the monitor re-verifies those children);
	//   - missing: skipped.
	RemoveByMintTx(ctx context.Context, mintTxID chainhash.Hash, ops []Outpoint) (RemoveByMintReport, error)

	// --- holds / queries ---

	// Freeze places an explicit hold on rows: invisible to claims, refuse
	// Spend and un-forced Remove. Already-frozen rows are no-ops. Missing
	// outpoints are reported as per-item [NotFoundError]s joined into the
	// returned error together with [ErrBatch]; present rows are still frozen.
	Freeze(ctx context.Context, ops []Outpoint) error

	// Unfreeze lifts the hold. Already-unfrozen rows are no-ops; missing
	// outpoints are reported like in Freeze.
	Unfreeze(ctx context.Context, ops []Outpoint) error

	// Balance sums the (userID, basket) coins: claimable per tier (unspent,
	// unreserved, not frozen) and reserved (reserved, unspent — frozen or
	// not). Spent rows and frozen unreserved rows count in neither bucket.
	Balance(ctx context.Context, userID int64, basket string) (Balance, error)

	// FindStaleReservations returns up to limit reservations (grouped refs of
	// unspent, UNPINNED reserved rows) whose oldest reservation time is before
	// olderThan, for the stale-reservation reaper. Pinned rows are invisible
	// here — neither as a reason a reservation is reported nor as a member of
	// a reported ref's Outpoints — so a reaper can never free the inputs of an
	// in-flight send (see [Store.Pin]). SHOULD return oldest first; approximate
	// backends may relax ordering. limit <= 0 returns (nil, nil).
	FindStaleReservations(ctx context.Context, olderThan time.Time, limit int) ([]ReservationRef, error)

	// Close releases the store's resources. Idempotent. After Close, all
	// other methods return errors.
	Close(ctx context.Context) error
}
