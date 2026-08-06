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
//     sentinel at the top level).
//  2. Claim methods are single-round-trip atomic transitions; no interface
//     method ever requires the caller to hold a lock across calls. This is
//     what makes one funder correct on both SQL and Aerospike.
//  3. Guards are exact-match state preconditions carried in the op; a failed
//     guard is per-item data, not corruption.
//  4. Idempotency: Mint (same data = no-op success), Spend (same spender =
//     success), Unspend (guard mismatch = skip), Release (already free =
//     skip), Remove (missing = no-op), Promote (already there = counts as
//     unchanged). Every op a crash-recovery outbox replays is idempotent by
//     construction.
//  5. Frozen rows are invisible to claims and refuse Spend ([FrozenError]),
//     but Release, Unspend, and Promote still apply to them.
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

	// ClaimSmallestSufficient reserves the claimable coin with the smallest
	// Satoshis >= minSats within s. Returns (nil, nil) when none qualifies.
	// Selection SHOULD be the true minimum; approximate backends may return
	// any sufficient claimable coin (see utxostoretest's exact-selection
	// option). Ties break deterministically (insertion order).
	ClaimSmallestSufficient(ctx context.Context, s Scope, reservation string, minSats uint64) (*UTXO, error)

	// ClaimLargestInsufficient reserves up to limit claimable coins with
	// Satoshis < capSats within s, largest first. Returns fewer (possibly
	// none) when the pool is short; that is not an error. limit <= 0 returns
	// (nil, nil).
	ClaimLargestInsufficient(ctx context.Context, s Scope, reservation string, capSats uint64, limit int) ([]*UTXO, error)

	// ClaimExact reserves up to count claimable coins with Satoshis ==
	// denomination within s. len(result) < count signals pool underflow and
	// is NOT an error. count <= 0 returns (nil, nil).
	ClaimExact(ctx context.Context, s Scope, reservation string, denomination uint64, count int) ([]*UTXO, error)

	// ReleaseReservation frees every unspent row held by (userID,
	// reservation) and returns how many it freed. Idempotent: an unknown or
	// already-released reservation returns 0, nil. Spent rows are never
	// touched — their reservation is provenance.
	ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error)

	// ReleaseOutpoints frees the given rows if — and only if — each is
	// currently reserved by exactly this reservation and unspent. Guard
	// mismatches, unreserved rows, spent rows, and missing outpoints are
	// per-item SKIPS, not errors (idempotency: a stale release replays
	// harmlessly).
	ReleaseOutpoints(ctx context.Context, reservation string, ops []Outpoint) error

	// --- spend lifecycle ---

	// Spend transitions rows reserved(op.Reservation) → spent(op.SpendingTxID).
	// Guards, per item: the row must exist ([NotFoundError]), must not be
	// frozen ([FrozenError]), and must be reserved by exactly op.Reservation
	// ([ReservedError] with HeldBy naming the actual holder, "" when
	// unreserved). Idempotent: a row already spent by the SAME SpendingTxID
	// succeeds; a row spent by a different transaction fails with
	// [SpentError] carrying the winner.
	//
	// Precedence: an already-recorded spend wins over a freeze — the
	// spent-state check comes BEFORE the frozen check, so a same-spender
	// replay (e.g. an outbox replaying after a crash) on a since-frozen row
	// is an idempotent success, and a different spender receives
	// [SpentError], never [FrozenError].
	//
	// Failures are reported per item via SpendOp.Err plus a top-level error
	// matching [ErrBatch].
	Spend(ctx context.Context, spends []*SpendOp) error

	// Unspend reverses recorded spends: for each op whose row has SpentBy ==
	// spendingTxID, it clears the spend AND the reservation, returning the
	// coin to the claimable pool. Guard mismatches (unspent rows, rows spent
	// by another transaction) and missing outpoints are skips, not errors.
	// Returns the number of rows released.
	Unspend(ctx context.Context, spendingTxID chainhash.Hash, ops []Outpoint) (int, error)

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
	// unspent reserved rows) whose oldest reservation time is before
	// olderThan, for the stale-reservation reaper. SHOULD return oldest
	// first; approximate backends may relax ordering. limit <= 0 returns
	// (nil, nil).
	FindStaleReservations(ctx context.Context, olderThan time.Time, limit int) ([]ReservationRef, error)

	// Close releases the store's resources. Idempotent. After Close, all
	// other methods return errors.
	Close(ctx context.Context) error
}
