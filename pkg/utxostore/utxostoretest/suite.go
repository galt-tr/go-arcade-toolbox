package utxostoretest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/internal/stress"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Default scope used by most subtests.
const (
	user   int64 = 1
	basket       = "default"
)

func scope(tier utxostore.Tier) utxostore.Scope {
	return utxostore.Scope{UserID: user, Basket: basket, Tier: tier}
}

// config holds suite options.
type config struct {
	exactSelection    bool
	newStoreWithClock func(t *testing.T) (utxostore.Store, AdvanceFunc)
}

// Option configures RunStoreSuite.
type Option func(*config)

// WithExactSelection enables the assertions that require optimal claim
// selection: the true smallest sufficient coin, strict largest-first
// ordering, and oldest-first stale reservations. Exact backends (memstore,
// SQL) enable it; approximate backends (bucket-based, e.g. Aerospike) omit it
// and are held only to the claim predicates themselves.
//
// Timestamp fidelity: without [WithManualClock] the timestamp-sensitive
// subtests fall back to short wall-clock sleeps for distinctness, so a
// backend enabling WithExactSelection must persist reservation timestamps
// with sub-millisecond fidelity for the ordering assertions to hold.
func WithExactSelection() Option {
	return func(c *config) { c.exactSelection = true }
}

// AdvanceFunc advances a store's manual clock by d.
type AdvanceFunc func(d time.Duration)

// WithManualClock registers an alternative store factory whose clock the
// suite can advance deterministically (e.g. memstore's WithClock option).
// Timestamp-sensitive subtests then run without wall-clock sleeps, and the
// StaleReservationDating subtest — which pins that a reservation is dated by
// its OLDEST row — runs only when this option is provided (it is skipped
// otherwise). Providers should supply it whenever their reservation
// timestamps originate from an injectable clock. Like newStore, the factory
// must return a FRESH store per call and register cleanup on t.
func WithManualClock(newStore func(t *testing.T) (utxostore.Store, AdvanceFunc)) Option {
	return func(c *config) { c.newStoreWithClock = newStore }
}

// RunStoreSuite runs the full conformance suite against stores built by
// newStore. Each subtest receives a FRESH store; newStore should register
// cleanup on t. Run it under -race: the claim-exclusivity subtest is the core
// safety property of the interface.
func RunStoreSuite(t *testing.T, newStore func(t *testing.T) utxostore.Store, opts ...Option) {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	s := &suite{newStore: newStore, cfg: cfg}

	// std builds the default store lazily inside the subtest. Subtests that
	// provision their own clocked store (FindStaleReservations,
	// StaleReservationDating) are routed directly so providers never
	// provision-then-discard a default store (which can be expensive for
	// schema-per-store backends).
	std := func(fn func(t *testing.T, store utxostore.Store)) func(t *testing.T) {
		return func(t *testing.T) { fn(t, s.newStore(t)) }
	}

	for _, tc := range []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"Health", std(s.health)},
		{"MintIdempotency", std(s.mintIdempotency)},
		{"MintPerItemErrors", std(s.mintPerItemErrors)},
		{"GetNotFound", std(s.getNotFound)},
		{"InputValidation", std(s.inputValidation)},
		{"ClaimSmallestSufficient", std(s.claimSmallestSufficient)},
		{"ClaimLargestInsufficient", std(s.claimLargestInsufficient)},
		{"ClaimExact", std(s.claimExact)},
		{"ClaimTieBreak", std(s.claimTieBreak)},
		{"ClaimScopeIsolation", std(s.claimScopeIsolation)},
		{"ClaimExclusivityConcurrent", std(s.claimExclusivityConcurrent)},
		{"ReserveOutpointsExclusivity", std(s.reserveOutpointsExclusivity)},
		{"ReleaseReservation", std(s.releaseReservation)},
		{"ReleaseOutpoints", std(s.releaseOutpoints)},
		{"PinLifecycle", std(s.pinLifecycle)},
		{"PinScopingAndNoOps", std(s.pinScopingAndNoOps)},
		{"PinClearedBySpendAndUnspend", std(s.pinClearedBySpendAndUnspend)},
		{"ReleaseOutpointsOverridesPin", std(s.releaseOutpointsOverridesPin)},
		{"SpendLifecycle", std(s.spendLifecycle)},
		{"SpendPerItemErrors", std(s.spendPerItemErrors)},
		{"SpendPrecedence", std(s.spendPrecedence)},
		{"SpendFactMode", std(s.spendFactMode)},
		{"Unspend", std(s.unspend)},
		{"Promote", std(s.promote)},
		{"FreezeUnfreeze", std(s.freezeUnfreeze)},
		{"Remove", std(s.remove)},
		{"RemoveByMintTx", std(s.removeByMintTx)},
		{"RemoveSpentBy", std(s.removeSpentBy)},
		{"Balance", std(s.balance)},
		{"FindStaleReservations", s.findStaleReservations},
		{"FindStaleReservationsSkipsPinned", s.findStaleReservationsSkipsPinned},
		{"StaleReservationDating", s.staleReservationDating},
		{"Close", std(s.closeStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Every subtest provisions its own store (its own schema for
			// PostgreSQL, its own temp file for SQLite, its own set for
			// Aerospike), so they are independent by construction and running
			// them in parallel is safe.
			//
			// It is also the point. internal/testenv's IsolatedSchemaDSN exists
			// specifically to make this possible, and nothing used it: the whole
			// suite ran serially against one container, so the backend was never
			// asked to serve concurrent unrelated work — which is exactly the
			// condition a 1000-TPS deployment puts it under. Serial subtests
			// cannot surface a lock the store takes too broadly.
			t.Parallel()
			tc.fn(t)
		})
	}
}

type suite struct {
	newStore func(t *testing.T) utxostore.Store
	cfg      config
}

func (s *suite) health(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	status, msg, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.NotEmpty(t, msg)
	require.GreaterOrEqual(t, status, 200)
	require.Less(t, status, 300)

	// The unhealthy path: after Close, Health MUST report unhealthy — a
	// non-2xx status (memstore uses 503) and a non-nil error.
	require.NoError(t, store.Close(ctx))
	status, _, err = store.Health(ctx, false)
	require.Error(t, err, "Health after Close must report unhealthy")
	require.True(t, status < 200 || status >= 300,
		"Health after Close must return a non-2xx status, got %d", status)
}

func (s *suite) mintIdempotency(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	ops := MintTx(t, store, "mint-idem", user, basket, utxostore.TierMined, 100, 200)

	// Same data twice: no-op success, no per-item errors.
	replay := []*utxostore.Mint{
		NewMint(ops[0], user, basket, utxostore.TierMined, 100),
		NewMint(ops[1], user, basket, utxostore.TierMined, 200),
	}
	require.NoError(t, store.Mint(ctx, replay))
	for _, m := range replay {
		require.NoError(t, m.Err)
	}

	// Same outpoint, DIFFERENT data: AlreadyExistsError per item + ErrBatch.
	conflict := NewMint(ops[0], user, basket, utxostore.TierMined, 999)
	err := store.Mint(ctx, []*utxostore.Mint{conflict})
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, conflict.Err, &utxostore.AlreadyExistsError{})
	var existsErr *utxostore.AlreadyExistsError
	require.ErrorAs(t, conflict.Err, &existsErr)
	require.Equal(t, ops[0], existsErr.Op)

	// The original row is untouched.
	u, err := store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.Equal(t, uint64(100), u.Satoshis)

	// Tier and lifecycle state are NOT identity: a replay after the row was
	// reserved (and would-be promoted) is still a no-op success.
	claimed, err := store.ClaimExact(ctx, scope(utxostore.TierMined), "res-idem", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	replayAfterClaim := NewMint(ops[0], user, basket, utxostore.TierSending, 100)
	require.NoError(t, store.Mint(ctx, []*utxostore.Mint{replayAfterClaim}))
	require.NoError(t, replayAfterClaim.Err)

	// Still reserved: the replay must not have reset lifecycle state.
	u, err = store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.Equal(t, "res-idem", u.ReservedBy)
	require.Equal(t, utxostore.TierMined, u.Tier)
}

func (s *suite) mintPerItemErrors(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	ops := MintTx(t, store, "mint-shape", user, basket, utxostore.TierMined, 100)

	// Batch with exactly one bad item: ErrBatch + only that item's Err set,
	// and the good items are still committed.
	batch := []*utxostore.Mint{
		NewMint(NewOutpoint("mint-shape-b", 0), user, basket, utxostore.TierMined, 10),
		NewMint(ops[0], user, basket, utxostore.TierMined, 555), // conflict
		NewMint(NewOutpoint("mint-shape-b", 1), user, basket, utxostore.TierMined, 30),
	}
	err := store.Mint(ctx, batch)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.NoError(t, batch[0].Err)
	require.ErrorIs(t, batch[1].Err, &utxostore.AlreadyExistsError{})
	require.NoError(t, batch[2].Err)

	for _, op := range []utxostore.Outpoint{batch[0].Outpoint, batch[2].Outpoint} {
		_, err := store.Get(ctx, op)
		require.NoError(t, err, "good item %s in a partially failing batch must be committed", op)
	}
}

func (s *suite) getNotFound(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	missing := NewOutpoint("no-such-tx", 0)
	_, err := store.Get(ctx, missing)
	require.ErrorIs(t, err, &utxostore.NotFoundError{})
	var nf *utxostore.NotFoundError
	require.ErrorAs(t, err, &nf)
	require.Equal(t, missing, nf.Op)
}

// inputValidation pins the programmer-error rejections: empty reservation
// tokens, underspecified scopes, and RemoveByMintTx ops that do not belong to
// the mint transaction. Rejected calls must leave state untouched.
func (s *suite) inputValidation(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := MintTx(t, store, "validate", user, basket, utxostore.TierMined, 100)

	// Empty reservation is rejected by every claim shape (an empty token
	// would make reserved rows indistinguishable from free ones)...
	_, err := store.ClaimSmallestSufficient(ctx, sc, "", 1)
	require.Error(t, err, "ClaimSmallestSufficient must reject an empty reservation")
	_, err = store.ClaimLargestInsufficient(ctx, sc, "", 1_000, 5)
	require.Error(t, err, "ClaimLargestInsufficient must reject an empty reservation")
	_, err = store.ClaimExact(ctx, sc, "", 100, 1)
	require.Error(t, err, "ClaimExact must reject an empty reservation")

	// ...and by both release paths (an empty guard would match free rows).
	_, err = store.ReleaseReservation(ctx, user, "")
	require.Error(t, err, "ReleaseReservation must reject an empty reservation")
	err = store.ReleaseOutpoints(ctx, "", ops)
	require.Error(t, err, "ReleaseOutpoints must reject an empty reservation")

	// ...and by both pin toggles, which are scoped by the very same token.
	_, err = store.Pin(ctx, user, "")
	require.Error(t, err, "Pin must reject an empty reservation")
	_, err = store.Unpin(ctx, user, "")
	require.Error(t, err, "Unpin must reject an empty reservation")

	// ...and by the outpoint-targeted reservation, which additionally rejects
	// an EMPTY op list: a caller that names zero inputs has lost its input
	// list, and an all-or-nothing guarantee over nothing is not a success.
	require.Error(t, store.ReserveOutpoints(ctx, user, "", ops),
		"ReserveOutpoints must reject an empty reservation")
	require.Error(t, store.ReserveOutpoints(ctx, user, "res-v", nil),
		"ReserveOutpoints must reject an empty op list")

	// Spend with an empty reservation guard: per-item failure under ErrBatch.
	sp := &utxostore.SpendOp{Outpoint: ops[0], Reservation: "", SpendingTxID: NewTxID("validate-tx")}
	err = store.Spend(ctx, []*utxostore.SpendOp{sp}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.Error(t, sp.Err, "Spend must reject an empty reservation per item")

	// ...including in fact mode: force waives STATE guards, never the
	// programmer-error check. The token is unread there, but a caller that
	// cannot name the funding run it is recording has a bug.
	forced := &utxostore.SpendOp{Outpoint: ops[0], Reservation: "", SpendingTxID: NewTxID("validate-tx")}
	err = store.Spend(ctx, []*utxostore.SpendOp{forced}, true)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.Error(t, forced.Err, "force must not waive the non-empty reservation check")

	// Underspecified scopes are rejected by every claim shape.
	for name, bad := range map[string]utxostore.Scope{
		"zero user":    {UserID: 0, Basket: basket, Tier: utxostore.TierMined},
		"empty basket": {UserID: user, Basket: "", Tier: utxostore.TierMined},
		"invalid tier": {UserID: user, Basket: basket, Tier: 0},
	} {
		_, err := store.ClaimSmallestSufficient(ctx, bad, "res-v", 1)
		require.Error(t, err, "ClaimSmallestSufficient must reject scope with %s", name)
		_, err = store.ClaimLargestInsufficient(ctx, bad, "res-v", 1_000, 5)
		require.Error(t, err, "ClaimLargestInsufficient must reject scope with %s", name)
		_, err = store.ClaimExact(ctx, bad, "res-v", 100, 1)
		require.Error(t, err, "ClaimExact must reject scope with %s", name)
	}

	// None of the rejected calls may have touched the row.
	u, err := store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.Empty(t, u.ReservedBy)
	require.Nil(t, u.SpentBy)

	// RemoveByMintTx with an op that is not an output of mintTxID: whole-call
	// error, nothing removed — not even ops that do belong.
	own := MintTx(t, store, "validate-own", user, basket, utxostore.TierMined, 10)
	foreign := ops[0]
	_, err = store.RemoveByMintTx(ctx, NewTxID("validate-own"), []utxostore.Outpoint{own[0], foreign})
	require.Error(t, err, "RemoveByMintTx must reject ops that do not belong to mintTxID")
	for _, op := range []utxostore.Outpoint{own[0], foreign} {
		_, err = store.Get(ctx, op)
		require.NoError(t, err, "whole-call rejection must not remove %s", op)
	}
}

// requireClaimed asserts the invariants every claimed coin must satisfy.
func requireClaimed(t *testing.T, u *utxostore.UTXO, sc utxostore.Scope, reservation string) {
	t.Helper()
	require.Equal(t, sc.UserID, u.UserID)
	require.Equal(t, sc.Basket, u.Basket)
	require.Equal(t, sc.Tier, u.Tier)
	require.Equal(t, reservation, u.ReservedBy)
	require.False(t, u.ReservedAt.IsZero(), "reserved coin must carry ReservedAt")
	require.Nil(t, u.SpentBy)
	require.False(t, u.Frozen)
	require.False(t, u.Pinned, "a freshly claimed coin is never pinned")
}

// requirePinInvariant asserts the store-wide pin invariant on one row: a pin
// only ever exists on top of a live reservation, so pinned => reserved AND
// unspent. The pin subtests call it after every mutation.
func requirePinInvariant(t *testing.T, u *utxostore.UTXO) {
	t.Helper()
	if !u.Pinned {
		return
	}
	require.NotEmpty(t, u.ReservedBy, "pinned row %s must be reserved", u.Outpoint)
	require.Nil(t, u.SpentBy, "pinned row %s must be unspent", u.Outpoint)
}

// getPinChecked fetches op and returns it after asserting the pin invariant,
// so every read inside the pin subtests re-proves it.
func getPinChecked(ctx context.Context, t *testing.T, store utxostore.Store, op utxostore.Outpoint) *utxostore.UTXO {
	t.Helper()
	u, err := store.Get(ctx, op)
	require.NoError(t, err)
	requirePinInvariant(t, u)
	return u
}

func (s *suite) claimSmallestSufficient(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "smallest", user, basket, utxostore.TierMined, 5, 10, 20, 50)

	first, err := store.ClaimSmallestSufficient(ctx, sc, "res-1", 10)
	require.NoError(t, err)
	require.NotNil(t, first)
	requireClaimed(t, first, sc, "res-1")
	require.GreaterOrEqual(t, first.Satoshis, uint64(10))
	if s.cfg.exactSelection {
		require.Equal(t, uint64(10), first.Satoshis, "exact selection: true minimum >= minSats")
	}

	// The claimed coin is reserved in the store, not just in the copy.
	got, err := store.Get(ctx, first.Outpoint)
	require.NoError(t, err)
	require.Equal(t, "res-1", got.ReservedBy)

	// A second claim must not see the first coin again.
	second, err := store.ClaimSmallestSufficient(ctx, sc, "res-2", 10)
	require.NoError(t, err)
	require.NotNil(t, second)
	requireClaimed(t, second, sc, "res-2")
	require.GreaterOrEqual(t, second.Satoshis, uint64(10))
	require.NotEqual(t, first.Outpoint, second.Outpoint)
	if s.cfg.exactSelection {
		require.Equal(t, uint64(20), second.Satoshis)
	}

	// Nothing sufficient: nil, nil — not an error.
	none, err := store.ClaimSmallestSufficient(ctx, sc, "res-3", 1_000)
	require.NoError(t, err)
	require.Nil(t, none)
}

func (s *suite) claimLargestInsufficient(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "largest", user, basket, utxostore.TierMined, 5, 10, 20, 50)

	// All claimable coins strictly below the cap, largest first.
	got, err := store.ClaimLargestInsufficient(ctx, sc, "res-1", 20, 10)
	require.NoError(t, err)
	require.LessOrEqual(t, len(got), 10)
	seen := map[utxostore.Outpoint]bool{}
	for _, u := range got {
		requireClaimed(t, u, sc, "res-1")
		require.Less(t, u.Satoshis, uint64(20))
		require.False(t, seen[u.Outpoint], "claim returned duplicate outpoint %s", u.Outpoint)
		seen[u.Outpoint] = true
	}
	if s.cfg.exactSelection {
		require.Equal(t, []uint64{10, 5}, satsOf(got), "exact selection: largest first")
	}

	// limit bounds the batch; already-claimed coins are invisible.
	got, err = store.ClaimLargestInsufficient(ctx, sc, "res-2", 1_000, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	requireClaimed(t, got[0], sc, "res-2")
	require.Less(t, got[0].Satoshis, uint64(1_000))
	require.False(t, seen[got[0].Outpoint])
	if s.cfg.exactSelection {
		require.Equal(t, uint64(50), got[0].Satoshis)
	}

	// Nothing below the cap: empty, not an error.
	got, err = store.ClaimLargestInsufficient(ctx, sc, "res-3", 5, 10)
	require.NoError(t, err)
	require.Empty(t, got)
}

func (s *suite) claimExact(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "exact", user, basket, utxostore.TierMined, 10, 10, 10, 20)

	// Denomination filter is exact by definition, in every selection mode.
	got, err := store.ClaimExact(ctx, sc, "res-1", 10, 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	for _, u := range got {
		requireClaimed(t, u, sc, "res-1")
		require.Equal(t, uint64(10), u.Satoshis)
	}
	require.NotEqual(t, got[0].Outpoint, got[1].Outpoint)

	// Pool underflow: short result, nil error.
	got, err = store.ClaimExact(ctx, sc, "res-2", 10, 5)
	require.NoError(t, err)
	require.Len(t, got, 1, "one 10-sat coin left; underflow returns short, not an error")
	require.Equal(t, uint64(10), got[0].Satoshis)

	// No coin of that denomination at all.
	got, err = store.ClaimExact(ctx, sc, "res-3", 7, 3)
	require.NoError(t, err)
	require.Empty(t, got)
}

// claimTieBreak pins the tie-break DIRECTION every exact backend must agree
// on when several claimable coins carry the same satoshi value — the common
// case for a fuel pool of equal-denomination coins, where the value comparator
// decides nothing and the tie-break decides everything.
//
// The directions are deliberately NOT uniform, and that asymmetry is the
// spec:
//
//   - ClaimSmallestSufficient and ClaimExact break ties OLDEST first
//     (insertion order), so an equal-value pool drains FIFO.
//   - ClaimLargestInsufficient breaks ties NEWEST first, because its scan runs
//     the index backwards (satoshis DESC) and a descending walk yields the
//     newest of an equal-value run first. Demanding oldest-first here would
//     force a sort node on top of the claim index on the 1000-TPS hot path.
//
// Gated on exact selection: approximate backends (bucket-based, e.g.
// Aerospike) may return any coin satisfying the predicate, so no tie order is
// defined for them.
func (s *suite) claimTieBreak(t *testing.T, store utxostore.Store) {
	if !s.cfg.exactSelection {
		t.Skip("requires WithExactSelection: tie order is undefined for approximate selection")
	}
	ctx := context.Background()

	const tieSats = 7_000

	// Four equal-value coins minted a, b, c, d — one transaction at a time, so
	// insertion order is unambiguous on every backend (a batch mint would leave
	// the intra-batch order to the engine). Each claim shape gets its OWN scope
	// so it sees all four coins unclaimed; identity is the outpoint, since the
	// satoshi values are indistinguishable by construction.
	mintTie := func(t *testing.T, basketName string) (utxostore.Scope, []utxostore.Outpoint) {
		t.Helper()
		ops := make([]utxostore.Outpoint, 0, 4)
		for _, coin := range []string{"a", "b", "c", "d"} {
			minted := MintTx(t, store, basketName+"-"+coin, user, basketName, utxostore.TierMined, tieSats)
			ops = append(ops, minted[0])
		}
		return utxostore.Scope{UserID: user, Basket: basketName, Tier: utxostore.TierMined}, ops
	}

	// ClaimExact: oldest first, so two coins are a then b.
	sc, ops := mintTie(t, "tie-exact")
	exact, err := store.ClaimExact(ctx, sc, "res-exact", tieSats, 2)
	require.NoError(t, err)
	for _, u := range exact {
		requireClaimed(t, u, sc, "res-exact")
	}
	require.Equal(t, []utxostore.Outpoint{ops[0], ops[1]}, opsOf(exact),
		"ClaimExact must break ties oldest-first (insertion order)")

	// ClaimSmallestSufficient: oldest first, so the one coin is a.
	sc, ops = mintTie(t, "tie-smallest")
	smallest, err := store.ClaimSmallestSufficient(ctx, sc, "res-smallest", tieSats)
	require.NoError(t, err)
	require.NotNil(t, smallest)
	requireClaimed(t, smallest, sc, "res-smallest")
	require.Equal(t, ops[0], smallest.Outpoint,
		"ClaimSmallestSufficient must break ties oldest-first (insertion order)")

	// ClaimLargestInsufficient: NEWEST first, so two coins are d then c.
	sc, ops = mintTie(t, "tie-largest")
	largest, err := store.ClaimLargestInsufficient(ctx, sc, "res-largest", tieSats+1, 2)
	require.NoError(t, err)
	for _, u := range largest {
		requireClaimed(t, u, sc, "res-largest")
	}
	require.Equal(t, []utxostore.Outpoint{ops[3], ops[2]}, opsOf(largest),
		"ClaimLargestInsufficient must break ties newest-first (reverse insertion order)")
}

func (s *suite) claimScopeIsolation(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// One coin in scope; better-fitting coins in a different user, basket,
	// and tier. Claims must never cross the scope boundary.
	inScope := MintTx(t, store, "iso-in", user, basket, utxostore.TierMined, 50)
	MintTx(t, store, "iso-user", user+1, basket, utxostore.TierMined, 10)
	MintTx(t, store, "iso-basket", user, "other-basket", utxostore.TierMined, 10)
	MintTx(t, store, "iso-tier", user, basket, utxostore.TierUnproven, 10)

	u, err := store.ClaimSmallestSufficient(ctx, sc, "res-1", 1)
	require.NoError(t, err)
	require.NotNil(t, u)
	require.Equal(t, inScope[0], u.Outpoint, "only the in-scope coin is eligible")

	// Scope is now exhausted for every claim shape, despite the three
	// out-of-scope 10-sat coins.
	none, err := store.ClaimSmallestSufficient(ctx, sc, "res-2", 1)
	require.NoError(t, err)
	require.Nil(t, none)

	batch, err := store.ClaimLargestInsufficient(ctx, sc, "res-2", 1_000, 10)
	require.NoError(t, err)
	require.Empty(t, batch)

	batch, err = store.ClaimExact(ctx, sc, "res-2", 10, 3)
	require.NoError(t, err)
	require.Empty(t, batch)

	// Out-of-scope coins are untouched.
	for _, op := range []utxostore.Outpoint{
		NewOutpoint("iso-user", 0), NewOutpoint("iso-basket", 0), NewOutpoint("iso-tier", 0),
	} {
		other, err := store.Get(ctx, op)
		require.NoError(t, err)
		require.Empty(t, other.ReservedBy)
	}
}

// claimExclusivityConcurrent is THE core safety property: under concurrent
// claiming across all three claim shapes, no outpoint is ever handed to two
// reservations, and no coin is lost. ErrContention is tolerated as a
// transient, retryable response (optimistic backends emit it under load);
// the perfect-partition assertion stays strict.
func (s *suite) claimExclusivityConcurrent(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	const (
		denomination         = 1_000
		perClaimBatchMax     = 3
		maxContentionRetries = 10_000 // per worker; ample for a transient signal
	)
	// Scaled by ARCADE_STRESS (see internal/stress): the default run stays at
	// 3 rounds of 90 coins, and a soak run turns the same assertions into a
	// long one. Exclusivity is a property that fails rarely, so the count it
	// runs at is the only thing separating "proven" from "not yet observed".
	var (
		rounds          = stress.Scale(3)
		coinsPerRound   = stress.Scale(90)
		workersPerShape = stress.Scale(3)
	)

	// Every shape claims from the same uniform pool, so all coins are
	// eligible for all workers.
	shapes := []struct {
		name  string
		claim func(token string) ([]*utxostore.UTXO, error)
	}{
		{"small", func(token string) ([]*utxostore.UTXO, error) {
			u, err := store.ClaimSmallestSufficient(ctx, sc, token, 1)
			if u == nil {
				return nil, err
			}
			return []*utxostore.UTXO{u}, err
		}},
		{"large", func(token string) ([]*utxostore.UTXO, error) {
			return store.ClaimLargestInsufficient(ctx, sc, token, denomination+1, perClaimBatchMax)
		}},
		{"exact", func(token string) ([]*utxostore.UTXO, error) {
			return store.ClaimExact(ctx, sc, token, denomination, perClaimBatchMax)
		}},
	}

	for round := 0; round < rounds; round++ {
		sats := make([]uint64, coinsPerRound)
		for i := range sats {
			sats[i] = denomination
		}
		MintTx(t, store, fmt.Sprintf("race-%d", round), user, basket, utxostore.TierMined, sats...)

		var (
			mu        sync.Mutex
			claimedBy = make(map[utxostore.Outpoint]string, coinsPerRound)
		)
		record := func(token string, coins []*utxostore.UTXO) {
			mu.Lock()
			defer mu.Unlock()
			for _, u := range coins {
				assert.Equal(t, token, u.ReservedBy)
				if holder, dup := claimedBy[u.Outpoint]; dup {
					t.Errorf("outpoint %s claimed by both %q and %q", u.Outpoint, holder, token)
					continue
				}
				claimedBy[u.Outpoint] = token
			}
		}

		var wg sync.WaitGroup
		for w := 0; w < workersPerShape; w++ {
			for _, shape := range shapes {
				token := fmt.Sprintf("%s-%d-%d", shape.name, round, w)
				claim := shape.claim
				wg.Add(1)
				go func() {
					defer wg.Done()
					contention := 0
					for {
						coins, err := claim(token)
						if errors.Is(err, utxostore.ErrContention) {
							// Legal transient response: retry within a bound.
							contention++
							if contention > maxContentionRetries {
								t.Errorf("worker %s: contention retries exhausted", token)
								return
							}
							continue
						}
						if !assert.NoError(t, err) || len(coins) == 0 {
							return
						}
						record(token, coins)
					}
				}()
			}
		}
		wg.Wait()

		require.Len(t, claimedBy, coinsPerRound,
			"round %d: every coin must be claimed exactly once", round)
	}
}

// reserveOutpointsExclusivity is the outpoint-targeted half of the exclusivity
// contract: coins the CALLER named must be held exactly as tightly as coins the
// funder selected, and a named input set is all-or-nothing — a half-reserved
// set is not a transaction, so a single bad op must leave every other row
// exactly as it found it.
func (s *suite) reserveOutpointsExclusivity(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// --- happy path: reserving by outpoint produces an ordinary reservation --
	ops := MintTx(t, store, "ro-happy", user, basket, utxostore.TierMined, 11, 12, 13)
	require.NoError(t, store.ReserveOutpoints(ctx, user, "res-ro", ops))

	reservedAt := make(map[utxostore.Outpoint]time.Time, len(ops))
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		// A named reservation is held to the SAME invariants a claim is: this
		// is rule 7 in assertion form.
		requireClaimed(t, u, sc, "res-ro")
		require.False(t, u.Pinned,
			"ReserveOutpoints must not pin: a reservation becomes a pin only when a broadcastable tx exists")
		reservedAt[op] = u.ReservedAt
	}

	// The pool is exactly those three rows, and every one of them is held, so
	// a claim must find nothing.
	none, err := store.ClaimSmallestSufficient(ctx, sc, "res-claim", 1)
	require.NoError(t, err)
	require.Nil(t, none, "rows held by ReserveOutpoints must be invisible to claims")

	// ...and being unpinned, they are ordinary sweepable work: the reaper must
	// see this token exactly as it sees a funder's. A named reservation that
	// hid from the sweep would strand the caller's coins on any abandoned
	// request.
	refs, err := store.FindStaleReservations(ctx, reservedAt[ops[0]].Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, refs, 1, "an unpinned named reservation must be visible to the stale reaper")
	require.Equal(t, "res-ro", refs[0].Reservation)
	require.Equal(t, user, refs[0].UserID)
	require.ElementsMatch(t, ops, refs[0].Outpoints,
		"the ref must list every row the named reservation holds")

	// Replay under the SAME token: satisfied, and nothing is re-stamped.
	require.NoError(t, store.ReserveOutpoints(ctx, user, "res-ro", ops),
		"a replay under the same reservation must be an idempotent success")
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		require.Equal(t, "res-ro", u.ReservedBy)
		require.Equal(t, reservedAt[op], u.ReservedAt,
			"a same-token replay must not re-stamp ReservedAt")
	}

	// The hold is released by the ordinary token path, like any reservation.
	n, err := store.ReleaseReservation(ctx, user, "res-ro")
	require.NoError(t, err)
	require.Equal(t, len(ops), n)
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		require.Empty(t, u.ReservedBy)
		require.True(t, u.ReservedAt.IsZero())
	}
	back, err := store.ClaimSmallestSufficient(ctx, sc, "res-back", 1)
	require.NoError(t, err)
	require.NotNil(t, back, "released rows are claimable again")
	_, err = store.ReleaseReservation(ctx, user, "res-back")
	require.NoError(t, err)

	// --- all-or-nothing: one bad op poisons the whole call ------------------
	s.reserveOutpointsAllOrNothing(t, store)

	// --- duplicates name ONE row, so they get one answer --------------------
	dup := MintTx(t, store, "ro-dup", user, basket, utxostore.TierMined, 41)[0]
	require.NoError(t, store.ReserveOutpoints(ctx, user, "res-dup", []utxostore.Outpoint{dup, dup}),
		"a repeated outpoint is the same row twice, not a conflict with itself")
	requireClaimed(t, getPinChecked(ctx, t, store, dup), sc, "res-dup")

	gone := NewOutpoint("ro-dup-never-minted", 0)
	dupErr := store.ReserveOutpoints(ctx, user, "res-dup-missing", []utxostore.Outpoint{gone, gone})
	require.ErrorIs(t, dupErr, utxostore.ErrBatch)
	require.Len(t, batchItemErrors(dupErr), 1,
		"duplicate outpoints are collapsed: one refusal per DISTINCT outpoint, not one per mention")

	// --- cross-user: another user's coin reads as absent, never as reserved --
	mine := MintTx(t, store, "ro-xuser-mine", user, basket, utxostore.TierMined, 21, 22)
	theirs := MintTx(t, store, "ro-xuser-theirs", user+1, basket, utxostore.TierMined, 23)
	xuser := append(append([]utxostore.Outpoint{}, mine...), theirs[0])

	err = store.ReserveOutpoints(ctx, user, "res-xuser", xuser)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, err, &utxostore.NotFoundError{},
		"another user's coin must read as absent, not as reserved — the reply must not confirm it exists")
	var nf *utxostore.NotFoundError
	require.ErrorAs(t, err, &nf)
	require.Equal(t, theirs[0], nf.Op)
	for _, op := range xuser {
		u := getPinChecked(ctx, t, store, op)
		require.Empty(t, u.ReservedBy, "a rejected call must leave %s untouched", op)
		require.True(t, u.ReservedAt.IsZero(), "an unreserved row must carry no ReservedAt")
	}

	// --- race: a claim and a reservation may not both take the same coin ----
	s.reserveOutpointsRacesClaim(t, store, sc)
}

// reserveOutpointsAllOrNothing drives the four ways one op can refuse — held by
// another token, spent, frozen, absent — and pins that each returns its typed
// cause under ErrBatch while leaving the batch's OTHER rows completely
// unreserved. A partially reserved input set would be the worst outcome: the
// caller sees a failure and the coins stay stuck until the reaper fires.
func (s *suite) reserveOutpointsAllOrNothing(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	// Each case owns a disjoint denomination range, so the reclaim assertion
	// below can only ever select the row this case minted.
	for i, tc := range []struct {
		name string
		// spoil makes the last op of the batch unreservable and returns the op
		// to substitute in (the same one, unless the case needs a missing row).
		spoil  func(t *testing.T, op utxostore.Outpoint) utxostore.Outpoint
		expect error
		// verify inspects the typed error the call must have joined.
		verify func(t *testing.T, err error, bad utxostore.Outpoint)
	}{
		{
			name: "reserved-by-other",
			spoil: func(t *testing.T, op utxostore.Outpoint) utxostore.Outpoint {
				require.NoError(t, store.ReserveOutpoints(ctx, user, "res-holder", []utxostore.Outpoint{op}))
				return op
			},
			expect: &utxostore.ReservedError{},
			verify: func(t *testing.T, err error, bad utxostore.Outpoint) {
				var re *utxostore.ReservedError
				require.ErrorAs(t, err, &re)
				require.Equal(t, bad, re.Op)
				require.Equal(t, "res-holder", re.HeldBy, "ReservedError must name the holder")
			},
		},
		{
			name: "spent",
			spoil: func(t *testing.T, op utxostore.Outpoint) utxostore.Outpoint {
				require.NoError(t, store.ReserveOutpoints(ctx, user, "res-spender", []utxostore.Outpoint{op}))
				sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-spender", SpendingTxID: NewTxID("ro-spender")}
				require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
				return op
			},
			expect: &utxostore.SpentError{},
			verify: func(t *testing.T, err error, bad utxostore.Outpoint) {
				var se *utxostore.SpentError
				require.ErrorAs(t, err, &se)
				require.Equal(t, bad, se.Op)
				require.Equal(t, NewTxID("ro-spender"), se.Winner,
					"a spend outranks the reservation that produced it")
			},
		},
		{
			name: "frozen",
			spoil: func(t *testing.T, op utxostore.Outpoint) utxostore.Outpoint {
				require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{op}))
				return op
			},
			expect: &utxostore.FrozenError{},
			verify: func(t *testing.T, err error, bad utxostore.Outpoint) {
				var fe *utxostore.FrozenError
				require.ErrorAs(t, err, &fe)
				require.Equal(t, bad, fe.Op)
			},
		},
		{
			name: "missing",
			spoil: func(_ *testing.T, _ utxostore.Outpoint) utxostore.Outpoint {
				return NewOutpoint("ro-never-minted", 7)
			},
			expect: &utxostore.NotFoundError{},
			verify: func(t *testing.T, err error, bad utxostore.Outpoint) {
				var nf *utxostore.NotFoundError
				require.ErrorAs(t, err, &nf)
				require.Equal(t, bad, nf.Op)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := "ro-aon-" + tc.name
			base := uint64(1_000 + i*10) //nolint:gosec // i is a small loop index
			minted := MintTx(t, store, seed, user, basket, utxostore.TierMined, base, base+1, base+2, base+3)
			good := minted[:3]
			bad := tc.spoil(t, minted[3])

			token := "res-aon-" + tc.name
			err := store.ReserveOutpoints(ctx, user, token, append(append([]utxostore.Outpoint{}, good...), bad))
			require.ErrorIs(t, err, utxostore.ErrBatch,
				"a refused op must surface under the batch sentinel")
			require.ErrorIs(t, err, tc.expect)
			tc.verify(t, err, bad)

			// The whole point: NOTHING was newly reserved.
			for _, op := range good {
				u := getPinChecked(ctx, t, store, op)
				require.Empty(t, u.ReservedBy,
					"all-or-nothing: %s must not be left reserved when a sibling op fails", op)
				require.True(t, u.ReservedAt.IsZero())
			}
			// ...and the good rows are still claimable.
			claimed, cerr := store.ClaimExact(ctx, scope(utxostore.TierMined), token+"-after", base, 1)
			require.NoError(t, cerr)
			require.Len(t, claimed, 1, "a failed reservation must leave its rows in the claimable pool")
			require.Equal(t, good[0], claimed[0].Outpoint)
			_, rerr := store.ReleaseReservation(ctx, user, token+"-after")
			require.NoError(t, rerr)
		})
	}
}

// reserveOutpointsRacesClaim runs a named reservation head-on against a scope
// claim for the SAME coin. Exactly one may take it: this is the property that
// makes caller-provided inputs safe, and it is why ReserveOutpoints exists at
// all. ErrContention is a legal transient answer from optimistic backends (the
// caller retries), and a losing reservation legitimately surfaces its refusal
// as a ReservedError under ErrBatch.
func (s *suite) reserveOutpointsRacesClaim(t *testing.T, store utxostore.Store, sc utxostore.Scope) {
	ctx := context.Background()

	// Far larger than every other coin this subtest mints, so the claim below
	// can only ever select this one row.
	const bigSats = 9_000_000
	const maxContentionRetries = 10_000

	raceOp := MintTx(t, store, "ro-race", user, basket, utxostore.TierMined, bigSats)[0]

	var (
		mu      sync.Mutex
		winners []string
	)
	win := func(token string) {
		mu.Lock()
		defer mu.Unlock()
		winners = append(winners, token)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for retries := 0; ; retries++ {
			err := store.ReserveOutpoints(ctx, user, "res-race-named", []utxostore.Outpoint{raceOp})
			switch {
			case err == nil:
				win("res-race-named")
			case errors.Is(err, utxostore.ErrContention):
				if retries < maxContentionRetries {
					continue // legal transient answer: retry within a bound
				}
				t.Errorf("ReserveOutpoints: contention retries exhausted")
			default:
				// The ONLY legal loss is a refusal. Checking the batch
				// sentinel first, with its own message, keeps a genuinely
				// broken backend from reading as a lost race — which is
				// exactly the confusion that would let an exclusivity bug
				// hide in this test.
				if !assert.ErrorIs(t, err, utxostore.ErrBatch,
					"a losing ReserveOutpoints must REFUSE under ErrBatch, not fail outright: %v", err) {
					return
				}
				assert.ErrorIs(t, err, &utxostore.ReservedError{},
					"the loser's refusal must be the claim's hold on the coin")
			}
			return
		}
	}()
	go func() {
		defer wg.Done()
		for retries := 0; ; retries++ {
			u, err := store.ClaimSmallestSufficient(ctx, sc, "res-race-claim", bigSats)
			if errors.Is(err, utxostore.ErrContention) {
				if retries < maxContentionRetries {
					continue
				}
				t.Errorf("ClaimSmallestSufficient: contention retries exhausted")
				return
			}
			if assert.NoError(t, err) && u != nil {
				win("res-race-claim")
			}
			return
		}
	}()
	wg.Wait()

	require.Len(t, winners, 1,
		"a claim and a named reservation must never both take %s", raceOp)
	u := getPinChecked(ctx, t, store, raceOp)
	require.Equal(t, winners[0], u.ReservedBy)
	require.False(t, u.Pinned)
}

func (s *suite) releaseReservation(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "release-res", user, basket, utxostore.TierMined, 10, 10, 10, 40)

	claimed, err := store.ClaimExact(ctx, sc, "res-a", 10, 3)
	require.NoError(t, err)
	require.Len(t, claimed, 3)

	// Release frees exactly the reservation's rows.
	n, err := store.ReleaseReservation(ctx, user, "res-a")
	require.NoError(t, err)
	require.Equal(t, 3, n)
	for _, u := range claimed {
		got, err := store.Get(ctx, u.Outpoint)
		require.NoError(t, err)
		require.Empty(t, got.ReservedBy)
		require.True(t, got.ReservedAt.IsZero())
	}

	// Idempotent: releasing again is a zero-count no-op.
	n, err = store.ReleaseReservation(ctx, user, "res-a")
	require.NoError(t, err)
	require.Zero(t, n)

	// Unknown reservation: zero-count no-op.
	n, err = store.ReleaseReservation(ctx, user, "never-existed")
	require.NoError(t, err)
	require.Zero(t, n)

	// Released coins are claimable again.
	reclaimed, err := store.ClaimExact(ctx, sc, "res-b", 10, 3)
	require.NoError(t, err)
	require.Len(t, reclaimed, 3)

	// Wrong user releases nothing.
	n, err = store.ReleaseReservation(ctx, user+1, "res-b")
	require.NoError(t, err)
	require.Zero(t, n)
	got, err := store.Get(ctx, reclaimed[0].Outpoint)
	require.NoError(t, err)
	require.Equal(t, "res-b", got.ReservedBy)

	// Spent rows are never touched: their reservation is provenance.
	forSpend, err := store.ClaimExact(ctx, sc, "res-c", 40, 1)
	require.NoError(t, err)
	require.Len(t, forSpend, 1)
	spendTx := NewTxID("release-res-spender")
	sp := &utxostore.SpendOp{Outpoint: forSpend[0].Outpoint, Reservation: "res-c", SpendingTxID: spendTx}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))

	n, err = store.ReleaseReservation(ctx, user, "res-c")
	require.NoError(t, err)
	require.Zero(t, n)
	got, err = store.Get(ctx, forSpend[0].Outpoint)
	require.NoError(t, err)
	require.NotNil(t, got.SpentBy)
	require.Equal(t, spendTx, *got.SpentBy)
	require.Equal(t, "res-c", got.ReservedBy, "spend keeps the reservation as provenance")
}

func (s *suite) releaseOutpoints(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "release-ops", user, basket, utxostore.TierMined, 10, 10)

	claimed, err := store.ClaimExact(ctx, sc, "res-a", 10, 2)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	a, b := claimed[0].Outpoint, claimed[1].Outpoint

	// Guard mismatch: per-item skip, NOT an error; the row stays reserved.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-other", []utxostore.Outpoint{a}))
	got, err := store.Get(ctx, a)
	require.NoError(t, err)
	require.Equal(t, "res-a", got.ReservedBy)

	// Matching guard releases exactly the listed rows.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-a", []utxostore.Outpoint{a}))
	got, err = store.Get(ctx, a)
	require.NoError(t, err)
	require.Empty(t, got.ReservedBy)
	got, err = store.Get(ctx, b)
	require.NoError(t, err)
	require.Equal(t, "res-a", got.ReservedBy, "unlisted row must stay reserved")

	// Missing outpoints are skips.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-a", []utxostore.Outpoint{NewOutpoint("gone", 0)}))

	// Spent rows are skips: release never touches them.
	spendTx := NewTxID("release-ops-spender")
	sp := &utxostore.SpendOp{Outpoint: b, Reservation: "res-a", SpendingTxID: spendTx}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-a", []utxostore.Outpoint{b}))
	got, err = store.Get(ctx, b)
	require.NoError(t, err)
	require.NotNil(t, got.SpentBy)
}

// pinLifecycle walks the core pre-broadcast-pin contract: a pin is a committed
// "a broadcastable transaction spends these coins" mark that makes the
// reservation unsweepable. Pinned rows stay reserved (so claims still refuse
// them) and ReleaseReservation — the janitor path — must free nothing until the
// pin is lifted by Unpin.
func (s *suite) pinLifecycle(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "pin-life", user, basket, utxostore.TierMined, 10, 10, 10)

	claimed, err := store.ClaimExact(ctx, sc, "res-pin", 10, 2)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	ops := []utxostore.Outpoint{claimed[0].Outpoint, claimed[1].Outpoint}
	for _, op := range ops {
		require.False(t, getPinChecked(ctx, t, store, op).Pinned, "a claim does not pin")
	}

	// Pin reports how many rows it newly pinned...
	n, err := store.Pin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Equal(t, 2, n)
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		require.True(t, u.Pinned)
		require.Equal(t, "res-pin", u.ReservedBy, "a pin never exists without its reservation")
	}

	// ...and is idempotent: already-pinned rows are not counted again.
	n, err = store.Pin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Zero(t, n, "Pin must be idempotent")

	// A token that grows between pins reports only the DELTA — the funder may
	// claim several times under one reservation, and a re-pin covering the new
	// rows must not double-count the ones already pinned.
	grown, err := store.ClaimExact(ctx, sc, "res-pin", 10, 1)
	require.NoError(t, err)
	require.Len(t, grown, 1)
	ops = append(ops, grown[0].Outpoint)
	n, err = store.Pin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Equal(t, 1, n, "Pin must count only the rows it newly pinned")
	for _, op := range ops {
		require.True(t, getPinChecked(ctx, t, store, op).Pinned)
	}

	// THE POINT: the janitor path frees nothing while the pin is held.
	n, err = store.ReleaseReservation(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Zero(t, n, "ReleaseReservation must never free a pinned row")
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		require.Equal(t, "res-pin", u.ReservedBy, "a refused release leaves the reservation intact")
		require.True(t, u.Pinned)
	}

	// Pinned rows are reserved, so they were already invisible to claims.
	poached, err := store.ClaimExact(ctx, sc, "res-poach", 10, 3)
	require.NoError(t, err)
	require.Empty(t, poached, "pinned (hence reserved) coins are not claimable")

	// Unpin leaves the rows RESERVED: release is a separate step, so a crash
	// between the two leaves coins merely reserved, never double-spendable.
	n, err = store.Unpin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Equal(t, 3, n)
	for _, op := range ops {
		u := getPinChecked(ctx, t, store, op)
		require.False(t, u.Pinned)
		require.Equal(t, "res-pin", u.ReservedBy, "Unpin must not release the reservation")
	}
	n, err = store.Unpin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Zero(t, n, "Unpin must be idempotent")

	// Now the release goes through.
	n, err = store.ReleaseReservation(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Equal(t, 3, n)
	for _, op := range ops {
		require.Empty(t, getPinChecked(ctx, t, store, op).ReservedBy)
	}
}

// pinScopingAndNoOps covers the cases where a pin must do NOTHING: a token the
// store never saw, a token whose rows are all spent, and a token owned by
// another user. Each returns a zero count and leaves state untouched, so a
// caller replaying a pin after a crash cannot reach across those boundaries.
func (s *suite) pinScopingAndNoOps(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "pin-scope", user, basket, utxostore.TierMined, 20, 30)

	// Unknown reservation: zero-count no-op on both sides.
	n, err := store.Pin(ctx, user, "never-existed")
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = store.Unpin(ctx, user, "never-existed")
	require.NoError(t, err)
	require.Zero(t, n)

	// A fully spent reservation pins nothing: its rows have left the live pool
	// and their token survives only as provenance.
	spentClaim, err := store.ClaimExact(ctx, sc, "res-spent", 20, 1)
	require.NoError(t, err)
	require.Len(t, spentClaim, 1)
	sp := &utxostore.SpendOp{Outpoint: spentClaim[0].Outpoint, Reservation: "res-spent", SpendingTxID: NewTxID("pin-scope-spender")}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	n, err = store.Pin(ctx, user, "res-spent")
	require.NoError(t, err)
	require.Zero(t, n, "a fully spent reservation has nothing to pin")
	require.False(t, getPinChecked(ctx, t, store, spentClaim[0].Outpoint).Pinned)

	// Wrong user pins nothing: the token is scoped by owner, as on release.
	owned, err := store.ClaimExact(ctx, sc, "res-owner", 30, 1)
	require.NoError(t, err)
	require.Len(t, owned, 1)
	n, err = store.Pin(ctx, user+1, "res-owner")
	require.NoError(t, err)
	require.Zero(t, n)
	require.False(t, getPinChecked(ctx, t, store, owned[0].Outpoint).Pinned)

	// ...and neither does the wrong user's UNPIN, once the owner has pinned.
	n, err = store.Pin(ctx, user, "res-owner")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = store.Unpin(ctx, user+1, "res-owner")
	require.NoError(t, err)
	require.Zero(t, n)
	require.True(t, getPinChecked(ctx, t, store, owned[0].Outpoint).Pinned,
		"another user's Unpin must not lift this owner's pin")
}

// pinClearedBySpendAndUnspend pins that the transaction's own lifecycle clears
// the pin: the spend it was protecting consumes it, and the unspend that
// reverses that spend returns a fully clean, claimable row.
func (s *suite) pinClearedBySpendAndUnspend(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "pin-spend", user, basket, utxostore.TierMined, 100)

	claimed, err := store.ClaimExact(ctx, sc, "res-p", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	op := claimed[0].Outpoint

	n, err := store.Pin(ctx, user, "res-p")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// A pin never blocks the spend it exists to protect.
	spender := NewTxID("pin-spend-spender")
	sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-p", SpendingTxID: spender}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	require.NoError(t, sp.Err)

	u := getPinChecked(ctx, t, store, op)
	require.NotNil(t, u.SpentBy)
	require.Equal(t, spender, *u.SpentBy)
	require.False(t, u.Pinned, "Spend clears the pin: the coin is consumed, not in flight")

	// Unspend reverses the spend and returns the row to the claimable pool
	// with no residue: no spend, no reservation, no pin.
	released, err := store.Unspend(ctx, spender, []utxostore.Outpoint{op})
	require.NoError(t, err)
	require.Equal(t, 1, released)
	u = getPinChecked(ctx, t, store, op)
	require.Nil(t, u.SpentBy)
	require.Empty(t, u.ReservedBy)
	require.False(t, u.Pinned, "Unspend clears the pin along with the reservation")

	reclaimed, err := store.ClaimExact(ctx, sc, "res-q", 100, 1)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, op, reclaimed[0].Outpoint)
	require.False(t, reclaimed[0].Pinned)
}

// releaseOutpointsOverridesPin pins the one release path a pin does NOT block.
// ReleaseOutpoints is token-guarded — its callers are the funder (whose rows
// are never pinned) and the reconciler's verified-dead release, which must be
// able to reclaim the inputs of a transaction proven never to broadcast — so a
// matching token clears the pin along with the reservation. A wrong token is
// still a skip.
func (s *suite) releaseOutpointsOverridesPin(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "pin-relops", user, basket, utxostore.TierMined, 10, 20)

	a, err := store.ClaimExact(ctx, sc, "res-a", 10, 1)
	require.NoError(t, err)
	require.Len(t, a, 1)
	b, err := store.ClaimExact(ctx, sc, "res-b", 20, 1)
	require.NoError(t, err)
	require.Len(t, b, 1)
	opA, opB := a[0].Outpoint, b[0].Outpoint

	for _, token := range []string{"res-a", "res-b"} {
		n, perr := store.Pin(ctx, user, token)
		require.NoError(t, perr)
		require.Equal(t, 1, n)
	}

	// Wrong token: a skip, exactly as for an unpinned row.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-wrong", []utxostore.Outpoint{opA}))
	u := getPinChecked(ctx, t, store, opA)
	require.True(t, u.Pinned, "a guard mismatch must not lift the pin")
	require.Equal(t, "res-a", u.ReservedBy)

	// Matching token: frees the row AND clears the pin.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-a", []utxostore.Outpoint{opA}))
	u = getPinChecked(ctx, t, store, opA)
	require.Empty(t, u.ReservedBy)
	require.False(t, u.Pinned, "a token-guarded release overrides the pin")

	reclaimed, err := store.ClaimExact(ctx, sc, "res-c", 10, 1)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, opA, reclaimed[0].Outpoint)

	// The other reservation is untouched.
	u = getPinChecked(ctx, t, store, opB)
	require.True(t, u.Pinned)
	require.Equal(t, "res-b", u.ReservedBy)
}

func (s *suite) spendLifecycle(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "spend", user, basket, utxostore.TierMined, 100, 200, 300)

	claimed, err := store.ClaimExact(ctx, sc, "res-1", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	op := claimed[0].Outpoint
	winner := NewTxID("spend-winner")

	// reserved(reservation) -> spent(txid).
	sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	require.NoError(t, sp.Err)
	got, err := store.Get(ctx, op)
	require.NoError(t, err)
	require.NotNil(t, got.SpentBy)
	require.Equal(t, winner, *got.SpentBy)

	// Same spender replay: idempotent success.
	replay := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{replay}, false))
	require.NoError(t, replay.Err)

	// Different spender: SpentError carrying the winner.
	loser := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: NewTxID("spend-loser")}
	err = store.Spend(ctx, []*utxostore.SpendOp{loser}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, loser.Err, &utxostore.SpentError{})
	var spent *utxostore.SpentError
	require.ErrorAs(t, loser.Err, &spent)
	require.Equal(t, winner, spent.Winner)
	require.Equal(t, op, spent.Op)

	// Spend of an unreserved row: reservation-guard failure, HeldBy == "".
	unreserved := NewOutpoint("spend", 1) // the 200-sat coin, never claimed
	noRes := &utxostore.SpendOp{Outpoint: unreserved, Reservation: "res-x", SpendingTxID: NewTxID("t")}
	err = store.Spend(ctx, []*utxostore.SpendOp{noRes}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var resErr *utxostore.ReservedError
	require.ErrorAs(t, noRes.Err, &resErr)
	require.Empty(t, resErr.HeldBy)

	// Spend with the wrong reservation: guard failure naming the holder.
	held, err := store.ClaimExact(ctx, sc, "res-holder", 300, 1)
	require.NoError(t, err)
	require.Len(t, held, 1)
	wrongRes := &utxostore.SpendOp{Outpoint: held[0].Outpoint, Reservation: "res-thief", SpendingTxID: NewTxID("t2")}
	err = store.Spend(ctx, []*utxostore.SpendOp{wrongRes}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorAs(t, wrongRes.Err, &resErr)
	require.Equal(t, "res-holder", resErr.HeldBy)
	got, err = store.Get(ctx, held[0].Outpoint)
	require.NoError(t, err)
	require.Nil(t, got.SpentBy, "failed guard must not spend the row")
}

func (s *suite) spendPerItemErrors(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "spend-shape", user, basket, utxostore.TierMined, 100)
	claimed, err := store.ClaimExact(ctx, sc, "res-1", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// One good item + one bad item: ErrBatch, exactly the bad item's Err set,
	// the good item still committed.
	good := &utxostore.SpendOp{Outpoint: claimed[0].Outpoint, Reservation: "res-1", SpendingTxID: NewTxID("shape-tx")}
	bad := &utxostore.SpendOp{Outpoint: NewOutpoint("spend-shape-missing", 0), Reservation: "res-1", SpendingTxID: NewTxID("shape-tx")}
	err = store.Spend(ctx, []*utxostore.SpendOp{good, bad}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.NoError(t, good.Err)
	require.ErrorIs(t, bad.Err, &utxostore.NotFoundError{})

	got, err := store.Get(ctx, good.Outpoint)
	require.NoError(t, err)
	require.NotNil(t, got.SpentBy, "good item in a partially failing batch must be committed")
}

// spendPrecedence pins the guard ordering on Spend: an already-recorded
// spend wins over a freeze. A same-spender replay (outbox crash recovery) on
// a since-frozen row is an idempotent success, and a competing spender gets
// SpentError — never FrozenError.
func (s *suite) spendPrecedence(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "spend-precedence", user, basket, utxostore.TierMined, 100)
	claimed, err := store.ClaimExact(ctx, sc, "res-1", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	op := claimed[0].Outpoint
	winner := NewTxID("precedence-winner")

	sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))

	// Freeze AFTER the spend was recorded.
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{op}))

	// Same-spender replay succeeds despite the freeze.
	replay := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{replay}, false),
		"same-spender replay on a since-frozen row must succeed")
	require.NoError(t, replay.Err)

	// A different spender sees the recorded spend, not the freeze.
	loser := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: NewTxID("precedence-loser")}
	err = store.Spend(ctx, []*utxostore.SpendOp{loser}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, loser.Err, &utxostore.SpentError{})
	require.NotErrorIs(t, loser.Err, &utxostore.FrozenError{},
		"the recorded spend, not the freeze, must be reported")
}

// spendFactMode pins the forced (force=true) mode: at broadcast-accept the
// spend has ALREADY happened on the network, so the store records the fact
// instead of guarding a transition. Reservation mismatches (including an
// unreserved row) and freezes are recorded anyway; only two errors survive —
// NotFoundError (a genuinely external input, the one error a caller may skip)
// and SpentError from a DIFFERENT spender (two facts cannot both hold).
func (s *suite) spendFactMode(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// Rows 0..6, one per scenario below. Denominations are distinct so each
	// ClaimExact names exactly one row; see the bucket note at the reclaim
	// probe before adding any coin to this list.
	ops := MintTx(t, store, "spend-fact", user, basket, utxostore.TierMined,
		100, 200, 300, 400, 500, 600, 700)
	fact := NewTxID("spend-fact-broadcast")

	// claimOne reserves the single coin worth denom under res.
	claimOne := func(res string, denom uint64) utxostore.Outpoint {
		t.Helper()
		claimed, cerr := store.ClaimExact(ctx, sc, res, denom, 1)
		require.NoError(t, cerr)
		require.Len(t, claimed, 1, "the %d-sat coin must be claimable under %s", denom, res)
		return claimed[0].Outpoint
	}

	// requireRecorded re-reads op and asserts the fact was written: spent by
	// the expected tx, and never left pinned (the coin is consumed, not in
	// flight).
	requireRecorded := func(op utxostore.Outpoint, spender chainhash.Hash) *utxostore.UTXO {
		t.Helper()
		u := getPinChecked(ctx, t, store, op)
		require.NotNil(t, u.SpentBy, "fact mode must record the spend on %s", op)
		require.Equal(t, spender, *u.SpentBy)
		require.False(t, u.Pinned, "a recorded spend clears the pin on %s", op)
		return u
	}

	// Row 0 (100): reserved under a DIFFERENT token than the op carries. The
	// reservation the broadcast was built under is long gone; the spend is not.
	require.Equal(t, ops[0], claimOne("res-other", 100))

	// Row 2 (300): reserved by the op's own token, then FROZEN.
	frozenOutpoint := claimOne("res-mine", 300)
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{frozenOutpoint}))

	// One batch, four items: wrong holder, unreserved (row 1, never claimed),
	// frozen, and an outpoint that is not in the inventory at all.
	missing := NewOutpoint("spend-fact-missing", 0)
	wrongHolder := &utxostore.SpendOp{Outpoint: ops[0], Reservation: "res-mine", SpendingTxID: fact}
	unreserved := &utxostore.SpendOp{Outpoint: ops[1], Reservation: "res-mine", SpendingTxID: fact}
	frozenOp := &utxostore.SpendOp{Outpoint: frozenOutpoint, Reservation: "res-mine", SpendingTxID: fact}
	external := &utxostore.SpendOp{Outpoint: missing, Reservation: "res-mine", SpendingTxID: fact}

	err := store.Spend(ctx, []*utxostore.SpendOp{wrongHolder, unreserved, frozenOp, external}, true)
	require.ErrorIs(t, err, utxostore.ErrBatch, "the missing row still fails the batch")
	require.NoError(t, wrongHolder.Err, "a foreign reservation must not refuse the fact")
	require.NoError(t, unreserved.Err, "an unreserved row must not refuse the fact")
	require.NoError(t, frozenOp.Err, "a freeze must not refuse the fact")
	require.ErrorIs(t, external.Err, &utxostore.NotFoundError{},
		"a missing row is a genuinely external input: still NotFoundError")

	u := requireRecorded(ops[0], fact)
	require.Equal(t, "res-other", u.ReservedBy,
		"the row keeps the reservation it carried; the op's token is never written to it")
	u = requireRecorded(ops[1], fact)
	require.Empty(t, u.ReservedBy,
		"fact mode records the spend, it does not adopt the op's token")
	u = requireRecorded(frozenOutpoint, fact)
	require.True(t, u.Frozen, "recording a spend does not lift the freeze")

	// The recorded fact takes the coin out of circulation on every backend: a
	// spent row is never claimable again, however it was reserved before.
	//
	// Load-bearing, not decoration: row 1 was the only row still IN the claim
	// index when the fact landed (the others were reserved or frozen, so their
	// index entry was already gone), which makes this the one assertion that
	// pins aerostore's fact-mode claimKey removal. It is also the only probe
	// whose answer must be empty, so keep it unambiguous: 200 sats sits alone
	// in aerospike's bucket 7 (128..255), and adding another 128..255-sat coin
	// to the mint list above could let a bounded best-of-sample probe miss a
	// leftover key and pass a regression.
	reclaim, err := store.ClaimExact(ctx, sc, "res-after", 200, 1)
	require.NoError(t, err)
	require.Empty(t, reclaim, "a fact-spent row must leave the claimable pool")

	// Row 3 (400): the spent_by arbiter is unchanged in fact mode.
	arb := claimOne("res-arb", 400)
	first := &utxostore.SpendOp{Outpoint: arb, Reservation: "res-arb", SpendingTxID: fact}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{first}, true))
	require.NoError(t, first.Err)

	replay := &utxostore.SpendOp{Outpoint: arb, Reservation: "res-gone", SpendingTxID: fact}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{replay}, true),
		"same-spender replay is idempotent in fact mode too")
	require.NoError(t, replay.Err)

	rival := &utxostore.SpendOp{Outpoint: arb, Reservation: "res-arb", SpendingTxID: NewTxID("spend-fact-rival")}
	err = store.Spend(ctx, []*utxostore.SpendOp{rival}, true)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, rival.Err, &utxostore.SpentError{},
		"two spend facts cannot both hold: a different spender must still fail")
	var spent *utxostore.SpentError
	require.ErrorAs(t, rival.Err, &spent)
	require.Equal(t, fact, spent.Winner)
	requireRecorded(arb, fact)

	// Row 4 (500): a PINNED row, force-spent under a foreign token — the pin
	// was protecting this very broadcast, and goes with the coin it protected.
	pinnedOutpoint := claimOne("res-pin", 500)
	n, err := store.Pin(ctx, user, "res-pin")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.True(t, getPinChecked(ctx, t, store, pinnedOutpoint).Pinned)

	pinnedOp := &utxostore.SpendOp{Outpoint: pinnedOutpoint, Reservation: "res-stale", SpendingTxID: fact}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{pinnedOp}, true))
	require.NoError(t, pinnedOp.Err)
	requireRecorded(pinnedOutpoint, fact)

	// Guarded mode is UNCHANGED by the new parameter: the two guards fact mode
	// drops still refuse without force. A distinct spender keeps these refusals
	// from being confused with the fact-mode spends above.
	guarded := NewTxID("spend-fact-guarded")

	// Row 5 (600): frozen, spent under its OWN token — only the freeze refuses.
	guardFrozen := claimOne("res-gf", 600)
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{guardFrozen}))
	gf := &utxostore.SpendOp{Outpoint: guardFrozen, Reservation: "res-gf", SpendingTxID: guarded}
	err = store.Spend(ctx, []*utxostore.SpendOp{gf}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, gf.Err, &utxostore.FrozenError{}, "without force a freeze still refuses")

	// Row 6 (700): unfrozen, spent under a FOREIGN token — only the guard refuses.
	guardHeld := claimOne("res-gh", 700)
	gh := &utxostore.SpendOp{Outpoint: guardHeld, Reservation: "res-thief", SpendingTxID: guarded}
	err = store.Spend(ctx, []*utxostore.SpendOp{gh}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var held *utxostore.ReservedError
	require.ErrorAs(t, gh.Err, &held, "without force a foreign reservation still refuses")
	require.Equal(t, "res-gh", held.HeldBy)

	for _, op := range []utxostore.Outpoint{guardFrozen, guardHeld} {
		require.Nil(t, getPinChecked(ctx, t, store, op).SpentBy,
			"a refused guarded spend must not record anything on %s", op)
	}
}

func (s *suite) unspend(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "unspend", user, basket, utxostore.TierMined, 100, 200)

	spender := NewTxID("unspend-spender")
	var ops []utxostore.Outpoint
	for _, denom := range []uint64{100, 200} {
		claimed, err := store.ClaimExact(ctx, sc, "res-1", denom, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		sp := &utxostore.SpendOp{Outpoint: claimed[0].Outpoint, Reservation: "res-1", SpendingTxID: spender}
		require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
		ops = append(ops, claimed[0].Outpoint)
	}

	// Exact-guard release: the row returns to the claimable pool entirely.
	n, err := store.Unspend(ctx, spender, ops[:1])
	require.NoError(t, err)
	require.Equal(t, 1, n)
	got, err := store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.Nil(t, got.SpentBy)
	require.Empty(t, got.ReservedBy, "unspend clears the reservation too")

	reclaimed, err := store.ClaimExact(ctx, sc, "res-2", 100, 1)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, ops[0], reclaimed[0].Outpoint, "unspent coin is claimable again")

	// Replay: guard no longer matches, skip with count 0.
	n, err = store.Unspend(ctx, spender, ops[:1])
	require.NoError(t, err)
	require.Zero(t, n)

	// Guard mismatch (different spender): skip, row stays spent.
	n, err = store.Unspend(ctx, NewTxID("unspend-other"), ops[1:])
	require.NoError(t, err)
	require.Zero(t, n)
	got, err = store.Get(ctx, ops[1])
	require.NoError(t, err)
	require.NotNil(t, got.SpentBy)

	// Missing outpoints: skip.
	n, err = store.Unspend(ctx, spender, []utxostore.Outpoint{NewOutpoint("gone", 0)})
	require.NoError(t, err)
	require.Zero(t, n)

	// Contract #5: Unspend still applies to FROZEN rows — the spend is
	// reverted (spend and reservation cleared) while the freeze itself
	// stays in place.
	require.NoError(t, store.Freeze(ctx, ops[1:]))
	n, err = store.Unspend(ctx, spender, ops[1:])
	require.NoError(t, err)
	require.Equal(t, 1, n, "unspend must apply to frozen rows")
	got, err = store.Get(ctx, ops[1])
	require.NoError(t, err)
	require.Nil(t, got.SpentBy)
	require.Empty(t, got.ReservedBy)
	require.True(t, got.Frozen, "unspend must not lift the freeze")

	// Still frozen, so still invisible to claims until unfrozen.
	frozenClaim, err := store.ClaimExact(ctx, sc, "res-3", 200, 1)
	require.NoError(t, err)
	require.Empty(t, frozenClaim)
	require.NoError(t, store.Unfreeze(ctx, ops[1:]))
	thawedClaim, err := store.ClaimExact(ctx, sc, "res-3", 200, 1)
	require.NoError(t, err)
	require.Len(t, thawedClaim, 1)
}

func (s *suite) promote(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	ops := MintTx(t, store, "promote", user, basket, utxostore.TierSending, 100)
	op := ops[0]

	// Not visible at a tier it does not have.
	got, err := store.ClaimSmallestSufficient(ctx, scope(utxostore.TierUnproven), "res-pre", 1)
	require.NoError(t, err)
	require.Nil(t, got)

	// Promote up; already-there counts as unchanged; missing is skipped.
	n, err := store.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierUnproven)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	n, err = store.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierUnproven)
	require.NoError(t, err)
	require.Zero(t, n, "already at target tier counts as unchanged")

	n, err = store.Promote(ctx, []utxostore.Outpoint{op, NewOutpoint("gone", 0)}, utxostore.TierMined)
	require.NoError(t, err)
	require.Equal(t, 1, n, "missing outpoints are skipped")

	u, err := store.Get(ctx, op)
	require.NoError(t, err)
	require.Equal(t, utxostore.TierMined, u.Tier)

	// Downgrade is allowed (reorg).
	n, err = store.Promote(ctx, []utxostore.Outpoint{op}, utxostore.TierUnproven)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Claims follow the tier.
	got, err = store.ClaimSmallestSufficient(ctx, scope(utxostore.TierMined), "res-a", 1)
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = store.ClaimSmallestSufficient(ctx, scope(utxostore.TierUnproven), "res-a", 1)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, op, got.Outpoint)
}

func (s *suite) freezeUnfreeze(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := MintTx(t, store, "freeze", user, basket, utxostore.TierMined, 10, 20)
	require.NoError(t, store.Freeze(ctx, ops))

	// Frozen rows are invisible to all three claim shapes.
	one, err := store.ClaimSmallestSufficient(ctx, sc, "res-1", 1)
	require.NoError(t, err)
	require.Nil(t, one)
	batch, err := store.ClaimLargestInsufficient(ctx, sc, "res-1", 1_000, 10)
	require.NoError(t, err)
	require.Empty(t, batch)
	batch, err = store.ClaimExact(ctx, sc, "res-1", 10, 1)
	require.NoError(t, err)
	require.Empty(t, batch)

	// Freeze is idempotent and visible via Get.
	require.NoError(t, store.Freeze(ctx, ops[:1]))
	u, err := store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.True(t, u.Frozen)

	// Unfreeze restores claimability.
	require.NoError(t, store.Unfreeze(ctx, ops[:1]))
	batch, err = store.ClaimExact(ctx, sc, "res-2", 10, 1)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, ops[0], batch[0].Outpoint)

	// A freeze placed AFTER a reservation refuses Spend with FrozenError.
	require.NoError(t, store.Freeze(ctx, ops[:1]))
	sp := &utxostore.SpendOp{Outpoint: ops[0], Reservation: "res-2", SpendingTxID: NewTxID("freeze-spender")}
	err = store.Spend(ctx, []*utxostore.SpendOp{sp}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, sp.Err, &utxostore.FrozenError{})
	var frozen *utxostore.FrozenError
	require.ErrorAs(t, sp.Err, &frozen)
	require.Equal(t, ops[0], frozen.Op)

	// Release still applies to frozen rows (contract #5)...
	n, err := store.ReleaseReservation(ctx, user, "res-2")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// ...as does Promote.
	n, err = store.Promote(ctx, ops[:1], utxostore.TierUnproven)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// After unfreezing, Spend works again.
	require.NoError(t, store.Unfreeze(ctx, ops[:1]))
	claimed, err := store.ClaimExact(ctx, scope(utxostore.TierUnproven), "res-3", 10, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	sp = &utxostore.SpendOp{Outpoint: ops[0], Reservation: "res-3", SpendingTxID: NewTxID("freeze-spender")}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))

	// Missing outpoints: per-item NotFoundError under ErrBatch; present rows
	// in the same batch are still frozen.
	missing := NewOutpoint("gone", 0)
	err = store.Freeze(ctx, []utxostore.Outpoint{ops[1], missing})
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var nf *utxostore.NotFoundError
	require.ErrorAs(t, err, &nf)
	require.Equal(t, missing, nf.Op)
	u, err = store.Get(ctx, ops[1])
	require.NoError(t, err)
	require.True(t, u.Frozen)

	err = store.Unfreeze(ctx, []utxostore.Outpoint{ops[1], missing})
	require.ErrorIs(t, err, utxostore.ErrBatch)
	u, err = store.Get(ctx, ops[1])
	require.NoError(t, err)
	require.False(t, u.Frozen)
}

func (s *suite) remove(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := MintTx(t, store, "remove", user, basket, utxostore.TierMined, 10, 20, 30, 40)
	free, reserved, spentOp, frozenOp := ops[0], ops[1], ops[2], ops[3]

	// Free row: removed.
	require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{free}, false))
	_, err := store.Get(ctx, free)
	require.ErrorIs(t, err, &utxostore.NotFoundError{})

	// Missing: no-op, no error.
	require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{free}, false))

	// Reserved row: refused without force, removed with force.
	claimed, err := store.ClaimExact(ctx, sc, "res-hold", 20, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	err = store.Remove(ctx, []utxostore.Outpoint{reserved}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var resErr *utxostore.ReservedError
	require.ErrorAs(t, err, &resErr)
	require.Equal(t, reserved, resErr.Op)
	require.Equal(t, "res-hold", resErr.HeldBy)
	_, err = store.Get(ctx, reserved)
	require.NoError(t, err, "refused row must survive")
	require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{reserved}, true))
	_, err = store.Get(ctx, reserved)
	require.ErrorIs(t, err, &utxostore.NotFoundError{})

	// Spent row: refused without force, removed with force.
	claimed, err = store.ClaimExact(ctx, sc, "res-spend", 30, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	winner := NewTxID("remove-spender")
	sp := &utxostore.SpendOp{Outpoint: spentOp, Reservation: "res-spend", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	err = store.Remove(ctx, []utxostore.Outpoint{spentOp}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var spentErr *utxostore.SpentError
	require.ErrorAs(t, err, &spentErr)
	require.Equal(t, winner, spentErr.Winner)
	require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{spentOp}, true))

	// Frozen row: refused without force, removed with force.
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{frozenOp}))
	err = store.Remove(ctx, []utxostore.Outpoint{frozenOp}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, err, &utxostore.FrozenError{})
	require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{frozenOp}, true))

	// Partial batch: removable items are removed even when others refuse.
	pair := MintTx(t, store, "remove-pair", user, basket, utxostore.TierMined, 50, 60)
	claimed, err = store.ClaimExact(ctx, sc, "res-pair", 60, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	err = store.Remove(ctx, pair, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	_, err = store.Get(ctx, pair[0])
	require.ErrorIs(t, err, &utxostore.NotFoundError{}, "free item in the batch must be removed")
	_, err = store.Get(ctx, pair[1])
	require.NoError(t, err, "reserved item in the batch must survive")
}

func (s *suite) removeByMintTx(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	const mintSeed = "mint-invalid"
	mintTxID := NewTxID(mintSeed)
	ops := MintTx(t, store, mintSeed, user, basket, utxostore.TierMined, 10, 20, 30, 40, 50)
	free, held, spent1, frozenOp, spent2 := ops[0], ops[1], ops[2], ops[3], ops[4]

	// held: reserved by an in-flight descendant.
	claimed, err := store.ClaimExact(ctx, sc, "res-desc", 20, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// spent1 + spent2: both spent by the SAME child tx (dedupe check).
	childTx := NewTxID("mint-invalid-child")
	for _, denom := range []uint64{30, 50} {
		c, err := store.ClaimExact(ctx, sc, "res-child", denom, 1)
		require.NoError(t, err)
		require.Len(t, c, 1)
		sp := &utxostore.SpendOp{Outpoint: c[0].Outpoint, Reservation: "res-child", SpendingTxID: childTx}
		require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	}

	// frozenOp: frozen but unreserved — a phantom coin, still removed.
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{frozenOp}))

	report, err := store.RemoveByMintTx(ctx, mintTxID, ops)
	require.NoError(t, err)

	require.ElementsMatch(t, []utxostore.Outpoint{free, frozenOp}, report.Removed)
	require.Len(t, report.AlreadyReserved, 1)
	ref := report.AlreadyReserved[0]
	require.Equal(t, "res-desc", ref.Reservation)
	require.Equal(t, user, ref.UserID)
	require.False(t, ref.ReservedAt.IsZero())
	require.ElementsMatch(t, []utxostore.Outpoint{held}, ref.Outpoints)
	require.Len(t, report.AlreadySpentBy, 1, "spenders must be deduplicated")
	require.Equal(t, childTx, report.AlreadySpentBy[0])

	// Removed rows are gone; classified rows survive untouched.
	for _, op := range []utxostore.Outpoint{free, frozenOp} {
		_, err = store.Get(ctx, op)
		require.ErrorIs(t, err, &utxostore.NotFoundError{})
	}
	u, err := store.Get(ctx, held)
	require.NoError(t, err)
	require.Equal(t, "res-desc", u.ReservedBy)
	for _, op := range []utxostore.Outpoint{spent1, spent2} {
		u, err = store.Get(ctx, op)
		require.NoError(t, err)
		require.NotNil(t, u.SpentBy)
	}

	// Replay: missing rows are skipped, classification is repeated.
	report, err = store.RemoveByMintTx(ctx, mintTxID, ops)
	require.NoError(t, err)
	require.Empty(t, report.Removed)
	require.Len(t, report.AlreadyReserved, 1)
	require.Len(t, report.AlreadySpentBy, 1)
}

// removeSpentBy verifies that RemoveSpentBy deletes exactly the rows spent by
// the given tx (a now-terminal spend), leaves coins spent by other txs, and is
// idempotent.
func (s *suite) removeSpentBy(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "removespentby", user, basket, utxostore.TierMined, 100, 200, 300)

	spenderA := NewTxID("removespentby-A")
	spenderB := NewTxID("removespentby-B")
	spend := func(denom uint64, spender chainhash.Hash) utxostore.Outpoint {
		claimed, err := store.ClaimExact(ctx, sc, "res", denom, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{
			{Outpoint: claimed[0].Outpoint, Reservation: "res", SpendingTxID: spender},
		}, false))
		return claimed[0].Outpoint
	}
	// 100 + 200 spent by A; 300 spent by a different tx B.
	opA1 := spend(100, spenderA)
	opA2 := spend(200, spenderA)
	opB := spend(300, spenderB)

	// RemoveSpentBy(A) drops both A-coins and leaves the B-coin untouched.
	n, err := store.RemoveSpentBy(ctx, spenderA)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	for _, op := range []utxostore.Outpoint{opA1, opA2} {
		_, gerr := store.Get(ctx, op)
		require.ErrorIs(t, gerr, &utxostore.NotFoundError{}, "A-spent coin removed")
	}
	gotB, err := store.Get(ctx, opB)
	require.NoError(t, err)
	require.NotNil(t, gotB.SpentBy, "B-spent coin untouched")

	// Idempotent: a second call removes nothing.
	n, err = store.RemoveSpentBy(ctx, spenderA)
	require.NoError(t, err)
	require.Zero(t, n)
}

func (s *suite) balance(t *testing.T, store utxostore.Store) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// Claimable: mined 100+200, unproven 50, sending 25.
	MintTx(t, store, "bal-mined", user, basket, utxostore.TierMined, 100, 200)
	MintTx(t, store, "bal-unproven", user, basket, utxostore.TierUnproven, 50)
	MintTx(t, store, "bal-sending", user, basket, utxostore.TierSending, 25)
	// Reserved 70; frozen 30; spent 500; reserved-AND-frozen 80.
	MintTx(t, store, "bal-edge", user, basket, utxostore.TierMined, 70, 30, 500, 80)
	// Foreign rows that must not leak in.
	MintTx(t, store, "bal-user2", user+1, basket, utxostore.TierMined, 1_000)
	MintTx(t, store, "bal-basket2", user, "other-basket", utxostore.TierMined, 1_000)

	claimed, err := store.ClaimExact(ctx, sc, "res-bal", 70, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// The 30-sat coin is frozen while unreserved: it counts in NEITHER bucket.
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{NewOutpoint("bal-edge", 1)}))

	// The 80-sat coin is reserved and THEN frozen: still Reserved — a freeze
	// does not release the reservation's hold on the satoshis.
	heldFrozen, err := store.ClaimExact(ctx, sc, "res-bal-frozen", 80, 1)
	require.NoError(t, err)
	require.Len(t, heldFrozen, 1)
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{heldFrozen[0].Outpoint}))

	spent, err := store.ClaimExact(ctx, sc, "res-bal-spent", 500, 1)
	require.NoError(t, err)
	require.Len(t, spent, 1)
	sp := &utxostore.SpendOp{Outpoint: spent[0].Outpoint, Reservation: "res-bal-spent", SpendingTxID: NewTxID("bal-spender")}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))

	b, err := store.Balance(ctx, user, basket)
	require.NoError(t, err)
	require.Equal(t, uint64(300), b.Claimable[utxostore.TierMined])
	require.Equal(t, uint64(50), b.Claimable[utxostore.TierUnproven])
	require.Equal(t, uint64(25), b.Claimable[utxostore.TierSending])
	require.Equal(t, uint64(150), b.Reserved,
		"reserved-but-unspent counts frozen or not (70 plain + 80 frozen); unreserved-frozen and spent count nowhere")

	// The COUNT partition mirrors the satoshi partition: two claimable mined
	// coins (100, 200), one each unproven/sending, and two reserved (70 + 80).
	require.Equal(t, 2, b.ClaimableCount[utxostore.TierMined])
	require.Equal(t, 1, b.ClaimableCount[utxostore.TierUnproven])
	require.Equal(t, 1, b.ClaimableCount[utxostore.TierSending])
	require.Equal(t, 2, b.ReservedCount,
		"two reserved-but-unspent coins (70 plain + 80 frozen)")

	// An empty (user, basket) has a zero balance.
	b, err = store.Balance(ctx, user, "empty-basket")
	require.NoError(t, err)
	require.Zero(t, b.Reserved)
	require.Zero(t, b.ReservedCount)
	var total uint64
	for _, v := range b.Claimable {
		total += v
	}
	require.Zero(t, total)
	var totalCount int
	for _, v := range b.ClaimableCount {
		totalCount += v
	}
	require.Zero(t, totalCount)
}

// clockedStore provisions the store for timestamp-sensitive subtests: the
// manual-clock factory when provided (deterministic timestamps, no sleeps),
// otherwise the default factory with a wall-clock sleep fallback that
// compresses logical time into short sleeps — which is why exact-ordering
// backends without clock control need sub-millisecond timestamp fidelity.
// Exactly one store is built either way.
func (s *suite) clockedStore(t *testing.T) (utxostore.Store, AdvanceFunc) {
	if s.cfg.newStoreWithClock != nil {
		return s.cfg.newStoreWithClock(t)
	}
	return s.newStore(t), func(time.Duration) { time.Sleep(5 * time.Millisecond) }
}

func (s *suite) findStaleReservations(t *testing.T) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	store, advance := s.clockedStore(t)

	MintTx(t, store, "stale", user, basket, utxostore.TierMined, 10, 10, 20, 30)

	// Three reservations, created oldest to newest with distinct timestamps.
	resA, err := store.ClaimExact(ctx, sc, "res-a", 10, 2)
	require.NoError(t, err)
	require.Len(t, resA, 2)
	advance(time.Minute)
	resB, err := store.ClaimExact(ctx, sc, "res-b", 20, 1)
	require.NoError(t, err)
	require.Len(t, resB, 1)
	advance(time.Minute)
	resC, err := store.ClaimExact(ctx, sc, "res-c", 30, 1)
	require.NoError(t, err)
	require.Len(t, resC, 1)

	// Cutoffs derive from the claimed rows' own timestamps, so they are
	// valid under any clock (manual or wall).
	future := resC[0].ReservedAt.Add(time.Hour)

	// All three are stale against a future cutoff.
	refs, err := store.FindStaleReservations(ctx, future, 10)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	byToken := map[string]utxostore.ReservationRef{}
	for _, ref := range refs {
		require.Equal(t, user, ref.UserID)
		require.False(t, ref.ReservedAt.IsZero())
		byToken[ref.Reservation] = ref
	}
	require.ElementsMatch(t, []utxostore.Outpoint{resA[0].Outpoint, resA[1].Outpoint}, byToken["res-a"].Outpoints)
	require.ElementsMatch(t, []utxostore.Outpoint{resB[0].Outpoint}, byToken["res-b"].Outpoints)
	require.ElementsMatch(t, []utxostore.Outpoint{resC[0].Outpoint}, byToken["res-c"].Outpoints)

	// limit bounds the result; exact backends return oldest first.
	refs, err = store.FindStaleReservations(ctx, future, 2)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	if s.cfg.exactSelection {
		require.Equal(t, "res-a", refs[0].Reservation)
		require.Equal(t, "res-b", refs[1].Reservation)
	}

	// Nothing predates a cutoff before the oldest reservation.
	refs, err = store.FindStaleReservations(ctx, resA[0].ReservedAt.Add(-time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, refs)

	// Released reservations disappear.
	_, err = store.ReleaseReservation(ctx, user, "res-a")
	require.NoError(t, err)
	refs, err = store.FindStaleReservations(ctx, future, 10)
	require.NoError(t, err)
	require.Len(t, refs, 2)

	// Spent rows drop out: a fully spent reservation is no longer stale work.
	sp := &utxostore.SpendOp{Outpoint: resB[0].Outpoint, Reservation: "res-b", SpendingTxID: NewTxID("stale-spender")}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	refs, err = store.FindStaleReservations(ctx, future, 10)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, "res-c", refs[0].Reservation)
}

// findStaleReservationsSkipsPinned is the sweep half of the pin contract: the
// stale-reservation reaper must not even SEE a pinned reservation, so it can
// never hand the inputs of an in-flight send to a release.
func (s *suite) findStaleReservationsSkipsPinned(t *testing.T) {
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	store, advance := s.clockedStore(t)

	MintTx(t, store, "pin-stale", user, basket, utxostore.TierMined, 10, 20)

	pinned, err := store.ClaimExact(ctx, sc, "res-pinned", 10, 1)
	require.NoError(t, err)
	require.Len(t, pinned, 1)
	advance(time.Minute)
	plain, err := store.ClaimExact(ctx, sc, "res-plain", 20, 1)
	require.NoError(t, err)
	require.Len(t, plain, 1)

	n, err := store.Pin(ctx, user, "res-pinned")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Both reservations are stale by age; only the unpinned one is sweepable.
	future := plain[0].ReservedAt.Add(time.Hour)
	refs, err := store.FindStaleReservations(ctx, future, 10)
	require.NoError(t, err)
	require.Len(t, refs, 1, "a pinned reservation is not stale work")
	require.Equal(t, "res-plain", refs[0].Reservation)
	require.ElementsMatch(t, []utxostore.Outpoint{plain[0].Outpoint}, refs[0].Outpoints,
		"no pinned outpoint may appear in a stale ref")
	held := getPinChecked(ctx, t, store, pinned[0].Outpoint)
	require.True(t, held.Pinned, "the skipped reservation is still pinned and reserved")
	require.Equal(t, "res-pinned", held.ReservedBy)

	// Unpinning hands the reservation back to the sweep.
	n, err = store.Unpin(ctx, user, "res-pinned")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	refs, err = store.FindStaleReservations(ctx, future, 10)
	require.NoError(t, err)
	require.Len(t, refs, 2, "an unpinned stale reservation is visible again")
}

// staleReservationDating pins that a reservation is dated by its OLDEST row:
// the funder may claim several times under one token, and staleness must be
// measured from the first claim, not the latest. Requires WithManualClock so
// the gap between the two claims is deterministic.
func (s *suite) staleReservationDating(t *testing.T) {
	if s.cfg.newStoreWithClock == nil {
		t.Skip("requires WithManualClock: backend must expose deterministic clock control")
	}
	store, advance := s.cfg.newStoreWithClock(t)
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	MintTx(t, store, "stale-dating", user, basket, utxostore.TierMined, 10, 20)

	// Two claims into ONE reservation, ten minutes apart.
	first, err := store.ClaimExact(ctx, sc, "res-slow", 10, 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	advance(10 * time.Minute)
	second, err := store.ClaimExact(ctx, sc, "res-slow", 20, 1)
	require.NoError(t, err)
	require.Len(t, second, 1)

	t0 := first[0].ReservedAt
	require.True(t, second[0].ReservedAt.After(t0), "manual clock must yield increasing timestamps")

	// A cutoff BETWEEN the two claim times must still report the
	// reservation as stale, dated by the oldest row.
	refs, err := store.FindStaleReservations(ctx, t0.Add(5*time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, refs, 1, "reservation age must be its OLDEST row, not its newest")
	require.Equal(t, "res-slow", refs[0].Reservation)
	require.True(t, refs[0].ReservedAt.Equal(t0), "ref must be dated by the oldest row")
	require.ElementsMatch(t,
		[]utxostore.Outpoint{first[0].Outpoint, second[0].Outpoint},
		refs[0].Outpoints, "the ref must list every unspent row of the reservation")

	// A cutoff before the first claim finds nothing.
	refs, err = store.FindStaleReservations(ctx, t0.Add(-time.Second), 10)
	require.NoError(t, err)
	require.Empty(t, refs)
}

func (s *suite) closeStore(t *testing.T, store utxostore.Store) {
	ctx := context.Background()

	require.NoError(t, store.Close(ctx))
	require.NoError(t, store.Close(ctx), "Close must be idempotent")

	_, err := store.Get(ctx, NewOutpoint("after-close", 0))
	require.Error(t, err, "operations after Close must fail")
}

// batchItemErrors flattens the TYPED per-item errors joined under [ErrBatch],
// so a subtest can assert how many items a call refused rather than only that
// it refused something. Both wrappers in play — fmt.Errorf's two-verb %w and
// errors.Join — expose Unwrap() []error, so one recursive walk covers them; the
// ErrBatch sentinel itself is skipped because it is not an item outcome.
func batchItemErrors(err error) []error {
	var out []error
	var walk func(error)
	walk = func(e error) {
		if e == nil {
			return
		}
		if multi, ok := e.(interface{ Unwrap() []error }); ok {
			for _, sub := range multi.Unwrap() {
				walk(sub)
			}
			return
		}
		switch {
		case errors.Is(e, &utxostore.NotFoundError{}),
			errors.Is(e, &utxostore.ReservedError{}),
			errors.Is(e, &utxostore.SpentError{}),
			errors.Is(e, &utxostore.FrozenError{}):
			out = append(out, e)
		}
	}
	walk(err)
	return out
}

// opsOf projects the outpoints of claimed coins, in order — the identity to
// assert on when the satoshi values cannot tell the coins apart.
func opsOf(coins []*utxostore.UTXO) []utxostore.Outpoint {
	ops := make([]utxostore.Outpoint, len(coins))
	for i, u := range coins {
		ops[i] = u.Outpoint
	}
	return ops
}

// satsOf projects the satoshi values of claimed coins, in order.
func satsOf(coins []*utxostore.UTXO) []uint64 {
	sats := make([]uint64, len(coins))
	for i, u := range coins {
		sats[i] = u.Satoshis
	}
	return sats
}
