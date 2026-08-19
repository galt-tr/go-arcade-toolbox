package sqlstore_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// The conformance suite pins Spend's taxonomy one verdict at a time (and, in
// SpendPerItemErrors, over a two-item batch). These add the case the SET-BASED
// PostgreSQL arm actually has to get right and the per-op loop could not get
// wrong: a batch whose items land in DIFFERENT taxonomy buckets from ONE
// statement plus ONE classifying read.
//
// They run against both engines from the same function — the SQLite runner
// below and the PostgreSQL one in set_based_pg_test.go — because "the two arms
// rule identically" is the whole claim, and a test only one arm runs proves
// nothing about the other.

// testSpendMixedBatch drives a single Spend call whose six items span every
// verdict the classifier can reach, and asserts both the per-item errors and
// the ROW COUNT that actually changed: three fresh spends recorded, one
// same-spender replay satisfied without a second write, one loser refused, one
// absent outpoint reported missing.
func testSpendMixedBatch(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// Five real coins on one synthetic transaction, all claimable, plus a
	// sixth outpoint that was never minted.
	ops := utxostoretest.MintTx(t, store, "mixed-batch", 1, "default",
		utxostore.TierMined, 100, 200, 300, 400, 500)
	missing := utxostoretest.NewOutpoint("mixed-batch-absent", 7)

	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-mixed", 1_000, 5)
	require.NoError(t, err)
	require.Len(t, claimed, 5, "all five coins must be held by one reservation")

	spender := utxostoretest.NewTxID("mixed-batch-spender")
	rival := utxostoretest.NewTxID("mixed-batch-rival")

	// Pre-state: op[3] is already spent by the SAME spender (the idempotent
	// replay), op[4] by a DIFFERENT one (the SpentError).
	pre := []*utxostore.SpendOp{
		{Outpoint: ops[3], Reservation: "res-mixed", SpendingTxID: spender},
		{Outpoint: ops[4], Reservation: "res-mixed", SpendingTxID: rival},
	}
	require.NoError(t, store.Spend(ctx, pre, false))
	for _, sp := range pre {
		require.NoError(t, sp.Err)
	}

	batch := []*utxostore.SpendOp{
		{Outpoint: ops[0], Reservation: "res-mixed", SpendingTxID: spender},
		{Outpoint: ops[1], Reservation: "res-mixed", SpendingTxID: spender},
		{Outpoint: ops[2], Reservation: "res-mixed", SpendingTxID: spender},
		{Outpoint: ops[3], Reservation: "res-mixed", SpendingTxID: spender}, // idempotent
		{Outpoint: ops[4], Reservation: "res-mixed", SpendingTxID: spender}, // lost to rival
		{Outpoint: missing, Reservation: "res-mixed", SpendingTxID: spender},
	}
	err = store.Spend(ctx, batch, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)

	for i := 0; i < 4; i++ {
		require.NoError(t, batch[i].Err, "item %d must succeed (fresh spend or same-spender replay)", i)
	}
	require.ErrorIs(t, batch[4].Err, &utxostore.SpentError{})
	var spent *utxostore.SpentError
	require.ErrorAs(t, batch[4].Err, &spent)
	require.Equal(t, rival, spent.Winner, "the SpentError must name the transaction that won the row")
	require.ErrorIs(t, batch[5].Err, &utxostore.NotFoundError{})

	// Row-count parity: exactly the three fresh coins moved to this spender,
	// the replayed one was already there, and the rival keeps its coin.
	for i := 0; i < 4; i++ {
		u, gerr := store.Get(ctx, ops[i])
		require.NoError(t, gerr)
		require.NotNil(t, u.SpentBy, "op %d must be spent", i)
		require.Equal(t, spender, *u.SpentBy, "op %d must be spent by this batch's spender", i)
	}
	u, err := store.Get(ctx, ops[4])
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy)
	require.Equal(t, rival, *u.SpentBy, "a lost row must keep the winner's spend, not be overwritten")
}

// testSpendMixedSpenders drives one Spend call carrying TWO spenders, which is
// what forces the set-based arm to split into groups: the write's SET clause
// and its reservation guard are per group, and the groups must keep the
// caller's order so the same outpoint named under both spenders is won by the
// EARLIER one — the ruling the sequential loop made.
func testSpendMixedSpenders(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := utxostoretest.MintTx(t, store, "two-spenders", 1, "default",
		utxostore.TierMined, 100, 200, 300)
	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-two", 1_000, 3)
	require.NoError(t, err)
	require.Len(t, claimed, 3)

	first := utxostoretest.NewTxID("two-spenders-first")
	second := utxostoretest.NewTxID("two-spenders-second")

	batch := []*utxostore.SpendOp{
		{Outpoint: ops[0], Reservation: "res-two", SpendingTxID: first},
		{Outpoint: ops[1], Reservation: "res-two", SpendingTxID: second},
		{Outpoint: ops[2], Reservation: "res-two", SpendingTxID: first},
		// Contested: named by BOTH spenders in one call. The first group wins.
		{Outpoint: ops[0], Reservation: "res-two", SpendingTxID: second},
	}
	err = store.Spend(ctx, batch, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.NoError(t, batch[0].Err)
	require.NoError(t, batch[1].Err)
	require.NoError(t, batch[2].Err)

	var spent *utxostore.SpentError
	require.ErrorAs(t, batch[3].Err, &spent, "the later spender must lose the contested row")
	require.Equal(t, first, spent.Winner)

	// Each row carries the spender its own group named.
	for _, want := range []struct {
		op      utxostore.Outpoint
		spender chainhash.Hash
	}{
		{ops[0], first},
		{ops[1], second},
		{ops[2], first},
	} {
		u, gerr := store.Get(ctx, want.op)
		require.NoError(t, gerr)
		require.NotNil(t, u.SpentBy)
		require.Equal(t, want.spender, *u.SpentBy, "%s must be spent by its own group's transaction", want.op)
	}
}

// testMixedTxIDBatch is the case that distinguishes a batch whose key arrays
// are paired POSITIONALLY from one that crosses them.
//
// Every other batch test here seeds its coins with MintTx, which mints one
// synthetic transaction — so all its outpoints share a txid and differ only by
// vout, and txid i paired with vout j names the same row as txid j with vout i.
// Those tests cannot see a mis-pairing at all. This one spans THREE txids and
// asks for a different vout from each, so the requested set {A:2, B:0, C:1} and
// the cross product {A,B,C} × {2,0,1} are nine rows apart: under a cross
// product this call would spend every coin the three transactions hold.
//
// It is a guard on [pgKeyRel]'s two-argument unnest, whose positional pairing
// is a documented property of the function rather than of how the planner
// expands set-returning functions in a target list — a rule that has differed
// between PostgreSQL major versions. It runs on both engines because the SQLite
// loop is the reference answer.
func testMixedTxIDBatch(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	// Three transactions, three outputs each, all distinct denominations so the
	// claim takes all nine and nothing is ambiguous.
	a := utxostoretest.MintTx(t, store, "mixed-txid-a", 1, "default", utxostore.TierMined, 100, 200, 300)
	b := utxostoretest.MintTx(t, store, "mixed-txid-b", 1, "default", utxostore.TierMined, 400, 500, 600)
	c := utxostoretest.MintTx(t, store, "mixed-txid-c", 1, "default", utxostore.TierMined, 700, 800, 900)
	require.NotEqual(t, a[0].TxID, b[0].TxID, "the three transactions must have distinct txids")
	require.NotEqual(t, b[0].TxID, c[0].TxID)

	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-mixed-txid", 1_000, 9)
	require.NoError(t, err)
	require.Len(t, claimed, 9)

	// Asymmetric vouts: one from each transaction, no two the same index.
	spender := utxostoretest.NewTxID("mixed-txid-spender")
	targets := []utxostore.Outpoint{a[2], b[0], c[1]}
	spends := make([]*utxostore.SpendOp, len(targets))
	for i, op := range targets {
		spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-mixed-txid", SpendingTxID: spender}
	}
	require.NoError(t, store.Spend(ctx, spends, false))
	for i, sp := range spends {
		require.NoError(t, sp.Err, "item %d", i)
	}

	spent := map[utxostore.Outpoint]bool{a[2]: true, b[0]: true, c[1]: true}
	all := append(append(append([]utxostore.Outpoint{}, a...), b...), c...)
	for _, op := range all {
		u, gerr := store.Get(ctx, op)
		require.NoError(t, gerr)
		if spent[op] {
			require.NotNil(t, u.SpentBy, "%s was named and must be spent", op)
			require.Equal(t, spender, *u.SpentBy)
			continue
		}
		require.Nil(t, u.SpentBy,
			"%s was NOT named: a crossed txid/vout pairing would have spent it too", op)
	}

	// The same asymmetry through the release statement, which takes the other
	// (no-RETURNING) shape: free one coin per transaction, again at differing
	// vouts, and prove the six untouched rows keep their state.
	released := []utxostore.Outpoint{a[0], b[2], c[0]}
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-mixed-txid", released))

	free := map[utxostore.Outpoint]bool{a[0]: true, b[2]: true, c[0]: true}
	for _, op := range all {
		u, gerr := store.Get(ctx, op)
		require.NoError(t, gerr)
		switch {
		case free[op]:
			require.Empty(t, u.ReservedBy, "%s was named for release", op)
		case spent[op]:
			require.NotNil(t, u.SpentBy, "%s is spent and release must skip it", op)
		default:
			require.Equal(t, "res-mixed-txid", u.ReservedBy,
				"%s was NOT named: a crossed pairing would have released it too", op)
		}
	}
}

// testSpendFactModeBatch pins the fact-mode arm over a batch: the guards
// guarded mode enforces are dropped (a foreign reservation and a freeze are
// recorded anyway), while the two that survive in BOTH modes still refuse —
// a missing row and a spend already recorded by a different transaction.
func testSpendFactModeBatch(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := utxostoretest.MintTx(t, store, "fact-batch", 1, "default",
		utxostore.TierMined, 100, 200, 300, 400)
	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-foreign", 1_000, 4)
	require.NoError(t, err)
	require.Len(t, claimed, 4)

	// ops[1] frozen, ops[3] already spent by a rival.
	require.NoError(t, store.Freeze(ctx, []utxostore.Outpoint{ops[1]}))
	rival := utxostoretest.NewTxID("fact-batch-rival")
	pre := []*utxostore.SpendOp{{Outpoint: ops[3], Reservation: "res-foreign", SpendingTxID: rival}}
	require.NoError(t, store.Spend(ctx, pre, false))

	accepted := utxostoretest.NewTxID("fact-batch-accepted")
	missing := utxostoretest.NewOutpoint("fact-batch-absent", 3)
	batch := []*utxostore.SpendOp{
		{Outpoint: ops[0], Reservation: "res-mine", SpendingTxID: accepted}, // foreign token
		{Outpoint: ops[1], Reservation: "res-mine", SpendingTxID: accepted}, // frozen
		{Outpoint: ops[2], Reservation: "res-mine", SpendingTxID: accepted},
		{Outpoint: ops[3], Reservation: "res-mine", SpendingTxID: accepted}, // rival's
		{Outpoint: missing, Reservation: "res-mine", SpendingTxID: accepted},
	}
	err = store.Spend(ctx, batch, true)
	require.ErrorIs(t, err, utxostore.ErrBatch)

	for i := 0; i < 3; i++ {
		require.NoError(t, batch[i].Err, "fact mode records item %d whatever the row state", i)
		u, gerr := store.Get(ctx, ops[i])
		require.NoError(t, gerr)
		require.NotNil(t, u.SpentBy)
		require.Equal(t, accepted, *u.SpentBy)
	}
	require.ErrorIs(t, batch[3].Err, &utxostore.SpentError{}, "two spend facts cannot both hold")
	require.ErrorIs(t, batch[4].Err, &utxostore.NotFoundError{})
}

// testSpendEmptyReservationBatch pins that the programmer-error item is settled
// before any statement and does not poison the batch around it.
func testSpendEmptyReservationBatch(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := utxostoretest.MintTx(t, store, "empty-res", 1, "default", utxostore.TierMined, 100, 200)
	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-ok", 1_000, 2)
	require.NoError(t, err)
	require.Len(t, claimed, 2)

	spender := utxostoretest.NewTxID("empty-res-spender")
	batch := []*utxostore.SpendOp{
		{Outpoint: ops[0], Reservation: "", SpendingTxID: spender},
		{Outpoint: ops[1], Reservation: "res-ok", SpendingTxID: spender},
	}
	require.ErrorIs(t, store.Spend(ctx, batch, false), utxostore.ErrBatch)
	require.Error(t, batch[0].Err)
	require.NoError(t, batch[1].Err, "a neighbour's validation failure must not refuse a good item")

	u, err := store.Get(ctx, ops[0])
	require.NoError(t, err)
	require.Nil(t, u.SpentBy, "the invalid item must not have been written")
}

// testReleaseOutpointsBatch pins the set-based release over a batch mixing rows
// the token holds, a row another token holds, a spent row and an absent one:
// only the first group is freed, and the rest are untouched skips.
func testReleaseOutpointsBatch(t *testing.T, store utxostore.Store) {
	t.Helper()
	ctx := context.Background()
	sc := scope(utxostore.TierMined)

	ops := utxostoretest.MintTx(t, store, "release-batch", 1, "default",
		utxostore.TierMined, 100, 200, 300, 400)
	mine, err := store.ClaimExact(ctx, sc, "res-mine", 100, 1)
	require.NoError(t, err)
	require.Len(t, mine, 1)
	more, err := store.ClaimExact(ctx, sc, "res-mine", 200, 1)
	require.NoError(t, err)
	require.Len(t, more, 1)
	theirs, err := store.ClaimExact(ctx, sc, "res-theirs", 300, 1)
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	spentHeld, err := store.ClaimExact(ctx, sc, "res-mine", 400, 1)
	require.NoError(t, err)
	require.Len(t, spentHeld, 1)

	spender := utxostoretest.NewTxID("release-batch-spender")
	sp := &utxostore.SpendOp{Outpoint: spentHeld[0].Outpoint, Reservation: "res-mine", SpendingTxID: spender}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))

	require.NoError(t, store.ReleaseOutpoints(ctx, "res-mine", []utxostore.Outpoint{
		ops[0], ops[1], ops[2], ops[3],
		utxostoretest.NewOutpoint("release-batch-absent", 0),
	}))

	for _, op := range []utxostore.Outpoint{ops[0], ops[1]} {
		u, gerr := store.Get(ctx, op)
		require.NoError(t, gerr)
		require.Empty(t, u.ReservedBy, "a row this token held must be freed")
	}
	held, err := store.Get(ctx, ops[2])
	require.NoError(t, err)
	require.Equal(t, "res-theirs", held.ReservedBy, "another token's row is a skip")
	still, err := store.Get(ctx, ops[3])
	require.NoError(t, err)
	require.NotNil(t, still.SpentBy, "a spent row is never released")
	require.Equal(t, "res-mine", still.ReservedBy, "a spent row keeps its reservation")
}

// scope builds the single-user scope every test in this file claims from.
func scope(tier utxostore.Tier) utxostore.Scope {
	return utxostore.Scope{UserID: 1, Basket: "default", Tier: tier}
}

// runBatchSemanticsSuite runs every batch-shape test against one engine.
func runBatchSemanticsSuite(t *testing.T, newStore func(t *testing.T) utxostore.Store) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, s utxostore.Store)
	}{
		{"SpendMixedBatch", testSpendMixedBatch},
		{"SpendMixedSpenders", testSpendMixedSpenders},
		{"MixedTxIDBatch", testMixedTxIDBatch},
		{"SpendFactModeBatch", testSpendFactModeBatch},
		{"SpendEmptyReservationBatch", testSpendEmptyReservationBatch},
		{"ReleaseOutpointsBatch", testReleaseOutpointsBatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newStore(t))
		})
	}
}

// TestBatchSemantics_SQLite runs the batch-shape tests against the per-op
// SQLite arm, which is the reference the PostgreSQL set-based arm must match.
func TestBatchSemantics_SQLite(t *testing.T) {
	runBatchSemanticsSuite(t, func(t *testing.T) utxostore.Store { return newSQLiteStore(t) })
}
