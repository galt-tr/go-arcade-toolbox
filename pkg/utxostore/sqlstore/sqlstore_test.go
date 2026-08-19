package sqlstore_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// baseTime anchors the manual clock at a microsecond-aligned instant, so
// timestamps round-trip identically through PostgreSQL TIMESTAMPTZ and SQLite
// INTEGER-microseconds.
var baseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// manualClock is a deterministic, race-safe clock for the timestamp-sensitive
// conformance subtests.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newSQLiteStore builds a fresh, migrated SQLite store on a temp-dir file (WAL
// needs a real file) and registers its teardown.
func newSQLiteStore(t testing.TB, opts ...sqlstore.Option) *sqlstore.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "utxo.db")
	s, err := sqlstore.OpenSQLite(context.Background(), path, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func newSQLiteStoreClock(t *testing.T) (utxostore.Store, utxostoretest.AdvanceFunc) {
	clk := &manualClock{t: baseTime}
	return newSQLiteStore(t, sqlstore.WithClock(clk.now)), clk.advance
}

// TestSQLStore_SQLite runs the full conformance suite against the SQLite
// engine with exact selection and deterministic-clock timestamp subtests.
func TestSQLStore_SQLite(t *testing.T) {
	utxostoretest.RunStoreSuite(t,
		func(t *testing.T) utxostore.Store { return newSQLiteStore(t) },
		utxostoretest.WithExactSelection(),
		utxostoretest.WithManualClock(newSQLiteStoreClock),
	)
}

// TestSharesDatabase pins the Mode A capability probe.
func TestSharesDatabase(t *testing.T) {
	s := newSQLiteStore(t)
	require.True(t, s.SharesDatabase(s.DB()))

	other := newSQLiteStore(t)
	require.False(t, s.SharesDatabase(other.DB()))
}

// TestAmbientTransaction exercises the internal/sqltx seam: statements run
// against a transaction carried in the context, and rolling that transaction
// back undoes them — proving the store honors the ambient transaction rather
// than its own pool.
func TestAmbientTransaction(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteStore(t)

	op := utxostoretest.NewOutpoint("ambient-tx", 0)
	mint := utxostoretest.NewMint(op, 1, "default", utxostore.TierMined, 100)

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	txCtx := sqltx.With(ctx, tx, s.DB())

	require.NoError(t, s.Mint(txCtx, []*utxostore.Mint{mint}))
	require.NoError(t, mint.Err)

	// Visible inside the ambient transaction...
	got, err := s.Get(txCtx, op)
	require.NoError(t, err)
	require.Equal(t, uint64(100), got.Satoshis)

	// ...and gone after rollback, since the mint never committed on its own.
	require.NoError(t, tx.Rollback())
	_, err = s.Get(ctx, op)
	require.ErrorIs(t, err, &utxostore.NotFoundError{})
}

// TestReserveOutpointsAmbientTxLeavesNothingReserved is the reason
// ReserveOutpoints validates every row BEFORE it writes any of them.
//
// The compact implementation — one statement that locks and reserves the
// matching rows, then fails the transaction when it matched fewer than
// len(ops) — gets its all-or-nothing guarantee from the rollback. That works
// only while the store owns the transaction. In Mode A it does not: [sqltx]
// hands it a caller-owned transaction that [Store.withTx] must neither commit
// nor roll back, so the good rows would stay reserved and ride the caller's
// commit out to disk.
//
// This test therefore commits the ambient transaction on purpose. The
// guarantee has to survive that, which it can only do by never having written.
func TestReserveOutpointsAmbientTxLeavesNothingReserved(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteStore(t)

	ops := utxostoretest.MintTx(t, s, "ambient-reserve", 1, "default", utxostore.TierMined, 10, 20, 30)
	// The last row refuses, so the first two must come back untouched.
	require.NoError(t, s.Freeze(ctx, ops[2:]))

	tx, err := s.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	txCtx := sqltx.With(ctx, tx, s.DB())

	err = s.ReserveOutpoints(txCtx, 1, "res-ambient", ops)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	require.ErrorIs(t, err, &utxostore.FrozenError{})

	// The caller owns this transaction and goes on to commit it.
	require.NoError(t, tx.Commit())

	for _, op := range ops[:2] {
		u, gerr := s.Get(ctx, op)
		require.NoError(t, gerr)
		require.Empty(t, u.ReservedBy,
			"a refused ReserveOutpoints must leave %s unreserved even when the ambient tx commits", op)
		require.True(t, u.ReservedAt.IsZero())
	}
}

// TestForeignTransactionNotEnlisted is the Mode B counterpart of
// TestAmbientTransaction: a transaction opened over a DIFFERENT *sql.DB must
// never be picked up. Before the ownership fix, sqltx.From ignored who placed
// the transaction and a store would enlist in whatever *sql.Tx it found on the
// context — so a utxostore wired on its own database, but called with a
// context that happens to carry the metastore's ambient transaction (Mode B),
// would silently run its statements against the wrong connection. Ownership is
// now structural: [sqltx.From] reports a hit only when the store's own
// *sql.DB matches the one [sqltx.With] recorded, so a foreign transaction is
// never returned and the store falls back to its own pool.
func TestForeignTransactionNotEnlisted(t *testing.T) {
	ctx := context.Background()
	storeA := newSQLiteStore(t) // the store under test
	storeB := newSQLiteStore(t) // a different database — Mode B

	txB, err := storeB.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txB.Rollback() })

	// ctx carries a transaction owned by storeB's *sql.DB, not storeA's.
	foreignCtx := sqltx.With(ctx, txB, storeB.DB())

	op := utxostoretest.NewOutpoint("foreign-tx", 0)
	mint := utxostoretest.NewMint(op, 1, "default", utxostore.TierMined, 250)

	// storeA must not enlist in txB: From(foreignCtx, storeA.DB()) reports
	// false because the owners differ, so the mint runs — and commits — on
	// storeA's own pool, each item its own statement (see Mint's doc).
	require.NoError(t, storeA.Mint(foreignCtx, []*utxostore.Mint{mint}))
	require.NoError(t, mint.Err)

	// Visible immediately through storeA's own pool, with no dependency on txB
	// ever committing.
	got, err := storeA.Get(ctx, op)
	require.NoError(t, err)
	require.Equal(t, uint64(250), got.Satoshis)

	// And never written to storeB at all — not even inside txB, the very
	// transaction foreignCtx carried. storeB.Get(foreignCtx, op) resolves
	// against txB (foreignCtx's owner IS storeB's db), so a hit here would
	// mean the mint leaked into the foreign transaction; it must not.
	_, err = storeB.Get(foreignCtx, op)
	require.ErrorIs(t, err, &utxostore.NotFoundError{})

	// Rolling back the foreign transaction must not undo storeA's write,
	// proving the mint never ran inside txB in the first place.
	require.NoError(t, txB.Rollback())
	got, err = storeA.Get(ctx, op)
	require.NoError(t, err)
	require.Equal(t, uint64(250), got.Satoshis)
}

// openRawSQLite opens a migration-free SQLite handle on a temp file with the
// store's DSN but NO pool posture, so each caller chooses its own.
func openRawSQLite(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", sqlkit.SQLiteDSN(filepath.Join(t.TempDir(), "shared.db")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestNewRejectsUnpinnedSQLitePool pins the Mode A construction guard. Every
// guarded mutation now carries its predicates in the WHERE, but SQLite's write
// serialization still rests on the single writer connection [OpenSQLite]
// installs — and [New] cannot install it, because the pool belongs to the
// caller (typically the metastore, which is sharing the same handle). So it
// VALIDATES instead of mutating: resizing somebody else's pool behind their
// back is the worse of the two surprises.
func TestNewRejectsUnpinnedSQLitePool(t *testing.T) {
	ctx := context.Background()

	t.Run("unpinned is refused", func(t *testing.T) {
		db := openRawSQLite(t) // database/sql default: unlimited connections
		_, err := sqlstore.New(ctx, db, sqlstore.EngineSQLite)
		require.Error(t, err)
		require.Contains(t, err.Error(), "SetMaxOpenConns(1)",
			"the error must name the fix, not just the symptom")
	})

	t.Run("oversized pool is refused", func(t *testing.T) {
		db := openRawSQLite(t)
		db.SetMaxOpenConns(4)
		_, err := sqlstore.New(ctx, db, sqlstore.EngineSQLite)
		require.Error(t, err)
		require.Contains(t, err.Error(), "MaxOpenConnections=4")
	})

	t.Run("pinned is accepted", func(t *testing.T) {
		db := openRawSQLite(t)
		db.SetMaxOpenConns(1)
		s, err := sqlstore.New(ctx, db, sqlstore.EngineSQLite)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(ctx) })

		// Fully usable: migrations ran against the shared handle.
		op := utxostoretest.NewOutpoint("pinned-shared", 0)
		mint := utxostoretest.NewMint(op, 1, "default", utxostore.TierMined, 400)
		require.NoError(t, s.Mint(ctx, []*utxostore.Mint{mint}))
		require.NoError(t, mint.Err)
	})

	t.Run("OpenSQLite pins its own pool", func(t *testing.T) {
		s := newSQLiteStore(t)
		require.Equal(t, 1, s.DB().Stats().MaxOpenConnections)
	})
}

// TestSpendGuardsAreInTheWhere exercises the UPDATE-first spend path (audit
// P1-7): the guarded UPDATE re-asserts the reservation token, so a spend under
// the wrong token matches nothing and the taxonomy is produced by the follow-up
// classification read rather than by a pre-flight SELECT. Both halves of that
// n==0 branch are covered — the refusal AND the idempotent same-spender replay,
// which is the one n==0 outcome that must still be a success.
func TestSpendGuardsAreInTheWhere(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStore(t)
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	utxostoretest.MintTx(t, store, "guard-where", 1, "default", utxostore.TierMined, 100)
	claimed, err := store.ClaimExact(ctx, sc, "res-B", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	op := claimed[0].Outpoint

	// Held by B, spent under A: the reserved_by conjunct rejects the UPDATE and
	// the classifier names the real holder.
	thief := &utxostore.SpendOp{Outpoint: op, Reservation: "res-A", SpendingTxID: utxostoretest.NewTxID("thief")}
	err = store.Spend(ctx, []*utxostore.SpendOp{thief}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var resErr *utxostore.ReservedError
	require.ErrorAs(t, thief.Err, &resErr)
	require.Equal(t, "res-B", resErr.HeldBy)

	got, err := store.Get(ctx, op)
	require.NoError(t, err)
	require.Nil(t, got.SpentBy, "a rejected guard must leave the row unspent")

	// The holder spends it, then replays: the replay's UPDATE matches nothing
	// (spent_by IS NULL is false now), and the classifier turns that into the
	// idempotent success rather than an error.
	winner := utxostoretest.NewTxID("guard-where-winner")
	sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-B", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
	require.NoError(t, sp.Err)

	replay := &utxostore.SpendOp{Outpoint: op, Reservation: "res-B", SpendingTxID: winner}
	require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{replay}, false))
	require.NoError(t, replay.Err)
}

// TestRemoveGuardsAreInTheWhere is the DELETE-first twin: a reserved row is
// refused by the guarded DELETE and classified afterwards, and a missing row
// stays a silent no-op even though nothing was read first.
func TestRemoveGuardsAreInTheWhere(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteStore(t)
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	ops := utxostoretest.MintTx(t, store, "remove-where", 1, "default", utxostore.TierMined, 100, 200)
	claimed, err := store.ClaimExact(ctx, sc, "res-hold", 100, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	missing := utxostoretest.NewOutpoint("remove-where-missing", 7)
	err = store.Remove(ctx, []utxostore.Outpoint{claimed[0].Outpoint, ops[1], missing}, false)
	require.ErrorIs(t, err, utxostore.ErrBatch)
	var resErr *utxostore.ReservedError
	require.ErrorAs(t, err, &resErr)
	require.Equal(t, "res-hold", resErr.HeldBy)

	// The unreserved sibling was still removed, and the missing row contributed
	// no error at all.
	_, err = store.Get(ctx, ops[1])
	require.ErrorIs(t, err, &utxostore.NotFoundError{})
	_, err = store.Get(ctx, claimed[0].Outpoint)
	require.NoError(t, err, "the refused row survives")
}
