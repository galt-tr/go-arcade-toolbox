package sqlstore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
