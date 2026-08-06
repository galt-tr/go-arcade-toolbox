package sqlstore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sqltx"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/utxostoretest"
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
	txCtx := sqltx.With(ctx, tx)

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
