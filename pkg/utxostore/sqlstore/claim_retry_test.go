package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// The claim path is the only statement in this package that runs outside a
// transaction, and it used to be the only one with no lock-error retry: every
// mutation goes through withTx -> sqlkit.WithRetry, while a claim went straight
// to the pool. A 40001/40P01 there surfaced raw to the funder, which retries
// only utxostore.ErrContention, and a raw driver lock error is not that — so
// the hottest path in the module failed on the one class of error every colder
// path survived. Mode A hid it, because the metastore's outer retry wraps the
// whole unit of work.
//
// These tests inject the lock error at the driver so the retry is asserted where
// it actually has to work, rather than by reading the call site.

// faultState is the shared arming mechanism for one fault driver instance.
type faultState struct {
	// remaining is how many further intercepted queries fail before the fault
	// stops firing. Zero (the default) means disarmed, so migrations and
	// seeding run untouched.
	remaining atomic.Int32
	// fired counts the queries the fault actually failed, which is what proves
	// a retry happened rather than a first-attempt success.
	fired atomic.Int32
	err   error
}

// arm makes the next n intercepted queries fail with err.
func (f *faultState) arm(n int32, err error) {
	f.err = err
	f.fired.Store(0)
	f.remaining.Store(n)
}

// check reports the injected error when armed, consuming one charge.
func (f *faultState) check() error {
	if f.remaining.Load() <= 0 {
		return nil
	}
	f.remaining.Add(-1)
	f.fired.Add(1)
	return f.err
}

// faultDriver wraps a real driver and fails queries while armed. It intercepts
// BOTH the conn-level and stmt-level query paths, because database/sql picks
// between them based on which optional interfaces the inner driver implements
// and the fault must fire either way.
type faultDriver struct {
	inner driver.Driver
	state *faultState
}

func (d *faultDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &faultConn{Conn: c, state: d.state}, nil
}

type faultConn struct {
	driver.Conn
	state *faultState
}

func (c *faultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.state.check(); err != nil {
		return nil, err
	}
	q, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return q.QueryContext(ctx, query, args)
}

func (c *faultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return e.ExecContext(ctx, query, args)
}

func (c *faultConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var (
		st  driver.Stmt
		err error
	)
	if p, ok := c.Conn.(driver.ConnPrepareContext); ok {
		st, err = p.PrepareContext(ctx, query)
	} else {
		st, err = c.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &faultStmt{Stmt: st, state: c.state}, nil
}

func (c *faultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	return c.Conn.Begin() //nolint:staticcheck // fallback for drivers without ConnBeginTx
}

type faultStmt struct {
	driver.Stmt
	state *faultState
}

func (s *faultStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.state.check(); err != nil {
		return nil, err
	}
	q, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return q.QueryContext(ctx, args)
}

func (s *faultStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	e, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return e.ExecContext(ctx, args)
}

// driverSeq keeps registered fault-driver names unique; sql.Register panics on
// a duplicate and the registry is process-global.
var driverSeq atomic.Int32

// minedScope is the scope every subtest claims from.
var minedScope = utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

// maxLockRetries mirrors the bound in internal/sqlkit. It is duplicated rather
// than exported because the exact number is sqlkit's business; this test only
// needs "more failures than the budget allows".
const maxLockRetries = 3

// newFaultStore builds a SQLite-backed store whose driver can be armed to fail
// queries with a chosen error. The store is returned disarmed, so New's
// migrations and any seeding run normally.
func newFaultStore(t *testing.T) (utxostore.Store, *faultState) {
	t.Helper()

	base := sql.Drivers() // ensure the modernc "sqlite" driver is registered
	require.Contains(t, base, "sqlite", "modernc sqlite driver must be registered")

	inner, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	innerDriver := inner.Driver()
	require.NoError(t, inner.Close())

	state := &faultState{}
	name := fmt.Sprintf("faultsqlite-%d", driverSeq.Add(1))
	sql.Register(name, &faultDriver{inner: innerDriver, state: state})

	path := filepath.Join(t.TempDir(), "claim-retry.db")
	db, err := sql.Open(name, path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	require.NoError(t, err)
	db.SetMaxOpenConns(1) // the store's own SQLite discipline: single writer
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlstore.New(context.Background(), db, sqlstore.EngineSQLite)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store, state
}

func TestClaimRetriesLockErrors(t *testing.T) {
	// Both classification paths in sqlkit.IsLockError: the PostgreSQL SQLSTATE
	// codes, and the driver-agnostic message match. A claim must survive either.
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"pg serialization failure 40001", &pgconn.PgError{Code: "40001", Message: "could not serialize access"}},
		{"pg deadlock detected 40P01", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
		{"pg lock not available 55P03", &pgconn.PgError{Code: "55P03", Message: "lock not available"}},
		{"sqlite database is locked", errors.New("database is locked")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, state := newFaultStore(t)

			utxostoretest.MintTx(t, store, "retry-fund", 1, "default", utxostore.TierMined, 50_000)

			// Fail every attempt but the last one the retry budget allows.
			state.arm(2, tc.err)

			got, err := store.ClaimSmallestSufficient(ctx, minedScope, "res-retry", 10_000)
			require.NoError(t, err,
				"a claim gave up on a retryable lock error. Every other statement in "+
					"this package survives %v by retrying; the hot path must not be the "+
					"one that fails the caller", tc.err)
			require.NotNil(t, got, "the retry succeeded but returned no coin")
			assert.Equal(t, "res-retry", got.ReservedBy)

			assert.Equal(t, int32(2), state.fired.Load(),
				"the fault never fired, so this passed without a retry ever happening "+
					"and proves nothing about the retry path")
		})
	}
}

func TestClaimSurfacesLockErrorAfterRetriesExhausted(t *testing.T) {
	ctx := context.Background()
	store, state := newFaultStore(t)

	utxostoretest.MintTx(t, store, "retry-exhaust", 1, "default", utxostore.TierMined, 50_000)

	// One more failure than the retry budget: the caller must see the error
	// rather than a silent nil-coin, which would read as "pool empty" and send
	// the funder down the not-enough-funds path with coins still available.
	state.arm(maxLockRetries+1, &pgconn.PgError{Code: "40001", Message: "could not serialize access"})

	got, err := store.ClaimSmallestSufficient(ctx, minedScope, "res-exhaust", 10_000)
	require.Error(t, err,
		"an exhausted retry budget returned no error. A claim that cannot run must "+
			"say so: reporting (nil, nil) is indistinguishable from an empty pool and "+
			"would strand spendable coins behind a false ErrNotEnoughFunds")
	assert.Nil(t, got)

	var pgErr *pgconn.PgError
	assert.True(t, errors.As(err, &pgErr),
		"the underlying lock error was swallowed rather than wrapped; callers and "+
			"metrics cannot tell contention from a real failure: %v", err)
}

func TestClaimRetryDoesNotDoubleAllocate(t *testing.T) {
	// The retry is only safe because a claim is a single atomic statement: a
	// failed attempt committed nothing. Prove the coin is reserved exactly once
	// after a retried claim — a re-run that had partially committed would leave
	// a second reservation or a lost coin.
	ctx := context.Background()
	store, state := newFaultStore(t)

	utxostoretest.MintTx(t, store, "retry-once", 1, "default", utxostore.TierMined, 50_000, 50_000, 50_000)

	state.arm(2, &pgconn.PgError{Code: "40001", Message: "could not serialize access"})

	first, err := store.ClaimSmallestSufficient(ctx, minedScope, "res-a", 10_000)
	require.NoError(t, err)
	require.NotNil(t, first)

	bal, err := store.Balance(ctx, 1, "default")
	require.NoError(t, err)
	assert.Equal(t, uint64(50_000), bal.Reserved,
		"exactly one coin must be reserved after a retried claim; a retry that "+
			"double-allocated would reserve more than the caller asked for")
	assert.Equal(t, uint64(100_000), bal.Claimable[utxostore.TierMined],
		"the two coins the claim did not take must still be claimable")
}
