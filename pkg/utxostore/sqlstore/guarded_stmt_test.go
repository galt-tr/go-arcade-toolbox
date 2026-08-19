package sqlstore_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// The guarded mutations must be write-FIRST: the predicates they used to check
// in a pre-flight SELECT now ride in the write's own WHERE, so the happy path
// is ONE statement and there is no check for a peer to invalidate between.
//
// That property is invisible to a behavioral test — the taxonomy is identical
// either way, which is the point of the refactor and precisely why an
// assertion on outcomes cannot pin it: the old check-then-write code passes
// those tests too. So these assert on the statements the store actually sends
// to the driver, using the same wrapper technique as claim_retry_test.go.

// recorded is one statement the store issued, captured with its text.
type recorded struct {
	exec bool // ExecContext (a write) rather than QueryContext (a read)
	sql  string
}

// sqlRecorder captures statements while armed. Migrations and seeding run
// disarmed, so a recording holds exactly one operation's traffic.
type sqlRecorder struct {
	mu    sync.Mutex
	armed bool
	stmts []recorded
}

func (r *sqlRecorder) start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed, r.stmts = true, nil
}

// stop disarms and returns the statements that touched the utxos table, which
// is the only traffic these assertions are about (SQLite's utxo_seq counter and
// the goose bookkeeping are noise here).
func (r *sqlRecorder) stop() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.armed = false
	var out []recorded
	for _, s := range r.stmts {
		if strings.Contains(s.sql, "utxos") {
			out = append(out, s)
		}
	}
	return out
}

func (r *sqlRecorder) note(exec bool, q string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.armed {
		r.stmts = append(r.stmts, recorded{exec: exec, sql: q})
	}
}

// recDriver wraps a real driver and records every statement. It covers BOTH
// the conn-level and the prepare/stmt paths, because database/sql chooses
// between them from the optional interfaces the inner driver implements.
//
// The legacy non-context Stmt.Exec/Query path is deliberately not wrapped:
// modernc's driver implements the context interfaces, so database/sql never
// takes it. It also fails CLOSED — a statement this recorder missed would lower
// the count, and every assertion below is an upper bound on statements or a
// requirement that a specific one is present.
type recDriver struct {
	inner driver.Driver
	rec   *sqlRecorder
}

func (d *recDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &recConn{Conn: c, rec: d.rec}, nil
}

type recConn struct {
	driver.Conn
	rec *sqlRecorder
}

func (c *recConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.rec.note(false, query)
	return q.QueryContext(ctx, query, args)
}

func (c *recConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	e, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	c.rec.note(true, query)
	return e.ExecContext(ctx, query, args)
}

func (c *recConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
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
	// Recorded on execution, not here: preparing is not issuing, and the text
	// is only available at this point, so the stmt carries it forward.
	return &recStmt{Stmt: st, rec: c.rec, sql: query}, nil
}

func (c *recConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	return c.Conn.Begin() //nolint:staticcheck // fallback for drivers without ConnBeginTx
}

type recStmt struct {
	driver.Stmt
	rec *sqlRecorder
	sql string
}

func (s *recStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	s.rec.note(false, s.sql)
	return q.QueryContext(ctx, args)
}

func (s *recStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	e, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	s.rec.note(true, s.sql)
	return e.ExecContext(ctx, args)
}

// newRecordingStore builds a SQLite store over a recording driver. The store is
// returned with the recorder disarmed, so migrations run untouched.
func newRecordingStore(t *testing.T) (utxostore.Store, *sqlRecorder) {
	t.Helper()

	inner, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	innerDriver := inner.Driver()
	require.NoError(t, inner.Close())

	rec := &sqlRecorder{}
	name := fmt.Sprintf("recsqlite-%d", driverSeq.Add(1))
	sql.Register(name, &recDriver{inner: innerDriver, rec: rec})

	path := filepath.Join(t.TempDir(), "guarded.db")
	db, err := sql.Open(name, sqlkit.SQLiteDSN(path))
	require.NoError(t, err)
	db.SetMaxOpenConns(1) // the shared-handle posture New now validates
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlstore.New(context.Background(), db, sqlstore.EngineSQLite)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store, rec
}

// requireSoleWrite asserts the recording holds exactly one utxos statement,
// that it is a write, and returns its text. A pre-flight SELECT — the shape
// this refactor removed — shows up here as a second entry.
func requireSoleWrite(t *testing.T, stmts []recorded) string {
	t.Helper()
	require.Len(t, stmts, 1, "the happy path must be ONE statement; got %s", render(stmts))
	require.True(t, stmts[0].exec, "the sole statement must be the write, not a read: %s", stmts[0].sql)
	return stmts[0].sql
}

func render(stmts []recorded) string {
	var b strings.Builder
	for _, s := range stmts {
		b.WriteString("\n  ")
		b.WriteString(strings.Join(strings.Fields(s.sql), " "))
	}
	return b.String()
}

// TestGuardedMutationsAreOneGuardedStatement pins the UPDATE-first/DELETE-first
// inversion (audit P1-7) at the wire: on the happy path each guarded mutation
// sends exactly one statement, and that statement carries the guard conjuncts
// its Go code used to test after a SELECT.
func TestGuardedMutationsAreOneGuardedStatement(t *testing.T) {
	ctx := context.Background()
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	// claimOne seeds a coin and reserves it, with the recorder disarmed.
	claimOne := func(t *testing.T, store utxostore.Store, seed string, denom uint64, res string) utxostore.Outpoint {
		t.Helper()
		utxostoretest.MintTx(t, store, seed, 1, "default", utxostore.TierMined, denom)
		claimed, err := store.ClaimExact(ctx, sc, res, denom, 1)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		return claimed[0].Outpoint
	}

	t.Run("guarded spend", func(t *testing.T) {
		store, rec := newRecordingStore(t)
		op := claimOne(t, store, "one-stmt-spend", 100, "res-1")

		rec.start()
		sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-1", SpendingTxID: utxostoretest.NewTxID("w")}
		require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, false))
		require.NoError(t, sp.Err)

		q := requireSoleWrite(t, rec.stop())
		require.Contains(t, q, "UPDATE utxos")
		require.Contains(t, q, "spent_by IS NULL", "the unrecorded-spend guard rides in the WHERE")
		require.Contains(t, q, "reserved_by=?", "the reservation guard rides in the WHERE")
		require.Contains(t, q, "NOT frozen", "the freeze guard rides in the WHERE")
	})

	t.Run("fact-mode spend drops the two guards it must", func(t *testing.T) {
		store, rec := newRecordingStore(t)
		op := claimOne(t, store, "one-stmt-fact", 100, "res-other")

		rec.start()
		// A foreign reservation AND a spend the network already accepted: fact
		// mode must record it, so neither dropped conjunct may appear.
		sp := &utxostore.SpendOp{Outpoint: op, Reservation: "res-mine", SpendingTxID: utxostoretest.NewTxID("f")}
		require.NoError(t, store.Spend(ctx, []*utxostore.SpendOp{sp}, true))
		require.NoError(t, sp.Err)

		q := requireSoleWrite(t, rec.stop())
		require.Contains(t, q, "UPDATE utxos")
		require.Contains(t, q, "spent_by IS NULL", "two spend facts cannot both hold, in either mode")
		require.NotContains(t, q, "reserved_by", "a stale reservation cannot un-happen an accepted spend")
		require.NotContains(t, q, "frozen", "a freeze cannot un-happen an accepted spend")
	})

	t.Run("guarded remove", func(t *testing.T) {
		store, rec := newRecordingStore(t)
		ops := utxostoretest.MintTx(t, store, "one-stmt-remove", 1, "default", utxostore.TierMined, 100)

		rec.start()
		require.NoError(t, store.Remove(ctx, ops, false))

		q := requireSoleWrite(t, rec.stop())
		require.Contains(t, q, "DELETE FROM utxos")
		require.Contains(t, q, "reserved_by IS NULL")
		require.Contains(t, q, "spent_by IS NULL")
		require.Contains(t, q, "NOT frozen")
	})

	t.Run("force remove stays unguarded and unread", func(t *testing.T) {
		store, rec := newRecordingStore(t)
		op := claimOne(t, store, "one-stmt-force", 100, "res-held")

		rec.start()
		require.NoError(t, store.Remove(ctx, []utxostore.Outpoint{op}, true))

		// Force asserts authority over the row whatever its state, so there is
		// nothing to classify and nothing to guard — and, notably, no pre-read.
		q := requireSoleWrite(t, rec.stop())
		require.Contains(t, q, "DELETE FROM utxos")
		require.NotContains(t, q, "reserved_by")
		require.NotContains(t, q, "frozen")
	})

	t.Run("a refusal is the one path that pays for a read", func(t *testing.T) {
		store, rec := newRecordingStore(t)
		op := claimOne(t, store, "one-stmt-refused", 100, "res-holder")

		rec.start()
		err := store.Remove(ctx, []utxostore.Outpoint{op}, false)
		require.ErrorIs(t, err, utxostore.ErrBatch)

		// Guarded DELETE first (matches nothing), then ONE classifying read —
		// never the other way round. That the read carries no FOR UPDATE is not
		// assertable here: forUpdate() is "" on SQLite, so the check would pass
		// against any implementation. It is a PostgreSQL property, covered by
		// the integration suite's behavior instead.
		stmts := rec.stop()
		require.Len(t, stmts, 2, "refusal = guarded write + one classifying read; got %s", render(stmts))
		require.True(t, stmts[0].exec, "the write comes FIRST: %s", render(stmts))
		require.Contains(t, stmts[0].sql, "DELETE FROM utxos")
		require.False(t, stmts[1].exec, "the classification is a read")
		require.Contains(t, stmts[1].sql, "SELECT")
	})
}
