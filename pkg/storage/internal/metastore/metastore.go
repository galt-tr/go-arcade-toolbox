package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	// Registers the "pgx" database/sql driver used by the PostgreSQL engine.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// Engine selects the SQL dialect a [Store] speaks. One implementation drives
// both; the differences are confined to the migration set, the placeholder
// syntax, the timestamp encoding, and the boolean spelling. It is an alias for
// [sqlkit.Engine], the single dialect enum shared with the utxostore.
type Engine = sqlkit.Engine

const (
	// EnginePostgres is PostgreSQL, driven through the pgx stdlib driver.
	EnginePostgres = sqlkit.EnginePostgres
	// EngineSQLite is SQLite, driven through the CGo-free modernc driver.
	EngineSQLite = sqlkit.EngineSQLite
)

// errClosed is returned by every method after Close.
var errClosed = errors.New("metastore: store is closed")

// Store is the wallet metadata store over PostgreSQL or SQLite. It holds
// everything the UTXO inventory ([utxostore]) does not: users, baskets,
// transactions/actions, labels/tags, outputs (descriptive + spend history),
// known transactions, certificates, sync state, key-values and the
// utxo_ops_outbox.
//
// Access the aggregate repositories via the accessor methods ([Store.Users],
// [Store.Transactions], …). Every repository method runs against execer(ctx):
// the ambient transaction carried by ctx (Mode A / [Store.Do]) when present,
// otherwise the pool. Construct a Store with [New] (wrap a shared *sql.DB),
// [OpenPostgres], [OpenSQLite], or [Open].
type Store struct {
	db        *sql.DB
	engine    Engine
	ownsDB    bool
	now       func() time.Time
	applyPool sqlkit.PoolConfig

	mu     sync.Mutex
	closed bool
}

// defaultPool backfills any field [WithConnPool] left zero for an owned
// PostgreSQL pool. The metadata store is not the claim hot path, so its default
// sizing is deliberately smaller than sqlstore's; the sizing mechanism is
// shared via [sqlkit.PoolConfig].
var defaultPool = sqlkit.PoolConfig{
	MaxOpen:     10,
	MaxIdle:     5,
	ConnMaxIdle: 5 * time.Minute,
	ConnMaxLife: 30 * time.Minute,
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the store's clock, which stamps created_at/updated_at on
// writes and anchors the age-window scans (abandoned transactions, suspect-
// failed known txs). Intended for deterministic tests; production stores use
// [time.Now].
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// WithConnPool configures the connection pool for a store that owns a
// PostgreSQL pool. Ignored for SQLite (single writer connection) and for [New]
// (the caller owns the shared pool).
func WithConnPool(maxOpen, maxIdle int, connMaxIdle, connMaxLife time.Duration) Option {
	return func(s *Store) {
		s.applyPool = sqlkit.PoolConfig{MaxOpen: maxOpen, MaxIdle: maxIdle, ConnMaxIdle: connMaxIdle, ConnMaxLife: connMaxLife}
	}
}

// New wraps an existing *sql.DB and runs the metadata migrations against it.
// The caller retains ownership of db: Close does NOT close it. This is the
// Mode A constructor — the metastore and the utxostore share one *sql.DB and
// enlist in a caller-owned transaction via internal/sqltx and [Store.Do].
// engine must name the dialect db actually speaks.
func New(ctx context.Context, db *sql.DB, engine Engine, opts ...Option) (*Store, error) {
	return newStore(ctx, db, engine, false, opts...)
}

// OpenPostgres opens a PostgreSQL Store from a DSN, configures its connection
// pool, and runs migrations. Close closes the pool.
func OpenPostgres(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("metastore: open postgres: %w", err)
	}
	s, err := newStore(ctx, db, EnginePostgres, true, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// OpenSQLite opens a SQLite Store backed by the file at path, applies the
// concurrency pragmas (WAL, busy_timeout, synchronous=NORMAL, foreign_keys,
// immediate transactions), caps the pool at a single writer connection, and
// runs migrations. path must be a real file (WAL needs one).
func OpenSQLite(ctx context.Context, path string, opts ...Option) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("metastore: open sqlite: %w", err)
	}
	// SQLite serializes writes: a single writer connection is the simplest
	// correct posture and keeps FOR-UPDATE-free statements exclusive.
	db.SetMaxOpenConns(1)
	s, err := newStore(ctx, db, EngineSQLite, true, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Open builds a Store from a [defs.Database] configuration, dispatching on its
// Engine.
func Open(ctx context.Context, cfg defs.Database, opts ...Option) (*Store, error) {
	engine, err := defs.ParseDBTypeStr(string(cfg.Engine))
	if err != nil {
		return nil, fmt.Errorf("metastore: %w", err)
	}
	switch engine {
	case defs.DBTypeSQLite:
		return OpenSQLite(ctx, cfg.SQLite.ConnectionString, opts...)
	case defs.DBTypePostgres:
		poolOpt := WithConnPool(cfg.MaxOpenConnections, cfg.MaxIdleConnections, cfg.MaxConnectionIdleTime, cfg.MaxConnectionTime)
		return OpenPostgres(ctx, postgresDSN(cfg.PostgreSQL), append([]Option{poolOpt}, opts...)...)
	default:
		return nil, fmt.Errorf("metastore: unsupported engine %q", cfg.Engine)
	}
}

func newStore(ctx context.Context, db *sql.DB, engine Engine, ownsDB bool, opts ...Option) (*Store, error) {
	switch engine {
	case EnginePostgres, EngineSQLite:
	default:
		return nil, fmt.Errorf("metastore: unknown engine %q", engine)
	}
	s := &Store{db: db, engine: engine, ownsDB: ownsDB, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	if ownsDB && engine == EnginePostgres {
		s.applyPool.ApplyTo(db, defaultPool)
	}
	if err := migrate(ctx, db, engine); err != nil {
		return nil, fmt.Errorf("metastore: migrate: %w", err)
	}
	return s, nil
}

// sqliteDSN builds the shared modernc SQLite DSN for path with the store's
// required concurrency pragmas. See [sqlkit.SQLiteDSN].
func sqliteDSN(path string) string {
	return sqlkit.SQLiteDSN(path)
}

// postgresDSN renders a postgres:// URL DSN from a [defs.PostgreSQL] config,
// percent-escaping every value so a crafted password cannot inject extra
// connection parameters. See [sqlkit.PostgresDSN].
func postgresDSN(cfg defs.PostgreSQL) string {
	return sqlkit.PostgresDSN(cfg)
}

// SharesDatabase reports whether this store runs over db, so the provider can
// detect Mode A (utxostore and metastore in one database) and share a single
// transaction across both via internal/sqltx.
func (s *Store) SharesDatabase(db *sql.DB) bool {
	return s.db == db
}

// DB returns the underlying handle. Intended for Mode A wiring and tests.
func (s *Store) DB() *sql.DB { return s.db }

// Engine returns the dialect this store speaks.
func (s *Store) Engine() Engine { return s.engine }

// Migrate brings the metadata schema up to the latest version. It is
// idempotent (goose applies only pending versions) and is already run by the
// constructors; expose it for callers that migrate explicitly.
func (s *Store) Migrate(ctx context.Context) error {
	return migrate(ctx, s.db, s.engine)
}

// Health reports the store's health with an HTTP-convention status code.
func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if s.isClosed() {
		return http.StatusServiceUnavailable, "closed", errClosed
	}
	if checkLiveness {
		if err := s.db.PingContext(ctx); err != nil {
			return http.StatusServiceUnavailable, "database unreachable", err
		}
	}
	return http.StatusOK, "OK", nil
}

// Close is idempotent; it closes the pool only when this store opened it.
func (s *Store) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.ownsDB {
		return s.db.Close()
	}
	return nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// --- transaction seam ------------------------------------------------------

// queryer is the subset of *sql.DB / *sql.Tx the repositories use, so the same
// code runs against the pool or a transaction (ambient from ctx, or a per-call
// one from inTx). It aliases [sqlkit.SQLExecer].
type queryer = sqlkit.SQLExecer

// execer returns the queryer for a statement: the ambient transaction carried
// by ctx (Mode A / Do) if present AND opened over this store's own *sql.DB,
// else the pool.
func (s *Store) execer(ctx context.Context) queryer {
	return sqlkit.Execer(ctx, s.db)
}

// inTx runs fn inside a transaction. If ctx already carries one opened over
// this store's own *sql.DB (Mode A / a surrounding Do) it is reused and
// neither committed nor rolled back here — the owner handles that, including
// retry. Otherwise — including when ctx carries a transaction opened over a
// DIFFERENT *sql.DB — a fresh transaction is opened, committed on success,
// rolled back on error, and the whole call is wrapped in the bounded
// lock-error retry.
func (s *Store) inTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := sqltx.From(ctx, s.db); ok {
		return fn(ctx)
	}
	return sqlkit.WithRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(sqltx.With(ctx, tx, s.db)); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

// --- dialect helpers -------------------------------------------------------

// rebind converts '?' placeholders to the $N form PostgreSQL requires; SQLite
// keeps '?'. See [sqlkit.Rebind] for the naive-scan caveat.
func (s *Store) rebind(q string) string {
	return sqlkit.Rebind(s.engine, q)
}

// forUpdateSkipLocked is the row-lock clause for the outbox drain: PostgreSQL
// hands each concurrent worker a disjoint batch; SQLite's single writer
// connection already serializes so it needs no clause.
func (s *Store) forUpdateSkipLocked() string {
	if s.engine == EnginePostgres {
		return " FOR UPDATE SKIP LOCKED"
	}
	return ""
}

// encTime encodes t for a bound parameter: a UTC time.Time for PostgreSQL
// TIMESTAMPTZ, or Unix microseconds for SQLite's INTEGER columns.
func (s *Store) encTime(t time.Time) any {
	return sqlkit.EncTime(s.engine, t)
}

// encTimePtr encodes a nullable time: NULL for a nil pointer, else encTime.
func (s *Store) encTimePtr(t *time.Time) any {
	return sqlkit.EncTimePtr(s.engine, t)
}

// boolVal encodes a boolean: a bool for PostgreSQL, 0/1 for SQLite.
func (s *Store) boolVal(b bool) any {
	return sqlkit.BoolVal(s.engine, b)
}

// boolScan holds a scanned boolean in whichever representation the engine uses.
// It aliases [sqlkit.BoolScan].
type boolScan = sqlkit.BoolScan

func (s *Store) boolDest(x *boolScan) any {
	return sqlkit.BoolDest(s.engine, x)
}

func (s *Store) boolGet(x boolScan) bool {
	return sqlkit.BoolGet(s.engine, x)
}

// tsScan holds a scanned nullable timestamp in whichever representation the
// engine uses. It aliases [sqlkit.TsScan].
type tsScan = sqlkit.TsScan

func (s *Store) tsDest(x *tsScan) any {
	return sqlkit.TsDest(s.engine, x)
}

// tsTime decodes a scanned timestamp to UTC; zero time when NULL.
func (s *Store) tsTime(x tsScan) time.Time {
	return sqlkit.TsGet(s.engine, x)
}

// tsTimePtr decodes a scanned timestamp to a *time.Time; nil when NULL.
func (s *Store) tsTimePtr(x tsScan) *time.Time {
	return sqlkit.TsGetPtr(s.engine, x)
}

// rowScanner is satisfied by *sql.Row and *sql.Rows. It aliases [sqlkit.RowScanner].
type rowScanner = sqlkit.RowScanner
