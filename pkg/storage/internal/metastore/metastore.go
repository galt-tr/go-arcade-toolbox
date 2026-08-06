package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	sqlitedrv "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	// Registers the "pgx" database/sql driver used by the PostgreSQL engine.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sqltx"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
)

// Engine selects the SQL dialect a [Store] speaks. One implementation drives
// both; the differences are confined to the migration set, the placeholder
// syntax, the timestamp encoding, and the boolean spelling.
type Engine string

const (
	// EnginePostgres is PostgreSQL, driven through the pgx stdlib driver.
	EnginePostgres Engine = "postgres"
	// EngineSQLite is SQLite, driven through the CGo-free modernc driver.
	EngineSQLite Engine = "sqlite"
)

// maxLockRetries bounds the retry loop for lock/deadlock/serialization errors
// on the transactional mutations the store owns (never on caller-owned
// ambient transactions — the caller owns those, including any retry).
const maxLockRetries = 3

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
	applyPool poolConfig

	mu     sync.Mutex
	closed bool
}

// poolConfig holds connection-pool knobs applied to a PostgreSQL pool the
// store owns. Zero fields fall back to sane defaults at apply time.
type poolConfig struct {
	maxOpen     int
	maxIdle     int
	connMaxIdle time.Duration
	connMaxLife time.Duration
}

func (p poolConfig) applyTo(db *sql.DB) {
	maxOpen, maxIdle := p.maxOpen, p.maxIdle
	connMaxIdle, connMaxLife := p.connMaxIdle, p.connMaxLife
	if maxOpen <= 0 {
		maxOpen = 10
	}
	if maxIdle <= 0 {
		maxIdle = 5
	}
	if connMaxIdle <= 0 {
		connMaxIdle = 5 * time.Minute
	}
	if connMaxLife <= 0 {
		connMaxLife = 30 * time.Minute
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxIdleTime(connMaxIdle)
	db.SetConnMaxLifetime(connMaxLife)
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
		s.applyPool = poolConfig{maxOpen: maxOpen, maxIdle: maxIdle, connMaxIdle: connMaxIdle, connMaxLife: connMaxLife}
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
		s.applyPool.applyTo(db)
	}
	if err := migrate(ctx, db, engine); err != nil {
		return nil, fmt.Errorf("metastore: migrate: %w", err)
	}
	return s, nil
}

// sqliteDSN builds a modernc SQLite DSN for path with the store's required
// pragmas.
func sqliteDSN(path string) string {
	q := url.Values{}
	q.Set("_journal_mode", "WAL")
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	q.Set("_txlock", "immediate")
	q.Add("_pragma", "synchronous(NORMAL)")
	return path + "?" + q.Encode()
}

// postgresDSN renders a postgres:// URL DSN from a [defs.PostgreSQL] config,
// percent-escaping every value so a crafted password cannot inject extra
// connection parameters.
func postgresDSN(cfg defs.PostgreSQL) string {
	ssl := cfg.SslMode
	if ssl == "" {
		ssl = "disable"
	}
	host := cfg.Host
	if cfg.Port != "" {
		host = net.JoinHostPort(cfg.Host, cfg.Port)
	}
	q := url.Values{}
	q.Set("sslmode", ssl)
	if cfg.Schema != "" {
		q.Set("search_path", cfg.Schema)
	}
	if cfg.TimeZone != "" {
		q.Set("TimeZone", cfg.TimeZone)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     host,
		Path:     "/" + cfg.DBName,
		RawQuery: q.Encode(),
	}
	return u.String()
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
// one from inTx).
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// execer returns the queryer for a statement: the ambient transaction carried
// by ctx (Mode A / Do) if present, else the pool.
func (s *Store) execer(ctx context.Context) queryer {
	if tx, ok := sqltx.From(ctx); ok {
		return tx
	}
	return s.db
}

// inTx runs fn inside a transaction. If ctx already carries one (Mode A / a
// surrounding Do) it is reused and neither committed nor rolled back here — the
// owner handles that, including retry. Otherwise a fresh transaction is opened,
// committed on success, rolled back on error, and the whole call is wrapped in
// the bounded lock-error retry.
func (s *Store) inTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := sqltx.From(ctx); ok {
		return fn(ctx)
	}
	return s.withRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(sqltx.With(ctx, tx)); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		if err == nil || !isLockError(err) || attempt >= maxLockRetries {
			return err
		}
		backoff := time.Duration(100<<attempt) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// isLockError classifies transient lock/deadlock/serialization errors a bounded
// retry can clear (pgx codes 40001/40P01/55P03; modernc BUSY/LOCKED).
func isLockError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01" || pgErr.Code == "55P03"
	}
	var liteErr *sqlitedrv.Error
	if errors.As(err, &liteErr) {
		code := liteErr.Code() & 0xFF
		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "deadlock")
}

// --- dialect helpers -------------------------------------------------------

// rebind converts '?' placeholders to the $N form PostgreSQL requires; SQLite
// keeps '?'. Naive scan: it would corrupt a literal '?' inside a string
// constant or a jsonb `?` operator, so no rebind-ed query may contain either.
func (s *Store) rebind(q string) string {
	if s.engine != EnginePostgres {
		return q
	}
	var b strings.Builder
	b.Grow(len(q) + 8)
	n := 0
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(q[i])
	}
	return b.String()
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
	if s.engine == EngineSQLite {
		return t.UnixMicro()
	}
	return t.UTC()
}

// encTimePtr encodes a nullable time: NULL for a nil pointer, else encTime.
func (s *Store) encTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return s.encTime(*t)
}

// boolVal encodes a boolean: a bool for PostgreSQL, 0/1 for SQLite.
func (s *Store) boolVal(b bool) any {
	if s.engine == EngineSQLite {
		if b {
			return int64(1)
		}
		return int64(0)
	}
	return b
}

// boolScan holds a scanned boolean in whichever representation the engine uses.
type boolScan struct {
	b bool
	i int64
}

func (s *Store) boolDest(x *boolScan) any {
	if s.engine == EngineSQLite {
		return &x.i
	}
	return &x.b
}

func (s *Store) boolGet(x boolScan) bool {
	if s.engine == EngineSQLite {
		return x.i != 0
	}
	return x.b
}

// tsScan holds a scanned nullable timestamp in whichever representation the
// engine uses.
type tsScan struct {
	t sql.NullTime
	i sql.NullInt64
}

func (s *Store) tsDest(x *tsScan) any {
	if s.engine == EngineSQLite {
		return &x.i
	}
	return &x.t
}

// tsTime decodes a scanned timestamp to UTC; zero time when NULL.
func (s *Store) tsTime(x tsScan) time.Time {
	if s.engine == EngineSQLite {
		if x.i.Valid {
			return time.UnixMicro(x.i.Int64).UTC()
		}
		return time.Time{}
	}
	if x.t.Valid {
		return x.t.Time.UTC()
	}
	return time.Time{}
}

// tsTimePtr decodes a scanned timestamp to a *time.Time; nil when NULL.
func (s *Store) tsTimePtr(x tsScan) *time.Time {
	valid := x.t.Valid
	if s.engine == EngineSQLite {
		valid = x.i.Valid
	}
	if !valid {
		return nil
	}
	t := s.tsTime(x)
	return &t
}

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}
