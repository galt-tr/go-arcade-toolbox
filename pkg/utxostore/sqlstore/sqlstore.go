package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	// Registers the "pgx" database/sql driver used by the PostgreSQL engine.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Engine selects the SQL dialect a [Store] speaks. One implementation drives
// both; the differences are confined to the migration set, the placeholder
// syntax, the claim statement shape, and the timestamp encoding. It is an alias
// for [sqlkit.Engine], the single dialect enum shared with the metastore.
type Engine = sqlkit.Engine

const (
	// EnginePostgres is PostgreSQL, driven through the pgx stdlib driver.
	EnginePostgres = sqlkit.EnginePostgres
	// EngineSQLite is SQLite, driven through the CGo-free modernc driver.
	EngineSQLite = sqlkit.EngineSQLite
)

// errClosed is returned by every method after Close.
var errClosed = errors.New("sqlstore: store is closed")

// utxoCols is the canonical column projection scanned by scanUTXO, including
// the trailing seq used only for deterministic claim ordering.
const utxoCols = "txid, vout, user_id, basket, tier, satoshis, input_size, " +
	"reserved_by, reserved_at, spent_by, frozen, pinned, created_at, seq"

// utxoColsU is utxoCols qualified with the "u" alias for RETURNING clauses in
// the PostgreSQL claim statements (UPDATE ... FROM candidate c).
const utxoColsU = "u.txid, u.vout, u.user_id, u.basket, u.tier, u.satoshis, u.input_size, " +
	"u.reserved_by, u.reserved_at, u.spent_by, u.frozen, u.pinned, u.created_at, u.seq"

// Store is a SQL-backed [utxostore.Store] over PostgreSQL or SQLite. Create it
// with [New], [OpenPostgres], [OpenSQLite], or [Open]. It is safe for
// concurrent use.
type Store struct {
	db     *sql.DB
	engine Engine
	ownsDB bool
	now    func() time.Time
	pool   sqlkit.PoolConfig

	mu     sync.Mutex
	closed bool
}

// compile-time interface check.
var _ utxostore.Store = (*Store)(nil)

// defaultPool sizes an owned PostgreSQL pool for the claim hot path, not the
// low defs defaults. It backfills any field [WithConnPool] left zero. The
// metastore keeps a smaller default set; the sizing mechanism is shared via
// [sqlkit.PoolConfig].
var defaultPool = sqlkit.PoolConfig{
	MaxOpen:     25,
	MaxIdle:     10,
	ConnMaxIdle: 5 * time.Minute,
	ConnMaxLife: 30 * time.Minute,
}

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the store's clock, which stamps reserved_at (on claims)
// and created_at (on mints). Intended for tests that need deterministic
// timestamps; production stores use the default [time.Now].
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// WithConnPool configures the connection pool for a store that owns its pool
// (PostgreSQL via [OpenPostgres]/[Open]). Any zero value falls back to a sane
// default. It is ignored for SQLite (which pins a single writer connection) and
// for [New] (the caller owns the shared pool).
func WithConnPool(maxOpen, maxIdle int, connMaxIdle, connMaxLife time.Duration) Option {
	return func(s *Store) {
		s.pool = sqlkit.PoolConfig{MaxOpen: maxOpen, MaxIdle: maxIdle, ConnMaxIdle: connMaxIdle, ConnMaxLife: connMaxLife}
	}
}

// New wraps an existing *sql.DB and runs migrations against it. The caller
// retains ownership of db: Close does NOT close it. This is the Mode A
// constructor — the utxostore and metastore share one *sql.DB and enlist in a
// caller-owned transaction via internal/sqltx. engine must name the dialect db
// actually speaks.
//
// A SQLite db must already be pinned to a single connection
// (db.SetMaxOpenConns(1)) — the posture [OpenSQLite] and metastore.OpenSQLite
// install on the pools they own. New validates it and refuses otherwise: the
// pool is the caller's to size, so the alternative is silently reshaping it.
func New(ctx context.Context, db *sql.DB, engine Engine, opts ...Option) (*Store, error) {
	return newStore(ctx, db, engine, false, opts...)
}

// OpenPostgres opens a PostgreSQL Store from a DSN (postgres://... or
// key=value form), configures its connection pool (see [WithConnPool]; sane
// defaults otherwise), and runs migrations. Close closes the pool.
func OpenPostgres(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: open postgres: %w", err)
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
// runs migrations. path must be a real file: WAL requires one, so ":memory:"
// is not supported. Close closes the pool.
func OpenSQLite(ctx context.Context, path string, opts ...Option) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("sqlstore: open sqlite: %w", err)
	}
	// Single writer connection: SQLite serializes writes, so one connection
	// is the simplest correct posture and makes the claim UPDATE ... RETURNING
	// trivially exclusive without SKIP LOCKED. See the package doc.
	db.SetMaxOpenConns(1)
	s, err := newStore(ctx, db, EngineSQLite, true, opts...)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Open builds a Store from a [defs.Database] configuration, dispatching on its
// Engine. It is the config-driven constructor used by service wiring.
func Open(ctx context.Context, cfg defs.Database, opts ...Option) (*Store, error) {
	engine, err := defs.ParseDBTypeStr(string(cfg.Engine))
	if err != nil {
		return nil, fmt.Errorf("sqlstore: %w", err)
	}
	switch engine {
	case defs.DBTypeSQLite:
		return OpenSQLite(ctx, cfg.SQLite.ConnectionString, opts...)
	case defs.DBTypePostgres:
		// Thread the config's pool knobs through (prepended so an explicit
		// caller WithConnPool still wins).
		poolOpt := WithConnPool(cfg.MaxOpenConnections, cfg.MaxIdleConnections, cfg.MaxConnectionIdleTime, cfg.MaxConnectionTime)
		return OpenPostgres(ctx, postgresDSN(cfg.PostgreSQL), append([]Option{poolOpt}, opts...)...)
	default:
		return nil, fmt.Errorf("sqlstore: unsupported engine %q", cfg.Engine)
	}
}

func newStore(ctx context.Context, db *sql.DB, engine Engine, ownsDB bool, opts ...Option) (*Store, error) {
	switch engine {
	case EnginePostgres, EngineSQLite:
	default:
		return nil, fmt.Errorf("sqlstore: unknown engine %q", engine)
	}
	// A SQLite pool this store did NOT open must already be pinned to a single
	// connection. Validate rather than mutate: the handle belongs to the caller
	// (in Mode A, to the metastore sharing it), and quietly reshaping somebody
	// else's pool is a worse surprise than refusing to start. See the "single
	// writer" note in the package doc for what rests on the pin.
	if engine == EngineSQLite && !ownsDB {
		if n := db.Stats().MaxOpenConnections; n != 1 {
			return nil, fmt.Errorf("sqlstore: a shared SQLite handle must be pinned to one connection "+
				"(db.SetMaxOpenConns(1)); got MaxOpenConnections=%d", n)
		}
	}
	// The other half of "the pool is the caller's": on PostgreSQL there is
	// nothing to validate, but the ceiling is still not this store's. In Mode A
	// the handle is typically the metastore's, sized for metadata work (its
	// defaults are 10/5, against this package's own 25/10), and the claim hot
	// path silently inherits it — claims past the ceiling queue inside
	// database/sql before a statement ever reaches the server, so the wait shows
	// up as store latency and in no query plan. Log the number once at
	// construction rather than resize somebody else's pool.
	//
	// PostgreSQL only: a shared SQLite handle was just proven to be exactly 1,
	// so logging it would say nothing the branch above did not already require.
	//
	// TODO: this is the package's only log line, so it takes slog.Default()
	// rather than introduce a logger to a store that has none. If a second one
	// ever appears, follow aerostore's precedent and add a WithLogger option
	// defaulting to slog.Default() instead of growing more package-level calls.
	if !ownsDB && engine == EnginePostgres {
		slog.Default().DebugContext(ctx, "sqlstore: using a caller-supplied connection pool",
			slog.String("engine", string(engine)),
			slog.Int("max_open_conns", db.Stats().MaxOpenConnections))
	}
	s := &Store{db: db, engine: engine, ownsDB: ownsDB, now: time.Now}
	for _, opt := range opts {
		opt(s)
	}
	// The store owns a PostgreSQL pool: size it before migrating (which uses
	// the pool). SQLite pins SetMaxOpenConns(1) in OpenSQLite; a shared SQLite
	// pool was validated above and is left exactly as the caller sized it.
	if ownsDB && engine == EnginePostgres {
		s.pool.ApplyTo(db, defaultPool)
	}
	if err := migrate(ctx, db, engine); err != nil {
		return nil, fmt.Errorf("sqlstore: migrate: %w", err)
	}
	return s, nil
}

// sqliteDSN builds the shared modernc SQLite DSN for path (WAL, busy_timeout,
// synchronous=NORMAL, foreign_keys, immediate transactions). See [sqlkit.SQLiteDSN].
func sqliteDSN(path string) string {
	return sqlkit.SQLiteDSN(path)
}

// postgresDSN renders a postgres:// URL DSN from a [defs.PostgreSQL] config,
// percent-escaping every value so a crafted password cannot inject extra
// connection parameters. See [sqlkit.PostgresDSN] and the round-trip unit test.
func postgresDSN(cfg defs.PostgreSQL) string {
	return sqlkit.PostgresDSN(cfg)
}

// SharesDatabase reports whether this store runs over db, so a co-located
// store (e.g. the metastore in Mode A) can decide to share one transaction
// with it via internal/sqltx.
func (s *Store) SharesDatabase(db *sql.DB) bool {
	return s.db == db
}

// DB returns the underlying handle. Intended for Mode A wiring (opening a
// shared transaction) and for tests/diagnostics; ordinary callers use the
// [utxostore.Store] methods.
func (s *Store) DB() *sql.DB {
	return s.db
}

// Health implements [utxostore.Store].
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

// Close implements [utxostore.Store]. Idempotent; closes the pool only when
// this store opened it.
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

// queryer is the subset of *sql.DB / *sql.Tx the store uses, so the same code
// runs against the pool or a transaction (an ambient one from ctx, or a
// per-call one from withTx). It aliases [sqlkit.SQLExecer].
type queryer = sqlkit.SQLExecer

// execer returns the queryer for a non-transactional statement: the ambient
// transaction carried by ctx (Mode A) if present AND opened over this store's
// own *sql.DB, else the pool.
func (s *Store) execer(ctx context.Context) queryer {
	return sqlkit.Execer(ctx, s.db)
}

// withTx runs fn inside a transaction. If ctx already carries one opened over
// this store's own *sql.DB (Mode A) it is reused and neither committed nor
// rolled back here — the caller owns it, including any retry. Otherwise —
// including when ctx carries a transaction opened over a DIFFERENT *sql.DB —
// a fresh transaction is opened and the call is wrapped in the bounded
// lock-error retry: fn must return a lock error to request a rollback-and-retry;
// any other non-nil error also rolls back but is returned as-is; nil commits.
//
// Per-item failures (guard mismatches recorded on op.Err or a joined item
// error) must NOT be returned from fn: they are not transaction failures, and
// the successful items in the same batch must still commit. Callers track them
// out of band and assemble [utxostore.ErrBatch] after withTx returns nil.
func (s *Store) withTx(ctx context.Context, fn func(q queryer) error) error {
	if tx, ok := sqltx.From(ctx, s.db); ok {
		return fn(tx)
	}
	return sqlkit.WithRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

// rebind converts the '?' placeholders used throughout this package to the
// $N form PostgreSQL requires; SQLite keeps '?'. The claim statements are
// engine-specific and already carry their own placeholders, so they bypass
// rebind. See [sqlkit.Rebind] for the naive-scan caveat.
func (s *Store) rebind(q string) string {
	return sqlkit.Rebind(s.engine, q)
}

// forUpdate is the row-lock clause for a SELECT whose decision cannot be folded
// into the following write's WHERE. PostgreSQL waits on the fixed outpoint's
// lock (no SKIP LOCKED — there is nothing to skip to); SQLite's single writer
// connection already serializes, so it needs no clause.
//
// Exactly one caller is left: [Store.classifyForReserve], where an all-or-
// nothing multi-outpoint hold acquires its row locks in a canonical order and
// must keep them for the life of the transaction. Every other mutation carries
// its predicates in the write itself (see the "Guarded mutations" note in the
// package doc), so it has no read to protect.
func (s *Store) forUpdate() string {
	if s.engine == EnginePostgres {
		return " FOR UPDATE"
	}
	return ""
}

// guardAttempts caps a guarded mutation's write→classify→retry loop. The retry
// exists for exactly one outcome: the guarded write matched nothing, yet the
// follow-up read finds the row eligible again — a concurrent release, unspend
// or re-reservation landed between the two statements. One extra attempt
// settles that flip; a second failure with the row still looking eligible is
// live contention rather than a lost race, and is reported as such.
const guardAttempts = 2

// pgKeyRel is the outpoint key relation every SET-BASED PostgreSQL mutation in
// this package joins its target rows against: $1 binds the txids as a bytea[]
// and $2 the vouts as a bigint[], so ONE statement carries a whole batch. Every
// statement built on it therefore starts its own parameters at $3.
//
// Two ARRAYS rather than N (txid, vout) tuples is what keeps the statement text
// constant across batch sizes: PostgreSQL caches one plan instead of one per
// distinct N, and a thousand-item batch comes nowhere near the 65535-parameter
// ceiling a tuple list would spend two placeholders per op against.
//
// The casts are load-bearing, not decoration. An untyped parameter defaults to
// text, and `bytea = text` resolves to no operator at all — the statement fails
// to parse rather than mis-matching quietly. pgx binds [][]byte to bytea[] and
// []int64 to bigint[] natively (see [pairArgs]).
//
// # Why the two-ARGUMENT unnest, and not two unnests in a target list
//
// `unnest(a, b)` in the FROM clause is DEFINED to walk its arrays positionally,
// emitting the i-th element of each as one row: the pairing between a txid and
// its vout is a property of the function. The equivalent-looking
// `SELECT unnest($1) AS txid, unnest($2) AS vout` is not — there the pairing is
// a property of how the PLANNER expands multiple set-returning functions in a
// target list, and that rule has changed across major versions (before 10 they
// were cycled to the least common multiple of their row counts; 10 and later
// advance them in lockstep under ProjectSet, padding the shorter with NULLs).
// PostgreSQL's own documentation discourages relying on it.
//
// The failure that buys is not theoretical: a batch's arrays hold one txid per
// op, and a mis-pairing under any such rule crosses txid i with vout j and
// SPENDS THE WRONG COIN. Only a batch spanning several txids with asymmetric
// vouts can tell the two apart, which is what the mixed-txid conformance case
// exists for — a same-txid batch pairs correctly either way.
//
// PostgreSQL only: SQLite keeps the per-op loops these replace. There the loop
// costs no network round trip (the store pins a single local writer connection)
// and the engine has neither unnest nor a column-alias list for a derived
// table, so a batch statement there would be more text for no saving.
const pgKeyRel = `unnest($1::bytea[], $2::bigint[]) AS k(txid, vout)`

// pgKeyMatch pairs a target row aliased "u" with [pgKeyRel]. The ::integer
// narrows the key's bigint to the vout column's own type, so the comparison is
// int4 = int4 and the (txid, vout) primary key is directly usable.
//
// A key appearing TWICE in the arrays still updates — and RETURNS — its row
// exactly once: PostgreSQL applies at most one UPDATE per target row per
// statement. That is what makes a batch carrying a duplicate outpoint behave
// exactly as the sequential loop did, where the second write simply matched
// nothing.
const pgKeyMatch = `u.txid = k.txid AND u.vout = k.vout::integer`

// pairArgs splits ops into the two parallel arrays [pgKeyRel] binds: txids as
// bytea[] and vouts as bigint[]. The txid slices alias ops' backing array,
// which the driver reads before the call returns.
func pairArgs(ops []utxostore.Outpoint) ([][]byte, []int64) {
	txids := make([][]byte, len(ops))
	vouts := make([]int64, len(ops))
	for i := range ops {
		txids[i] = ops[i].TxID[:]
		vouts[i] = int64(ops[i].Vout)
	}
	return txids, vouts
}

// scanOutpointSet collects a (txid, vout) projection — a set-based statement's
// RETURNING clause, or the classifying join's key columns — into a set. It is
// the batch counterpart of RowsAffected: which of the ops the statement carried
// actually matched, rather than merely how many did.
func scanOutpointSet(rows *sql.Rows) (map[utxostore.Outpoint]struct{}, error) {
	defer func() { _ = rows.Close() }()
	out := make(map[utxostore.Outpoint]struct{})
	for rows.Next() {
		var (
			txid []byte
			vout uint32
		)
		if err := rows.Scan(&txid, &vout); err != nil {
			return nil, err
		}
		var op utxostore.Outpoint
		copy(op.TxID[:], txid)
		op.Vout = vout
		out[op] = struct{}{}
	}
	return out, rows.Err()
}

// notPinned is the "row is not pinned" predicate. It needs no dialect switch:
// SQLite has no boolean type, but NOT over an INTEGER 0/1 evaluates correctly
// (NOT 0 = 1, NOT 1 = 0), the same ruling [Store.Balance] already makes for
// "NOT frozen". Keeping it ONE constant is load-bearing rather than tidy —
// SQLite matches a partial index by comparing predicate text, so this is
// verbatim the clause idx_utxos_reserved_at is declared with in the 00002
// migrations, and the plan test pins that the stale scan never degrades to a
// table scan because the two drifted apart.
const notPinned = "NOT pinned"

// notPinnedOn is [notPinned] for a column carrying a table alias ("u" → "NOT
// u.pinned"), used by the self-joined stale scan.
func notPinnedOn(alias string) string { return "NOT " + alias + ".pinned" }

// notFrozen is the "row is not frozen" predicate carried by the guarded
// mutations. Like [notPinned] it needs no dialect switch: NOT over SQLite's
// INTEGER 0/1 evaluates correctly, the same ruling [Store.Balance] and the
// claim statements already make.
const notFrozen = "NOT frozen"

// notFrozenOn is [notFrozen] for a column carrying a table alias ("u" → "NOT
// u.frozen"), used by the set-based mutations that join against [pgKeyRel].
func notFrozenOn(alias string) string { return "NOT " + alias + ".frozen" }

// boolLit renders a boolean LITERAL for the engine (PostgreSQL TRUE/FALSE,
// SQLite 1/0), for the SET clauses that carry the value inline rather than as a
// bound parameter — so adding one does not renumber a statement's placeholders.
func (s *Store) boolLit(b bool) string {
	if s.engine == EnginePostgres {
		if b {
			return "TRUE"
		}
		return "FALSE"
	}
	if b {
		return "1"
	}
	return "0"
}

// encTime encodes t for a bound parameter: a UTC time.Time for PostgreSQL
// TIMESTAMPTZ, or Unix microseconds for SQLite's INTEGER columns.
func (s *Store) encTime(t time.Time) any {
	return sqlkit.EncTime(s.engine, t)
}

// boolVal encodes a boolean for a bound parameter: a bool for PostgreSQL, or
// 0/1 for SQLite's INTEGER columns.
func (s *Store) boolVal(b bool) any {
	return sqlkit.BoolVal(s.engine, b)
}

// boolScan holds a scanned boolean in whichever representation the engine uses
// (PostgreSQL BOOLEAN, SQLite INTEGER 0/1). Mirrors tsScan: use boolDest to
// build the Scan target and boolGet to decode. It aliases [sqlkit.BoolScan].
type boolScan = sqlkit.BoolScan

func (s *Store) boolDest(x *boolScan) any {
	return sqlkit.BoolDest(s.engine, x)
}

func (s *Store) boolGet(x boolScan) bool {
	return sqlkit.BoolGet(s.engine, x)
}

// decodeHash builds a chainhash.Hash from a spent_by column's bytes. The four
// spent_by decode sites share it. It stays local to sqlstore: the metastore
// stores txids as opaque bytes and never joins them against chainhash, so this
// is not shared dialect plumbing.
func decodeHash(b []byte) (*chainhash.Hash, error) {
	h, err := chainhash.NewHash(b)
	if err != nil {
		return nil, fmt.Errorf("sqlstore: decode spent_by: %w", err)
	}
	return h, nil
}

// tsScan holds a scanned nullable timestamp in whichever representation the
// engine uses. Use tsDest to build the Scan target and tsTime to decode. It
// aliases [sqlkit.TsScan].
type tsScan = sqlkit.TsScan

func (s *Store) tsDest(x *tsScan) any {
	return sqlkit.TsDest(s.engine, x)
}

func (s *Store) tsTime(x tsScan) time.Time {
	return sqlkit.TsGet(s.engine, x)
}

// rowScanner is satisfied by *sql.Row and *sql.Rows. It aliases [sqlkit.RowScanner].
type rowScanner = sqlkit.RowScanner

// scanUTXO scans the utxoCols projection into a UTXO plus its seq (used only
// for deterministic claim ordering).
func (s *Store) scanUTXO(sc rowScanner) (*utxostore.UTXO, int64, error) {
	var (
		u          utxostore.UTXO
		txid       []byte
		reservedBy sql.NullString
		spentBy    []byte
		seq        int64
		reservedAt tsScan
		createdAt  tsScan
		frozen     boolScan
		pinned     boolScan
	)
	dest := []any{
		&txid, &u.Vout, &u.UserID, &u.Basket, &u.Tier, &u.Satoshis, &u.InputSize,
		&reservedBy, s.tsDest(&reservedAt), &spentBy, s.boolDest(&frozen), s.boolDest(&pinned),
		s.tsDest(&createdAt), &seq,
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, 0, err
	}

	copy(u.TxID[:], txid)
	u.ReservedBy = reservedBy.String
	u.ReservedAt = s.tsTime(reservedAt)
	u.CreatedAt = s.tsTime(createdAt)
	u.Frozen = s.boolGet(frozen)
	u.Pinned = s.boolGet(pinned)
	if len(spentBy) > 0 {
		h, err := decodeHash(spentBy)
		if err != nil {
			return nil, 0, err
		}
		u.SpentBy = h
	}
	return &u, seq, nil
}

// batchErr wraps the ErrBatch sentinel with a count summary.
func batchErr(failed, total int) error {
	return fmt.Errorf("%w: %d of %d items failed", utxostore.ErrBatch, failed, total)
}

// joinBatch wraps per-item errors under ErrBatch (errors.Is finds the
// sentinel, errors.As finds the item errors), or returns nil when empty.
func joinBatch(itemErrs []error) error {
	if len(itemErrs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", utxostore.ErrBatch, errors.Join(itemErrs...))
}

// validateReserveOutpoints rejects underspecified outpoint-reservation inputs.
// An empty op list is a programmer error rather than a degenerate success: the
// caller asked for an all-or-nothing hold and named nothing to hold.
func validateReserveOutpoints(reservation string, ops []utxostore.Outpoint) error {
	switch {
	case reservation == "":
		return errors.New("sqlstore: reservation must be non-empty")
	case len(ops) == 0:
		return errors.New("sqlstore: ops must be non-empty")
	}
	return nil
}

// validateClaim rejects underspecified claim inputs; see the interface doc.
func validateClaim(sc utxostore.Scope, reservation string) error {
	switch {
	case reservation == "":
		return errors.New("sqlstore: reservation must be non-empty")
	case sc.UserID <= 0:
		return errors.New("sqlstore: scope user id must be positive")
	case sc.Basket == "":
		return errors.New("sqlstore: scope basket must be non-empty")
	case !sc.Tier.Valid():
		return fmt.Errorf("sqlstore: invalid scope tier %d", sc.Tier)
	}
	return nil
}
