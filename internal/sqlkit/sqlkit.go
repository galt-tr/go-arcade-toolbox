// Package sqlkit is the shared SQL dialect scaffolding for the toolbox's
// database/sql-backed stores over PostgreSQL and SQLite (currently
// pkg/utxostore/sqlstore and pkg/storage/internal/metastore). One
// implementation of each helper drives both engines through a dialect switch
// over raw database/sql (no ORM).
//
// It exists to REMOVE the copy-adapted duplication that previously lived, byte
// for byte, in both stores: the '?'→$N placeholder rewrite, the lock-error
// classifier and bounded retry, the timestamp/boolean codecs, the DSN builders
// (with their concurrency pragmas), the connection-pool sizing, the goose
// migration runner, and the ambient-transaction resolver. Single-sourcing them
// is not cosmetic: in Mode A the two stores share ONE *sql.DB (and one SQLite
// file), so their DSN pragmas must match or the enlisted connections have an
// inconsistent locking posture, and the outer transaction owner's retry
// classifier governs retries across BOTH statement chains. Two hand-maintained
// copies were a latent source of silent divergence; this package is the one
// source of truth.
//
// sqlkit is the dialect/tx/DSN/migration plumbing ONLY. Anything store-specific
// — the row-scan projections, the SQL query strings, the FOR UPDATE vs FOR
// UPDATE SKIP LOCKED clauses, the interface implementations — stays in each
// store. The ambient-transaction context seam itself is internal/sqltx, which
// remains a separate package; sqlkit consumes it via [Execer].
package sqlkit

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sqltx"
)

// Engine selects the SQL dialect a store speaks. One set of helpers drives both;
// the differences are confined to the migration set, the placeholder syntax, the
// timestamp encoding and the boolean spelling.
type Engine string

const (
	// EnginePostgres is PostgreSQL, driven through the pgx stdlib driver.
	EnginePostgres Engine = "postgres"
	// EngineSQLite is SQLite, driven through the CGo-free modernc driver.
	EngineSQLite Engine = "sqlite"
)

// Rebind converts the '?' placeholders used throughout the stores to the $N form
// PostgreSQL requires; SQLite keeps '?'. Statements that carry engine-specific
// placeholders already (e.g. the sqlstore claim statements) must bypass Rebind.
//
// Caveat: this is a naive scan that rewrites every '?', so it would corrupt a
// literal '?' inside a string constant or a PostgreSQL jsonb `?` operator. No
// rebind-ed query may contain either; keep it that way.
func Rebind(engine Engine, query string) string {
	if engine != EnginePostgres {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

// SQLExecer is the subset of *sql.DB / *sql.Tx the stores use, so the same code
// runs against the pool or a transaction (an ambient one from ctx, or a
// per-call one a store opens itself).
type SQLExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Execer returns the SQLExecer for a statement: the ambient transaction carried
// by ctx (Mode A) if present, otherwise db.
//
// Caveat (the SharesDatabase contract): an ambient transaction is honored
// UNCONDITIONALLY — there is no portable way to prove a *sql.Tx belongs to db
// from the *sql.Tx alone. The caller is responsible for only ever placing a
// transaction opened from the same *sql.DB the store runs over (see
// internal/sqltx and each store's SharesDatabase probe).
func Execer(ctx context.Context, db SQLExecer) SQLExecer {
	if tx, ok := sqltx.From(ctx); ok {
		return tx
	}
	return db
}

// RowScanner is satisfied by *sql.Row and *sql.Rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// --- timestamp codec -------------------------------------------------------

// EncTime encodes t for a bound parameter: a UTC time.Time for PostgreSQL
// TIMESTAMPTZ, or Unix microseconds for SQLite's INTEGER columns. Microsecond
// resolution matches PostgreSQL, so a timestamp round-trips identically through
// both engines.
func EncTime(engine Engine, t time.Time) any {
	if engine == EngineSQLite {
		return t.UnixMicro()
	}
	return t.UTC()
}

// EncTimePtr encodes a nullable time: NULL for a nil pointer, else EncTime.
func EncTimePtr(engine Engine, t *time.Time) any {
	if t == nil {
		return nil
	}
	return EncTime(engine, *t)
}

// TsScan holds a scanned nullable timestamp in whichever representation the
// engine uses (PostgreSQL TIMESTAMPTZ, SQLite INTEGER Unix micros). Build the
// Scan target with [TsDest] and decode with [TsGet]/[TsGetPtr].
type TsScan struct {
	t sql.NullTime  // postgres
	i sql.NullInt64 // sqlite
}

// TsDest returns the Scan target for a TsScan under engine.
func TsDest(engine Engine, x *TsScan) any {
	if engine == EngineSQLite {
		return &x.i
	}
	return &x.t
}

// TsGet decodes a scanned timestamp to UTC; the zero time when NULL.
func TsGet(engine Engine, x TsScan) time.Time {
	if engine == EngineSQLite {
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

// TsGetPtr decodes a scanned timestamp to a *time.Time; nil when NULL.
func TsGetPtr(engine Engine, x TsScan) *time.Time {
	valid := x.t.Valid
	if engine == EngineSQLite {
		valid = x.i.Valid
	}
	if !valid {
		return nil
	}
	t := TsGet(engine, x)
	return &t
}

// --- boolean codec ---------------------------------------------------------

// BoolVal encodes a boolean for a bound parameter: a bool for PostgreSQL, or
// 0/1 for SQLite's INTEGER columns.
func BoolVal(engine Engine, b bool) any {
	if engine == EngineSQLite {
		if b {
			return int64(1)
		}
		return int64(0)
	}
	return b
}

// BoolScan holds a scanned boolean in whichever representation the engine uses
// (PostgreSQL BOOLEAN, SQLite INTEGER 0/1). Build the Scan target with
// [BoolDest] and decode with [BoolGet].
type BoolScan struct {
	b bool  // postgres
	i int64 // sqlite
}

// BoolDest returns the Scan target for a BoolScan under engine.
func BoolDest(engine Engine, x *BoolScan) any {
	if engine == EngineSQLite {
		return &x.i
	}
	return &x.b
}

// BoolGet decodes a scanned boolean.
func BoolGet(engine Engine, x BoolScan) bool {
	if engine == EngineSQLite {
		return x.i != 0
	}
	return x.b
}
