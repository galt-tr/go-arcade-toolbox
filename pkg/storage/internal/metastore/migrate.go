package metastore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	goosedb "github.com/pressly/goose/v3/database"
)

// migrationsFS holds the embedded goose migration sets, one directory per
// engine. Separate directories keep the PostgreSQL and SQLite DDL — which
// differ in column types, boolean spelling and timestamp encoding —
// independent.
//
//go:embed migrations/postgres/*.sql migrations/sqlite/*.sql
var migrationsFS embed.FS

// versionTable is the goose bookkeeping table name. It is deliberately DISTINCT
// from sqlstore's goose_db_version_utxo so that, in Mode A (metastore and
// utxostore sharing one database), each package tracks its own migration chain
// without colliding.
const versionTable = "goose_db_version_meta"

// migrate brings db up to the latest metadata schema for engine. Idempotent:
// goose applies only pending versions.
func migrate(ctx context.Context, db *sql.DB, engine Engine) error {
	var (
		dialect goose.Dialect
		dir     string
	)
	switch engine {
	case EnginePostgres:
		dialect, dir = goose.DialectPostgres, "migrations/postgres"
	case EngineSQLite:
		dialect, dir = goose.DialectSQLite3, "migrations/sqlite"
	default:
		return fmt.Errorf("no migrations for engine %q", engine)
	}

	sub, err := fs.Sub(migrationsFS, dir)
	if err != nil {
		return fmt.Errorf("sub filesystem %q: %w", dir, err)
	}

	store, err := goosedb.NewStore(dialect, versionTable)
	if err != nil {
		return fmt.Errorf("goose store: %w", err)
	}
	provider, err := goose.NewProvider("", db, sub, goose.WithStore(store))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}
