package sqlkit

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	goosedb "github.com/pressly/goose/v3/database"
)

// Migrate brings db up to the latest schema for engine using the goose
// migrations embedded in migrationsFS. It is idempotent: goose applies only
// pending versions.
//
// migrationsFS must contain one directory per engine — "migrations/postgres" and
// "migrations/sqlite" — matching each store's embed pattern. versionTable is the
// goose bookkeeping table name; each store passes its OWN distinct name (e.g.
// goose_db_version_utxo vs goose_db_version_meta) so that, in Mode A (both
// stores sharing one database), each package tracks its own migration chain
// without colliding.
func Migrate(ctx context.Context, db *sql.DB, migrationsFS fs.FS, engine Engine, versionTable string) error {
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

	// A custom store carries the non-default version table name. goose requires
	// the dialect argument to NewProvider be empty when a store is supplied.
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
