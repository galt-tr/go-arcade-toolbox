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
	provider, err := migrationProvider(db, migrationsFS, engine, versionTable)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// MigrateDown rolls back the single most recently applied migration, through
// the same dialect and directory mapping [Migrate] uses so a caller never
// re-derives them. It exists for the migration tests that prove a Down path
// actually runs (dropping a column an index still references, say); production
// wiring only ever migrates up.
func MigrateDown(ctx context.Context, db *sql.DB, migrationsFS fs.FS, engine Engine, versionTable string) error {
	provider, err := migrationProvider(db, migrationsFS, engine, versionTable)
	if err != nil {
		return err
	}
	if _, err := provider.Down(ctx); err != nil {
		return fmt.Errorf("goose down: %w", err)
	}
	return nil
}

// migrationProvider builds the goose provider for engine's migration directory.
func migrationProvider(db *sql.DB, migrationsFS fs.FS, engine Engine, versionTable string) (*goose.Provider, error) {
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
		return nil, fmt.Errorf("no migrations for engine %q", engine)
	}

	sub, err := fs.Sub(migrationsFS, dir)
	if err != nil {
		return nil, fmt.Errorf("sub filesystem %q: %w", dir, err)
	}

	// A custom store carries the non-default version table name. goose requires
	// the dialect argument to NewProvider be empty when a store is supplied.
	store, err := goosedb.NewStore(dialect, versionTable)
	if err != nil {
		return nil, fmt.Errorf("goose store: %w", err)
	}
	provider, err := goose.NewProvider("", db, sub, goose.WithStore(store))
	if err != nil {
		return nil, fmt.Errorf("goose provider: %w", err)
	}
	return provider, nil
}
