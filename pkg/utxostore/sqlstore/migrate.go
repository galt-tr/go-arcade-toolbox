package sqlstore

import (
	"context"
	"database/sql"
	"embed"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/sqlkit"
)

// migrationsFS holds the embedded goose migration sets, one directory per
// engine. Separate directories keep the PostgreSQL and SQLite DDL — which
// differ in column types, the seq scheme, and boolean spelling — independent.
//
//go:embed migrations/postgres/*.sql migrations/sqlite/*.sql
var migrationsFS embed.FS

// versionTable is the goose bookkeeping table name. It is deliberately NOT the
// default ("goose_db_version") so that, in Mode A (utxostore and metastore
// sharing one database), each package tracks its own migration chain without
// colliding.
const versionTable = "goose_db_version_utxo"

// migrate brings db up to the latest schema for engine using the embedded
// goose migrations. It is idempotent: goose applies only pending versions. The
// shared runner lives in [sqlkit.Migrate]; this package supplies its own
// embed.FS and version-table name.
func migrate(ctx context.Context, db *sql.DB, engine Engine) error {
	return sqlkit.Migrate(ctx, db, migrationsFS, engine, versionTable)
}
