package metastore

import (
	"context"
	"database/sql"
	"embed"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqlkit"
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
// goose applies only pending versions. The shared runner lives in
// [sqlkit.Migrate]; this package supplies its own embed.FS and version-table name.
func migrate(ctx context.Context, db *sql.DB, engine Engine) error {
	return sqlkit.Migrate(ctx, db, migrationsFS, engine, versionTable)
}
