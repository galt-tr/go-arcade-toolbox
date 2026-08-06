package defs

import (
	"fmt"
	"time"
)

// DBType represents supported database types
type DBType string

// Supported database types.
//
// DBTypeMySQL is retained for compile compatibility with configs/enums that
// still reference it, but MySQL is not a supported engine in go-arcade-toolbox:
// Database.Validate rejects it (use postgres or sqlite).
const (
	DBTypeMySQL    DBType = "mysql"
	DBTypeSQLite   DBType = "sqlite"
	DBTypePostgres DBType = "postgres"
)

// ParseDBTypeStr parses a string to a DBType or returns an error.
// Note: "mysql" parses successfully (compile compat) but is rejected by Database.Validate.
func ParseDBTypeStr(dbType string) (DBType, error) {
	return parseEnumCaseInsensitive(dbType, DBTypeMySQL, DBTypeSQLite, DBTypePostgres)
}

// Defaults for database configuration
const (
	DSNDefault         = "./storage.sqlite" // DSN for connection (file or memory, default is memory)
	DefaultTablePrefix = "bsv_"
)

// Database is a struct that configures the database connection
type Database struct {
	// Engine is the database engine (PostgreSQL, SQLite)
	Engine DBType `mapstructure:"engine"`

	// SQLite is configuration struct for SQLite database
	SQLite SQLite `mapstructure:"sqlite"`

	// PostgreSQL is configuration for PostgreSQL databases
	PostgreSQL PostgreSQL `mapstructure:"postgresql"`

	// MaxIdleConnections defines the maximum number of idle connections allowed for the database.
	MaxIdleConnections int `mapstructure:"max_idle_connections"`

	// MaxConnectionIdleTime sets the maximum duration an idle connection can remain open before being closed.
	// Typically set in seconds.
	MaxConnectionIdleTime time.Duration `mapstructure:"max_connection_idle_time"`

	// MaxConnectionTime defines the maximum amount of time a connection may be reused.
	// Typically set in seconds.
	MaxConnectionTime time.Duration `mapstructure:"max_connection_time"`

	// MaxOpenConnections specifies the maximum number of open connections to the database.
	MaxOpenConnections int `mapstructure:"max_open_connections"`
}

// SQLite is configuration struct for SQLite database
type SQLite struct {
	// ConnectionString is the path to SQLite DB
	ConnectionString string `mapstructure:"connection_string"`
}

// PostgreSQL is configuration struct for PostgreSQL database
type PostgreSQL struct {
	SQLCommon `mapstructure:",squash"`

	// ssl mode  [disable|allow|prefer|require|verify-ca|verify-full]. Will default to disable if not provided
	SslMode string `mapstructure:"ssl_mode"`

	// Schema allows separating instances within a single Postgres DB (default is empty/public)
	Schema string `mapstructure:"schema"`
}

// SQLCommon is configuration struct for common properties for SQL databases such as postgres
type SQLCommon struct {
	Host     string `mapstructure:"host"`
	DBName   string `mapstructure:"db_name"`
	Password string `mapstructure:"password"`
	Port     string `mapstructure:"port"`
	TimeZone string `mapstructure:"time_zone"`
	User     string `mapstructure:"user"`
}

// DefaultDBConfig sets default configuration for the database
func DefaultDBConfig() Database {
	return Database{
		Engine:                DBTypeSQLite,
		SQLite:                SQLite{ConnectionString: DSNDefault},
		MaxIdleConnections:    5,
		MaxConnectionIdleTime: 360 * time.Second,
		MaxConnectionTime:     60 * time.Second,
		MaxOpenConnections:    5,
		PostgreSQL: PostgreSQL{
			SslMode: "verify-full",
			SQLCommon: SQLCommon{
				Host:     "localhost",
				DBName:   "storage",
				User:     "postgres",
				Password: "<set-via-secret>",
				Port:     "5432",
				TimeZone: "UTC",
			},
		},
	}
}

// Validate validates if database configuration is valid
func (db *Database) Validate() (err error) {
	if db.Engine, err = ParseDBTypeStr(string(db.Engine)); err != nil {
		return fmt.Errorf("invalid DB engine: %w", err)
	}

	if db.Engine == DBTypeMySQL {
		return fmt.Errorf("mysql is not supported by go-arcade-toolbox; use postgres or sqlite")
	}

	return nil
}
