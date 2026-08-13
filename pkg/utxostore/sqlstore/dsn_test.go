package sqlstore

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// TestPostgresDSNRoundTrip pins that postgresDSN produces a DSN pgx can parse
// back with every field intact — in particular a password containing a space
// (which a raw key=value DSN silently truncates) and reserved characters that
// a naive builder would let inject extra connection parameters.
func TestPostgresDSNRoundTrip(t *testing.T) {
	cfg := defs.PostgreSQL{
		SQLCommon: defs.SQLCommon{
			Host:     "db.example.com",
			Port:     "6543",
			User:     "wallet user",
			Password: "p@ss w0rd/# sslmode=disable&x=1",
			DBName:   "arcade",
			TimeZone: "UTC",
		},
		SslMode: "require",
		Schema:  "utxo_schema",
	}

	dsn := postgresDSN(cfg)
	parsed, err := pgconn.ParseConfig(dsn)
	require.NoError(t, err, "built DSN must parse: %s", dsn)

	require.Equal(t, cfg.Password, parsed.Password, "password must round-trip exactly, spaces and all")
	require.Equal(t, cfg.User, parsed.User)
	require.Equal(t, cfg.Host, parsed.Host)
	require.Equal(t, uint16(6543), parsed.Port)
	require.Equal(t, cfg.DBName, parsed.Database)
	require.Equal(t, cfg.Schema, parsed.RuntimeParams["search_path"])

	// The injection payload in the password must NOT downgrade sslmode: require
	// leaves pgx with a TLS config, disable would not.
	require.NotNil(t, parsed.TLSConfig, "sslmode=require must survive; password must not inject sslmode=disable")
}
