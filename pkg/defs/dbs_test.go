package defs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
)

func TestDatabaseValidate(t *testing.T) {
	t.Run("default (sqlite) config validates", func(t *testing.T) {
		cfg := defs.DefaultDBConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("postgres engine validates", func(t *testing.T) {
		cfg := defs.DefaultDBConfig()
		cfg.Engine = defs.DBTypePostgres
		require.NoError(t, cfg.Validate())
	})

	t.Run("engine is parsed case-insensitively", func(t *testing.T) {
		cfg := defs.DefaultDBConfig()
		cfg.Engine = "SQLite"
		require.NoError(t, cfg.Validate())
		require.Equal(t, defs.DBTypeSQLite, cfg.Engine)
	})

	t.Run("mysql engine is rejected with a clear error", func(t *testing.T) {
		cfg := defs.DefaultDBConfig()
		cfg.Engine = defs.DBTypeMySQL
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "mysql is not supported by go-arcade-toolbox")
		require.Contains(t, err.Error(), "postgres or sqlite")
	})

	t.Run("unknown engine is rejected", func(t *testing.T) {
		cfg := defs.DefaultDBConfig()
		cfg.Engine = "oracle"
		require.Error(t, cfg.Validate())
	})
}
