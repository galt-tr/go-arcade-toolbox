//go:build integration

package testenv_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"

	// Registers the "pgx" database/sql driver.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
)

// TestPostgresHelper_SelfTest proves StartPostgres works end-to-end against a
// real container runtime: start the container, connect with database/sql
// over the pgx stdlib driver, run a query, and exercise IsolatedSchemaDSN's
// isolation guarantee. This is the file the Task-9 spec asks for to prove
// the helpers work on a real Fedora + rootless-podman box; it is the only
// build-tagged file in this package — everything else here is a library, not
// a test, and must build without the "integration" tag.
func TestPostgresHelper_SelfTest(t *testing.T) {
	pg := testenv.StartPostgres(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", pg.DSN("testenv"))
	require.NoError(t, err, "open connection via pgx stdlib driver")
	defer db.Close()

	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)

	t.Run("IsolatedSchemaDSN isolates concurrent tests", func(t *testing.T) {
		dsn1 := pg.IsolatedSchemaDSN(t)
		dsn2 := pg.IsolatedSchemaDSN(t)
		require.NotEqual(t, dsn1, dsn2, "two calls must mint two distinct schemas")

		db1, err := sql.Open("pgx", dsn1)
		require.NoError(t, err)
		defer db1.Close()

		db2, err := sql.Open("pgx", dsn2)
		require.NoError(t, err)
		defer db2.Close()

		_, err = db1.ExecContext(ctx, "CREATE TABLE widgets (id int PRIMARY KEY)")
		require.NoError(t, err)
		_, err = db1.ExecContext(ctx, "INSERT INTO widgets (id) VALUES (1)")
		require.NoError(t, err)

		// A table created under dsn1's schema must not be visible from
		// dsn2's schema: search_path scopes each connection to its own
		// schema alone.
		_, err = db2.ExecContext(ctx, "SELECT * FROM widgets")
		require.Error(t, err, "widgets table must not be visible from the other isolated schema")
	})
}

// TestAerospikeHelper_SelfTest proves StartAerospike works end-to-end: start
// the container, connect with the real aerospike-client-go library, and run
// an info command that verifies both the namespace name and, critically,
// that default-ttl is 0 (never expire) — the invariant the aerostore
// provider requires and that this whole helper exists to guarantee.
func TestAerospikeHelper_SelfTest(t *testing.T) {
	as := testenv.StartAerospike(t)

	client, aerr := aerospike.NewClient(as.Host(), as.Port())
	require.NoError(t, aerr, "connect with aerospike-client-go")
	defer client.Close()

	require.True(t, client.IsConnected())

	nodes := client.GetNodes()
	require.NotEmpty(t, nodes, "expected at least one aerospike node")

	info, aerr := nodes[0].RequestInfo(client.DefaultInfoPolicy, "namespaces")
	require.NoError(t, aerr, "info command: namespaces")
	namespaces := strings.Split(strings.TrimSpace(info["namespaces"]), ";")
	require.Contains(t, namespaces, as.Namespace(), "configured namespace must be reported by the server")

	nsConfig, aerr := nodes[0].RequestInfo(client.DefaultInfoPolicy, "get-config:context=namespace;id="+as.Namespace())
	require.NoError(t, aerr, "info command: get-config for namespace")

	config := parseSemicolonKV(nsConfig["get-config:context=namespace;id="+as.Namespace()])
	require.Equal(t, "0", config["default-ttl"],
		"namespace %q must have default-ttl=0 (never expire); aerostore refuses nonzero default-ttl namespaces", as.Namespace())
}

// parseSemicolonKV parses Aerospike's "key1=value1;key2=value2;..." info
// response format into a map.
func parseSemicolonKV(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ";") {
		if kv == "" {
			continue
		}
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			continue
		}
		out[parts[0]] = parts[1]
	}
	return out
}
