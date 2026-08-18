package testenv

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	// Registers the "pgx" database/sql driver used by DSN consumers and by
	// this file's own readiness probe and schema/database admin queries.
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	// postgresImage pins the exact image StartPostgres runs. Bump
	// deliberately (and re-run the integration self-test) when moving to a
	// new Postgres major version.
	postgresImage = "postgres:17-alpine"

	postgresPort = "5432/tcp"
)

// PostgresContainer is a running Postgres testcontainer, plus enough state
// to mint per-test DSNs, isolated schemas, and isolated databases against
// it.
//
// # Why core testcontainers-go API instead of the postgres module
//
// testcontainers-go ships a modules/postgres package with a purpose-built
// postgres.Run helper. This file deliberately does not use it: the module
// pulls in its own dependency surface (docker/go-connections aside, it adds
// snapshot/restore helpers, additional wait-strategy machinery, and a second
// place image-tag defaults can drift from what CreateDatabase/
// IsolatedSchemaDSN below assume) for functionality this package doesn't
// need — go-arcade-toolbox's tests only need "start postgres, wait until it
// accepts queries, hand back a DSN, tear it down." wait.ForSQL plus
// testcontainers.ContainerRequest covers that in about the same number of
// lines as calling the module would, without an extra go.mod entry to keep
// in sync with the testcontainers-go core version bumps this package
// already tracks for StartAerospike's custom wait strategy.
type PostgresContainer struct {
	container testcontainers.Container

	host     string
	port     int
	user     string
	password string
	adminDB  string
}

type postgresConfig struct {
	image    string
	user     string
	password string
	db       string
}

func postgresConfigFromEnv() postgresConfig {
	return postgresConfig{
		image:    getenvDefault("TESTENV_POSTGRES_IMAGE", postgresImage),
		user:     getenvDefault("TESTENV_POSTGRES_USER", "postgres"),
		password: getenvDefault("TESTENV_POSTGRES_PASSWORD", "postgres"),
		db:       getenvDefault("TESTENV_POSTGRES_DB", "testenv"),
	}
}

// PostgresOption customizes the container StartPostgres launches.
type PostgresOption func(*postgresOptions)

type postgresOptions struct {
	serverArgs []string
}

// WithPostgresServerArgs appends `-c key=value` server flags to the postgres
// command line.
//
// It exists for settings that CANNOT be changed after start-up: max_connections
// is the motivating one. The image's default is 100, so any test opening a
// larger pool exhausts it and fails with connection resets rather than with
// whatever it was actually measuring — a perf sweep at 128 workers reads as a
// toolbox failure when it is really the database refusing to talk. ALTER SYSTEM
// cannot fix that from inside the test: max_connections needs a restart, not a
// reload, so it has to be set here or not at all.
//
//	testenv.StartPostgres(t, testenv.WithPostgresServerArgs("max_connections=400"))
func WithPostgresServerArgs(args ...string) PostgresOption {
	return func(o *postgresOptions) {
		for _, a := range args {
			o.serverArgs = append(o.serverArgs, "-c", a)
		}
	}
}

// StartPostgres starts a Postgres container (see postgresImage for the
// pinned tag; override with TESTENV_POSTGRES_IMAGE), waits until it accepts
// connections and answers a real query, and registers its teardown in
// t.Cleanup — via startContainer, which registers Terminate even when
// testcontainers.Run fails after the container has already started (Run's
// non-atomic contract; see container.go). It skips the test with an
// actionable message if no container runtime is available.
//
// Cleanup is MANDATORY here (not merely best-effort): under rootless podman,
// Configure disables ryuk (see package doc), so that t.Cleanup call is the
// only thing that stops the container from outliving the test process.
func StartPostgres(t *testing.T, opts ...PostgresOption) *PostgresContainer {
	t.Helper()
	RequireRuntime(t)

	var o postgresOptions
	for _, opt := range opts {
		opt(&o)
	}

	cfg := postgresConfigFromEnv()

	// The inner ForSQL timeout is aligned with readinessBudget because
	// ForSQL otherwise applies its own private 60-second default; the outer
	// WithWaitStrategyAndDeadline below is the single authoritative budget
	// (see the readinessBudget doc in container.go).
	waitStrategy := wait.ForSQL(nat.Port(postgresPort), "pgx", func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", cfg.user, cfg.password, host, port.Port(), cfg.db)
	}).WithStartupTimeout(readinessBudget)

	ctr := startContainer(t, "postgres", cfg.image,
		testcontainers.WithExposedPorts(postgresPort),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_USER":     cfg.user,
			"POSTGRES_PASSWORD": cfg.password,
			"POSTGRES_DB":       cfg.db,
		}),
		// NOT testcontainers.WithWaitStrategy: that wrapper hardwires a
		// 60-second deadline that silently caps any longer inner timeout.
		// See the readinessBudget doc in container.go.
		testcontainers.WithWaitStrategyAndDeadline(readinessBudget, waitStrategy),
		// Appended to the image's own entrypoint command, so the defaults the
		// entrypoint sets (initdb, socket dir, …) are preserved and only these
		// flags are added. No args means no change to the image's behavior.
		testcontainers.WithCmdArgs(o.serverArgs...),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("testenv: postgres container host: %v", err)
	}
	mapped, err := ctr.MappedPort(ctx, nat.Port(postgresPort))
	if err != nil {
		t.Fatalf("testenv: postgres container mapped port: %v", err)
	}

	return &PostgresContainer{
		container: ctr,
		host:      host,
		port:      mapped.Int(),
		user:      cfg.user,
		password:  cfg.password,
		adminDB:   cfg.db,
	}
}

// DSN returns a postgres:// connection string for dbname on this container,
// as the user/password StartPostgres configured it with. sslmode=disable is
// always set (there is no TLS listener to disable it for on a throwaway
// local container).
func (p *PostgresContainer) DSN(dbname string) string {
	return p.dsn(dbname, nil)
}

func (p *PostgresContainer) dsn(dbname string, extra url.Values) string {
	params := url.Values{}
	for k, v := range extra {
		params[k] = v
	}
	if params.Get("sslmode") == "" {
		params.Set("sslmode", "disable")
	}

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(p.user, p.password),
		Host:     net.JoinHostPort(p.host, strconv.Itoa(p.port)),
		Path:     "/" + dbname,
		RawQuery: params.Encode(),
	}
	return u.String()
}

// adminConn opens a fresh *sql.DB against the container's admin database
// (the one StartPostgres created it with). Callers must Close it.
func (p *PostgresContainer) adminConn(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", p.DSN(p.adminDB))
	if err != nil {
		t.Fatalf("testenv: open admin connection: %v", err)
	}
	return db
}

// CreateDatabase creates a fresh, empty database named name in the running
// Postgres instance and returns a DSN pointed at it. The database is dropped
// (FORCE, disconnecting anything still attached) in t.Cleanup.
//
// Prefer [PostgresContainer.IsolatedSchemaDSN] for ordinary per-test
// isolation: a schema is far cheaper to create/drop than a whole database
// and is sufficient for almost every test. Reach for CreateDatabase only
// when a test genuinely needs database-level isolation (e.g. asserting
// behavior that is scoped per-database, such as catalog contents or
// connection-limit settings, which a schema can't isolate).
func (p *PostgresContainer) CreateDatabase(t *testing.T, name string) string {
	t.Helper()
	if err := validateIdentifier(name); err != nil {
		t.Fatalf("testenv: CreateDatabase(%q): %v", name, err)
	}

	admin := p.adminConn(t)
	defer func() { _ = admin.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+quoteIdent(name)); err != nil {
		t.Fatalf("testenv: CREATE DATABASE %s: %v", name, err)
	}

	t.Cleanup(func() {
		cleanupConn, err := sql.Open("pgx", p.DSN(p.adminDB))
		if err != nil {
			t.Logf("testenv: reopen admin connection to drop database %s: %v", name, err)
			return
		}
		defer func() { _ = cleanupConn.Close() }()

		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := cleanupConn.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+quoteIdent(name)+" WITH (FORCE)"); err != nil {
			t.Logf("testenv: DROP DATABASE %s: %v", name, err)
		}
	})

	return p.DSN(name)
}

// IsolatedSchemaDSN creates a fresh schema in the container's admin
// database, derives its name from t.Name() plus a random suffix (see
// testSchemaName), and returns a DSN whose search_path is that schema alone.
// The schema is dropped (CASCADE) in t.Cleanup.
//
// This is the preferred per-test isolation mechanism when many tests share
// one Postgres container. The pattern is a single parent test that starts
// the container once and t.Run-spawns (optionally parallel) subtests, each
// minting its own schema:
//
//	func TestSQLStoreConformance(t *testing.T) {
//		pg := testenv.StartPostgres(t)
//		t.Run("caseA", func(t *testing.T) {
//			t.Parallel()
//			dsn := pg.IsolatedSchemaDSN(t)
//			// migrate + exercise the store against dsn
//		})
//		t.Run("caseB", func(t *testing.T) { t.Parallel(); /* ... */ })
//	}
//
// (StartPostgres cannot be hoisted into TestMain: it requires a *testing.T
// for Skipf/Fatalf/Cleanup and TestMain only has a *testing.M.) Cleanups run
// LIFO and each subtest's cleanups complete before the parent's do, so every
// schema drop runs before the parent's container Terminate. Whatever a
// subtest's migrations create lands inside its own schema, so concurrent
// subtests never see each other's tables or rows, without needing a
// container (or even a database) per test.
//
// pgx applies the DSN's search_path query parameter as a run-time parameter
// at connection startup (equivalent to `SET search_path = ...` immediately
// after connecting), so no caller-side setup beyond using the returned DSN
// is required.
func (p *PostgresContainer) IsolatedSchemaDSN(t *testing.T) string {
	t.Helper()
	schema := testSchemaName(t)

	admin := p.adminConn(t)
	defer func() { _ = admin.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoteIdent(schema)); err != nil {
		t.Fatalf("testenv: CREATE SCHEMA %s: %v", schema, err)
	}

	t.Cleanup(func() {
		cleanupConn, err := sql.Open("pgx", p.DSN(p.adminDB))
		if err != nil {
			t.Logf("testenv: reopen admin connection to drop schema %s: %v", schema, err)
			return
		}
		defer func() { _ = cleanupConn.Close() }()

		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if _, err := cleanupConn.ExecContext(dropCtx, "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE"); err != nil {
			t.Logf("testenv: DROP SCHEMA %s: %v", schema, err)
		}
	})

	params := url.Values{}
	params.Set("search_path", schema)
	return p.dsn(p.adminDB, params)
}

var identifierSanitizer = regexp.MustCompile(`[^a-z0-9_]+`)

// testSchemaName derives a unique, valid Postgres identifier from t.Name():
// lowercase, with every run of characters outside [a-z0-9_] (subtest
// slashes, spaces, punctuation) collapsed to a single "_"; prefixed with
// "t_" (test names may start with a digit, which a bare Postgres identifier
// cannot); and suffixed with 4 crypto/rand bytes, hex-encoded, so that two
// runs of the same test — or two subtests whose names sanitize to the same
// string — never collide. Postgres identifiers are capped at 63 bytes; a
// name that would exceed that keeps its prefix and, critically, its last 13
// bytes (which always contain the full random suffix) rather than truncating
// from the end, which would silently throw the uniqueness guarantee away.
func testSchemaName(t *testing.T) string {
	t.Helper()
	sanitized := identifierSanitizer.ReplaceAllString(strings.ToLower(t.Name()), "_")

	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("testenv: generate random schema suffix: %v", err)
	}

	name := "t_" + sanitized + "_" + hex.EncodeToString(suffix)
	const maxIdentifierLen = 63
	if len(name) > maxIdentifierLen {
		name = name[:maxIdentifierLen-13] + name[len(name)-13:]
	}
	return name
}

// quoteIdent double-quotes a Postgres identifier for safe interpolation into
// DDL (CREATE/DROP DATABASE and CREATE/DROP SCHEMA take no bind parameters
// for identifiers). Every caller in this file passes either a
// validateIdentifier-checked, caller-supplied name or a name this file
// generated itself (testSchemaName), but quoting is cheap insurance against
// reserved words and mixed case regardless.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

var validIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateIdentifier rejects names that aren't plain SQL identifiers.
// CreateDatabase interpolates name directly into DDL, so this is the only
// thing standing between a caller-supplied name and a SQL injection /
// malformed-statement bug.
func validateIdentifier(name string) error {
	if len(name) == 0 || len(name) > 63 || !validIdentifierPattern.MatchString(name) {
		return fmt.Errorf("invalid identifier %q: must match %s and be 1-63 bytes", name, validIdentifierPattern.String())
	}
	return nil
}
