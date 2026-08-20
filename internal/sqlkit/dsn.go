package sqlkit

import (
	"database/sql"
	"net"
	"net/url"
	"time"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// SQLiteDSN builds a modernc SQLite DSN for path with the concurrency pragmas
// the stores require. Every pragma is set through the driver-portable
// _pragma=name(value) query form — not the driver-specific shorthand keys
// (_journal_mode=WAL, _busy_timeout=5000, _foreign_keys=on) that some
// modernc versions parse and others silently drop. _pragma=name(value) is
// the one form modernc.org/sqlite has honored on every release since
// v1.14.7, so a future driver version change can't silently regress the
// concurrency posture. modernc applies these per-connection on every new
// connection, so the pool never hands out a connection missing them.
//
// Caveat: unlike the shorthand keys, which the driver rejects when the
// connection is opened for a bad value, a _pragma string is executed
// verbatim — a typo in the pragma name or value fails silently instead of
// erroring.
// TestSQLiteDSNAppliedPragmas is the guard: it opens a live connection and
// reads each pragma back to confirm it actually applied, rather than
// trusting the DSN string alone.
//
// _txlock=immediate is separate from the pragmas: it configures the driver's
// BEGIN mode, making a write transaction take its RESERVED lock up front
// rather than deadlocking on upgrade.
//
// These settings are Mode-A-critical: two stores sharing one SQLite file MUST
// open it with the same locking posture (WAL journaling, a busy timeout, and
// _txlock=immediate). This is the single canonical set.
func SQLiteDSN(path string) string {
	q := url.Values{}
	q.Set("_txlock", "immediate")
	q.Add("_pragma", "busy_timeout(5000)") // (modernc sorts it first anyway; explicit for clarity)
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
	return path + "?" + q.Encode()
}

// PostgresDSN renders a postgres:// URL DSN from a [defs.PostgreSQL] config. It
// uses net/url so that values with spaces or reserved characters (notably the
// password) are percent-escaped rather than truncated by a key=value parser —
// and so a crafted value cannot inject extra connection parameters (e.g.
// downgrade sslmode). See the PostgresDSN round-trip unit tests.
func PostgresDSN(cfg defs.PostgreSQL) string {
	ssl := cfg.SslMode
	if ssl == "" {
		ssl = "disable"
	}
	host := cfg.Host
	if cfg.Port != "" {
		host = net.JoinHostPort(cfg.Host, cfg.Port)
	}
	q := url.Values{}
	q.Set("sslmode", ssl)
	if cfg.Schema != "" {
		q.Set("search_path", cfg.Schema)
	}
	if cfg.TimeZone != "" {
		q.Set("TimeZone", cfg.TimeZone)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     host,
		Path:     "/" + cfg.DBName,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// PoolConfig holds connection-pool knobs applied to a pool a store owns. Any
// zero field falls back to the corresponding field of the defaults passed to
// [PoolConfig.ApplyTo]; the two stores keep DIFFERENT defaults (the claim hot
// path wants a larger pool than the metadata store), so the mechanism is shared
// here while each store supplies its own default sizing.
type PoolConfig struct {
	MaxOpen     int
	MaxIdle     int
	ConnMaxIdle time.Duration
	ConnMaxLife time.Duration
}

// ApplyTo configures db's pool, substituting the matching field of def for any
// zero field of p.
func (p PoolConfig) ApplyTo(db *sql.DB, def PoolConfig) {
	v := p
	if v.MaxOpen <= 0 {
		v.MaxOpen = def.MaxOpen
	}
	if v.MaxIdle <= 0 {
		v.MaxIdle = def.MaxIdle
	}
	if v.ConnMaxIdle <= 0 {
		v.ConnMaxIdle = def.ConnMaxIdle
	}
	if v.ConnMaxLife <= 0 {
		v.ConnMaxLife = def.ConnMaxLife
	}
	db.SetMaxOpenConns(v.MaxOpen)
	db.SetMaxIdleConns(v.MaxIdle)
	db.SetConnMaxIdleTime(v.ConnMaxIdle)
	db.SetConnMaxLifetime(v.ConnMaxLife)
}
