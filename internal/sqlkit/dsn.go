package sqlkit

import (
	"database/sql"
	"net"
	"net/url"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
)

// SQLiteDSN builds a modernc SQLite DSN for path with the concurrency pragmas
// the stores require. modernc parses these query parameters and applies them on
// every new connection, so the pool never hands out a connection missing them.
//
// These pragmas are Mode-A-critical: two stores sharing one SQLite file MUST
// open it with the same locking posture (WAL journaling, a busy timeout, and
// _txlock=immediate so a write transaction takes its RESERVED lock up front
// rather than deadlocking on upgrade). This is the single canonical set.
func SQLiteDSN(path string) string {
	q := url.Values{}
	q.Set("_journal_mode", "WAL")
	q.Set("_busy_timeout", "5000")
	q.Set("_foreign_keys", "on")
	q.Set("_txlock", "immediate")
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
