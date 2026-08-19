package sqlkit

import (
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	sqlitedrv "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestRebind(t *testing.T) {
	const q = "SELECT * FROM t WHERE a=? AND b=? AND c=?"
	if got := Rebind(EngineSQLite, q); got != q {
		t.Fatalf("sqlite must pass through unchanged: got %q", got)
	}
	want := "SELECT * FROM t WHERE a=$1 AND b=$2 AND c=$3"
	if got := Rebind(EnginePostgres, q); got != want {
		t.Fatalf("postgres rebind: got %q want %q", got, want)
	}
	if got := Rebind(EnginePostgres, "no placeholders"); got != "no placeholders" {
		t.Fatalf("no-placeholder rebind changed the string: %q", got)
	}
}

func TestIsLockError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"pg serialization", &pgconn.PgError{Code: "40001"}, true},
		{"pg deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"pg lock not available", &pgconn.PgError{Code: "55P03"}, true},
		{"pg other", &pgconn.PgError{Code: "23505"}, false},
		{"sqlite busy", &sqlitedrv.Error{}, false}, // zero-value code 0, not BUSY/LOCKED
		{"string database is locked", errors.New("database is locked"), true},
		{"string database table is locked", errors.New("database table is locked"), true},
		{"string deadlock", errors.New("deadlock detected somewhere"), true},
		{"unrelated", errors.New("connection refused"), false},
	}
	for _, c := range cases {
		if got := IsLockError(c.err); got != c.want {
			t.Errorf("%s: IsLockError = %v want %v", c.name, got, c.want)
		}
	}

	// Sanity-check that the modernc primary codes we classify are the intended
	// BUSY/LOCKED constants (extended codes mask to these).
	if sqlite3.SQLITE_BUSY == 0 || sqlite3.SQLITE_LOCKED == 0 {
		t.Fatal("unexpected modernc primary code constants")
	}
}

func TestTimestampCodecRoundTrip(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 34, 56, 789000*1000, time.UTC) // microsecond-aligned
	for _, engine := range []Engine{EnginePostgres, EngineSQLite} {
		enc := EncTime(engine, base)

		// Simulate a scan by pushing the encoded value into the engine's Scan
		// target, then decoding it back.
		var x TsScan
		dest := TsDest(engine, &x)
		switch engine {
		case EngineSQLite:
			*dest.(*sql.NullInt64) = sql.NullInt64{Int64: enc.(int64), Valid: true}
		case EnginePostgres:
			*dest.(*sql.NullTime) = sql.NullTime{Time: enc.(time.Time), Valid: true}
		}
		if got := TsGet(engine, x); !got.Equal(base) {
			t.Errorf("%s: timestamp round-trip: got %v want %v", engine, got, base)
		}
		if got := TsGetPtr(engine, x); got == nil || !got.Equal(base) {
			t.Errorf("%s: timestamp ptr round-trip: got %v want %v", engine, got, base)
		}

		// A NULL scan decodes to the zero time / a nil pointer on both engines.
		var null TsScan
		if got := TsGet(engine, null); !got.IsZero() {
			t.Errorf("%s: NULL timestamp must decode to zero time, got %v", engine, got)
		}
		if got := TsGetPtr(engine, null); got != nil {
			t.Errorf("%s: NULL timestamp ptr must decode to nil, got %v", engine, got)
		}
	}
}

func TestBoolCodec(t *testing.T) {
	for _, engine := range []Engine{EnginePostgres, EngineSQLite} {
		for _, b := range []bool{true, false} {
			enc := BoolVal(engine, b)
			var x BoolScan
			dest := BoolDest(engine, &x)
			switch engine {
			case EngineSQLite:
				if enc.(int64) != map[bool]int64{true: 1, false: 0}[b] {
					t.Errorf("sqlite BoolVal(%v) = %v", b, enc)
				}
				*dest.(*int64) = enc.(int64)
			case EnginePostgres:
				*dest.(*bool) = enc.(bool)
			}
			if got := BoolGet(engine, x); got != b {
				t.Errorf("%s: bool round-trip %v: got %v", engine, b, got)
			}
		}
	}
}

// TestSQLiteDSNPragmas pins the DSN string shape: _txlock=immediate plus the
// four pragmas in the driver-portable _pragma=name(value) form. See
// TestSQLiteDSNAppliedPragmas below for the runtime counterpart that proves
// these values are actually honored by the driver, not just present in the
// string.
func TestSQLiteDSNPragmas(t *testing.T) {
	dsn := SQLiteDSN("/data/wallet.db")
	base, rawQuery, ok := splitDSN(dsn)
	if !ok || base != "/data/wallet.db" {
		t.Fatalf("SQLiteDSN base path mangled: %q", dsn)
	}
	q, err := url.ParseQuery(rawQuery)
	require.NoError(t, err)

	require.Equal(t, "immediate", q.Get("_txlock"))
	require.ElementsMatch(t, []string{
		"busy_timeout(5000)",
		"journal_mode(WAL)",
		"foreign_keys(ON)",
		"synchronous(NORMAL)",
	}, q["_pragma"], "SQLiteDSN must set all four pragmas via the portable _pragma=name(value) form")

	for _, k := range []string{"_journal_mode", "_busy_timeout", "_foreign_keys", "_synchronous"} {
		require.NotContains(t, q, k, "mattn shorthand %s must not reappear; modernc <=v1.54.0 drops it", k)
	}
}

// TestSQLiteDSNAppliedPragmas opens a real file-backed SQLite DB through
// SQLiteDSN and asserts the pragmas the driver actually applied, not just
// the DSN string. This is the test that would have caught the audit finding
// (P1-5): the DSN previously used mattn-go-sqlite3-style shorthand keys
// (_journal_mode=WAL, _busy_timeout=5000, _foreign_keys=on) which
// modernc.org/sqlite <= v1.54.0 silently DROPPED — no parse error, just a
// connection opened with journal_mode=delete, busy_timeout=0, and
// foreign_keys=off. This module pins v1.55.0, which happens to also parse
// those shorthand keys, so this test PASSES against the pre-hardening DSN
// too — that's expected, this is a hardening change, not a fix for
// currently-observed breakage. Its job is to pin the APPLIED pragmas so a
// future driver version change can never silently revert them again.
//
// _txlock=immediate is deliberately not covered here: it configures the
// driver's BEGIN mode, not a SQLite pragma, so there is no PRAGMA to read
// back and confirm it took effect.
func TestSQLiteDSNAppliedPragmas(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.db") // WAL does not apply to :memory:

	db, err := sql.Open("sqlite", SQLiteDSN(path))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cases := []struct {
		pragma string
		want   string
	}{
		{"journal_mode", "wal"},
		{"busy_timeout", "5000"},
		{"foreign_keys", "1"},
		{"synchronous", "1"}, // NORMAL
	}
	for _, c := range cases {
		t.Run(c.pragma, func(t *testing.T) {
			var got string
			require.NoError(t, db.QueryRow("PRAGMA "+c.pragma).Scan(&got))
			require.Equal(t, c.want, got, "PRAGMA %s applied value", c.pragma)
		})
	}
}

// splitDSN splits "path?query" into its path and raw-query halves.
func splitDSN(dsn string) (base, rawQuery string, ok bool) {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '?' {
			return dsn[:i], dsn[i+1:], true
		}
	}
	return dsn, "", false
}
