package sqlstore_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// benchPoolSizes are the equal-value pool sizes exercised by BenchmarkClaim.
// The partial-index lesson predicts per-claim cost is flat across them; a
// large gap between the two would signal the planner has stopped using
// idx_utxos_claim (see claim_explain_test.go for the hard regression guard).
//
// Recorded baseline (i9-13900K, modernc SQLite on tmpfs temp dir, PostgreSQL 17
// in rootless podman with host networking). Per-claim cost is ~flat across a
// 250x pool increase, confirming the claim is a bounded index walk:
//
//	SQLite                       pool=1k     ~53 µs/op    pool=250k   ~79 µs/op
//	PostgreSQL (query+index)     pool=1k     ~84 µs/op    pool=250k   ~87 µs/op
//	PostgreSQL (durable commit)  pool=1k    ~12.9 ms/op   pool=250k  ~13.0 ms/op
//
// The PostgreSQL "query+index" row is measured with synchronous_commit=off so
// the number reflects the claim's own work (round trip + index walk + UPDATE
// RETURNING): 87 µs on 250k matches the go-wallet-toolbox "<0.1 ms" figure and
// contrasts with the ~130 ms/claim that a sequential scan + external sort over
// a 250k pool cost before the partial index existed. The "durable commit" row
// is the same claim with synchronous_commit=on: the extra cost is a fixed WAL
// fsync per commit (here, slow container storage), still independent of pool
// size — flatness holds either way.
var benchPoolSizes = []int{1_000, 250_000}

const benchSats uint64 = 1_000

// txidOf builds a unique 32-byte txid for row i, so a seeded pool has distinct
// primary keys without hashing.
func txidOf(i int) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], uint64(i)) //nolint:gosec // i >= 0
	return b
}

// claimLoop measures b.N smallest-sufficient claims against s. When the pool
// empties it pauses the timer, runs reset to return every coin to claimable,
// and resumes — so the measurement is steady-state per-claim cost at the
// seeded pool size rather than a decreasing pool.
func claimLoop(b *testing.B, s *sqlstore.Store, sc utxostore.Scope, reset func()) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, err := s.ClaimSmallestSufficient(ctx, sc, "bench", 1)
		if err != nil {
			b.Fatal(err)
		}
		if u == nil {
			b.StopTimer()
			reset()
			b.StartTimer()
		}
	}
}

// seedUTXOsSQLite bulk-loads n equal-value claimable rows directly (one
// transaction, one prepared insert), bypassing Mint so setup is fast.
func seedUTXOsSQLite(tb testing.TB, s *sqlstore.Store, sc utxostore.Scope, sats uint64, n int) {
	tb.Helper()
	db := s.DB()
	tx, err := db.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(`INSERT INTO utxos
		(txid, vout, user_id, basket, tier, satoshis, input_size, seq, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	require.NoError(tb, err)
	for i := 0; i < n; i++ {
		_, err := stmt.Exec(txidOf(i), 0, sc.UserID, sc.Basket, int64(sc.Tier), sats, 107, i+1, baseTime.UnixMicro())
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// resetSQL returns the pool-reset statement for the given engine.
func resetSQL(engine sqlstore.Engine, userID int64) (string, []any) {
	if engine == sqlstore.EnginePostgres {
		return "UPDATE utxos SET reserved_by=NULL, reserved_at=NULL WHERE user_id=$1", []any{userID}
	}
	return "UPDATE utxos SET reserved_by=NULL, reserved_at=NULL WHERE user_id=?", []any{userID}
}

func poolReset(tb testing.TB, db *sql.DB, engine sqlstore.Engine, userID int64) func() {
	q, args := resetSQL(engine, userID)
	return func() {
		if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
			tb.Fatalf("pool reset: %v", err)
		}
	}
}

// BenchmarkClaim_SQLite reports per-claim cost at each pool size on SQLite.
func BenchmarkClaim_SQLite(b *testing.B) {
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	for _, n := range benchPoolSizes {
		b.Run(fmt.Sprintf("pool=%d", n), func(b *testing.B) {
			s := newSQLiteStore(b)
			seedUTXOsSQLite(b, s, sc, benchSats, n)
			claimLoop(b, s, sc, poolReset(b, s.DB(), sqlstore.EngineSQLite, sc.UserID))
		})
	}
}
