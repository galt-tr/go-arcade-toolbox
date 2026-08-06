//go:build integration

package sqlstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// BenchmarkClaim_Postgres reports per-claim cost at each pool size on
// PostgreSQL. Compare the two pool sizes: with idx_utxos_claim the cost is flat
// (the claim is a bounded index walk regardless of pool depth).
//
// The testenv container helper requires a *testing.T, which a benchmark cannot
// provide, so this benchmark connects to a PostgreSQL instance named by the
// BENCH_PG_DSN environment variable and skips when it is unset. Point it at a
// throwaway container to gather numbers, e.g.:
//
//	BENCH_PG_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
//	  go test -tags integration -run '^$' -bench BenchmarkClaim_Postgres ./pkg/utxostore/sqlstore/...
func BenchmarkClaim_Postgres(b *testing.B) {
	dsn := os.Getenv("BENCH_PG_DSN")
	if dsn == "" {
		b.Skip("set BENCH_PG_DSN to a postgres DSN to run this benchmark")
	}
	ctx := context.Background()
	s, err := sqlstore.OpenPostgres(ctx, dsn)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close(ctx) })

	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}
	for _, n := range benchPoolSizes {
		b.Run(fmt.Sprintf("pool=%d", n), func(b *testing.B) {
			if _, err := s.DB().ExecContext(ctx, "TRUNCATE utxos"); err != nil {
				b.Fatal(err)
			}
			seedUTXOsPostgres(b, s, sc, benchSats, n)
			if _, err := s.DB().ExecContext(ctx, "ANALYZE utxos"); err != nil {
				b.Fatal(err)
			}
			claimLoop(b, s, sc, poolReset(b, s.DB(), sqlstore.EnginePostgres, sc.UserID))
		})
	}
}
