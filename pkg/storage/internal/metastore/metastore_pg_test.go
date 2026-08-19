//go:build integration

package metastore_test

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/sqltx"
	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
)

// newPostgresMeta opens a migrated metastore on a fresh isolated schema.
func newPostgresMeta(t *testing.T, pg *testenv.PostgresContainer, opts ...metastore.Option) *metastore.Store {
	t.Helper()
	dsn := pg.IsolatedSchemaDSN(t)
	s, err := metastore.OpenPostgres(context.Background(), dsn, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// TestMetastore_Postgres runs the full repository suite against PostgreSQL, one
// isolated schema per subtest. Skips gracefully when no container runtime is
// available.
func TestMetastore_Postgres(t *testing.T) {
	pg := testenv.StartPostgres(t)
	runMetastoreSuite(t, func(t *testing.T, opts ...metastore.Option) *metastore.Store {
		return newPostgresMeta(t, pg, opts...)
	})
}

// TestOutbox_SkipLocked_Postgres proves FOR UPDATE SKIP LOCKED hands two
// concurrently-open transactions DISJOINT batches (no row double-handed-out,
// no blocking): two overlapping transactions each FetchPending(5) over a
// 10-row backlog receive the two halves.
func TestOutbox_SkipLocked_Postgres(t *testing.T) {
	pg := testenv.StartPostgres(t)
	s := newPostgresMeta(t, pg)
	ctx := context.Background()

	const rows = 10
	for i := 0; i < rows; i++ {
		require.NoError(t, s.Outbox().Enqueue(ctx, randBytes(t, 32), "spend", 0, []byte(`{}`)))
	}

	db := s.DB()
	tx1, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx1.Rollback() }()
	tx2, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()

	batch1, err := s.Outbox().FetchPending(sqltx.With(ctx, tx1, db), 5)
	require.NoError(t, err)
	batch2, err := s.Outbox().FetchPending(sqltx.With(ctx, tx2, db), 5)
	require.NoError(t, err)

	require.Len(t, batch1, 5, "first transaction locks its half")
	require.Len(t, batch2, 5, "second transaction SKIPs the locked rows and gets the other half")

	seen := make(map[string]int, rows)
	for _, e := range batch1 {
		seen[hex.EncodeToString(e.TxID)]++
	}
	for _, e := range batch2 {
		seen[hex.EncodeToString(e.TxID)]++
	}
	require.Len(t, seen, rows, "union of both batches is the whole backlog")
	for id, count := range seen {
		require.Equal(t, 1, count, "row %s handed to both transactions (SKIP LOCKED failed)", id)
	}

	require.NoError(t, tx1.Commit())
	require.NoError(t, tx2.Commit())
	t.Logf("SKIP LOCKED disjoint batches confirmed: %d rows split 5/5 across two concurrent transactions", rows)
}
