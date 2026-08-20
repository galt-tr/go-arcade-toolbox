//go:build integration

package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// This file is the PostgreSQL half of the batch work: the semantics tests from
// spend_batch_test.go re-run on the set-based arm, the statement-COUNT guards
// that pin it as set-based, and a wide round trip that proves the array binding
// the whole design rests on actually reaches the server.

// TestBatchSemantics_Postgres runs the engine-independent batch-shape tests
// against the set-based arm. They pass on SQLite's loop by construction; the
// claim worth testing is that PostgreSQL's ONE-statement rewrite reaches the
// identical verdicts, item for item.
func TestBatchSemantics_Postgres(t *testing.T) {
	pg := testenv.StartPostgres(t)
	runBatchSemanticsSuite(t, func(t *testing.T) utxostore.Store {
		return newPostgresStore(t, pg)
	})
}

// newRecordingPostgresStore builds a store over the pgx driver wrapped in the
// statement recorder from guarded_stmt_test.go, on a fresh isolated schema.
// Returned disarmed, so migrations run untouched.
func newRecordingPostgresStore(t *testing.T, pg *testenv.PostgresContainer) (utxostore.Store, *sqlRecorder) {
	t.Helper()

	rec := &sqlRecorder{}
	name := fmt.Sprintf("recpgx-%d", driverSeq.Add(1))
	sql.Register(name, &recDriver{inner: stdlib.GetDefaultDriver(), rec: rec})

	db, err := sql.Open(name, pg.IsolatedSchemaDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlstore.New(context.Background(), db, sqlstore.EnginePostgres)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store, rec
}

// requireSoleStatement asserts the recording holds exactly one utxos statement
// and returns its text.
//
// Unlike requireSoleWrite it does not require the driver to have seen an Exec.
// A set-based UPDATE ... RETURNING is issued through QueryContext, because the
// batch needs to know WHICH keys the write matched rather than merely how many
// did, and database/sql hands that to the driver as a query. The distinction is
// about the call shape, not about whether the statement writes.
func requireSoleStatement(t *testing.T, stmts []recorded) string {
	t.Helper()
	require.Len(t, stmts, 1, "the batch must be ONE statement; got %s", render(stmts))
	return stmts[0].sql
}

// TestSetBasedMutationsAreOneStatement is the perf regression guard for the
// batch rewrite. Every assertion here is a statement COUNT, because that is the
// property the rewrite bought and the one no behavioral test can see: the
// taxonomy, the counts and the row states are identical whether the store sends
// one statement or N, which is exactly why a loop could be reintroduced without
// a single suite failure.
//
// N is deliberately larger than any group these calls form, so a per-op
// implementation cannot pass by coincidence.
func TestSetBasedMutationsAreOneStatement(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	// claimAll seeds n coins and holds them all under one reservation, with the
	// recorder disarmed.
	claimAll := func(t *testing.T, store utxostore.Store, seed string, n int, res string) []utxostore.Outpoint {
		t.Helper()
		sats := make([]uint64, n)
		for i := range sats {
			sats[i] = uint64(100 * (i + 1))
		}
		utxostoretest.MintTx(t, store, seed, 1, "default", utxostore.TierMined, sats...)
		claimed, err := store.ClaimLargestInsufficient(ctx, sc, res, uint64(100*(n+1)), n)
		require.NoError(t, err)
		require.Len(t, claimed, n)
		ops := make([]utxostore.Outpoint, len(claimed))
		for i, u := range claimed {
			ops[i] = u.Outpoint
		}
		return ops
	}

	t.Run("a happy-path spend of N ops is ONE statement", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-spend", 8, "res-set")

		spends := make([]*utxostore.SpendOp, len(ops))
		for i, op := range ops {
			spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-set", SpendingTxID: utxostoretest.NewTxID("set-tx")}
		}

		rec.start()
		require.NoError(t, store.Spend(ctx, spends, false))
		stmts := rec.stop()

		q := requireSoleStatement(t, stmts)
		require.Contains(t, q, "UPDATE utxos u")
		require.Contains(t, q, "unnest($1::bytea[], $2::bigint[]) AS k(txid, vout)",
			"the whole batch rides in TWO array parameters, paired positionally by unnest itself")
		// Every guard conjunct the per-op write carried is still in the WHERE.
		require.Contains(t, q, "u.spent_by IS NULL", "the unrecorded-spend guard rides in the WHERE")
		require.Contains(t, q, "u.reserved_by=$4", "the reservation guard rides in the WHERE")
		require.Contains(t, q, "NOT u.frozen", "the freeze guard rides in the WHERE")
		require.Contains(t, q, "pinned=FALSE", "a recorded spend consumes the coin and its pin")
	})

	t.Run("a fact-mode spend of N ops is ONE statement without the dropped guards", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-fact", 6, "res-other")

		spends := make([]*utxostore.SpendOp, len(ops))
		for i, op := range ops {
			// A foreign token AND rows the network already took: fact mode must
			// record them all, so neither dropped conjunct may appear.
			spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-mine", SpendingTxID: utxostoretest.NewTxID("set-fact-tx")}
		}

		rec.start()
		require.NoError(t, store.Spend(ctx, spends, true))
		stmts := rec.stop()

		q := requireSoleStatement(t, stmts)
		require.Contains(t, q, "u.spent_by IS NULL", "two spend facts cannot both hold, in either mode")
		require.NotContains(t, q, "reserved_by", "a stale reservation cannot un-happen an accepted spend")
		require.NotContains(t, q, "frozen", "a freeze cannot un-happen an accepted spend")
	})

	t.Run("misses cost ONE classifying read for the whole batch", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-miss", 6, "res-miss")

		// Three of the six are already spent by a rival, so they all miss the
		// guarded write together. The per-op loop would have paid a read each.
		rival := utxostoretest.NewTxID("set-miss-rival")
		pre := make([]*utxostore.SpendOp, 3)
		for i := range pre {
			pre[i] = &utxostore.SpendOp{Outpoint: ops[i], Reservation: "res-miss", SpendingTxID: rival}
		}
		require.NoError(t, store.Spend(ctx, pre, false))

		spends := make([]*utxostore.SpendOp, len(ops))
		for i, op := range ops {
			spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-miss", SpendingTxID: utxostoretest.NewTxID("set-miss-tx")}
		}

		rec.start()
		require.ErrorIs(t, store.Spend(ctx, spends, false), utxostore.ErrBatch)
		stmts := rec.stop()

		require.Len(t, stmts, 2, "batch = one guarded write + one classifying read; got %s", render(stmts))
		require.Contains(t, stmts[0].sql, "UPDATE utxos u", "the write comes FIRST: %s", render(stmts))
		require.Contains(t, stmts[1].sql, "SELECT u.txid, u.vout, u.spent_by",
			"and the misses are classified in one read: %s", render(stmts))

		// Three refused, three recorded — the batch commits what it can.
		for i := 0; i < 3; i++ {
			require.ErrorIs(t, spends[i].Err, &utxostore.SpentError{})
		}
		for i := 3; i < len(spends); i++ {
			require.NoError(t, spends[i].Err)
		}
	})

	t.Run("two spenders are two statements, not two per op", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-groups", 8, "res-groups")

		first := utxostoretest.NewTxID("set-groups-first")
		second := utxostoretest.NewTxID("set-groups-second")
		spends := make([]*utxostore.SpendOp, len(ops))
		for i, op := range ops {
			txid := first
			if i%2 == 1 {
				txid = second
			}
			spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-groups", SpendingTxID: txid}
		}

		rec.start()
		require.NoError(t, store.Spend(ctx, spends, false))
		stmts := rec.stop()

		require.Len(t, stmts, 2, "one statement per spender, not per op; got %s", render(stmts))
		for _, s := range stmts {
			require.Contains(t, s.sql, "UPDATE utxos u", "both statements are the guarded write: %s", render(stmts))
		}
	})

	t.Run("release, unspend, promote and freeze are ONE statement each", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-colder", 7, "res-colder")

		rec.start()
		require.NoError(t, store.ReleaseOutpoints(ctx, "res-colder", ops))
		q := requireSoleStatement(t, rec.stop())
		require.Contains(t, q, "unnest($1::bytea[], $2::bigint[]) AS k(txid, vout)")
		require.Contains(t, q, "u.reserved_by=$3")
		require.Contains(t, q, "u.spent_by IS NULL")

		rec.start()
		changed, err := store.Promote(ctx, ops, utxostore.TierUnproven)
		require.NoError(t, err)
		require.Equal(t, len(ops), changed, "every row moved tier")
		q = requireSoleStatement(t, rec.stop())
		require.Contains(t, q, "u.tier<>$3", "the count stays 'rows that actually changed'")

		rec.start()
		require.NoError(t, store.Freeze(ctx, ops))
		q = requireSoleStatement(t, rec.stop())
		require.Contains(t, q, "RETURNING u.txid, u.vout", "the misses are the per-item NotFoundErrors")
		require.NoError(t, store.Unfreeze(ctx, ops))

		// Unspend needs recorded spends to reverse.
		spender := utxostoretest.NewTxID("set-colder-tx")
		spends := make([]*utxostore.SpendOp, len(ops))
		for i, op := range ops {
			spends[i] = &utxostore.SpendOp{Outpoint: op, Reservation: "res-colder", SpendingTxID: spender}
		}
		require.NoError(t, store.Spend(ctx, spends, true))

		rec.start()
		released, err := store.Unspend(ctx, spender, ops)
		require.NoError(t, err)
		require.Equal(t, len(ops), released)
		q = requireSoleStatement(t, rec.stop())
		require.Contains(t, q, "u.spent_by=$3")
	})

	t.Run("freeze reports its misses in op order", func(t *testing.T) {
		store, rec := newRecordingPostgresStore(t, pg)
		ops := claimAll(t, store, "set-freeze-miss", 3, "res-freeze")
		absent := []utxostore.Outpoint{
			utxostoretest.NewOutpoint("set-freeze-gone", 0),
			utxostoretest.NewOutpoint("set-freeze-gone", 1),
		}
		mixed := []utxostore.Outpoint{ops[0], absent[0], ops[1], absent[1], ops[2]}

		rec.start()
		err := store.Freeze(ctx, mixed)
		stmts := rec.stop()
		require.ErrorIs(t, err, utxostore.ErrBatch)
		require.Len(t, stmts, 1, "even a partly-missing freeze is ONE statement; got %s", render(stmts))

		// Both absent outpoints are reported, and the present rows are frozen.
		for _, op := range absent {
			require.Contains(t, err.Error(), op.String())
		}
		for _, op := range ops {
			u, gerr := store.Get(ctx, op)
			require.NoError(t, gerr)
			require.True(t, u.Frozen, "%s must still be frozen despite the misses", op)
		}
	})
}

// TestArrayBindingWideBatch is the direct proof of the binding the whole
// set-based design rests on: pgx must encode a [][]byte as bytea[] and a
// []int64 as bigint[] through database/sql, for a batch far wider than any
// unit test builds. It is worth its own test because the failure mode is not
// subtle-but-rare — an unbound or mistyped array fails EVERY call — and until
// this work landed no statement in the package bound an array at all.
//
// 100 ops in two parameters is also the point of the shape: the tuple-list
// alternative would have bound 200, and a batch ten times this size would have
// approached PostgreSQL's 65535-parameter ceiling.
func TestArrayBindingWideBatch(t *testing.T) {
	pg := testenv.StartPostgres(t)
	store := newPostgresStore(t, pg)
	ctx := context.Background()
	sc := utxostore.Scope{UserID: 1, Basket: "default", Tier: utxostore.TierMined}

	const n = 100
	sats := make([]uint64, n)
	for i := range sats {
		sats[i] = uint64(1_000 + i)
	}
	utxostoretest.MintTx(t, store, "wide-batch", 1, "default", utxostore.TierMined, sats...)

	claimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-wide", 1_000_000, n)
	require.NoError(t, err)
	require.Len(t, claimed, n)
	ops := make([]utxostore.Outpoint, n)
	for i, u := range claimed {
		ops[i] = u.Outpoint
	}

	// Release all 100 in one statement, then re-claim them: proof the write
	// matched every key, not just the first.
	require.NoError(t, store.ReleaseOutpoints(ctx, "res-wide", ops))
	reclaimed, err := store.ClaimLargestInsufficient(ctx, sc, "res-wide-2", 1_000_000, n)
	require.NoError(t, err)
	require.Len(t, reclaimed, n, "all 100 rows must have been released by the one statement")

	// Spend all 100 in one statement.
	spender := utxostoretest.NewTxID("wide-batch-tx")
	spends := make([]*utxostore.SpendOp, n)
	for i, u := range reclaimed {
		spends[i] = &utxostore.SpendOp{Outpoint: u.Outpoint, Reservation: "res-wide-2", SpendingTxID: spender}
	}
	require.NoError(t, store.Spend(ctx, spends, false))
	for i, sp := range spends {
		require.NoError(t, sp.Err, "item %d", i)
	}

	removed, err := store.RemoveSpentBy(ctx, spender)
	require.NoError(t, err)
	require.Equal(t, n, removed, "every one of the 100 rows carries the spend")
}

// TestSetBasedStatementsRejectUntypedArrays documents WHY the ::bytea[] and
// ::bigint[] casts in pgKeyRel are load-bearing rather than stylistic: without
// them PostgreSQL types the parameter as text and the join against a bytea
// column resolves to no operator at all, so the statement fails to parse. A
// future edit that "tidies" the casts away would break every set-based
// mutation, and this pins the reason in the one place it is provable.
func TestSetBasedStatementsRejectUntypedArrays(t *testing.T) {
	pg := testenv.StartPostgres(t)
	store := newPostgresStore(t, pg)
	ctx := context.Background()

	_, err := store.DB().ExecContext(ctx,
		`UPDATE utxos u SET frozen=TRUE FROM unnest($1, $2) AS k(txid, vout)
		 WHERE u.txid = k.txid AND u.vout = k.vout::integer`,
		[][]byte{{0x01}}, []int64{0})
	require.Error(t, err, "an uncast array parameter must not silently work")
	t.Logf("uncast: %v", err)

	// The same statement with the casts pgKeyRel carries parses and runs (it
	// matches nothing, which is the point — the failure above was structural,
	// not about the data).
	_, err = store.DB().ExecContext(ctx,
		`UPDATE utxos u SET frozen=TRUE FROM unnest($1::bytea[], $2::bigint[]) AS k(txid, vout)
		 WHERE u.txid = k.txid AND u.vout = k.vout::integer`,
		[][]byte{{0x01}}, []int64{0})
	require.NoError(t, err, "the cast form is what makes the join resolvable")
}
