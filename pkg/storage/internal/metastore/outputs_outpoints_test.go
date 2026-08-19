package metastore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// FindOutputsByOutpoints replaces a keyed query per input with one statement
// per chunk, on two paths that walk every input of a transaction. What it must
// not change is which row each outpoint resolves to, so these run the same
// function against BOTH dialects: the key set is spelled as a row-value IN over
// a VALUES list precisely because it has to satisfy two engines with different
// tolerance for untyped parameters, and only running both proves it does.

// testFindOutputsByOutpoints covers what the callers depend on: the happy
// multi-transaction lookup, an empty request, outpoints with no row, vout
// precision, and the user scoping that keeps one wallet's coins out of
// another's answer.
func testFindOutputsByOutpoints(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	mine := mustUser(ctx, t, s, "outpoints-mine")
	other := mustUser(ctx, t, s, "outpoints-other")

	// Two of this user's transactions, three outputs each, plus one belonging
	// to somebody else at an outpoint we will deliberately ask for.
	txA, txidA := insertTxWithTxID(ctx, t, s, mine, "outpoints-a")
	txB, txidB := insertTxWithTxID(ctx, t, s, mine, "outpoints-b")
	txC, txidC := insertTxWithTxID(ctx, t, s, other, "outpoints-c")

	idA := make([]uint, 3)
	idB := make([]uint, 3)
	for vout := 0; vout < 3; vout++ {
		var err error
		idA[vout], err = s.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: mine, TransactionID: txA, Vout: uint32(vout), Satoshis: int64(100 + vout),
			Basket: strptr("default"), LockingScript: []byte{0xa1, byte(vout)},
		})
		require.NoError(t, err)
		idB[vout], err = s.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: mine, TransactionID: txB, Vout: uint32(vout), Satoshis: int64(200 + vout),
			Basket: strptr("default"), DerivationPrefix: strptr("pfx"), DerivationSuffix: strptr("sfx"),
		})
		require.NoError(t, err)
	}
	_, err := s.Outputs().Insert(ctx, metastore.NewOutput{
		UserID: other, TransactionID: txC, Vout: 0, Satoshis: 999, Basket: strptr("default"),
	})
	require.NoError(t, err)

	t.Run("empty request costs nothing", func(t *testing.T) {
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, nil)
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("resolves across transactions and carries the row's fields", func(t *testing.T) {
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{
			{TxID: txidA, Vout: 0},
			{TxID: txidB, Vout: 2},
			{TxID: txidA, Vout: 1},
		})
		require.NoError(t, err)
		require.Len(t, rows, 3)

		byOp := indexRows(rows)
		a0 := byOp[wdk.OutPoint{TxID: txidA, Vout: 0}]
		require.NotNil(t, a0)
		require.Equal(t, idA[0], a0.OutputID)
		require.EqualValues(t, 100, a0.Satoshis)
		require.EqualValues(t, []byte{0xa1, 0x00}, a0.LockingScript, "the locking script the create path needs")

		b2 := byOp[wdk.OutPoint{TxID: txidB, Vout: 2}]
		require.NotNil(t, b2)
		require.Equal(t, idB[2], b2.OutputID)
		require.Equal(t, strptr("pfx"), b2.DerivationPrefix, "the derivation material the create path needs")

		require.NotNil(t, byOp[wdk.OutPoint{TxID: txidA, Vout: 1}])
	})

	t.Run("rows come back ordered by output_id", func(t *testing.T) {
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{
			{TxID: txidB, Vout: 2},
			{TxID: txidA, Vout: 0},
			{TxID: txidB, Vout: 0},
		})
		require.NoError(t, err)
		require.Len(t, rows, 3)
		for i := 1; i < len(rows); i++ {
			require.Less(t, rows[i-1].OutputID, rows[i].OutputID,
				"the caller maps by outpoint, but a stable order is what makes 'first row wins' meaningful")
		}
	})

	t.Run("outpoints with no row are simply absent", func(t *testing.T) {
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{
			{TxID: txidA, Vout: 0},
			{TxID: txidA, Vout: 99},      // vout past the end
			{TxID: randTxID(t), Vout: 0}, // txid nobody has
		})
		require.NoError(t, err)
		require.Len(t, rows, 1, "a miss is an absence, never an error")
		require.EqualValues(t, 0, rows[0].Vout)
	})

	t.Run("vout is part of the key", func(t *testing.T) {
		// Asking for one vout of a transaction must not drag in its siblings:
		// the row-value comparison pairs txid WITH vout rather than filtering
		// on them independently.
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{{TxID: txidA, Vout: 1}})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 1, rows[0].Vout)
	})

	t.Run("crossed pairs match nothing", func(t *testing.T) {
		// txidA:5 and txidB:5 both miss even though txid A and vout 5 each
		// exist somewhere in the table — the pair is what is looked up.
		_, err := s.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: mine, TransactionID: txB, Vout: 5, Satoshis: 500, Basket: strptr("default"),
		})
		require.NoError(t, err)
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{{TxID: txidA, Vout: 5}})
		require.NoError(t, err)
		require.Empty(t, rows, "vout 5 exists, but not under this txid")
	})

	t.Run("another user's coin is invisible", func(t *testing.T) {
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{
			{TxID: txidC, Vout: 0},
			{TxID: txidA, Vout: 0},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1, "the other user's output must not leak into this user's answer")
		require.Equal(t, idA[0], rows[0].OutputID)
	})

	t.Run("a request wider than one chunk is still one answer", func(t *testing.T) {
		// 450 > outpointChunk (400), so this crosses the chunk boundary. The
		// three real outpoints are placed on either side of it so a bug that
		// dropped or duplicated a chunk shows up as a wrong count.
		ops := make([]wdk.OutPoint, 0, 450)
		for i := 0; i < 450; i++ {
			switch i {
			case 0:
				ops = append(ops, wdk.OutPoint{TxID: txidA, Vout: 0})
			case 399:
				ops = append(ops, wdk.OutPoint{TxID: txidA, Vout: 1})
			case 400:
				ops = append(ops, wdk.OutPoint{TxID: txidB, Vout: 0})
			default:
				ops = append(ops, wdk.OutPoint{TxID: randTxID(t), Vout: uint32(i)})
			}
		}
		rows, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, ops)
		require.NoError(t, err)
		require.Len(t, rows, 3, "every chunk's rows must reach the result exactly once")
	})

	t.Run("a malformed txid is a caller error", func(t *testing.T) {
		_, err := s.Outputs().FindOutputsByOutpoints(ctx, mine, []wdk.OutPoint{{TxID: "not-hex", Vout: 0}})
		require.Error(t, err)
	})
}

// insertTxWithTxID inserts a transaction for userID and stamps it with a fresh
// random txid, returning both.
func insertTxWithTxID(ctx context.Context, t *testing.T, s *metastore.Store, userID int, ref string) (uint, string) {
	t.Helper()
	id, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: userID, Status: wdk.TxStatusCompleted, Reference: ref,
	})
	require.NoError(t, err)
	txid := randTxID(t)
	require.NoError(t, s.Transactions().SetTxID(ctx, id, txid))
	return id, txid
}

// indexRows keys rows by their outpoint, first row winning — the mapping the
// storage caller performs.
func indexRows(rows []metastore.OutputRow) map[wdk.OutPoint]*metastore.OutputRow {
	out := make(map[wdk.OutPoint]*metastore.OutputRow, len(rows))
	for i := range rows {
		if rows[i].TxID == nil {
			continue
		}
		op := wdk.OutPoint{TxID: *rows[i].TxID, Vout: rows[i].Vout}
		if _, dup := out[op]; dup {
			continue
		}
		out[op] = &rows[i]
	}
	return out
}

// TestFindOutputsByOutpoints_SQLite runs the batch lookup against SQLite.
func TestFindOutputsByOutpoints_SQLite(t *testing.T) {
	testFindOutputsByOutpoints(t, newSQLiteMeta)
}
