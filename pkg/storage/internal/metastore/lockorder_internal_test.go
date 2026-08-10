package metastore

// White-box tests for the deadlock fix: the ORDER the bulk mutators bind and
// lock their txids in. The ordering itself is only observable in the generated
// SQL and in the bind-argument order, so it is asserted directly here — the
// functional "concurrent overlapping bulk updates all succeed" half lives in
// the repository suite (KnownTx_BulkMutatorsOverlappingTxIDs).

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxIDLockOrderedIn_PostgresLocksInTxIDOrder(t *testing.T) {
	pg := &Store{engine: EnginePostgres}

	// PostgreSQL is free to lock the rows matched by a plain IN list in whatever
	// order its plan produces, so two writers with overlapping sets can invert on
	// each other. The ORDER BY'd FOR UPDATE sub-select removes that freedom.
	require.Equal(t,
		"txid IN (SELECT txid FROM known_txs WHERE txid IN (?, ?, ?) ORDER BY txid FOR UPDATE)",
		pg.txidLockOrderedIn("known_txs", "txid", 3))

	// transactions.txid is not unique, so the ordering falls back to the primary
	// key to stay total (two rows sharing a txid must still have one lock order).
	require.Equal(t,
		"txid IN (SELECT txid FROM transactions WHERE txid IN (?, ?) ORDER BY txid, transaction_id FOR UPDATE)",
		pg.txidLockOrderedIn("transactions", "txid, transaction_id", 2))

	// A single row cannot deadlock against itself: no sub-select, no cost.
	require.Equal(t, "txid IN (?)", pg.txidLockOrderedIn("known_txs", "txid", 1))
}

func TestTxIDLockOrderedIn_SQLiteKeepsPlainInList(t *testing.T) {
	// SQLite serializes every write onto one connection, so there is no row-lock
	// order to invert and no reason to pay for the sub-select.
	lite := &Store{engine: EngineSQLite}
	require.Equal(t, "txid IN (?, ?, ?)", lite.txidLockOrderedIn("known_txs", "txid", 3))
}

func TestTxIDArgs_SortedAscendingAndDeduplicated(t *testing.T) {
	// Arrival order (what the SSE batch hands us) is deliberately NOT storage
	// order; the mutators must re-order before binding so every writer agrees.
	in := []string{"ff00", "0011", "ff00", "8000"}
	args, err := txidArgs(in)
	require.NoError(t, err)
	require.Len(t, args, 3, "the duplicate is dropped")
	require.Equal(t, []byte{0x00, 0x11}, args[0])
	require.Equal(t, []byte{0x80, 0x00}, args[1])
	require.Equal(t, []byte{0xff, 0x00}, args[2])

	_, err = txidArgs([]string{"not-hex"})
	require.Error(t, err)
}

func TestLessTxID_MatchesStorageByteOrder(t *testing.T) {
	// The per-row writers (the mined applier's SetProof loop) sort with this, so
	// it must agree with the BYTEA ordering the set-based statements lock in.
	require.True(t, LessTxID("0011", "8000"))
	require.False(t, LessTxID("8000", "0011"))
	require.False(t, LessTxID("8000", "8000"))
	// Undecodable input still yields a stable (total) order rather than panicking.
	require.True(t, LessTxID("aaa", "bbb"))
}
