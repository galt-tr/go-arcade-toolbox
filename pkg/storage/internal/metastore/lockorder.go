package metastore

import (
	"bytes"
	"sort"
)

// This file holds the ONE rule that keeps the concurrent status-apply writers
// deadlock-free: every statement that touches a SET of rows keyed by txid must
// acquire its row locks in the SAME global order.
//
// The bug it fixes: the bulk mutators issued
//
//	UPDATE known_txs SET ... WHERE txid IN (?, ?, …)
//
// with the txids in ARRIVAL order, while the mined applier wrote its per-row
// SetProof updates in arrival order too. PostgreSQL is free to lock the matched
// rows in whatever order its plan produces, so two concurrent apply shards (or
// an apply shard racing the poll sweep) whose txid sets OVERLAP could take the
// same two rows in opposite orders and deadlock — the SQLSTATE 40P01 seen under
// sustained load, absorbed 4× by [sqlkit.WithRetry] before surfacing.
//
// The fix has two halves, both needed:
//   - [lessRawTxID] / [LessTxID] give a single canonical order (ascending raw
//     stored txid bytes — exactly what PostgreSQL's BYTEA collation uses), which
//     the callers apply to any per-row loop; and
//   - [Store.txidLockOrderedIn] forces a set-based statement to take its locks
//     through an ORDER BY'd `FOR UPDATE` sub-select, so the plan can no longer
//     choose its own order.
//
// SQLite needs none of this — a single writer connection serializes every write
// — so it keeps the plain IN list and pays nothing.

// LessTxID reports whether display-hex txid a sorts before b in STORAGE order:
// the ascending raw-byte order of the BYTEA/BLOB txid column, which is the order
// [Store.txidLockOrderedIn] makes every set-based mutator lock in. Callers that
// write rows ONE AT A TIME in a loop (the mined applier's per-row SetProof) must
// sort with this so their lock order matches the set-based statements'.
// Undecodable txids fall back to a stable string comparison.
func LessTxID(a, b string) bool {
	ra, errA := encTxID(a)
	rb, errB := encTxID(b)
	if errA != nil || errB != nil {
		return a < b
	}
	return bytes.Compare(ra, rb) < 0
}

// sortedRawTxIDs decodes txids to their stored raw form, drops duplicates, and
// returns them in ascending storage order — the canonical bind-argument order
// for every IN-list mutator.
func sortedRawTxIDs(txids []string) ([][]byte, error) {
	raws := make([][]byte, 0, len(txids))
	seen := make(map[string]struct{}, len(txids))
	for _, t := range txids {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		raw, err := encTxID(t)
		if err != nil {
			return nil, err
		}
		raws = append(raws, raw)
	}
	sort.Slice(raws, func(i, j int) bool { return bytes.Compare(raws[i], raws[j]) < 0 })
	return raws, nil
}

// txidArgs is sortedRawTxIDs widened to the []any database/sql wants.
func txidArgs(txids []string) ([]any, error) {
	raws, err := sortedRawTxIDs(txids)
	if err != nil {
		return nil, err
	}
	args := make([]any, len(raws))
	for i := range raws {
		args[i] = raws[i]
	}
	return args, nil
}

// txidLockOrderedIn renders the `txid IN (…)` predicate of a set-based mutator
// over table so that concurrent writers acquire their row locks in ASCENDING
// txid order and can therefore never invert on each other (see the file
// comment). orderBy must be a fully deterministic ordering of table's rows,
// i.e. "txid" where txid is unique and "txid, <pk>" where it is not.
//
// PostgreSQL gets the ORDER BY'd `FOR UPDATE` sub-select; SQLite (one writer
// connection, no row-lock ordering to invert) gets the plain IN list. n is the
// number of bind placeholders, which the caller must bind in the same ascending
// order (see [txidArgs]) so the sub-select and the outer statement agree.
func (s *Store) txidLockOrderedIn(table, orderBy string, n int) string {
	in := "txid IN (" + inPlaceholders(n) + ")"
	if s.engine != EnginePostgres || n <= 1 {
		// A single row cannot deadlock against itself, so skip the sub-select.
		return in
	}
	return "txid IN (SELECT txid FROM " + table + " WHERE " + in + " ORDER BY " + orderBy + " FOR UPDATE)"
}
