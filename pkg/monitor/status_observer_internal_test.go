package monitor

// White-box tests for [WithStatusObserver] — the hook that lets an application
// react to arcade status without opening a second SSE stream on the same
// callback token (which arcade serves as a full duplicate, at double its own
// per-event fan-out cost; see arcade.Client.StreamStatus).
//
// The contract under test is narrow and worth pinning: an observer sees a batch
// only after it applied, it sees the records as they arrived rather than a view
// filtered by what the wallet already knew, and a batch that failed to apply is
// NOT reported — the cursor holds those events for redelivery instead, so
// reporting them here would hand the application a state the wallet does not
// share and then hand it to them again on the replay.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
)

// observedBatches collects what an observer was handed. It deliberately copies
// each batch: the contract says the slice must not be retained, so a test that
// kept the caller's slice would be asserting against memory it was told not to
// hold.
type observedBatches struct {
	batches [][]arcade.TxRecord
}

func (o *observedBatches) observe(recs []arcade.TxRecord) {
	cp := make([]arcade.TxRecord, len(recs))
	copy(cp, recs)
	o.batches = append(o.batches, cp)
}

// TestStatusObserver_ReceivesAppliedBatchInArrivalOrder is the core of the
// feature: one connection feeds both the wallet apply and the application.
func TestStatusObserver_ReceivesAppliedBatchInArrivalOrder(t *testing.T) {
	const (
		a = "aaaa"
		b = "bbbb"
	)
	store := newModelStorage([]string{a, b})
	var seen observedBatches

	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(),
		WithoutDistributedLock(), WithStatusObserver(seen.observe))
	require.NoError(t, err)

	batch := []arcade.StatusEvent{
		rec(a, arcade.StatusSeenOnNetwork),
		rec(b, arcade.StatusSeenOnNetwork),
		rec(a, arcade.StatusMined),
	}
	dmn.applyStatusBatch(context.Background(), batch, &cursorTracker{daemon: dmn})

	require.Len(t, seen.batches, 1, "one applied batch is one observer call — not one per shard")
	got := seen.batches[0]
	require.Len(t, got, len(batch), "the whole batch is reported, not a per-txid collapse")

	// Arrival order is preserved, which is what lets an observer apply the same
	// supersession reasoning the storage layer does.
	wantTxIDs := []string{a, b, a}
	wantStatus := []arcade.Status{arcade.StatusSeenOnNetwork, arcade.StatusSeenOnNetwork, arcade.StatusMined}
	for i := range got {
		require.Equalf(t, wantTxIDs[i], got[i].TxID, "record %d txid", i)
		require.Equalf(t, wantStatus[i], got[i].Status, "record %d status", i)
	}
}

// TestStatusObserver_ReportsTxidsTheWalletDoesNotKnow pins the "as they arrived,
// not filtered by what the wallet knew" half of the contract. An application
// tracking its own transactions — a UI over a chain of covenant spends, say —
// must see every record, including any the wallet has no row for; filtering to
// known_txs would silently drop exactly the events such an application exists
// to display.
func TestStatusObserver_ReportsTxidsTheWalletDoesNotKnow(t *testing.T) {
	const known = "aaaa"
	const foreign = "ffff" // never seeded, so the wallet has no state for it

	store := newModelStorage([]string{known})
	var seen observedBatches

	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(),
		WithoutDistributedLock(), WithStatusObserver(seen.observe))
	require.NoError(t, err)

	dmn.applyStatusBatch(context.Background(), []arcade.StatusEvent{
		rec(known, arcade.StatusMined),
		rec(foreign, arcade.StatusMined),
	}, &cursorTracker{daemon: dmn})

	require.Len(t, seen.batches, 1)
	require.Len(t, seen.batches[0], 2, "an unknown txid is still reported to the observer")
	require.Equal(t, foreign, seen.batches[0][1].TxID)
}

// TestStatusObserver_SilentWhenTheBatchFailedToApply guards the interaction with
// the cursor hold. A failed batch is not a fact yet: its events stay un-applied
// and a reconnect re-delivers them, so telling the observer now would report a
// state the wallet does not share — and report it twice.
func TestStatusObserver_SilentWhenTheBatchFailedToApply(t *testing.T) {
	const a = "aaaa"
	store := newModelStorage([]string{a})
	store.batchErr = errors.New("database is down mid-batch")

	var seen observedBatches
	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(),
		WithoutDistributedLock(), WithStatusObserver(seen.observe))
	require.NoError(t, err)

	batch := []arcade.StatusEvent{rec(a, arcade.StatusMined)}
	cursor := &cursorTracker{daemon: dmn}
	dmn.applyStatusBatch(context.Background(), batch, cursor)

	require.Empty(t, seen.batches, "a batch that did not apply must not be observed")
	// And the events are still queued for redelivery rather than skipped.
	require.Empty(t, cursor.resume(context.Background()), "the cursor is held behind the failure")
}

// TestStatusObserver_UnsetIsANoOp proves the option is additive: the pipeline
// behaves exactly as before when no observer is registered.
func TestStatusObserver_UnsetIsANoOp(t *testing.T) {
	const a = "aaaa"
	store := newModelStorage([]string{a})

	dmn, err := NewDaemon(logging.NewTestLogger(t), store, nil, nil, defs.DefaultMonitorConfig(),
		WithoutDistributedLock())
	require.NoError(t, err)
	require.Nil(t, dmn.statusObserver)

	cursor := &cursorTracker{daemon: dmn}
	dmn.applyStatusBatch(context.Background(), []arcade.StatusEvent{rec(a, arcade.StatusMined)}, cursor)

	require.Equal(t, "completed", store.stateOf(a).status, "the apply is unaffected")
	require.Equal(t, a+"-"+string(arcade.StatusMined), cursor.resume(context.Background()))
}
