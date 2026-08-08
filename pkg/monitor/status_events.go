package monitor

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
)

// LastEventIDKey is the key_values key holding the SSE replay cursor across
// restarts (the id of the last event of the last fully-applied batch).
const LastEventIDKey = "arcade_sse_last_event_id"

// sseReconnectBackoff bounds the outer reconnect loop when StreamStatus returns
// without the context being canceled. The arcade client reconnects internally
// and only returns on cancel; this is a safety net so a short-lived return
// cannot hot-loop the CPU.
const sseReconnectBackoff = time.Second

// Status-apply pipeline sizing. Events for distinct txids are independent;
// applying them one-at-a-time (plus one cursor write each) would cap throughput
// far below a busy stream, so a small worker pool with per-batch cursor
// persistence keeps up while preserving replay safety (events are idempotent;
// the cursor advances only to the last event of a fully-applied batch).
const (
	defaultApplyWorkers = 8
	applyBatchMax       = 64
	applyQueueSize      = 1024

	// applyDeadlockAttempts bounds the retry of a transient (deadlock/lock)
	// apply failure; the victim is safe to retry immediately.
	applyDeadlockAttempts = 3
)

// handleStatusEvents consumes the arcade status SSE stream and applies each
// event through storage.ApplyStatusUpdate, persisting a replay cursor after
// every batch so the stream resumes without gaps after a restart. It must be
// started as a goroutine (Daemon.Start does, tracked by the wait group) and
// returns only when ctx is canceled.
//
// Architecture (two concurrent actors sharing the cursor under cursorMu):
//   - this goroutine runs the reconnect loop and calls oracle.StreamStatus,
//     whose onEvent only ENQUEUES into a bounded (1024) channel;
//   - one applier goroutine drains the channel into batches (≤64) and hands each
//     batch to storage.ApplyStatusBatch, which collapses the per-event DB round
//     trips into one bulk load + one batched write transaction (the throughput
//     win) while preserving per-txid arrival order and every apply guard.
//
// The cursor advances only to the newest non-empty event id of a fully-applied
// batch, never regresses to "", and never advances past what was durably
// persisted (so a reconnect resumes from a real position).
func (d *Daemon) handleStatusEvents(ctx context.Context) {
	d.logger.InfoContext(ctx, "starting arcade status event handler")

	id, _, err := d.storage.GetKeyValue(ctx, LastEventIDKey)
	if err != nil {
		d.logger.WarnContext(ctx, "failed to load SSE replay cursor, starting from beginning", slog.String("error", err.Error()))
	}

	// lastEventID is read by this goroutine on reconnect and written by the
	// applier after each batch; cursorMu makes the handoff race-free.
	var cursorMu sync.Mutex
	lastEventID := string(id)
	readCursor := func() string {
		cursorMu.Lock()
		defer cursorMu.Unlock()
		return lastEventID
	}

	events := make(chan arcade.StatusEvent, applyQueueSize)
	onEvent := func(ev arcade.StatusEvent) error {
		select {
		case events <- ev:
		case <-ctx.Done():
		}
		// Always nil: a single slow/bad event must never wedge or stop the stream.
		return nil
	}

	applierDone := make(chan struct{})
	go func() {
		defer close(applierDone)
		for {
			// Block for the first event of a batch, then drain what is ready.
			var batch []arcade.StatusEvent
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				batch = append(batch, ev)
			}
		drain:
			for len(batch) < applyBatchMax {
				select {
				case ev := <-events:
					batch = append(batch, ev)
				default:
					break drain
				}
			}
			d.applyStatusBatch(ctx, batch, &cursorMu, &lastEventID)
		}
	}()

	for ctx.Err() == nil {
		streamErr := d.oracle.StreamStatus(ctx, readCursor(), onEvent)
		if ctx.Err() != nil {
			break
		}
		if streamErr != nil {
			d.logger.ErrorContext(ctx, "arcade status stream terminated unexpectedly, will retry", slog.String("error", streamErr.Error()))
		} else {
			d.logger.WarnContext(ctx, "arcade status stream returned without error, will retry")
		}
		select {
		case <-ctx.Done():
		case <-time.After(sseReconnectBackoff):
		}
	}

	<-applierDone
	d.logger.InfoContext(ctx, "arcade status event handler stopped")
}

// applyStatusBatch hands the whole batch to storage.ApplyStatusBatch (records in
// ARRIVAL ORDER), then persists the replay cursor once — to the newest event
// that carries an id. A failed batch never blocks the cursor (replaying it is
// safe; the polls are the safety net), so the cursor still advances.
//
// The per-txid arrival-order serialization that the terminal/lattice guards rely
// on is now enforced inside ApplyStatusBatch (it collapses a txid's records to
// its final surviving status, honoring supersession), so this method simply
// preserves arrival order in the slice it passes down. (The queue drains FIFO =
// SSE ns-id arrival order.)
func (d *Daemon) applyStatusBatch(ctx context.Context, batch []arcade.StatusEvent, cursorMu *sync.Mutex, lastEventID *string) {
	records := make([]arcade.TxRecord, len(batch))
	for i := range batch {
		records[i] = batch[i].Record
	}
	d.applyRecords(ctx, records)

	// Persist the cursor to the newest event that actually carries an id: an
	// empty id must never overwrite a good cursor with "" (a restart would then
	// resume with no Last-Event-ID and skip the gap — replay is safe, skip is not).
	cursorID := ""
	for i := len(batch) - 1; i >= 0; i-- {
		if batch[i].ID != "" {
			cursorID = batch[i].ID
			break
		}
	}
	if cursorID == "" {
		d.logger.WarnContext(ctx, "status batch carried no event ids, replay cursor not advanced", slog.Int("batchSize", len(batch)))
		return
	}
	if err := d.storage.SetKeyValue(ctx, LastEventIDKey, []byte(cursorID)); err != nil {
		// Keep the old in-memory cursor so a reconnect resumes from the last
		// DURABLY persisted position, not one the DB never reached.
		d.logger.ErrorContext(ctx, "failed to persist SSE replay cursor", slog.String("eventID", cursorID), slog.String("error", err.Error()))
		return
	}
	cursorMu.Lock()
	*lastEventID = cursorID
	cursorMu.Unlock()
}

// applyRecords applies one batch's records through storage.ApplyStatusBatch,
// retrying transient DB contention (deadlock / lock) — the whole batch is safe
// to retry because ApplyStatusBatch is idempotent. Other failures are logged and
// swallowed (replay is safe; the polls are the safety net).
func (d *Daemon) applyRecords(ctx context.Context, records []arcade.TxRecord) {
	var err error
	for range applyDeadlockAttempts {
		err = d.storage.ApplyStatusBatch(ctx, records)
		if err == nil || !isTransientDBError(err) {
			break
		}
	}
	if err != nil {
		d.logger.ErrorContext(ctx, "ApplyStatusBatch failed",
			slog.Int("batchSize", len(records)),
			slog.String("error", err.Error()))
	}
}

// isTransientDBError reports whether err looks like retryable DB contention
// (Postgres deadlock/serialization or SQLite busy/locked).
func isTransientDBError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "deadlock") ||
		strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "busy") ||
		strings.Contains(msg, "serialization")
}
