package monitor

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

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
	applyWorkers   = 8
	applyBatchMax  = 64
	applyQueueSize = 1024

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
// Architecture (three concurrent actors sharing the cursor under cursorMu):
//   - this goroutine runs the reconnect loop and calls oracle.StreamStatus,
//     whose onEvent only ENQUEUES into a bounded (1024) channel;
//   - one applier goroutine drains the channel into batches (≤64);
//   - a fresh bounded worker pool (8) applies each batch in parallel.
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

// applyStatusBatch applies a batch with bounded concurrency, then persists the
// replay cursor once — to the newest event that carries an id — after every
// event has been attempted. A failed event never fails the batch (replaying it
// is safe; the polls are the safety net), so the cursor still advances.
//
// Events are SHARDED BY TXID: every event for one txid runs IN ARRIVAL ORDER on
// a single worker, while distinct txids run in parallel (the throughput win).
// This is load-bearing, not an optimization. ApplyStatusUpdate's terminal /
// lattice guards read the row OUTSIDE the write transaction, and three of its
// writes are unconditional (Promote is not tier-monotonic and will LOWER a tier;
// SetArcadeStatus and MarkSuspect are WHERE-txid-only). If a stale frame (SEEN /
// REJECTED) and a MINED for the SAME txid ran on parallel workers and the stale
// one committed last, it would downgrade the tier, regress arcade_status, or
// clobber a completed tx to suspectFailed — corruption that does not self-heal
// (a completed tx is excluded from the polls) and that M4.2's reject reconciler
// would then act on. This is reachable on cursor-resume-after-outage, where the
// backlog replay co-batches a txid's SEEN and MINED. Serializing per txid makes
// the later-arriving frame's writes land last and win, restoring the guard
// logic's implicit per-txid-serialized-apply assumption. (The queue drains FIFO
// = SSE ns-id arrival order, so intra-shard order is arrival order.)
func (d *Daemon) applyStatusBatch(ctx context.Context, batch []arcade.StatusEvent, cursorMu *sync.Mutex, lastEventID *string) {
	shards := make(map[string][]arcade.StatusEvent)
	order := make([]string, 0, len(batch))
	for _, ev := range batch {
		txid := ev.Record.TxID
		if _, seen := shards[txid]; !seen {
			order = append(order, txid)
		}
		shards[txid] = append(shards[txid], ev)
	}

	g := new(errgroup.Group)
	g.SetLimit(applyWorkers)
	for _, txid := range order {
		events := shards[txid]
		g.Go(func() error {
			for _, ev := range events {
				d.applyStatusEvent(ctx, ev) // in-order within the txid shard
			}
			return nil // a failed event never aborts the batch; the cursor still advances
		})
	}
	_ = g.Wait()

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

// applyStatusEvent applies one event, retrying transient DB contention
// (deadlock / lock) that parallel appliers can form. Other failures are logged
// and swallowed (replay is safe; the polls are the safety net).
func (d *Daemon) applyStatusEvent(ctx context.Context, ev arcade.StatusEvent) {
	var err error
	for range applyDeadlockAttempts {
		err = d.storage.ApplyStatusUpdate(ctx, ev.Record)
		if err == nil || !isTransientDBError(err) {
			break
		}
	}
	if err != nil {
		d.logger.ErrorContext(ctx, "ApplyStatusUpdate failed",
			slog.String("txID", ev.Record.TxID),
			slog.String("eventID", ev.ID),
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
