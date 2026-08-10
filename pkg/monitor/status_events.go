package monitor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/txtrace"
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
	// applyBatchMax is the max events drained into one batch before applying.
	// Larger batches amortize the bulk load/write and give the sharded apply
	// more to parallelize (a mined block delivers thousands of MINED events at
	// once — they must clear in seconds, not back up the reader).
	applyBatchMax = 512
	// applyQueueSize is the reader→applier hand-off buffer. It must be large
	// enough that a burst (a freshly mined block's worth of events) is absorbed
	// without onEvent blocking — a blocked reader stops draining the socket, so
	// arcade back-pressures and SEEN/MINED events are missed entirely.
	applyQueueSize = 16384

	// applyDeadlockAttempts bounds the retry of a transient (deadlock/lock)
	// apply failure; the victim is safe to retry immediately.
	applyDeadlockAttempts = 3
)

// applyShards is the number of parallel apply workers a batch is fanned out
// across, keyed by txid so a given tx's events always land on the same shard
// (preserving the per-txid arrival order the terminal/lattice guards need) while
// distinct txids apply concurrently.
//
// It reads the value [WithApplyConcurrency] configured. This used to be a
// hardcoded const, which silently made the option DEAD: a deployment that raised
// it still applied through 8 workers, the 16384-slot hand-off queue saturated,
// onEvent blocked the SSE reader, and arcade — seeing a slow client — DROPPED our
// events outright (~25k/s of "dropped events for slow SSE client"). Those dropped
// events are the origin of the stranded, never-statused transactions; the repair
// poll in storage is the safety net, this is the fix.
func (d *Daemon) applyShards() int {
	if d.applyConcurrency > 0 {
		return d.applyConcurrency
	}
	return defaultApplyWorkers
}

// handleStatusEvents consumes the arcade status SSE stream and applies each
// event through storage.ApplyStatusUpdate, persisting a replay cursor after
// every batch so the stream resumes without gaps after a restart. It must be
// started as a goroutine (Daemon.Start does, tracked by the wait group) and
// returns only when ctx is canceled.
//
// Architecture (two concurrent actors sharing the cursor under cursorMu):
//   - this goroutine runs the reconnect loop and calls oracle.StreamStatus,
//     whose onEvent only ENQUEUES into a large (applyQueueSize) channel so a
//     burst never blocks the reader (a blocked reader stops draining the socket,
//     so arcade back-pressures and events are missed);
//   - one applier goroutine drains the channel into batches (≤applyBatchMax) and,
//     per batch, fans the records out across applyShards workers keyed by txid —
//     distinct txids apply concurrently through storage.ApplyStatusBatch, while a
//     given txid's events stay on one shard so their arrival order (and every
//     apply guard) is preserved. Sharded parallelism + bulk batching together
//     clear a mined block's thousand-event burst in seconds.
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
// that carries an id — but ONLY when the whole batch applied.
//
// CURSOR HOLD ON FAILURE. This used to advance the cursor unconditionally, on
// the theory that "replaying is safe and the polls are the safety net". They
// were not: the poll could not reach a row that never got a status (it ordered
// by updated_at and a failed apply writes nothing), so a failed batch's events
// were gone for good and its transactions diverged from arcade permanently.
//
// Of the two ways to fix that — hold the cursor at the last FULLY-applied event,
// or persist the failed event ids somewhere for a repair pass — holding the
// cursor is the one that cannot lose events: it needs no new durable store, and
// it reuses the guarantee we already depend on (arcade replays everything after
// Last-Event-ID, and every apply is idempotent). A dead-letter list, by
// contrast, is itself a write that can fail — leaving nothing at all. The cost
// of holding is bounded and benign: a reconnect re-delivers events we may have
// already applied, which is a no-op.
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
	applyStart := time.Now()
	applyErr := d.applyRecords(ctx, records)
	applyDone := time.Now()
	d.traceBatch(batch, applyStart, applyDone)

	if applyErr != nil {
		sample := make([]string, 0, txidSampleSize)
		for i := range records {
			if len(sample) == txidSampleSize {
				break
			}
			sample = append(sample, records[i].TxID)
		}
		d.logger.ErrorContext(ctx, "status batch apply failed; holding SSE replay cursor so no event is lost",
			slog.Int("batchSize", len(records)),
			slog.Any("sampleTxIDs", sample),
			slog.String("error", applyErr.Error()))
		return
	}

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

// traceBatch emits opt-in txtrace lines for an applied batch: one "mined_batch"
// summary per block height present (so a block's MINED burst is measured as a
// whole, not fragmented across the apply shards) and one "applied" line per
// marked (sampled) txid. A batch with no MINED records and no marked txids does
// a single O(batch) scan and emits nothing, so it is safe on the hot path.
func (d *Daemon) traceBatch(batch []arcade.StatusEvent, applyStart, applyDone time.Time) {
	type blockAgg struct {
		count            int
		recvMin, recvMax time.Time
		arcadeTsMin      time.Time
	}
	var mined map[uint64]*blockAgg
	for i := range batch {
		rec := batch[i].Record
		recvAt := batch[i].RecvAt
		if rec.Status == arcade.StatusMined {
			if mined == nil {
				mined = make(map[uint64]*blockAgg)
			}
			a := mined[rec.BlockHeight]
			if a == nil {
				a = &blockAgg{}
				mined[rec.BlockHeight] = a
			}
			a.count++
			if !recvAt.IsZero() {
				if a.recvMin.IsZero() || recvAt.Before(a.recvMin) {
					a.recvMin = recvAt
				}
				if recvAt.After(a.recvMax) {
					a.recvMax = recvAt
				}
			}
			if !rec.Timestamp.IsZero() && (a.arcadeTsMin.IsZero() || rec.Timestamp.Before(a.arcadeTsMin)) {
				a.arcadeTsMin = rec.Timestamp
			}
		}
		if txtrace.Marked(rec.TxID) {
			txtrace.Emit(d.logger, "applied", rec.TxID,
				"status", string(rec.Status),
				"recv_at", tsOrEmpty(recvAt),
				"applied_at", applyDone.Format(time.RFC3339Nano))
		}
	}
	for h, a := range mined {
		txtrace.Emit(d.logger, "mined_batch", "",
			"height", h,
			"count", a.count,
			"recv_min", tsOrEmpty(a.recvMin),
			"recv_max", tsOrEmpty(a.recvMax),
			"arcade_ts_min", tsOrEmpty(a.arcadeTsMin),
			"apply_start", applyStart.Format(time.RFC3339Nano),
			"apply_done", applyDone.Format(time.RFC3339Nano))
	}
}

// tsOrEmpty formats t as RFC3339Nano, or "" when zero, so a trace line never
// carries a misleading "0001-01-01T..." timestamp.
func tsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

// txidSampleSize caps how many txids a diagnostic log line carries: enough to
// go look one up, never enough to blow up the log at 1000 TPS.
const txidSampleSize = 5

// applyRecords applies one batch by fanning it out across [Daemon.applyShards]
// workers, keyed by txid: a given txid's records always go to the same shard (so
// their arrival order — which the terminal/lattice guards depend on — is
// preserved), while distinct txids apply concurrently. Shards hold disjoint txid
// sets, so their ApplyStatusBatch transactions never conflict. This multiplies
// apply throughput so the reader's hand-off buffer drains fast enough that
// onEvent never blocks (a blocked reader = missed SSE events). A single shard,
// or a batch too small to shard, applies inline without spawning goroutines.
//
// It returns the joined error of every shard that could not be applied, so the
// caller can hold the replay cursor rather than skipping past lost events.
func (d *Daemon) applyRecords(ctx context.Context, records []arcade.TxRecord) error {
	if len(records) == 0 {
		return nil
	}
	width := d.applyShards()
	if len(records) <= applyBatchMax/width || width <= 1 {
		return d.applyShard(ctx, records)
	}

	shards := make([][]arcade.TxRecord, width)
	for _, r := range records {
		s := shardOf(r.TxID, width)
		shards[s] = append(shards[s], r)
	}
	errs := make([]error, width)
	var wg sync.WaitGroup
	for i := range shards {
		if len(shards[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, recs []arcade.TxRecord) {
			defer wg.Done()
			errs[idx] = d.applyShard(ctx, recs)
		}(i, shards[i])
	}
	wg.Wait()
	return errors.Join(errs...)
}

// applyShard applies one shard's records (a disjoint txid set) through
// storage.ApplyStatusBatch, retrying transient DB contention — the batch is
// idempotent so retry is safe. A shard that still fails is logged and its error
// returned: the caller holds the replay cursor on it, because the events in a
// failed shard are otherwise gone (arcade never re-sends past Last-Event-ID).
func (d *Daemon) applyShard(ctx context.Context, records []arcade.TxRecord) error {
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
		return fmt.Errorf("apply shard of %d records: %w", len(records), err)
	}
	return nil
}

// shardOf maps a txid to one of width buckets. FNV-1a over the txid string
// spreads txids evenly and is stable, so every event for a txid routes to the
// same shard within a batch.
func shardOf(txid string, width int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(txid))
	//nolint:gosec // width is a small positive worker count.
	return int(h.Sum32() % uint32(width))
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
