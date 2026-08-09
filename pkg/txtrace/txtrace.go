// Package txtrace provides lightweight, opt-in transaction-lifecycle tracing:
// mark a single sampled transaction — by context on the synchronous create/
// broadcast call stack, or by txid for the asynchronous SSE status path — and
// emit correlated structured log lines (msg="txtrace") at each hop (create,
// broadcast, SSE receive, apply). Correlate the lines by txid in
// post-processing to reconstruct the create → SEEN_MULTIPLE_NODES → MINED
// timeline.
//
// It is zero-cost when a transaction is not being traced: Sampled is a single
// context lookup and Marked is a single sync.Map load. Nothing is retained
// beyond the small set of marked txids, so it is safe to leave compiled in.
package txtrace

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type ctxKey struct{}

// WithSample returns a context marked so the in-stack broadcast path emits a
// trace for the transaction created under it. Set it on the CreateAction call
// of the one sampled transaction (its txid is not yet known at that point).
func WithSample(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, true)
}

// Sampled reports whether ctx was marked by WithSample.
func Sampled(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKey{}).(bool)
	return v
}

// marked holds the set of txids whose asynchronous status events should be
// traced. Only a handful are ever added (the sampled transactions), so entries
// are never removed.
var marked sync.Map // txid string -> struct{}

// Mark registers a txid so the asynchronous status path (SSE receive + apply)
// emits traces for it. Call it once the sampled transaction's txid is known,
// i.e. after CreateAction returns.
func Mark(txid string) { marked.Store(txid, struct{}{}) }

// Marked reports whether txid was registered with Mark.
func Marked(txid string) bool {
	_, ok := marked.Load(txid)
	return ok
}

// Emit writes one structured trace line at Info level: msg="txtrace" with a
// wall-clock timestamp (RFC3339Nano), the hop name, the txid, and any extra
// key/value pairs. It is a no-op when logger is nil. Keeping every hop on a
// single msg="txtrace" line lets a run be post-processed with a simple grep.
func Emit(logger *slog.Logger, hop, txid string, kv ...any) {
	if logger == nil {
		return
	}
	args := make([]any, 0, 6+len(kv))
	args = append(args, "hop", hop, "txid", txid, "ts", time.Now().Format(time.RFC3339Nano))
	args = append(args, kv...)
	logger.Info("txtrace", args...)
}
