// Package mockarcade provides in-process httptest.Server doubles for the two
// external services the toolbox talks to over HTTP/SSE: Arcade (the tx-truth
// oracle) and ChainTracks (the block-header source). They speak the exact wire
// contracts that pkg/arcade.Client and pkg/headers.Client expect, so tests wire
// the REAL clients against these servers rather than in-process fakes — which is
// the point: the e2e write-path and the M4 monitor exercise the true HTTP/SSE
// code paths, and the ChainTracks double serves synthetic-but-internally-
// consistent headers so headers.VerifyMerkleRoot can validate a synthetic BUMP.
package mockarcade

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Broadcast records one POST /tx submission the client made to the mock.
type Broadcast struct {
	// TxID is the txid path/echo the mock assigned to the submission. The
	// arcade client passes the EF body only, so the mock derives the txid it
	// echoes from the configured responder (default: the double-SHA txid is not
	// recomputed here — the mock echoes ArcadeEchoTxID unless overridden).
	TxID string
	// EF is the raw Extended Format transaction bytes posted verbatim
	// (Content-Type: application/octet-stream).
	EF []byte
}

// Arcade is a scriptable mock of the Arcade HTTP/SSE service. It implements the
// four routes pkg/arcade.Client hits: POST /tx, GET /tx/{txid}, GET /health,
// and the SSE GET /events. It is safe for concurrent use.
type Arcade struct {
	tb     testing.TB
	server *httptest.Server

	mu          sync.Mutex
	broadcasts  []Broadcast
	records     map[string]map[string]any // txid -> GET /tx record body
	earlyStatus string                    // txStatus echoed on a 202
	// broadcastResponder, when set, overrides the default 202 for the next (and
	// subsequent) POST /tx: return the HTTP status code and a JSON body. Used to
	// script 4xx rejections and 503 backpressure. Return code 0 to fall through
	// to the default 202 behavior.
	broadcastResponder func(txid string, ef []byte) (code int, body map[string]any, headers map[string]string)
	echoTxID           string

	// SSE fan-out.
	subsMu sync.Mutex
	subs   map[chan sseFrame]struct{}
}

type sseFrame struct {
	id   string
	data string
}

// NewArcade starts a mock Arcade server and registers cleanup on tb.
func NewArcade(tb testing.TB) *Arcade {
	tb.Helper()
	a := &Arcade{
		tb:          tb,
		records:     make(map[string]map[string]any),
		earlyStatus: "SEEN_ON_NETWORK",
		echoTxID:    "",
		subs:        make(map[chan sseFrame]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tx", a.handleBroadcast)
	mux.HandleFunc("GET /tx/{txid}", a.handleGetTx)
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /events", a.handleEvents)
	a.server = httptest.NewServer(mux)
	tb.Cleanup(a.server.Close)
	return a
}

// URL is the base URL to hand to defs.Arcade.URL.
func (a *Arcade) URL() string { return a.server.URL }

// SetEarlyStatus sets the txStatus echoed in the 202 body of POST /tx (the
// "early" broadcast verdict).
func (a *Arcade) SetEarlyStatus(status string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.earlyStatus = status
}

// SetEchoTxID pins the txid the mock echoes back on POST /tx (the arcade wire
// requires a non-empty txid in the 202 body). Tests that assert on the recorded
// broadcast set this to the txid they expect to broadcast.
func (a *Arcade) SetEchoTxID(txid string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.echoTxID = txid
}

// SetTxRecord registers the JSON body returned by GET /tx/{txid}. Absent a
// registration, GET /tx/{txid} returns 404 (ErrTxNotFound on the client side).
func (a *Arcade) SetTxRecord(txid string, record map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records[txid] = record
}

// SetBroadcastResponder installs a scriptable POST /tx responder (for 4xx/503
// cases). Return code 0 to fall through to the default 202.
func (a *Arcade) SetBroadcastResponder(fn func(txid string, ef []byte) (code int, body map[string]any, headers map[string]string)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.broadcastResponder = fn
}

// Broadcasts returns a copy of every POST /tx the mock has received, in order.
func (a *Arcade) Broadcasts() []Broadcast {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Broadcast, len(a.broadcasts))
	copy(out, a.broadcasts)
	return out
}

// BroadcastCount is the number of POST /tx submissions received.
func (a *Arcade) BroadcastCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.broadcasts)
}

func (a *Arcade) handleBroadcast(w http.ResponseWriter, r *http.Request) {
	ef, _ := readAll(r)
	a.mu.Lock()
	echo := a.echoTxID
	early := a.earlyStatus
	responder := a.broadcastResponder
	a.broadcasts = append(a.broadcasts, Broadcast{TxID: echo, EF: ef})
	a.mu.Unlock()

	if responder != nil {
		if code, body, headers := responder(echo, ef); code != 0 {
			for k, v := range headers {
				w.Header().Set(k, v)
			}
			writeJSON(w, code, body)
			return
		}
	}

	txid := echo
	if txid == "" {
		txid = "mockarcade-tx"
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"txid":     txid,
		"status":   202,
		"txStatus": early,
	})
}

func (a *Arcade) handleGetTx(w http.ResponseWriter, r *http.Request) {
	txid := r.PathValue("txid")
	a.mu.Lock()
	rec, ok := a.records[txid]
	early := a.earlyStatus
	a.mu.Unlock()
	if !ok {
		// Default to a live "unproven" record echoing the early status so a poll
		// for a just-broadcast tx does not 404.
		writeJSON(w, http.StatusOK, map[string]any{"txid": txid, "txStatus": early, "status": 200})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (a *Arcade) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"healthy":     true,
		"version":     "mockarcade",
		"blockHeight": 0,
	})
}

// EmitStatus pushes a status SSE event to all connected /events subscribers,
// carrying a nanosecond-resolution event id. The data payload is a TxRecord with
// the given txid and txStatus (plus any extra fields).
func (a *Arcade) EmitStatus(txid, txStatus string, extra map[string]any) {
	rec := map[string]any{"txid": txid, "txStatus": txStatus}
	for k, v := range extra {
		rec[k] = v
	}
	data, _ := json.Marshal(rec)
	frame := sseFrame{id: strconv.FormatInt(time.Now().UnixNano(), 10), data: string(data)}
	a.subsMu.Lock()
	for ch := range a.subs {
		select {
		case ch <- frame:
		default:
		}
	}
	a.subsMu.Unlock()
}

func (a *Arcade) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan sseFrame, 16)
	a.subsMu.Lock()
	a.subs[ch] = struct{}{}
	a.subsMu.Unlock()
	defer func() {
		a.subsMu.Lock()
		delete(a.subs, ch)
		a.subsMu.Unlock()
	}()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			_, _ = w.Write([]byte(": keepalive\n\n"))
			flusher.Flush()
		case f := <-ch:
			var b strings.Builder
			b.WriteString("id: ")
			b.WriteString(f.id)
			b.WriteString("\nevent: status\ndata: ")
			b.WriteString(f.data)
			b.WriteString("\n\n")
			_, _ = w.Write([]byte(b.String()))
			flusher.Flush()
		}
	}
}
