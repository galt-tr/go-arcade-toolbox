package defs

import (
	"time"

	"github.com/go-softwarelab/common/pkg/must"
)

// Default tuning for the reject→release reconciler (Task 19). These back the
// [ReconcilerConfig] defaults and are exported so the monitor and tests can
// share one source of truth.
const (
	// DefaultRejectReleaseInterval is how often the reconciler pass runs. It is
	// shorter than the grace window so a suspect is looked at on several passes
	// across its quarantine (the two-pass guard needs at least two).
	DefaultRejectReleaseInterval = 60 * time.Second

	// DefaultSuspectGrace is the window a suspect must sit before the reconciler
	// considers it AND the minimum separation between the two rejected-verification
	// passes before inputs are released. Chosen so a transient reject that recovers
	// within a minute or two is never acted on.
	DefaultSuspectGrace = 90 * time.Second

	// DefaultMaxQuarantine is how long a suspect may stay unresolved (an
	// ambiguous status, or a double spend whose competitor never won) before it
	// is escalated to the terminal, operator-visible stuck state — never
	// auto-released.
	DefaultMaxQuarantine = 24 * time.Hour
)

// ReconcilerConfig tunes the reject→release reconciler. Zero fields fall back to
// the package defaults (see [Monitor.ReconcilerOrDefault]).
type ReconcilerConfig struct {
	// SuspectGraceSeconds is the grace window (see [DefaultSuspectGrace]).
	SuspectGraceSeconds uint `mapstructure:"suspect_grace_seconds"`
	// MaxQuarantineSeconds is the escalate-to-stuck ceiling (see
	// [DefaultMaxQuarantine]).
	MaxQuarantineSeconds uint `mapstructure:"max_quarantine_seconds"`
}

// SuspectGrace returns the configured grace, or [DefaultSuspectGrace] when unset.
func (c ReconcilerConfig) SuspectGrace() time.Duration {
	if c.SuspectGraceSeconds == 0 {
		return DefaultSuspectGrace
	}
	return time.Duration(must.ConvertToInt64FromUnsigned(c.SuspectGraceSeconds)) * time.Second
}

// MaxQuarantine returns the configured ceiling, or [DefaultMaxQuarantine] when
// unset.
func (c ReconcilerConfig) MaxQuarantine() time.Duration {
	if c.MaxQuarantineSeconds == 0 {
		return DefaultMaxQuarantine
	}
	return time.Duration(must.ConvertToInt64FromUnsigned(c.MaxQuarantineSeconds)) * time.Second
}

// ReconcilerReport is the outcome of one [VerifyAndReleaseSuspects] pass. Its
// counters are the reconciler's observability surface: Released, FalsePositive
// and Stuck are the reconciler_released_total / reconciler_false_positive_total
// / reconciler_stuck_total metrics the monitor emits per pass — the leak that
// the old design hid is now countable.
type ReconcilerReport struct {
	// Scanned is how many suspect rows the pass examined.
	Scanned int
	// Released is how many suspects were verified provably-dead and had their
	// inputs released (reconciler_released_total).
	Released int
	// FalsePositive is how many suspects recovered on re-verification and were
	// routed back into the normal lifecycle without any release
	// (reconciler_false_positive_total).
	FalsePositive int
	// Stuck is how many suspects were escalated to the terminal stuck state
	// (reconciler_stuck_total).
	Stuck int
	// Ambiguous is how many suspects were left in quarantine for a later pass
	// (transient status / GetTx error / double spend with no winner yet).
	Ambiguous int
	// Cascaded is how many descendant transactions were re-marked suspect
	// because they had spent a released tx's now-phantom change.
	Cascaded int
}

// Add accumulates another report into this one.
func (r *ReconcilerReport) Add(o ReconcilerReport) {
	r.Scanned += o.Scanned
	r.Released += o.Released
	r.FalsePositive += o.FalsePositive
	r.Stuck += o.Stuck
	r.Ambiguous += o.Ambiguous
	r.Cascaded += o.Cascaded
}

// OutboxDrainReport is the outcome of one outbox-drain pass (Mode B crash
// recovery): how many pending utxo-op rows were replayed to completion, how many
// failed this pass, and how many crossed the attempts ceiling and parked.
type OutboxDrainReport struct {
	Drained int
	Failed  int
	Parked  int
	// ParkedTotal is the STANDING backlog of exhausted rows after the pass, not
	// a per-pass delta like the three above. Without it parking is invisible
	// between the one line a row logs as it crosses the ceiling and whenever
	// someone next queries the table by hand: exhausted rows are excluded from
	// the pending set by construction, so every other counter reads zero while
	// the backlog grows.
	ParkedTotal int
}
