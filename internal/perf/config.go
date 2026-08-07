// Package perf is the performance harness for the go-arcade-toolbox write
// path. It drives N concurrent workers through the full wallet write path
// (CreateAction -> SignAction -> ProcessAction/broadcast) against a real
// storage.Provider wired to the in-process mockarcade doubles, so throughput
// measures storage + wallet cost rather than network latency. A background
// monitor daemon consumes arcade's status SSE stream and an optional auto-miner
// emits MINED frames so change matures through the real async pipeline under
// load.
//
// The harness is backend-agnostic: it builds the provider through
// pkg/storage/perfprovider, so the same code path serves the perf test suite
// (containers via internal/testenv) and cmd/perfrunner (connection flags).
//
// # Recycling model (and its documented limitation)
//
// The task's ideal is the throughput-strategy fuel pool: FanOutFuel mints
// exact-value denominations that a payment claims via the funder's closed-form
// ClaimExact fast path. In this codebase a plain wallet.CreateAction always
// funds from the change basket ("default") with Denomination=0 — the bounded
// tiered (privacy) claim — regardless of strategy; the exact-claim fast path is
// a separate funding route not reached by ordinary payments. The harness
// therefore uses the robust, proven model: pre-mint a pool of spendable coins
// into the change basket, and let each payment's change recycle implicitly
// through the provider's UTXO selection (mined coins first, then unproven).
// This still exercises real claim contention under load AND the async
// maturation loop (monitor + miner promote change unproven -> mined), but it is
// NOT the throughput exact-claim path. See the run report's notes.
package perf

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
)

// TargetTPS is the headline throughput target the report compares against.
const TargetTPS = 1000.0

// Mode selects how each worker op drives the write path.
type Mode string

const (
	// ModeSignAndProcess issues one wallet.CreateAction with SignAndProcess=true:
	// create + sign + process + broadcast in a single round trip (fewest round
	// trips, highest TPS). Only the end-to-end phase is measured.
	ModeSignAndProcess Mode = "signandprocess"
	// ModeTwoStep issues CreateAction(SignAndProcess=false) then SignAction,
	// yielding two independently-timed phases (create = fund+reserve+persist;
	// sign_process = sign+broadcast+commit) plus their end-to-end sum.
	ModeTwoStep Mode = "twostep"
)

// Config is the full run configuration.
type Config struct {
	// Backend + connection parameters (see perfprovider.Config).
	Backend       perfprovider.Backend
	SQLitePath    string
	PostgresDSN   string
	AeroHost      string
	AeroPort      int
	AeroNamespace string
	AeroSet       string
	MaxDBConns    int

	// Workers is the number of concurrent worker goroutines.
	Workers int
	// TargetTPS gates the global op rate; 0 means ungated (max throughput).
	TargetTPS float64
	// Duration is the measured window AFTER Warmup. Warmup ops are excluded
	// from sustained TPS and percentile math.
	Duration time.Duration
	Warmup   time.Duration

	// PoolSize is how many spendable coins to pre-mint into the change basket.
	PoolSize int
	// Denomination is the satoshi value of each pre-minted coin.
	Denomination uint64
	// PaymentSats is the value of the single payment output each op creates.
	PaymentSats uint64

	// Mode selects single-call vs two-step timing.
	Mode Mode
	// RunMonitor starts the monitor daemon (SSE apply pipeline).
	RunMonitor bool
	// Mine runs the auto-miner that emits MINED frames for broadcast txids so
	// change matures through the async pipeline. Requires RunMonitor.
	Mine bool
	// MaxOpRetries caps per-op retries on contention/deadlock before the op is
	// counted as failed.
	MaxOpRetries int

	// RealArcadeURL, when set, redirects the arcade oracle (broadcast + status
	// SSE) at a live Arcade service instead of the in-process mockarcade, so
	// broadcasts measure true end-to-end cost including the network. The
	// ChainTracks header source stays the in-process mock (it validates the
	// harness's synthetic seed proofs); the auto-miner is disabled because the
	// harness no longer controls the SSE stream.
	RealArcadeURL string

	Network defs.BSVNetwork
	Label   string
}

// DefaultConfig returns sane defaults for a given backend.
func DefaultConfig(backend perfprovider.Backend) Config {
	return Config{
		Backend:      backend,
		Workers:      32,
		TargetTPS:    0,
		Duration:     5 * time.Minute,
		Warmup:       30 * time.Second,
		PoolSize:     2000,
		Denomination: 1_000_000,
		PaymentSats:  1000,
		Mode:         ModeTwoStep,
		RunMonitor:   true,
		Mine:         true,
		MaxOpRetries: 12,
		Network:      defs.NetworkTestnet,
	}
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	switch c.Backend {
	case perfprovider.BackendSQLite, perfprovider.BackendPostgres, perfprovider.BackendAerospikeHybrid:
	default:
		return fmt.Errorf("perf: invalid backend %q", c.Backend)
	}
	if c.Workers <= 0 {
		return fmt.Errorf("perf: workers must be > 0")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("perf: duration must be > 0")
	}
	if c.Warmup < 0 {
		return fmt.Errorf("perf: warmup must be >= 0")
	}
	if c.PoolSize <= 0 {
		return fmt.Errorf("perf: pool size must be > 0")
	}
	if c.Denomination == 0 {
		return fmt.Errorf("perf: denomination must be > 0")
	}
	// The payment plus a generous per-tx fee headroom must fit inside one coin
	// so a single claimed coin can normally fund a whole payment.
	if c.PaymentSats+50_000 >= c.Denomination {
		return fmt.Errorf("perf: denomination %d too small for payment %d (leave fee headroom)", c.Denomination, c.PaymentSats)
	}
	switch c.Mode {
	case ModeSignAndProcess, ModeTwoStep:
	default:
		return fmt.Errorf("perf: invalid mode %q", c.Mode)
	}
	if c.Mine && !c.RunMonitor {
		return fmt.Errorf("perf: mine requires run-monitor")
	}
	return nil
}

// perfproviderConfig maps the run config onto a perfprovider.Config.
func (c *Config) perfproviderConfig() perfprovider.Config {
	return perfprovider.Config{
		Backend:       c.Backend,
		SQLitePath:    c.SQLitePath,
		PostgresDSN:   c.PostgresDSN,
		AeroHost:      c.AeroHost,
		AeroPort:      c.AeroPort,
		AeroNamespace: c.AeroNamespace,
		AeroSet:       c.AeroSet,
		MaxDBConns:    c.MaxDBConns,
		Network:       c.Network,
		StorageName:   "perf",
	}
}
