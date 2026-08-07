package perf

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SchemaVersion is the version of the RunResult JSON schema. Bump on any
// backward-incompatible change to the field layout.
const SchemaVersion = 1

// RunResult is the versioned, self-describing record of one perf run. One file
// is written per run.
type RunResult struct {
	SchemaVersion int         `json:"schemaVersion"`
	GeneratedAt   time.Time   `json:"generatedAt"`
	Label         string      `json:"label,omitempty"`
	Backend       string      `json:"backend"`
	Mode          string      `json:"mode"`
	Environment   Environment `json:"environment"`
	Config        ConfigEcho  `json:"config"`
	Throughput    Throughput  `json:"throughput"`
	// Phases keys: "create", "sign_process", "e2e".
	Phases     map[string]PhaseStats `json:"phases"`
	Contention Contention            `json:"contention"`
	Monitor    MonitorStats          `json:"monitor"`
	PoolDepth  []poolSampleJSON      `json:"poolDepth,omitempty"`
	Notes      []string              `json:"notes,omitempty"`
}

// Environment captures the machine + toolchain the run executed on.
type Environment struct {
	Hostname      string `json:"hostname"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	Kernel        string `json:"kernel"`
	CPUModel      string `json:"cpuModel"`
	LogicalCores  int    `json:"logicalCores"`
	RAMBytes      uint64 `json:"ramBytes"`
	GoVersion     string `json:"goVersion"`
	PodmanVersion string `json:"podmanVersion,omitempty"`
}

// ConfigEcho echoes the run knobs into the result for reproducibility.
type ConfigEcho struct {
	Workers      int     `json:"workers"`
	TargetTPS    float64 `json:"targetTps"`
	DurationSec  float64 `json:"durationSec"`
	WarmupSec    float64 `json:"warmupSec"`
	PoolSize     int     `json:"poolSize"`
	Denomination uint64  `json:"denomination"`
	PaymentSats  uint64  `json:"paymentSats"`
	MaxDBConns   int     `json:"maxDbConns"`
	Network      string  `json:"network"`
}

// Throughput is the headline throughput result.
type Throughput struct {
	SustainedTPS  float64     `json:"sustainedTps"`
	TotalOps      int         `json:"totalOps"`
	WindowSeconds float64     `json:"windowSeconds"`
	TargetTPS     float64     `json:"targetTps"`
	PctOfTarget   float64     `json:"pctOfTarget"`
	BucketWidthS  float64     `json:"bucketWidthSec"`
	Buckets       []BucketTPS `json:"buckets,omitempty"`
}

// BucketTPS is one point of the TPS-over-time series.
type BucketTPS struct {
	StartSec float64 `json:"startSec"`
	TPS      float64 `json:"tps"`
}

// PhaseStats is the latency distribution of one phase, in milliseconds.
type PhaseStats struct {
	Count  int     `json:"count"`
	MinMs  float64 `json:"minMs"`
	P50Ms  float64 `json:"p50Ms"`
	P95Ms  float64 `json:"p95Ms"`
	P99Ms  float64 `json:"p99Ms"`
	MaxMs  float64 `json:"maxMs"`
	MeanMs float64 `json:"meanMs"`
}

// Contention captures claim-contention and error counters.
type Contention struct {
	Attempted         int64 `json:"attempted"`
	Succeeded         int64 `json:"succeeded"`
	ContentionRetries int64 `json:"contentionRetries"`
	DeadlockRetries   int64 `json:"deadlockRetries"`
	ContentionFails   int64 `json:"contentionFails"`
	DeadlockFails     int64 `json:"deadlockFails"`
	OtherErrors       int64 `json:"otherErrors"`
}

// MonitorStats captures the async-loop (SSE maturation) results.
type MonitorStats struct {
	Enabled          bool    `json:"enabled"`
	Mining           bool    `json:"mining"`
	MinedEmitted     int64   `json:"minedEmitted"`
	ActionsTotal     uint32  `json:"actionsTotal"`
	ActionsCompleted uint32  `json:"actionsCompleted"`
	MaturationPct    float64 `json:"maturationPct"`
}

type poolSampleJSON struct {
	AtSec       float64 `json:"atSec"`
	BalanceSats uint64  `json:"balanceSats"`
}

// WriteJSON marshals r to a pretty-printed JSON file at path, creating parent
// directories as needed.
func WriteJSON(r *RunResult, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("perf: mkdir for result: %w", err)
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("perf: marshal result: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil { //nolint:gosec // world-readable perf artifacts are fine
		return fmt.Errorf("perf: write result: %w", err)
	}
	return nil
}

// ReadJSON parses a RunResult JSON file.
func ReadJSON(path string) (*RunResult, error) {
	b, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied
	if err != nil {
		return nil, fmt.Errorf("perf: read result: %w", err)
	}
	var r RunResult
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("perf: unmarshal result: %w", err)
	}
	if r.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("perf: unsupported schema version %d (want %d)", r.SchemaVersion, SchemaVersion)
	}
	return &r, nil
}

// DefaultResultFilename returns a stable, descriptive filename for a run's JSON.
func DefaultResultFilename(backend, mode string, t time.Time) string {
	return fmt.Sprintf("%s-%s-%s.json", t.Format("20060102-150405"), backend, mode)
}
