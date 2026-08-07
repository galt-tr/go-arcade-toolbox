package perf

import (
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
)

func TestPercentile(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := []struct {
		p    float64
		want float64
	}{
		{0, 1},
		{100, 10},
		{50, 5.5},  // linear interpolation: rank = 0.5*9 = 4.5 -> between 5 and 6
		{90, 9.1},  // rank = 0.9*9 = 8.1 -> between 9 and 10
		{25, 3.25}, // rank = 0.25*9 = 2.25 -> between 3 and 4
	}
	for _, c := range cases {
		got := percentile(data, c.p)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("percentile(%.0f) = %v, want %v", c.p, got, c.want)
		}
	}
}

func TestPercentileEdgeCases(t *testing.T) {
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
	if got := percentile([]float64{42}, 99); got != 42 {
		t.Errorf("percentile(single) = %v, want 42", got)
	}
}

func TestPhaseStatsFromMillis(t *testing.T) {
	ms := []float64{10, 20, 30, 40, 50}
	ps := phaseStatsFromMillis(ms)
	if ps.Count != 5 {
		t.Errorf("Count = %d, want 5", ps.Count)
	}
	if ps.MinMs != 10 || ps.MaxMs != 50 {
		t.Errorf("Min/Max = %v/%v, want 10/50", ps.MinMs, ps.MaxMs)
	}
	if ps.MeanMs != 30 {
		t.Errorf("Mean = %v, want 30", ps.MeanMs)
	}
	// Ensure the input slice is not mutated (a copy is sorted).
	if ms[0] != 10 {
		t.Errorf("input slice mutated: %v", ms)
	}
}

func TestBucketTPS(t *testing.T) {
	start := time.Unix(1000, 0)
	end := start.Add(30 * time.Second)
	var samples []opSample
	// 20 ops in the first 10s bucket, 40 in the second, 0 in the third.
	for i := 0; i < 20; i++ {
		samples = append(samples, opSample{doneAt: start.Add(time.Duration(i) * 100 * time.Millisecond)})
	}
	for i := 0; i < 40; i++ {
		samples = append(samples, opSample{doneAt: start.Add(10*time.Second + time.Duration(i)*100*time.Millisecond)})
	}
	buckets := bucketTPS(samples, start, end, 10*time.Second)
	if len(buckets) != 4 {
		t.Fatalf("len(buckets) = %d, want 4", len(buckets))
	}
	if buckets[0].TPS != 2 { // 20 / 10s
		t.Errorf("bucket0 TPS = %v, want 2", buckets[0].TPS)
	}
	if buckets[1].TPS != 4 { // 40 / 10s
		t.Errorf("bucket1 TPS = %v, want 4", buckets[1].TPS)
	}
	if buckets[2].TPS != 0 {
		t.Errorf("bucket2 TPS = %v, want 0", buckets[2].TPS)
	}
}

func sampleResult() *RunResult {
	return &RunResult{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
		Label:         "unit-test",
		Backend:       "postgres",
		Mode:          "twostep",
		Environment: Environment{
			Hostname: "box", CPUModel: "Test CPU", LogicalCores: 32,
			RAMBytes: 64 << 30, GoVersion: "go1.26.3", OS: "linux", Arch: "amd64",
			Kernel: "7.1.5", PodmanVersion: "podman version 5.0.0",
		},
		Config: ConfigEcho{Workers: 32, TargetTPS: 0, DurationSec: 60, WarmupSec: 10, PoolSize: 2000, Denomination: 1_000_000, PaymentSats: 1000, MaxDBConns: 40, Network: "testnet"},
		Throughput: Throughput{
			SustainedTPS: 512.5, TotalOps: 30750, WindowSeconds: 60, TargetTPS: TargetTPS, PctOfTarget: 51.25, BucketWidthS: 10,
			Buckets: []BucketTPS{{StartSec: 0, TPS: 500}, {StartSec: 10, TPS: 525}},
		},
		Phases: map[string]PhaseStats{
			"create":       {Count: 30750, MinMs: 0.5, P50Ms: 1.2, P95Ms: 3.4, P99Ms: 5.6, MaxMs: 40, MeanMs: 1.5},
			"sign_process": {Count: 30750, MinMs: 0.8, P50Ms: 2.1, P95Ms: 6.7, P99Ms: 9.9, MaxMs: 60, MeanMs: 2.5},
			"e2e":          {Count: 30750, MinMs: 1.3, P50Ms: 3.3, P95Ms: 9.1, P99Ms: 14.2, MaxMs: 80, MeanMs: 4.0},
		},
		Contention: Contention{Attempted: 31000, Succeeded: 30750, ContentionRetries: 240, DeadlockRetries: 5, ContentionFails: 10, OtherErrors: 0},
		Monitor:    MonitorStats{Enabled: true, Mining: true, MinedEmitted: 28000, ActionsTotal: 30760, ActionsCompleted: 9000, MaturationPct: 90},
		PoolDepth:  []poolSampleJSON{{AtSec: 10, BalanceSats: 1_999_000_000}, {AtSec: 20, BalanceSats: 1_998_000_000}},
		Notes:      []string{"a note"},
	}
}

func TestSchemaRoundTrip(t *testing.T) {
	orig := sampleResult()
	path := filepath.Join(t.TempDir(), "run.json")
	if err := WriteJSON(orig, path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	got, err := ReadJSON(path)
	if err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	if got.SchemaVersion != orig.SchemaVersion || got.Backend != orig.Backend || got.Mode != orig.Mode {
		t.Errorf("header mismatch: %+v", got)
	}
	if got.Throughput.SustainedTPS != orig.Throughput.SustainedTPS {
		t.Errorf("sustained TPS: got %v want %v", got.Throughput.SustainedTPS, orig.Throughput.SustainedTPS)
	}
	if got.Phases["e2e"].P99Ms != orig.Phases["e2e"].P99Ms {
		t.Errorf("e2e p99: got %v want %v", got.Phases["e2e"].P99Ms, orig.Phases["e2e"].P99Ms)
	}
	if got.Contention.ContentionRetries != orig.Contention.ContentionRetries {
		t.Errorf("contention retries: got %v want %v", got.Contention.ContentionRetries, orig.Contention.ContentionRetries)
	}
	if len(got.PoolDepth) != len(orig.PoolDepth) {
		t.Errorf("pool depth len: got %d want %d", len(got.PoolDepth), len(orig.PoolDepth))
	}
}

func TestReadJSONRejectsBadVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	orig := sampleResult()
	orig.SchemaVersion = 999
	if err := WriteJSON(orig, path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if _, err := ReadJSON(path); err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestRender(t *testing.T) {
	md := Render(sampleResult())
	for _, want := range []string{
		"# Benchmark: PostgreSQL (Mode A)",
		"Sustained TPS",
		"512.5",
		"Phase latency percentiles",
		"end-to-end",
		"Contention & errors",
		"Async loop",
		"Environment",
		"Test CPU",
		"a note",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report missing %q", want)
		}
	}
}

func TestDefaultConfigValidates(t *testing.T) {
	for _, b := range []string{"sqlite", "postgres", "aerospike-hybrid"} {
		cfg := DefaultConfig(perfprovider.Backend(b))
		cfg.Duration = time.Second
		cfg.Warmup = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("default %s config invalid: %v", b, err)
		}
	}
}
