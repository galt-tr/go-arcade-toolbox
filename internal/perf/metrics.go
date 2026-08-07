package perf

import (
	"sort"
	"sync/atomic"
	"time"
)

// opSample records the timings of one completed op and when it finished.
type opSample struct {
	createNs int64 // create phase (two-step only; 0 in single-call mode)
	signNs   int64 // sign+process phase (two-step only)
	e2eNs    int64 // end-to-end (both modes)
	doneAt   time.Time
}

// poolSample records a point-in-time snapshot of spendable pool value.
type poolSample struct {
	atSec       float64
	balanceSats uint64
}

// counters holds lock-free run counters.
type counters struct {
	attempted         atomic.Int64
	succeeded         atomic.Int64
	contentionRetries atomic.Int64 // contention retries across all ops
	deadlockRetries   atomic.Int64 // deadlock retries across all ops
	contentionFails   atomic.Int64 // ops that exhausted retries on contention
	deadlockFails     atomic.Int64 // ops that exhausted retries on a DB deadlock
	otherErrors       atomic.Int64
	minedEmitted      atomic.Int64 // MINED SSE frames emitted by the auto-miner
}

// collector accumulates per-worker samples (no shared lock on the hot path)
// and run-wide atomic counters.
type collector struct {
	counters counters
	// perWorker[i] is worker i's private sample slice, merged at snapshot time.
	perWorker [][]opSample
	pool      []poolSample
}

func newCollector(workers int) *collector {
	c := &collector{perWorker: make([][]opSample, workers)}
	for i := range c.perWorker {
		c.perWorker[i] = make([]opSample, 0, 4096)
	}
	return c
}

// record appends a sample to worker i's slice. Called only by worker i, so no
// synchronization is needed.
func (c *collector) record(worker int, s opSample) {
	c.perWorker[worker] = append(c.perWorker[worker], s)
}

func (c *collector) recordPool(s poolSample) {
	c.pool = append(c.pool, s)
}

// mergedSamples flattens all workers' samples into one slice.
func (c *collector) mergedSamples() []opSample {
	n := 0
	for _, w := range c.perWorker {
		n += len(w)
	}
	out := make([]opSample, 0, n)
	for _, w := range c.perWorker {
		out = append(out, w...)
	}
	return out
}

// percentile returns the p-th percentile (0..100) of an ascending-sorted slice
// using linear interpolation between closest ranks (the NumPy default). Returns
// 0 for an empty slice.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}
	rank := (p / 100) * float64(n-1)
	lo := int(rank)
	frac := rank - float64(lo)
	if lo+1 >= n {
		return sorted[n-1]
	}
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

// phaseStatsFromMillis computes summary statistics over a slice of latencies in
// milliseconds. It sorts a copy, so the caller's slice is untouched.
func phaseStatsFromMillis(ms []float64) PhaseStats {
	if len(ms) == 0 {
		return PhaseStats{}
	}
	s := append([]float64(nil), ms...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	return PhaseStats{
		Count:  len(s),
		P50Ms:  round3(percentile(s, 50)),
		P95Ms:  round3(percentile(s, 95)),
		P99Ms:  round3(percentile(s, 99)),
		MaxMs:  round3(s[len(s)-1]),
		MeanMs: round3(sum / float64(len(s))),
		MinMs:  round3(s[0]),
	}
}

func round3(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}

func nsToMs(ns int64) float64 { return float64(ns) / 1e6 }

// bucketTPS groups completions into fixed-width buckets over [start, end] and
// returns the per-bucket throughput.
func bucketTPS(samples []opSample, start, end time.Time, width time.Duration) []BucketTPS {
	if end.Before(start) || width <= 0 {
		return nil
	}
	total := end.Sub(start)
	n := int(total/width) + 1
	if n <= 0 {
		return nil
	}
	counts := make([]int, n)
	for _, s := range samples {
		if s.doneAt.Before(start) || s.doneAt.After(end) {
			continue
		}
		idx := int(s.doneAt.Sub(start) / width)
		if idx >= 0 && idx < n {
			counts[idx]++
		}
	}
	out := make([]BucketTPS, 0, n)
	for i, c := range counts {
		out = append(out, BucketTPS{
			StartSec: round3(float64(i) * width.Seconds()),
			TPS:      round3(float64(c) / width.Seconds()),
		})
	}
	return out
}
