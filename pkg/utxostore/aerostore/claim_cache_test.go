package aerostore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

// These tests drive the claim cache through the [prober] seam, with no
// Aerospike server anywhere. That is the point of the seam: every availability
// bug audit P2-5/P2-6/P2-8 found was a decision the cache made BEFORE it reached
// the network — whether to probe at all, what to do with a snapshot an empty
// probe just disproved, whether to ask the narrower question, how many buckets
// to keep — and each one is asserted here by counting probes rather than by
// staging a cluster.

// fakeClock is a manually advanced clock, so TTL expiry is a deterministic step
// rather than a sleep.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// recordingProber is a [prober] under test control: it counts whole-bucket
// refills separately from value-filtered fallbacks (the two probe kinds the
// cache can issue) and serves each from its own pool.
type recordingProber struct {
	mu           sync.Mutex
	unfiltered   int
	filtered     int
	pool         []candidate // what an unfiltered whole-bucket probe returns
	filteredPool []candidate // what a value-filtered probe returns
	err          error
	hook         func(claimKey string) // runs inside the probe, before it answers
}

func (p *recordingProber) probe(ctx context.Context, claimKey string, valueFilter *as.Expression, maxRecords int64) ([]candidate, error) {
	p.mu.Lock()
	hook := p.hook
	p.mu.Unlock()
	if hook != nil {
		hook(claimKey)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	src := p.pool
	if valueFilter != nil {
		p.filtered++
		src = p.filteredPool
	} else {
		p.unfiltered++
	}
	if p.err != nil {
		return nil, p.err
	}
	out := make([]candidate, 0, len(src))
	for _, c := range src {
		if int64(len(out)) >= maxRecords {
			break
		}
		out = append(out, c)
	}
	return out, nil
}

func (p *recordingProber) counts() (unfiltered, filtered int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unfiltered, p.filtered
}

func (p *recordingProber) setPool(cands ...candidate) {
	p.mu.Lock()
	p.pool = cands
	p.mu.Unlock()
}

// requireProbes asserts the exact number of probes of each kind issued so far.
func requireProbes(t *testing.T, p *recordingProber, unfiltered, filtered int, msg string) {
	t.Helper()
	gotU, gotF := p.counts()
	require.Equalf(t, unfiltered, gotU, "%s: whole-bucket probes", msg)
	require.Equalf(t, filtered, gotF, "%s: value-filtered probes", msg)
}

var (
	anyCoin       = func(uint64) bool { return true }
	smallestFirst = func(a, b uint64) bool { return a < b }
)

// TestClaimCache_EmptyProbeSuppressionExpires is the audit P2-5 unit repro: the
// negative cache is cleared only by THIS process's writes, so a coin another
// replica makes claimable is invisible until the TTL lets the bucket be probed
// again. Before the TTL that invisibility was permanent — a replica that once
// saw the bucket empty refused payments on a funded wallet until restart.
func TestClaimCache_EmptyProbeSuppressionExpires(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{}
	cc := newClaimCache(8, 0, time.Second, clk.now)
	ctx := context.Background()

	got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)
	requireProbes(t, p, 1, 0, "first claim probes")

	// Within the TTL the empty verdict is reused: idle claims on an always-empty
	// tier must not cost a query each.
	for i := 0; i < 3; i++ {
		got, err = cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
		require.NoError(t, err)
		require.Empty(t, got)
	}
	requireProbes(t, p, 1, 0, "within TTL")

	// ANOTHER PROCESS mints into the bucket. Nothing calls markClaimable here —
	// that is exactly the case the event-driven invalidation cannot cover.
	p.setPool(candidate{sats: 1000})

	clk.advance(999 * time.Millisecond)
	got, err = cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got, "still inside the TTL")
	requireProbes(t, p, 1, 0, "still inside the TTL")

	clk.advance(2 * time.Millisecond) // now 1.001s since the empty probe
	got, err = cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Len(t, got, 1, "past the TTL the bucket must be re-probed and the foreign coin found")
	require.Equal(t, uint64(1000), got[0].sats)
	requireProbes(t, p, 2, 0, "past the TTL")
}

// TestClaimCache_EmptyTTLZeroNeverSuppresses pins the escape hatch: with the TTL
// disabled every claim probes, so there is no staleness window at all.
func TestClaimCache_EmptyTTLZeroNeverSuppresses(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{}
	cc := newClaimCache(8, 0, 0, clk.now)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
		require.NoError(t, err)
		require.Empty(t, got)
		requireProbes(t, p, i, 0, "every claim probes")
	}
}

// TestClaimCache_InProcessRestoreBeatsTheTTL pins that markClaimable is still
// the fast path: a coin this process restores is claimable on the very next
// claim, without waiting out the TTL.
func TestClaimCache_InProcessRestoreBeatsTheTTL(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{}
	cc := newClaimCache(8, 0, time.Hour, clk.now)
	ctx := context.Background()

	got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)

	p.setPool(candidate{sats: 42})
	cc.markClaimable("k")

	got, err = cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(42), got[0].sats)
	requireProbes(t, p, 2, 0, "markClaimable forces the re-probe immediately")
}

// TestClaimCache_EmptyRefillDropsStaleCandidates is the audit P2-6a unit repro.
// A refill that comes back empty proves every cached candidate is dead; keeping
// them meant the cache kept handing out coins whose reserve CAS could only
// fail, which the claim paths report as ErrContention — forever, because a
// non-empty snapshot also defeats the known-empty short circuit.
func TestClaimCache_EmptyRefillDropsStaleCandidates(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{pool: []candidate{{sats: 10}, {sats: 20}}}
	cc := newClaimCache(8, 0, time.Second, clk.now)
	ctx := context.Background()
	big := func(sats uint64) bool { return sats >= 100 }

	// Fill the snapshot and drain one coin; {20} stays cached.
	got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, smallestFirst, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(10), got[0].sats)
	requireProbes(t, p, 1, 0, "first claim")

	// Both coins are gone on the server now (claimed elsewhere, spent, frozen —
	// the cache cannot tell). A predicate the stale candidate cannot satisfy
	// forces the refill, which comes back empty.
	p.setPool()
	got, err = cc.take(ctx, p.probe, "k", nil, big, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)
	requireProbes(t, p, 2, 0, "the unsatisfiable predicate forces a refill")

	// The empty probe disproved {20}: it must be gone, and the bucket must now
	// count as known empty.
	got, err = cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got, "a candidate an empty refill disproved must not be handed out")
	requireProbes(t, p, 2, 0, "the empty refill also made the bucket known-empty")
}

// TestClaimCache_FilteredProbeFallback is the audit P2-6b unit repro. The
// unfiltered refill is a SAMPLE of the bucket, so a bucket with more coins than
// the refill size can return a sample in which nothing matches the caller's
// value predicate while a matching coin sits just outside it. Reporting that as
// underflow is a false not-enough-funds on a funded wallet.
func TestClaimCache_FilteredProbeFallback(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{
		// A refill of 3 on a bucket whose first three rows are all too small…
		pool: []candidate{{sats: 10}, {sats: 11}, {sats: 12}},
		// …while the server, asked the narrow question, finds the big one.
		filteredPool: []candidate{{sats: 1000}},
	}
	cc := newClaimCache(3, 0, time.Second, clk.now)
	ctx := context.Background()
	big := func(sats uint64) bool { return sats >= 100 }
	filter := as.ExpGreaterEq(as.ExpIntBin(binSats), as.ExpIntVal(100))

	got, err := cc.take(ctx, p.probe, "k", filter, big, smallestFirst, 1)
	require.NoError(t, err)
	require.Len(t, got, 1, "a sufficient coin outside the unfiltered sample must still be found")
	require.Equal(t, uint64(1000), got[0].sats)
	requireProbes(t, p, 1, 1, "one refill, then one narrow probe")

	// The fallback is bounded at one probe of each kind per call — it is not a
	// retry loop, and it never caches its value-biased result.
	got, err = cc.take(ctx, p.probe, "k", filter, big, smallestFirst, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	requireProbes(t, p, 2, 2, "at most one probe of each kind per call")
}

// TestClaimCache_NoFilteredProbeWhenPartiallySatisfied pins the gate on the
// fallback: a claim that got SOMETHING is progress the caller will act on and
// come back for, and topping it up from a second, value-biased probe could hand
// back a row still sitting in the snapshot — the same coin twice in one result.
func TestClaimCache_NoFilteredProbeWhenPartiallySatisfied(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{
		pool:         []candidate{{sats: 1000}, {sats: 10}, {sats: 11}},
		filteredPool: []candidate{{sats: 1000}, {sats: 2000}},
	}
	cc := newClaimCache(3, 0, time.Second, clk.now)
	ctx := context.Background()
	big := func(sats uint64) bool { return sats >= 100 }
	filter := as.ExpGreaterEq(as.ExpIntBin(binSats), as.ExpIntVal(100))

	got, err := cc.take(ctx, p.probe, "k", filter, big, smallestFirst, 2)
	require.NoError(t, err)
	require.Len(t, got, 1, "a short result is honest: the caller loops")
	require.Equal(t, uint64(1000), got[0].sats)
	requireProbes(t, p, 1, 0, "a partially satisfied claim does not fall back")
}

// TestClaimCache_NoFilteredProbeWhenBucketEmpty pins the other half of the
// bound: when the unfiltered refill proves the bucket holds nothing at all, a
// narrower probe cannot find anything either, so it is not issued.
func TestClaimCache_NoFilteredProbeWhenBucketEmpty(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{filteredPool: []candidate{{sats: 1000}}}
	cc := newClaimCache(8, 0, time.Second, clk.now)
	ctx := context.Background()
	filter := as.ExpGreaterEq(as.ExpIntBin(binSats), as.ExpIntVal(100))

	got, err := cc.take(ctx, p.probe, "k", filter, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Empty(t, got)
	requireProbes(t, p, 1, 0, "an empty bucket needs no narrow probe")
}

// TestClaimCache_LRUEviction is the audit P2-8 unit proof: the bucket map is
// capped and evicts least-recently-used, and eviction can only ever cost a
// re-probe — never a wrong answer, because the only thing an evicted bucket
// could have carried forward is a suppression.
func TestClaimCache_LRUEviction(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{}
	cc := newClaimCache(8, 2, time.Hour, clk.now)
	ctx := context.Background()

	claim := func(key string) {
		t.Helper()
		_, err := cc.take(ctx, p.probe, key, nil, anyCoin, nil, 1)
		require.NoError(t, err)
	}

	claim("a")
	claim("b")
	claim("c") // over the cap: "a", the least recently used, goes
	requireProbes(t, p, 3, 0, "three distinct buckets, three probes")
	require.Len(t, cc.buckets, 2, "the map is capped")
	require.Nil(t, cc.lookup("a"), "the least recently used bucket is evicted")
	require.NotNil(t, cc.lookup("b"))
	require.NotNil(t, cc.lookup("c"))

	// A cache HIT refreshes recency too, so the bucket a claim just used is not
	// the next victim.
	claim("b") // served from b's known-empty state: no probe, but b moves to the front
	requireProbes(t, p, 3, 0, "a known-empty hit costs no probe")

	claim("a") // re-created, evicting "c" (now the least recently used)
	requireProbes(t, p, 4, 0, "an evicted bucket is re-probed, never assumed empty")
	require.Nil(t, cc.lookup("c"))
	require.NotNil(t, cc.lookup("a"))
	require.NotNil(t, cc.lookup("b"))

	// markClaimable on a bucket nobody is claiming from is a no-op: it must not
	// resurrect an evicted bucket or fill the map with write-side keys.
	cc.markClaimable("c")
	require.Nil(t, cc.lookup("c"))
	require.Len(t, cc.buckets, 2)
}

// TestClaimCache_UnboundedWhenCapDisabled pins that the cap is opt-out.
func TestClaimCache_UnboundedWhenCapDisabled(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{}
	cc := newClaimCache(8, 0, time.Hour, clk.now)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		_, err := cc.take(ctx, p.probe, string(rune('a'+i%26))+string(rune('a'+i/26)), nil, anyCoin, nil, 1)
		require.NoError(t, err)
	}
	require.Len(t, cc.buckets, 50)
}

// TestClaimCache_SingleFlight pins the property the refill token exists for: a
// thundering herd of claims on an empty snapshot collapses to ONE probe. It is
// the reason the cache exists at all (the aerospike client serializes queries on
// a per-node routing lock), so the TTL and fallback work above must not have
// widened it.
func TestClaimCache_SingleFlight(t *testing.T) {
	const takers = 8

	clk := newFakeClock()
	release := make(chan struct{})
	probing := make(chan struct{}, 1)
	var arrived atomic.Int64

	p := &recordingProber{pool: []candidate{{sats: 1}, {sats: 2}, {sats: 3}, {sats: 4}}}
	p.hook = func(string) {
		select {
		case probing <- struct{}{}: // announce the first probe
		default:
		}
		<-release // hold it open while the herd piles up
	}
	cc := newClaimCache(8, 0, time.Hour, clk.now)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < takers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arrived.Add(1)
			_, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
			require.NoError(t, err)
		}()
	}

	<-probing
	for arrived.Load() < takers {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the stragglers reach the refill token
	close(release)
	wg.Wait()

	requireProbes(t, p, 1, 0, "the whole herd shares one probe")
}

// TestClaimCache_HonorsContext pins that a claim stops costing index reads the
// moment its caller's budget is gone — on entry, and through the probe.
func TestClaimCache_HonorsContext(t *testing.T) {
	clk := newFakeClock()

	t.Run("canceled on entry", func(t *testing.T) {
		p := &recordingProber{pool: []candidate{{sats: 1}}}
		cc := newClaimCache(8, 0, time.Hour, clk.now)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, got)
		requireProbes(t, p, 0, 0, "a canceled claim must not probe")
	})

	t.Run("canceled mid-probe", func(t *testing.T) {
		p := &recordingProber{pool: []candidate{{sats: 1}}}
		cc := newClaimCache(8, 0, time.Hour, clk.now)
		ctx, cancel := context.WithCancel(context.Background())
		p.hook = func(string) { cancel() }

		got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, got)
	})
}

// TestClaimCache_ProbeErrorLeavesSnapshotIntact pins that a backend failure is
// reported rather than mistaken for an empty bucket: a failed probe must not
// mark the bucket empty (which would suppress the retry) nor discard candidates
// it never disproved.
func TestClaimCache_ProbeErrorLeavesSnapshotIntact(t *testing.T) {
	clk := newFakeClock()
	p := &recordingProber{pool: []candidate{{sats: 10}, {sats: 20}}}
	cc := newClaimCache(8, 0, time.Second, clk.now)
	ctx := context.Background()
	big := func(sats uint64) bool { return sats >= 100 }

	_, err := cc.take(ctx, p.probe, "k", nil, anyCoin, smallestFirst, 1)
	require.NoError(t, err)

	boom := errors.New("backend down")
	p.mu.Lock()
	p.err = boom
	p.mu.Unlock()

	_, err = cc.take(ctx, p.probe, "k", nil, big, nil, 1)
	require.ErrorIs(t, err, boom)

	p.mu.Lock()
	p.err = nil
	p.mu.Unlock()

	// The surviving candidate is still there, and the bucket was never marked
	// empty, so the next claim is served (or re-probes) normally.
	got, err := cc.take(ctx, p.probe, "k", nil, anyCoin, nil, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint64(20), got[0].sats)
}
