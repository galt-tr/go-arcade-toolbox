package aerostore

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// TestReservePolicy_NoRetries pins the invariant that keeps audit P2-7's phantom
// reservation unreachable: the claim-reserve CAS must never be auto-retried.
//
// The write removes claimKey, which is the very thing its own FilterExpression
// demands, so a retry of a request that SUCCEEDED but whose reply was lost comes
// back FILTERED_OUT — [Store.reserve] reads that as "lost the race" and reports
// no coin, while the server holds the coin reserved under a token the caller
// thinks it never obtained. Only the stale-reservation sweep would free it.
//
// The client makes this true by default (NewWritePolicy documents "writes may
// not be idempotent" and sets MaxRetries = 0) and the store never overrides it.
// This test is the tripwire for the day someone does HERE. It is deliberately
// not a tripwire for the other route — a dynamic client configuration setting
// max_retries, which is enabled by the AEROSPIKE_CLIENT_CONFIG_URL environment
// variable rather than by any code in this package, and which patches a COPY of
// the policy at command time, so no assertion on the policy this constructor
// returns could ever see it. That route is covered by the startup warning
// (TestWarnOnDynamicClientConfig), not by this test.
//
// Raising MaxRetries is not a one-line change either way: see the per-call-nonce
// design in [reservePolicy]'s doc, which is what a retry-safe reserve requires.
func TestReservePolicy_NoRetries(t *testing.T) {
	wp := reservePolicy("1|default|3|9")
	require.Zero(t, wp.MaxRetries, "the reserve CAS is not idempotent: it must not be auto-retried")
	require.Equal(t, as.UPDATE_ONLY, wp.RecordExistsAction, "a reserve must never resurrect a deleted coin")
	require.NotNil(t, wp.FilterExpression, "the reserve is only exclusive because of its claimKey guard")

	// The default the invariant rides on, asserted directly: if a future client
	// release changes it, this fails here rather than in production.
	require.Zero(t, as.NewWritePolicy(0, 0).MaxRetries, "aerospike write policies must default to no retries")
}

// TestWarnOnDynamicClientConfig covers the half of audit P2-7 no policy
// assertion can reach: a dynamic client configuration can raise write retries at
// command time, on a copy of the policy, without this package being involved. It
// cannot be asserted away, so it is announced.
func TestWarnOnDynamicClientConfig(t *testing.T) {
	warned := func(configURL string) bool {
		var buf bytes.Buffer
		s := &Store{logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))}
		s.warnOnDynamicClientConfig(configURL)
		return strings.Contains(buf.String(), "max_retries")
	}

	require.False(t, warned(""), "no dynamic configuration: nothing to say")
	require.False(t, warned("   "), "the client trims before testing emptiness; so must we")
	require.True(t, warned("file:///etc/aerospike/client.yaml"), "an active dynamic configuration must be announced")
}

// TestNoteClaimableAllTiers is the audit P2-6c unit proof. The restore paths
// cannot know which tier's bucket a coin became claimable in — restoreClaimKeyOp
// derives that server-side from the LIVE tier bin, precisely so a racing Promote
// cannot strand a stale-tier key — so marking only the snapshot's tier left the
// coin invisible behind the real bucket's empty-probe suppression.
func TestNoteClaimableAllTiers(t *testing.T) {
	clk := newFakeClock()
	cc := newClaimCache(8, 0, time.Hour, clk.now)
	s := &Store{claimCache: cc}

	const (
		userID = int64(7)
		basket = "default"
		sats   = uint64(1000)
	)
	bucket := bucketOf(sats)

	// Every tier's bucket exists and is suppressed by a previous empty probe.
	for _, tier := range allTiers() {
		b := cc.bucket(claimKeyFor(userID, basket, tier, bucket))
		b.dataMu.Lock()
		b.probedEmpty, b.probedAt = true, clk.now()
		b.dataMu.Unlock()
	}

	s.noteClaimableAllTiers(userID, basket, sats)

	for _, tier := range allTiers() {
		b := cc.lookup(claimKeyFor(userID, basket, tier, bucket))
		require.NotNil(t, b)
		b.dataMu.Lock()
		suppressed := b.knownEmptyLocked(clk.now(), cc.emptyTTL)
		b.dataMu.Unlock()
		require.Falsef(t, suppressed, "tier %s must be re-probed after a restore", tier)
	}

	// A neighboring value-bucket is none of its business.
	other := cc.bucket(claimKeyFor(userID, basket, utxostore.TierMined, bucket+1))
	other.dataMu.Lock()
	other.probedEmpty, other.probedAt = true, clk.now()
	other.dataMu.Unlock()
	s.noteClaimableAllTiers(userID, basket, sats)
	other.dataMu.Lock()
	stillSuppressed := other.knownEmptyLocked(clk.now(), cc.emptyTTL)
	other.dataMu.Unlock()
	require.True(t, stillSuppressed, "only the coin's own value-bucket is invalidated")

	// Disabled cache: still a no-op, not a nil dereference.
	(&Store{}).noteClaimableAllTiers(userID, basket, sats)
}

func TestSampleSizeAndPreferredMatches(t *testing.T) {
	require.Equal(t, int64(claimSample), sampleSize(1), "a small request still samples for best-fit")
	require.Equal(t, int64(claimSample+5), sampleSize(claimSample+5))

	cands := []candidate{{sats: 30}, {sats: 10}, {sats: 5}, {sats: 20}}
	big := func(sats uint64) bool { return sats >= 10 }

	got := preferredMatches(cands, big, smallestFirst, 2)
	require.Len(t, got, 2)
	require.Equal(t, uint64(10), got[0].sats)
	require.Equal(t, uint64(20), got[1].sats, "prefer orders the survivors before the trim")

	largest := func(a, b uint64) bool { return a > b }
	got = preferredMatches(cands, big, largest, 1)
	require.Len(t, got, 1)
	require.Equal(t, uint64(30), got[0].sats)

	require.Empty(t, preferredMatches(cands, func(uint64) bool { return false }, nil, 3))
	require.Len(t, preferredMatches(cands, big, nil, 10), 3, "a short result is not padded")
}
