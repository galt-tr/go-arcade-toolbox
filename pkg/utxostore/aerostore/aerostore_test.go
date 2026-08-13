package aerostore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

func TestBucketOf(t *testing.T) {
	cases := []struct {
		sats uint64
		want int
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{3, 1},
		{4, 2},
		{7, 2},
		{8, 3},
		{1000, 9},
		{1023, 9},
		{1024, 10},
		{1 << 20, 20},
		{(1 << 20) + 1, 20},
		{1 << 62, 62},
	}
	for _, c := range cases {
		require.Equalf(t, c.want, bucketOf(c.sats), "bucketOf(%d)", c.sats)
	}
}

// TestBucketOf_Monotonic pins the property the claim walk relies on: bucketOf is
// non-decreasing, and any coin sufficient for minSats lives in a bucket >=
// bucketOf(minSats) — so walking upward from that bucket never skips a
// sufficient coin.
func TestBucketOf_Monotonic(t *testing.T) {
	prev := 0
	for sats := uint64(1); sats < 5000; sats++ {
		b := bucketOf(sats)
		require.GreaterOrEqual(t, b, prev)
		prev = b
	}
	// A coin of value v >= minSats is always in bucket >= bucketOf(minSats).
	for _, minSats := range []uint64{1, 3, 500, 1000, 4095} {
		lo := bucketOf(minSats)
		for _, v := range []uint64{minSats, minSats + 1, minSats * 2, minSats * 100} {
			require.GreaterOrEqualf(t, bucketOf(v), lo, "coin %d sufficient for %d must not be below its bucket", v, minSats)
		}
	}
}

func TestClaimKeyEncoding(t *testing.T) {
	require.Equal(t, "1|default|3|9", claimKeyFor(1, "default", utxostore.TierMined, 9))
	require.Equal(t, "42|fuel|1|20", claimKeyFor(42, "fuel", utxostore.TierSending, 20))
	require.Equal(t, "1|default", invKeyFor(1, "default"))

	// Distinct scopes never collide on the claimKey for normal basket names.
	seen := map[string]bool{}
	for _, uid := range []int64{1, 2} {
		for _, basket := range []string{"default", "fuel", "reserve"} {
			for _, tier := range []utxostore.Tier{1, 2, 3} {
				for b := 0; b < 5; b++ {
					k := claimKeyFor(uid, basket, tier, b)
					require.False(t, seen[k], "collision on %q", k)
					seen[k] = true
				}
			}
		}
	}
}

func TestOutpointKeyDeterministicAndDistinct(t *testing.T) {
	op1 := utxostore.Outpoint{Vout: 0}
	op1.TxID[0] = 0xAA
	op2 := op1
	op2.Vout = 1
	require.Equal(t, outpointKey(op1), outpointKey(op1))
	require.NotEqual(t, outpointKey(op1), outpointKey(op2))
	require.Len(t, outpointKey(op1), 36)
}

func TestValidateDefaultTTL(t *testing.T) {
	ok := "objects=0;default-ttl=0;replication-factor=1"
	require.NoError(t, validateDefaultTTL("test", ok))

	bad := "objects=0;default-ttl=2592000;replication-factor=1"
	err := validateDefaultTTL("test", bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "REFUSING TO START")
	require.Contains(t, err.Error(), "2592000")

	missing := "objects=0;replication-factor=1"
	require.Error(t, validateDefaultTTL("test", missing))
}

func TestParseDSN(t *testing.T) {
	cfg, err := parseDSN("aerospike://db.example:3100/wallet?set=coins")
	require.NoError(t, err)
	require.Equal(t, "db.example", cfg.host)
	require.Equal(t, 3100, cfg.port)
	require.Equal(t, "wallet", cfg.namespace)
	require.Equal(t, "coins", cfg.set)

	// Defaults: port 3000, set "utxos".
	cfg, err = parseDSN("aerospike://localhost/test")
	require.NoError(t, err)
	require.Equal(t, DefaultPort, cfg.port)
	require.Equal(t, DefaultSet, cfg.set)

	// Credentials via userinfo.
	cfg, err = parseDSN("aerospike://user:pass@host/ns")
	require.NoError(t, err)
	require.Equal(t, "user", cfg.user)
	require.Equal(t, "pass", cfg.password)

	for _, bad := range []string{
		"postgres://host/ns",     // wrong scheme
		"aerospike:///ns",        // no host
		"aerospike://host",       // no namespace
		"aerospike://host/a/b",   // multi-segment namespace
		"aerospike://host:xx/ns", // bad port
	} {
		_, err := parseDSN(bad)
		require.Errorf(t, err, "expected %q to fail", bad)
	}
}

// TestQueryHonorsContext pins the contract [Store.streamQuery] exists for: the
// caller's context is checked BEFORE the client is dialed, so a canceled or
// expired budget stops the index walk instead of being silently discarded.
//
// These queries are index scans — the invKey sindex behind Balance measured
// 303,264 entries per bval on the live cluster — and the Aerospike client has no
// context-aware API, so a discarded context meant a caller's 5s budget bounded
// nothing at all.
//
// The store here has no client, which is the point: reaching Query would panic,
// so the assertions can only pass if the context was honored first.
func TestQueryHonorsContext(t *testing.T) {
	s := &Store{namespace: "test", set: "utxos"}

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := s.Balance(ctx, 1, "default")
		require.ErrorIs(t, err, context.Canceled)

		_, err = s.FindStaleReservations(ctx, time.Now(), 10)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("expired deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		_, err := s.Balance(ctx, 1, "default")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
