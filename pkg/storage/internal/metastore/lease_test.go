package metastore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLeaseRepo_AcquireContendReclaimRelease exercises the monitor job-lease CAS
// primitives with explicit nanosecond timestamps (no wall clock): acquire,
// same-owner renew, contention by another owner, reclaim after expiry, and
// early release.
func TestLeaseRepo_AcquireContendReclaimRelease(t *testing.T) {
	ctx := context.Background()
	s := newSQLiteMeta(t)
	lr := s.Leases()

	const ttl = int64(100)
	base := int64(1000)

	// A acquires a fresh lease.
	ok, err := lr.Acquire(ctx, "job", "A", base+ttl, base)
	require.NoError(t, err)
	require.True(t, ok, "A acquires a fresh lease")

	// B is refused while A's lease is live.
	ok, err = lr.Acquire(ctx, "job", "B", base+50+ttl, base+50)
	require.NoError(t, err)
	require.False(t, ok, "B refused while A holds an unexpired lease")

	// A renews its own live lease (the normal in-process renewal path).
	ok, err = lr.Acquire(ctx, "job", "A", base+60+ttl, base+60)
	require.NoError(t, err)
	require.True(t, ok, "A renews its own lease")

	// After A's lease (until base+160) expires, B reclaims.
	ok, err = lr.Acquire(ctx, "job", "B", base+300+ttl, base+300)
	require.NoError(t, err)
	require.True(t, ok, "B reclaims after A's lease expires")

	// A cannot take it back while B's lease is live.
	ok, err = lr.Acquire(ctx, "job", "A", base+310+ttl, base+310)
	require.NoError(t, err)
	require.False(t, ok, "A refused while B holds the lease")

	// B releases early; A can acquire immediately.
	require.NoError(t, lr.Release(ctx, "job", "B", base+320))
	ok, err = lr.Acquire(ctx, "job", "A", base+330+ttl, base+330)
	require.NoError(t, err)
	require.True(t, ok, "A acquires after B releases")

	// A foreign release (wrong owner) is a no-op: A keeps the lease.
	require.NoError(t, lr.Release(ctx, "job", "C", base+335))
	ok, err = lr.Acquire(ctx, "job", "B", base+340+ttl, base+340)
	require.NoError(t, err)
	require.False(t, ok, "a wrong-owner release must not free the lease")
}
