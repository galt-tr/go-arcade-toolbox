//go:build integration

package aerostore

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
)

// TestProbe_FilterExpressionCAS verifies the crux assumptions of the design
// against a real Aerospike container: single-record Operate with a
// FilterExpression + UPDATE_ONLY behaves as a CAS (FILTERED_OUT on a failed
// guard), RemoveBin+Put are atomic in one Operate, a reserved record drops out
// of the claimKey secondary index, durable-delete behavior on CE, and the
// default-ttl asinfo query.
func TestProbe_FilterExpressionCAS(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ns := ctr.Namespace()
	set := "probe"

	policy := as.NewClientPolicy()
	policy.Timeout = 5 * time.Second
	client, err := as.NewClientWithPolicy(policy, ctr.Host(), ctr.Port())
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// --- default-ttl asinfo probe -------------------------------------------
	nodes := client.GetNodes()
	require.NotEmpty(t, nodes)
	info, ierr := nodes[0].RequestInfo(as.NewInfoPolicy(), "namespace/"+ns)
	require.NoError(t, ierr)
	raw := info["namespace/"+ns]
	t.Logf("namespace info: %s", raw)
	defaultTTL := "MISSING"
	for _, kv := range strings.Split(raw, ";") {
		if strings.HasPrefix(kv, "default-ttl=") {
			defaultTTL = strings.TrimPrefix(kv, "default-ttl=")
		}
	}
	t.Logf("default-ttl = %q", defaultTTL)
	require.Equal(t, "0", defaultTTL, "testenv namespace must have default-ttl=0")

	// --- edition probe ------------------------------------------------------
	ed, eerr := nodes[0].RequestInfo(as.NewInfoPolicy(), "edition")
	require.NoError(t, eerr)
	t.Logf("edition = %q", ed["edition"])

	// --- put a claimable record --------------------------------------------
	key, kerr := as.NewKey(ns, set, []byte("outpoint-1"))
	require.NoError(t, kerr)
	claimKey := "1|default|3|9"
	wp := as.NewWritePolicy(0, 0)
	wp.RecordExistsAction = as.CREATE_ONLY
	err = client.PutBins(
		wp, key,
		as.NewBin("sats", 1000),
		as.NewBin("userId", 1),
		as.NewBin("claimKey", claimKey),
		as.NewBin("invKey", "1|default"),
	)
	require.NoError(t, err)

	// CREATE_ONLY again => KEY_EXISTS_ERROR (mint-conflict mechanism).
	err = client.PutBins(wp, key, as.NewBin("sats", 1000))
	require.Error(t, err)
	require.True(t, err.Matches(types.KEY_EXISTS_ERROR), "want KEY_EXISTS_ERROR, got %v", err)

	// --- secondary index on claimKey ---------------------------------------
	task, terr := client.CreateIndex(nil, ns, set, "probe_claimKey", "claimKey", as.STRING)
	require.NoError(t, terr)
	waitIndex(t, task)

	// SI query returns the claimable record.
	require.Equal(t, 1, siCount(t, client, ns, set, "claimKey", claimKey),
		"claimable record must be visible in the claimKey SI")

	// --- CAS reserve: FilterExpression(BinExists claimKey) + UPDATE_ONLY ----
	casPolicy := as.NewWritePolicy(0, 0)
	casPolicy.RecordExistsAction = as.UPDATE_ONLY
	casPolicy.FilterExpression = as.ExpBinExists("claimKey")

	rec, oerr := client.Operate(
		casPolicy, key,
		as.PutOp(as.NewBin("resBy", "res-1")),
		as.PutOp(as.NewBin("resAt", time.Now().UnixMilli())),
		as.PutOp(as.NewBin("claimKey", nil)), // remove bin
		as.GetOp(),
	)
	require.NoError(t, oerr, "first CAS reserve must succeed")
	require.NotNil(t, rec)

	// Atomicity: resBy set AND claimKey gone in the SAME operate.
	got, gerr := client.Get(nil, key)
	require.NoError(t, gerr)
	require.Equal(t, "res-1", got.Bins["resBy"])
	_, hasClaim := got.Bins["claimKey"]
	require.False(t, hasClaim, "claimKey must be removed atomically with the reserve")

	// Reserved record drops out of the claimKey SI.
	require.Equal(t, 0, siCount(t, client, ns, set, "claimKey", claimKey),
		"reserved record must disappear from the claimKey SI synchronously")

	// --- second CAS reserve => FILTERED_OUT (lost the race) -----------------
	_, oerr2 := client.Operate(
		casPolicy, key,
		as.PutOp(as.NewBin("resBy", "res-2")),
	)
	require.Error(t, oerr2)
	require.True(t, oerr2.Matches(types.FILTERED_OUT), "want FILTERED_OUT, got %v", oerr2)
	var aeErr *as.AerospikeError
	require.True(t, errors.As(oerr2, &aeErr))
	require.Equal(t, types.FILTERED_OUT, aeErr.ResultCode)

	// --- UPDATE_ONLY on a missing key => KEY_NOT_FOUND_ERROR ----------------
	missing, _ := as.NewKey(ns, set, []byte("no-such-outpoint"))
	_, merr := client.Operate(casPolicy, missing, as.PutOp(as.NewBin("resBy", "x")))
	require.Error(t, merr)
	require.True(t, merr.Matches(types.KEY_NOT_FOUND_ERROR), "want KEY_NOT_FOUND, got %v", merr)

	// --- durable delete behavior on this edition ---------------------------
	ddPolicy := as.NewWritePolicy(0, 0)
	ddPolicy.DurableDelete = true
	existed, derr := client.Delete(ddPolicy, key)
	if derr != nil {
		t.Logf("durable delete error (expected on CE): matches ENTERPRISE_ONLY=%v: %v",
			derr.Matches(types.ENTERPRISE_ONLY), derr)
	} else {
		t.Logf("durable delete succeeded, existed=%v", existed)
	}
}

func waitIndex(t *testing.T, task *as.IndexTask) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		done, err := task.IsDone()
		require.NoError(t, err)
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("index did not build in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func siCount(t *testing.T, client *as.Client, ns, set, bin, val string) int {
	t.Helper()
	stmt := as.NewStatement(ns, set)
	require.NoError(t, stmt.SetFilter(as.NewEqualFilter(bin, val)))
	rs, err := client.Query(nil, stmt)
	require.NoError(t, err)
	n := 0
	for res := range rs.Results() {
		require.NoError(t, res.Err, fmt.Sprintf("query result error: %v", res.Err))
		n++
	}
	return n
}
