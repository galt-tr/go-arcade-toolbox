package metastore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// TestFindResendable pins the SendWaiting work-list that makes stranded
// transactions self-heal: the delayed queue ('unsent') sends promptly, while
// pre-broadcast strays ('unprocessed' — e.g. a broadcast the circuit breaker
// short-circuited) are re-driven only after a grace and only when they still
// carry a raw tx and were never broadcast.
func TestFindResendable(t *testing.T) {
	ctx := context.Background()
	clk := &manualClock{t: baseTime}
	s := newSQLiteMeta(t, metastore.WithClock(clk.now))
	kt := s.KnownTx()

	raw := []byte{0x01, 0x02, 0x03}
	hexid := func(n byte) string { return fmt.Sprintf("%064x", n) }
	up := func(txid string, status wdk.ProvenTxReqStatus, rawtx []byte, broadcast bool) {
		require.NoError(t, kt.Upsert(ctx, metastore.KnownTx{
			TxID: txid, Status: status, RawTx: rawtx, WasBroadcast: broadcast,
		}))
	}

	unsent := hexid(0xa1)        // delayed queue → always re-drivable
	stranded := hexid(0xb2)      // aged unprocessed + raw + not broadcast → re-drivable
	sentAlready := hexid(0xd4)   // unprocessed but already broadcast → excluded
	noRaw := hexid(0xe5)         // unprocessed but no raw tx → excluded (can't re-send)
	freshStranded := hexid(0xc3) // unprocessed within grace → skipped this cycle

	up(unsent, wdk.ProvenTxStatusUnsent, raw, false)
	up(stranded, wdk.ProvenTxStatusUnprocessed, raw, false)
	up(sentAlready, wdk.ProvenTxStatusUnprocessed, raw, true)
	up(noRaw, wdk.ProvenTxStatusUnprocessed, nil, false)

	clk.advance(time.Minute) // age everything above past the grace
	up(freshStranded, wdk.ProvenTxStatusUnprocessed, raw, false)

	got, err := kt.FindResendable(ctx, 30*time.Second, 100)
	require.NoError(t, err)

	found := map[string]bool{}
	for i := range got {
		found[got[i].TxID] = true
	}
	require.True(t, found[unsent], "unsent sends promptly (no grace)")
	require.True(t, found[stranded], "aged unprocessed with raw tx is re-drivable")
	require.False(t, found[freshStranded], "unprocessed within grace is skipped this cycle")
	require.False(t, found[sentAlready], "already-broadcast tx is excluded")
	require.False(t, found[noRaw], "unprocessed with no raw tx is excluded")
}
