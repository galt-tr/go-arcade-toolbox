package arcade

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedTS is the timestamp shared by the golden fixtures. UTC so the RFC3339
// encoding is deterministic ("...Z") and unmarshal round-trips to an equal
// time.Time.
var fixedTS = time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

// --- Status wire-value pins ---

// TestStatus_WireValues pins every status constant to its EXACT wire string.
// These are a copied contract with arcade; a drift here silently breaks status
// interop with the oracle.
func TestStatus_WireValues(t *testing.T) {
	want := map[Status]string{
		StatusUnknown:              "UNKNOWN",
		StatusReceived:             "RECEIVED",
		StatusSentToNetwork:        "SENT_TO_NETWORK",
		StatusAcceptedByNetwork:    "ACCEPTED_BY_NETWORK",
		StatusSeenOnNetwork:        "SEEN_ON_NETWORK",
		StatusSeenMultipleNodes:    "SEEN_MULTIPLE_NODES",
		StatusDoubleSpendAttempted: "DOUBLE_SPEND_ATTEMPTED",
		StatusRejected:             "REJECTED",
		StatusPendingRetry:         "PENDING_RETRY",
		StatusStumpProcessing:      "STUMP_PROCESSING",
		StatusMined:                "MINED",
		StatusImmutable:            "IMMUTABLE",
	}
	for status, str := range want {
		assert.Equal(t, str, string(status))
	}
	assert.Len(t, want, 12, "expected exactly 12 statuses")
}

// TestStatus_SeenMultipleNodesTrap is the standalone regression for the naming
// trap: the wire value is SEEN_MULTIPLE_NODES, NOT SEEN_ON_MULTIPLE_NODES.
func TestStatus_SeenMultipleNodesTrap(t *testing.T) {
	assert.Equal(t, "SEEN_MULTIPLE_NODES", string(StatusSeenMultipleNodes))
	assert.NotEqual(t, "SEEN_ON_MULTIPLE_NODES", string(StatusSeenMultipleNodes))
}

// TestAllStatuses checks AllStatuses enumerates every constant, in order.
func TestAllStatuses(t *testing.T) {
	require.Equal(t, []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusDoubleSpendAttempted, StatusRejected, StatusPendingRetry,
		StatusStumpProcessing, StatusMined, StatusImmutable,
	}, AllStatuses())
}

// --- HexBytes ---

// TestHexBytes_Marshal pins the nil-vs-empty-vs-data distinction on the wire,
// matching arcade's models.HexBytes exactly.
func TestHexBytes_Marshal(t *testing.T) {
	cases := []struct {
		name string
		in   HexBytes
		want string
	}{
		{"nil", nil, "null"},
		{"empty-non-nil", HexBytes{}, `""`},
		{"data", HexBytes{0xab, 0xcd, 0xef}, `"abcdef"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.in.MarshalJSON()
			require.NoError(t, err)
			assert.Equal(t, c.want, string(got))
		})
	}
}

// TestHexBytes_Unmarshal pins decode behavior: null -> nil, "" -> non-nil
// empty, hex -> bytes. null and "" are NOT interchangeable.
func TestHexBytes_Unmarshal(t *testing.T) {
	var fromNull HexBytes
	require.NoError(t, fromNull.UnmarshalJSON([]byte("null")))
	assert.Nil(t, fromNull)

	var fromEmpty HexBytes
	require.NoError(t, fromEmpty.UnmarshalJSON([]byte(`""`)))
	require.NotNil(t, fromEmpty)
	assert.Len(t, fromEmpty, 0)

	var fromData HexBytes
	require.NoError(t, fromData.UnmarshalJSON([]byte(`"abcdef"`)))
	assert.Equal(t, HexBytes{0xab, 0xcd, 0xef}, fromData)
}

// TestHexBytes_UnmarshalInvalid rejects non-hex input.
func TestHexBytes_UnmarshalInvalid(t *testing.T) {
	var h HexBytes
	assert.Error(t, h.UnmarshalJSON([]byte(`"zz"`)))
}

// --- Golden TxRecord fixtures ---

// goldenFixtures pair a TxRecord with the exact JSON arcade marshals for it.
// The JSON was derived from arcade's encoder (models.TransactionStatus), which
// this package mirrors. Note that timestamp always appears, and nextRetryAt /
// merkleRegisteredAt ALWAYS appear as "0001-01-01T00:00:00Z" when zero because
// encoding/json omitempty is a no-op on time.Time.
func goldenFixtures() []struct {
	name   string
	record TxRecord
	json   string
} {
	return []struct {
		name   string
		record TxRecord
		json   string
	}{
		{
			name: "mined_with_merklepath_and_rawtx",
			record: TxRecord{
				TxID:        "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
				Status:      StatusMined,
				Timestamp:   fixedTS,
				BlockHash:   "00000000000000000002a7c4c1e48d76c5a37902165a9e5f1e0f8f9c9d4c7b3a",
				BlockHeight: 800123,
				MerklePath:  HexBytes{0xfe, 0xc7, 0x0c, 0x0d, 0x00},
				RawTx:       HexBytes{0x01, 0x00, 0x00, 0x00, 0x01},
			},
			json: `{
				"txid":"4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
				"txStatus":"MINED",
				"timestamp":"2024-01-15T10:30:00Z",
				"blockHash":"00000000000000000002a7c4c1e48d76c5a37902165a9e5f1e0f8f9c9d4c7b3a",
				"blockHeight":800123,
				"merklePath":"fec70c0d00",
				"rawTx":"0100000001",
				"nextRetryAt":"0001-01-01T00:00:00Z",
				"merkleRegisteredAt":"0001-01-01T00:00:00Z"
			}`,
		},
		{
			name: "rejected_with_extrainfo_and_competing",
			record: TxRecord{
				TxID:         "8e5f2d1c9b0a7f6e5d4c3b2a1908070605040302010f0e0d0c0b0a0908070605",
				Status:       StatusRejected,
				Timestamp:    fixedTS,
				ExtraInfo:    "arc error 465: transaction fee is too low",
				CompetingTxs: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"},
			},
			json: `{
				"txid":"8e5f2d1c9b0a7f6e5d4c3b2a1908070605040302010f0e0d0c0b0a0908070605",
				"txStatus":"REJECTED",
				"timestamp":"2024-01-15T10:30:00Z",
				"extraInfo":"arc error 465: transaction fee is too low",
				"competingTxs":["1111111111111111111111111111111111111111111111111111111111111111","2222222222222222222222222222222222222222222222222222222222222222"],
				"nextRetryAt":"0001-01-01T00:00:00Z",
				"merkleRegisteredAt":"0001-01-01T00:00:00Z"
			}`,
		},
		{
			name: "minimal_received_omitempty",
			record: TxRecord{
				TxID:      "deadbeef",
				Status:    StatusReceived,
				Timestamp: fixedTS,
			},
			// Only txid, txStatus, timestamp and the two (omitempty-ineffective)
			// time fields survive; every zero-valued omitempty field is dropped.
			json: `{
				"txid":"deadbeef",
				"txStatus":"RECEIVED",
				"timestamp":"2024-01-15T10:30:00Z",
				"nextRetryAt":"0001-01-01T00:00:00Z",
				"merkleRegisteredAt":"0001-01-01T00:00:00Z"
			}`,
		},
	}
}

// TestTxRecord_MarshalGolden marshals each fixture struct and asserts it equals
// the golden JSON.
func TestTxRecord_MarshalGolden(t *testing.T) {
	for _, f := range goldenFixtures() {
		t.Run(f.name, func(t *testing.T) {
			got, err := json.Marshal(f.record)
			require.NoError(t, err)
			assert.JSONEq(t, f.json, string(got))
		})
	}
}

// TestTxRecord_UnmarshalGolden unmarshals each golden JSON and asserts the
// resulting struct equals the fixture struct (full struct equality; time.Time
// zero and UTC values round-trip under require.Equal).
func TestTxRecord_UnmarshalGolden(t *testing.T) {
	for _, f := range goldenFixtures() {
		t.Run(f.name, func(t *testing.T) {
			var got TxRecord
			require.NoError(t, json.Unmarshal([]byte(f.json), &got))
			assert.Equal(t, f.record, got)
		})
	}
}

// TestTxRecord_RoundTrip proves each fixture survives a struct->JSON->struct
// round-trip unchanged.
func TestTxRecord_RoundTrip(t *testing.T) {
	for _, f := range goldenFixtures() {
		t.Run(f.name, func(t *testing.T) {
			b, err := json.Marshal(f.record)
			require.NoError(t, err)
			var back TxRecord
			require.NoError(t, json.Unmarshal(b, &back))
			assert.Equal(t, f.record, back)
		})
	}
}

// --- Transition lattice ---

// TestStatus_IsTerminal mirrors arcade's models TestStatus_IsTerminal: the
// terminal set is exactly REJECTED, DOUBLE_SPEND_ATTEMPTED, MINED, IMMUTABLE.
func TestStatus_IsTerminal(t *testing.T) {
	cases := map[Status]bool{
		StatusUnknown:              false,
		StatusReceived:             false,
		StatusSentToNetwork:        false,
		StatusAcceptedByNetwork:    false,
		StatusSeenOnNetwork:        false,
		StatusSeenMultipleNodes:    false,
		StatusPendingRetry:         false,
		StatusStumpProcessing:      false,
		StatusRejected:             true,
		StatusDoubleSpendAttempted: true,
		StatusMined:                true,
		StatusImmutable:            true,
	}
	for s, want := range cases {
		assert.Equalf(t, want, s.IsTerminal(), "IsTerminal(%s)", s)
	}
}

// TestCanSupersede_TerminalStaysTerminal: once DOUBLE_SPEND_ATTEMPTED / MINED /
// IMMUTABLE, no in-flight status may regress it. (REJECTED recovery is covered
// separately.) Mirrors arcade's TerminalStaysTerminal.
func TestCanSupersede_TerminalStaysTerminal(t *testing.T) {
	terminals := []Status{StatusDoubleSpendAttempted, StatusMined, StatusImmutable}
	regressions := []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusPendingRetry, StatusStumpProcessing,
	}
	for _, prev := range terminals {
		for _, next := range regressions {
			assert.Falsef(t, next.CanSupersede(prev), "%s should not supersede %s", next, prev)
		}
	}
}

// TestCanSupersede_RejectedRecovery: REJECTED is only a partial terminal —
// forward acceptance/seen states may recover it, but pre-broadcast and retry
// states may not. Mirrors arcade's RejectedRecovery.
func TestCanSupersede_RejectedRecovery(t *testing.T) {
	allowed := []Status{
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusMined, StatusImmutable,
	}
	for _, next := range allowed {
		assert.Truef(t, next.CanSupersede(StatusRejected), "REJECTED -> %s should be allowed", next)
	}
	blocked := []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusPendingRetry, StatusStumpProcessing,
	}
	for _, next := range blocked {
		assert.Falsef(t, next.CanSupersede(StatusRejected), "REJECTED -> %s should be blocked", next)
	}
}

// TestCanSupersede_ImmutableSink: IMMUTABLE is a true sink — nothing overwrites
// it except an idempotent IMMUTABLE. Mirrors arcade's Immutable test.
func TestCanSupersede_ImmutableSink(t *testing.T) {
	others := []Status{
		StatusUnknown, StatusReceived, StatusSentToNetwork,
		StatusAcceptedByNetwork, StatusSeenOnNetwork, StatusSeenMultipleNodes,
		StatusPendingRetry, StatusStumpProcessing,
		StatusRejected, StatusDoubleSpendAttempted, StatusMined,
	}
	for _, next := range others {
		assert.Falsef(t, next.CanSupersede(StatusImmutable), "%s must not supersede IMMUTABLE", next)
	}
	assert.True(t, StatusImmutable.CanSupersede(StatusImmutable), "IMMUTABLE -> IMMUTABLE is idempotent")
}

// TestCanSupersede_HappyPath spot-checks the forward edges the pipeline relies
// on, including the two lattice subtleties: PENDING_RETRY -> SENT_TO_NETWORK and
// MINED -> IMMUTABLE. Mirrors arcade's HappyPath.
func TestCanSupersede_HappyPath(t *testing.T) {
	allowed := []struct{ prev, next Status }{
		{"", StatusReceived},
		{StatusReceived, StatusSentToNetwork},
		{StatusSentToNetwork, StatusAcceptedByNetwork},
		{StatusAcceptedByNetwork, StatusSeenOnNetwork},
		{StatusSeenOnNetwork, StatusSeenMultipleNodes},
		{StatusSeenMultipleNodes, StatusMined},
		{StatusMined, StatusImmutable},
		{StatusSentToNetwork, StatusRejected},
		{StatusSeenOnNetwork, StatusDoubleSpendAttempted},
		{StatusSentToNetwork, StatusPendingRetry},
		{StatusPendingRetry, StatusSentToNetwork},
		{StatusReceived, StatusReceived},
		{StatusMined, StatusMined},
		{StatusRejected, StatusAcceptedByNetwork},
		{StatusRejected, StatusSeenOnNetwork},
		{StatusRejected, StatusSeenMultipleNodes},
	}
	for _, c := range allowed {
		assert.Truef(t, c.next.CanSupersede(c.prev), "%s -> %s should be allowed", c.prev, c.next)
	}
}

// TestCanSupersede_Regressions covers regressions that must be blocked. Mirrors
// arcade's Regressions test.
func TestCanSupersede_Regressions(t *testing.T) {
	regressions := []struct{ prev, next Status }{
		{StatusMined, StatusSeenOnNetwork},
		{StatusMined, StatusSeenMultipleNodes},
		{StatusMined, StatusPendingRetry},
		{StatusMined, StatusRejected},
		{StatusImmutable, StatusMined},
		{StatusImmutable, StatusSeenOnNetwork},
		{StatusRejected, StatusSentToNetwork},
		{StatusDoubleSpendAttempted, StatusSeenOnNetwork},
		{StatusSeenMultipleNodes, StatusSeenOnNetwork},
		{StatusAcceptedByNetwork, StatusSentToNetwork},
	}
	for _, c := range regressions {
		assert.Falsef(t, c.next.CanSupersede(c.prev), "%s -> %s should be blocked", c.prev, c.next)
	}
}

// --- Open-enum forward compatibility ---

// TestStatus_UnknownValue_OpenEnum pins the forward-compatibility contract for
// status values this package does not know (a future arcade may add statuses):
// they are non-terminal, they survive JSON round-trips verbatim, and the
// lattice treats them as unrestricted.
func TestStatus_UnknownValue_OpenEnum(t *testing.T) {
	future := Status("FUTURE_STATUS")

	// Unknown statuses are non-terminal.
	assert.False(t, future.IsTerminal())

	// Unknown statuses round-trip through JSON verbatim.
	rec := TxRecord{TxID: "deadbeef", Status: future, Timestamp: fixedTS}
	b, err := json.Marshal(rec)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"txStatus":"FUTURE_STATUS"`)
	var back TxRecord
	require.NoError(t, json.Unmarshal(b, &back))
	assert.Equal(t, future, back.Status)

	// An unknown status hits the lattice's default case (no banned previous
	// statuses), so it can supersede ANY prev — even IMMUTABLE. This mirrors
	// arcade's default-open lattice (models.Status.DisallowedPreviousStatuses
	// returns an empty set for statuses outside its switch), and is pinned here
	// deliberately: if arcade ever closes its default, this test must change
	// with it.
	for _, prev := range AllStatuses() {
		assert.Truef(t, future.CanSupersede(prev), "unknown status should supersede %s (default-open lattice)", prev)
	}
	assert.True(t, future.CanSupersede(StatusImmutable))
	assert.True(t, future.CanSupersede(""))
}
