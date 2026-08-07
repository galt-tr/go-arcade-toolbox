package aerostore

import (
	"encoding/binary"
	"math/bits"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

// bucketCount is the number of best-fit buckets: bucket = floor(log2(sats))
// over uint64, so the largest possible bucket is 63. A denominated pool (every
// coin the same value) collapses to a single bucket, which is what makes
// ClaimExact a pure secondary-index hit; an arbitrary-amount pool spreads
// across buckets that the claim walk visits smallest- or largest-first.
const bucketCount = 64

// bucketOf returns floor(log2(sats)), the best-fit bucket for a coin of the
// given value. sats == 0 maps to bucket 0 (no coin should be zero-valued, but
// the function stays total). The result is always in [0, 63].
func bucketOf(sats uint64) int {
	if sats == 0 {
		return 0
	}
	return bits.Len64(sats) - 1
}

// outpointKey encodes an outpoint into the 36-byte record-key value used to
// derive the Aerospike digest: txid (32 bytes, internal byte order) followed by
// vout as 4-byte big-endian. Two distinct outpoints never collide, and the same
// outpoint always maps to the same digest (so Get/Spend/Remove are exact
// single-record lookups without a secondary index).
func outpointKey(op utxostore.Outpoint) []byte {
	b := make([]byte, 36)
	copy(b[:32], op.TxID[:])
	binary.BigEndian.PutUint32(b[32:], op.Vout)
	return b
}

// claimKeyFor builds the claimKey bin value "{userId}|{basket}|{tier}|{bucket}".
// The bin is present ONLY while a coin is claimable (unreserved, unspent, not
// frozen); it is removed atomically on reserve/spend/freeze and restored on
// release/unspend/unfreeze. A claim is therefore a secondary-index equality
// probe on this exact string for the requested (user, basket, tier, bucket).
//
// Basket names are wallet identifiers ("default", "fuel", …) and are assumed
// free of the '|' separator; the store never mixes users because userId is the
// leading, numeric component.
func claimKeyFor(userID int64, basket string, tier utxostore.Tier, bucket int) string {
	var sb strings.Builder
	sb.WriteString(strconv.FormatInt(userID, 10))
	sb.WriteByte('|')
	sb.WriteString(basket)
	sb.WriteByte('|')
	sb.WriteString(strconv.Itoa(int(tier)))
	sb.WriteByte('|')
	sb.WriteString(strconv.Itoa(bucket))
	return sb.String()
}

// invKeyFor builds the invKey bin value "{userId}|{basket}", present until a row
// is spent (or removed). It backs Balance and any per-(user, basket) inventory
// scan via a string-equality secondary index.
func invKeyFor(userID int64, basket string) string {
	return strconv.FormatInt(userID, 10) + "|" + basket
}
