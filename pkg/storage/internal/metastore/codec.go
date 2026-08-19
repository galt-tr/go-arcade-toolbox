package metastore

import (
	"database/sql"
	"encoding/hex"
	"fmt"
)

// nullStr converts a scanned nullable string column to a *string (nil on NULL).
func nullStr(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	v := n.String
	return &v
}

// strPtrArg encodes an optional string for a bound parameter: NULL for a nil
// (or empty) pointer, else the string value.
func strPtrArg(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// uintPtrArg encodes an optional unsigned id for a bound parameter: NULL for a
// nil pointer, else the value. Used for nullable FK columns such as
// outputs.spent_by.
func uintPtrArg(p *uint) any {
	if p == nil {
		return nil
	}
	return *p
}

// bytesArg encodes an optional byte slice for a bound parameter: NULL for an
// empty/nil slice, else the bytes.
func bytesArg(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

// encTxID decodes a required display-hex txid string into the raw bytes stored
// in the BYTEA/BLOB txid columns. The decode round-trips exactly with
// decTxIDPtr; the metastore keeps txids internally consistent and never joins
// them against the utxostore's chainhash-encoded txids, so no byte-order
// reversal is applied.
func encTxID(txid string) ([]byte, error) {
	b, err := hex.DecodeString(txid)
	if err != nil {
		return nil, fmt.Errorf("metastore: decode txid %q: %w", txid, err)
	}
	return b, nil
}

// decTxIDPtr encodes stored txid bytes back to a display-hex string pointer;
// nil bytes yield a nil pointer.
func decTxIDPtr(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := hex.EncodeToString(b)
	return &s
}
