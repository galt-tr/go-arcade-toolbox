package storage

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// ARC status codes arcade maps from Teranode failure lines (see arcade
// classifyFailureLine). 466 is StatusConflict (TX_CONFLICTING).
const arcStatusConflict = 466

// spendConflictInfo is the wallet-facing interpretation of an Arcade REJECTED
// (or DOUBLE_SPEND) record that asserts some of our inputs are already gone.
//
// Arcade often surfaces spend conflicts as pure REJECTED with ExtraInfo like:
//
//	UTXO_SPENT (70): … <txid>:<vout> utxo already spent by tx <spender>[0]
//
// rather than DOUBLE_SPEND_ATTEMPTED + CompetingTxs. Blind two-pass release of
// ALL inputs would resurrect already-spent outpoints into the claimable pool
// (reject churn / false balance). This struct drives a winner-union-style
// partial release instead.
type spendConflictInfo struct {
	// Conflict is true when ExtraInfo / StatusCode indicates a spend-conflict
	// class rejection (not a pure policy/script reject where inputs stay free).
	Conflict bool
	// Spent is the set of outpoints the oracle text asserts are already spent.
	// Empty does not mean "none spent" when Conflict is true — it means we
	// could not parse which ones (caller must not free everything).
	Spent map[utxostore.Outpoint]struct{}
	// Spenders are competing/spending txids mentioned in ExtraInfo (or taken
	// from CompetingTxs by the caller). Used for winner-union when those txs
	// are themselves terminal-successful with readable rawTx.
	Spenders []string
}

// outpointInText matches a 64-hex txid : vout pair (the Teranode UTXO_SPENT
// shape: "256fc714…:2 utxo already spent…").
var outpointInText = regexp.MustCompile(`(?i)\b([0-9a-f]{64}):(\d+)\b`)

// spentByTxInText matches "spent by tx <64-hex>" (Teranode UTXO_SPENT prose).
var spentByTxInText = regexp.MustCompile(`(?i)spent\s+by\s+tx\s+([0-9a-f]{64})`)

// teranodeCodePrefix matches a leading "NAME (n):" Teranode code on a line.
var teranodeCodePrefix = regexp.MustCompile(`(?i)^\s*([A-Z_]+)\s*\(\d+\)\s*:`)

// isSpendConflictRecord reports whether an Arcade record should be treated as
// a spend-conflict (partial-release) rather than a pure invalid-tx (release all
// after two-pass). CompetingTxs alone count: Arcade may populate them without
// renaming the status to DOUBLE_SPEND_ATTEMPTED.
func isSpendConflictRecord(rec *arcade.TxRecord) bool {
	if rec == nil {
		return false
	}
	if len(rec.CompetingTxs) > 0 {
		return true
	}
	if rec.StatusCode == arcStatusConflict {
		return true
	}
	return parseSpendConflictExtra(rec.ExtraInfo).Conflict
}

// parseSpendConflictExtra interprets Arcade ExtraInfo for spend-conflict class
// and extracts asserted spent outpoints + spender txids. Safe on empty input.
func parseSpendConflictExtra(extra string) spendConflictInfo {
	info := spendConflictInfo{
		Spent: make(map[utxostore.Outpoint]struct{}),
	}
	if strings.TrimSpace(extra) == "" {
		return info
	}
	info.Conflict = extraLooksLikeSpendConflict(extra)

	// Outpoints mentioned anywhere in the text (typically the spent UTXO).
	for _, m := range outpointInText.FindAllStringSubmatch(extra, -1) {
		h, err := chainhash.NewHashFromHex(m[1])
		if err != nil {
			continue
		}
		// Parse with an explicit 32-bit bound. The hand-rolled accumulator this
		// replaces wrapped silently: the regex admits up to 10 digits, so e.g.
		// ":4294967296" folded to vout 0 and asserted the WRONG outpoint spent —
		// holding an input the conflict never named while leaving the real one
		// releasable. A vout that does not fit is not a real outpoint reference,
		// so skip it rather than invent one.
		vout64, perr := strconv.ParseUint(m[2], 10, 32)
		if perr != nil {
			continue
		}
		info.Spent[utxostore.Outpoint{TxID: *h, Vout: uint32(vout64)}] = struct{}{}
	}

	// "spent by tx <txid>" spenders.
	seen := make(map[string]struct{})
	for _, m := range spentByTxInText.FindAllStringSubmatch(extra, -1) {
		id := strings.ToLower(m[1])
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		info.Spenders = append(info.Spenders, id)
	}
	return info
}

// extraLooksLikeSpendConflict is the keyword / code classifier for ExtraInfo.
func extraLooksLikeSpendConflict(extra string) bool {
	u := strings.ToUpper(extra)
	// Explicit Teranode / prose markers.
	if strings.Contains(u, "UTXO_SPENT") ||
		strings.Contains(u, "TX_CONFLICTING") ||
		strings.Contains(u, "TX_INVALID_DOUBLE_SPEND") ||
		strings.Contains(u, "ALREADY SPENT") ||
		strings.Contains(u, "DOUBLE SPEND") ||
		strings.Contains(u, "DOUBLE_SPEND") {
		return true
	}
	// Per-line leading codes (multi-line ExtraInfo from aggregated peers).
	for _, line := range strings.Split(extra, "\n") {
		m := teranodeCodePrefix.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		switch strings.ToUpper(m[1]) {
		case "UTXO_SPENT", "TX_CONFLICTING", "TX_INVALID_DOUBLE_SPEND":
			return true
		case "TX_INVALID":
			// TX_INVALID often wraps "because UTXO_SPENT" — covered above;
			// bare TX_INVALID is policy/script and not conflict-class.
		}
	}
	return false
}

// mergeSpenders de-duplicates CompetingTxs + ExtraInfo spenders (lowercase hex).
func mergeSpenders(competing []string, fromExtra []string) []string {
	seen := make(map[string]struct{}, len(competing)+len(fromExtra))
	var out []string
	add := func(ids []string) {
		for _, id := range ids {
			id = strings.ToLower(strings.TrimSpace(id))
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	add(competing)
	add(fromExtra)
	return out
}

// filterReleaseInputs returns inputs safe to free: those not in the spent set.
// Order of allInputs is preserved.
func filterReleaseInputs(all []utxostore.Outpoint, spent map[utxostore.Outpoint]struct{}) (release []utxostore.Outpoint, held int) {
	if len(spent) == 0 {
		return append([]utxostore.Outpoint(nil), all...), 0
	}
	release = make([]utxostore.Outpoint, 0, len(all))
	for _, op := range all {
		if _, bad := spent[op]; bad {
			held++
			continue
		}
		release = append(release, op)
	}
	return release, held
}
