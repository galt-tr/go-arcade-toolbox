package storage

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

func TestParseSpendConflictExtra_UTXOSpentProductionShape(t *testing.T) {
	// Live Arcade/Teranode shape after propagation surfaces non-500 bodies.
	extra := "UTXO_SPENT (70): UTXO_SPENT (70): 256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43:2 utxo already spent by tx 7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489[0]\n"
	info := parseSpendConflictExtra(extra)
	require.True(t, info.Conflict)
	require.Len(t, info.Spent, 1)
	require.Len(t, info.Spenders, 1)
	assert.Equal(t, "7dfbe0fcacf630066b23036bc007e73d58a61ab9bcb8e66f6b214f8ef5f28489", info.Spenders[0])

	wantTx, err := chainhash.NewHashFromHex("256fc7143c6ef5436cd5258ade24b68c43d0f8268c086bd9fa90055dd5a7ae43")
	require.NoError(t, err)
	_, ok := info.Spent[utxostore.Outpoint{TxID: *wantTx, Vout: 2}]
	assert.True(t, ok, "spent outpoint must be parsed")
}

func TestParseSpendConflictExtra_PurePolicyReject(t *testing.T) {
	info := parseSpendConflictExtra("TX_INVALID (31): [ProcessTransaction][aa] fee too low")
	assert.False(t, info.Conflict, "bare fee/script reject is not spend-conflict class")
	assert.Empty(t, info.Spent)
}

func TestParseSpendConflictExtra_AlreadySpentProse(t *testing.T) {
	info := parseSpendConflictExtra("tx is invalid because UTXO_SPENT")
	assert.True(t, info.Conflict)
}

func TestIsSpendConflictRecord_CompetingTxs(t *testing.T) {
	assert.True(t, isSpendConflictRecord(&arcade.TxRecord{
		Status:       arcade.StatusRejected,
		CompetingTxs: []string{"aa"},
	}))
	assert.True(t, isSpendConflictRecord(&arcade.TxRecord{
		Status:     arcade.StatusRejected,
		StatusCode: arcStatusConflict,
	}))
	assert.False(t, isSpendConflictRecord(&arcade.TxRecord{
		Status:    arcade.StatusRejected,
		ExtraInfo: "policy violation",
	}))
}

func TestFilterReleaseInputs(t *testing.T) {
	a := utxostore.Outpoint{TxID: chainhash.Hash{1}, Vout: 0}
	b := utxostore.Outpoint{TxID: chainhash.Hash{2}, Vout: 0}
	c := utxostore.Outpoint{TxID: chainhash.Hash{3}, Vout: 0}
	spent := map[utxostore.Outpoint]struct{}{b: {}}
	rel, held := filterReleaseInputs([]utxostore.Outpoint{a, b, c}, spent)
	assert.Equal(t, 1, held)
	assert.Equal(t, []utxostore.Outpoint{a, c}, rel)
}
