package storage

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// logCapture is a slog.Handler that keeps every record, so a test can assert
// on what the provider logged. WithAttrs/WithGroup return the same handler:
// logging.Child attaches a service attr, and the capture must survive that.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

// find returns the first captured record at level whose message contains sub.
func (c *logCapture) find(level slog.Level, sub string) (slog.Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Level == level && strings.Contains(r.Message, sub) {
			return r, true
		}
	}
	return slog.Record{}, false
}

// intAttrs flattens a record's integer attributes into a name→value map.
func intAttrs(r slog.Record) map[string]int64 {
	out := make(map[string]int64)
	r.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindInt64 {
			out[a.Key] = a.Value.Int64()
		}
		return true
	})
	return out
}

// syncArgsAB is the source→target chunk request used by the spend-state tests.
func syncArgsAB() wdk.RequestSyncChunkArgs {
	return wdk.RequestSyncChunkArgs{
		FromStorageIdentityKey: "storage-identity-key",
		ToStorageIdentityKey:   "storage-b",
		IdentityKey:            testIdentityKey,
	}
}

// chunkOutput returns the chunk's output for (txid, vout).
func chunkOutput(t *testing.T, chunk *wdk.SyncChunk, txid string, vout uint32) *wdk.TableOutput {
	t.Helper()
	for _, o := range chunk.Outputs {
		if o == nil || o.TxID == nil {
			continue
		}
		if *o.TxID == txid && o.Vout == vout {
			return o
		}
	}
	t.Fatalf("chunk has no output %s:%d", txid, vout)
	return nil
}

// TestSync_SpentAtSource_IsNotMinted is the P1-2 double-spend guard: a change
// coin the SOURCE already spent must ship as non-spendable, carry its spend
// history, and must never be resurrected as claimable inventory on the target.
func TestSync_SpentAtSource_IsNotMinted(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)

	// A mined wallet payment lands as a claimable change coin...
	fundedTxID := a.internalizeMinedPayment(t, 0xD1, 50_000)

	// ...which the source then spends in a broadcast transaction.
	res, err := a.p.CreateAction(ctx, a.auth, paymentArgs(10_000))
	require.NoError(t, err)
	signed := buildSignedTx(t, res)
	spendTxID := signed.TxID().String()
	_, err = a.p.ProcessAction(ctx, a.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(spendTxID),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	chunk, err := a.p.GetSyncChunk(ctx, syncArgsAB())
	require.NoError(t, err)

	// (b) the wire carries the source's spend state.
	spent := chunkOutput(t, chunk, fundedTxID, 0)
	assert.False(t, spent.Spendable, "a coin spent at the source is not spendable")
	require.NotNil(t, spent.SpentBy, "the chunk carries the source-local spending transaction id")

	// (a) the receiver must not mint it.
	b := newSyncTargetProvider(t)
	_, err = b.p.ProcessSyncChunk(ctx, syncArgsAB(), chunk)
	require.NoError(t, err)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}

	// The spent coin must not exist as claimable inventory on the target: this
	// is the double spend the audit found — the source has already committed
	// that input to a broadcast transaction.
	listed, err := b.p.ListOutputs(ctx, bAuth, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange),
		Limit:  10,
	})
	require.NoError(t, err)
	var seen bool
	for _, o := range listed.Outputs {
		if o.Outpoint == primitives.NewOutpointString(fundedTxID, 0) {
			seen = true
			assert.False(t, o.Spendable, "a coin spent at the source is never minted on the target")
		}
	}
	require.True(t, seen, "the descriptive row still synced across")

	// Only the source's own live change coin rebuilds, so the target's balance
	// matches the source's.
	srcBal, err := a.p.GetBalance(ctx, a.auth, "")
	require.NoError(t, err)
	require.NotZero(t, srcBal, "precondition: the source still holds its change")
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	assert.Equal(t, srcBal, bal, "the target rebuilds exactly the source's live inventory")

	// The descriptive row still exists, with spent_by translated into the
	// target's own transaction id space.
	rows, err := b.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{UserID: &uid, TxID: &fundedTxID})
	require.NoError(t, err)
	require.Len(t, rows, 1, "history is preserved on the target")
	require.NotNil(t, rows[0].SpentBy, "spent_by survives the round-trip")

	spendRow, found, err := b.meta.Transactions().FindByReference(ctx, uid, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, spendRow.TransactionID, *rows[0].SpentBy,
		"spent_by is translated through the chunk's old→new transaction map")
}

// TestSync_ReservedAtSource_IsNotMinted: a coin held by an in-flight
// reservation at the source is mid-spend there, so the target must treat it as
// non-spendable rather than hand a second transaction the same input.
func TestSync_ReservedAtSource_IsNotMinted(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)

	fundedTxID := a.internalizeMinedPayment(t, 0xD2, 50_000)
	_, err := a.p.CreateAction(ctx, a.auth, paymentArgs(10_000)) // reserves, never signs
	require.NoError(t, err)

	chunk, err := a.p.GetSyncChunk(ctx, syncArgsAB())
	require.NoError(t, err)

	reserved := chunkOutput(t, chunk, fundedTxID, 0)
	assert.False(t, reserved.Spendable, "a reserved coin is conservatively non-spendable")
	assert.Nil(t, reserved.SpentBy, "reserved is not spent")

	b := newSyncTargetProvider(t)
	_, err = b.p.ProcessSyncChunk(ctx, syncArgsAB(), chunk)
	require.NoError(t, err)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bal, "a reserved-at-source coin is not claimable on the target")
}

// TestSync_UnspentChange_IsStillMinted pins the green path: a live, claimable
// change coin at the source still rebuilds inventory on the target.
func TestSync_UnspentChange_IsStillMinted(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)
	fundedTxID := a.internalizeMinedPayment(t, 0xD3, 33_000)

	chunk, err := a.p.GetSyncChunk(ctx, syncArgsAB())
	require.NoError(t, err)
	live := chunkOutput(t, chunk, fundedTxID, 0)
	assert.True(t, live.Spendable, "a live coin ships as spendable")
	assert.Nil(t, live.SpentBy)

	b := newSyncTargetProvider(t)
	_, err = b.p.ProcessSyncChunk(ctx, syncArgsAB(), chunk)
	require.NoError(t, err)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(33_000), bal, "inventory is still rebuilt for live coins")
}

// TestSync_UntranslatableSpentBy_StaysNullAndUnminted: when the spending
// transaction is not part of the chunk the id cannot be translated, so the
// history is dropped (NULL) — but the coin is STILL not minted, because
// spent-at-source is decided from the chunk value, not from the translation.
func TestSync_UntranslatableSpentBy_StaysNullAndUnminted(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)
	fundedTxID := a.internalizeMinedPayment(t, 0xD4, 44_000)

	chunk, err := a.p.GetSyncChunk(ctx, syncArgsAB())
	require.NoError(t, err)
	out := chunkOutput(t, chunk, fundedTxID, 0)
	require.True(t, out.Spendable, "precondition: the source considers it live")
	ghost := uint(999_999) // a source-local id that is in no chunk transaction
	out.SpentBy = &ghost

	b := newSyncTargetProvider(t)
	_, err = b.p.ProcessSyncChunk(ctx, syncArgsAB(), chunk)
	require.NoError(t, err)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bal, "a spent-at-source coin is never minted, translatable or not")

	rows, err := b.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{UserID: &uid, TxID: &fundedTxID})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Nil(t, rows[0].SpentBy, "an untranslatable spending id is stored as NULL")
}

// TestSync_OldSourceChunk_WarnsThatNothingWasRebuilt: a chunk from a storage
// that predates the spendability projection ships Spendable=false for every
// output, so the receiver rebuilds nothing. That is the safe direction, but it
// is silent and NOT self-healing (re-syncing the same target will not repair
// it), so ProcessSyncChunk must say so out loud.
func TestSync_OldSourceChunk_WarnsThatNothingWasRebuilt(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)
	fundedTxID := a.internalizeMinedPayment(t, 0xD5, 55_000)

	chunk, err := a.p.GetSyncChunk(ctx, syncArgsAB())
	require.NoError(t, err)
	live := chunkOutput(t, chunk, fundedTxID, 0)
	require.True(t, live.Spendable, "precondition: this source does project spendability")
	// Downgrade the chunk to what a pre-change source would have sent.
	for _, o := range chunk.Outputs {
		o.Spendable = false
	}

	capture := &logCapture{}
	b := newSyncTargetProviderWithLogger(t, slog.New(capture))
	_, err = b.p.ProcessSyncChunk(ctx, syncArgsAB(), chunk)
	require.NoError(t, err)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	require.Equal(t, uint64(0), bal, "an old-source chunk rebuilds no inventory")

	rec, found := capture.find(slog.LevelWarn, "rebuilt no spendable inventory")
	require.True(t, found, "the silent-empty-rebuild case must warn")

	counters := intAttrs(rec)
	assert.Equal(t, int64(1), counters["changeCandidates"], "the chunk did carry a change coin")
	assert.Equal(t, int64(0), counters["minted"])
	assert.Equal(t, int64(0), counters["skippedNoParentTx"], "the parent tx was inserted; only the mint gate refused")
	assert.Positive(t, counters["inserts"])
}
