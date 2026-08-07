package storage_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// BenchmarkCreateProcess is a provider-level throughput smoke test over the
// full CreateAction -> ProcessAction (accept) pipeline against memstore +
// SQLite — a cheap early signal, not a substitute for the real M6 perf
// harness. It also does NOT duplicate pkg/utxostore/sqlstore's claim EXPLAIN
// regression guard or claim micro-bench (Task 8): those cover the
// claim-selection hot path in isolation; this covers the whole provider
// round-trip a wallet client actually drives.
func BenchmarkCreateProcess(b *testing.B) {
	ctx := context.Background()
	p := newMemstoreSQLiteProvider(b, &conformance.FakeHeaders{})
	if _, err := p.Migrate(ctx, "bench", "bench-identity-key"); err != nil {
		b.Fatal(err)
	}
	resp, err := p.FindOrInsertUser(ctx, conformance.NewIdentityKey(b))
	if err != nil {
		b.Fatal(err)
	}
	uid := resp.User.UserID
	auth := wdk.AuthID{IdentityKey: "bench-user", UserID: &uid}
	sender := conformance.NewIdentityKey(b)

	// Seed enough distinct, individually-sufficient mined coins for b.N
	// payments (one CreateAction+ProcessAction round-trip per coin).
	const denomination = 1_000_000
	sats := make([]uint64, b.N)
	for i := range sats {
		sats[i] = denomination
	}
	atomic, _ := conformance.BuildMinedAtomicBEEF(b, 0x70, 950_000, sats...)
	outs := make([]*wdk.InternalizeOutput, b.N)
	for i := range outs {
		outs[i] = conformance.WalletPaymentOutput(uint32(i), sender) //nolint:gosec // i < b.N, small
	}
	if _, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: outs,
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(500_000))
		if err != nil {
			b.Fatal(err)
		}
		signed := conformance.BuildSignedTx(b, res)
		txid := signed.TxID().String()
		if _, err := p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
			IsNewTx:   true,
			Reference: strPtrBench(res.Reference),
			TxID:      txidPtrBench(txid),
			RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func strPtrBench(s string) *string { return &s }

func txidPtrBench(s string) *primitives.TXIDHexString {
	v := primitives.TXIDHexString(s)
	return &v
}
