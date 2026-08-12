package txtrace

import (
	"context"
	"testing"
)

func TestWithSampleRoundTrip(t *testing.T) {
	if Sampled(context.Background()) {
		t.Fatal("bare context must not be sampled")
	}
	ctx := WithSample(context.Background())
	if !Sampled(ctx) {
		t.Fatal("WithSample context must report Sampled")
	}
	// A child context keeps the flag.
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	if !Sampled(child) {
		t.Fatal("child of a sampled context must remain sampled")
	}
}

func TestMarkMarked(t *testing.T) {
	const txid = "0000000000000000000000000000000000000000000000000000000000000abc"
	if Marked(txid) {
		t.Fatal("unmarked txid must report false")
	}
	Mark(txid)
	if !Marked(txid) {
		t.Fatal("marked txid must report true")
	}
	if Marked("deadbeef") {
		t.Fatal("a different txid must not be reported as marked")
	}
}

func TestEmitNilLoggerNoPanic(_ *testing.T) {
	// Must be a safe no-op so callers need not nil-check.
	Emit(nil, "created", "abc", "k", "v")
}
