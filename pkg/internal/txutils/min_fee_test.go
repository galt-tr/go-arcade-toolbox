package txutils_test

import (
	"testing"

	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/txutils"
)

func TestMinRequiredFee(t *testing.T) {
	tests := []struct {
		name    string
		size    uint64
		satPerK int64
		want    uint64
	}{
		// The node truncates. A wallet that rounds up pays at or above the floor;
		// one that rounds down by even a satoshi is refused outright, which is why
		// this has to match rather than approximate.
		{"truncates rather than rounds", 226, 100, 22},
		{"exact multiple", 1000, 100, 100},
		{"one byte over a multiple still truncates", 1009, 100, 100},

		// A non-empty transaction is never free while a floor is in force.
		{"tiny transaction still owes one satoshi", 9, 100, 1},
		{"one byte at one sat per kB owes one satoshi", 1, 1, 1},

		// No floor configured, or nothing to charge for.
		{"no floor", 5000, 0, 0},
		{"negative rate is treated as no floor", 5000, -1, 0},
		{"empty transaction", 0, 100, 0},

		// Measured against arcade's BDK engine: a 1-input transaction with a
		// 1000-byte source locking script serializes to 73 standard bytes (1090 in
		// extended format) and is accepted at 100 sat/kB with a fee of 7. The
		// extended-format size is NOT what is billed; this case pins that.
		{"bdk-verified: standard size is what is priced", 73, 100, 7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := txutils.MinRequiredFee(tc.size, tc.satPerK); got != tc.want {
				t.Errorf("MinRequiredFee(%d, %d) = %d, want %d", tc.size, tc.satPerK, got, tc.want)
			}
		})
	}
}
