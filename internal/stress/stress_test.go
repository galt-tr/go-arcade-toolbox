package stress_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/galt-tr/go-arcade-toolbox/internal/stress"
)

func TestParseFactor(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want int
	}{
		{"unset means no scaling", "", 1},
		{"explicit 1", "1", 1},
		{"scales up", "20", 20},
		{"large factor", "1000", 1000},

		// A soak knob must never scale a test DOWN. A concurrency test that
		// runs zero rounds passes while asserting nothing, which is strictly
		// worse than ignoring a bad value — so every non-positive or malformed
		// input falls back to 1 rather than being taken literally.
		{"zero is ignored", "0", 1},
		{"negative is ignored", "-5", 1},
		{"malformed is ignored", "not-a-number", 1},
		{"float is ignored", "2.5", 1},
		{"whitespace is ignored", " 4 ", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, stress.ParseFactor(tc.raw),
				"%s=%q", stress.EnvVar, tc.raw)
		})
	}
}

func TestScale(t *testing.T) {
	// Factor reads the ambient environment, so assert Scale's shape relative to
	// it rather than pinning an absolute number — this test must pass whether
	// or not the run itself was invoked under ARCADE_STRESS.
	f := stress.Factor()
	assert.GreaterOrEqual(t, f, 1, "the factor must never drop below 1")

	assert.Equal(t, 12*f, stress.Scale(12))
	assert.Equal(t, 1*f, stress.Scale(1))

	// Non-positive inputs pass through unscaled so a caller's own "disabled"
	// sentinel is not silently turned into something else.
	assert.Equal(t, 0, stress.Scale(0))
	assert.Equal(t, -1, stress.Scale(-1))
}
