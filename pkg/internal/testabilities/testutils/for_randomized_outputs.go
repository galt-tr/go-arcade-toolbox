package testutils

import (
	"slices"
	"testing"

	"github.com/go-softwarelab/common/pkg/seq"
	"github.com/go-softwarelab/common/pkg/types"
	"github.com/stretchr/testify/require"
)

// FindOutput is ported from go-wallet-toolbox (see upstream docs).
func FindOutput[T any](
	t *testing.T,
	outputs []*T,
	finder func(p *T) bool,
) (*T, uint32) {
	t.Helper()
	index := slices.IndexFunc(outputs, finder)
	require.GreaterOrEqual(t, index, 0)

	return outputs[index], uint32(index) //nolint:gosec // index is always a valid slice index, fits in uint32
}

// CountOutputsWithCondition is ported from go-wallet-toolbox (see upstream docs).
func CountOutputsWithCondition[T any](
	t *testing.T,
	outputs []*T,
	finder func(p *T) bool,
) int {
	t.Helper()

	return seq.Count(seq.Filter(seq.FromSlice(outputs), finder))
}

// SumOutputsWithCondition is ported from go-wallet-toolbox (see upstream docs).
func SumOutputsWithCondition[T any, S types.Number](
	t *testing.T,
	outputs []*T,
	getter func(p *T) S,
	finder func(p *T) bool,
) S {
	t.Helper()

	var sum S
	for _, output := range outputs {
		if finder(output) {
			sum += getter(output)
		}
	}
	return sum
}

// ForEveryOutput is ported from go-wallet-toolbox (see upstream docs).
func ForEveryOutput[T any](
	t *testing.T,
	outputs []T,
	finder func(p T) bool,
	validator func(p T),
) {
	t.Helper()

	for _, output := range outputs {
		if finder(output) {
			validator(output)
		}
	}
}
