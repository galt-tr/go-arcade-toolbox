package arcade

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubOracle is a no-op TxOracle used only to prove the interface is coherent
// and implementable at compile time. The real implementation lands in M3.1.
type stubOracle struct{}

func (stubOracle) Broadcast(context.Context, string, []byte) (*BroadcastResult, error) {
	return nil, nil
}
func (stubOracle) GetTx(context.Context, string) (*TxRecord, error) { return nil, nil }
func (stubOracle) StreamStatus(context.Context, string, func(StatusEvent) error) error {
	return nil
}
func (stubOracle) Health(context.Context) (*Health, error) { return nil, nil }

// Compile-time assertion that the contract is implementable.
var _ TxOracle = stubOracle{}

// TestErrTxNotFound pins the portable not-found contract: implementations wrap
// ErrTxNotFound and consumers match it with errors.Is.
func TestErrTxNotFound(t *testing.T) {
	wrapped := fmt.Errorf("GET /tx/%s: %w", "deadbeef", ErrTxNotFound)
	assert.True(t, errors.Is(wrapped, ErrTxNotFound))
	assert.False(t, errors.Is(errors.New("arcade: transaction not found"), ErrTxNotFound),
		"matching must be by identity (errors.Is), not by message string")
}

// TestBackpressureError verifies it satisfies error, carries RetryAfter, and is
// discoverable via errors.As so callers can branch on backpressure.
func TestBackpressureError(t *testing.T) {
	var err error = &BackpressureError{RetryAfter: time.Second}

	assert.Contains(t, err.Error(), "backpressure")
	assert.Contains(t, err.Error(), "1s")

	var bp *BackpressureError
	require.True(t, errors.As(err, &bp))
	assert.Equal(t, time.Second, bp.RetryAfter)
}
