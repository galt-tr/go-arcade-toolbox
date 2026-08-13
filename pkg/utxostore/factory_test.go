package utxostore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

func TestFactory_OpenDispatch(t *testing.T) {
	marker := errors.New("factory-test-marker")
	var gotDSN string
	utxostore.Register("factorytest", func(_ context.Context, dsn string) (utxostore.Store, error) {
		gotDSN = dsn
		return nil, marker
	})

	_, err := utxostore.Open(context.Background(), "factorytest://host:1/ns?set=x")
	require.ErrorIs(t, err, marker, "Open must dispatch to the registered opener")
	require.Equal(t, "factorytest://host:1/ns?set=x", gotDSN)
}

func TestFactory_UnknownScheme(t *testing.T) {
	_, err := utxostore.Open(context.Background(), "no-such-scheme://host/ns")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no-such-scheme")
	require.Contains(t, err.Error(), "no provider registered")
}

func TestFactory_RegisterGuards(t *testing.T) {
	require.Panics(t, func() { utxostore.Register("", nil) })
	require.Panics(t, func() { utxostore.Register("x", nil) })

	utxostore.Register("dupscheme", func(context.Context, string) (utxostore.Store, error) { return nil, nil })
	require.Panics(t, func() {
		utxostore.Register("dupscheme", func(context.Context, string) (utxostore.Store, error) { return nil, nil })
	}, "duplicate scheme registration must panic")
}
