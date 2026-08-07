package defs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAerospikeValidate(t *testing.T) {
	ok := DefaultAerospikeConfig()
	require.NoError(t, ok.Validate())

	require.Error(t, (&Aerospike{Namespace: "n"}).Validate())            // no host
	require.Error(t, (&Aerospike{Hosts: []string{"h:3000"}}).Validate()) // no namespace
}

func TestAerospikeDSN(t *testing.T) {
	a := Aerospike{Hosts: []string{"db:3100"}, Namespace: "wallet", Set: "coins"}
	require.Equal(t, "aerospike://db:3100/wallet?set=coins", a.DSN())

	// Empty set defaults to utxos; credentials are embedded.
	a = Aerospike{Hosts: []string{"h"}, Namespace: "ns", User: "u", Password: "p"}
	require.Equal(t, "aerospike://u:p@h/ns?set=utxos", a.DSN())
}
