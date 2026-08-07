// Package register wires the Aerospike provider into the utxostore factory. A
// binary opts into the Aerospike backend — and only then links the Aerospike
// client dependency — with a blank import:
//
//	import _ "github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/aerostore/register"
//
// After that, utxostore.Open("aerospike://host:port/namespace?set=utxos")
// returns an Aerospike-backed store. There are no build tags: the linker prunes
// the Aerospike client from binaries that never import this package.
package register

import (
	"context"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/aerostore"
)

func init() {
	utxostore.Register("aerospike", func(ctx context.Context, dsn string) (utxostore.Store, error) {
		s, err := aerostore.Open(ctx, dsn)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
}
