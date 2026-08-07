package testhelper

import ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

// IdentityKeyFromHex is ported from go-wallet-toolbox (see upstream docs).
func IdentityKeyFromHex(hex string) *ec.PublicKey {
	result, err := ec.PublicKeyFromString(hex)
	if err != nil {
		panic(err)
	}
	return result
}
