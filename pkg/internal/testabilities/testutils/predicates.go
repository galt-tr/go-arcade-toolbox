package testutils

import "github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"

// ProvidedByYouCondition is ported from go-wallet-toolbox (see upstream docs).
func ProvidedByYouCondition(p *wdk.StorageCreateTransactionSdkOutput) bool {
	return p.ProvidedBy == wdk.ProvidedByYou
}

// ProvidedByStorageCondition is ported from go-wallet-toolbox (see upstream docs).
func ProvidedByStorageCondition(p *wdk.StorageCreateTransactionSdkOutput) bool {
	return p.ProvidedBy == wdk.ProvidedByStorage
}

// CommissionOutputCondition is ported from go-wallet-toolbox (see upstream docs).
func CommissionOutputCondition(p *wdk.StorageCreateTransactionSdkOutput) bool {
	return p.Purpose == "storage-commission"
}
