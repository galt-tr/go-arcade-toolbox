// Package actions is ported from go-wallet-toolbox (see upstream docs).
package actions

import (
	"context"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// WalletStorageCreateAndProcessAction is ported from go-wallet-toolbox (see upstream docs).
type WalletStorageCreateAndProcessAction interface {
	WalletStorageCreateAction
	WalletStorageProcessAction
}

// WalletStorageCreateAction is ported from go-wallet-toolbox (see upstream docs).
type WalletStorageCreateAction interface {
	CreateAction(ctx context.Context, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error)
}

// WalletStorageProcessAction is ported from go-wallet-toolbox (see upstream docs).
type WalletStorageProcessAction interface {
	ProcessAction(ctx context.Context, args wdk.ProcessActionArgs) (*wdk.ProcessActionResult, error)
}
