package storage

import (
	"context"
	"fmt"
	"log/slog"
	stdslices "slices"

	"github.com/go-softwarelab/common/pkg/is"
	"github.com/go-softwarelab/common/pkg/to"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/managed"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

var _ wdk.WalletStorage = (*WalletStorageManager)(nil)

// WalletStorageManager binds a single active [wdk.WalletStorageProvider] to a
// user identity key and exposes the auth-bound [wdk.WalletStorage] interface the
// wallet consumes: it resolves the user via MakeAvailable/FindOrInsertUser and
// injects the resolved [wdk.AuthID] into every write/read call.
//
// NOTE (M3 scope): this is the single-active-store manager. The go-wallet-toolbox
// original also drives multi-store backup partitioning and BRC-40 reader→writer
// sync (SyncToWriter, conflicting-active resolution). Those depend on the storage
// sync subsystem, which is deferred to a later milestone; AddWalletStorageProvider
// and SetActive here handle only the single-active-store case.
type WalletStorageManager struct {
	isAvailable   bool
	identityKey   string
	activeStorage *managed.Storage
	logger        *slog.Logger
	stores        []*managed.Storage
}

// NewWalletStorageManager initializes a WalletStorageManager with an identity key and an active storage provider.
// Active storage and identity key must be provided, and it will panic if they are not.
func NewWalletStorageManager(identityKey string, logger *slog.Logger, active wdk.WalletStorageProvider, backups ...wdk.WalletStorageProvider) *WalletStorageManager {
	if is.BlankString(identityKey) {
		panic("identity key must be provided and cannot be empty")
	}

	var stores []*managed.Storage
	storesNum := len(backups) + to.IfThen(active != nil, 1).ElseThen(0)
	if storesNum > 0 {
		stores = make([]*managed.Storage, 0, storesNum)
		if active != nil {
			stores = append(stores, managed.NewManagedStorage(active))
		}
		for _, b := range backups {
			stores = append(stores, managed.NewManagedStorage(b))
		}
	}

	logger = logging.Child(logger, "StorageManager")

	return &WalletStorageManager{
		identityKey: identityKey,
		logger:      logger,
		stores:      stores,
	}
}

// AddWalletStorageProvider adds a new storage provider to the manager after construction.
// This enables the remote-wallet pattern where the wallet is created with an empty storage
// manager, then the storage client (which needs the wallet for auth) is added dynamically.
func (m *WalletStorageManager) AddWalletStorageProvider(ctx context.Context, provider wdk.WalletStorageProvider) error {
	store := managed.NewManagedStorage(provider)
	if _, err := store.MakeAvailableStorage(ctx, m.identityKey); err != nil {
		return fmt.Errorf("failed to make new storage provider available: %w", err)
	}
	m.stores = append(m.stores, store)
	m.isAvailable = false
	if _, err := m.MakeAvailable(ctx); err != nil {
		return fmt.Errorf("failed to make storage available after adding provider: %w", err)
	}
	return nil
}

// IsActiveEnabled reports whether the active storage is "enabled": its
// storageIdentityKey matches the user's currently selected activeStorage.
func (m *WalletStorageManager) IsActiveEnabled() bool {
	return m.activeStorage != nil &&
		m.activeStorage.Settings.StorageIdentityKey == m.activeStorage.User.ActiveStorage
}

// MakeAvailable makes the storage available for the user.
func (m *WalletStorageManager) MakeAvailable(ctx context.Context) (*wdk.TableSettings, error) {
	if m.isAvailable {
		return m.activeStorage.Settings, nil
	}

	if len(m.stores) == 0 {
		return nil, fmt.Errorf("no storage providers configured")
	}

	m.activeStorage = m.stores[0] // first storage is the active storage candidate
	_, err := m.activeStorage.MakeAvailableStorage(ctx, m.identityKey)
	if err != nil {
		return nil, fmt.Errorf("failed to make available active storage: %w", err)
	}

	m.isAvailable = true

	return m.activeStorage.Settings, nil
}

// GetAuth retrieves the authentication identity of the user after ensuring the storage is available and active.
func (m *WalletStorageManager) GetAuth(ctx context.Context) (wdk.AuthID, error) {
	_, err := m.MakeAvailable(ctx)
	if err != nil {
		return wdk.AuthID{}, fmt.Errorf("failed to make storage available: %w", err)
	}

	return wdk.AuthID{
		UserID:      to.Ptr(m.activeStorage.User.UserID),
		IdentityKey: m.identityKey,
		IsActive:    to.Ptr(m.activeStorage.Settings.StorageIdentityKey == m.activeStorage.User.ActiveStorage),
	}, nil
}

// SetActive switches to a new active storage provider from among the managed
// providers. In the single-active-store configuration this validates the target
// and updates the active-storage selection; it does not perform backup sync.
func (m *WalletStorageManager) SetActive(ctx context.Context, storageIdentityKey string) error {
	if is.BlankString(storageIdentityKey) {
		return fmt.Errorf("storage identity key must be provided and cannot be empty")
	}

	if _, err := m.MakeAvailable(ctx); err != nil {
		return fmt.Errorf("failed to make storage available: %w", err)
	}

	if m.activeStorage != nil && m.activeStorage.Settings.StorageIdentityKey == storageIdentityKey {
		// already active - persist the selection to be safe and return.
		return m.activeStorage.SetActive(ctx, m.authID(), storageIdentityKey)
	}

	newActiveIndex := stdslices.IndexFunc(m.stores, func(storage *managed.Storage) bool {
		return storage.Settings.StorageIdentityKey == storageIdentityKey
	})
	if newActiveIndex == -1 {
		return fmt.Errorf("storage with identity key %s not found among managed storages", storageIdentityKey)
	}

	newActive := m.stores[newActiveIndex]
	if err := newActive.SetActive(ctx, wdk.AuthID{IdentityKey: m.identityKey, UserID: to.Ptr(newActive.User.UserID)}, storageIdentityKey); err != nil {
		return fmt.Errorf("failed to set active storage %q: %w", storageIdentityKey, err)
	}

	for _, store := range m.stores {
		store.User.ActiveStorage = storageIdentityKey
	}

	m.activeStorage = newActive
	return nil
}

// GetActive returns the currently active storage provider, or nil if none is set.
func (m *WalletStorageManager) GetActive() wdk.WalletStorageProvider {
	if m.activeStorage == nil {
		return nil
	}
	return m.activeStorage.WalletStorageProvider
}

// GetActiveStore returns the identity key of the currently active storage provider, or an empty string if none is set.
func (m *WalletStorageManager) GetActiveStore() string {
	if m.activeStorage == nil {
		return ""
	}
	return m.activeStorage.Settings.StorageIdentityKey
}

func (m *WalletStorageManager) authID() wdk.AuthID {
	return wdk.AuthID{IdentityKey: m.identityKey, UserID: to.Ptr(m.activeStorage.User.UserID)}
}

func (m *WalletStorageManager) getActiveReader() wdk.WalletStorageProvider {
	if m.activeStorage == nil {
		return nil
	}
	return m.activeStorage
}

func (m *WalletStorageManager) getActiveWriter() wdk.WalletStorageProvider {
	if m.activeStorage == nil {
		return nil
	}
	return m.activeStorage
}

// FindOutputBaskets finds output baskets for the authenticated user based on the provided filters.
// This is an alias to FindOutputBasketsAuth for TS-version compatibility.
func (m *WalletStorageManager) FindOutputBaskets(ctx context.Context, filters wdk.FindOutputBasketsArgs) (wdk.TableOutputBaskets, error) {
	return m.FindOutputBasketsAuth(ctx, filters)
}

// FindOutputs finds outputs for the authenticated user based on the provided filters.
// This is an alias to FindOutputsAuth for TS-version compatibility.
func (m *WalletStorageManager) FindOutputs(ctx context.Context, filters wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
	return m.FindOutputsAuth(ctx, filters)
}
