package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	stdslices "slices"

	"github.com/go-softwarelab/common/pkg/is"
	"github.com/go-softwarelab/common/pkg/to"

	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/managed"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

var _ wdk.WalletStorage = (*WalletStorageManager)(nil)

// ErrStorageNotActive is returned by every write the manager exposes when the
// store it is bound to is not the one the user has selected as their active
// storage. Callers recover with [WalletStorageManager.SetActive].
var ErrStorageNotActive = errors.New("storage: writes disabled: store is not the user's active storage")

// ErrNoActiveStorageConfigured is returned when the manager has no storage
// provider to route a call to at all — both by the write gate and by
// MakeAvailable, so the auth-first wrappers (which reach MakeAvailable through
// GetAuth before they ever reach the gate) surface the same sentinel.
var ErrNoActiveStorageConfigured = errors.New("storage: no active storage provider configured")

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
// storageIdentityKey matches the user's currently selected activeStorage. It is
// false (never a panic) while Settings/User are still unpopulated —
// ThinksItIsActive folds in the IsAvailable nil-guard.
func (m *WalletStorageManager) IsActiveEnabled() bool {
	return m.activeStorage != nil && m.activeStorage.ThinksItIsActive()
}

// MakeAvailable makes the storage available for the user.
//
// Like SetActive, it is annotated @Write on [wdk.WalletStorageProvider] but is
// deliberately NOT routed through [WalletStorageManager.getActiveWriter]: it is
// what ESTABLISHES the user selection the gate compares against (it loads
// Settings and the user row), so gating it would be circular.
func (m *WalletStorageManager) MakeAvailable(ctx context.Context) (*wdk.TableSettings, error) {
	if m.isAvailable {
		return m.activeStorage.Settings, nil
	}

	if len(m.stores) == 0 {
		// The same sentinel the write gate returns: the auth-first wrappers
		// reach MakeAvailable (via GetAuth) before they reach the gate, so
		// without this an empty manager's CreateAction would fail an
		// errors.Is(err, ErrNoActiveStorageConfigured) check.
		return nil, ErrNoActiveStorageConfigured
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
//
// SetActive is deliberately NOT subject to the [WalletStorageManager.getActiveWriter]
// gate, even though [wdk.WalletStorageProvider] annotates it @Write. It is the
// sole in-process recovery path out of a manager whose writes are being
// refused: gating it would make that lockout unrecoverable without a restart,
// which is exactly the failure the caller is trying to fix.
func (m *WalletStorageManager) SetActive(ctx context.Context, storageIdentityKey string) error {
	if is.BlankString(storageIdentityKey) {
		return fmt.Errorf("storage identity key must be provided and cannot be empty")
	}

	if _, err := m.MakeAvailable(ctx); err != nil {
		return fmt.Errorf("failed to make storage available: %w", err)
	}

	// HasStorageIdentityKey folds in the availability check, so a half-made
	// store cannot nil-deref its Settings here.
	if m.activeStorage != nil && m.activeStorage.HasStorageIdentityKey(storageIdentityKey) {
		// Already the active candidate: persist the selection to be safe AND
		// refresh the cached User rows, exactly as the switch branch below
		// does. Persisting alone is not enough — IsActiveEnabled compares
		// Settings.StorageIdentityKey against the CACHED User.ActiveStorage,
		// so a stale cache would leave the write gate refusing until the next
		// process start. This path is precisely the escape hatch out of that
		// refusal, so it must land in memory too.
		if err := m.activeStorage.SetActive(ctx, m.authID(), storageIdentityKey); err != nil {
			return fmt.Errorf("failed to set active storage %q: %w", storageIdentityKey, err)
		}
		m.refreshCachedActiveStorage(storageIdentityKey)
		return nil
	}

	// HasStorageIdentityKey (not a raw Settings compare): a store that was
	// never made available has no Settings/User at all, and matching it would
	// nil-deref on the SetActive call below.
	newActiveIndex := stdslices.IndexFunc(m.stores, func(storage *managed.Storage) bool {
		return storage.HasStorageIdentityKey(storageIdentityKey)
	})
	if newActiveIndex == -1 {
		// The hint is a hedge, not a diagnosis: this fires for a mistyped key
		// just as readily as for a store that was never made available, and
		// the two are indistinguishable here — an unavailable store has no
		// identity to compare against in the first place.
		return fmt.Errorf("storage with identity key %s not found among managed storages "+
			"(a storage that was never made available has no identity to match, so it cannot be found here "+
			"— add it via AddWalletStorageProvider)", storageIdentityKey)
	}

	newActive := m.stores[newActiveIndex]
	if err := newActive.SetActive(ctx, wdk.AuthID{IdentityKey: m.identityKey, UserID: to.Ptr(newActive.User.UserID)}, storageIdentityKey); err != nil {
		return fmt.Errorf("failed to set active storage %q: %w", storageIdentityKey, err)
	}

	m.refreshCachedActiveStorage(storageIdentityKey)

	m.activeStorage = newActive
	return nil
}

// refreshCachedActiveStorage points every managed store's cached user selection
// at storageIdentityKey, so IsActiveEnabled (and therefore the write gate) sees
// the switch without waiting for a re-read of the users row.
func (m *WalletStorageManager) refreshCachedActiveStorage(storageIdentityKey string) {
	for _, store := range m.stores {
		if store.User == nil {
			continue // never made available; nothing cached to refresh
		}
		store.User.ActiveStorage = storageIdentityKey
	}
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

// getActiveReader returns the provider reads are routed to. It keeps its
// nil-return (rather than getActiveWriter's error) because every generated
// read wrapper calls GetAuth first, and GetAuth's MakeAvailable either
// populates activeStorage or fails the call — so the nil branch is unreachable
// from them, and only a future direct caller would need the error form.
func (m *WalletStorageManager) getActiveReader() wdk.WalletStorageProvider {
	if m.activeStorage == nil {
		return nil
	}
	return m.activeStorage
}

// getActiveWriter returns the provider every WRITE must go through, refusing
// with [ErrStorageNotActive] when this manager's store is not the one the user
// has selected as active.
//
// Without the check a manager whose activeStorage is merely stores[0] would
// happily create actions and mint change in a store the user does not consider
// active, leaving the same coin spendable in two storages — the double spend
// audit finding P1-2(c). Readers are deliberately NOT gated: inspecting a
// non-active (e.g. backup) store is legitimate.
func (m *WalletStorageManager) getActiveWriter() (wdk.WalletStorageProvider, error) {
	if m.activeStorage == nil {
		return nil, ErrNoActiveStorageConfigured
	}
	// Bootstrap-safe: before MakeAvailable populates Settings/User there is no
	// user selection to compare against, and the writes that run in that window
	// (Migrate, FindOrInsertUser) are precisely the ones that establish
	// availability. Gate only once the store knows who it is — ThinksItIsActive
	// is false for both "not selected" and "not yet available", so the
	// IsAvailable term is what separates the two.
	if m.activeStorage.IsAvailable() && !m.activeStorage.ThinksItIsActive() {
		return nil, fmt.Errorf("%w: this store is %q, the user's active storage is %q — call SetActive to cut over",
			ErrStorageNotActive,
			m.activeStorage.Settings.StorageIdentityKey, m.activeStorage.User.ActiveStorage)
	}
	return m.activeStorage, nil
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
