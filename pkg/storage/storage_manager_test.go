package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/managed"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// managerStore is one migrated provider a WalletStorageManager can manage.
type managerStore struct {
	p    *Provider
	meta *metastore.Store
	utxo *memstore.Store
	key  string
}

// newManagerStore builds a migrated (but user-less) provider whose storage
// identity key is `key`.
func newManagerStore(t *testing.T, key string) *managerStore {
	t.Helper()
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), key+".db")
	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New()
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	p, err := New(logger, meta, utxo, funder.New(logger, utxo, defs.DefaultFeeModel()),
		&fakeOracle{}, &fakeHeaders{},
		WithNetwork(defs.NetworkTestnet), WithStorageName(key), WithScriptsVerifier(alwaysValidScripts{}))
	require.NoError(t, err)
	_, err = p.Migrate(ctx, key, key)
	require.NoError(t, err)
	return &managerStore{p: p, meta: meta, utxo: utxo, key: key}
}

// fund mints one claimable coin (and its ancestry) for userID.
func (s *managerStore) fund(t *testing.T, userID int, seed byte, sats uint64) {
	t.Helper()
	ctx := context.Background()

	src := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = seed
	src.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: testP2PKH(t)})
	require.NoError(t, s.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID:   src.TxID().String(),
		Status: wdk.ProvenTxStatusCompleted,
		RawTx:  src.Bytes(),
	}))

	m := &utxostore.Mint{
		Outpoint:  utxostore.Outpoint{TxID: *src.TxID(), Vout: 0},
		UserID:    int64(userID),
		Basket:    wdk.BasketNameForChange,
		Satoshis:  sats,
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      utxostore.TierMined,
	}
	require.NoError(t, s.utxo.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(t, m.Err)
}

// TestManager_WriteRefusedWhenNotTheActiveStore is audit P1-2(c): a manager
// bound to a store the user has NOT selected as active must refuse writes
// rather than mint a coin that is also spendable in the real active store.
func TestManager_WriteRefusedWhenNotTheActiveStore(t *testing.T) {
	ctx := context.Background()
	primary := newManagerStore(t, "store-a")
	backup := newManagerStore(t, "store-b")

	// The user exists on store-a but has selected store-b as their active one.
	resp, err := primary.p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	uid := resp.User.UserID
	require.NoError(t, primary.p.SetActive(ctx,
		wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}, backup.key))
	primary.fund(t, uid, 0xE1, 100_000)

	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), primary.p, backup.p)
	_, err = m.MakeAvailable(ctx)
	require.NoError(t, err)
	require.False(t, m.IsActiveEnabled(), "store-a is not the user's active storage")

	_, err = m.CreateAction(ctx, paymentArgs(10_000))
	require.ErrorIs(t, err, ErrStorageNotActive)
	assert.ErrorContains(t, err, "store-a", "the error names this store")
	assert.ErrorContains(t, err, "store-b", "and the one the user selected")

	// Reads are unaffected: a non-active store may still be inspected, and
	// really reads — the coin is visible even though it may not be spent here.
	bal, err := m.GetBalance(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(100_000), bal, "reads pass through the gate untouched")
}

// TestManager_SetActiveOnCurrentStoreLiftsTheRefusal is the C1 regression: the
// already-active fast path in SetActive must refresh the CACHED user selection,
// not just persist it. Persisting alone leaves IsActiveEnabled reading a stale
// cache, which makes the gate's only in-process escape hatch a no-op — writes
// would stay refused until the process restarts.
func TestManager_SetActiveOnCurrentStoreLiftsTheRefusal(t *testing.T) {
	ctx := context.Background()
	primary := newManagerStore(t, "store-a")
	backup := newManagerStore(t, "store-b")

	resp, err := primary.p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	uid := resp.User.UserID
	require.NoError(t, primary.p.SetActive(ctx,
		wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}, backup.key))
	primary.fund(t, uid, 0xE3, 100_000)

	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), primary.p, backup.p)
	_, err = m.MakeAvailable(ctx)
	require.NoError(t, err)

	_, err = m.CreateAction(ctx, paymentArgs(10_000))
	require.ErrorIs(t, err, ErrStorageNotActive, "precondition: writes are refused")

	// The documented recovery: cut the user over to the store we are bound to.
	require.NoError(t, m.SetActive(ctx, primary.key))
	require.True(t, m.IsActiveEnabled(), "the cached selection must follow the persisted one")

	res, err := m.CreateAction(ctx, paymentArgs(10_000))
	require.NoError(t, err, "SetActive is the escape hatch; it must actually work in-process")
	assert.NotEmpty(t, res.Reference)
}

// TestManager_WriteAllowedOnTheActiveStore is the green counterpart: with the
// user's active storage pointing at the managed store, writes proceed.
func TestManager_WriteAllowedOnTheActiveStore(t *testing.T) {
	ctx := context.Background()
	primary := newManagerStore(t, "store-a")
	backup := newManagerStore(t, "store-b")

	resp, err := primary.p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	primary.fund(t, resp.User.UserID, 0xE2, 100_000)

	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), primary.p, backup.p)
	_, err = m.MakeAvailable(ctx)
	require.NoError(t, err)
	require.True(t, m.IsActiveEnabled(), "a fresh user's active storage defaults to store-a")

	res, err := m.CreateAction(ctx, paymentArgs(10_000))
	require.NoError(t, err)
	assert.NotEmpty(t, res.Reference)
}

// TestManager_BootstrapWritesAllowedBeforeAvailability: Migrate and
// FindOrInsertUser are what ESTABLISH availability, so the gate must let them
// through while Settings/User are still unpopulated.
func TestManager_BootstrapWritesAllowedBeforeAvailability(t *testing.T) {
	ctx := context.Background()
	store := newManagerStore(t, "store-a")

	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), store.p)
	// Pre-availability state: an active candidate with no Settings/User yet.
	m.activeStorage = managed.NewManagedStorage(store.p)
	require.False(t, m.activeStorage.IsAvailable())

	_, err := m.Migrate(ctx, "store-a", "store-a")
	require.NoError(t, err, "Migrate must survive the bootstrap window")
	_, err = m.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err, "FindOrInsertUser must survive the bootstrap window")
}

// TestManager_WriteWithoutActiveStorageErrors: no configured active storage is
// an error, not a nil-pointer panic.
func TestManager_WriteWithoutActiveStorageErrors(t *testing.T) {
	ctx := context.Background()
	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), nil)

	// The gate itself, reached directly by a wrapper that takes no auth.
	_, err := m.Migrate(ctx, "store-a", "store-a")
	require.ErrorIs(t, err, ErrNoActiveStorageConfigured)

	// And the auth-first path: CreateAction reaches MakeAvailable (via GetAuth)
	// long before it reaches the gate, so MakeAvailable must return the same
	// sentinel or errors.Is would be false here.
	_, err = m.CreateAction(ctx, paymentArgs(10_000))
	require.ErrorIs(t, err, ErrNoActiveStorageConfigured)
}

// TestManager_SetActiveRejectsNeverAvailableStore: a managed store that was
// never made available has no Settings/User at all, so selecting it must fail
// with an explanation rather than nil-dereference its identity.
func TestManager_SetActiveRejectsNeverAvailableStore(t *testing.T) {
	ctx := context.Background()
	primary := newManagerStore(t, "store-a")
	backup := newManagerStore(t, "store-b")

	m := NewWalletStorageManager(testIdentityKey, logging.NewTestLogger(t), primary.p)
	_, err := m.MakeAvailable(ctx)
	require.NoError(t, err)

	// Append the backup WITHOUT making it available (AddWalletStorageProvider
	// is what normally does that), then try to cut over to it.
	m.stores = append(m.stores, managed.NewManagedStorage(backup.p))

	err = m.SetActive(ctx, backup.key)
	require.Error(t, err)
	assert.ErrorContains(t, err, "never made available", "the hedged hint explains the likely cause")
	assert.ErrorContains(t, err, "AddWalletStorageProvider", "and names the workaround")
}
