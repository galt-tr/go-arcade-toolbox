package testusers

import (
	"testing"

	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/go-softwarelab/common/pkg/to"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// NOTE: Testabilities can modify user IDs, to match ID with database

// User is a fixed test user identity, backed by a deterministic private key.
type User struct {
	// Name of the user just for in tests logging purpose
	Name    string
	ID      int
	PrivKey string
}

// Alice is a fixed test user.
var Alice = User{
	Name:    "Alice",
	ID:      1,
	PrivKey: "143ab18a84d3b25e1a13cefa90038411e5d2014590a2a4a57263d1593c8dee1c",
}

// Bob is a fixed test user.
var Bob = User{
	Name:    "Bob",
	ID:      2,
	PrivKey: "0881208859876fc227d71bfb8b91814462c5164b6fee27e614798f6e85d2547d",
}

// AuthID returns the wdk.AuthID for the user.
func (u User) AuthID() wdk.AuthID {
	return wdk.AuthID{
		IdentityKey: u.mustGetPublicKey().ToDERHex(),
		UserID:      &u.ID,
		IsActive:    to.Ptr(true),
	}
}

// KeyDeriver returns a key deriver for the user's private key.
func (u User) KeyDeriver(t testing.TB) *sdk.KeyDeriver {
	t.Helper()
	key, err := primitives.PrivateKeyFromHex(u.PrivKey)
	require.NoError(t, err)

	return sdk.NewKeyDeriver(key)
}

// IdentityKey returns the user's public identity key in DER hex format.
func (u User) IdentityKey(t testing.TB) string {
	t.Helper()
	return u.PubKey(t)
}

func (u User) mustGetPublicKey() *primitives.PublicKey {
	priv, err := primitives.PrivateKeyFromHex(u.PrivKey)
	if err != nil {
		panic(err)
	}
	return priv.PubKey()
}

// PubKey returns the user's public key in DER hex format.
func (u User) PubKey(t testing.TB) string {
	t.Helper()
	return u.PublicKey(t).ToDERHex()
}

// PrivateKey returns the user's private key.
func (u User) PrivateKey(t testing.TB) *primitives.PrivateKey {
	t.Helper()

	priv, err := primitives.PrivateKeyFromHex(u.PrivKey)
	require.NoError(t, err)
	return priv
}

// PublicKey returns the user's public key.
func (u User) PublicKey(t testing.TB) *primitives.PublicKey {
	t.Helper()

	priv, err := primitives.PrivateKeyFromHex(u.PrivKey)
	require.NoError(t, err)

	return priv.PubKey()
}

// Address returns the mainnet P2PKH address for the user's public key.
func (u User) Address(t testing.TB) *script.Address {
	address, err := script.NewAddressFromPublicKey(u.PublicKey(t), false)
	require.NoErrorf(t, err, "Failed to create address for user %s, invalid test setup", u.Name)
	return address
}

// All returns all fixed test users.
func All() []*User {
	return []*User{&Alice, &Bob}
}
