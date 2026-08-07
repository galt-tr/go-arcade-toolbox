// Package compat_test is a compile-time (and, for the JSON-wire section,
// behavioral) replica of real go-wallet-toolbox call sites. It exists to
// enforce the migration promise this repo makes: a go-wallet-toolbox user
// switches over by changing import paths only.
//
// Every type/stub/literal in this file is modeled on an actual call site in
// github.com/bsv-blockchain/go-wallet-toolbox (its own tests are the source
// of the shapes below). If a signature, field, tag, or generic constraint
// drifts from the original, this file stops compiling (or, for the JSON
// golden checks, starts failing) — that's the point.
//
// This package must never import the old module. See the GROWTH section at
// the bottom for what is intentionally still missing (later milestones).
package compat_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/go-softwarelab/common/pkg/to"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/brc29"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	storagepkg "github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wallet/pending"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// M2 growth: the real storage.Provider — not just the stub above — satisfies
// wdk.WalletStorageProvider end to end. This is the compile assertion the M2
// GROWTH TODO called for. The Provider's constructor wiring and option funcs
// (New, WithChangeBasket, WithCommission, ...) are exercised in pkg/storage's
// own tests; here we only need the interface-satisfaction guarantee.
var _ wdk.WalletStorageProvider = (*storagepkg.Provider)(nil)

// ===========================================================================
// 1. wdk.WalletStorageProvider — the 21-method write-capable storage
// interface. This is the single most important assertion in this file: if a
// method signature drifts, this stops compiling.
//
// Modeled on the real implementer shape in go-wallet-toolbox's
// pkg/storage.Provider (a struct satisfying wdk.WalletStorageProvider, wired
// into storage.NewWalletStorageManager(identityKey, logger, active, backups...)).
// ===========================================================================

type stubWalletStorageProvider struct{}

var _ wdk.WalletStorageProvider = (*stubWalletStorageProvider)(nil)

func (s *stubWalletStorageProvider) Migrate(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

func (s *stubWalletStorageProvider) MakeAvailable(_ context.Context) (*wdk.TableSettings, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) SetActive(_ context.Context, _ wdk.AuthID, _ string) error {
	return nil
}

func (s *stubWalletStorageProvider) FindOrInsertUser(_ context.Context, _ string) (*wdk.FindOrInsertUserResponse, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) InternalizeAction(
	_ context.Context, _ wdk.AuthID, _ wdk.InternalizeActionArgs,
) (*wdk.InternalizeActionResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) CreateAction(
	_ context.Context, _ wdk.AuthID, _ wdk.ValidCreateActionArgs,
) (*wdk.StorageCreateActionResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) ProcessAction(
	_ context.Context, _ wdk.AuthID, _ wdk.ProcessActionArgs,
) (*wdk.ProcessActionResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) InsertCertificateAuth(
	_ context.Context, _ wdk.AuthID, _ *wdk.TableCertificateX,
) (uint, error) {
	return 0, nil
}

func (s *stubWalletStorageProvider) RelinquishCertificate(
	_ context.Context, _ wdk.AuthID, _ wdk.RelinquishCertificateArgs,
) error {
	return nil
}

func (s *stubWalletStorageProvider) RelinquishOutput(
	_ context.Context, _ wdk.AuthID, _ wdk.RelinquishOutputArgs,
) error {
	return nil
}

func (s *stubWalletStorageProvider) ListCertificates(
	_ context.Context, _ wdk.AuthID, _ wdk.ListCertificatesArgs,
) (*wdk.ListCertificatesResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) ListOutputs(
	_ context.Context, _ wdk.AuthID, _ wdk.ListOutputsArgs,
) (*wdk.ListOutputsResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) ListActions(
	_ context.Context, _ wdk.AuthID, _ wdk.ListActionsArgs,
) (*wdk.ListActionsResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) GetSyncChunk(
	_ context.Context, _ wdk.RequestSyncChunkArgs,
) (*wdk.SyncChunk, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) FindOrInsertSyncStateAuth(
	_ context.Context, _ wdk.AuthID, _, _ string,
) (*wdk.FindOrInsertSyncStateAuthResponse, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) ProcessSyncChunk(
	_ context.Context, _ wdk.RequestSyncChunkArgs, _ *wdk.SyncChunk,
) (*wdk.ProcessSyncChunkResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) AbortAction(
	_ context.Context, _ wdk.AuthID, _ wdk.AbortActionArgs,
) (*wdk.AbortActionResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) FindOutputBasketsAuth(
	_ context.Context, _ wdk.AuthID, _ wdk.FindOutputBasketsArgs,
) (wdk.TableOutputBaskets, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) FindOutputsAuth(
	_ context.Context, _ wdk.AuthID, _ wdk.FindOutputsArgs,
) (wdk.TableOutputs, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) ListTransactions(
	_ context.Context, _ wdk.AuthID, _ wdk.ListTransactionsArgs,
) (*wdk.ListTransactionsResult, error) {
	return nil, nil
}

func (s *stubWalletStorageProvider) GetBalance(_ context.Context, _ wdk.AuthID, _ string) (uint64, error) {
	return 0, nil
}

// ===========================================================================
// 2. wdk.WalletStorage / wdk.WalletStorageBasic — the generated,
// auth-pre-bound interfaces client-gen produces from WalletStorageProvider.
//
// Modeled on go-wallet-toolbox's pkg/storage.WalletStorageManager, which is
// the real implementer of wdk.WalletStorage (`var _ wdk.WalletStorage =
// (*WalletStorageManager)(nil)`), and on WithStorageManager(mgr
// wdk.WalletStorage) / Wallet.storage (field typed wdk.WalletStorage) in
// pkg/wallet/wallet.go.
// ===========================================================================

// stubStorageManager implements WalletStorageBasic on its own: the
// auth-stripped, sync-method-stripped method set client-gen produces for the
// wallet-facing manager (skip-methods: MakeAvailable/SetActive are still
// present here per the interface; GetSyncChunk & co. are the ones dropped).
type stubStorageManager struct{}

var _ wdk.WalletStorageBasic = (*stubStorageManager)(nil)

func (m *stubStorageManager) Migrate(_ context.Context, _ string, _ string) (string, error) {
	return "", nil
}

func (m *stubStorageManager) MakeAvailable(_ context.Context) (*wdk.TableSettings, error) {
	return nil, nil
}

func (m *stubStorageManager) SetActive(_ context.Context, _ string) error {
	return nil
}

func (m *stubStorageManager) FindOrInsertUser(_ context.Context, _ string) (*wdk.FindOrInsertUserResponse, error) {
	return nil, nil
}

func (m *stubStorageManager) InternalizeAction(
	_ context.Context, _ wdk.InternalizeActionArgs,
) (*wdk.InternalizeActionResult, error) {
	return nil, nil
}

func (m *stubStorageManager) CreateAction(
	_ context.Context, _ wdk.ValidCreateActionArgs,
) (*wdk.StorageCreateActionResult, error) {
	return nil, nil
}

func (m *stubStorageManager) ProcessAction(
	_ context.Context, _ wdk.ProcessActionArgs,
) (*wdk.ProcessActionResult, error) {
	return nil, nil
}

func (m *stubStorageManager) InsertCertificateAuth(_ context.Context, _ *wdk.TableCertificateX) (uint, error) {
	return 0, nil
}

func (m *stubStorageManager) RelinquishCertificate(_ context.Context, _ wdk.RelinquishCertificateArgs) error {
	return nil
}

func (m *stubStorageManager) RelinquishOutput(_ context.Context, _ wdk.RelinquishOutputArgs) error {
	return nil
}

func (m *stubStorageManager) ListCertificates(
	_ context.Context, _ wdk.ListCertificatesArgs,
) (*wdk.ListCertificatesResult, error) {
	return nil, nil
}

func (m *stubStorageManager) ListOutputs(_ context.Context, _ wdk.ListOutputsArgs) (*wdk.ListOutputsResult, error) {
	return nil, nil
}

func (m *stubStorageManager) ListActions(_ context.Context, _ wdk.ListActionsArgs) (*wdk.ListActionsResult, error) {
	return nil, nil
}

func (m *stubStorageManager) AbortAction(_ context.Context, _ wdk.AbortActionArgs) (*wdk.AbortActionResult, error) {
	return nil, nil
}

func (m *stubStorageManager) FindOutputBasketsAuth(
	_ context.Context, _ wdk.FindOutputBasketsArgs,
) (wdk.TableOutputBaskets, error) {
	return nil, nil
}

func (m *stubStorageManager) FindOutputsAuth(_ context.Context, _ wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
	return nil, nil
}

func (m *stubStorageManager) ListTransactions(
	_ context.Context, _ wdk.ListTransactionsArgs,
) (*wdk.ListTransactionsResult, error) {
	return nil, nil
}

func (m *stubStorageManager) GetBalance(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

// stubWalletStorage adds GetAuth on top of the embedded manager — the shape
// wdk.WalletStorage (the field type Wallet.storage / WithStorageManager's
// parameter) requires.
type stubWalletStorage struct {
	stubStorageManager
}

var _ wdk.WalletStorage = (*stubWalletStorage)(nil)

func (w *stubWalletStorage) GetAuth(_ context.Context) (wdk.AuthID, error) {
	return wdk.AuthID{}, nil
}

// ===========================================================================
// 3. wdk.Services — the 3rd-party-services facade (chain tracker, broadcast,
// merkle proofs, tx status). Modeled on go-wallet-toolbox's
// pkg/wallet/wallet.go and pkg/storage/provider.go usage of wdk.Services as
// a constructor/field type.
// ===========================================================================

type stubServices struct{}

var _ wdk.Services = (*stubServices)(nil)

func (s *stubServices) ChainHeaderByHeight(_ context.Context, _ uint32) (*wdk.ChainBlockHeader, error) {
	return nil, nil
}

func (s *stubServices) IsValidRootForHeight(_ context.Context, _ *chainhash.Hash, _ uint32) (bool, error) {
	return false, nil
}

func (s *stubServices) CurrentHeight(_ context.Context) (uint32, error) {
	return 0, nil
}

func (s *stubServices) PostFromBEEF(_ context.Context, _ *transaction.Beef, _ []string) (wdk.PostFromBeefResult, error) {
	return nil, nil
}

func (s *stubServices) MerklePath(_ context.Context, _ string) (*wdk.MerklePathResult, error) {
	return nil, nil
}

func (s *stubServices) FindChainTipHeader(_ context.Context) (*wdk.ChainBlockHeader, error) {
	return nil, nil
}

func (s *stubServices) RawTx(_ context.Context, _ string) (wdk.RawTxResult, error) {
	return wdk.RawTxResult{}, nil
}

func (s *stubServices) GetBEEF(_ context.Context, _ string, _ []string) (*transaction.Beef, error) {
	return nil, nil
}

func (s *stubServices) NLockTimeIsFinal(_ context.Context, _ any) (bool, error) {
	return false, nil
}

func (s *stubServices) GetStatusForTxIDs(_ context.Context, _ []string) (*wdk.GetStatusForTxIDsResult, error) {
	return nil, nil
}

// ===========================================================================
// 4. Args construction — literal construction of the key BRC-100 arg/result
// structs the way a real go-wallet-toolbox call site writes them. Modeled on
// pkg/internal/fixtures/default_*.go and the storage provider tests.
// ===========================================================================

func TestValidCreateActionArgsCallSiteShape(t *testing.T) {
	args := wdk.ValidCreateActionArgs{
		Description: "spend existing output",
		InputBEEF:   primitives.BEEF{0, 1, 255},
		Inputs: []wdk.ValidCreateActionInput{
			{
				Outpoint:         wdk.OutPoint{TxID: "abcd1234", Vout: 1},
				InputDescription: "input 0",
			},
		},
		Outputs: []wdk.ValidCreateActionOutput{
			{
				LockingScript:      "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac",
				Satoshis:           42000,
				OutputDescription:  "test output",
				CustomInstructions: to.Ptr("custom instructions"),
				Tags:               []primitives.StringUnder300{"test-tag"},
			},
		},
		LockTime: 0,
		Version:  1,
		Labels:   []primitives.StringUnder300{"test-label"},
		Options: wdk.ValidCreateActionOptions{
			AcceptDelayedBroadcast: to.Ptr[primitives.BooleanDefaultTrue](false),
			SendWith:               []primitives.TXIDHexString{},
			SignAndProcess:         to.Ptr(primitives.BooleanDefaultTrue(true)),
			KnownTxids:             []primitives.TXIDHexString{},
			NoSendChange:           []wdk.OutPoint{},
			RandomizeOutputs:       false,
			TrustSelf:              nil,
			// FuelShape is a toolbox extension (throughput UTXO management),
			// absent from a plain BRC-100 call site unless the fuel keeper set it.
			FuelShape: &wdk.ShapedChange{
				Count:    3,
				Satoshis: 546,
				Basket:   wdk.BasketNameForFuel,
			},
		},
		IsSendWith:                   false,
		IsDelayed:                    false,
		IsNoSend:                     false,
		IsNewTx:                      true,
		IsRemixChange:                false,
		IsSignAction:                 false,
		IncludeAllSourceTransactions: true,
	}

	require.Len(t, args.Inputs, 1)
	require.Len(t, args.Outputs, 1)
	assert.Equal(t, wdk.BasketNameForFuel, string(args.Options.FuelShape.Basket))
}

func TestInternalizeActionArgsCallSiteShape(t *testing.T) {
	// Both protocol variants are packed into one Outputs slice here for
	// economy; real call sites typically use one protocol per call.
	args := wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray{0, 1, 2, 3},
		Outputs: []*wdk.InternalizeOutput{
			{
				OutputIndex: 0,
				Protocol:    wdk.WalletPaymentProtocol,
				PaymentRemittance: &wdk.WalletPayment{
					DerivationPrefix:  "cHJlZml4",
					DerivationSuffix:  "c3VmZml4",
					SenderIdentityKey: "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
				},
			},
			{
				OutputIndex: 1,
				Protocol:    wdk.BasketInsertionProtocol,
				InsertionRemittance: &wdk.BasketInsertion{
					Basket:             "custom-basket",
					CustomInstructions: to.Ptr("custom instructions"),
					Tags:               []primitives.StringUnder300{"tag1", "tag2"},
				},
			},
		},
		Labels:         []primitives.StringUnder300{"label1", "label2"},
		Description:    "first internalize",
		SeekPermission: to.Ptr(primitives.BooleanDefaultTrue(true)),
	}

	require.Len(t, args.Outputs, 2)
	require.NoError(t, args.Outputs[0].Validate())
	require.NoError(t, args.Outputs[1].Validate())
}

func TestListActionsArgsCallSiteShape(t *testing.T) {
	args := wdk.ListActionsArgs{
		Labels:         []primitives.StringUnder300{"run", "action time from 1704067200000"},
		Limit:          10,
		Offset:         0,
		LabelQueryMode: to.Ptr(defs.QueryModeAll),
		SeekPermission: to.Ptr(primitives.BooleanDefaultTrue(true)),
	}

	require.Len(t, args.Labels, 2)
	require.NotNil(t, args.LabelQueryMode)
}

func TestListOutputsArgsCallSiteShape(t *testing.T) {
	args := wdk.ListOutputsArgs{
		Basket:       primitives.StringUnder300(wdk.BasketNameForChange),
		Limit:        100,
		TagQueryMode: to.Ptr(defs.QueryModeAny),
		KnownTxids:   []string{"abcd1234"},
	}

	require.Equal(t, wdk.BasketNameForChange, string(args.Basket))
}

func TestProcessActionArgsCallSiteShape(t *testing.T) {
	args := wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: to.Ptr("some-reference"),
		TxID:      to.Ptr(primitives.TXIDHexString("abcd1234")),
		RawTx:     primitives.ExplicitByteArray{0x01, 0x02},
		SendWith:  []primitives.TXIDHexString{},
	}

	require.True(t, args.IsNewTx)
	require.Equal(t, "some-reference", *args.Reference)
}

func TestAbortActionArgsCallSiteShape(t *testing.T) {
	args := wdk.AbortActionArgs{
		Reference: primitives.Base64String("ybQus1rq4M4gi/7L"),
	}

	require.NoError(t, args.Reference.Validate())
}

func TestAuthIDCallSiteShape(t *testing.T) {
	userID := 7
	auth := wdk.AuthID{
		IdentityKey: "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
		UserID:      &userID,
		IsActive:    to.Ptr(true),
	}

	require.NotNil(t, auth.UserID)
	assert.Equal(t, 7, *auth.UserID)
}

// ===========================================================================
// 5. JSON wire pinning — the golden strings below were generated by
// marshaling this EXACT literal against github.com/bsv-blockchain/
// go-wallet-toolbox directly (a throwaway program with a `replace` directive
// to the old module, run outside of both repos and discarded). Any tag or
// type change that alters the wire format — in either direction — fails
// this test.
// ===========================================================================

func TestValidCreateActionArgsWireFormatMatchesOldRepo(t *testing.T) {
	args := wdk.ValidCreateActionArgs{
		Description: "spend existing output for compat check",
		InputBEEF:   primitives.BEEF{0, 1, 2, 255},
		Inputs: []wdk.ValidCreateActionInput{
			{
				Outpoint:         wdk.OutPoint{TxID: "abcd1234", Vout: 1},
				InputDescription: "input 0",
				SequenceNumber:   0,
				UnlockingScript:  to.Ptr(primitives.HexString("48656c6c6f")),
			},
			{
				Outpoint:              wdk.OutPoint{TxID: "deadbeef", Vout: 2},
				InputDescription:      "input 1",
				SequenceNumber:        5,
				UnlockingScriptLength: to.Ptr(primitives.PositiveInteger(107)),
			},
		},
		Outputs: []wdk.ValidCreateActionOutput{
			{
				LockingScript:      "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac",
				Satoshis:           1000,
				OutputDescription:  "payment output",
				Basket:             to.Ptr(primitives.StringUnder300("payments")),
				CustomInstructions: to.Ptr("custom-instructions"),
				Tags:               []primitives.StringUnder300{"tag1", "tag2"},
			},
		},
		LockTime:                     0,
		Version:                      1,
		Labels:                       []primitives.StringUnder300{"label1"},
		IsSignAction:                 false,
		RandomVals:                   to.Ptr([]int{1, 2, 3}),
		IncludeAllSourceTransactions: true,
		Options: wdk.ValidCreateActionOptions{
			AcceptDelayedBroadcast: to.Ptr(primitives.BooleanDefaultTrue(false)),
			ReturnTXIDOnly:         to.Ptr(primitives.BooleanDefaultFalse(true)),
			NoSend:                 to.Ptr(primitives.BooleanDefaultFalse(false)),
			SendWith:               []primitives.TXIDHexString{"aa11"},
			SignAndProcess:         to.Ptr(primitives.BooleanDefaultTrue(true)),
			TrustSelf:              to.Ptr(sdk.TrustSelfKnown),
			KnownTxids:             []primitives.TXIDHexString{"bb22"},
			NoSendChange:           []wdk.OutPoint{{TxID: "cc33", Vout: 0}},
			RandomizeOutputs:       true,
			FuelShape: &wdk.ShapedChange{
				Count:    3,
				Satoshis: 546,
				Basket:   "fuel",
			},
		},
		IsSendWith:    false,
		IsNewTx:       true,
		IsRemixChange: false,
		IsNoSend:      false,
		IsDelayed:     false,
	}

	// Golden generated from github.com/bsv-blockchain/go-wallet-toolbox@main
	// by marshaling the identical literal above (fields renamed wdk-> to
	// match, only the import path differs).
	const golden = `{"description":"spend existing output for compat check","inputBEEF":[0,1,2,255],"inputs":[{"outpoint":{"txid":"abcd1234","vout":1},"inputDescription":"input 0","unlockingScript":"48656c6c6f"},{"outpoint":{"txid":"deadbeef","vout":2},"inputDescription":"input 1","sequenceNumber":5,"unlockingScriptLength":107}],"outputs":[{"lockingScript":"76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac","satoshis":1000,"outputDescription":"payment output","basket":"payments","customInstructions":"custom-instructions","tags":["tag1","tag2"]}],"version":1,"labels":["label1"],"randomVals":[1,2,3],"includeAllSourceTransactions":true,"options":{"acceptDelayedBroadcast":false,"returnTXIDOnly":true,"noSend":false,"sendWith":["aa11"],"signAndProcess":true,"trustSelf":"known","knownTxids":["bb22"],"noSendChange":[{"txid":"cc33","vout":0}],"randomizeOutputs":true,"fuelShape":{"count":3,"satoshis":546,"basket":"fuel"}},"isNewTx":true}`

	data, err := json.Marshal(args)
	require.NoError(t, err)
	assert.JSONEq(t, golden, string(data))
}

func TestListOutputsResultWireFormatMatchesOldRepo(t *testing.T) {
	result := wdk.ListOutputsResult{
		TotalOutputs: 2,
		BEEF:         primitives.ExplicitByteArray{0, 1, 2, 255},
		Outputs: []*wdk.WalletOutput{
			{
				Satoshis:           1000,
				Spendable:          true,
				Outpoint:           "abcd1234.1",
				CustomInstructions: to.Ptr("custom-instructions"),
				LockingScript:      to.Ptr(primitives.HexString("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac")),
				Tags:               []primitives.StringUnder300{"tag1", "tag2"},
				Labels:             []primitives.StringUnder300{"label1"},
			},
			{
				Satoshis:  500,
				Spendable: false,
				Outpoint:  "deadbeef.2",
			},
		},
	}

	// Golden generated from github.com/bsv-blockchain/go-wallet-toolbox@main
	// by marshaling the identical literal above.
	const golden = `{"totalOutputs":2,"BEEF":[0,1,2,255],"outputs":[{"satoshis":1000,"spendable":true,"outpoint":"abcd1234.1","customInstructions":"custom-instructions","lockingScript":"76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac","tags":["tag1","tag2"],"labels":["label1"]},{"satoshis":500,"spendable":false,"outpoint":"deadbeef.2"}]}`

	data, err := json.Marshal(result)
	require.NoError(t, err)
	assert.JSONEq(t, golden, string(data))
}

// ===========================================================================
// 6. defs — network/db/fee/arcade/commission/change-basket config call
// sites. Modeled on pkg/defs/*_test.go and pkg/storage's provider option
// tests (WithChangeBasket, WithCommission).
// ===========================================================================

func TestBSVNetworkConstantsCallSiteShape(t *testing.T) {
	networks := []defs.BSVNetwork{defs.NetworkMainnet, defs.NetworkTestnet, defs.NetworkTTN, defs.NetworkTSTN}
	for _, n := range networks {
		require.NoError(t, n.Validate())
	}

	parsed, err := defs.ParseBSVNetworkStr("main")
	require.NoError(t, err)
	assert.Equal(t, defs.NetworkMainnet, parsed)
}

func TestDatabaseConfigCallSiteShape(t *testing.T) {
	sqliteCfg := defs.DefaultDBConfig()
	sqliteCfg.Engine = defs.DBTypeSQLite
	require.NoError(t, sqliteCfg.Validate())

	pgCfg := defs.DefaultDBConfig()
	pgCfg.Engine = defs.DBTypePostgres
	require.NoError(t, pgCfg.Validate())

	// DBTypeMySQL is retained only for compile compatibility with old
	// configs/enums that reference it; go-arcade-toolbox does not support the
	// engine (Database.Validate rejects it — see pkg/defs/dbs.go).
	mysqlCfg := defs.DefaultDBConfig()
	mysqlCfg.Engine = defs.DBTypeMySQL
	require.Error(t, mysqlCfg.Validate())

	parsed, err := defs.ParseDBTypeStr("sqlite")
	require.NoError(t, err)
	assert.Equal(t, defs.DBTypeSQLite, parsed)
}

func TestFeeModelCallSiteShape(t *testing.T) {
	feeModel := defs.FeeModel{
		Type:  defs.SatPerKB,
		Value: 100,
	}

	require.NoError(t, feeModel.Validate())
	assert.Equal(t, defs.DefaultFeeModel(), feeModel)
}

func TestArcadeConfigCallSiteShape(t *testing.T) {
	arcade := defs.Arcade{
		Enabled:           true,
		URL:               defs.ArcadeURL,
		CallbackURL:       "https://example.com/callback",
		FullStatusUpdates: true,
		CircuitBreaker: defs.ArcadeCircuitBreaker{
			FailureThreshold:           3,
			HealthProbeIntervalSeconds: 30,
		},
	}

	require.NoError(t, arcade.Validate())
	assert.Equal(t, defs.ArcadeURL, arcade.EventsURL)
}

func TestCommissionCallSiteShape(t *testing.T) {
	commission := defs.Commission{
		PubKeyHex: "03398d26f180996f8a2cb175a99620630d76257ccfef4ac7d303c8aa6f90c3190c",
		Satoshis:  10,
	}

	require.NoError(t, commission.Validate())
	require.True(t, commission.Enabled())
}

func TestChangeBasketCallSiteShape(t *testing.T) {
	changeBasket := defs.ChangeBasket{
		NumberOfDesiredUTXOs:    5000,
		MinimumDesiredUTXOValue: 2000,
		MaxChangeOutputsPerTx:   20,
	}

	assert.Equal(t, uint64(20), changeBasket.MaxChangeOutputsPerTx)
	assert.Equal(t, defs.DefaultChangeBasket().NumberOfDesiredUTXOs, int64(32))
}

// ===========================================================================
// 7. brc29 — generic instantiations exactly as old call sites write them
// (string-hex, WIF, and *sdk.KeyDeriver key sources). Modeled on
// pkg/brc29/brc29_template_test.go and pkg/brc29/brc29_address_test.go.
//
// The key material below is the same published BRC-29 test vector used in
// go-wallet-toolbox's own test fixtures (pkg/brc29/fixtures_test.go) — not a
// real key.
// ===========================================================================

const (
	compatSenderPrivateKeyHex    = "143ab18a84d3b25e1a13cefa90038411e5d2014590a2a4a57263d1593c8dee1c"
	compatSenderPublicKeyHex     = "0320bbfb879bbd6761ecd2962badbb41ba9d60ca88327d78b07ae7141af6b6c810"
	compatSenderWIF              = "Kwu2vS6fqkd5WnRgB9VXd4vYpL9mwkXePZWtG9Nr5s6JmfHcLsQr"
	compatRecipientPrivateKeyHex = "0000000000000000000000000000000000000000000000000000000000000001"
	compatRecipientPublicKeyHex  = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
	compatDerivationPrefix       = "Pr=="
	compatDerivationSuffix       = "Su=="
)

func TestBRC29LockForCounterpartyCallSiteShape(t *testing.T) {
	keyID := brc29.KeyID{DerivationPrefix: compatDerivationPrefix, DerivationSuffix: compatDerivationSuffix}

	lockingScript, err := brc29.LockForCounterparty(
		brc29.PrivHex(compatSenderPrivateKeyHex), keyID, brc29.PubHex(compatRecipientPublicKeyHex),
	)
	require.NoError(t, err)
	require.NotNil(t, lockingScript)

	// Same call, but via WIF (a documented alternative key source).
	lockingScriptFromWIF, err := brc29.LockForCounterparty(
		brc29.WIF(compatSenderWIF), keyID, brc29.PubHex(compatRecipientPublicKeyHex),
	)
	require.NoError(t, err)
	require.NotNil(t, lockingScriptFromWIF)

	// Same call, but with a *sdk.KeyDeriver as the sender private-key source —
	// the pattern from go-wallet-toolbox's brc29_address_test.go
	// ("key deriver as sender private key source").
	senderPriv, err := ec.PrivateKeyFromHex(compatSenderPrivateKeyHex)
	require.NoError(t, err)

	lockingScriptFromDeriver, err := brc29.LockForCounterparty(
		sdk.NewKeyDeriver(senderPriv), keyID, brc29.PubHex(compatRecipientPublicKeyHex),
	)
	require.NoError(t, err)
	require.NotNil(t, lockingScriptFromDeriver)
	assert.Equal(t, lockingScript, lockingScriptFromDeriver)
}

func TestBRC29LockForSelfCallSiteShape(t *testing.T) {
	keyID := brc29.KeyID{DerivationPrefix: compatDerivationPrefix, DerivationSuffix: compatDerivationSuffix}

	lockingScript, err := brc29.LockForSelf(
		brc29.PubHex(compatSenderPublicKeyHex), keyID, brc29.PrivHex(compatRecipientPrivateKeyHex),
	)
	require.NoError(t, err)
	require.NotNil(t, lockingScript)
}

func TestBRC29AddressForSelfCallSiteShape(t *testing.T) {
	keyID := brc29.KeyID{DerivationPrefix: compatDerivationPrefix, DerivationSuffix: compatDerivationSuffix}

	address, err := brc29.AddressForSelf(
		brc29.PubHex(compatSenderPublicKeyHex), keyID, brc29.PrivHex(compatRecipientPrivateKeyHex), brc29.WithTestNet(),
	)
	require.NoError(t, err)
	require.NotNil(t, address)
}

func TestBRC29UnlockCallSiteShape(t *testing.T) {
	keyID := brc29.KeyID{DerivationPrefix: compatDerivationPrefix, DerivationSuffix: compatDerivationSuffix}

	unlocker, err := brc29.Unlock(
		brc29.PubHex(compatSenderPublicKeyHex), keyID, brc29.PrivHex(compatRecipientPrivateKeyHex),
	)
	require.NoError(t, err)
	require.NotNil(t, unlocker)

	// Same call with *sdk.KeyDeriver on BOTH generic arms (sender is the
	// CounterpartyPublicKey source, recipient the CounterpartyPrivateKey
	// source) — the "Unlock with key derivers" shape from the old repo's
	// brc29.Unlock docs and tests.
	senderPriv, err := ec.PrivateKeyFromHex(compatSenderPrivateKeyHex)
	require.NoError(t, err)
	recipientPriv, err := ec.PrivateKeyFromHex(compatRecipientPrivateKeyHex)
	require.NoError(t, err)

	unlockerFromDerivers, err := brc29.Unlock(
		sdk.NewKeyDeriver(senderPriv), keyID, sdk.NewKeyDeriver(recipientPriv),
	)
	require.NoError(t, err)
	require.NotNil(t, unlockerFromDerivers)

	// Unlock returns a transaction.UnlockingScriptTemplate implementation.
	var _ transaction.UnlockingScriptTemplate = unlocker

	length := unlocker.EstimateLength(transaction.NewTransaction(), 0)
	assert.Positive(t, length)
}

// ===========================================================================
// 8. pending — SignActionsRepository construction and interface usage.
// Modeled on pkg/wallet/pending/local_pending_sign_actions_repo_test.go and
// pkg/wallet/wallet.go's `pending.NewSignActionLocalRepository(logger,
// pending.DefaultPendingSignActionsTTL)` wiring.
// ===========================================================================

func TestPendingSignActionsRepositoryCallSiteShape(t *testing.T) {
	var repo pending.SignActionsRepository = pending.NewSignActionLocalRepository(
		slog.Default(), pending.DefaultPendingSignActionsTTL,
	)

	action := &pending.SignAction{
		Tx:        transaction.Transaction{},
		InputBEEF: nil,
		CreateActionArgs: wdk.ValidCreateActionArgs{
			Description: "test transaction",
			Inputs:      []wdk.ValidCreateActionInput{},
			Outputs:     []wdk.ValidCreateActionOutput{},
		},
	}

	require.NoError(t, repo.Save("ref1", action))

	got, err := repo.Get("ref1")
	require.NoError(t, err)
	assert.Equal(t, *action, *got)

	require.NoError(t, repo.Delete("ref1"))

	_, err = repo.Get("ref1")
	require.ErrorIs(t, err, wdk.ErrNotFoundError)
}

// ===========================================================================
// 9. GROWTH — this file is the living compat checklist. Add to it as each
// later milestone ports its package, following the same pattern: stub +
// interface-satisfaction assertion for new interfaces, literal construction
// for new arg/result types, and a golden JSON pin for anything wire-visible.
//
// DONE(M2 — pkg/storage): storage.Provider satisfies wdk.WalletStorageProvider
// end to end — see the `var _ wdk.WalletStorageProvider = (*storagepkg.Provider)(nil)`
// assertion near the top of this file. The constructor (storage.New) takes live
// subsystems (metastore/utxostore/funder/oracle/headers), so it is wired and
// exercised in pkg/storage's own tests rather than reconstructed here; the
// option funcs (WithChangeBasket, WithCommission, WithNetwork, ...) live there
// too. The go-wallet-toolbox WalletStorageManager wrapper is not ported.
//
// TODO(M3 — pkg/wallet): wallet.New(...) generic constructors, the Wallet
// struct's exported methods (CreateAction/ProcessAction/ListActions/
// ListOutputs/InternalizeAction/AbortAction/...), and the wallet option funcs
// (WithStorageManager, WithServices, WithPendingSignActionsRepo, ...) —
// pkg/wallet/wallet.go and pkg/wallet/internal/wallet_opts in the old repo.
//
// TODO(M4 — pkg/monitor): monitor construction and its task/runner option
// funcs, once ported.
// ===========================================================================
