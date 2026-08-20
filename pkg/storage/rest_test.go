package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// fakeProvider is a configurable wdk.WalletStorageProvider stand-in for the REST
// transport tests. Only the hooks a given test exercises are set; every other
// method returns a zero value. FindOrInsertUser must be set for any auth-scoped
// route (the server resolves the AuthID through it).
type fakeProvider struct {
	findUser     func(ctx context.Context, identityKey string) (*wdk.FindOrInsertUserResponse, error)
	getBalance   func(ctx context.Context, auth wdk.AuthID, basket string) (uint64, error)
	createAction func(ctx context.Context, auth wdk.AuthID, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error)
}

func (f *fakeProvider) FindOrInsertUser(ctx context.Context, identityKey string) (*wdk.FindOrInsertUserResponse, error) {
	if f.findUser != nil {
		return f.findUser(ctx, identityKey)
	}
	return &wdk.FindOrInsertUserResponse{User: wdk.TableUser{UserID: 1, IdentityKey: identityKey}}, nil
}

func (f *fakeProvider) GetBalance(ctx context.Context, auth wdk.AuthID, basket string) (uint64, error) {
	if f.getBalance != nil {
		return f.getBalance(ctx, auth, basket)
	}
	return 0, nil
}

func (f *fakeProvider) CreateAction(ctx context.Context, auth wdk.AuthID, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
	if f.createAction != nil {
		return f.createAction(ctx, auth, args)
	}
	return &wdk.StorageCreateActionResult{}, nil
}

func (f *fakeProvider) Migrate(context.Context, string, string) (string, error) { return "", nil }
func (f *fakeProvider) MakeAvailable(context.Context) (*wdk.TableSettings, error) {
	return &wdk.TableSettings{StorageName: "fake"}, nil
}
func (f *fakeProvider) SetActive(context.Context, wdk.AuthID, string) error { return nil }
func (f *fakeProvider) InternalizeAction(context.Context, wdk.AuthID, wdk.InternalizeActionArgs) (*wdk.InternalizeActionResult, error) {
	return nil, nil
}

func (f *fakeProvider) ProcessAction(context.Context, wdk.AuthID, wdk.ProcessActionArgs) (*wdk.ProcessActionResult, error) {
	return nil, nil
}

func (f *fakeProvider) InsertCertificateAuth(context.Context, wdk.AuthID, *wdk.TableCertificateX) (uint, error) {
	return 0, nil
}

func (f *fakeProvider) RelinquishCertificate(context.Context, wdk.AuthID, wdk.RelinquishCertificateArgs) error {
	return nil
}

func (f *fakeProvider) RelinquishOutput(context.Context, wdk.AuthID, wdk.RelinquishOutputArgs) error {
	return nil
}

func (f *fakeProvider) ListCertificates(context.Context, wdk.AuthID, wdk.ListCertificatesArgs) (*wdk.ListCertificatesResult, error) {
	return nil, nil
}

func (f *fakeProvider) ListOutputs(context.Context, wdk.AuthID, wdk.ListOutputsArgs) (*wdk.ListOutputsResult, error) {
	return nil, nil
}

func (f *fakeProvider) ListActions(context.Context, wdk.AuthID, wdk.ListActionsArgs) (*wdk.ListActionsResult, error) {
	return nil, nil
}

func (f *fakeProvider) GetSyncChunk(context.Context, wdk.RequestSyncChunkArgs) (*wdk.SyncChunk, error) {
	return nil, nil
}

func (f *fakeProvider) FindOrInsertSyncStateAuth(context.Context, wdk.AuthID, string, string) (*wdk.FindOrInsertSyncStateAuthResponse, error) {
	return nil, nil
}

func (f *fakeProvider) ProcessSyncChunk(context.Context, wdk.RequestSyncChunkArgs, *wdk.SyncChunk) (*wdk.ProcessSyncChunkResult, error) {
	return nil, nil
}

func (f *fakeProvider) AbortAction(context.Context, wdk.AuthID, wdk.AbortActionArgs) (*wdk.AbortActionResult, error) {
	return nil, nil
}

func (f *fakeProvider) FindOutputBasketsAuth(context.Context, wdk.AuthID, wdk.FindOutputBasketsArgs) (wdk.TableOutputBaskets, error) {
	return nil, nil
}

func (f *fakeProvider) FindOutputsAuth(context.Context, wdk.AuthID, wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
	return nil, nil
}

func (f *fakeProvider) ListTransactions(context.Context, wdk.AuthID, wdk.ListTransactionsArgs) (*wdk.ListTransactionsResult, error) {
	return nil, nil
}

// newFakeClient wires a Client over an httptest.Server serving fake through the
// REST adapter, with the given server options.
func newFakeClient(t *testing.T, fake wdk.WalletStorageProvider, opts ...storage.ServerOption) wdk.WalletStorageProvider {
	t.Helper()
	srv := storage.NewServer(logging.NewTestLogger(t), fake, opts...)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	client, cleanup, err := storage.NewClient(ts.URL, storage.WithClientLogger(logging.NewTestLogger(t)))
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return client
}

// TestREST_AuthDerivesAuthID proves the server authenticates the caller (default
// X-Identity-Key header) and resolves a fully-populated AuthID — its numeric
// UserID filled server-side via FindOrInsertUser, never trusted from the body.
func TestREST_AuthDerivesAuthID(t *testing.T) {
	const identity = "02abc"
	var captured wdk.AuthID
	fake := &fakeProvider{
		findUser: func(_ context.Context, idKey string) (*wdk.FindOrInsertUserResponse, error) {
			assert.Equal(t, identity, idKey, "server must resolve the authenticated identity key")
			return &wdk.FindOrInsertUserResponse{User: wdk.TableUser{UserID: 42, IdentityKey: idKey}, IsNew: false}, nil
		},
		getBalance: func(_ context.Context, auth wdk.AuthID, _ string) (uint64, error) {
			captured = auth
			return 12345, nil
		},
	}
	client := newFakeClient(t, fake)

	bal, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: identity}, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), bal)

	require.NotNil(t, captured.UserID)
	assert.Equal(t, 42, *captured.UserID, "UserID resolved server-side")
	assert.Equal(t, identity, captured.IdentityKey)
	require.NotNil(t, captured.IsActive)
	assert.True(t, *captured.IsActive)
}

// TestREST_RejectsUnauthenticated proves an auth-scoped route with no caller
// identity is rejected 401, while a storage-level route (MakeAvailable) still
// works anonymously.
func TestREST_RejectsUnauthenticated(t *testing.T) {
	client := newFakeClient(t, &fakeProvider{})

	// No identity key -> the default client authenticator sets no header ->
	// server has an anonymous caller -> 401 on an auth-scoped route.
	_, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: ""}, "")
	require.Error(t, err)
	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusUnauthorized, re.Status)
	assert.Equal(t, storage.CodeUnauthenticated, re.Code)

	// Storage-level route needs no identity.
	settings, err := client.MakeAvailable(context.Background())
	require.NoError(t, err)
	require.NotNil(t, settings)
	assert.Equal(t, "fake", settings.StorageName)
}

// forbidAuthenticator rejects every request — a stand-in for a strict
// (BRC-103/104) Authenticator that fails the handshake.
type forbidAuthenticator struct{}

func (forbidAuthenticator) Authenticate(*http.Request) (string, error) {
	return "", errors.New("handshake failed")
}

// TestREST_AuthenticatorRejection proves a pluggable Authenticator that errors
// rejects the request 401 before the provider is touched.
func TestREST_AuthenticatorRejection(t *testing.T) {
	called := false
	fake := &fakeProvider{getBalance: func(context.Context, wdk.AuthID, string) (uint64, error) {
		called = true
		return 0, nil
	}}
	client := newFakeClient(t, fake, storage.WithAuthenticator(forbidAuthenticator{}))

	_, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: "02abc"}, "")
	require.Error(t, err)
	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusUnauthorized, re.Status)
	assert.False(t, called, "provider must not be reached when auth fails")
}

// TestREST_ErrorMapping_NotEnoughFunds proves a funder.ErrNotEnoughFunds from
// the provider round-trips to a client error that errors.Is-matches BOTH the
// funder sentinel (what the provider emits) and the wdk sentinel (the public
// equivalent), with a 422 status and the mapped code.
func TestREST_ErrorMapping_NotEnoughFunds(t *testing.T) {
	fake := &fakeProvider{
		createAction: func(context.Context, wdk.AuthID, wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
			return nil, fmt.Errorf("fund selection: %w", funder.ErrNotEnoughFunds)
		},
	}
	client := newFakeClient(t, fake)

	_, err := client.CreateAction(context.Background(), wdk.AuthID{IdentityKey: "02abc"}, wdk.ValidCreateActionArgs{})
	require.Error(t, err)
	assert.ErrorIs(t, err, funder.ErrNotEnoughFunds, "matches the emitted internal sentinel")
	assert.ErrorIs(t, err, wdk.ErrNotEnoughFunds, "matches the public wdk sentinel")

	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusUnprocessableEntity, re.Status)
	assert.Equal(t, storage.CodeNotEnoughFunds, re.Code)
}

// TestREST_ErrorMapping_UTXOContention proves a raw utxostore.ErrContention —
// contention raised OUTSIDE the funder, as the fact-mode Spend on the
// accepted-broadcast path raises it — maps to 409 rather than 500.
//
// funder.ErrUTXOContention is a plain sentinel that does not wrap the utxostore
// one, so before this mapping existed a contended accept was reported to a
// remote caller as ERR_INTERNAL: "something is broken here" for a transaction
// that is merely in flight and whose apply the send sweep re-drives on its own.
// The client half matters as much as the status — a caller that retries on
// errors.Is(err, utxostore.ErrContention) has to keep working across the wire.
func TestREST_ErrorMapping_UTXOContention(t *testing.T) {
	fake := &fakeProvider{
		createAction: func(context.Context, wdk.AuthID, wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
			return nil, fmt.Errorf("storage: record spends for accepted broadcast abc: %w", utxostore.ErrContention)
		},
	}
	client := newFakeClient(t, fake)

	_, err := client.CreateAction(context.Background(), wdk.AuthID{IdentityKey: "02abc"}, wdk.ValidCreateActionArgs{})
	require.Error(t, err)
	assert.ErrorIs(t, err, utxostore.ErrContention, "the store sentinel survives the round trip")
	assert.ErrorIs(t, err, wdk.ErrUTXOContention, "and so does the public equivalent")

	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusConflict, re.Status, "contention is 'ask again' (409), not 'broken' (500)")
	assert.Equal(t, storage.CodeUTXOContention, re.Code)
}

// TestREST_ErrorMapping_NotFound proves wdk.ErrNotFoundError maps to 404 and
// round-trips with errors.Is fidelity.
func TestREST_ErrorMapping_NotFound(t *testing.T) {
	fake := &fakeProvider{
		getBalance: func(context.Context, wdk.AuthID, string) (uint64, error) {
			return 0, wdk.ErrNotFoundError
		},
	}
	client := newFakeClient(t, fake)

	_, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: "02abc"}, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, wdk.ErrNotFoundError)
	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusNotFound, re.Status)
	assert.Equal(t, storage.CodeNotFound, re.Code)
}

// TestREST_ErrorMapping_Internal proves an unmapped error becomes a 500
// RemoteError that preserves the message but does NOT falsely match a sentinel.
func TestREST_ErrorMapping_Internal(t *testing.T) {
	fake := &fakeProvider{
		getBalance: func(context.Context, wdk.AuthID, string) (uint64, error) {
			return 0, errors.New("boom: disk on fire")
		},
	}
	client := newFakeClient(t, fake)

	_, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: "02abc"}, "")
	require.Error(t, err)
	var re *storage.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusInternalServerError, re.Status)
	assert.Equal(t, storage.CodeInternal, re.Code)
	assert.Contains(t, re.Message, "disk on fire")
	assert.NotErrorIs(t, err, wdk.ErrNotEnoughFunds)
	assert.NotErrorIs(t, err, funder.ErrNotEnoughFunds)
}

// TestREST_Health proves the liveness probe is served unauthenticated.
func TestREST_Health(t *testing.T) {
	srv := storage.NewServer(logging.NewTestLogger(t), &fakeProvider{})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + storage.RouteHealth)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestREST_MaxRequestBody proves the request-body cap (WithMaxRequestBody): a
// body with a declared Content-Length over the cap is rejected 413 up front,
// and an oversized chunked/no-Content-Length body fails cleanly (400) when the
// capped read trips mid-decode — no hang, no 5xx.
func TestREST_MaxRequestBody(t *testing.T) {
	const limit = 64
	srv := storage.NewServer(logging.NewTestLogger(t), &fakeProvider{}, storage.WithMaxRequestBody(limit))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	oversized := `{"junk":"` + strings.Repeat("A", limit*4) + `"}`

	t.Run("declared content-length over cap -> 413", func(t *testing.T) {
		resp, err := http.Post(ts.URL+storage.RouteMakeAvailable, "application/json", bytes.NewReader([]byte(oversized)))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	})

	t.Run("chunked over cap -> 400", func(t *testing.T) {
		// io.NopCloser hides the concrete reader type, so net/http cannot infer
		// a Content-Length and sends the body chunked (ContentLength == 0). The
		// up-front size check can't catch it; the capped read trips mid-decode.
		req, err := http.NewRequest(http.MethodPost, ts.URL+storage.RouteMakeAvailable, io.NopCloser(strings.NewReader(oversized)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		require.Equal(t, int64(0), req.ContentLength, "body must be sent without a declared length")

		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

// TestREST_AdminAuthenticator proves WithAdminAuthenticator gates the
// storage-level routes: default (unset) leaves them anonymous; once set, a
// storage-level route without the admin credential is refused 401, and passes
// with it.
func TestREST_AdminAuthenticator(t *testing.T) {
	t.Run("default is anonymous", func(t *testing.T) {
		client := newFakeClient(t, &fakeProvider{})
		_, err := client.FindOrInsertUser(context.Background(), "02abc")
		require.NoError(t, err)
	})

	t.Run("gated rejects without credential", func(t *testing.T) {
		client := newFakeClient(t, &fakeProvider{},
			storage.WithAdminAuthenticator(storage.HeaderAuthenticator{Header: "X-Admin-Key"}))
		// The default client authenticator sets X-Identity-Key, not X-Admin-Key,
		// so the admin authenticator sees an anonymous caller.
		_, err := client.FindOrInsertUser(context.Background(), "02abc")
		require.Error(t, err)
		var re *storage.RemoteError
		require.ErrorAs(t, err, &re)
		assert.Equal(t, http.StatusUnauthorized, re.Status)
	})

	t.Run("gated passes with credential", func(t *testing.T) {
		srv := storage.NewServer(logging.NewTestLogger(t), &fakeProvider{},
			storage.WithAdminAuthenticator(storage.HeaderAuthenticator{Header: "X-Admin-Key"}))
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(ts.Close)
		client, cleanup, err := storage.NewClient(ts.URL,
			storage.WithClientAuthenticator(storage.HeaderClientAuthenticator{Header: "X-Admin-Key"}))
		require.NoError(t, err)
		t.Cleanup(cleanup)

		// FindOrInsertUser sends its identityKey via the (admin-named) header.
		_, err = client.FindOrInsertUser(context.Background(), "02admin")
		require.NoError(t, err)
	})
}

// TestREST_CustomIdentityHeader proves the identity-key header name is
// pluggable end to end (client sets it, server reads it).
func TestREST_CustomIdentityHeader(t *testing.T) {
	const header = "X-My-Identity"
	var captured wdk.AuthID
	fake := &fakeProvider{getBalance: func(_ context.Context, auth wdk.AuthID, _ string) (uint64, error) {
		captured = auth
		return 7, nil
	}}
	srv := storage.NewServer(logging.NewTestLogger(t), fake,
		storage.WithAuthenticator(storage.HeaderAuthenticator{Header: header}))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	client, cleanup, err := storage.NewClient(ts.URL,
		storage.WithClientAuthenticator(storage.HeaderClientAuthenticator{Header: header}))
	require.NoError(t, err)
	t.Cleanup(cleanup)

	bal, err := client.GetBalance(context.Background(), wdk.AuthID{IdentityKey: "02dead"}, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), bal)
	assert.Equal(t, "02dead", captured.IdentityKey)
}
