package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// Client is the remote-storage HTTP client: a full [wdk.WalletStorageProvider]
// that POSTs each method to a [Server]'s /storage/v1/* routes, marshaling args
// and unmarshaling results via the shared wdk types and reconstructing typed
// errors. It is the drop-in remote provider a wallet uses:
//
//	client, cleanup, err := storage.NewClient("https://storage.example.com")
//	// ...
//	w, err := wallet.New(chain, key, client)
//
// Client is safe for concurrent use (its http.Client is).
//
// Sync-chunk transport (follow-up): GetSyncChunk/ProcessSyncChunk send the
// whole chunk in a single JSON body, so a chunk must fit under the server's
// request-body cap (WithMaxRequestBody, default 1 MiB). Streaming/paged sync
// for large datasets is a deferred follow-up.
type Client struct {
	baseURL string
	http    *http.Client
	auth    ClientAuthenticator
	logger  *slog.Logger
}

// compile-time proof the remote client satisfies the full provider interface.
var _ wdk.WalletStorageProvider = (*Client)(nil)

// ClientOption configures a [Client].
type ClientOption func(*clientConfig)

type clientConfig struct {
	http   *http.Client
	auth   ClientAuthenticator
	logger *slog.Logger
}

// WithHTTPClient sets the underlying *http.Client (timeouts, transport). The
// default is a zero-value http.Client (no timeout). This is also the seam a
// BRC-103/104 auth transport plugs into.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(o *clientConfig) {
		if c != nil {
			o.http = c
		}
	}
}

// WithClientAuthenticator overrides how outbound requests are authorized. The
// default is a [HeaderClientAuthenticator] (sets X-Identity-Key).
func WithClientAuthenticator(a ClientAuthenticator) ClientOption {
	return func(o *clientConfig) {
		if a != nil {
			o.auth = a
		}
	}
}

// WithClientLogger sets the client logger.
func WithClientLogger(l *slog.Logger) ClientOption {
	return func(o *clientConfig) {
		if l != nil {
			o.logger = l
		}
	}
}

// NewClient constructs a remote-storage provider pointed at baseURL (e.g.
// "https://storage.example.com" — the /storage/v1 prefix is added
// automatically). The returned cleanup func is a no-op today (HTTP is one-shot
// per call) but is part of the contract so a future pooled/streaming transport
// can release resources.
func NewClient(baseURL string, opts ...ClientOption) (wdk.WalletStorageProvider, func(), error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil, fmt.Errorf("storage client: baseURL is required")
	}
	cfg := clientConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.http == nil {
		cfg.http = &http.Client{}
	}
	if cfg.auth == nil {
		cfg.auth = HeaderClientAuthenticator{}
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    cfg.http,
		auth:    cfg.auth,
		logger:  logging.Child(cfg.logger, "storage-rest-client"),
	}
	return c, func() {}, nil
}

// RemoteError is the client's reconstruction of a non-2xx response. It exposes
// the HTTP status and the server's error code/message so callers can branch on
// them. When the code maps to known sentinels (see sentinelMappings), errors.Is
// against any of those sentinels — funder.Err*, wdk.Err*, ErrAuthorization —
// matches, so a remote failure is indistinguishable from a direct one to a
// caller using errors.Is.
type RemoteError struct {
	Status  int
	Code    string
	Message string

	// sentinels are the errors this remote error stands in for; errors.Is
	// against any of them matches. Empty for unmapped codes.
	sentinels []error
}

func (e *RemoteError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("storage remote error (%d %s): %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("storage remote error (%d): %s", e.Status, e.Message)
}

// Is reports whether this remote error stands in for target, so errors.Is
// bridges the wire for every mapped sentinel.
func (e *RemoteError) Is(target error) bool {
	for _, s := range e.sentinels {
		if errors.Is(s, target) {
			return true
		}
	}
	return false
}

// call performs one POST round-trip: marshal req, apply the client
// authenticator for identityKey, decode a 2xx body into Resp, or reconstruct a
// typed error from a non-2xx response.
func call[Req, Resp any](ctx context.Context, c *Client, path, identityKey string, req Req) (Resp, error) {
	var zero Resp

	body, err := json.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("storage client: marshal %s request: %w", path, err)
	}
	// The target is c.baseURL (operator-configured) + a fixed in-package route
	// constant — not attacker-controlled — so this is not an SSRF vector.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body)) //nolint:gosec // G704: fixed route + operator baseURL
	if err != nil {
		return zero, fmt.Errorf("storage client: build %s request: %w", path, err)
	}
	httpReq.Header.Set("Content-Type", contentTypeJSON)
	if err := c.auth.Authorize(httpReq, identityKey); err != nil {
		return zero, fmt.Errorf("storage client: authorize %s request: %w", path, err)
	}

	resp, err := c.http.Do(httpReq) //nolint:gosec // G704: request target is the operator baseURL + fixed route
	if err != nil {
		return zero, fmt.Errorf("storage client: POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, fmt.Errorf("storage client: read %s response: %w", path, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return zero, decodeRemoteError(resp.StatusCode, data)
	}

	var out Resp
	if len(bytes.TrimSpace(data)) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return zero, fmt.Errorf("storage client: unmarshal %s response (raw: %s): %w", path, truncate(data), err)
	}
	return out, nil
}

// decodeRemoteError turns a non-2xx response into a Go error. A [RemoteError]
// carrying the mapped sentinels is returned for a known code (so errors.Is
// keeps working across the wire); an unknown code, or an unparseable body,
// yields a [RemoteError] with no sentinels.
func decodeRemoteError(status int, data []byte) error {
	var env errorEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Error != nil {
		return &RemoteError{
			Status:    status,
			Code:      env.Error.Code,
			Message:   env.Error.Message,
			sentinels: sentinelsForCode(env.Error.Code),
		}
	}
	return &RemoteError{Status: status, Message: strings.TrimSpace(string(data))}
}

func truncate(b []byte) string {
	const maxLen = 512
	if len(b) > maxLen {
		return string(b[:maxLen]) + "..."
	}
	return string(b)
}

// --- WalletStorageProvider implementation ------------------------------------

// Migrate migrates the remote storage database.
func (c *Client) Migrate(ctx context.Context, storageName, storageIdentityKey string) (string, error) {
	resp, err := call[migrateRequest, migrateResponse](ctx, c, RouteMigrate, "",
		migrateRequest{StorageName: storageName, StorageIdentityKey: storageIdentityKey})
	return resp.MigratedTo, err
}

// MakeAvailable makes the remote storage available and returns its settings.
func (c *Client) MakeAvailable(ctx context.Context) (*wdk.TableSettings, error) {
	return call[emptyRequest, *wdk.TableSettings](ctx, c, RouteMakeAvailable, "", emptyRequest{})
}

// SetActive updates the active storage identity key for the authenticated user.
func (c *Client) SetActive(ctx context.Context, auth wdk.AuthID, newActiveStorageIdentityKey string) error {
	_, err := call[setActiveRequest, emptyResponse](ctx, c, RouteSetActive, auth.IdentityKey,
		setActiveRequest{NewActiveStorageIdentityKey: newActiveStorageIdentityKey})
	return err
}

// FindOrInsertUser retrieves or inserts a user by identity key.
func (c *Client) FindOrInsertUser(ctx context.Context, identityKey string) (*wdk.FindOrInsertUserResponse, error) {
	return call[findOrInsertUserRequest, *wdk.FindOrInsertUserResponse](ctx, c, RouteFindOrInsertUser, identityKey,
		findOrInsertUserRequest{IdentityKey: identityKey})
}

// InternalizeAction internalizes a transaction from outside the wallet.
func (c *Client) InternalizeAction(ctx context.Context, auth wdk.AuthID, args wdk.InternalizeActionArgs) (*wdk.InternalizeActionResult, error) {
	return call[wdk.InternalizeActionArgs, *wdk.InternalizeActionResult](ctx, c, RouteInternalizeAction, auth.IdentityKey, args)
}

// CreateAction creates a new transaction ready to be signed and processed.
func (c *Client) CreateAction(ctx context.Context, auth wdk.AuthID, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
	return call[wdk.ValidCreateActionArgs, *wdk.StorageCreateActionResult](ctx, c, RouteCreateAction, auth.IdentityKey, args)
}

// ProcessAction processes a signed transaction created by CreateAction.
func (c *Client) ProcessAction(ctx context.Context, auth wdk.AuthID, args wdk.ProcessActionArgs) (*wdk.ProcessActionResult, error) {
	return call[wdk.ProcessActionArgs, *wdk.ProcessActionResult](ctx, c, RouteProcessAction, auth.IdentityKey, args)
}

// InsertCertificateAuth adds a new certificate for the authenticated user.
func (c *Client) InsertCertificateAuth(ctx context.Context, auth wdk.AuthID, certificate *wdk.TableCertificateX) (uint, error) {
	resp, err := call[*wdk.TableCertificateX, insertCertificateResponse](ctx, c, RouteInsertCertificate, auth.IdentityKey, certificate)
	return resp.CertificateID, err
}

// RelinquishCertificate revokes a certificate from the user's certificates.
func (c *Client) RelinquishCertificate(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishCertificateArgs) error {
	_, err := call[wdk.RelinquishCertificateArgs, emptyResponse](ctx, c, RouteRelinquishCertificate, auth.IdentityKey, args)
	return err
}

// RelinquishOutput removes an output from the user's outputs.
func (c *Client) RelinquishOutput(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishOutputArgs) error {
	_, err := call[wdk.RelinquishOutputArgs, emptyResponse](ctx, c, RouteRelinquishOutput, auth.IdentityKey, args)
	return err
}

// ListCertificates lists certificates for the authenticated user.
func (c *Client) ListCertificates(ctx context.Context, auth wdk.AuthID, args wdk.ListCertificatesArgs) (*wdk.ListCertificatesResult, error) {
	return call[wdk.ListCertificatesArgs, *wdk.ListCertificatesResult](ctx, c, RouteListCertificates, auth.IdentityKey, args)
}

// ListOutputs lists wallet outputs for the authenticated user.
func (c *Client) ListOutputs(ctx context.Context, auth wdk.AuthID, args wdk.ListOutputsArgs) (*wdk.ListOutputsResult, error) {
	return call[wdk.ListOutputsArgs, *wdk.ListOutputsResult](ctx, c, RouteListOutputs, auth.IdentityKey, args)
}

// ListActions lists wallet actions for the authenticated user.
func (c *Client) ListActions(ctx context.Context, auth wdk.AuthID, args wdk.ListActionsArgs) (*wdk.ListActionsResult, error) {
	return call[wdk.ListActionsArgs, *wdk.ListActionsResult](ctx, c, RouteListActions, auth.IdentityKey, args)
}

// ListTransactions lists transactions with status updates for the authenticated user.
func (c *Client) ListTransactions(ctx context.Context, auth wdk.AuthID, args wdk.ListTransactionsArgs) (*wdk.ListTransactionsResult, error) {
	return call[wdk.ListTransactionsArgs, *wdk.ListTransactionsResult](ctx, c, RouteListTransactions, auth.IdentityKey, args)
}

// GetBalance returns spendable satoshis in a basket for the authenticated user.
func (c *Client) GetBalance(ctx context.Context, auth wdk.AuthID, basket string) (uint64, error) {
	resp, err := call[getBalanceRequest, getBalanceResponse](ctx, c, RouteGetBalance, auth.IdentityKey,
		getBalanceRequest{Basket: basket})
	return resp.Balance, err
}

// FindOutputBasketsAuth finds output baskets for the authenticated user.
func (c *Client) FindOutputBasketsAuth(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputBasketsArgs) (wdk.TableOutputBaskets, error) {
	return call[wdk.FindOutputBasketsArgs, wdk.TableOutputBaskets](ctx, c, RouteFindOutputBaskets, auth.IdentityKey, filters)
}

// FindOutputsAuth finds outputs for the authenticated user.
func (c *Client) FindOutputsAuth(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
	return call[wdk.FindOutputsArgs, wdk.TableOutputs](ctx, c, RouteFindOutputs, auth.IdentityKey, filters)
}

// AbortAction aborts an in-progress, not-yet-finalized transaction.
func (c *Client) AbortAction(ctx context.Context, auth wdk.AuthID, args wdk.AbortActionArgs) (*wdk.AbortActionResult, error) {
	return call[wdk.AbortActionArgs, *wdk.AbortActionResult](ctx, c, RouteAbortAction, auth.IdentityKey, args)
}

// GetSyncChunk retrieves a chunk of sync data (storage-to-storage; no AuthID).
func (c *Client) GetSyncChunk(ctx context.Context, args wdk.RequestSyncChunkArgs) (*wdk.SyncChunk, error) {
	return call[wdk.RequestSyncChunkArgs, *wdk.SyncChunk](ctx, c, RouteGetSyncChunk, "", args)
}

// FindOrInsertSyncStateAuth retrieves or inserts a sync state for the user.
func (c *Client) FindOrInsertSyncStateAuth(ctx context.Context, auth wdk.AuthID, storageIdentityKey, storageName string) (*wdk.FindOrInsertSyncStateAuthResponse, error) {
	return call[findOrInsertSyncStateRequest, *wdk.FindOrInsertSyncStateAuthResponse](ctx, c, RouteFindOrInsertSyncState, auth.IdentityKey,
		findOrInsertSyncStateRequest{StorageIdentityKey: storageIdentityKey, StorageName: storageName})
}

// ProcessSyncChunk applies a sync chunk (storage-to-storage; no AuthID).
func (c *Client) ProcessSyncChunk(ctx context.Context, args wdk.RequestSyncChunkArgs, chunk *wdk.SyncChunk) (*wdk.ProcessSyncChunkResult, error) {
	return call[processSyncChunkRequest, *wdk.ProcessSyncChunkResult](ctx, c, RouteProcessSyncChunk, "",
		processSyncChunkRequest{Args: args, Chunk: chunk})
}
