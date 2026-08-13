package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// Server is the REST /storage/v1 adapter: it exposes a
// [wdk.WalletStorageProvider] over HTTP so remote wallets can talk to a
// centrally hosted storage provider. Construct it with [NewServer] and mount
// [Server.Handler] on an http.Server.
//
// Each WalletStorageProvider method is one route — the route constants are in
// rest_wire.go (RouteCreateAction, RouteListOutputs, …). Request/response
// bodies are the wdk arg/result JSON shapes verbatim. The authenticated
// caller's identity key is resolved to a [wdk.AuthID] (its numeric UserID
// filled via the provider's FindOrInsertUser) before every auth-scoped method
// runs. Typed provider errors are mapped to an HTTP status + a JSON error
// envelope the [Client] reconstructs.
//
// # Route authentication tiers
//
// Routes fall into two tiers:
//
//   - User-scoped (CreateAction, ListOutputs, GetBalance, … — 16 routes) run
//     through the [Authenticator] (WithAuthenticator) and require a resolved
//     AuthID.
//   - Storage-level (Migrate, MakeAvailable, FindOrInsertUser, GetSyncChunk,
//     ProcessSyncChunk) are NOT user-scoped and do NOT go through the
//     Authenticator. By DEFAULT they are anonymous. This is safe under the
//     default header-trust auth on a trusted network, but note that Migrate is
//     schema-admin and GetSyncChunk/ProcessSyncChunk read/mutate storage-peer
//     sync state — so once a real Authenticator (BRC-103/104) is wired via
//     WithAuthenticator, these routes STILL need a separate admin/storage-peer
//     credential. Supply one with [WithAdminAuthenticator]; leaving it unset
//     preserves the anonymous v0.1.0 behavior.
//
// # Sync-chunk transport (follow-up)
//
// GetSyncChunk/ProcessSyncChunk carry the whole chunk in one JSON body, capped
// by WithMaxRequestBody (default 1 MiB). Large-dataset sync should move to a
// streaming/paged transport — deferred; raise the cap for now.
type Server struct {
	logger    *slog.Logger
	storage   wdk.WalletStorageProvider
	auth      Authenticator
	adminAuth Authenticator
	maxBody   int64
	mux       *http.ServeMux
}

// ServerOption configures a [Server].
type ServerOption func(*Server)

// WithAuthenticator overrides the authenticator for the user-scoped routes.
// The default is a [HeaderAuthenticator] (trusts X-Identity-Key — see its
// security note). It does NOT gate the storage-level routes; use
// [WithAdminAuthenticator] for those.
func WithAuthenticator(a Authenticator) ServerOption {
	return func(s *Server) {
		if a != nil {
			s.auth = a
		}
	}
}

// WithAdminAuthenticator gates the storage-level routes (Migrate,
// MakeAvailable, FindOrInsertUser, GetSyncChunk, ProcessSyncChunk) behind an
// [Authenticator] representing an admin / storage-peer credential. When set, a
// request to any of those routes that the authenticator rejects (error) or
// leaves anonymous (empty identity) is refused with 401 before the provider is
// touched.
//
// The default is nil: storage-level routes stay anonymous, preserving v0.1.0
// behavior. Set this once a real (BRC-103/104) auth scheme is in play so these
// non-user-scoped, privileged routes are not left open — see the Server doc.
func WithAdminAuthenticator(a Authenticator) ServerOption {
	return func(s *Server) { s.adminAuth = a }
}

// WithMaxRequestBody caps an inbound request body in bytes (0 disables the cap).
// The default is 1 MiB; raise it on a deployment that serves large sync chunks.
func WithMaxRequestBody(n int64) ServerOption {
	return func(s *Server) { s.maxBody = n }
}

// NewServer builds a remote-storage REST server over the given provider.
func NewServer(logger *slog.Logger, storage wdk.WalletStorageProvider, opts ...ServerOption) *Server {
	s := &Server{
		logger:  logging.Child(logger, "storage-rest-server"),
		storage: storage,
		auth:    HeaderAuthenticator{},
		maxBody: defaultMaxRequestBody,
	}
	for _, o := range opts {
		o(s)
	}
	s.routes()
	return s
}

// Handler returns the http.Handler serving /storage/v1/*. It is safe to reuse
// across requests and goroutines.
func (s *Server) Handler() http.Handler {
	if s.maxBody > 0 {
		return maxBytesMiddleware(s.mux, s.maxBody)
	}
	return s.mux
}

// routes registers one handler per WalletStorageProvider method plus the health
// probe. Go 1.22+ method-qualified patterns give the specific routes priority.
func (s *Server) routes() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET "+RouteHealth, s.health)

	// --- storage-level routes (no resolved per-user AuthID) ---
	//
	// SECURITY: these routes do NOT go through the user Authenticator. By
	// default they are anonymous. Migrate is schema-admin; GetSyncChunk /
	// ProcessSyncChunk read/mutate storage-peer sync state; FindOrInsertUser
	// provisions users. Once a real (BRC-103/104) Authenticator is wired for
	// the user-scoped routes, these STILL need a separate admin/storage-peer
	// credential — supply it via WithAdminAuthenticator (noAuth enforces it
	// when set). Do not add a new storage-level route here without deciding
	// whether it belongs behind the admin gate.
	mux.Handle("POST "+RouteMigrate, noAuth(s, func(ctx context.Context, req migrateRequest) (migrateResponse, error) {
		to, err := s.storage.Migrate(ctx, req.StorageName, req.StorageIdentityKey)
		return migrateResponse{MigratedTo: to}, err
	}))
	mux.Handle("POST "+RouteMakeAvailable, noAuth(s, func(ctx context.Context, _ emptyRequest) (*wdk.TableSettings, error) {
		return s.storage.MakeAvailable(ctx)
	}))
	mux.Handle("POST "+RouteFindOrInsertUser, noAuth(s, func(ctx context.Context, req findOrInsertUserRequest) (*wdk.FindOrInsertUserResponse, error) {
		return s.storage.FindOrInsertUser(ctx, req.IdentityKey)
	}))
	mux.Handle("POST "+RouteGetSyncChunk, noAuth(s, func(ctx context.Context, args wdk.RequestSyncChunkArgs) (*wdk.SyncChunk, error) {
		return s.storage.GetSyncChunk(ctx, args)
	}))
	mux.Handle("POST "+RouteProcessSyncChunk, noAuth(s, func(ctx context.Context, req processSyncChunkRequest) (*wdk.ProcessSyncChunkResult, error) {
		return s.storage.ProcessSyncChunk(ctx, req.Args, req.Chunk)
	}))

	// --- auth-scoped (resolved AuthID) ---
	mux.Handle("POST "+RouteSetActive, authScoped(s, func(ctx context.Context, auth wdk.AuthID, req setActiveRequest) (emptyResponse, error) {
		return emptyResponse{}, s.storage.SetActive(ctx, auth, req.NewActiveStorageIdentityKey)
	}))
	mux.Handle("POST "+RouteCreateAction, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
		return s.storage.CreateAction(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteProcessAction, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ProcessActionArgs) (*wdk.ProcessActionResult, error) {
		return s.storage.ProcessAction(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteInternalizeAction, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.InternalizeActionArgs) (*wdk.InternalizeActionResult, error) {
		return s.storage.InternalizeAction(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteAbortAction, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.AbortActionArgs) (*wdk.AbortActionResult, error) {
		return s.storage.AbortAction(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteListActions, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ListActionsArgs) (*wdk.ListActionsResult, error) {
		return s.storage.ListActions(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteListOutputs, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ListOutputsArgs) (*wdk.ListOutputsResult, error) {
		return s.storage.ListOutputs(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteListTransactions, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ListTransactionsArgs) (*wdk.ListTransactionsResult, error) {
		return s.storage.ListTransactions(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteListCertificates, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.ListCertificatesArgs) (*wdk.ListCertificatesResult, error) {
		return s.storage.ListCertificates(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteGetBalance, authScoped(s, func(ctx context.Context, auth wdk.AuthID, req getBalanceRequest) (getBalanceResponse, error) {
		bal, err := s.storage.GetBalance(ctx, auth, req.Basket)
		return getBalanceResponse{Balance: bal}, err
	}))
	mux.Handle("POST "+RouteFindOutputBaskets, authScoped(s, func(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputBasketsArgs) (wdk.TableOutputBaskets, error) {
		return s.storage.FindOutputBasketsAuth(ctx, auth, filters)
	}))
	mux.Handle("POST "+RouteFindOutputs, authScoped(s, func(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
		return s.storage.FindOutputsAuth(ctx, auth, filters)
	}))
	mux.Handle("POST "+RouteInsertCertificate, authScoped(s, func(ctx context.Context, auth wdk.AuthID, cert wdk.TableCertificateX) (insertCertificateResponse, error) {
		id, err := s.storage.InsertCertificateAuth(ctx, auth, &cert)
		return insertCertificateResponse{CertificateID: id}, err
	}))
	mux.Handle("POST "+RouteRelinquishCertificate, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishCertificateArgs) (emptyResponse, error) {
		return emptyResponse{}, s.storage.RelinquishCertificate(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteRelinquishOutput, authScoped(s, func(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishOutputArgs) (emptyResponse, error) {
		return emptyResponse{}, s.storage.RelinquishOutput(ctx, auth, args)
	}))
	mux.Handle("POST "+RouteFindOrInsertSyncState, authScoped(s, func(ctx context.Context, auth wdk.AuthID, req findOrInsertSyncStateRequest) (*wdk.FindOrInsertSyncStateAuthResponse, error) {
		return s.storage.FindOrInsertSyncStateAuth(ctx, auth, req.StorageIdentityKey, req.StorageName)
	}))

	s.mux = mux
}

// health is a liveness probe: 200 with a small JSON body, no auth.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "storage-rest"})
}

// resolveAuth authenticates the caller and resolves a fully-populated AuthID
// (identity key + numeric UserID). The UserID is authoritative: it is derived
// server-side from the authenticated identity via FindOrInsertUser, never
// trusted from the request body, so a caller cannot act as another user.
func (s *Server) resolveAuth(r *http.Request) (wdk.AuthID, error) {
	idKey, err := s.auth.Authenticate(r)
	if err != nil {
		return wdk.AuthID{}, &httpError{status: http.StatusUnauthorized, code: CodeUnauthenticated, msg: err.Error()}
	}
	if idKey == "" {
		return wdk.AuthID{}, errUnauthenticated
	}
	resp, err := s.storage.FindOrInsertUser(r.Context(), idKey)
	if err != nil {
		return wdk.AuthID{}, err
	}
	uid := resp.User.UserID
	active := true
	return wdk.AuthID{IdentityKey: idKey, UserID: &uid, IsActive: &active}, nil
}

// requireAdmin enforces the storage-level (admin/storage-peer) gate. It is a
// no-op — anonymous access allowed — unless an admin authenticator was set via
// [WithAdminAuthenticator], in which case the request must authenticate to a
// non-empty identity or it is refused 401.
func (s *Server) requireAdmin(r *http.Request) error {
	if s.adminAuth == nil {
		return nil
	}
	idKey, err := s.adminAuth.Authenticate(r)
	if err != nil {
		return &httpError{status: http.StatusUnauthorized, code: CodeUnauthenticated, msg: err.Error()}
	}
	if idKey == "" {
		return errUnauthenticated
	}
	return nil
}

// --- generic handler glue ----------------------------------------------------

// authScoped builds a handler that resolves the AuthID, decodes the request
// body into A, invokes fn, and encodes the result R (or an error).
func authScoped[A, R any](s *Server, fn func(context.Context, wdk.AuthID, A) (R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth, err := s.resolveAuth(r)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		var args A
		if err := decodeBody(r, &args); err != nil {
			s.writeErr(w, r, badRequest(err))
			return
		}
		res, err := fn(r.Context(), auth, args)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

// noAuth builds a handler for a storage-level method that needs no resolved
// per-user AuthID: it enforces the admin gate (a no-op unless
// [WithAdminAuthenticator] is set), then decode A, invoke fn, encode R.
func noAuth[A, R any](s *Server, fn func(context.Context, A) (R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.requireAdmin(r); err != nil {
			s.writeErr(w, r, err)
			return
		}
		var args A
		if err := decodeBody(r, &args); err != nil {
			s.writeErr(w, r, badRequest(err))
			return
		}
		res, err := fn(r.Context(), args)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

// writeErr maps an error to an HTTP status + JSON error envelope. httpError
// (bad request / unauthenticated) carries its own status; otherwise a typed
// sentinel from sentinelMappings is matched with errors.Is; everything else is a
// 500 internal error echoing the message.
func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var he *httpError
	if errors.As(err, &he) {
		s.logErr(r, he.status, err)
		writeJSON(w, he.status, errorEnvelope{Error: &restError{Code: he.code, Message: he.msg}})
		return
	}
	if code, status, ok := codeStatusFor(err); ok {
		s.logErr(r, status, err)
		writeJSON(w, status, errorEnvelope{Error: &restError{Code: code, Message: err.Error()}})
		return
	}
	s.logErr(r, http.StatusInternalServerError, err)
	writeJSON(w, http.StatusInternalServerError, errorEnvelope{Error: &restError{Code: CodeInternal, Message: err.Error()}})
}

func (s *Server) logErr(r *http.Request, status int, err error) {
	lvl := slog.LevelWarn
	if status >= 500 {
		lvl = slog.LevelError
	}
	s.logger.LogAttrs(r.Context(), lvl, "request failed",
		slog.String("path", r.URL.Path), slog.Int("status", status), slog.String("error", err.Error()))
}

// --- small HTTP helpers ------------------------------------------------------

// httpError is a transport-level error carrying an explicit HTTP status + code
// (as opposed to a domain sentinel mapped through sentinelMappings).
type httpError struct {
	status int
	code   string
	msg    string
}

func (e *httpError) Error() string { return e.msg }

var errUnauthenticated = &httpError{
	status: http.StatusUnauthorized,
	code:   CodeUnauthenticated,
	msg:    "unauthenticated: missing or invalid caller identity",
}

func badRequest(err error) *httpError {
	return &httpError{status: http.StatusBadRequest, code: CodeBadRequest, msg: err.Error()}
}

// decodeBody decodes the JSON request body into out. An empty body is allowed
// (out keeps its zero value) so no-argument methods and optional-arg methods
// (e.g. GetBalance with an empty basket) work with an empty POST body.
func decodeBody(r *http.Request, out any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// maxBytesMiddleware rejects oversized bodies up front (413) and caps the read
// of everything that gets through.
func maxBytesMiddleware(next http.Handler, limit int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > limit {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}
