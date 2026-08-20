package storage

import (
	"errors"
	"net/http"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// This file defines the shared wire contract for the REST /storage/v1 remote
// storage transport: the route table, the small request/response envelopes for
// methods whose body is not a single wdk arg/result type, and the
// sentinel<->code<->HTTP-status mapping used by both the [Server] (encode) and
// the [Client] (decode). Client and server share the wdk arg/result types
// verbatim (same JSON tags), so no per-method DTOs exist beyond the scalar
// envelopes below.

// RESTPathPrefix is the URL prefix under which every remote-storage route is
// served. One route per [wdk.WalletStorageProvider] method, plus a health
// probe.
const RESTPathPrefix = "/storage/v1"

// Route paths. Each constant below maps 1:1 to a WalletStorageProvider method
// (the constant name is the method name), except RouteHealth which is a
// liveness probe. This block IS the authoritative route map.
const (
	RouteHealth                = RESTPathPrefix + "/health"
	RouteMigrate               = RESTPathPrefix + "/migrate"
	RouteMakeAvailable         = RESTPathPrefix + "/makeAvailable"
	RouteSetActive             = RESTPathPrefix + "/setActive"
	RouteFindOrInsertUser      = RESTPathPrefix + "/findOrInsertUser"
	RouteInternalizeAction     = RESTPathPrefix + "/internalizeAction"
	RouteCreateAction          = RESTPathPrefix + "/createAction"
	RouteProcessAction         = RESTPathPrefix + "/processAction"
	RouteInsertCertificate     = RESTPathPrefix + "/insertCertificate"
	RouteRelinquishCertificate = RESTPathPrefix + "/relinquishCertificate"
	RouteRelinquishOutput      = RESTPathPrefix + "/relinquishOutput"
	RouteListCertificates      = RESTPathPrefix + "/listCertificates"
	RouteListOutputs           = RESTPathPrefix + "/listOutputs"
	RouteListActions           = RESTPathPrefix + "/listActions"
	RouteListTransactions      = RESTPathPrefix + "/listTransactions"
	RouteGetBalance            = RESTPathPrefix + "/getBalance"
	RouteFindOutputBaskets     = RESTPathPrefix + "/findOutputBaskets"
	RouteFindOutputs           = RESTPathPrefix + "/findOutputs"
	RouteAbortAction           = RESTPathPrefix + "/abortAction"
	RouteGetSyncChunk          = RESTPathPrefix + "/getSyncChunk"
	RouteFindOrInsertSyncState = RESTPathPrefix + "/findOrInsertSyncState"
	RouteProcessSyncChunk      = RESTPathPrefix + "/processSyncChunk"
)

// IdentityKeyHeader is the header the default (simple) auth seam uses to carry
// the caller's identity key. See [HeaderAuthenticator].
const IdentityKeyHeader = "X-Identity-Key"

const contentTypeJSON = "application/json"

// defaultMaxRequestBody caps an inbound request body (1 MiB), matching the
// reference server's default. Sync chunks can be larger; raise via
// [WithMaxRequestBody] on a sync-serving deployment.
const defaultMaxRequestBody int64 = 1 << 20

// --- Scalar/multi-arg request & response envelopes ---------------------------
//
// Methods whose body is a single wdk arg type (CreateAction, ListOutputs, ...)
// send/receive that type directly. The methods below take scalars or multiple
// args, so they get a tiny named envelope shared by client and server.

type migrateRequest struct {
	StorageName        string `json:"storageName"`
	StorageIdentityKey string `json:"storageIdentityKey"`
}

type migrateResponse struct {
	MigratedTo string `json:"migratedTo"`
}

type setActiveRequest struct {
	NewActiveStorageIdentityKey string `json:"newActiveStorageIdentityKey"`
}

type findOrInsertUserRequest struct {
	IdentityKey string `json:"identityKey"`
}

type getBalanceRequest struct {
	Basket string `json:"basket"`
}

type getBalanceResponse struct {
	Balance uint64 `json:"balance"`
}

type insertCertificateResponse struct {
	CertificateID uint `json:"certificateId"`
}

type findOrInsertSyncStateRequest struct {
	StorageIdentityKey string `json:"storageIdentityKey"`
	StorageName        string `json:"storageName"`
}

type processSyncChunkRequest struct {
	Args  wdk.RequestSyncChunkArgs `json:"args"`
	Chunk *wdk.SyncChunk           `json:"chunk"`
}

// emptyRequest is the body for no-argument methods (MakeAvailable). emptyResponse
// is the body for methods that only return an error (SetActive, Relinquish*).
type (
	emptyRequest  struct{}
	emptyResponse struct{}
)

// --- Error wire shape + mapping ----------------------------------------------

// errorEnvelope is the JSON body of a non-2xx response.
type errorEnvelope struct {
	Error *restError `json:"error"`
}

type restError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes carried in restError.Code. The client maps a known code back to
// the matching sentinel so errors.Is keeps working across the wire; unknown
// codes surface as a [RemoteError].
const (
	CodeNotEnoughFunds   = "ERR_NOT_ENOUGH_FUNDS"
	CodeUTXOContention   = "ERR_UTXO_CONTENTION"
	CodeInputUnavailable = "ERR_INPUT_UNAVAILABLE"
	CodeNotFound         = "ERR_NOT_FOUND"
	CodeAuthorization    = "ERR_AUTHORIZATION"
	CodeUnauthenticated  = "ERR_UNAUTHENTICATED"
	CodeBadRequest       = "ERR_BAD_REQUEST"
	CodeInternal         = "ERR_INTERNAL"

	// CodeDivergentReDrive carries [ErrDivergentReDrive]: a ProcessAction whose
	// reference is already bound to different bytes.
	CodeDivergentReDrive = "ERR_DIVERGENT_REDRIVE"
	// CodeAbortLostToSend carries [ErrAbortLostToSend]: an abort that lost to a
	// broadcaster which had already claimed the raw transaction.
	CodeAbortLostToSend = "ERR_ABORT_LOST_TO_SEND"
	// CodeNotAbortable carries the generic [wdk.ErrNotAbortableAction]: every
	// abort refusal that is NOT [ErrAbortLostToSend]. Three of them come from
	// [Provider.AbortAction]'s own gate before any fence runs — the reference
	// resolves to no transaction, the transaction is not outgoing, its status is
	// not in abortableStatuses — plus the wallet-side status CAS losing to a
	// concurrent transition inside [Provider.fenceAborted].
	//
	// All four are 409, INCLUDING the unresolved reference, which is a
	// deliberate choice and not an oversight. 404 on this route would say "no
	// such endpoint": the route exists and was dispatched, and it is the
	// reference in the BODY that resolved to nothing. 409 also carries the
	// property that matters operationally — non-retryable. The status it
	// replaces was an unmapped 500, which invites exactly the wrong behavior
	// from a retrying mesh or client for a refusal that will never change its
	// mind.
	//
	// ORDERING TRAP for anyone revisiting that call: this mapping sits ABOVE
	// CodeNotFound in sentinelMappings, and codeStatusFor stops at the first
	// match. Adding [wdk.ErrNotFoundError] alongside the sentinel at the
	// unresolved-reference refusal would therefore change nothing on the wire —
	// the error would still match this row first and still report 409. Giving
	// that case a 404 lane means a NEW sentinel with its own mapping row placed
	// ABOVE this one, which is the same specific-before-generic pattern
	// [ErrAbortLostToSend] uses.
	CodeNotAbortable = "ERR_NOT_ABORTABLE"
)

// sentinelMapping ties a wire code + HTTP status to the set of sentinel errors
// it represents. It is the single source of truth consulted by the server (an
// error -> code+status) and the client (a code -> the sentinels a reconstructed
// error must satisfy errors.Is for).
//
// A code carries MULTIPLE sentinels because the same failure surfaces under
// both a domain sentinel and its internal origin: the provider's funding path
// returns funder.ErrNotEnoughFunds / funder.ErrUTXOContention (what the
// conformance suite asserts), while the public wdk.Err* sentinels are the
// interface-level equivalents a remote caller may check. Mapping both lets a
// reconstructed client error match either, so errors.Is keeps working across
// the wire regardless of which sentinel the caller tests against.
//
// utxostore.ErrContention is in that set for the same reason and a different
// path. funder.ErrUTXOContention is a plain sentinel that does NOT wrap it, so
// contention raised OUTSIDE the funder — most importantly by the fact-mode
// Spend on the accepted-broadcast path — matched no mapping and left as a 500
// ERR_INTERNAL. That is the wrong thing to tell a caller about a transaction
// that is in flight and whose apply the send sweep will simply re-drive: 409 is
// "ask again", 500 is "something is broken here".
type sentinelMapping struct {
	code      string
	status    int
	sentinels []error
}

// ORDER MATTERS for codeStatusFor, which returns the FIRST mapping the error
// matches. Provided-input unavailability leads because it is the most specific
// statement about the failure: only the named-input path produces it, while a
// funding sentinel could plausibly be alongside it in a wrapped chain.
//
// The abort pair is ordered for the same reason and needs it more, since one is
// deliberately a subset of the other: [ErrAbortLostToSend] WRAPS
// [wdk.ErrNotAbortableAction], so the specific row must come first or the
// generic one would swallow it and the wire would lose the only abort
// distinction a caller can act on. The specific row lists just its own
// sentinel — the client's reconstruction matches through the wrap, so a remote
// caller testing the BRC-100 sentinel still succeeds.
var sentinelMappings = []sentinelMapping{
	{CodeInputUnavailable, http.StatusConflict, []error{wdk.ErrInputUnavailable}},
	{CodeAbortLostToSend, http.StatusConflict, []error{ErrAbortLostToSend}},
	{CodeNotAbortable, http.StatusConflict, []error{wdk.ErrNotAbortableAction}},
	{CodeDivergentReDrive, http.StatusConflict, []error{ErrDivergentReDrive}},
	{CodeNotEnoughFunds, http.StatusUnprocessableEntity, []error{funder.ErrNotEnoughFunds, wdk.ErrNotEnoughFunds}},
	{CodeUTXOContention, http.StatusConflict, []error{funder.ErrUTXOContention, wdk.ErrUTXOContention, utxostore.ErrContention}},
	{CodeAuthorization, http.StatusForbidden, []error{ErrAuthorization}},
	{CodeNotFound, http.StatusNotFound, []error{wdk.ErrNotFoundError}},
}

// codeStatusFor scans the mappings for the first sentinel err matches, returning
// its wire code and HTTP status.
func codeStatusFor(err error) (code string, status int, ok bool) {
	for _, m := range sentinelMappings {
		for _, s := range m.sentinels {
			if errors.Is(err, s) {
				return m.code, m.status, true
			}
		}
	}
	return "", 0, false
}

// sentinelsForCode returns the sentinels a reconstructed client error should
// satisfy errors.Is for, or nil for an unknown code.
func sentinelsForCode(code string) []error {
	for _, m := range sentinelMappings {
		if m.code == code {
			return m.sentinels
		}
	}
	return nil
}
