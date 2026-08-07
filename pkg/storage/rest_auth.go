package storage

import "net/http"

// Authenticator authenticates an inbound HTTP request to the remote-storage
// [Server] and returns the caller's identity key (a hex-encoded compressed
// secp256k1 public key). It is the server-side seam for pluggable auth.
//
// Returning ("", nil) marks the caller anonymous: routes that need a resolved
// [wdk.AuthID] then reject with 401, while auth-free routes (Migrate,
// MakeAvailable, the sync trio) still run. Returning a non-nil error rejects
// the request outright with 401.
//
// The default is [HeaderAuthenticator]. A production deployment on an untrusted
// network should supply a real mutual-auth implementation (BRC-103/104 via
// go-bsv-middleware) through [WithAuthenticator] — see the package doc for the
// deferred follow-up.
type Authenticator interface {
	Authenticate(r *http.Request) (identityKey string, err error)
}

// HeaderAuthenticator is the DEFAULT server Authenticator. It trusts the
// caller's identity key taken verbatim from a request header (default
// [IdentityKeyHeader], "X-Identity-Key").
//
// SECURITY: this performs NO cryptographic verification — any caller can claim
// any identity by setting the header. It is appropriate only for trusted
// networks (a private cluster, localhost, a gateway that terminates real auth
// upstream) and for tests. Do NOT expose a HeaderAuthenticator-backed server to
// an untrusted network.
type HeaderAuthenticator struct {
	// Header overrides the identity-key header name. Empty uses IdentityKeyHeader.
	Header string
}

// Authenticate implements [Authenticator].
func (a HeaderAuthenticator) Authenticate(r *http.Request) (string, error) {
	return r.Header.Get(a.headerName()), nil
}

func (a HeaderAuthenticator) headerName() string {
	if a.Header != "" {
		return a.Header
	}
	return IdentityKeyHeader
}

// ClientAuthenticator decorates an outbound request from the remote-storage
// [Client] with the caller's identity/credentials. It is the client-side mirror
// of [Authenticator] — the seam a BRC-103/104 signer slots into later.
//
// identityKey is the identity the call is made on behalf of (from the AuthID of
// the method, or the identityKey argument for FindOrInsertUser); it is empty for
// storage-level methods (Migrate, MakeAvailable, the sync-chunk methods).
type ClientAuthenticator interface {
	Authorize(req *http.Request, identityKey string) error
}

// HeaderClientAuthenticator is the DEFAULT client authenticator. It sets the
// identity-key header (default [IdentityKeyHeader]) to identityKey. It is the
// exact counterpart of [HeaderAuthenticator] and carries the same security
// caveat: the identity is asserted, not proven.
type HeaderClientAuthenticator struct {
	// Header overrides the identity-key header name. Empty uses IdentityKeyHeader.
	Header string
}

// Authorize implements [ClientAuthenticator].
func (a HeaderClientAuthenticator) Authorize(req *http.Request, identityKey string) error {
	if identityKey == "" {
		return nil
	}
	name := IdentityKeyHeader
	if a.Header != "" {
		name = a.Header
	}
	req.Header.Set(name, identityKey)
	return nil
}
