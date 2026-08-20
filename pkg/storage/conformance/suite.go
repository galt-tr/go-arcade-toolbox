package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// config holds suite options.
type config struct {
	approximateSelection bool
	rejectingProvider    func(t *testing.T) wdk.WalletStorageProvider
	rrEnv                func(t *testing.T) RejectReleaseEnv
}

// RejectReleaseEnv is everything the reject->release and concurrent-lifecycle
// subtests need beyond the provider itself.
//
// Two of the three cannot be reached through [wdk.WalletStorageProvider] and
// that is why this type exists rather than another bare constructor:
//
//   - Oracle, because the subtests must make arcade reject a specific
//     transaction, and each backend builds its own fake inside its constructor.
//   - Advance, because the release path is gated on a grace period. Without a
//     movable clock the only alternative is a test that sleeps for the grace,
//     and a test that sleeps is a test someone deletes.
//
// Every backend can supply all three: metastore, memstore, sqlstore and
// aerostore each expose WithClock, so the clock reaches all three layers that
// stamp time.
type RejectReleaseEnv struct {
	// Provider is a fresh, MIGRATED provider. Unlike the suite's plain
	// constructor, the suite does not migrate this one — the builder owns it,
	// because a backend may need the clock installed before migration.
	Provider wdk.WalletStorageProvider
	// Oracle is the fake the provider was built with, so a subtest can script
	// the verdict arcade will give for a txid it does not know in advance.
	Oracle *FakeOracle
	// Advance moves the provider's clock forward across all layers.
	Advance func(time.Duration)
}

// Option configures [RunProviderSuite].
type Option func(*config)

// WithApproximateSelection relaxes assertions that assume optimal/minimal coin
// selection (e.g. that every request in [ContentionClaim]'s shared-basket
// scramble succeeds, or a tight predicted change amount) down to
// correctness-only assertions: balance conservation, no double-spend/disjoint
// inputs, and legal error shapes on failure. Exact backends (memstore, SQL)
// should omit it, holding them to the tighter bar; an approximate/bucketed
// backend (e.g. a future Aerospike hybrid) should enable it.
//
// Note the INVERTED polarity relative to utxostoretest's WithExactSelection:
// there the default is lenient and you opt IN to strictness; here the default
// is strict and you opt OUT. Deliberate — a backend that forgets the flag
// fails loudly rather than silently under-testing itself.
func WithApproximateSelection() Option {
	return func(c *config) { c.approximateSelection = true }
}

// WithRejectingHeadersProvider registers an alternate provider constructor
// whose headers source rejects every merkle root (VerifyMerkleRoot always
// reports false — see [RejectingHeaders]), so the Internalize subtest's
// bad-BUMP-rejected assertion can run. Every backend can trivially supply
// this: it costs nothing beyond wiring a second headers fake alongside the
// primary one, the same way the primary [FakeHeaders] is wired. Without it,
// that specific assertion is skipped (not failed).
func WithRejectingHeadersProvider(newProvider func(t *testing.T) wdk.WalletStorageProvider) Option {
	return func(c *config) { c.rejectingProvider = newProvider }
}

// WithRejectReleaseEnv registers a builder for [RejectReleaseEnv], enabling the
// RejectRelease and ConcurrentLifecycle subtests. Without it both skip rather
// than fail, following the same precedent as
// [WithRejectingHeadersProvider] — a backend is never failed for a capability
// the suite cannot reach through the storage interface.
//
// Skipping is the right outcome for a genuinely-unreconcilable provider (the
// REST Client cannot: /storage/v1 exposes no reconciler route, because the
// monitor runs in-process with the Provider). It is the WRONG outcome for a real
// backend that simply has not wired the option, and the skip message says so —
// a skip nobody reads is how reject->release went unproven for four backends
// while the suite reported green.
func WithRejectReleaseEnv(build func(t *testing.T) RejectReleaseEnv) Option {
	return func(c *config) { c.rrEnv = build }
}

// RunProviderSuite runs the full provider-level conformance suite against
// providers built by newProvider. Each subtest calls newProvider exactly once
// and gets a FRESH [storage.Provider] over an isolated metastore/utxostore
// pair; newProvider should register any teardown on t via t.Cleanup and return
// an unmigrated provider — the suite calls Migrate itself.
//
// Every assertion goes through the exported [wdk.WalletStorageProvider]
// surface only (Migrate, FindOrInsertUser, InternalizeAction, CreateAction,
// ProcessAction, AbortAction, ListOutputs, FindOutputsAuth, ListTransactions,
// GetBalance, …) — never metastore/utxostore internals — so the suite runs
// unmodified against any backend combination.
func RunProviderSuite(t *testing.T, newProvider func(t *testing.T) wdk.WalletStorageProvider, opts ...Option) {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	s := &suite{newProvider: newProvider, cfg: cfg}

	for _, tc := range []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"CreateProcessRoundtrip", s.createProcessRoundtrip},
		{"ContentionClaim", s.contentionClaim},
		{"ProvidedInputExclusivity", s.providedInputExclusivity},
		{"Internalize", s.internalize},
		{"AbortRestores", s.abortRestores},
		{"AbortFence", s.abortFence},
		{"MultiUserIsolation", s.multiUserIsolation},
		{"ListAndBalanceConsistency", s.listAndBalanceConsistency},
		{"RejectRelease", s.rejectRelease},
		{"ConcurrentLifecycle", s.concurrentLifecycle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each subtest calls newProvider exactly once and gets a provider
			// over its own isolated metastore/utxostore pair, so they share no
			// state and parallelism is safe.
			//
			// It is also deliberate load: run serially, one shared PostgreSQL
			// container is never asked to serve concurrent unrelated
			// transactions, which is the condition this storage layer claims to
			// be correct under.
			t.Parallel()
			tc.fn(t)
		})
	}
}

// suite holds the shared configuration every subtest method reads.
type suite struct {
	newProvider func(t *testing.T) wdk.WalletStorageProvider
	cfg         config
}

// freshProvider builds and migrates a fresh provider for one subtest, using
// the suite's primary constructor.
func (s *suite) freshProvider(t *testing.T) wdk.WalletStorageProvider {
	t.Helper()
	return s.freshProviderFrom(t, s.newProvider)
}

// freshProviderFrom is freshProvider parameterized over the constructor, so
// subtests needing an alternate wiring (e.g. [WithRejectingHeadersProvider])
// can reuse the same migrate-and-return plumbing.
func (s *suite) freshProviderFrom(t *testing.T, newProvider func(t *testing.T) wdk.WalletStorageProvider) wdk.WalletStorageProvider {
	t.Helper()
	p := newProvider(t)
	_, err := p.Migrate(context.Background(), "conformance", "conformance-identity-key")
	require.NoError(t, err)
	return p
}

// freshEnv builds one [RejectReleaseEnv] for a subtest. The builder is
// responsible for migrating, so a backend can install its clock first; the
// suite only checks that it did.
func (s *suite) freshEnv(t *testing.T) RejectReleaseEnv {
	t.Helper()
	env := s.cfg.rrEnv(t)
	require.NotNil(t, env.Provider, "RejectReleaseEnv.Provider must be set")
	require.NotNil(t, env.Oracle, "RejectReleaseEnv.Oracle must be set")
	require.NotNil(t, env.Advance, "RejectReleaseEnv.Advance must be set")
	return env
}

// newAuth provisions a fresh user on p via FindOrInsertUser and returns its
// resolved [wdk.AuthID].
func (s *suite) newAuth(t *testing.T, p wdk.WalletStorageProvider, identityKey string) wdk.AuthID {
	t.Helper()
	resp, err := p.FindOrInsertUser(context.Background(), identityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)
	uid := resp.User.UserID
	return wdk.AuthID{IdentityKey: identityKey, UserID: &uid}
}
