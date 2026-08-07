package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// config holds suite options.
type config struct {
	approximateSelection bool
	rejectingProvider    func(t *testing.T) *storage.Provider
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
func WithRejectingHeadersProvider(newProvider func(t *testing.T) *storage.Provider) Option {
	return func(c *config) { c.rejectingProvider = newProvider }
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
func RunProviderSuite(t *testing.T, newProvider func(t *testing.T) *storage.Provider, opts ...Option) {
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
		{"Internalize", s.internalize},
		{"AbortRestores", s.abortRestores},
		{"MultiUserIsolation", s.multiUserIsolation},
		{"ListAndBalanceConsistency", s.listAndBalanceConsistency},

		// TODO(M4 - RejectRelease): a verified on-chain rejection (a final,
		// signed 4xx/definitive reject from the tx oracle) must release its
		// reservation's still-unspent inputs and the suspectFailed known-tx
		// must eventually be swept. storage.Provider.ProcessAction's reject
		// path deliberately leaves rejected-tx inputs reserved today — see
		// TestProcessAction_Reject in pkg/storage/process_test.go — because
		// that release is the M4 reconciler's job, which does not exist yet.
		// Wire a "RejectRelease" subtest in here once it lands.
	} {
		t.Run(tc.name, tc.fn)
	}
}

// suite holds the shared configuration every subtest method reads.
type suite struct {
	newProvider func(t *testing.T) *storage.Provider
	cfg         config
}

// freshProvider builds and migrates a fresh provider for one subtest, using
// the suite's primary constructor.
func (s *suite) freshProvider(t *testing.T) *storage.Provider {
	t.Helper()
	return s.freshProviderFrom(t, s.newProvider)
}

// freshProviderFrom is freshProvider parameterized over the constructor, so
// subtests needing an alternate wiring (e.g. [WithRejectingHeadersProvider])
// can reuse the same migrate-and-return plumbing.
func (s *suite) freshProviderFrom(t *testing.T, newProvider func(t *testing.T) *storage.Provider) *storage.Provider {
	t.Helper()
	p := newProvider(t)
	_, err := p.Migrate(context.Background(), "conformance", "conformance-identity-key")
	require.NoError(t, err)
	return p
}

// newAuth provisions a fresh user on p via FindOrInsertUser and returns its
// resolved [wdk.AuthID].
func (s *suite) newAuth(t *testing.T, p *storage.Provider, identityKey string) wdk.AuthID {
	t.Helper()
	resp, err := p.FindOrInsertUser(context.Background(), identityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)
	uid := resp.User.UserID
	return wdk.AuthID{IdentityKey: identityKey, UserID: &uid}
}
