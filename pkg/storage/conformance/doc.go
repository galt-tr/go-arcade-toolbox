// Package conformance is the backend-agnostic, exported provider-level
// conformance suite for [storage.Provider]. It exercises the SAME behavioral
// assertions — proven once, by hand, in pkg/storage's own provider_test.go,
// process_test.go, internalize_abort_test.go and provider_integration_test.go
// — against ANY (utxostore backend x metastore engine) combination a caller
// wires up, by driving the provider exclusively through the exported
// [wdk.WalletStorageProvider] surface. It never reaches into the metastore or
// utxostore internals: every assertion is something a real wallet client could
// observe by calling the storage API.
//
// # Usage
//
//	func TestProviderConformance(t *testing.T) {
//		conformance.RunProviderSuite(t, func(t *testing.T) *storage.Provider {
//			// build a fresh metastore + utxostore + funder + Provider here.
//			return p
//		})
//	}
//
// newProvider MUST return a freshly constructed, unmigrated [storage.Provider]
// backed by an isolated metastore/utxostore pair every time it is called — the
// suite calls it once per subtest (mirroring the utxostoretest pattern) and
// migrates it itself.
//
// # Layout and import direction
//
// This package lives at pkg/storage/conformance, one level below pkg/storage,
// and imports pkg/storage (for the [storage.Provider] type the suite drives)
// plus the storage-internal funder package ONLY for its sentinel errors used
// in behavioral assertions (funder.ErrNotEnoughFunds, funder.ErrUTXOContention)
// — never for state introspection. That internal import is legal because
// pkg/storage/conformance sits inside the pkg/storage/ tree that Go's
// internal-package rule grants access to. (The wiring test files additionally
// touch the internal metastore for construction, but the conformance package
// itself does not.)
//
// The wiring test files that use this package (pkg/storage's
// conformance_sqlite_test.go and conformance_pg_test.go) live in package
// storage_test: they import both pkg/storage and pkg/storage/conformance,
// which itself imports pkg/storage. This is acyclic — pkg/storage never
// imports pkg/storage/conformance — so there is no import cycle to route
// around.
//
// # Options
//
// [WithApproximateSelection] relaxes assertions that assume optimal/minimal
// coin selection (e.g. exact successful-claim counts under contention) down to
// correctness-only assertions (balance conservation, no double-spend, legal
// error shapes on failure). Exact backends (memstore, SQL) omit it so the
// suite holds them to the tighter bar; a future approximate/bucketed backend
// (e.g. the Aerospike hybrid, M5) enables it.
//
// Its polarity is INVERTED relative to utxostoretest's WithExactSelection:
// there the default is lenient and a backend opts IN to the strict bar; here
// the default is strict and a backend opts OUT of it. This is deliberate — a
// backend that forgets the flag fails loudly (held to the tighter bar it may
// not meet) rather than silently under-testing itself.
//
// [WithRejectingHeadersProvider] registers an alternate provider constructor
// whose headers source rejects every merkle root, so the Internalize subtest
// can assert a bad BUMP is rejected and nothing is persisted. Every backend
// can trivially supply this (it costs nothing beyond wiring a second headers
// fake alongside the primary one); without it, that specific assertion is
// skipped rather than failed.
//
// # Deferred
//
// RejectRelease — asserting that a verified-reject (a final on-chain
// rejection) releases its reserved inputs and that suspectFailed known-txs
// eventually get swept — needs the M4 reconciler, which does not exist yet.
// storage.Provider.ProcessAction today deliberately leaves rejected-tx inputs
// reserved (that release is the reconciler's job). See the commented stub in
// [RunProviderSuite]'s subtest table.
package conformance
