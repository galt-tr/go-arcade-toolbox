// Package services is the public-API compatibility shim over the toolbox's
// lean arcade-only internals.
//
// The rest of the toolbox (pkg/storage) talks directly to [arcade.TxOracle]
// and [headers.Headers] — the two narrow contracts the arcade-only design
// settled on (see docs/arcade-only-design.md §3.2). But the old
// go-wallet-toolbox public surface was built around a much wider
// [wdk.Services] interface (broadcast, merkle proofs, tx status, block
// headers, nlocktime), and callers who still hold code written against that
// interface (or who construct a [wdk.Services] themselves, e.g. a test double
// or a bespoke integration) need somewhere to plug in.
//
// [Services] is that adapter: it implements the full [wdk.Services] interface
// by delegating every method to an [arcade.TxOracle] + a [headers.Headers],
// so `WithServices(wdk.Services)`-shaped call sites keep compiling and working
// unchanged, while everything internal to this repo (pkg/storage) stays on
// the lean contracts.
//
// # What is intentionally NOT here
//
// The old concrete WalletServices additionally exposed GetUtxoStatus, IsUtxo,
// GetScriptHashHistory and BsvExchangeRate. None of those are part of the
// [wdk.Services] INTERFACE (verified against pkg/wdk/services.interface.go —
// it embeds chaintracker.ChainTracker and BlockHeaderLoader and adds exactly
// PostFromBEEF/MerklePath/FindChainTipHeader/RawTx/GetBEEF/NLockTimeIsFinal/
// GetStatusForTxIDs, ten methods in total), so dropping them costs nothing on
// the wdk.Services compatibility promise. Likewise the multi-provider
// fallback queues and the broadcast router's failover chain from the old
// go-wallet-toolbox are deleted: Arcade is the single broadcast target, and
// [Services.PostFromBEEF] relies entirely on the circuit-breaker/backpressure
// behavior already built into [arcade.Client] rather than re-implementing
// failover here.
//
// # The local-first RawTx/GetBEEF seam
//
// [Services.RawTx] and [Services.GetBEEF] consult an optional [RawTxSource]
// before falling back to the Arcade oracle. This exists so a wallet's own
// storage (which retains full ancestry for its own transactions) does not
// have to pay a network round trip — or worse, come up empty for a tx Arcade
// itself never learned about — for data it already has on disk. This package
// deliberately does NOT import pkg/storage to get that data: pkg/storage
// already depends on pkg/services' sibling packages (arcade, headers), and a
// services -> storage import would close a cycle back through the future
// pkg/wallet composition root (wallet -> services, wallet -> storage). Instead
// [RawTxSource] is a small interface Services declares; the composition root
// wires a storage-backed implementation via [WithLocalRawTxSource].
//
// # OracleFromServices — the reverse bridge
//
// [OracleFromServices] goes the other direction: it lets a caller who has a
// (possibly hand-rolled) [wdk.Services] hand it to something that wants an
// [arcade.TxOracle] — e.g. pkg/storage's constructor. This is necessarily a
// DEGRADED bridge: wdk.Services has no push/event surface, so the returned
// oracle's StreamStatus always fails fast (see its doc comment for the exact
// contract), and GetTx's status resolution is coarser than Arcade's own
// lifecycle (collapsed to mined/known/not-found, since that is all
// GetStatusForTxIDs exposes on the wdk.Services interface).
package services
