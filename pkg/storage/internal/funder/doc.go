// Package funder is the wallet's coin-selection and change-computation engine.
// It answers one question — "which coins cover this transaction, and what fee
// and change does that imply?" — against the [utxostore.Store] interface, so
// the same funder is correct on every backend (SQLite, PostgreSQL, Aerospike).
//
// # BoundedTieredClaim
//
// The core algorithm is a claim-as-you-go, target-aware walk. For each status
// tier (safest first — see [SpendTiers]) it issues bounded micro-queries
// against the store:
//
//   - ClaimSmallestSufficient reserves the smallest coin that alone covers the
//     freshly recomputed remaining need. If one exists it is allocated and the
//     loop recomputes.
//   - Otherwise ClaimLargestInsufficient reserves a bounded batch of the
//     largest coins below the remaining need; the funder drains them largest
//     first while each stays strictly below the (recomputed) remaining, then
//     releases the untouched tail so it re-enters the claimable pool. The
//     moment a batch row would be sufficient, draining stops and the outer loop
//     re-issues the sufficient claim — reproducing per-step best-fit selection.
//
// Because each claim atomically reserves the rows it returns, the funder needs
// no cross-call lock and no exclusion-list threading: rows it has taken are
// simply reserved and invisible to its own subsequent claims, and rows it
// declines are released explicitly. Cost is flat in pool size.
//
// A full walk over every tier that allocates nothing yields [ErrNotEnoughFunds]
// (reservation released) — unless the pool says otherwise; see the next
// section. [utxostore.ErrContention] triggers a bounded, jittered retry of the
// whole selection; persistent contention yields [ErrUTXOContention]
// (reservation released).
//
// # Telling a starved pass from an empty pool
//
// A SQL claim reserves rows FOR UPDATE SKIP LOCKED. In Mode A (utxostore and
// metastore sharing one transaction) those locks are held until the caller's
// whole CreateAction commits, so a CONCURRENT CreateAction skips the locked
// rows and sees a pool that looks empty but is not. Reporting
// ErrNotEnoughFunds there is a lie the user feels: the funder does not retry
// exhaustion, so a payment fails while the coins are still in the wallet and
// may yet be released by a rollback (audit finding P2-4).
//
// So a pass that is ABOUT to report ErrNotEnoughFunds asks the store one more
// question first, through the optional ClaimableExists capability: is there a
// claimable coin in any of these tiers right now? If there is, the walk should
// have found it, so it was hidden — the funder reports [utxostore.ErrContention]
// instead and its own retry loop handles it. If there is not, the answer stays
// ErrNotEnoughFunds. Stores that do not implement the capability (memstore,
// whose claims are mutex-serialized; aerostore, whose claims already report
// contention natively) are unaffected.
//
// Two properties matter enough to state plainly, because both were violated by
// the obvious design of probing inside the store on every empty claim:
//
//   - NOTHING is probed on the allocating path. An empty ClaimSmallestSufficient
//     is the ordinary precondition of the drain, and a denominated ClaimExact
//     misses on every tier holding no fuel, so probing there would add a query
//     per payment at full throughput. The probe runs at most once per tier, on a
//     pass that already failed.
//   - Contention is reported ONLY when the WHOLE pass allocated nothing. A
//     mid-walk empty stays "this tier allocated nothing" and the walk continues,
//     so a single big coin locked by an uncommitted peer can never pre-empt a
//     fund that a lower tier could have covered.
//
// # Transient over-reservation under concurrency
//
// The largest-insufficient path reserves a whole batch (up to
// insufficientBatchSize = 16 rows) before releasing the surplus at the end of
// each round. Under concurrency on a tight pool this means several funders can
// transiently hold far more rows than they ultimately keep, so a peer can find
// the pool momentarily empty and return a spurious — but still correct —
// [ErrNotEnoughFunds] even though enough coins exist once the surplus is
// released. This is never a safety violation: no coin is ever double-allocated,
// and nothing leaks. Callers that need to avoid the spurious failure should
// either retry or size the pool above the aggregate peak reservation
// (funders × 16). The denominated throughput fast path avoids the effect
// entirely: it claims exact coins one denomination at a time, never over-reserving.
//
// # Change computation
//
// As coins are allocated the collector recomputes the fee from the transaction
// size and derives the change shape: a change-output count driven by the
// basket's desired-UTXO parameters, clamped to the per-transaction cap and the
// basket's remaining headroom (NumberOfDesiredUTXOs − ExistingBasketCount), and
// reduced while any per-output value would fall below the dust floor (2× the
// fee of a minimal future spend). Change below the dust floor is donated to the
// fee. This math is a behavioral port of go-wallet-toolbox's funder.
//
// Donating sub-dust change means the transaction comes out with one fewer
// output than the caller planned. A transaction with no other output would then
// be invalid, so the walk already keeps allocating until the change clears the
// dust floor in that case; [FundArgs.RequireChange] extends the same guarantee
// to callers whose spending conditions commit to the output shape.
//
// ExistingBasketCount is an INPUT, fetched by the caller before it opens any
// database transaction: fetching it lazily inside the funder would deadlock a
// single-connection SQLite transaction.
//
// # Throughput fast path
//
// When a denominated fuel pool is configured (FundArgs.Denomination > 0) the
// funder skips straight to a closed-form ClaimExact of the required number of
// denomination coins, topping up any residual need or pool underflow through
// the bounded walk. [ShapeChange] is the companion primitive the fan-out
// keeper uses to size denominated change.
package funder
