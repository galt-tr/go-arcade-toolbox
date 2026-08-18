package perf

// Optimistic-ceiling parameters.
//
// These pin the most favorable transaction shape the toolbox can be asked to
// produce, so a throughput run measures the write path itself rather than the
// work a realistic payment happens to carry:
//
//   - ONE input. The pool is denominated, so the funder's ClaimExact fast path
//     closes on n=1 and never walks tiers.
//   - ONE output — the payment. No change is minted, so ProcessAction does no
//     Mint at all and the pool drains exactly one coin per transaction.
//   - No chained spending. Nothing waits for a coin to mature to
//     TierUnproven, because no transaction spends another's change.
//
// The no-change property is arithmetic, not luck. The funder donates a
// remainder below the dust floor to the fee rather than minting an
// uneconomic coin (funder/collector.go, dustFloor):
//
//	dustFloor      = ceil(minSpendTxSize/1000 * feeRate) * 2
//	               = ceil(192/1000 * 100) * 2                = 40 sat
//	tx fee (1-in, 1-out)                                     ≈ 20 sat
//	remainder      = Denomination - Payment - fee
//	               = 1050 - 1000 - 20                        = 30 sat  < 40
//
// So the remainder is donated and no change output exists. The margin either
// side is deliberately small: raise Denomination past ~1060 and a change output
// appears (which changes what the run measures); drop it toward 1020 and the
// transaction stops covering its own fee.
//
// TestOptimisticShape_OneInputNoChange in pkg/storage asserts the resulting
// shape against a real provider, so this arithmetic cannot rot silently — it is
// the guard that makes the throughput number mean what it claims.
const (
	// OptimisticDenomination is the satoshi value of every coin in the pool.
	OptimisticDenomination uint64 = 1050
	// OptimisticPaymentSats is the value of the single payment output.
	OptimisticPaymentSats uint64 = 1000
)
