package sqlstore

// The production PostgreSQL claim statements, exposed to the (external) EXPLAIN
// regression test so it asserts the plan of the exact queries the hot path
// runs — one guard per claim shape.
const (
	ClaimSmallestSufficientPGSQL  = claimSmallestSufficientPG
	ClaimLargestInsufficientPGSQL = claimLargestInsufficientPG
	ClaimExactPGSQL               = claimExactPG
)

// ClaimableProbePGSQL is the production PostgreSQL text of the claimable probe
// [Store.ClaimableExists] runs, exposed for the same reason: the EXPLAIN
// regression test plans the exact statement. It must stay on idx_utxos_claim —
// the probe's whole justification is that it costs one index descent on a path
// that was already about to report insufficient funds.
var ClaimableProbePGSQL = (&Store{engine: EnginePostgres}).claimCandidateExistsSQL(shapeSmallestSufficient)
