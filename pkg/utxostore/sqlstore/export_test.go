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

// The two statements idx_utxos_reserved exists for, in their production
// PostgreSQL text, exposed so sweep_explain_test.go plans exactly what runs.
// The sweep is a background tick over the WHOLE pool rather than a keyed
// lookup, so it is the statement a lost index hurts most and shows least.
var (
	// StaleReservationsPGSQL is [Store.FindStaleReservations]' statement,
	// bound to (cutoff, group limit).
	StaleReservationsPGSQL = (&Store{engine: EnginePostgres}).staleReservationsSQL()
	// ReleaseReservationPGSQL is [Store.ReleaseReservation]'s statement,
	// bound to (user id, reservation token).
	ReleaseReservationPGSQL = (&Store{engine: EnginePostgres}).releaseReservationSQL()
)
