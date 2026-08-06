package sqlstore

// The production PostgreSQL claim statements, exposed to the (external) EXPLAIN
// regression test so it asserts the plan of the exact queries the hot path
// runs — one guard per claim shape.
const (
	ClaimSmallestSufficientPGSQL  = claimSmallestSufficientPG
	ClaimLargestInsufficientPGSQL = claimLargestInsufficientPG
	ClaimExactPGSQL               = claimExactPG
)
