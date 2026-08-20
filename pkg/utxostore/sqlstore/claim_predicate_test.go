package sqlstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The claimable probe is only sound while it selects EXACTLY the rows the claim
// selects: it answers "were there claimable candidates after all?" for a claim
// that came back empty, so a predicate looser than the claim's invents
// contention (retry storms ending in ErrUTXOContention) and one tighter than
// the claim's misses it (the spurious insufficient-funds P2-4 is about).
//
// The three PostgreSQL claim statements are frozen text — claim_explain_test.go
// EXPLAINs them byte-for-byte through the exported constants — so the probe
// does not rewrite them into a shared fragment. This test is the drift guard
// instead: it slices each statement's candidate WHERE clause out and requires
// it to EQUAL the probe's predicate. Equality, not containment: a term appended
// after the satoshi comparison (say "AND basket <> 'fuel'") would still contain
// the probe's text while making the probe strictly looser than the claim.
//
// Whitespace is normalized before comparing, and only whitespace: any change to
// a column, an operator, a NULL test or the frozen spelling on either side
// fails here.
func TestClaimProbePredicateMatchesTheClaimStatements(t *testing.T) {
	shapes := []struct {
		name  string
		shape claimShape
		claim string
	}{
		{"SmallestSufficient", shapeSmallestSufficient, claimSmallestSufficientPG},
		{"LargestInsufficient", shapeLargestInsufficient, claimLargestInsufficientPG},
		{"Exact", shapeExact, claimExactPG},
	}

	for _, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			want := normalizeSQL(claimCandidatePG + " AND " + tc.shape.pg)
			assert.Equal(t, want, candidateWhereOf(t, tc.claim),
				"the probe no longer asks the same question the %s claim asks. Whichever side "+
					"moved, the two must move together: the probe exists to tell a genuinely "+
					"empty pool from one whose candidates are locked by an uncommitted peer",
				tc.name)

			// The SQLite twin is the same predicate in SQLite's dialect: '?'
			// placeholders and the "frozen = 0" spelling its partial index is
			// declared with. Deriving it mechanically pins both engines to one
			// predicate; the SQLite claim statements are BUILT from these
			// constants, so on that engine there is no second text to drift.
			assert.Equal(t, sqliteSpelling(claimCandidatePG), claimCandidateSQLite,
				"the SQLite candidate predicate drifted from the PostgreSQL one")
			assert.Equal(t, sqliteSpelling(tc.shape.pg), tc.shape.sqlite,
				"the SQLite %s satoshi condition drifted from the PostgreSQL one", tc.name)
		})
	}
}

// TestClaimProbeSQLIsANonLockingExistenceCheck pins the probe's shape rather
// than its predicate: it must be a read that takes NO locks and touches no
// rows it does not have to. A probe that acquired row locks would queue behind
// the very claim it is diagnosing (it can run on the ambient transaction), and
// one that fetched rows instead of EXISTS would pay for a scan on the
// not-enough-funds path.
func TestClaimProbeSQLIsANonLockingExistenceCheck(t *testing.T) {
	for _, engine := range []Engine{EnginePostgres, EngineSQLite} {
		t.Run(string(engine), func(t *testing.T) {
			s := &Store{engine: engine}
			for _, shape := range []claimShape{shapeSmallestSufficient, shapeLargestInsufficient, shapeExact} {
				q := s.claimCandidateExistsSQL(shape)

				assert.Contains(t, q, "SELECT EXISTS(", "the probe must be an existence check")
				for _, banned := range []string{"FOR UPDATE", "SKIP LOCKED", "UPDATE ", "ORDER BY", "LIMIT"} {
					assert.NotContains(t, q, banned,
						"%q makes the probe do more than ask whether a candidate exists: %s", banned, q)
				}

				// One bind per scope column plus the satoshi bound, in the order
				// claimableExists passes them.
				if engine == EnginePostgres {
					for _, ph := range []string{"$1", "$2", "$3", "$4"} {
						assert.Contains(t, q, ph)
					}
					assert.NotContains(t, q, "$5")
				} else {
					require.Equal(t, 4, strings.Count(q, "?"), "probe must bind exactly four parameters: %s", q)
				}
			}
		})
	}
}

// candidateWhereOf slices a claim statement's candidate predicate out: the text
// between the FIRST "WHERE " (the candidate SELECT's; the outer UPDATE's join
// predicate comes later) and the "ORDER BY" that closes it, normalized.
func candidateWhereOf(t *testing.T, stmt string) string {
	t.Helper()
	_, after, ok := strings.Cut(stmt, "WHERE ")
	require.True(t, ok, "claim statement has no WHERE clause: %s", stmt)
	where, _, ok := strings.Cut(after, "ORDER BY")
	require.True(t, ok, "claim statement has no ORDER BY closing its candidate WHERE: %s", stmt)
	return normalizeSQL(where)
}

// normalizeSQL collapses every run of whitespace to a single space so two
// spellings of the same clause compare equal regardless of how the surrounding
// statement is laid out.
func normalizeSQL(q string) string { return strings.Join(strings.Fields(q), " ") }

// sqliteSpelling rewrites a PostgreSQL predicate fragment into the SQLite
// dialect the claim statements use: positional '?' placeholders and the
// "frozen = 0" form (SQLite has no boolean type, and its partial index is
// declared with that exact text).
func sqliteSpelling(pg string) string {
	out := strings.NewReplacer("$1", "?", "$2", "?", "$3", "?", "$4", "?").Replace(pg)
	return strings.ReplaceAll(out, "NOT frozen", "frozen = 0")
}
