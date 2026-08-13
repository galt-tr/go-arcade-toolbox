package metastore

import (
	"strings"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// inPlaceholders returns a comma-separated run of n '?' placeholders for an
// IN (...) clause, e.g. inPlaceholders(3) == "?, ?, ?". n must be >= 1.
func inPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// joinAnd joins WHERE conditions with " AND ".
func joinAnd(conds []string) string {
	return strings.Join(conds, " AND ")
}

// joinComma joins pre-built placeholder fragments with ", ".
func joinComma(parts []string) string {
	return strings.Join(parts, ", ")
}

// anyArgs widens a []string to the []any that database/sql variadic args want.
func anyArgs(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// limitOffsetClause builds the trailing "LIMIT ? OFFSET ?" pagination clause
// and its args. A non-positive limit means "no limit": PostgreSQL spells that
// LIMIT ALL and SQLite LIMIT -1, both of which still accept the OFFSET that a
// bare OFFSET would reject on SQLite.
func (s *Store) limitOffsetClause(limit, offset int) (string, []any) {
	if limit > 0 {
		return " LIMIT ? OFFSET ?", []any{limit, offset}
	}
	if s.engine == EnginePostgres {
		return " LIMIT ALL OFFSET ?", []any{offset}
	}
	return " LIMIT -1 OFFSET ?", []any{offset}
}

// isAllMode reports whether the query mode means "match ALL of the names"
// (QueryModeAll) versus the default "match ANY of them" (QueryModeAny). A nil
// or empty mode defaults to ANY, matching defs.QueryMode.Value().
func isAllMode(mode *defs.QueryMode) bool {
	return mode != nil && *mode == defs.QueryModeAll
}
