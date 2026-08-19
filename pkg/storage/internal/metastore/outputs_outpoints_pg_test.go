//go:build integration

package metastore_test

import (
	"testing"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
)

// TestFindOutputsByOutpoints_Postgres runs the batch lookup against PostgreSQL.
// It is the dialect the shape actually has to be argued with: PostgreSQL types
// a VALUES list's columns from its FIRST row and refuses an untyped parameter
// against a bytea column, so the casts outputTuple adds are what make the
// statement parse at all — and only this test proves they do.
func TestFindOutputsByOutpoints_Postgres(t *testing.T) {
	pg := testenv.StartPostgres(t)
	testFindOutputsByOutpoints(t, func(t *testing.T, opts ...metastore.Option) *metastore.Store {
		return newPostgresMeta(t, pg, opts...)
	})
}
