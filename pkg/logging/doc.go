// Package logging provides small helpers around log/slog shared across
// go-arcade-toolbox: a logger configurer, a resty logging adapter, and
// test-only helpers for capturing and asserting on log output.
//
// NOTE: this package was ported ahead of its own dedicated task because
// pkg/wallet/pending depends on logging.Child. All production files
// (configurer.go, resty_adapter.go, test_logger.go, test_writer.go, utils.go)
// and configurer_test.go were ported verbatim (import-path rewrite only).
// Only utils_test.go was intentionally left out: it additionally requires
// pkg/internal/satoshi, which is outside the scope of the task that
// introduced this package. A future task that ports utils_test.go (and
// pkg/internal/satoshi) should reconcile with the files here rather than
// assume the package is new.
package logging
