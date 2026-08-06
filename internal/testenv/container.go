package testenv

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// readinessBudget is the single authoritative time budget for a started
// container to become ready (readiness = the wait strategy passing; image
// pull and container creation are budgeted separately by startContainer's
// overall context). It is passed to testcontainers.WithWaitStrategyAndDeadline
// at every call site in this package.
//
// It must be WithWaitStrategyAndDeadline, never the tempting
// testcontainers.WithWaitStrategy: that convenience wrapper is hardwired as
// WithWaitStrategyAndDeadline(60*time.Second, ...), and wait.MultiStrategy
// applies its deadline to the parent context BEFORE any inner strategy runs
// — so any longer timeout configured on an inner strategy is silently capped
// at 60 seconds. Inner strategies additionally apply their own private
// 60-second default startup timeout when none is set (wait.ForSQL,
// wait.ForListeningPort both do), so call sites must also align the inner
// strategy's timeout with this budget (see postgres.go and aerospike.go) or
// the inner default undercuts the outer deadline from the other direction.
const readinessBudget = 5 * time.Minute

// runContainer is a test seam over testcontainers.Run. Production code never
// reassigns it; the unit test in container_test.go swaps it out to simulate
// Run's non-atomic (container, error) contract without a container runtime.
var runContainer = func(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	ctr, err := testcontainers.Run(ctx, image, opts...)
	if ctr == nil {
		// Return a true untyped nil: wrapping a nil *DockerContainer in the
		// Container interface would make startContainer's `ctr != nil` check
		// pass and its cleanup call Terminate on a nil pointer.
		return nil, err
	}
	return ctr, err
}

// startContainer starts image with opts and registers the container's
// termination in tb.Cleanup, returning the running container. On any error
// it fails the test via tb.Fatalf (and does not return).
//
// CONTRACT NOTE — testcontainers.Run is NOT atomic: it can return a non-nil,
// already-running container TOGETHER with a non-nil error (container
// creation and start succeed, then a post-start hook such as the readiness
// wait strategy fails). The Terminate cleanup is therefore registered for
// any non-nil container BEFORE the error is acted on. Getting this order
// wrong is not cosmetic: with ryuk disabled under rootless podman (see
// package doc) a Fatalf before registration permanently leaks the
// half-started container — reproduced live on this machine, where a failing
// wait strategy left a running container behind under podman until it was
// removed by hand.
func startContainer(tb testing.TB, kind, image string, opts ...testcontainers.ContainerCustomizer) testcontainers.Container {
	tb.Helper()

	// Overall budget for image pull + create + start + readiness. The
	// readiness portion alone is bounded tighter, by readinessBudget, via
	// each call site's WithWaitStrategyAndDeadline option.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ctr, err := runContainer(ctx, image, opts...)
	if ctr != nil {
		// MANDATORY, and deliberately before the err check — see the
		// contract note above and the package doc's "Ryuk is disabled"
		// section.
		tb.Cleanup(func() {
			termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer termCancel()
			if terr := ctr.Terminate(termCtx); terr != nil {
				tb.Logf("testenv: terminate %s container: %v", kind, terr)
			}
		})
	}
	if err != nil {
		tb.Fatalf("testenv: start %s container (image %q): %v", kind, image, err)
	}
	return ctr
}
