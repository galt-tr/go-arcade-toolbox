package testenv

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// These tests run untagged (no container runtime needed): they exercise
// startContainer's ordering contract through the runContainer seam with a
// fake container, never touching testcontainers.Run or Configure.

// fakeContainer records Terminate calls. Embedding the (nil) Container
// interface satisfies the rest of the interface; only Terminate is called.
type fakeContainer struct {
	testcontainers.Container
	terminated bool
}

func (f *fakeContainer) Terminate(_ context.Context, _ ...testcontainers.TerminateOption) error {
	f.terminated = true
	return nil
}

// errFatalfSentinel is what recordingTB.Fatalf panics with, standing in for
// the real Fatalf's runtime.Goexit (Fatalf must not return normally).
var errFatalfSentinel = errors.New("recordingTB: Fatalf called")

// recordingTB records Cleanup registrations and Fatalf calls instead of
// executing them, so a test can assert on startContainer's behavior in its
// failure path. Embedding testing.TB satisfies the interface (including its
// unexported method); every method startContainer touches is overridden.
type recordingTB struct {
	testing.TB
	cleanups    []func()
	fatalMsg    string
	fatalCalled bool
}

func (r *recordingTB) Helper()             {}
func (r *recordingTB) Logf(string, ...any) {}
func (r *recordingTB) Cleanup(fn func())   { r.cleanups = append(r.cleanups, fn) }
func (r *recordingTB) Fatalf(f string, a ...any) {
	r.fatalCalled = true
	r.fatalMsg = fmt.Sprintf(f, a...)
	panic(errFatalfSentinel)
}

// callStartContainer invokes startContainer expecting its Fatalf path, and
// swallows the sentinel panic that stands in for Fatalf's Goexit.
func callStartContainer(t *testing.T, rec *recordingTB) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil && r != errFatalfSentinel { //nolint:errorlint // sentinel identity, not a wrapped chain
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	startContainer(rec, "fake", "example.invalid/simulated:1")
	t.Fatal("startContainer must not return normally when runContainer errors")
}

// TestStartContainer_TerminatesPartiallyStartedContainer pins the fix for a
// real, live-reproduced leak: testcontainers.Run is not atomic and can
// return a non-nil, already-running container TOGETHER with a non-nil error
// (start succeeds, readiness wait fails). startContainer must register the
// Terminate cleanup for that container BEFORE acting on the error —
// otherwise, with ryuk disabled under rootless podman, the half-started
// container leaks forever.
func TestStartContainer_TerminatesPartiallyStartedContainer(t *testing.T) {
	orig := runContainer
	t.Cleanup(func() { runContainer = orig })

	fake := &fakeContainer{}
	runContainer = func(_ context.Context, _ string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		// Simulate Run's non-atomic contract: container started, wait failed.
		return fake, errors.New("wait until ready: simulated readiness failure")
	}

	rec := &recordingTB{TB: t}
	callStartContainer(t, rec)

	require.True(t, rec.fatalCalled, "startContainer must still fail the test on error")
	require.Contains(t, rec.fatalMsg, "simulated readiness failure")
	require.Len(t, rec.cleanups, 1,
		"Terminate cleanup must be registered even though runContainer returned an error")

	rec.cleanups[0]() // what the testing package would run at test end
	require.True(t, fake.terminated,
		"the partially-started container must be terminated by the registered cleanup")
}

// TestStartContainer_SuccessRegistersCleanup covers the happy path: a clean
// (ctr, nil) from runContainer returns the container, fails nothing, and
// still registers the Terminate cleanup.
func TestStartContainer_SuccessRegistersCleanup(t *testing.T) {
	orig := runContainer
	t.Cleanup(func() { runContainer = orig })

	fake := &fakeContainer{}
	runContainer = func(_ context.Context, _ string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		return fake, nil
	}

	rec := &recordingTB{TB: t}
	ctr := startContainer(rec, "fake", "example.invalid/simulated:1")

	require.False(t, rec.fatalCalled, "no error means no Fatalf")
	require.Same(t, fake, ctr, "the started container must be returned")
	require.Len(t, rec.cleanups, 1)
	rec.cleanups[0]()
	require.True(t, fake.terminated)
}

// TestStartContainer_NoCleanupWithoutContainer covers the other half of the
// contract: when runContainer yields no container at all (e.g. creation
// itself failed), there is nothing to terminate and no cleanup must be
// registered.
func TestStartContainer_NoCleanupWithoutContainer(t *testing.T) {
	orig := runContainer
	t.Cleanup(func() { runContainer = orig })

	runContainer = func(_ context.Context, _ string, _ ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		return nil, errors.New("create container: simulated failure")
	}

	rec := &recordingTB{TB: t}
	callStartContainer(t, rec)

	require.True(t, rec.fatalCalled)
	require.Empty(t, rec.cleanups, "no container means no Terminate cleanup to register")
}
