// Package testenv provides podman-aware testcontainers helpers shared by
// go-arcade-toolbox's integration tests: container-runtime detection
// (docker.go), a Postgres helper with cheap per-test isolation
// (postgres.go), and an Aerospike helper configured to satisfy the
// aerostore provider's default-ttl=0 requirement (aerospike.go).
//
// # Fedora + rootless podman
//
// The primary target for `make test-integration` is a Fedora workstation
// running rootless podman, with no Docker installed. Set it up once:
//
//	systemctl --user enable --now podman.socket
//
// That starts the podman API socket at $XDG_RUNTIME_DIR/podman/podman.sock
// (typically /run/user/$(id -u)/podman/podman.sock for a normal login
// session). The first call into this package (StartPostgres, StartAerospike,
// or RequireRuntime) detects that socket automatically and points
// testcontainers-go at it by exporting DOCKER_HOST for the rest of the
// process — you do not need to export it yourself. `make check-podman` runs
// the same check from the shell and prints the DOCKER_HOST value, if you
// want to export it manually (e.g. to run `go test` directly without make,
// or to hand it to another tool).
//
// If DOCKER_HOST is already set in the environment, Configure leaves its
// VALUE alone: you know best if you're pointing at a remote engine, a
// rootful daemon, or something this package's detection wouldn't find on its
// own. This is also what makes the same test suite work unmodified in CI,
// where GitHub Actions' ubuntu-latest runners provide a real rootful Docker
// daemon instead of podman.
//
// # Ryuk is disabled under rootless podman
//
// testcontainers-go normally starts "ryuk", a reaper sidecar container that
// garbage-collects containers left behind if a test process dies before
// calling Terminate. Ryuk depends on privileges (mounting the engine socket
// into itself) that are unreliable under rootless podman — confirmed
// directly on a Fedora + rootless-podman machine, where ryuk failed to start
// at all ("could not start container") even with DOCKER_HOST already
// pointed at the rootless socket. Because that failure is a property of the
// socket, not of who set DOCKER_HOST, Configure disables ryuk
// (TESTCONTAINERS_RYUK_DISABLED=true) whenever the resolved DOCKER_HOST is a
// podman socket, whether Configure set that value itself (case 2 above) or
// found it already set (case 1) — see disableRyukForPodman in docker.go. A
// caller that has already exported TESTCONTAINERS_RYUK_DISABLED itself
// (to any value) is left alone.
//
// With ryuk off, each helper's t.Cleanup(terminate) is THE ONLY cleanup
// mechanism — there is no safety net. That is why every Start* helper in
// this package registers its container's Terminate in t.Cleanup before
// returning: skipping that would leak containers on every test run. If a
// test process is killed with SIGKILL (or the machine loses power) before
// cleanup runs, containers can still be left behind; run
// `make clean-containers` after an interrupted run to remove them.
//
// # go test ./... never touches a container runtime
//
// Nothing in this package performs container detection on import: Configure
// is explicit and sync.Once-guarded, and is only invoked lazily, from the
// Start* helpers or RequireRuntime. Only files tagged `//go:build integration`
// should call into this package; plain `go test ./...` (no build tags) must
// build and pass on a machine with no container runtime installed at all.
package testenv

import (
	"os"
	"testing"
)

// RequireRuntime skips the calling test, with an actionable message, if no
// container runtime is available. StartPostgres and StartAerospike already
// call this internally — use it directly only if a test needs a container
// runtime without going through one of those two helpers.
func RequireRuntime(t *testing.T) {
	t.Helper()
	ok, reason := RuntimeAvailable()
	if !ok {
		t.Skipf("testenv: %s", reason)
	}
}

// getenvDefault returns the value of the named environment variable, or def
// if it is unset or empty. Every knob this package exposes (image tags,
// credentials, namespace names) goes through this so defaults are visible in
// one place per file and overridable without code changes.
func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
