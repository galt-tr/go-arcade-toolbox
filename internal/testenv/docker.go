package testenv

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// dockerSocketPath is the conventional rootful Docker socket location, used
// as a fallback probe when no rootless podman socket is found (e.g. CI
// runners such as GitHub Actions' ubuntu-latest, which ship a real Docker
// daemon rather than podman).
const dockerSocketPath = "/var/run/docker.sock"

var (
	configureOnce sync.Once

	// Set exactly once, inside configureOnce.Do — sync.Once's memory barrier
	// makes it safe for every later RuntimeAvailable call (on any goroutine)
	// to read these without extra locking.
	runtimeOK      bool
	unavailableWhy string
)

// Configure detects the container runtime available to this process and
// adjusts testcontainers-go's environment so it works against rootless
// podman. It is cheap to call repeatedly: the actual detection logic runs at
// most once per process, guarded by sync.Once. Configure is never called
// from an init() function — importing this package (or any package that
// imports it) has no side effects; detection only happens when a test
// actually asks for a container, via StartPostgres, StartAerospike, or
// RequireRuntime.
//
// Detection order:
//
//  1. If $DOCKER_HOST is already set, Configure does not change its value —
//     the caller (or CI) knows what it's pointing at. GitHub Actions'
//     ubuntu-latest runners, for example, work fine without $DOCKER_HOST set
//     at all (a real rootful Docker daemon is auto-detected), and a
//     developer who has deliberately pointed $DOCKER_HOST at something this
//     package wouldn't find on its own should not have it overridden.
//
//  2. Otherwise, Configure looks for a rootless podman API socket at
//     $XDG_RUNTIME_DIR/podman/podman.sock, falling back to
//     /run/user/$UID/podman/podman.sock if XDG_RUNTIME_DIR is unset (both
//     are populated by a normal systemd user session with podman.socket
//     enabled). If found, Configure sets DOCKER_HOST=unix://<socket>.
//
//  3. Otherwise, Configure checks for a conventional rootful Docker socket
//     at /var/run/docker.sock. If present, nothing needs to change —
//     testcontainers-go auto-detects it the normal way.
//
//  4. If none of the above found a runtime, no container runtime is
//     available. RuntimeAvailable reports that, with an actionable message.
//
// Independently of how $DOCKER_HOST was determined above (steps 1 or 2),
// Configure then disables ryuk if the resolved socket is a podman socket —
// see disableRyukForPodman for why this happens even when $DOCKER_HOST was
// already set by the caller, not just when Configure set it itself.
func Configure() {
	configureOnce.Do(configure)
}

func configure() {
	dockerHost := os.Getenv("DOCKER_HOST")

	if dockerHost == "" {
		sock := rootlessPodmanSocket()
		switch {
		case isSocket(sock):
			dockerHost = "unix://" + sock
			_ = os.Setenv("DOCKER_HOST", dockerHost)
		case isSocket(dockerSocketPath):
			// Rootful Docker daemon: testcontainers-go auto-detects it with
			// no env changes needed, and ryuk works fine against it, so this
			// early return also (deliberately) skips the ryuk-disable below.
			runtimeOK = true
			return
		default:
			runtimeOK = false
			unavailableWhy = fmt.Sprintf(
				"no container runtime found (checked $DOCKER_HOST, %s, and %s). "+
					"On Fedora with podman installed, run: systemctl --user enable --now podman.socket "+
					"(or `make check-podman` to check/print guidance), then re-run the test. "+
					"Alternatively, install/start Docker, or set $DOCKER_HOST to point at a running "+
					"Docker- or podman-compatible engine.",
				sock, dockerSocketPath,
			)
			return
		}
	}

	runtimeOK = true
	disableRyukForPodman(dockerHost)
}

// disableRyukForPodman disables testcontainers-go's "ryuk" reaper sidecar
// whenever dockerHost resolves to a podman socket — including when
// dockerHost was already set by the caller/environment, not just when
// Configure detected and set it itself. Ryuk's unreliability under rootless
// podman is a property of the socket it's asked to run against, not of who
// pointed DOCKER_HOST there.
//
// This was not a theoretical concern: verified directly on a Fedora +
// rootless podman machine with $DOCKER_HOST already exported (by the shell
// profile, pointing at the rootless podman socket) and
// TESTCONTAINERS_RYUK_DISABLED left unset, StartPostgres and StartAerospike
// both failed identically —
//
//	reaper: new reaper: run container: ... container exited with code 1:
//	could not start container
//
// — ryuk itself cannot start under rootless podman, regardless of who
// configured DOCKER_HOST. A caller that has already made an explicit choice
// (TESTCONTAINERS_RYUK_DISABLED set to any non-empty value, including
// "false") is left alone: this function only fills in a default.
//
// TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE is also set (to the bare socket
// path, without the unix:// scheme) so that any bind-mounted-socket
// behavior inside spawned containers resolves against the same podman
// socket testcontainers-go itself is using.
func disableRyukForPodman(dockerHost string) {
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") != "" {
		return
	}
	if !strings.Contains(dockerHost, "podman.sock") {
		return
	}

	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	if sock, ok := strings.CutPrefix(dockerHost, "unix://"); ok {
		_ = os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", sock)
	}
}

// rootlessPodmanSocket returns the conventional location of the current
// user's rootless podman API socket.
func rootlessPodmanSocket() string {
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return filepath.Join(dir, "podman", "podman.sock")
	}
	return fmt.Sprintf("/run/user/%d/podman/podman.sock", os.Getuid())
}

// statFile is a test seam over os.Stat so the unit tests in docker_test.go
// can simulate socket presence/absence without touching the real filesystem.
// Production code never reassigns it.
var statFile = os.Stat

// isSocket reports whether path exists and is a Unix domain socket.
func isSocket(path string) bool {
	info, err := statFile(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSocket != 0
}

// RuntimeAvailable reports whether a container runtime (Docker, or rootless
// podman via the socket Configure detects) is usable. It calls Configure
// first, so callers never need to call Configure themselves. When available
// is false, reason is a human-readable, actionable message suitable for
// t.Skipf.
func RuntimeAvailable() (available bool, reason string) {
	Configure()
	return runtimeOK, unavailableWhy
}
