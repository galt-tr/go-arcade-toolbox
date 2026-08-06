package testenv

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests exercise docker.go's pure detection logic — Configure's branch
// order, disableRyukForPodman's env precedence, and the podman socket path
// derivation — hermetically: env access goes through t.Setenv and filesystem
// probes through the statFile seam, so they run untagged on machines with no
// container runtime and never touch the real sockets.

// resetConfigureForTest re-arms Configure so each test can run a fresh
// detection pass. TEST-FILE-ONLY: production code must never reset the
// sync.Once — process-wide single detection is part of Configure's contract.
func resetConfigureForTest() {
	configureOnce = sync.Once{}
	runtimeOK = false
	unavailableWhy = ""
}

// fakeFileInfo is the minimal os.FileInfo statFile fakes need.
type fakeFileInfo struct{ mode os.FileMode }

func (f fakeFileInfo) Name() string       { return "fake" }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return false }
func (f fakeFileInfo) Sys() any           { return nil }

// setupDetectionTest gives the test a clean slate: every env var configure
// may read or write is pinned (t.Setenv also registers restoration of the
// pre-test value, covering configure's own os.Setenv writes), statFile finds
// no sockets anywhere, and the sync.Once is re-armed both now and on
// cleanup.
func setupDetectionTest(t *testing.T) {
	t.Helper()

	t.Setenv("DOCKER_HOST", "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "")
	t.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "")

	origStat := statFile
	statFile = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
	t.Cleanup(func() {
		statFile = origStat
		resetConfigureForTest()
	})
	resetConfigureForTest()
}

// socketsAt returns a statFile fake that reports a Unix socket at exactly
// the given paths and ENOENT everywhere else.
func socketsAt(paths ...string) func(string) (os.FileInfo, error) {
	return func(path string) (os.FileInfo, error) {
		for _, p := range paths {
			if path == p {
				return fakeFileInfo{mode: os.ModeSocket}, nil
			}
		}
		return nil, os.ErrNotExist
	}
}

func TestConfigure_PresetPodmanDockerHostKeptButRyukDisabled(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("DOCKER_HOST", "unix:///run/user/1000/podman/podman.sock")

	ok, reason := RuntimeAvailable()

	require.True(t, ok)
	require.Empty(t, reason)
	require.Equal(t, "unix:///run/user/1000/podman/podman.sock", os.Getenv("DOCKER_HOST"),
		"a pre-set DOCKER_HOST value must never be changed")
	require.Equal(t, "true", os.Getenv("TESTCONTAINERS_RYUK_DISABLED"),
		"ryuk must be disabled for a podman socket even when the caller set DOCKER_HOST")
	require.Equal(t, "/run/user/1000/podman/podman.sock", os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"))
}

func TestConfigure_PresetNonPodmanDockerHostLeavesRyukAlone(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("DOCKER_HOST", "tcp://docker.example.com:2376")

	ok, reason := RuntimeAvailable()

	require.True(t, ok)
	require.Empty(t, reason)
	require.Equal(t, "tcp://docker.example.com:2376", os.Getenv("DOCKER_HOST"))
	require.Empty(t, os.Getenv("TESTCONTAINERS_RYUK_DISABLED"),
		"a non-podman DOCKER_HOST must not have ryuk disabled")
	require.Empty(t, os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"))
}

func TestConfigure_DetectsRootlessPodmanSocket(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("XDG_RUNTIME_DIR", "/fake-run")
	statFile = socketsAt("/fake-run/podman/podman.sock")

	ok, reason := RuntimeAvailable()

	require.True(t, ok)
	require.Empty(t, reason)
	require.Equal(t, "unix:///fake-run/podman/podman.sock", os.Getenv("DOCKER_HOST"))
	require.Equal(t, "true", os.Getenv("TESTCONTAINERS_RYUK_DISABLED"))
	require.Equal(t, "/fake-run/podman/podman.sock", os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"))
}

func TestConfigure_FallsBackToDockerSocketWithoutRyukDisable(t *testing.T) {
	setupDetectionTest(t)
	statFile = socketsAt(dockerSocketPath)

	ok, reason := RuntimeAvailable()

	require.True(t, ok)
	require.Empty(t, reason)
	require.Empty(t, os.Getenv("DOCKER_HOST"),
		"the docker.sock fallback needs no DOCKER_HOST: testcontainers-go auto-detects it")
	require.Empty(t, os.Getenv("TESTCONTAINERS_RYUK_DISABLED"),
		"ryuk works against rootful Docker and must stay enabled")
}

func TestConfigure_PodmanSocketWinsOverDockerSocket(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("XDG_RUNTIME_DIR", "/fake-run")
	statFile = socketsAt("/fake-run/podman/podman.sock", dockerSocketPath)

	ok, _ := RuntimeAvailable()

	require.True(t, ok)
	require.Equal(t, "unix:///fake-run/podman/podman.sock", os.Getenv("DOCKER_HOST"),
		"the rootless podman socket is probed before the docker.sock fallback")
}

func TestConfigure_NoRuntimeFound(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("XDG_RUNTIME_DIR", "/fake-run")

	ok, reason := RuntimeAvailable()

	require.False(t, ok)
	require.Contains(t, reason, "systemctl --user enable --now podman.socket",
		"the skip reason must be actionable")
	require.Contains(t, reason, "/fake-run/podman/podman.sock",
		"the skip reason must name the exact socket path that was probed")
	require.Contains(t, reason, dockerSocketPath)
	require.Empty(t, os.Getenv("DOCKER_HOST"))
	require.Empty(t, os.Getenv("TESTCONTAINERS_RYUK_DISABLED"))
}

func TestConfigure_RunsDetectionOnlyOnce(t *testing.T) {
	setupDetectionTest(t)

	ok1, _ := RuntimeAvailable()
	require.False(t, ok1, "no runtime in the hermetic setup")

	// A socket appearing after the first Configure must not change the
	// already-recorded verdict: detection is once per process.
	statFile = socketsAt(dockerSocketPath)
	ok2, _ := RuntimeAvailable()
	require.False(t, ok2, "Configure must not re-run detection")
}

func TestDisableRyukForPodman_ExplicitSettingWins(t *testing.T) {
	setupDetectionTest(t)
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "false")

	disableRyukForPodman("unix:///run/user/1000/podman/podman.sock")

	require.Equal(t, "false", os.Getenv("TESTCONTAINERS_RYUK_DISABLED"),
		"an explicit caller choice (any non-empty value) must be left alone")
	require.Empty(t, os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"))
}

func TestDisableRyukForPodman_NonUnixPodmanHostSkipsSocketOverride(t *testing.T) {
	setupDetectionTest(t)

	// podman.sock over a non-unix scheme: ryuk is still disabled (podman is
	// podman) but there is no local socket path to point the override at.
	disableRyukForPodman("ssh://user@host/run/user/1000/podman/podman.sock")

	require.Equal(t, "true", os.Getenv("TESTCONTAINERS_RYUK_DISABLED"))
	require.Empty(t, os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"))
}

func TestRootlessPodmanSocket(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/custom/runtime")
	require.Equal(t, "/custom/runtime/podman/podman.sock", rootlessPodmanSocket())

	t.Setenv("XDG_RUNTIME_DIR", "")
	require.Equal(t,
		fmt.Sprintf("/run/user/%d/podman/podman.sock", os.Getuid()),
		rootlessPodmanSocket(),
		"with XDG_RUNTIME_DIR unset the path must fall back to /run/user/$UID")
}
