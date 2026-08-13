package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
)

// smokeConfig returns a self-contained SQLite config with the monitor disabled
// (so no ChainTracks/Arcade SSE dials happen at boot) and dummy — never dialed
// during a boot+health+shutdown smoke — oracle/headers URLs.
func smokeConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.SQLitePath = filepath.Join(t.TempDir(), "smoke.db")
	cfg.MonitorEnabled = false
	cfg.LogLevel = "error"
	cfg.Arcade.URL = "http://127.0.0.1:1"
	cfg.ChainTracks.URL = "http://127.0.0.1:1"
	return cfg
}

// TestStorageServer_BuildAndHealth proves the server boots (oracle + headers +
// migrated provider + REST handler) and serves an unauthenticated /health
// probe, then shuts its stores down cleanly.
func TestStorageServer_BuildAndHealth(t *testing.T) {
	cfg := smokeConfig(t)
	logger := logging.NewTestLogger(t)

	handler, shutdown, err := buildServer(context.Background(), logger, cfg)
	require.NoError(t, err)
	require.NotNil(t, handler)

	req := httptestNewRequest(t, http.MethodGet, storage.RouteHealth)
	rec := &recorder{header: http.Header{}}
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.status)
	assert.Contains(t, rec.body.String(), "ok")

	require.NoError(t, shutdown(context.Background()))
}

// TestStorageServer_RunGracefulShutdown proves run() serves over a real socket
// and returns cleanly when its context is canceled (SIGINT/SIGTERM path).
func TestStorageServer_RunGracefulShutdown(t *testing.T) {
	cfg := smokeConfig(t)
	cfg.HTTPAddress = freeAddr(t)
	logger := logging.NewTestLogger(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, logger, cfg) }()

	healthURL := "http://" + cfg.HTTPAddress + storage.RouteHealth
	require.Eventually(t, func() bool {
		resp, err := http.Get(healthURL) //nolint:noctx // test poll
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 25*time.Millisecond, "server should serve /health")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "run must return cleanly on graceful shutdown")
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancellation")
	}
}

func TestLoadConfig(t *testing.T) {
	t.Run("defaults are valid", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Arcade.URL = "http://arcade"
		cfg.ChainTracks.URL = "http://chaintracks"
		require.NoError(t, cfg.Validate())
	})

	t.Run("missing chaintracks url rejected", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Arcade.URL = "http://arcade"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "chaintracks")
	})

	t.Run("postgres backend needs dsn", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Backend = "postgres"
		cfg.Arcade.URL = "http://arcade"
		cfg.ChainTracks.URL = "http://chaintracks"
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "postgres_dsn")
	})

	t.Run("env override applies", func(t *testing.T) {
		t.Setenv("STORAGE_HTTP_ADDRESS", ":9999")
		t.Setenv("STORAGE_ARCADE_URL", "http://arcade")
		t.Setenv("STORAGE_CHAINTRACKS_URL", "http://chaintracks")
		cfg, err := LoadConfig("")
		require.NoError(t, err)
		assert.Equal(t, ":9999", cfg.HTTPAddress)
	})
}

// --- small test helpers (avoid importing httptest just for a request) --------

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func httptestNewRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, target, nil)
	require.NoError(t, err)
	return req
}

// recorder is a minimal http.ResponseWriter capturing status + body.
type recorder struct {
	header http.Header
	status int
	body   stringBuilder
}

func (r *recorder) Header() http.Header { return r.header }
func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(b)
}
func (r *recorder) WriteHeader(status int) { r.status = status }

type stringBuilder struct{ b []byte }

func (s *stringBuilder) Write(p []byte) (int, error) { s.b = append(s.b, p...); return len(p), nil }
func (s *stringBuilder) String() string              { return string(s.b) }
