.PHONY: test test-integration test-conformance test-perf bench perf-report lint check-podman clean-containers

# Run unit tests only (no build tags).
# Guarded: `go test ./...` errors out on a module with zero packages, which
# is the case before any code has landed yet.
test:
	@if [ -z "$$(go list ./... 2>/dev/null)" ]; then \
		echo "no packages yet, skipping go test"; \
	else \
		go test ./...; \
	fi

# Run integration tests (requires the "integration" build tag).
test-integration:
	@if [ -z "$$(go list -tags integration ./... 2>/dev/null)" ]; then \
		echo "no packages yet, skipping go test -tags integration"; \
	else \
		go test -tags integration ./...; \
	fi

# Run conformance tests, a subset of the integration suite.
test-conformance:
	@if [ -z "$$(go list -tags integration ./... 2>/dev/null)" ]; then \
		echo "no packages yet, skipping conformance tests"; \
	else \
		go test -tags integration -run 'TestConformance' ./...; \
	fi

# Run performance tests. Skips gracefully if test/perf doesn't exist yet.
test-perf:
	@if [ ! -d test/perf ]; then \
		echo "test/perf directory not found, skipping perf tests"; \
	else \
		go test -tags perf -run 'TestPerf' -timeout 30m ./test/perf/...; \
	fi

# Run benchmarks across the module.
bench:
	@if [ -z "$$(go list ./... 2>/dev/null)" ]; then \
		echo "no packages yet, skipping benchmarks"; \
	else \
		go test -bench . -benchmem -run '^$$' ./...; \
	fi

# Run a benchmark via cmd/perfrunner and render its Markdown report into
# docs/benchmarks/. SQLite is self-contained; for postgres/aerospike-hybrid pass
# connection flags via PERF_ARGS. Override any knob on the command line, e.g.:
#   make perf-report PERF_BACKEND=postgres PERF_DURATION=5m \
#     PERF_ARGS='-pg-dsn postgres://user:pass@localhost:5432/db -max-db-conns 72'
PERF_BACKEND ?= sqlite
PERF_WORKERS ?= 32
PERF_DURATION ?= 60s
PERF_WARMUP ?= 15s
perf-report:
	go run ./cmd/perfrunner -backend $(PERF_BACKEND) -workers $(PERF_WORKERS) \
		-duration $(PERF_DURATION) -warmup $(PERF_WARMUP) $(PERF_ARGS)

# Run golangci-lint if available, otherwise print install instructions.
# Guarded: golangci-lint errors on a module with zero go files, which is the
# case before any code has landed yet.
lint:
	@if [ -z "$$(go list ./... 2>/dev/null)" ]; then \
		echo "no packages yet, skipping lint"; \
	elif command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Install it from https://golangci-lint.run/welcome/install/ (e.g. 'brew install golangci-lint' or 'go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest')"; \
		exit 1; \
	fi

# Verify the rootless podman socket is active and print DOCKER_HOST guidance
# for testcontainers-based integration tests.
check-podman:
	@if systemctl --user is-active podman.socket >/dev/null 2>&1; then \
		echo "podman.socket is active"; \
	else \
		echo "podman.socket is not active. Start it with: systemctl --user enable --now podman.socket"; \
	fi
	@echo "Set DOCKER_HOST=unix:///run/user/$$(id -u)/podman/podman.sock for testcontainers to use rootless podman"

# Remove any leftover containers created by testcontainers.
# Prefers podman, falls back to docker. Fails if the engine's ps command
# itself errors (e.g. the daemon/socket is unreachable).
clean-containers:
	@if command -v podman >/dev/null 2>&1; then \
		engine=podman; \
	elif command -v docker >/dev/null 2>&1; then \
		engine=docker; \
	else \
		echo "neither podman nor docker found"; \
		exit 1; \
	fi; \
	ids="$$($$engine ps -aq --filter label=org.testcontainers=true)" || exit 1; \
	if [ -n "$$ids" ]; then \
		$$engine rm -f $$ids; \
	else \
		echo "no testcontainers containers to remove"; \
	fi
