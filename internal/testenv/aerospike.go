package testenv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// aerospikeImage pins the exact Aerospike Community Edition image
	// StartAerospike runs. See the package-level comment on
	// AerospikeContainer for why this image (env-var configuration) was
	// chosen over the bitcoin-sv/testcontainers-aerospike-go module.
	aerospikeImage = "aerospike/aerospike-server:8.1.2.4"

	aerospikePort = "3000/tcp"
)

// AerospikeContainer is a running Aerospike Community Edition testcontainer
// with a single namespace configured with default-ttl=0 (never expire).
//
// # Why default-ttl=0 is guaranteed, and how
//
// go-arcade-toolbox's aerostore provider refuses to start against a
// namespace whose default-ttl is nonzero (a wallet's UTXO rows must never
// silently expire — see pkg/utxostore's "No TTL deletion — ever" doc). The
// aerospike/aerospike-server image's entrypoint renders
// /etc/aerospike/aerospike.conf from a shell template
// (aerospike-server.docker's releases/*/community/*/aerospike.template.conf)
// driven entirely by environment variables — there is no separate config
// file to mount for this. The template's namespace stanza is:
//
//	namespace ${NAMESPACE} {
//		...
//		$( [[ "${DEFAULT_TTL}" != "0" ]] && echo "default-ttl ${DEFAULT_TTL}")
//	}
//
// i.e. the default-ttl line is emitted only when DEFAULT_TTL is set to
// something other than "0" — and the entrypoint itself defaults DEFAULT_TTL
// to "0" when the variable is unset. So the image's out-of-the-box default
// namespace already has default-ttl=0. StartAerospike still sets
// DEFAULT_TTL=0 explicitly (verified live against the image below) rather
// than relying on that default silently: an image update that changed its
// own default would otherwise violate the aerostore contract without this
// package's tests noticing.
//
// Verified directly against aerospike/aerospike-server:8.1.2.4 on this
// machine (rootless podman):
//
//	$ podman run -d -p 13000:3000 -e NAMESPACE=test -e DEFAULT_TTL=0 \
//	    aerospike/aerospike-server:8.1.2.4
//	$ podman exec <id> asinfo -v "get-config:context=namespace;id=test" \
//	    | tr ';' '\n' | grep default-ttl
//	default-ttl=0
//
// # Why core testcontainers-go API instead of testcontainers-aerospike-go
//
// github.com/bitcoin-sv/testcontainers-aerospike-go (and its
// bsv-blockchain/testcontainers-aerospike-go successor, used by teranode)
// wraps exactly this image with the same NAMESPACE env var and a custom wait
// strategy — but as of this writing neither exposes a DEFAULT_TTL
// customizer, so using it would still require passing raw env through a
// generic option, buying nothing over calling testcontainers-go directly.
// Pulling in the module would add a second package (with its own
// testcontainers-go version to keep in sync) purely to save the ~15 lines
// of ContainerRequest setup below. This file's wait strategy is modeled
// directly on that module's wait.go (port open, then poll with a real
// aerospike-client-go connection until the cluster reports an active node)
// since that approach is the actual precedent worth keeping — it is
// meaningfully more reliable than waiting on a startup log line, which is
// undocumented and has changed wording across Aerospike releases.
type AerospikeContainer struct {
	container testcontainers.Container

	host      string
	port      int
	namespace string
}

// StartAerospike starts an Aerospike Community Edition container (see
// aerospikeImage for the pinned tag; override with TESTENV_AEROSPIKE_IMAGE)
// with its default namespace (override with TESTENV_AEROSPIKE_NAMESPACE,
// default "test") configured with default-ttl=0, waits until the cluster is
// answering client requests, and registers its teardown in t.Cleanup — via
// startContainer, which registers Terminate even when testcontainers.Run
// fails after the container has already started (Run's non-atomic contract;
// see container.go). It skips the test with an actionable message if no
// container runtime is available.
//
// Cleanup is MANDATORY here for the same reason as StartPostgres: ryuk is
// disabled under rootless podman (see package doc), so t.Cleanup is the only
// thing standing between this container and a permanent leak.
func StartAerospike(t *testing.T) *AerospikeContainer {
	t.Helper()
	RequireRuntime(t)

	image := getenvDefault("TESTENV_AEROSPIKE_IMAGE", aerospikeImage)
	namespace := getenvDefault("TESTENV_AEROSPIKE_NAMESPACE", "test")

	ctr := startContainer(t, "aerospike", image,
		testcontainers.WithExposedPorts(aerospikePort),
		testcontainers.WithEnv(map[string]string{
			"NAMESPACE":   namespace,
			"DEFAULT_TTL": "0", // never expire; see the AerospikeContainer doc comment.
		}),
		// NOT testcontainers.WithWaitStrategy: that wrapper hardwires a
		// 60-second deadline that silently caps any longer inner timeout.
		// readinessBudget (container.go) is the single authoritative budget;
		// the strategy carries no timeout of its own and runs against the
		// context deadline this option installs.
		testcontainers.WithWaitStrategyAndDeadline(readinessBudget, &aerospikeWaitStrategy{
			pollInterval: 200 * time.Millisecond,
		}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("testenv: aerospike container host: %v", err)
	}
	mapped, err := ctr.MappedPort(ctx, nat.Port(aerospikePort))
	if err != nil {
		t.Fatalf("testenv: aerospike container mapped port: %v", err)
	}

	return &AerospikeContainer{
		container: ctr,
		host:      host,
		port:      mapped.Int(),
		namespace: namespace,
	}
}

// Host returns the host the Aerospike container's service port is published
// on (loopback for a local podman/docker engine).
func (a *AerospikeContainer) Host() string { return a.host }

// Port returns the host-mapped port for the Aerospike service (3000/tcp in
// the container).
func (a *AerospikeContainer) Port() int { return a.port }

// Namespace returns the name of the namespace StartAerospike configured
// with default-ttl=0.
func (a *AerospikeContainer) Namespace() string { return a.namespace }

// aerospikeWaitStrategy waits for the Aerospike service port to accept TCP
// connections, then polls with a real aerospike-client-go connection until
// the server reports at least one active cluster node. See the
// AerospikeContainer doc comment for why this (rather than a log-line match)
// is the strategy used.
//
// The strategy deliberately carries NO timeout of its own: the single
// authoritative readiness budget is the deadline WithWaitStrategyAndDeadline
// installs on the context WaitUntilReady receives (see the readinessBudget
// doc in container.go), and this strategy simply runs until that context
// expires.
//
// Used only via a *aerospikeWaitStrategy (never a bare value): testcontainers-go's
// wait option wrappers always wrap their argument(s) in a *wait.MultiStrategy,
// whose String() method calls reflect.ValueOf(strategy).IsNil() on every
// entry without checking Kind() first — that panics ("call of
// reflect.Value.IsNil on struct Value") for a struct value. A pointer's
// reflect.Kind is Ptr, which IsNil supports, so this type is always
// constructed and passed as a pointer.
type aerospikeWaitStrategy struct {
	pollInterval time.Duration
}

var _ wait.Strategy = (*aerospikeWaitStrategy)(nil)

// String implements fmt.Stringer so MultiStrategy's own String() (used in
// testcontainers-go's "waiting for container" log line) prints something
// readable instead of falling back to the Go type name.
func (s *aerospikeWaitStrategy) String() string {
	return "Aerospike cluster ready (port open + client probe)"
}

func (s *aerospikeWaitStrategy) WaitUntilReady(ctx context.Context, target wait.StrategyTarget) error {
	port := nat.Port(aerospikePort)

	// Align the ForListeningPort sub-strategy with whatever remains of the
	// context's deadline: left alone it would apply its own private
	// 60-second default startup timeout, silently undercutting the single
	// authoritative budget carried by ctx.
	portWait := wait.ForListeningPort(port)
	if deadline, ok := ctx.Deadline(); ok {
		portWait = portWait.WithStartupTimeout(time.Until(deadline))
	}
	if err := portWait.WaitUntilReady(ctx, target); err != nil {
		return fmt.Errorf("testenv: waiting for aerospike port to open: %w", err)
	}

	host, err := target.Host(ctx)
	if err != nil {
		return fmt.Errorf("testenv: aerospike host: %w", err)
	}
	mapped, err := target.MappedPort(ctx, port)
	if err != nil {
		return fmt.Errorf("testenv: aerospike mapped port: %w", err)
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		ready, err := aerospikeClusterReady(host, mapped.Int())
		if err != nil {
			return err
		}
		if ready {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("testenv: timed out waiting for aerospike cluster to become ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// aerospikeClusterReady connects to host:port with a short timeout and
// reports whether the cluster is up and answering: connected, at least one
// node discovered, and every discovered node active. A connection refusal or
// an "invalid node" error (the cluster map isn't ready yet) is treated as
// "not ready yet" rather than a hard failure; any other error aborts the
// wait.
func aerospikeClusterReady(host string, port int) (bool, error) {
	policy := aerospike.NewClientPolicy()
	policy.Timeout = 2 * time.Second

	client, err := aerospike.NewClientWithPolicy(policy, host, port)
	if err != nil {
		if err.Matches(types.INVALID_NODE_ERROR) {
			return false, nil
		}
		return false, fmt.Errorf("testenv: connect to aerospike: %w", err)
	}
	defer client.Close()

	if !client.IsConnected() {
		return false, nil
	}

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return false, nil
	}
	for _, node := range nodes {
		if !node.IsActive() {
			return false, nil
		}
	}
	return true, nil
}
