package utxostore

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// OpenFunc constructs a Store from a DSN. A provider registers one under its
// URL scheme via [Register]; [Open] dispatches to it.
type OpenFunc func(ctx context.Context, dsn string) (Store, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]OpenFunc{}
)

// Register associates a DSN scheme (e.g. "aerospike") with an opener. Providers
// call it from a blank-importable subpackage (e.g. aerostore/register) so a
// binary links a backend's client dependency only when it imports that
// subpackage — no build tags. Registering a duplicate or empty scheme panics
// (a programmer error surfaced at init time).
func Register(scheme string, fn OpenFunc) {
	if scheme == "" {
		panic("utxostore: Register scheme must be non-empty")
	}
	if fn == nil {
		panic("utxostore: Register opener must be non-nil")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[scheme]; dup {
		panic(fmt.Sprintf("utxostore: scheme %q already registered", scheme))
	}
	registry[scheme] = fn
}

// Open builds a Store from a DSN, dispatching on its URL scheme to the opener
// registered for it. It returns an error naming the registered schemes when the
// DSN's scheme is unknown — the usual symptom of a missing blank import of the
// backend's register subpackage.
func Open(ctx context.Context, dsn string) (Store, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("utxostore: parse dsn: %w", err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("utxostore: dsn %q has no scheme", dsn)
	}

	registryMu.RLock()
	fn, ok := registry[u.Scheme]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("utxostore: no provider registered for scheme %q (known: %s); did you forget to blank-import the backend's register package?",
			u.Scheme, strings.Join(registeredSchemes(), ", "))
	}
	return fn(ctx, dsn)
}

// registeredSchemes returns the registered schemes, sorted, for diagnostics.
func registeredSchemes() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for s := range registry {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
