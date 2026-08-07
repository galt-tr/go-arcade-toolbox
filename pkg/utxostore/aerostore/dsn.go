package aerostore

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DefaultPort is the Aerospike service port assumed when a DSN host omits one.
const DefaultPort = 3000

// DefaultSet is the set name used when a DSN does not specify ?set=.
const DefaultSet = "utxos"

// dsnConfig is the parsed form of an aerospike:// DSN.
type dsnConfig struct {
	host      string
	port      int
	namespace string
	set       string
	user      string
	password  string
}

// parseDSN parses an "aerospike://[user:pass@]host[:port]/namespace[?set=utxos]"
// DSN. The scheme must be "aerospike", the host is required, the port defaults
// to [DefaultPort], the namespace is the (single-segment) path, and the set
// defaults to [DefaultSet].
func parseDSN(dsn string) (dsnConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsnConfig{}, fmt.Errorf("aerostore: parse dsn: %w", err)
	}
	if u.Scheme != "aerospike" {
		return dsnConfig{}, fmt.Errorf("aerostore: dsn scheme must be %q, got %q", "aerospike", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return dsnConfig{}, fmt.Errorf("aerostore: dsn %q has no host", dsn)
	}
	port := DefaultPort
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return dsnConfig{}, fmt.Errorf("aerostore: dsn port %q: %w", p, err)
		}
	}

	namespace := strings.Trim(u.Path, "/")
	if namespace == "" {
		return dsnConfig{}, fmt.Errorf("aerostore: dsn %q has no namespace (path)", dsn)
	}
	if strings.Contains(namespace, "/") {
		return dsnConfig{}, fmt.Errorf("aerostore: dsn namespace %q must be a single path segment", namespace)
	}

	set := u.Query().Get("set")
	if set == "" {
		set = DefaultSet
	}

	cfg := dsnConfig{host: host, port: port, namespace: namespace, set: set}
	if u.User != nil {
		cfg.user = u.User.Username()
		cfg.password, _ = u.User.Password()
	}
	return cfg, nil
}
