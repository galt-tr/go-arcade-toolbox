package defs

import (
	"fmt"
	"net/url"
	"strings"
)

// DBTypeAerospike is the utxostore engine discriminator for the Aerospike
// hot-path inventory backend. Unlike the SQL engines it is NOT a valid
// metastore engine (Database.Validate still rejects anything but postgres or
// sqlite); Aerospike backs the split-store "Mode B" utxostore only.
const DBTypeAerospike DBType = "aerospike"

// DefaultAerospikePort is the Aerospike service port used when a host omits one.
const DefaultAerospikePort = 3000

// Aerospike configures the Aerospike-backed utxostore. It is consumed by
// pkg/utxostore/aerostore (directly or via the aerospike:// DSN that [DSN]
// builds); the store refuses to start unless the namespace's default-ttl is 0.
type Aerospike struct {
	// Hosts is the seed node list ("host" or "host:port"); the first is used to
	// build a DSN. At least one is required.
	Hosts []string `mapstructure:"hosts"`

	// Namespace is the Aerospike namespace holding the UTXO set (required).
	Namespace string `mapstructure:"namespace"`

	// Set is the Aerospike set name; defaults to "utxos" when empty.
	Set string `mapstructure:"set"`

	// User and Password are the optional Enterprise-security credentials.
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

// DefaultAerospikeConfig returns a config pointed at a local single node.
func DefaultAerospikeConfig() Aerospike {
	return Aerospike{
		Hosts:     []string{fmt.Sprintf("localhost:%d", DefaultAerospikePort)},
		Namespace: "test",
		Set:       "utxos",
	}
}

// Validate reports whether the Aerospike configuration is usable.
func (a *Aerospike) Validate() error {
	if len(a.Hosts) == 0 || strings.TrimSpace(a.Hosts[0]) == "" {
		return fmt.Errorf("aerospike: at least one host is required")
	}
	if strings.TrimSpace(a.Namespace) == "" {
		return fmt.Errorf("aerospike: namespace is required")
	}
	return nil
}

// DSN renders an "aerospike://[user:pass@]host[:port]/namespace?set=…" DSN from
// the first host, suitable for utxostore.Open. It does not validate; call
// [Aerospike.Validate] first.
func (a *Aerospike) DSN() string {
	host := ""
	if len(a.Hosts) > 0 {
		host = a.Hosts[0]
	}
	set := a.Set
	if set == "" {
		set = "utxos"
	}
	u := url.URL{
		Scheme:   "aerospike",
		Host:     host,
		Path:     "/" + a.Namespace,
		RawQuery: url.Values{"set": {set}}.Encode(),
	}
	if a.User != "" {
		u.User = url.UserPassword(a.User, a.Password)
	}
	return u.String()
}
