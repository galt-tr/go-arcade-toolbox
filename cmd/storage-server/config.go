package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
)

// Config is the storage-server deployment configuration, loaded from a YAML
// file (see config.example.yaml) with a few environment-variable overrides.
type Config struct {
	// Network is the BSV network: main | test | stn | teratestnet.
	Network string `yaml:"network"`
	// HTTPAddress is the listen address for the REST server, e.g. ":8100".
	HTTPAddress string `yaml:"http_address"`

	// StorageName and StorageIdentityKey identify this storage instance; they
	// are written into the settings row by Migrate.
	StorageName        string `yaml:"storage_name"`
	StorageIdentityKey string `yaml:"storage_identity_key"`

	// Backend selects the store wiring: sqlite | postgres | aerospike-hybrid.
	Backend string `yaml:"backend"`
	// SQLitePath is the on-disk file for the sqlite backend.
	SQLitePath string `yaml:"sqlite_path"`
	// PostgresDSN is the metastore DSN for the postgres and aerospike-hybrid
	// backends.
	PostgresDSN string          `yaml:"postgres_dsn"`
	Aerospike   AerospikeConfig `yaml:"aerospike"`
	// MaxDBConns caps the shared SQL pool (0 = driver default).
	MaxDBConns int `yaml:"max_db_conns"`

	Arcade      ArcadeConfig      `yaml:"arcade"`
	ChainTracks ChainTracksConfig `yaml:"chaintracks"`

	// MonitorEnabled runs the background monitor daemon (SSE apply pipeline +
	// tip/reorg consumers + the reject→release reconciler and scheduled tasks)
	// alongside the REST server.
	MonitorEnabled bool `yaml:"monitor_enabled"`

	// LogLevel is debug | info | warn | error. LogHandler is text | json.
	LogLevel   string `yaml:"log_level"`
	LogHandler string `yaml:"log_handler"`

	// MaxRequestBodyBytes caps an inbound request body (0 = server default, 1 MiB).
	MaxRequestBodyBytes int64 `yaml:"max_request_body_bytes"`
	// ShutdownTimeoutSeconds bounds graceful shutdown.
	ShutdownTimeoutSeconds int `yaml:"shutdown_timeout_seconds"`
}

// AerospikeConfig describes the Aerospike inventory for the hybrid backend.
type AerospikeConfig struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Namespace string `yaml:"namespace"`
	Set       string `yaml:"set"`
}

// ArcadeConfig points the storage provider at its Arcade tx oracle.
type ArcadeConfig struct {
	Enabled   bool   `yaml:"enabled"`
	URL       string `yaml:"url"`
	EventsURL string `yaml:"events_url"`
}

// ChainTracksConfig points the storage provider at its ChainTracks headers source.
type ChainTracksConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
}

// DefaultConfig returns a self-contained, single-node SQLite configuration.
func DefaultConfig() Config {
	return Config{
		Network:                "test",
		HTTPAddress:            ":8100",
		StorageName:            "arcade-storage",
		Backend:                string(perfprovider.BackendSQLite),
		SQLitePath:             "./storage.db",
		Aerospike:              AerospikeConfig{Port: 3000, Namespace: "test"},
		Arcade:                 ArcadeConfig{Enabled: true},
		ChainTracks:            ChainTracksConfig{Enabled: true},
		MonitorEnabled:         true,
		LogLevel:               "info",
		LogHandler:             "text",
		ShutdownTimeoutSeconds: 15,
	}
}

// LoadConfig reads the YAML config at path onto the defaults (so unset fields
// keep their defaults), then applies environment overrides. An empty path
// yields DefaultConfig with overrides applied.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if strings.TrimSpace(path) != "" {
		data, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}
	cfg.applyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnvOverrides lets a few operationally-important fields be set from the
// environment (handy for containers) without a config file rewrite.
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("STORAGE_HTTP_ADDRESS"); v != "" {
		c.HTTPAddress = v
	}
	if v := os.Getenv("STORAGE_SQLITE_PATH"); v != "" {
		c.SQLitePath = v
	}
	if v := os.Getenv("STORAGE_POSTGRES_DSN"); v != "" {
		c.PostgresDSN = v
	}
	if v := os.Getenv("STORAGE_IDENTITY_KEY"); v != "" {
		c.StorageIdentityKey = v
	}
	if v := os.Getenv("STORAGE_ARCADE_URL"); v != "" {
		c.Arcade.URL = v
	}
	if v := os.Getenv("STORAGE_CHAINTRACKS_URL"); v != "" {
		c.ChainTracks.URL = v
	}
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	if _, err := defs.ParseBSVNetworkStr(c.Network); err != nil {
		return fmt.Errorf("invalid network %q: %w", c.Network, err)
	}
	if strings.TrimSpace(c.HTTPAddress) == "" {
		return fmt.Errorf("http_address is required")
	}
	backend, err := perfprovider.ParseBackend(c.Backend)
	if err != nil {
		return err
	}
	switch backend {
	case perfprovider.BackendSQLite:
		if c.SQLitePath == "" {
			return fmt.Errorf("sqlite_path is required for the sqlite backend")
		}
	case perfprovider.BackendPostgres:
		if c.PostgresDSN == "" {
			return fmt.Errorf("postgres_dsn is required for the postgres backend")
		}
	case perfprovider.BackendAerospikeHybrid:
		if c.PostgresDSN == "" {
			return fmt.Errorf("postgres_dsn is required for the aerospike-hybrid backend")
		}
		if c.Aerospike.Host == "" {
			return fmt.Errorf("aerospike.host is required for the aerospike-hybrid backend")
		}
	}
	if c.ChainTracks.URL == "" {
		return fmt.Errorf("chaintracks.url is required (the storage provider needs a headers source)")
	}
	if c.Arcade.URL == "" {
		return fmt.Errorf("arcade.url is required (the storage provider needs a tx oracle)")
	}
	return nil
}

// network parses the validated network string.
func (c *Config) network() defs.BSVNetwork {
	n, _ := defs.ParseBSVNetworkStr(c.Network)
	return n
}

// perfproviderConfig maps the server config to a perfprovider.Config.
func (c *Config) perfproviderConfig() perfprovider.Config {
	backend, _ := perfprovider.ParseBackend(c.Backend)
	return perfprovider.Config{
		Backend:       backend,
		SQLitePath:    c.SQLitePath,
		PostgresDSN:   c.PostgresDSN,
		AeroHost:      c.Aerospike.Host,
		AeroPort:      c.Aerospike.Port,
		AeroNamespace: c.Aerospike.Namespace,
		AeroSet:       c.Aerospike.Set,
		MaxDBConns:    c.MaxDBConns,
		Network:       c.network(),
		StorageName:   c.StorageName,
	}
}

// arcadeConfig maps to defs.Arcade.
func (c *Config) arcadeConfig() defs.Arcade {
	events := c.Arcade.EventsURL
	if events == "" {
		events = c.Arcade.URL
	}
	return defs.Arcade{Enabled: c.Arcade.Enabled, URL: c.Arcade.URL, EventsURL: events}
}

// chainTracksConfig maps to defs.ChainTracks.
func (c *Config) chainTracksConfig() defs.ChainTracks {
	return defs.ChainTracks{Enabled: c.ChainTracks.Enabled, URL: c.ChainTracks.URL}
}
