// Package perfprovider builds ready-to-use storage.Provider instances over the
// three benchmark backends the performance harness targets, WITHOUT depending
// on the testing package. It lives under pkg/storage so it can reach the
// otherwise-internal metastore and funder subsystems, and exposes a single
// public constructor that internal/perf, test/perf and cmd/perfrunner all share.
//
//   - BackendSQLite:          metastore + sqlstore over one SQLite file (Mode A).
//   - BackendPostgres:        metastore + sqlstore over one Postgres DB (Mode A).
//   - BackendAerospikeHybrid: metastore (Postgres) + aerostore (Aerospike),
//     split stores the provider detects as Mode B.
//
// The oracle and headers source are supplied by the caller (the harness wires
// them to the in-process mockarcade doubles), so the provider exercises the
// real arcade and headers clients over HTTP/SSE exactly as production would.
package perfprovider

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/aerostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
)

// Backend identifies which storage backend to build.
type Backend string

const (
	// BackendSQLite is a single-file SQLite deployment (Mode A). Baseline only.
	BackendSQLite Backend = "sqlite"
	// BackendPostgres is a single-database PostgreSQL deployment (Mode A).
	BackendPostgres Backend = "postgres"
	// BackendAerospikeHybrid is an Aerospike inventory + PostgreSQL metadata
	// split deployment (Mode B).
	BackendAerospikeHybrid Backend = "aerospike-hybrid"
)

// ParseBackend converts a string into a Backend, rejecting unknown values.
func ParseBackend(s string) (Backend, error) {
	switch Backend(s) {
	case BackendSQLite, BackendPostgres, BackendAerospikeHybrid:
		return Backend(s), nil
	default:
		return "", fmt.Errorf("perfprovider: unknown backend %q (want sqlite|postgres|aerospike-hybrid)", s)
	}
}

// Config parameterises the backend construction. Only the fields relevant to
// the chosen Backend are read.
type Config struct {
	Backend Backend

	// SQLitePath is the on-disk SQLite file (BackendSQLite).
	SQLitePath string
	// PostgresDSN is the metastore DSN (BackendPostgres, BackendAerospikeHybrid).
	PostgresDSN string
	// AeroHost/AeroPort/AeroNamespace/AeroSet describe the Aerospike inventory
	// (BackendAerospikeHybrid).
	AeroHost      string
	AeroPort      int
	AeroNamespace string
	AeroSet       string

	// MaxDBConns caps the shared SQL connection pool (Postgres/SQLite). It is a
	// primary throughput knob for the SQL backends: too low and workers queue
	// on connections, too high and Postgres context-switches. 0 leaves the
	// driver default.
	MaxDBConns int

	// Network and StorageName flow through to the provider settings.
	Network     defs.BSVNetwork
	StorageName string

	// FeeModel funds transactions; DefaultFeeModel when zero-valued.
	FeeModel defs.FeeModel

	// ExtraOptions are appended to the provider options (after Network and
	// StorageName), letting callers override the fee model, change basket, etc.
	ExtraOptions []storage.Option
}

// CloseFunc releases the backend's stores. It is safe to call once.
type CloseFunc func(context.Context) error

// New builds (but does not migrate) a storage.Provider over cfg.Backend, wired
// to the given oracle and headers source. The returned CloseFunc closes the
// underlying metastore and utxostore. Callers migrate via Provider.Migrate.
func New(
	ctx context.Context,
	logger *slog.Logger,
	cfg Config,
	oracle arcade.TxOracle,
	hdrs headers.Headers,
) (*storage.Provider, CloseFunc, error) {
	feeModel := cfg.FeeModel
	if feeModel == (defs.FeeModel{}) {
		feeModel = defs.DefaultFeeModel()
	}
	network := cfg.Network
	if network == "" {
		network = defs.NetworkTestnet
	}
	name := cfg.StorageName
	if name == "" {
		name = "perf"
	}

	switch cfg.Backend {
	case BackendSQLite:
		return newSQL(ctx, logger, cfg, oracle, hdrs, feeModel, network, name, cfg.SQLitePath, true)
	case BackendPostgres:
		return newSQL(ctx, logger, cfg, oracle, hdrs, feeModel, network, name, cfg.PostgresDSN, false)
	case BackendAerospikeHybrid:
		return newHybrid(ctx, logger, cfg, oracle, hdrs, feeModel, network, name)
	default:
		return nil, nil, fmt.Errorf("perfprovider: unsupported backend %q", cfg.Backend)
	}
}

// newSQL builds the shared-database Mode A provider (SQLite or Postgres).
func newSQL(
	ctx context.Context,
	logger *slog.Logger,
	cfg Config,
	oracle arcade.TxOracle,
	hdrs headers.Headers,
	feeModel defs.FeeModel,
	network defs.BSVNetwork,
	name, target string,
	sqlite bool,
) (*storage.Provider, CloseFunc, error) {
	var (
		meta *metastore.Store
		err  error
	)
	if sqlite {
		if target == "" {
			return nil, nil, fmt.Errorf("perfprovider: sqlite path is required")
		}
		meta, err = metastore.OpenSQLite(ctx, target)
	} else {
		if target == "" {
			return nil, nil, fmt.Errorf("perfprovider: postgres dsn is required")
		}
		meta, err = metastore.OpenPostgres(ctx, target)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("perfprovider: open metastore: %w", err)
	}

	// Share one SQL connection pool between the metastore and the sqlstore
	// utxostore (Mode A). The pool size gates write concurrency directly.
	db := meta.DB()
	if cfg.MaxDBConns > 0 {
		db.SetMaxOpenConns(cfg.MaxDBConns)
		db.SetMaxIdleConns(cfg.MaxDBConns)
	}

	utxo, err := sqlstore.New(ctx, db, sqlstore.Engine(string(meta.Engine())))
	if err != nil {
		_ = meta.Close(ctx)
		return nil, nil, fmt.Errorf("perfprovider: open sqlstore: %w", err)
	}

	fnd := funder.New(logger, utxo, feeModel)
	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		append([]storage.Option{
			storage.WithNetwork(network),
			storage.WithStorageName(name),
			storage.WithFeeModel(feeModel),
		}, cfg.ExtraOptions...)...,
	)
	if err != nil {
		_ = utxo.Close(ctx)
		_ = meta.Close(ctx)
		return nil, nil, fmt.Errorf("perfprovider: build provider: %w", err)
	}

	closer := func(ctx context.Context) error {
		_ = utxo.Close(ctx)
		return meta.Close(ctx)
	}
	return p, closer, nil
}

// newHybrid builds the Aerospike-inventory + Postgres-metadata split (Mode B).
func newHybrid(
	ctx context.Context,
	logger *slog.Logger,
	cfg Config,
	oracle arcade.TxOracle,
	hdrs headers.Headers,
	feeModel defs.FeeModel,
	network defs.BSVNetwork,
	name string,
) (*storage.Provider, CloseFunc, error) {
	if cfg.PostgresDSN == "" {
		return nil, nil, fmt.Errorf("perfprovider: postgres dsn is required for the aerospike hybrid")
	}
	if cfg.AeroHost == "" {
		return nil, nil, fmt.Errorf("perfprovider: aerospike host is required for the aerospike hybrid")
	}

	meta, err := metastore.OpenPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("perfprovider: open metastore: %w", err)
	}
	if cfg.MaxDBConns > 0 {
		meta.DB().SetMaxOpenConns(cfg.MaxDBConns)
		meta.DB().SetMaxIdleConns(cfg.MaxDBConns)
	}

	opts := []aerostore.Option{aerostore.WithLogger(logger)}
	if cfg.AeroSet != "" {
		opts = append(opts, aerostore.WithSet(cfg.AeroSet))
	}
	utxo, err := aerostore.New(ctx, cfg.AeroHost, cfg.AeroPort, cfg.AeroNamespace, opts...)
	if err != nil {
		_ = meta.Close(ctx)
		return nil, nil, fmt.Errorf("perfprovider: open aerostore: %w", err)
	}

	fnd := funder.New(logger, utxo, feeModel)
	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		append([]storage.Option{
			storage.WithNetwork(network),
			storage.WithStorageName(name),
			storage.WithFeeModel(feeModel),
		}, cfg.ExtraOptions...)...,
	)
	if err != nil {
		_ = utxo.Close(ctx)
		_ = meta.Close(ctx)
		return nil, nil, fmt.Errorf("perfprovider: build provider: %w", err)
	}

	closer := func(ctx context.Context) error {
		_ = utxo.Close(ctx)
		return meta.Close(ctx)
	}
	return p, closer, nil
}
