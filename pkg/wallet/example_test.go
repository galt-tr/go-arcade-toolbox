package wallet_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv/mockarcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/services"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/perfprovider"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wallet"
)

// Example builds a SQLite-backed BRC-100 wallet and reads back its identity
// (receive) key. It wires the real arcade and headers clients against in-process
// mocks; in production those URLs point at a real Arcade deployment. The
// identity key is deterministic because the wallet is built from the well-known
// private key "1".
func Example() {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The two external services, as in-process mocks.
	arc, closeArc := mockarcade.NewArcadeServer()
	defer closeArc()
	ct, closeCT := mockarcade.NewChainTracksServer()
	defer closeCT()

	// The production clients, pointed at the mocks.
	oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arc.URL(), EventsURL: arc.URL()})
	hdrs, err := headers.New(logger, defs.ChainTracks{Enabled: true, URL: ct.URL()})
	if err != nil {
		log.Fatal(err)
	}

	// SQLite storage (Mode A) in a temp dir.
	dir, err := os.MkdirTemp("", "wallet-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
		Backend:     perfprovider.BackendSQLite,
		SQLitePath:  filepath.Join(dir, "wallet.db"),
		Network:     defs.NetworkTestnet,
		StorageName: "example",
	}, oracle, hdrs)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = closeProv(context.Background()) }()

	if _, err := provider.Migrate(ctx, "example", "example-identity-key"); err != nil {
		log.Fatal(err)
	}

	svc := services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(defs.NetworkTestnet))

	w, err := wallet.New(defs.NetworkTestnet, "0000000000000000000000000000000000000000000000000000000000000001", provider,
		wallet.WithServices(svc),
		wallet.WithLogger(logger),
	)
	if err != nil {
		log.Fatal(err)
	}

	pub, err := w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, "example.com")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(pub.PublicKey.ToDERHex())
	// Output:
	// 0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798
}
