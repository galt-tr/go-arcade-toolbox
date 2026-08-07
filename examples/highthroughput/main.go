// Command highthroughput demonstrates the toolbox's throughput building blocks:
// the denominated fuel-pool configuration, the FanOutFuel minting primitive,
// and the fuel-keeper top-up loop. It is illustrative — it runs against the
// in-process mocks to show the APIs and print real derived sizing numbers, but
// a real 1000-TPS deployment needs a tuned PostgreSQL/Aerospike backend and a
// pre-provisioned pool.
//
// Run it:
//
//	go run ./examples/highthroughput
//
// Read docs/high-throughput-guide.md for the full guide, including the MEASURED
// throughput/durability tradeoff (~575-646 TPS/node durable; 1379 TPS with
// relaxed durability; scale-out or group-commit to exceed 1000 durably).
//
// FOLLOW-UP CAVEAT: a dedicated, wallet-signable fuel basket is not wired yet
// (storage does not honor Options.FuelShape), so FanOutFuel currently mints its
// outputs as ordinary change into the default basket rather than into a separate
// pool basket. The denominated-pool funding path (ClaimExact) itself is wired
// and benchmarked; provisioning that pool through the public wallet API is the
// remaining piece. See the gap analysis in docs/benchmarks/README.md.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bsv-blockchain/go-arcade-toolbox/examples/internal/demoenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wallet/fuelkeeper"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

func main() {
	ctx := context.Background()

	// 1. Size the denominated fuel pool from your workload. Start from the
	//    defaults and describe your typical transaction and target rate; the
	//    config derives the coin denomination and the pool target.
	um := defs.DefaultUTXOManagement()
	um.Strategy = defs.StrategyThroughput
	um.Throughput.ExpectedTxSizeBytes = 400  // signed size incl. the fuel input
	um.Throughput.ExpectedOutputSatoshis = 0 // pure fee/data action
	um.Throughput.TargetTPS = 1000           // sustained fuel-claim rate
	um.Throughput.ExpectedConfirmationSeconds = 300
	um.Throughput.SpendPolicy = defs.SpendPolicyPreferMined

	if err := um.Validate(defs.DefaultFeeModel(), defs.Commission{}); err != nil {
		log.Fatalf("throughput config invalid: %v", err)
	}

	denomination, err := um.Throughput.Denomination(defs.DefaultFeeModel(), defs.Commission{})
	if err != nil {
		log.Fatalf("derive denomination: %v", err)
	}
	fmt.Printf("throughput config:\n")
	fmt.Printf("  strategy:        %s\n", um.Strategy)
	fmt.Printf("  fuel denomination: %d sat/coin\n", denomination)
	fmt.Printf("  target pool size:  %d coins (target_tps x confirmation x headroom)\n", um.Throughput.TargetPool())
	fmt.Printf("  spend policy:      %s\n", um.Throughput.SpendPolicy)
	fmt.Printf("  pool basket:       %q  reserve basket: %q\n", um.Throughput.PoolBasket, um.Throughput.ReserveBasket)

	// To run the provider in throughput mode you wire this config into storage:
	//
	//   provider, _, _ := perfprovider.New(ctx, logger, perfprovider.Config{
	//       Backend:      perfprovider.BackendPostgres,
	//       PostgresDSN:  dsn,
	//       MaxDBConns:   72,
	//       ExtraOptions: []storage.Option{storage.WithUTXOManagement(um)},
	//   }, oracle, hdrs)
	//
	// CreateAction then funds via the closed-form ClaimExact fast path over the
	// denominated pool instead of the tiered best-fit claim.

	// 2. Stand up a wallet (privacy mode, so this demo can actually fund and run)
	//    and give it some coins to fan out.
	env, err := demoenv.Setup(ctx)
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
	defer env.Close()

	if _, err := env.SeedFunds(ctx, 1_000_000, 820_000); err != nil {
		log.Fatalf("seed funds: %v", err)
	}

	// 3. FanOutFuel mints a batch of equal-value fuel coins in one transaction.
	//    This is the primitive the fuel keeper calls to refill the pool.
	shape := wdk.ShapedChange{
		Count:    8,
		Satoshis: primitives.SatoshiValue(denomination),
		Basket:   primitives.StringUnder300(um.Throughput.PoolBasket),
	}
	res, err := env.Wallet.FanOutFuel(ctx, shape, demoenv.Originator)
	if err != nil {
		log.Fatalf("fan-out fuel: %v", err)
	}
	fmt.Printf("\nfanned out %d fuel coins of %d sat in tx %s\n", shape.Count, denomination, res.Txid)

	// 4. The fuel keeper automates the fan-out: it watches the pool and mints
	//    more coins whenever it drops below the low-water mark. It holds the
	//    operator's keys and runs client-side (not in the server monitor).
	keeperCfg := fuelkeeper.FromThroughput(um.Throughput, denomination)
	keeperCfg.Originator = demoenv.Originator
	keeper, err := fuelkeeper.New(env.Wallet, keeperCfg, env.Logger)
	if err != nil {
		log.Fatalf("build fuel keeper: %v", err)
	}
	// keeper.Run(ctx) runs the top-up loop until ctx is canceled; RunOnce does a
	// single pass. We only construct it here to show the wiring.
	_ = keeper
	fmt.Printf("fuel keeper configured: refill when pool < %d%% of %d coins\n",
		um.Throughput.LowWaterPercent, um.Throughput.TargetPool())

	fmt.Printf("\nSee docs/high-throughput-guide.md for backend tuning and the\n")
	fmt.Printf("durable-vs-relaxed throughput numbers.\n")
}
