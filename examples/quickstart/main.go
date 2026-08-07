// Command quickstart is the 5-minute go-arcade-toolbox tour: build a SQLite
// wallet, fund it, send a payment, and read the balance back — end to end,
// against in-process Arcade + ChainTracks mocks so it runs with no external
// services.
//
// Run it:
//
//	go run ./examples/quickstart
//
// The wallet wiring (SQLite storage + real arcade/headers clients + wallet.New)
// lives in examples/internal/demoenv; read that package to see how a wallet is
// assembled. In production you point the Arcade/ChainTracks URLs at a real
// Arcade deployment (docs/arcade-integration.md) and fund the wallet by
// receiving a real payment (examples/internalize) rather than with the demo
// faucet used here.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/bsv-blockchain/go-sdk/script"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/go-arcade-toolbox/examples/internal/demoenv"
)

func main() {
	ctx := context.Background()

	// 1. Stand up a wallet backed by SQLite, wired to (mock) Arcade + ChainTracks.
	env, err := demoenv.Setup(ctx)
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
	defer env.Close()

	fmt.Printf("wallet identity (receive) key: %s\n", env.RecipientHex)

	// 2. A fresh arcade-only wallet has ZERO spendable balance: it learns about
	//    coins only from transactions it created and from InternalizeAction.
	//    There is no restore-from-seed. See docs/operations.md.
	balance, err := env.Wallet.Balance(ctx)
	if err != nil {
		log.Fatalf("balance: %v", err)
	}
	fmt.Printf("balance before funding: %d sat\n", balance)

	// 3. Fund the wallet. Here the demo faucet internalizes a mined payment; in
	//    production this is a real InternalizeAction of a payment you received.
	const seedSats = 100_000
	seedTxid, err := env.SeedFunds(ctx, seedSats, 800_000)
	if err != nil {
		log.Fatalf("seed funds: %v", err)
	}
	fmt.Printf("funded with %d sat via tx %s\n", seedSats, seedTxid)

	balance, err = env.Wallet.Balance(ctx)
	if err != nil {
		log.Fatalf("balance: %v", err)
	}
	fmt.Printf("balance after funding: %d sat\n", balance)

	// 4. Send a payment. CreateAction with SignAndProcess funds from the wallet's
	//    coins, signs with real BRC-29 signatures, and broadcasts the Extended
	//    Format transaction through the Arcade client in one call.
	const paymentSats = 1_000
	lockingScript, err := script.NewFromHex("76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac")
	if err != nil {
		log.Fatalf("locking script: %v", err)
	}
	res, err := env.Wallet.CreateAction(ctx, sdk.CreateActionArgs{
		Description: "quickstart payment",
		Outputs: []sdk.CreateActionOutput{{
			LockingScript:     lockingScript.Bytes(),
			Satoshis:          paymentSats,
			OutputDescription: "a payment to a counterparty",
		}},
		Labels: []string{"quickstart"},
		Options: &sdk.CreateActionOptions{
			SignAndProcess:         ptr(true),
			AcceptDelayedBroadcast: ptr(false),
			RandomizeOutputs:       ptr(false),
		},
	}, demoenv.Originator)
	if err != nil {
		log.Fatalf("create action: %v", err)
	}
	fmt.Printf("sent payment in tx %s\n", res.Txid)
	fmt.Printf("arcade received %d broadcast(s)\n", env.Arcade.BroadcastCount())

	// 5. The change came back to the wallet as spendable coins. Balance now
	//    reflects seed - payment - fee.
	balance, err = env.Wallet.Balance(ctx)
	if err != nil {
		log.Fatalf("balance: %v", err)
	}
	fmt.Printf("balance after payment: %d sat (change from seed - payment - fee)\n", balance)

	// 6. List the wallet's spendable outputs.
	outputs, err := env.Wallet.ListOutputs(ctx, sdk.ListOutputsArgs{}, demoenv.Originator)
	if err != nil {
		log.Fatalf("list outputs: %v", err)
	}
	fmt.Printf("wallet has %d spendable output(s):\n", outputs.TotalOutputs)
	for _, o := range outputs.Outputs {
		fmt.Printf("  %s  %d sat  spendable=%t\n", o.Outpoint.String(), o.Satoshis, o.Spendable)
	}
}

// ptr returns a pointer to v. The go-sdk options structs use pointer fields to
// distinguish "unset" from a zero value.
func ptr[T any](v T) *T { return &v }
