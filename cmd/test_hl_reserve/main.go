// Command test_hl_reserve buys Hyperliquid request capacity with
// reserveRequestWeight. It is separate from cmd/test_hl_orders so the main-wallet
// key is used by this one command only. Works on testnet or mainnet, selected by
// -network. Sends nothing without -live. Mainnet with -live also needs
// -confirm-mainnet.
//
// Keys and addresses come from internal/hlnet:
//
//	testnet: HL_TESTNET_MASTER_KEY, HL_TESTNET_ACCOUNT
//	mainnet: HL_MAINNET_MASTER_KEY, HL_MAINNET_MASTER_ADDRESS
//
// Usage:
//
//	set -a; source .env; set +a
//	go run ./cmd/test_hl_reserve -network testnet -weight 12000          # dry run
//	go run ./cmd/test_hl_reserve -network testnet -weight 12000 -live
//	go run ./cmd/test_hl_reserve -network mainnet -weight 12000 -live -confirm-mainnet
//
// Cost: 0.0005 USDC per weight unit, from the perps balance.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/internal/hlnet"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid/exchange"
)

var (
	flagNetwork        = flag.String("network", "testnet", "testnet (default) or mainnet")
	flagWeight         = flag.Int("weight", 0, "request weight to reserve (0.0005 USDC each)")
	flagLive           = flag.Bool("live", false, "required to send the reserve action")
	flagConfirmMainnet = flag.Bool("confirm-mainnet", false, "required with -live -network mainnet: spends real USDC")
)

const maxWeight = 20000 // guard against a typo: 20000 units = 10 USDC

func main() {
	flag.Parse()
	os.Exit(run())
}

func run() int {
	env, err := hlnet.Resolve(*flagNetwork)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REFUSED: %v\n", err)
		return 2
	}
	if *flagWeight <= 0 || *flagWeight > maxWeight {
		fmt.Fprintf(os.Stderr, "REFUSED: -weight must be between 1 and %d\n", maxWeight)
		return 2
	}
	if env.MasterKey == "" || env.Account == "" {
		fmt.Fprintf(os.Stderr, "REFUSED: %s and %s must both be set for %s\n", env.MasterVar, env.AccountVar, env.Network)
		return 2
	}
	if !hlnet.ValidAddr(env.Account) {
		fmt.Fprintf(os.Stderr, "REFUSED: %s is not a 0x + 40 hex address\n", env.AccountVar)
		return 2
	}
	if env.Network == "mainnet" && *flagLive && !*flagConfirmMainnet {
		fmt.Fprintln(os.Stderr, "REFUSED: mainnet reserve needs -confirm-mainnet (spends real USDC).")
		return 2
	}
	cost := float64(*flagWeight) * 0.0005
	fmt.Printf("plan: reserve weight=%d on %s, cost ~%.4f USDC from the perps balance, account %s\n",
		*flagWeight, env.Network, cost, env.Account)
	if !*flagLive {
		fmt.Println("dry run: -live not given. Nothing sent.")
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := exchange.New(ctx, exchange.Config{Network: env.Network, AgentKey: env.MasterKey, Account: env.Account})
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		return 1
	}
	res, err := c.ReserveRequestWeight(ctx, *flagWeight)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reserve failed: %v\n", err)
		return 1
	}
	fmt.Printf("reserve result: status=%s response=%+v\n", res.Status, res.Response)
	if res.Status != "ok" {
		return 1
	}
	return 0
}
