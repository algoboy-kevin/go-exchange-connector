// Command test_hl_fill runs a real fill round trip on Hyperliquid: an IOC buy that
// crosses the book, verified against userFills and the position, then a
// reduce-only IOC sell that flattens it, then an IOC that cannot fill (must rest
// nothing). Works on testnet or mainnet, selected by -network.
//
// All reads go through the connector client (Meta, Mids, Positions, UserFills,
// OpenOrders). Orders go through hl.Client.Place. Price and size strings are
// formatted with go-common/decimal.
//
// Sends nothing without -live. Mainnet with -live also needs -confirm-mainnet.
// Entry notional is small by default ($11, venue minimum is $10). The position is
// closed in the same run. If the close fails the command exits 1 and prints the
// open position so it can be handled by hand.
//
// Usage:
//
//	set -a; source .env; set +a
//	go run ./cmd/test_hl_fill -network testnet                 # plan only
//	go run ./cmd/test_hl_fill -network testnet -live
//	go run ./cmd/test_hl_fill -network mainnet -live -confirm-mainnet
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/algoboy-kevin/go-common/decimal"
	"github.com/algoboy-kevin/go-exchange-connector/internal/hlnet"
	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid/exchange"
)

var (
	flagNetwork        = flag.String("network", "testnet", "testnet (default) or mainnet")
	flagCoin           = flag.String("coin", "BTC", "perp coin")
	flagNotional       = flag.Float64("notional", 11, "entry notional in USD (venue minimum $10, max $50)")
	flagLive           = flag.Bool("live", false, "required to place the fill round trip")
	flagConfirmMainnet = flag.Bool("confirm-mainnet", false, "required with -live -network mainnet")
)

func main() {
	flag.Parse()
	os.Exit(run())
}

func parseF(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// feeOf totals one side of a round trip: size, average price, fee and closed PnL.
func feeOf(fills []hl.Fill) (sz, avg, fee float64, feeToken string, pnl float64) {
	var notional float64
	for _, f := range fills {
		s := parseF(f.Sz)
		sz += s
		notional += s * parseF(f.Px)
		fee += parseF(f.Fee)
		pnl += parseF(f.ClosedPnl)
		feeToken = f.FeeToken
	}
	if sz > 0 {
		avg = notional / sz
	}
	return
}

// fillsFor polls UserFills until a fill with the given oid appears or the wait ends.
func fillsFor(ctx context.Context, c *hl.Client, oid int64, wait time.Duration) ([]hl.Fill, error) {
	deadline := time.Now().Add(wait)
	for {
		all, err := c.UserFills(ctx)
		if err != nil {
			return nil, err
		}
		var mine []hl.Fill
		for _, f := range all {
			if f.Oid == oid {
				mine = append(mine, f)
			}
		}
		if len(mine) > 0 {
			return mine, nil
		}
		if time.Now().After(deadline) {
			return nil, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// signedPosition returns the signed perp size of coin on the master account (0 if flat).
func signedPosition(ctx context.Context, c *hl.Client, coin string) (float64, error) {
	ps, err := c.Positions(ctx)
	if err != nil {
		return 0, err
	}
	for _, p := range ps {
		if p.Coin == coin {
			return parseF(p.Szi), nil
		}
	}
	return 0, nil
}

// szDecimalsOf returns the size decimals for coin from the venue meta cached at New.
func szDecimalsOf(ctx context.Context, c *hl.Client, coin string) (int, bool) {
	m, err := c.Meta(ctx)
	if err != nil {
		return 0, false
	}
	for _, a := range m.Universe {
		if a.Name == coin {
			return a.SzDecimals, true
		}
	}
	return 0, false
}

func run() int {
	env, err := hlnet.Resolve(*flagNetwork)
	if err != nil {
		fmt.Printf("REFUSED: %v\n", err)
		return 2
	}
	if *flagNotional < 10 || *flagNotional > 50 {
		fmt.Println("REFUSED: -notional must be between 10 and 50 USD")
		return 2
	}
	if env.AgentKey == "" || env.Account == "" {
		fmt.Printf("REFUSED: %s and %s must both be set for %s\n", env.AgentVar, env.AccountVar, env.Network)
		return 2
	}
	if !hlnet.ValidAddr(env.Account) {
		fmt.Printf("REFUSED: %s is not a 0x + 40 hex address\n", env.AccountVar)
		return 2
	}
	if env.Network == "mainnet" && *flagLive && !*flagConfirmMainnet {
		fmt.Println("REFUSED: mainnet fill test needs -confirm-mainnet (places real orders).")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	c, err := hl.New(ctx, hl.Config{Network: env.Network, AgentKey: env.AgentKey, Account: env.Account})
	if err != nil {
		fmt.Printf("client FAIL: %v\n", err) // ConfigError text carries shape only
		return 1
	}

	mids, err := c.Mids(ctx)
	if err != nil {
		fmt.Printf("market read FAIL: %v\n", err)
		return 1
	}
	mid := parseF(mids[*flagCoin])
	if mid <= 0 {
		fmt.Printf("market read FAIL: no mid for %s\n", *flagCoin)
		return 1
	}
	szDec, ok := szDecimalsOf(ctx, c, *flagCoin)
	if !ok {
		fmt.Printf("market read FAIL: %s not in meta universe\n", *flagCoin)
		return 1
	}

	entryPx := decimal.Sig(mid*1.005, 5, 6-szDec)
	entrySz := decimal.CeilStep(*flagNotional/parseF(entryPx), szDec)
	fmt.Printf("network=%s live=%v coin=%s mid=%.1f szDecimals=%d\n", env.Network, *flagLive, *flagCoin, mid, szDec)
	fmt.Printf("plan: IOC buy px=%s sz=%s (~$%.2f), then reduce-only IOC sell to flatten, then IOC no-fill probe\n",
		entryPx, entrySz, parseF(entryPx)*parseF(entrySz))
	if !*flagLive {
		fmt.Println("dry run: -live not given. Nothing sent.")
		return 0
	}

	before, err := signedPosition(ctx, c, *flagCoin)
	if err != nil {
		fmt.Printf("position read FAIL: %v\n", err)
		return 1
	}
	if before != 0 {
		fmt.Printf("REFUSED: %s position is already %v; flatten it first so the test starts flat.\n", *flagCoin, before)
		return 2
	}

	fails := 0
	check := func(name string, pass bool, detail string) {
		mark := "PASS"
		if !pass {
			mark = "FAIL"
			fails++
		}
		fmt.Printf("[%s] %s: %s\n", mark, name, detail)
	}

	// 1. Entry: IOC buy crossing the ask.
	t0 := time.Now()
	st, err := c.Place(ctx, []hl.Order{{Coin: *flagCoin, IsBuy: true, Px: entryPx, Sz: entrySz, Tif: hl.TifIoc}})
	entryMs := time.Since(t0)
	if err != nil || len(st) != 1 {
		fmt.Printf("entry place FAIL: %v\n", err)
		return 1
	}
	entry := st[0]
	check("entry IOC filled", entry.Filled && entry.Error == "", fmt.Sprintf("oid=%d totalSz=%s avgPx=%s err=%q ack=%s",
		entry.Oid, entry.TotalSz, entry.AvgPx, entry.Error, entryMs.Round(time.Millisecond)))
	if !entry.Filled {
		fmt.Println("stopping: entry did not fill, nothing to close.")
		return 1
	}

	// 2. Verify the entry fill against userFills, then the position.
	fills, err := fillsFor(ctx, c, entry.Oid, 15*time.Second)
	if err != nil {
		fmt.Printf("userFills FAIL: %v\n", err)
		return flattenFailed(ctx, c, env)
	}
	fsz, favg, ffee, ftok, _ := feeOf(fills)
	check("entry in userFills", len(fills) > 0, fmt.Sprintf("fills=%d size=%.5f avgPx=%.1f fee=%.6f %s",
		len(fills), fsz, favg, ffee, ftok))
	pos, err := signedPosition(ctx, c, *flagCoin)
	if err != nil {
		fmt.Printf("position read FAIL: %v\n", err)
		return flattenFailed(ctx, c, env)
	}
	check("position opened", math.Abs(pos-parseF(entry.TotalSz)) < 1e-9, fmt.Sprintf("szi=%v expected=%s", pos, entry.TotalSz))

	// 3. Exit: reduce-only IOC sell crossing the bid, for the filled size.
	exitPx := decimal.Sig(mid*0.995, 5, 6-szDec)
	t1 := time.Now()
	st2, err := c.Place(ctx, []hl.Order{{Coin: *flagCoin, IsBuy: false, Px: exitPx, Sz: entry.TotalSz, Tif: hl.TifIoc, ReduceOnly: true}})
	exitMs := time.Since(t1)
	if err != nil || len(st2) != 1 {
		fmt.Printf("close place FAIL: %v\n", err)
		return flattenFailed(ctx, c, env)
	}
	exit := st2[0]
	check("exit reduce-only IOC filled", exit.Filled && exit.Error == "", fmt.Sprintf("oid=%d totalSz=%s avgPx=%s err=%q ack=%s",
		exit.Oid, exit.TotalSz, exit.AvgPx, exit.Error, exitMs.Round(time.Millisecond)))
	if !exit.Filled {
		return flattenFailed(ctx, c, env)
	}
	xfills, err := fillsFor(ctx, c, exit.Oid, 15*time.Second)
	if err != nil {
		fmt.Printf("userFills FAIL: %v\n", err)
		return flattenFailed(ctx, c, env)
	}
	xsz, xavg, xfee, _, xpnl := feeOf(xfills)
	check("exit in userFills", len(xfills) > 0, fmt.Sprintf("fills=%d size=%.5f avgPx=%.1f fee=%.6f closedPnl=%.6f",
		len(xfills), xsz, xavg, xfee, xpnl))

	// 4. Position must be flat again.
	after, err := signedPosition(ctx, c, *flagCoin)
	if err != nil {
		fmt.Printf("position read FAIL: %v\n", err)
		return flattenFailed(ctx, c, env)
	}
	check("position flat after close", math.Abs(after) < 1e-9, fmt.Sprintf("szi=%v", after))

	// 5. IOC that cannot fill: buy far below the book; must not fill and must not rest.
	farPx := decimal.Sig(mid*0.5, 5, 6-szDec)
	st3, err := c.Place(ctx, []hl.Order{{Coin: *flagCoin, IsBuy: true, Px: farPx, Sz: entrySz, Tif: hl.TifIoc}})
	if err != nil || len(st3) != 1 {
		fmt.Printf("no-fill probe FAIL: %v\n", err)
		fails++
	} else {
		p := st3[0]
		check("IOC no-fill does not fill", !p.Filled, fmt.Sprintf("filled=%v resting=%v err=%q", p.Filled, p.Resting, p.Error))
		check("IOC no-fill does not rest", !p.Resting, fmt.Sprintf("resting=%v", p.Resting))
	}
	open, err := c.OpenOrders(ctx)
	if err != nil {
		fmt.Printf("openOrders FAIL: %v\n", err)
		fails++
	} else {
		check("no open orders left", len(open) == 0, fmt.Sprintf("count=%d", len(open)))
	}

	fmt.Printf("\nsummary: round-trip fee=%.6f %s, closedPnl=%.6f, place->fill ack entry=%s exit=%s, failures=%d\n",
		ffee+xfee, ftok, xpnl, entryMs.Round(time.Millisecond), exitMs.Round(time.Millisecond), fails)
	if fails > 0 {
		return 1
	}
	return 0
}

// flattenFailed reports the open position so it can be closed by hand. It does
// not retry, so a second order cannot be sent by surprise.
func flattenFailed(ctx context.Context, c *hl.Client, env hlnet.Env) int {
	pos, err := signedPosition(ctx, c, *flagCoin)
	if err != nil {
		fmt.Printf("WARNING: could not read position after failed close: %v\n", err)
	} else {
		fmt.Printf("WARNING: position after failed close is szi=%v on %s %s. Close it by hand.\n", pos, env.Network, *flagCoin)
	}
	return 1
}
