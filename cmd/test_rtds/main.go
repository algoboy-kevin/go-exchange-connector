// Command test_rtds validates the Polymarket RTDS real-time price streams.
//
// It connects to the RTDS WebSocket (wss://ws-live-data.polymarket.com) and
// subscribes to Binance crypto, Chainlink crypto, Chainlink TWAP, and/or
// equity (Pyth) prices, printing each event dispatched by the connector.
//
// Usage:
//
//	# Binance BTC + ETH for 15 seconds:
//	go run ./cmd/test_rtds/ -symbols btcusdt,ethusdt -duration 15s
//
//	# Add Chainlink feeds and equity symbols:
//	go run ./cmd/test_rtds/ -symbols btcusdt -chainlink eth/usd,btc/usd -equity AAPL,TSLA -duration 15s
//
//	# Chainlink TWAP (btc/usd, 60s window):
//	go run ./cmd/test_rtds/ -twap btc/usd -twap-window 60 -duration 15s
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
)

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	symbolsFlag := flag.String("symbols", "btcusdt", "Comma-separated Binance symbols (e.g. btcusdt,ethusdt)")
	chainlinkFlag := flag.String("chainlink", "", "Comma-separated Chainlink feeds (e.g. eth/usd,btc/usd)")
	twapFlag := flag.String("twap", "", "Comma-separated Chainlink TWAP feeds (e.g. btc/usd,eth/usd)")
	twapWindowFlag := flag.Int("twap-window", 60, "Chainlink TWAP lookback window in seconds (30 or 60)")
	equityFlag := flag.String("equity", "", "Comma-separated equity symbols (e.g. AAPL,TSLA)")
	durationFlag := flag.Duration("duration", 15*time.Second, "How long to stream")
	verbose := flag.Bool("verbose", false, "Enable debug logging")
	flag.Parse()

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	symbols := splitList(*symbolsFlag)
	chainlink := splitList(*chainlinkFlag)
	twap := splitList(*twapFlag)
	equity := splitList(*equityFlag)
	if len(symbols) == 0 && len(chainlink) == 0 && len(twap) == 0 && len(equity) == 0 {
		fmt.Fprintln(os.Stderr, "❌ at least one symbol/feed required")
		os.Exit(1)
	}

	// RTDS is a public data stream — works in paper mode too.
	conn := polymarket.New(false, polymarket.Config{}, nil)

	conn.SetDispatcher(func(ev any) {
		switch e := ev.(type) {
		case *connector.CryptoPriceEvent:
			win := ""
			if e.WindowSeconds > 0 {
				win = fmt.Sprintf(" win=%ds", e.WindowSeconds)
			}
			fmt.Printf("[RTDS] %s(%s) price=%s ts=%s%s\n", e.Symbol, e.Source, e.Price, e.Timestamp.Format(time.RFC3339Nano), win)
		case *connector.EquityPriceEvent:
			fwd := ""
			if e.IsCarriedForward {
				fwd = " (carried forward)"
			}
			fmt.Printf("[RTDS] equity %s price=%s ts=%s%s\n", e.Symbol, e.Price, e.Timestamp.Format(time.RFC3339Nano), fwd)
		case *connector.PriceSnapshotEvent:
			fmt.Printf("[RTDS] %s snapshot %s points=%d ts=%s\n", e.Source, e.Symbol, len(e.Points), e.Timestamp.Format(time.RFC3339Nano))
		default:
			// Ignore other event types.
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), *durationFlag)
	defer cancel()

	if err := conn.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "❌ failed to start connector: %v\n", err)
		os.Exit(1)
	}
	defer conn.Stop()

	if len(symbols) > 0 {
		conn.SubscribeCryptoPrices(ctx, symbols)
	}
	if len(chainlink) > 0 {
		conn.SubscribeChainlinkPrices(ctx, chainlink)
	}
	if len(twap) > 0 {
		conn.SubscribeChainlinkTWAP(ctx, *twapWindowFlag, twap)
	}
	if len(equity) > 0 {
		conn.SubscribeEquityPrices(ctx, equity)
	}
	fmt.Printf("🟢 subscribed: binance=%v chainlink=%v twap=%v win=%ds equity=%v (streaming for %s)\n", symbols, chainlink, twap, *twapWindowFlag, equity, *durationFlag)

	// Wait for the dispatcher to deliver events until ctx expires.
	<-ctx.Done()
	fmt.Println("✅ done")
}
