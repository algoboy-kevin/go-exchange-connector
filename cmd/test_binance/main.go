// Command test_binance validates the Binance spot + perpetual futures market
// data streams.
//
// It connects to Binance's raw WebSocket endpoints (spot and USDⓈ-M futures)
// and subscribes to bookTicker, aggTrade, depth, and/or kline streams,
// printing each event dispatched by the connector.
//
// Usage:
//
//	# Spot BTC + ETH bookTicker for 15 seconds:
//	go run ./cmd/test_binance/ -spot btcusdt,ethusdt -duration 15s
//
//	# Perpetual BTC bookTicker + trades + depth + klines:
//	go run ./cmd/test_binance/ -perp btcusdt -trades -depth -kline 1m -duration 15s
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
	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance"
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
	spotFlag := flag.String("spot", "btcusdt", "Comma-separated spot symbols (e.g. btcusdt,ethusdt)")
	perpFlag := flag.String("perp", "", "Comma-separated perpetual symbols (e.g. btcusdt)")
	tradesFlag := flag.Bool("trades", false, "Subscribe aggTrade streams")
	depthFlag := flag.Bool("depth", false, "Subscribe depth streams (local order book)")
	klineFlag := flag.String("kline", "", "Kline interval to subscribe (e.g. 1m, 15m, 1h); empty = no klines")
	durationFlag := flag.Duration("duration", 15*time.Second, "How long to stream")
	verbose := flag.Bool("verbose", false, "Enable debug logging")
	flag.Parse()

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	spot := splitList(*spotFlag)
	perp := splitList(*perpFlag)
	if len(spot) == 0 && len(perp) == 0 {
		fmt.Fprintln(os.Stderr, "❌ at least one spot or perpetual symbol required")
		os.Exit(1)
	}

	// Binance streams are public market data — works in paper mode too.
	base := connector.New(false, nil)

	bn := binance.New(base)
	bn.SetDispatcher(func(ev any) {
		switch e := ev.(type) {
		case *connector.BinanceBookTickerEvent:
			fmt.Printf("[BINANCE] bookTicker %s/%s bid=%s@%s ask=%s@%s ts=%s\n",
				e.Symbol, e.Market, e.BestBidPrice, e.BestBidQty, e.BestAskPrice, e.BestAskQty,
				e.Timestamp.Format(time.RFC3339Nano))
		case *connector.BinanceAggTradeEvent:
			fmt.Printf("[BINANCE] aggTrade %s/%s id=%d price=%s qty=%s maker=%v ts=%s\n",
				e.Symbol, e.Market, e.TradeID, e.Price, e.Quantity, e.IsBuyerMaker,
				e.Timestamp.Format(time.RFC3339Nano))
		case *connector.BinanceDepthEvent:
			fmt.Printf("[BINANCE] depth %s/%s u=%d bids=%d asks=%d best_bid=%s best_ask=%s\n",
				e.Symbol, e.Market, e.LastUpdateID, len(e.Bids), len(e.Asks),
				firstPrice(e.Bids), firstPrice(e.Asks))
		case *connector.BinanceKlineEvent:
			fmt.Printf("[BINANCE] kline %s/%s %s O=%s C=%s H=%s L=%s V=%s final=%v\n",
				e.Symbol, e.Market, e.Interval, e.Open, e.Close, e.High, e.Low, e.Volume, e.IsFinal)
		default:
			// Ignore other event types.
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), *durationFlag)
	defer cancel()

	if err := bn.Start(ctx, 0); err != nil {
		fmt.Fprintf(os.Stderr, "❌ failed to start binance: %v\n", err)
		os.Exit(1)
	}
	defer bn.Stop()

	if len(spot) > 0 {
		bn.SubscribeBookTicker(ctx, binance.MarketSpot, spot)
		if *tradesFlag {
			bn.SubscribeTrades(ctx, binance.MarketSpot, spot)
		}
		if *depthFlag {
			bn.SubscribeDepth(ctx, binance.MarketSpot, spot)
		}
		if *klineFlag != "" {
			if err := bn.SubscribeKlines(ctx, binance.MarketSpot, spot, *klineFlag); err != nil {
				fmt.Fprintf(os.Stderr, "❌ %v\n", err)
				os.Exit(1)
			}
		}
	}
	if len(perp) > 0 {
		bn.SubscribeBookTicker(ctx, binance.MarketPerp, perp)
		if *tradesFlag {
			bn.SubscribeTrades(ctx, binance.MarketPerp, perp)
		}
		if *depthFlag {
			bn.SubscribeDepth(ctx, binance.MarketPerp, perp)
		}
		if *klineFlag != "" {
			if err := bn.SubscribeKlines(ctx, binance.MarketPerp, perp, *klineFlag); err != nil {
				fmt.Fprintf(os.Stderr, "❌ %v\n", err)
				os.Exit(1)
			}
		}
	}
	fmt.Printf("🟢 subscribed spot=%v perp=%v trades=%v depth=%v kline=%s (streaming for %s)\n",
		spot, perp, *tradesFlag, *depthFlag, *klineFlag, *durationFlag)

	<-ctx.Done()
	fmt.Println("✅ done")
}

func firstPrice(levels []connector.Level) string {
	if len(levels) == 0 {
		return "-"
	}
	return levels[0].Price
}
