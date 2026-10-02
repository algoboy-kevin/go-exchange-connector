// Probe: call the CryptoPrice client for a Polymarket crypto market and verify
// the window close price is present (window completed).
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	conn := polymarket.New(false, polymarket.Config{}, nil)

	req := connector.CryptoPriceRequest{
		Slug:                "btc-updown-5m-1787403300",
		Symbol:              "BTC",
		Variant:             "fiveminute",
		TWAPEnabled:         true,
		TWAPLookbackSeconds: 60,
	}

	price, err := conn.GetCryptoPrice(req)
	if err != nil {
		fmt.Println("❌ GetCryptoPrice:", err)
		return
	}

	b, _ := json.MarshalIndent(price, "", "  ")
	fmt.Printf("price:\n%s\n", string(b))
	fmt.Printf("open=%.6f close=", price.OpenPrice)
	if price.ClosePrice != nil {
		fmt.Printf("%.6f", *price.ClosePrice)
	} else {
		fmt.Print("MISSING (nil)")
	}
	fmt.Printf(" completed=%v ts=%s\n",
		price.Completed,
		time.UnixMilli(price.Timestamp).UTC().Format(time.RFC3339))
}
