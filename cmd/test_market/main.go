// Command test_market validates Polymarket market-order lifecycle end-to-end.
//
//  1. Connect to market WS + authenticated user WS
//  2. Fetch a market by slug from the Gamma API
//  3. Place a BUY market order for $1
//  4. Verify the OrderFillEvent is received, capturing shares bought
//  5. Place a SELL market order for all remaining shares
//  6. Verify the OrderFillEvent is received
//  7. Print a latency-annotated results table
//
// Usage:
//
//	cd /path/to/project  # auto-detects .env
//	go run ./cmd/test_market/ --slug <market-slug>
//
//	# Derive credentials on the fly:
//	go run ./cmd/test_market/ --slug <slug> --derive
package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"github.com/ethereum/go-ethereum/crypto"
)

// ─────────────────────────────────────────────────────────────
// CLI flags & env
// ─────────────────────────────────────────────────────────────

type config struct {
	// Polymarket credentials (from .env or environment).
	PrivateKey    string
	Funder        string
	APIKey        string
	Secret        string
	Passphrase    string
	ClobURL       string
	SignatureType int

	// Derived or auto-filled.
	OwnerUUID     string
	MakerAddress  string
	SignerAddress string

	// Test parameters (from flags).
	Slug        string
	Outcome     string
	BuyAmount   float64
	MaxPrice    float64
	Verbose     bool
	Derive      bool
}

func loadConfig() config {
	cfg := config{
		PrivateKey:    getEnv("PRIVATE_KEY", ""),
		Funder:        getEnv("FUNDER", ""),
		APIKey:        getEnv("POLYMARKET_KEY", ""),
		Secret:        getEnv("POLYMARKET_SECRET", ""),
		Passphrase:    getEnv("POLYMARKET_PASSPHRASE", ""),
		ClobURL:       getEnv("POLY_CLOB_URL", ""),
		SignatureType: getEnvInt("POLY_SIGNATURE_TYPE", 1),
		BuyAmount:     1.0, // default $1
		MaxPrice:      0.99,
	}

	flag.StringVar(&cfg.Slug, "slug", "", "Market slug (required, e.g. will-eth-reach-10k-by-2025)")
	flag.StringVar(&cfg.Outcome, "outcome", "YES", "Outcome to trade: YES or NO")
	flag.Float64Var(&cfg.BuyAmount, "amount", 1.0, "Dollar amount to spend on BUY (USDC)")
	flag.Float64Var(&cfg.MaxPrice, "max-price", 0.99, "Slippage protection price")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable debug logging")
	flag.BoolVar(&cfg.Derive, "derive", false, "Auto-derive API credentials from PRIVATE_KEY+FUNDER")

	flag.Parse()

	// Auto-derive missing credentials from PRIVATE_KEY + FUNDER.
	if cfg.Derive && (cfg.Secret == "" || cfg.Passphrase == "") {
		fmt.Println("🔑 Auto-deriving API credentials from PRIVATE_KEY + FUNDER...")
		key := parsePrivateKey(cfg.PrivateKey)
		creds, err := polymarket.DeriveCredentials(cfg.ClobURL, key, cfg.Funder)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to derive credentials: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ Derived: apiKey=%s  secret=%s  passphrase=%s\n",
			creds.APIKey, maskSecret(creds.Secret), maskSecret(creds.Passphrase))
		cfg.APIKey = creds.APIKey
		cfg.Secret = creds.Secret
		cfg.Passphrase = creds.Passphrase
	}

	// Auto-fill derived fields.
	if cfg.OwnerUUID == "" {
		cfg.OwnerUUID = cfg.APIKey
	}
	if cfg.MakerAddress == "" {
		cfg.MakerAddress = cfg.Funder
	}
	if cfg.SignerAddress == "" {
		cfg.SignerAddress = addressFromPrivateKey(cfg.PrivateKey)
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := fmt.Sscanf(v, "%d", &fallback)
	if err != nil || n == 0 {
		return fallback
	}
	return fallback
}

func loadDotEnv() {
	f, err := os.Open(".env")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		val = strings.Trim(val, `"'`)
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Warn(".env scanner error", "err", err)
	}
}

func maskSecret(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}

func parsePrivateKey(hexStr string) *ecdsa.PrivateKey {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	keyBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY hex: %v\n", err)
		os.Exit(1)
	}
	key, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY: %v\n", err)
		os.Exit(1)
	}
	return key
}

func addressFromPrivateKey(hexStr string) string {
	key := parsePrivateKey(hexStr)
	return crypto.PubkeyToAddress(key.PublicKey).Hex()
}

// ─────────────────────────────────────────────────────────────
// Event collector
// ─────────────────────────────────────────────────────────────

type fillRecord struct {
	Event  *connector.OrderFillEvent
	RecvAt time.Time
}

type eventCollector struct {
	fills     []fillRecord
	fillCh    chan *connector.OrderFillEvent
}

func newEventCollector() *eventCollector {
	return &eventCollector{
		fillCh: make(chan *connector.OrderFillEvent, 64),
	}
}

func (ec *eventCollector) dispatcher() func(any) {
	return func(ev any) {
		switch e := ev.(type) {
		case *connector.OrderFillEvent:
			ec.fills = append(ec.fills, fillRecord{
				Event:  e,
				RecvAt: time.Now(),
			})
			select {
			case ec.fillCh <- e:
			default:
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Result table
// ─────────────────────────────────────────────────────────────

type testStep struct {
	Label      string
	OrderID    string
	BrokerID   string
	AssetID    string
	Side       string
	Price      float64
	Size       float64
	IsMaker    bool
	Latency    time.Duration
	Success    bool
	Error      string
}

func printResults(steps []testStep) {
	colLabel := 30
	colOrderID := 22
	colAssetID := 20
	colSide := 6
	colPrice := 10
	colSize := 10
	colLatency := 12
	colSuccess := 8

	sep := func() {
		fmt.Print("+")
		fmt.Print(strings.Repeat("-", colLabel+2), "+")
		fmt.Print(strings.Repeat("-", colOrderID+2), "+")
		fmt.Print(strings.Repeat("-", colAssetID+2), "+")
		fmt.Print(strings.Repeat("-", colSide+2), "+")
		fmt.Print(strings.Repeat("-", colPrice+2), "+")
		fmt.Print(strings.Repeat("-", colSize+2), "+")
		fmt.Print(strings.Repeat("-", colLatency+2), "+")
		fmt.Print(strings.Repeat("-", colSuccess+2), "+")
		fmt.Println()
	}

	row := func(s testStep) {
		orderID := s.OrderID
		if len(orderID) > colOrderID {
			orderID = orderID[:colOrderID-3] + "..."
		}
		assetID := s.AssetID
		if len(assetID) > colAssetID {
			assetID = assetID[:colAssetID-3] + "..."
		}
		lat := s.Latency.Round(time.Millisecond).String()
		icon := "✅"
		if !s.Success {
			icon = "❌"
		}

		var sizeStr string
		if s.IsMaker {
			sizeStr = fmt.Sprintf("%.4f*", s.Size)
		} else {
			sizeStr = fmt.Sprintf("%.4f", s.Size)
		}

		fmt.Printf("| %-*s | %-*s | %-*s | %-*s | %*s | %*s | %-*s | %-*s |\n",
			colLabel, s.Label,
			colOrderID, orderID,
			colAssetID, assetID,
			colSide, s.Side,
			colPrice, fmt.Sprintf("%.4f", s.Price),
			colSize, sizeStr,
			colLatency, lat,
			colSuccess, icon,
		)
	}

	sep()
	fmt.Printf("| %-*s | %-*s | %-*s | %-*s | %*s | %*s | %-*s | %-*s |\n",
		colLabel, "Step",
		colOrderID, "Order ID",
		colAssetID, "Asset ID",
		colSide, "Side",
		colPrice, "Price",
		colSize, "Size",
		colLatency, "Latency",
		colSuccess, "Result",
	)
	sep()
	for _, s := range steps {
		row(s)
	}
	sep()
	fmt.Println("  * maker fill (sub-order)")

	for _, s := range steps {
		if s.Error != "" {
			fmt.Printf("\n⚠️  %s: %s\n", s.Label, s.Error)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

func main() {
	loadDotEnv()
	cfg := loadConfig()

	// ── Validation ──────────────────────────────────────────

	missing := false
	if cfg.Slug == "" {
		fmt.Fprintln(os.Stderr, "❌ --slug is required")
		missing = true
	}
	if cfg.PrivateKey == "" {
		fmt.Fprintln(os.Stderr, "❌ missing PRIVATE_KEY")
		missing = true
	}
	if cfg.Funder == "" {
		fmt.Fprintln(os.Stderr, "❌ missing FUNDER")
		missing = true
	}
	if cfg.APIKey == "" {
		fmt.Fprintln(os.Stderr, "❌ missing POLYMARKET_KEY")
		missing = true
	}
	if cfg.Secret == "" {
		fmt.Fprintln(os.Stderr, "❌ missing POLYMARKET_SECRET (use --derive to auto-derive)")
		missing = true
	}
	if cfg.Passphrase == "" {
		fmt.Fprintln(os.Stderr, "❌ missing POLYMARKET_PASSPHRASE (use --derive to auto-derive)")
		missing = true
	}
	if missing {
		os.Exit(1)
	}

	// ── Logging ─────────────────────────────────────────────

	logLvl := slog.LevelInfo
	if cfg.Verbose {
		logLvl = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLvl,
	})))

	// ── Build polymarket config ─────────────────────────────

	pmCfg := polymarket.Config{
		APIKey:            cfg.APIKey,
		Secret:            cfg.Secret,
		Passphrase:        cfg.Passphrase,
		ClobURL:           cfg.ClobURL,
		ClobOwnerUUID:     cfg.OwnerUUID,
		ClobMakerAddress:  cfg.MakerAddress,
		ClobSignerAddress: cfg.SignerAddress,
		ClobSigningKeyHex: cfg.PrivateKey,
		ClobSignatureType: cfg.SignatureType,
	}

	// ── Create connector ────────────────────────────────────

	conn := polymarket.New(true, pmCfg, nil)

	ec := newEventCollector()
	conn.SetDispatcher(ec.dispatcher())

	fmt.Println("🚀 Starting Polymarket connector...")
	tStart := time.Now()
	if err := conn.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Start failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ Connector started in %v\n", time.Since(tStart).Round(time.Millisecond))

	defer conn.Stop()

	// Wait for WS to stabilize.
	time.Sleep(2 * time.Second)

	// ── 1. Fetch market by slug ────────────────────────────
	fmt.Printf("\n📡 Fetching market by slug: %s...\n", cfg.Slug)
	market, err := conn.GetMarket("", cfg.Slug)
	steps := []testStep{}

	if err != nil {
		steps = append(steps, testStep{
			Label:   "Fetch Market",
			Success: false,
			Error:   err.Error(),
		})
		printResults(steps)
		os.Exit(1)
	}

	steps = append(steps, testStep{
		Label: "Fetch Market",
		Size:  cfg.BuyAmount,
	})

	fmt.Printf("✅ Market: %s\n", market.Question)
	fmt.Printf("   ConditionID: %s\n", market.ConditionID)
	fmt.Printf("   YesAssetID:  %s\n", market.YesAssetID)
	fmt.Printf("   NoAssetID:   %s\n", market.NoAssetID)
	fmt.Printf("   TickSize:    %v\n", market.TickSize)

	// ── Pick asset ──────────────────────────────────────────

	outcome := strings.ToUpper(cfg.Outcome)
	var assetID string
	switch outcome {
	case "YES":
		assetID = market.YesAssetID
	case "NO":
		assetID = market.NoAssetID
	default:
		fmt.Fprintf(os.Stderr, "❌ invalid --outcome: %s (must be YES or NO)\n", outcome)
		os.Exit(1)
	}

	// ── 2. Place BUY market order for $1 ────────────────────
	fmt.Printf("\n📝 Placing BUY market order: $%.2f (max price %.4f) on %s\n",
		cfg.BuyAmount, cfg.MaxPrice, assetID)

	buyOrder := connector.MarketOrder{
		OrderID:  fmt.Sprintf("buy-%d", time.Now().UnixNano()),
		AssetID:  assetID,
		MarketID: market.ConditionID,
		Side:     "BUY",
		Price:    cfg.MaxPrice,
		Size:     cfg.BuyAmount,
	}

	tBuy := time.Now()
	buyResult, err := conn.PlaceMarketOrder(buyOrder)
	latBuy := time.Since(tBuy)

	buyOrderID := ""
	if len(buyResult.Orders) > 0 {
		buyOrderID = buyResult.Orders[0].OrderID
	}

	buySuccess := len(buyResult.Orders) > 0 && buyResult.Orders[0].Success
	buyErr := ""
	if !buySuccess && len(buyResult.Orders) > 0 {
		buyErr = buyResult.Orders[0].ErrorMsg
	}

	if err != nil {
		buySuccess = false
		buyErr = err.Error()
	}

	steps = append(steps, testStep{
		Label:   "Market BUY (submit)",
		OrderID: buyOrderID,
		AssetID: assetID,
		Side:    "BUY",
		Price:   cfg.MaxPrice,
		Size:    cfg.BuyAmount,
		Latency: latBuy,
		Success: buySuccess,
		Error:   buyErr,
	})

	if !buySuccess {
		fmt.Printf("❌ BUY market order failed: %s\n", buyErr)
		printResults(steps)
		os.Exit(1)
	}
	fmt.Printf("✅ BUY order placed: %s (latency: %v)\n", buyOrderID, latBuy.Round(time.Millisecond))

	// ── 3. Wait for BUY fill events ─────────────────────────
	fmt.Printf("\n📡 Waiting for OrderFillEvent(s) matching order %s...\n", buyOrderID)

	var totalSharesBought float64
	var buyFillCount int
	buyFillTimeout := 15 * time.Second
	buyFillTimer := time.NewTimer(buyFillTimeout)
	defer buyFillTimer.Stop()

	buyFillStart := time.Now()
collectBuyFills:
	for {
		select {
		case fill := <-ec.fillCh:
			// Only count fills whose BrokerID matches our order.
			if fill.BrokerID != buyOrderID {
				continue
			}
			latFill := time.Since(buyFillStart)
			totalSharesBought += fill.Size
			buyFillCount++
			makerLabel := ""
			if fill.IsMaker {
				makerLabel = " (maker sub-fill)"
			}
			steps = append(steps, testStep{
				Label:    "Market BUY (fill)",
				OrderID:  fill.TradeID,
				BrokerID: fill.BrokerID,
				AssetID:  fill.AssetID,
				Side:     fill.Side,
				Price:    fill.Price,
				Size:     fill.Size,
				IsMaker:  fill.IsMaker,
				Latency:  latFill,
				Success:  true,
			})
			fmt.Printf("   Fill #%d%s: %.4f shares @ %.4f (total: %.4f)\n",
				buyFillCount, makerLabel, fill.Size, fill.Price, totalSharesBought)
			// Keep collecting for a short grace period after each fill.
			buyFillTimer.Reset(3 * time.Second)

		case <-buyFillTimer.C:
			break collectBuyFills
		}
	}

	if buyFillCount == 0 {
		steps = append(steps, testStep{
			Label:   "Market BUY (fill)",
			Success: false,
			Error:   fmt.Sprintf("no fill events received within %v", buyFillTimeout),
		})
		printResults(steps)
		os.Exit(1)
	}

	fmt.Printf("✅ BUY fill complete: %.4f shares bought in %d fill(s)\n",
		totalSharesBought, buyFillCount)

	if totalSharesBought <= 0 {
		fmt.Println("❌ No shares were bought — cannot sell")
		printResults(steps)
		os.Exit(1)
	}

	// ── 4. Place SELL market order for all shares ────────────
	// Use slightly below max-price for sell slippage protection, so we
	// accept fills at or above this floor.
	sellPrice := cfg.MaxPrice - 0.01
	if sellPrice <= 0 {
		sellPrice = 0.01
	}
	fmt.Printf("\n📝 Placing SELL market order: %.4f shares (min price %.4f) on %s\n",
		totalSharesBought, sellPrice, assetID)

	sellOrder := connector.MarketOrder{
		OrderID:  fmt.Sprintf("sell-%d", time.Now().UnixNano()),
		AssetID:  assetID,
		MarketID: market.ConditionID,
		Side:     "SELL",
		Price:    sellPrice,
		Size:     totalSharesBought,
	}

	tSell := time.Now()
	sellResult, err := conn.PlaceMarketOrder(sellOrder)
	latSell := time.Since(tSell)

	sellOrderID := ""
	if len(sellResult.Orders) > 0 {
		sellOrderID = sellResult.Orders[0].OrderID
	}

	sellSuccess := len(sellResult.Orders) > 0 && sellResult.Orders[0].Success
	sellErr := ""
	if !sellSuccess && len(sellResult.Orders) > 0 {
		sellErr = sellResult.Orders[0].ErrorMsg
	}

	if err != nil {
		sellSuccess = false
		sellErr = err.Error()
	}

	steps = append(steps, testStep{
		Label:   "Market SELL (submit)",
		OrderID: sellOrderID,
		AssetID: assetID,
		Side:    "SELL",
		Price:   cfg.MaxPrice,
		Size:    totalSharesBought,
		Latency: latSell,
		Success: sellSuccess,
		Error:   sellErr,
	})

	if !sellSuccess {
		fmt.Printf("❌ SELL market order failed: %s\n", sellErr)
		printResults(steps)
		os.Exit(1)
	}
	fmt.Printf("✅ SELL order placed: %s (latency: %v)\n", sellOrderID, latSell.Round(time.Millisecond))

	// ── 5. Wait for SELL fill events ────────────────────────
	fmt.Printf("\n📡 Waiting for OrderFillEvent(s) matching order %s...\n", sellOrderID)

	var totalSharesSold float64
	var sellFillCount int
	sellFillTimeout := 15 * time.Second
	sellFillTimer := time.NewTimer(sellFillTimeout)
	defer sellFillTimer.Stop()

	sellFillStart := time.Now()
collectSellFills:
	for {
		select {
		case fill := <-ec.fillCh:
			// Only count fills whose BrokerID matches our sell order.
			if fill.BrokerID != sellOrderID {
				continue
			}
			latFill := time.Since(sellFillStart)
			totalSharesSold += fill.Size
			sellFillCount++
			makerLabel := ""
			if fill.IsMaker {
				makerLabel = " (maker sub-fill)"
			}
			steps = append(steps, testStep{
				Label:    "Market SELL (fill)",
				OrderID:  fill.TradeID,
				BrokerID: fill.BrokerID,
				AssetID:  fill.AssetID,
				Side:     fill.Side,
				Price:    fill.Price,
				Size:     fill.Size,
				IsMaker:  fill.IsMaker,
				Latency:  latFill,
				Success:  true,
			})
			fmt.Printf("   Fill #%d%s: %.4f shares @ %.4f (total: %.4f)\n",
				sellFillCount, makerLabel, fill.Size, fill.Price, totalSharesSold)

			sellFillTimer.Reset(3 * time.Second)

		case <-sellFillTimer.C:
			break collectSellFills
		}
	}

	if sellFillCount == 0 {
		steps = append(steps, testStep{
			Label:   "Market SELL (fill)",
			Success: false,
			Error:   fmt.Sprintf("no fill events received within %v", sellFillTimeout),
		})
	}

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════\n")
	fmt.Printf("  Buy:   $%.2f → %.4f shares\n", cfg.BuyAmount, totalSharesBought)
	fmt.Printf("  Sell:  %.4f shares → %.4f USDC\n", totalSharesSold, totalSharesSold*0.5) // approximate
	fmt.Printf("═══════════════════════════════════════════\n")

	printResults(steps)
}
