// Command test_ws validates the Polymarket connector lifecycle end-to-end.
//
//  1. Connect to market WS + authenticated user WS
//  2. Fetch a market by slug from the Gamma API
//  3. Place a limit order via CLOB API
//  4. Verify the OrderPlacementEvent is received via user WS
//  5. Cancel the order
//  6. Verify the OrderCancelEvent is received
//  7. Print a latency-annotated results table
//
// Usage:
//
//	cd /path/to/project  # auto-detects .env
//	go run ./cmd/test_ws/ --slug <market-slug> \
//	  --price 0.65 --size 100 --side BUY --cancel-after 5s
//
//	# Or derive credentials on the fly:
//	go run ./cmd/test_ws/ --slug <slug> --price 0.65 --size 100 --derive
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
	Side        string
	Price       float64
	Size        float64
	CancelAfter time.Duration
	Derive      bool
	Verbose     bool
}

func loadConfig() config {
	cfg := config{
		PrivateKey:    getEnv("PRIVATE_KEY", ""),
		Funder:        getEnv("FUNDER", ""),
		APIKey:        getEnv("POLYMARKET_KEY", ""),
		Secret:        getEnv("POLYMARKET_SECRET", ""),
		Passphrase:    getEnv("POLYMARKET_PASSPHRASE", ""),
		ClobURL:       getEnv("POLY_CLOB_URL", ""),
		SignatureType: getEnvInt("POLY_SIGNATURE_TYPE", 1), // default POLY_PROXY
	}

	flag.StringVar(&cfg.Slug, "slug", "", "Market slug (required, e.g. will-eth-reach-10k-by-2025)")
	flag.StringVar(&cfg.Side, "side", "BUY", "Order side: BUY or SELL")
	flag.Float64Var(&cfg.Price, "price", 0, "Order price (required)")
	flag.Float64Var(&cfg.Size, "size", 0, "Order size (required, BUY=USDC amount, SELL=shares)")
	flag.DurationVar(&cfg.CancelAfter, "cancel-after", 5*time.Second, "Wait before cancelling")
	flag.BoolVar(&cfg.Derive, "derive", false, "Auto-derive API credentials from PRIVATE_KEY+FUNDER")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable debug logging")

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
		cfg.OwnerUUID = cfg.APIKey // same UUID
	}
	if cfg.MakerAddress == "" {
		cfg.MakerAddress = cfg.Funder // proxy address
	}
	if cfg.SignerAddress == "" {
		// For POLY_PROXY: signer = EOA address from PRIVATE_KEY, NOT the funder
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

// loadDotEnv reads .env from the working directory and populates
// any env vars that aren't already set.
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

// addressFromPrivateKey derives the Ethereum address from a hex-encoded private key.
func addressFromPrivateKey(hexStr string) string {
	key := parsePrivateKey(hexStr)
	return crypto.PubkeyToAddress(key.PublicKey).Hex()
}

// ─────────────────────────────────────────────────────────────
// Event collector
// ─────────────────────────────────────────────────────────────

type eventCollector struct {
	placements []placementRecord
	cancels    []cancelRecord

	placementCh chan *connector.OrderPlacementEvent
	cancelCh    chan *connector.OrderCancelEvent
}

type placementRecord struct {
	Event  *connector.OrderPlacementEvent
	RecvAt time.Time
}

type cancelRecord struct {
	Event  *connector.OrderCancelEvent
	RecvAt time.Time
}

func newEventCollector() *eventCollector {
	return &eventCollector{
		placementCh: make(chan *connector.OrderPlacementEvent, 16),
		cancelCh:    make(chan *connector.OrderCancelEvent, 16),
	}
}

// dispatcher returns a function suitable for SetDispatcher.
func (ec *eventCollector) dispatcher() func(any) {
	return func(ev any) {
		switch e := ev.(type) {
		case *connector.OrderPlacementEvent:
			ec.placements = append(ec.placements, placementRecord{
				Event:  e,
				RecvAt: time.Now(),
			})
			select {
			case ec.placementCh <- e:
			default:
			}
		case *connector.OrderCancelEvent:
			ec.cancels = append(ec.cancels, cancelRecord{
				Event:  e,
				RecvAt: time.Now(),
			})
			select {
			case ec.cancelCh <- e:
			default:
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Result table
// ─────────────────────────────────────────────────────────────

type testResult struct {
	Step      string
	OrderID   string
	BrokerID  string
	OrderType string
	MarketID  string
	Side      string
	Price     float64
	Size      float64
	Latency   time.Duration
	Success   bool
	Error     string
}

func printResults(results []testResult) {
	// Column widths.
	colStep := 18
	colOrderID := 22
	colBrokerID := 22
	colType := 12
	colMarket := 20
	colSide := 6
	colLatency := 12
	colSuccess := 8

	sep := func() {
		fmt.Print("+")
		fmt.Print(strings.Repeat("-", colStep+2), "+")
		fmt.Print(strings.Repeat("-", colOrderID+2), "+")
		fmt.Print(strings.Repeat("-", colBrokerID+2), "+")
		fmt.Print(strings.Repeat("-", colType+2), "+")
		fmt.Print(strings.Repeat("-", colMarket+2), "+")
		fmt.Print(strings.Repeat("-", colSide+2), "+")
		fmt.Print(strings.Repeat("-", colLatency+2), "+")
		fmt.Print(strings.Repeat("-", colSuccess+2), "+")
		fmt.Println()
	}

	row := func(r testResult) {
		orderID := r.OrderID
		if len(orderID) > colOrderID {
			orderID = orderID[:colOrderID-3] + "..."
		}
		brokerID := r.BrokerID
		if len(brokerID) > colBrokerID {
			brokerID = brokerID[:colBrokerID-3] + "..."
		}
		marketID := r.MarketID
		if len(marketID) > colMarket {
			marketID = marketID[:colMarket-3] + "..."
		}
		lat := r.Latency.Round(time.Millisecond).String()
		success := "✅"
		if !r.Success {
			success = "❌"
		}

		fmt.Printf("| %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s | %-*s |\n",
			colStep, r.Step,
			colOrderID, orderID,
			colBrokerID, brokerID,
			colType, r.OrderType,
			colMarket, marketID,
			colSide, r.Side,
			colLatency, lat,
			colSuccess, success,
		)
	}

	header := testResult{
		Step:      "Step",
		OrderID:   "Order ID",
		BrokerID:  "Broker ID",
		OrderType: "Type",
		MarketID:  "Market ID",
		Side:      "Side",
		Latency:   0,
		Success:   true,
	}

	sep()
	row(header)
	sep()
	for _, r := range results {
		row(r)
	}
	sep()

	// Print errors if any.
	for _, r := range results {
		if r.Error != "" {
			fmt.Printf("\n⚠️  %s: %s\n", r.Step, r.Error)
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
	if cfg.Price <= 0 {
		fmt.Fprintln(os.Stderr, "❌ --price is required and must be > 0")
		missing = true
	}
	if cfg.Size <= 0 {
		fmt.Fprintln(os.Stderr, "❌ --size is required and must be > 0")
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

	// ── Wait for WS to stabilize ────────────────────────────
	time.Sleep(2 * time.Second)

	// ── 1. Fetch market by slug ────────────────────────────
	fmt.Printf("\n📡 Fetching market by slug: %s...\n", cfg.Slug)
	t0 := time.Now()
	market, err := conn.GetMarket("", cfg.Slug)
	latFetch := time.Since(t0)

	results := []testResult{}

	if err != nil {
		results = append(results, testResult{
			Step:    "Fetch Market",
			Success: false,
			Error:   err.Error(),
		})
		printResults(results)
		os.Exit(1)
	}

	results = append(results, testResult{
		Step:     "Fetch Market",
		MarketID: market.ID,
		Success:  true,
		Latency:  latFetch,
	})

	fmt.Printf("✅ Market: %s\n", market.Question)
	fmt.Printf("   ConditionID: %s\n", market.ConditionID)
	fmt.Printf("   YesAssetID:  %s\n", market.YesAssetID)
	fmt.Printf("   NoAssetID:   %s\n", market.NoAssetID)
	fmt.Printf("   TickSize:    %v\n", market.TickSize)

	// ── 2. Place limit order ────────────────────────────────
	assetID := market.YesAssetID
	side := strings.ToUpper(cfg.Side)

	order := connector.LimitOrder{
		OrderID:  fmt.Sprintf("test-%d", time.Now().UnixNano()),
		AssetID:  assetID,
		MarketID: market.ID,
		Side:     side,
		Price:    cfg.Price,
		Size:     cfg.Size,
	}

	fmt.Printf("\n📝 Placing %s limit order: %.2f @ %.4f on %s\n",
		side, cfg.Size, cfg.Price, assetID)

	tPlace := time.Now()
	orderResult, err := conn.PlaceLimitOrders([]connector.LimitOrder{order})
	latPlace := time.Since(tPlace)

	if err != nil {
		results = append(results, testResult{
			Step:    "Place Order",
			Success: false,
			Error:   err.Error(),
		})
		printResults(results)
		os.Exit(1)
	}

	orderID := ""
	if len(orderResult.Orders) > 0 {
		orderID = orderResult.Orders[0].OrderID
	}

	placeSuccess := len(orderResult.Orders) > 0 && orderResult.Orders[0].Success
	placeErr := ""
	if !placeSuccess && len(orderResult.Orders) > 0 {
		placeErr = orderResult.Orders[0].ErrorMsg
	}

	results = append(results, testResult{
		Step:      "Place Order",
		OrderID:   orderID,
		MarketID:  market.ID,
		Side:      side,
		Price:     cfg.Price,
		Size:      cfg.Size,
		OrderType: "LIMIT",
		Latency:   latPlace,
		Success:   placeSuccess,
		Error:     placeErr,
	})

	if placeSuccess {
		fmt.Printf("✅ Order placed: %s (latency: %v)\n", orderID, latPlace.Round(time.Millisecond))
	} else {
		fmt.Printf("❌ Order failed: %s\n", placeErr)
		printResults(results)
		os.Exit(1)
	}

	// ── 3. Wait for placement event from user WS ────────────
	fmt.Printf("\n📡 Waiting for OrderPlacementEvent from user WS...\n")

	select {
	case placementEv := <-ec.placementCh:
		latWS := time.Since(tPlace)
		results = append(results, testResult{
			Step:      "WS Placement",
			OrderID:   orderID,
			BrokerID:  placementEv.BrokerID,
			MarketID:  market.ID, // use Gamma market ID, not condition ID
			Side:      placementEv.Side,
			Price:     placementEv.Price,
			Size:      placementEv.Size,
			OrderType: "LIMIT",
			Latency:   latWS,
			Success:   true,
		})
		fmt.Printf("✅ Placement event received in %v\n", latWS.Round(time.Millisecond))
		fmt.Printf("   BrokerID: %s | AssetID: %s | Side: %s | Price: %.4f | Size: %.2f\n",
			placementEv.BrokerID, placementEv.AssetID, placementEv.Side,
			placementEv.Price, placementEv.Size)

	case <-time.After(10 * time.Second):
		results = append(results, testResult{
			Step:    "WS Placement",
			OrderID: orderID,
			Success: false,
			Error:   "timeout waiting for OrderPlacementEvent (10s)",
		})
		fmt.Println("⚠️  Timeout waiting for placement event — user WS may not be connected properly")
	}

	// ── 4. Wait before cancelling ───────────────────────────
	fmt.Printf("\n⏳ Waiting %v before cancelling...\n", cfg.CancelAfter)
	time.Sleep(cfg.CancelAfter)

	// ── 5. Cancel the order ─────────────────────────────────
	fmt.Printf("\n🗑️  Cancelling order %s...\n", orderID)
	tCancel := time.Now()
	err = conn.CancelOrders([]string{orderID})
	latCancel := time.Since(tCancel)

	if err != nil {
		results = append(results, testResult{
			Step:    "Cancel Order",
			OrderID: orderID,
			Success: false,
			Error:   err.Error(),
		})
		fmt.Printf("❌ Cancel failed: %v\n", err)
	} else {
		results = append(results, testResult{
			Step:    "Cancel Order",
			OrderID: orderID,
			Latency: latCancel,
			Success: true,
		})
		fmt.Printf("✅ Cancel request sent in %v\n", latCancel.Round(time.Millisecond))
	}

	// ── 6. Wait for cancel event from user WS ───────────────
	fmt.Printf("\n📡 Waiting for OrderCancelEvent from user WS...\n")

	select {
	case cancelEv := <-ec.cancelCh:
		latCancelWS := time.Since(tCancel)
		results = append(results, testResult{
			Step:      "WS Cancel",
			OrderID:   orderID,
			BrokerID:  cancelEv.BrokerID,
			MarketID:  "", // OrderCancelEvent has no MarketID field
			OrderType: "CANCEL",
			Latency:   latCancelWS,
			Success:   true,
		})
		fmt.Printf("✅ Cancel event received in %v\n", latCancelWS.Round(time.Millisecond))
		fmt.Printf("   BrokerID: %s | AssetID: %s\n", cancelEv.BrokerID, cancelEv.AssetID)

	case <-time.After(10 * time.Second):
		results = append(results, testResult{
			Step:    "WS Cancel",
			OrderID: orderID,
			Success: false,
			Error:   "timeout waiting for OrderCancelEvent (10s)",
		})
		fmt.Println("⚠️  Timeout waiting for cancel event — user WS may not be connected properly")
	}

	// ── 7. Print results ────────────────────────────────────
	fmt.Println("\n" + strings.Repeat("═", 120))
	fmt.Println("                          TEST RESULTS")
	fmt.Println(strings.Repeat("═", 120))
	printResults(results)

	fmt.Println("\n✨ Done.")
}
