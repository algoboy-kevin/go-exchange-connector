// Command test_redeem performs a CTF redeemPositions transaction.
//
// Redeems winning outcome tokens back into pUSD (collateral) after a market
// has been resolved. This burns your position tokens and returns the
// equivalent collateral value for the winning outcome(s).
//
// Unlike split/merge, there is no amount parameter — the full balance of
// each position token in the partition is redeemed. For a resolved market
// where the outcome is YES, passing partition=[1,2] will redeem the YES
// tokens and burn the worthless NO tokens.
//
// Supports two modes:
//   - Direct on-chain (requires MATIC for gas, uses --rpc-url):
//     go run ./cmd/test_redeem/ --slug <slug>
//   - Gasless relayer (no MATIC needed, uses RELAYER_API_KEY):
//     go run ./cmd/test_redeem/ --slug <slug> --relayer
//
// It fetches the market by slug and automatically resolves the condition ID.
// The proxy wallet address is auto-derived from your signer key via CREATE2.
//
// Environment variables (auto-loaded from .env):
//
//	PRIVATE_KEY          — Wallet private key (required)
//	CTF_RPC_URL          — Polygon RPC URL (for direct on-chain mode)
//	CTF_CHAIN_ID         — Chain ID (optional, defaults to 137)
//	RELAYER_API_KEY      — Relayer API key (for gasless mode)
//	RELAYER_API_KEY_ADDR — Address that owns the relayer key (for gasless mode)
//
// Flags:
//
//	--slug <slug>          Market slug (required)
//	--partition <indices>  Partition indices to redeem (default: "1,2" = YES and NO)
//	--relayer              Use gasless relayer instead of direct on-chain
//	--rpc-url <url>        Override RPC URL (for direct on-chain mode)
//	--chain-id <id>        Override chain ID (default: 137)
//	--collateral <addr>    Collateral token (default: USDC on Polygon)
//	--parent <hash>        Parent collection ID (default: zero hash)
//	--verbose              Enable debug logging
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ─────────────────────────────────────────────────────────────
// CLI flags & env
// ─────────────────────────────────────────────────────────────

type config struct {
	PrivateKey     string
	RPCURL         string
	ChainID        int64
	Slug           string
	PartitionRaw   string
	Collateral     string
	ParentID       string
	Relayer        bool
	RelayerKey     string
	RelayerKeyAddr string
	Verbose        bool
}

func loadConfig() config {
	cfg := config{
		PrivateKey:     getEnv("PRIVATE_KEY", ""),
		RPCURL:         getEnv("CTF_RPC_URL", ""),
		ChainID:        getEnvInt64("CTF_CHAIN_ID", polymarket.PolygonCTFChainID),
		RelayerKey:     getEnv("RELAYER_API_KEY", ""),
		RelayerKeyAddr: getEnv("RELAYER_API_KEY_ADDR", ""),
	}

	flag.StringVar(&cfg.Slug, "slug", "", "Market slug (required, e.g. will-eth-reach-10k-by-2025)")
	flag.StringVar(&cfg.PartitionRaw, "partition", "1,2", "Partition indices to redeem (default: '1,2' = YES and NO)")
	flag.BoolVar(&cfg.Relayer, "relayer", false, "Use gasless relayer instead of direct on-chain")
	flag.StringVar(&cfg.RPCURL, "rpc-url", cfg.RPCURL, "Polygon RPC URL (for direct on-chain)")
	flag.Int64Var(&cfg.ChainID, "chain-id", cfg.ChainID, "Chain ID (137 = Polygon mainnet, 80002 = Amoy)")
	flag.StringVar(&cfg.Collateral, "collateral", "", "Collateral token address (default: USDC on Polygon)")
	flag.StringVar(&cfg.ParentID, "parent", "", "Parent collection ID (default: zero hash)")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable debug logging")

	flag.Parse()

	if !cfg.Relayer && cfg.RPCURL == "" {
		fmt.Fprintln(os.Stderr, "❌ RPC URL is required for on-chain mode — set CTF_RPC_URL in .env or use --rpc-url")
		fmt.Fprintln(os.Stderr, "   Or use --relayer for gasless mode")
		os.Exit(1)
	}
	if cfg.Relayer && (cfg.RelayerKey == "" || cfg.RelayerKeyAddr == "") {
		fmt.Fprintln(os.Stderr, "❌ RELAYER_API_KEY and RELAYER_API_KEY_ADDR are required in relayer mode")
		fmt.Fprintln(os.Stderr, "   Get these from https://polymarket.com/settings → API Keys")
		os.Exit(1)
	}

	return cfg
}

// parsePartition converts a comma-separated string like "1,2" to []*big.Int.
func parsePartition(s string) ([]*big.Int, error) {
	parts := strings.Split(s, ",")
	result := make([]*big.Int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid partition index %q: %w", p, err)
		}
		result = append(result, big.NewInt(n))
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("partition is empty")
	}
	return result, nil
}

func main() {
	loadDotEnv()
	cfg := loadConfig()

	if !cfg.Verbose {
		slog.SetLogLoggerLevel(slog.LevelWarn)
	}

	// ── Validate required flags ─────────────────────────────
	if cfg.Slug == "" {
		fmt.Fprintln(os.Stderr, "❌ --slug is required")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.PrivateKey == "" {
		fmt.Fprintln(os.Stderr, "❌ PRIVATE_KEY is not set in .env or environment")
		os.Exit(1)
	}

	// ── Parse private key ───────────────────────────────────
	hexStr := strings.TrimPrefix(cfg.PrivateKey, "0x")
	keyBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY hex: %v\n", err)
		os.Exit(1)
	}
	privateKey, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY: %v\n", err)
		os.Exit(1)
	}

	signerAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// ── Parse partition ─────────────────────────────────────
	partition, err := parsePartition(cfg.PartitionRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid --partition: %v\n", err)
		os.Exit(1)
	}

	// ── Fetch market by slug → get condition ID ─────────────
	fmt.Printf("📡 Fetching market by slug: %s...\n", cfg.Slug)

	gamma := polymarket.NewGammaClient("")
	market, err := gamma.FetchMarketBySlug(cfg.Slug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to fetch market: %v\n", err)
		os.Exit(1)
	}

	conditionID := common.HexToHash(market.ConditionID)
	if conditionID == (common.Hash{}) {
		fmt.Fprintf(os.Stderr, "❌ invalid condition ID from market: %s\n", market.ConditionID)
		os.Exit(1)
	}

	// ── Check market resolution ─────────────────────────────
	if market.Resolution == nil {
		fmt.Println("⚠️  Market is not yet resolved — redeem may not yield any collateral.")
		fmt.Println("   Only winning outcome tokens have value after resolution.")
	} else {
		fmt.Printf("✅ Market resolved: %s\n", *market.Resolution)
	}

	// ── Resolve collateral ──────────────────────────────────
	collateral := polymarket.CollateralAddress(cfg.ChainID)
	if cfg.Collateral != "" {
		collateral = common.HexToAddress(cfg.Collateral)
	}

	// ── Resolve parent collection ID ────────────────────────
	parentID := common.Hash{}
	if cfg.ParentID != "" {
		parentID = common.HexToHash(cfg.ParentID)
	}

	// ── Resolve adapter address based on neg-risk flag ──────
	adapter := polymarket.CollateralAdapterAddress
	negRiskLabel := "No"
	if market.NegRisk {
		adapter = polymarket.NegRiskCollateralAdapterAddress
		negRiskLabel = "Yes"
	}

	// ── Print summary ───────────────────────────────────────
	mode := "🔗 Direct On-Chain"
	if cfg.Relayer {
		mode = "⚡ Gasless Relayer"
	}
	fmt.Printf("\n%s\n", mode)
	fmt.Println(strings.Repeat("─", 60))
	fmt.Printf("   Market:         %s\n", market.Question)
	fmt.Printf("   Slug:           %s\n", cfg.Slug)
	fmt.Printf("   Condition ID:   %s\n", conditionID.Hex())
	fmt.Printf("   Signer:         %s\n", signerAddr.Hex())
	fmt.Printf("   Chain ID:       %d\n", cfg.ChainID)
	fmt.Printf("   Neg-Risk:       %s\n", negRiskLabel)
	fmt.Printf("   Adapter:        %s\n", adapter.Hex())
	if market.Resolution != nil {
		fmt.Printf("   Resolution:     %s\n", *market.Resolution)
	}
	fmt.Printf("   Collateral:     %s\n", collateral.Hex())
	fmt.Printf("   Parent:         %s\n", parentID.Hex())
	fmt.Printf("   Partition:      %s\n", formatPartition(partition))
	if !cfg.Relayer {
		fmt.Printf("   RPC URL:        %s\n", maskRPCURL(cfg.RPCURL))
	}
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("   Note: Redeems full balance — no amount parameter needed.")
	fmt.Println(strings.Repeat("─", 60))

	// ── Build request ───────────────────────────────────────
	req := &polymarket.RedeemPositionsRequest{
		CollateralToken:    collateral,
		ParentCollectionID: parentID,
		ConditionID:        conditionID,
		Partition:          partition,
	}

	// ── Execute redeem ──────────────────────────────────────
	var resp *polymarket.RedeemPositionsResponse

	if cfg.Relayer {
		fmt.Println("⏳ Setting up gasless relayer...")
		relayer := polymarket.NewRelayerClient(cfg.RelayerKey, cfg.RelayerKeyAddr)
		if relayer == nil {
			fmt.Fprintln(os.Stderr, "❌ Failed to create relayer client — check RELAYER_API_KEY in .env")
			os.Exit(1)
		}

		fmt.Println("⏳ Submitting redeemPositions via relayer...")
		resp, err = relayer.RedeemPositionsViaRelayer(context.Background(), req, privateKey, market.NegRisk)
	} else {
		fmt.Println("⏳ Sending direct on-chain redeemPositions transaction...")
		ctfClient, ctfErr := polymarket.NewCTFClient(cfg.RPCURL, cfg.ChainID)
		if ctfErr != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to create CTF client: %v\n", ctfErr)
			os.Exit(1)
		}
		defer ctfClient.Close()
		resp, err = ctfClient.RedeemPositions(context.Background(), req, privateKey)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ RedeemPositions failed: %v\n", err)
		os.Exit(1)
	}

	// ── Success ─────────────────────────────────────────────
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("✅ Redeem successful!")
	fmt.Printf("   Tx Hash:    %s\n", resp.TransactionHash.Hex())
	if resp.BlockNumber > 0 {
		fmt.Printf("   Block:      %d\n", resp.BlockNumber)
	}
	fmt.Printf("   Explorer:   https://polygonscan.com/tx/%s\n", resp.TransactionHash.Hex())
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("\n📝 Winning outcome tokens have been redeemed for pUSD.")
	fmt.Println("   Check your wallet balance or use a market tool to verify.")
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func formatPartition(p []*big.Int) string {
	parts := make([]string, len(p))
	for i, v := range p {
		parts[i] = v.String()
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
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
		fmt.Fprintf(os.Stderr, "⚠️  error reading .env: %v\n", err)
	}
}

func maskRPCURL(url string) string {
	if idx := strings.LastIndexByte(url, '/'); idx > 0 && len(url)-idx > 10 {
		return url[:idx+1] + "..." + url[len(url)-4:]
	}
	return url
}
