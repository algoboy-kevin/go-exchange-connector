package polymarket

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// PolymarketConnector is the full Polymarket exchange connector.
//
// It combines:
//   - Market WebSocket (real-time price data, orderbooks, trades)
//   - User WebSocket (authenticated order/trade events — LIVE mode only)
//   - Gamma REST API (market metadata, resolution)
//   - Paper-mode order simulation (via the base Connector's PlaceLimitOrders)
//
// Usage:
//
//	cfg := polymarket.Config{APIKey: "...", Secret: "..."}
//	conn := polymarket.New(false, cfg)
//	conn.SetDispatcher(func(ev any) {
//	    switch e := ev.(type) {
//	    case *connector.PriceChangeEvent:     ...
//	    case *connector.BookSnapshotEvent:    ...
//	    case *connector.TradeEvent:           ...
//	    case *connector.OrderPlacementEvent:  ...
//	    case *connector.OrderFillEvent:       ...
//	    }
//	})
//	conn.Start(ctx)
//	defer conn.Stop()
//	conn.Subscribe([]string{"asset_id_1"})
type PolymarketConnector struct {
	*connector.Connector

	cfg    Config
	gamma  *GammaClient
	market *WSPolymarketMarket
	user   *WSPolymarketUserWS
}

// New creates a new PolymarketConnector.
//
//	isLive: true  → connects market WS + user WS, orders use real API
//	        false → connects market WS only, orders simulated via handlers
//	cfg:    connection configuration (API keys, URL overrides)
//	now:    function returning current time; defaults to time.Now if nil.
//	        Pass a backtest clock for backtest mode.
func New(isLive bool, cfg Config, now func() time.Time) *PolymarketConnector {
	gammaURL := cfg.GammaAPIURL
	if gammaURL == "" {
		gammaURL = defaultGammaAPIURL
	}

	gamma := NewGammaClient(gammaURL)

	// ── CLOB client (LIVE mode) ────────────────────────────
	clobURL := cfg.ClobURL
	if clobURL == "" {
		clobURL = defaultClobURL
	}

	clob := NewClobClient(clobURL, cfg.APIKey, cfg.Secret, cfg.Passphrase,
		WithOwnerUUID(cfg.ClobOwnerUUID),
		WithMakerAddress(cfg.ClobMakerAddress),
		WithSignerAddress(cfg.ClobSignerAddress),
	)

	sigType := cfg.ClobSignatureType
	if sigType == 0 {
		sigType = 1 // default POLY_PROXY
	}
	clob.signatureType = sigType

	// ── EIP-712 signing key ─────────────────────────────────
	var signingKey *ecdsa.PrivateKey
	if cfg.ClobSigningKeyHex != "" {
		hexStr := strings.TrimPrefix(cfg.ClobSigningKeyHex, "0x")
		keyBytes, err := hex.DecodeString(hexStr)
		if err != nil {
			slog.Warn("polymarket: invalid signing key hex, orders will fail", "err", err)
		} else {
			key, err := crypto.ToECDSA(keyBytes)
			if err != nil {
				slog.Warn("polymarket: invalid signing key, orders will fail", "err", err)
			} else {
				signingKey = key
			}
		}
	} else {
		slog.Warn("polymarket: no signing key configured — set ClobSigningKeyHex for LIVE orders")
	}

	pc := &PolymarketConnector{
		Connector: connector.New(isLive, &polymarketLiveExecutor{
			gamma:      gamma,
			clob:       clob,
			signingKey: signingKey,
		}),
		cfg:   cfg,
		gamma: gamma,
	}

	if now != nil {
		pc.Connector.Now = now
	}

	return pc
}

// Start connects the market WebSocket (always) and user WebSocket (LIVE only).
func (p *PolymarketConnector) Start(ctx context.Context) error {
	// Enable async dispatch so WS goroutines never block on the engine handler.
	p.Connector.EnableAsyncDispatch(0) // 0 = default buffer (4096)

	// ── Market WS (all modes) ───────────────────────────────
	wsURL := p.cfg.MarketWSURL
	if wsURL == "" {
		wsURL = marketWSSURL
	}

	p.market = NewWSPolymarketMarket(p.Connector, p.cfg)
	if err := p.market.Start(ctx, wsURL, p.cfg.ReconnectIntervalMs); err != nil {
		return err
	}

	// ── User WS (LIVE mode only) ────────────────────────────
	if p.IsLive && p.cfg.APIKey != "" && p.cfg.Secret != "" && p.cfg.Passphrase != "" {
		userURL := p.cfg.UserWSURL
		if userURL == "" {
			userURL = userWSSURL
		}

		auth := UserAuth{
			APIKey:     p.cfg.APIKey,
			Secret:     p.cfg.Secret,
			Passphrase: p.cfg.Passphrase,
		}

		p.user = NewWSPolymarketUserWS(p.Connector, auth, UserHandlers{})
		if err := p.user.Start(ctx, userURL); err != nil {
			slog.Warn("polymarket: user WS failed to start (continuing)", "err", err)
		}
	}

	slog.Info("polymarket: connector started",
		"is_live", p.IsLive,
		"market_ws", true,
		"user_ws", p.user != nil,
	)

	return nil
}

// Stop shuts down all WebSocket connections gracefully.
func (p *PolymarketConnector) Stop() {
	if p.user != nil {
		p.user.Stop()
	}

	if p.market != nil {
		p.market.Stop()
	}
}

// MarketStatus returns the current WebSocket connection status.
// Consumers can use this to track uptime or disconnection duration.
func (p *PolymarketConnector) MarketStatus() ws.ConnectionStatus {
	if p.market == nil {
		return ws.StatusDisconnected
	}
	return p.market.Status()
}

// SetOnMarketStatusChange registers a callback that fires whenever the market
// WebSocket connection status changes (connected/disconnected/connecting).
// Useful for tracking uptime, computing disconnect duration, etc.
// Must be called before Start.
func (p *PolymarketConnector) SetOnMarketStatusChange(fn func(ws.ConnectionStatus)) {
	if p.market != nil {
		p.market.SetOnStatusChange(fn)
	}
}

// Subscribe starts receiving market data for the given asset IDs.
func (p *PolymarketConnector) Subscribe(assetIDs []string) {
	if p.market != nil {
		// Use background context for subscription calls since the
		// connector is already running.
		p.market.Subscribe(context.Background(), assetIDs)
	}
}

// Unsubscribe stops receiving market data for the given asset IDs.
func (p *PolymarketConnector) Unsubscribe(assetIDs []string) {
	if p.market != nil {
		p.market.Unsubscribe(context.Background(), assetIDs)
	}
}

// GetMarket fetches a market by ID or slug from the Gamma API.
func (p *PolymarketConnector) GetMarket(id, slug string) (*connector.Market, error) {
	if p.Connector.IsLive && p.Connector.Live != nil {
		return p.Connector.Live.GetMarket(id, slug)
	}
	return p.gammaFetch(id, slug)
}

// GetResolution checks if a market has resolved.
func (p *PolymarketConnector) GetResolution(marketID string) (*connector.Resolution, error) {
	if p.Connector.IsLive && p.Connector.Live != nil {
		return p.Connector.Live.GetResolution(marketID)
	}

	gm, err := p.gamma.FetchMarketByID(marketID)
	if err != nil {
		return nil, err
	}
	if gm.Resolution == nil {
		return nil, nil
	}
	res := connector.Resolution(*gm.Resolution)
	return &res, nil
}

// ─────────────────────────────────────────────────────────────
// CTF (Conditional Token Framework) operations
// ─────────────────────────────────────────────────────────────

// SplitPosition splits USDC into YES/NO outcome tokens for a market.
//
//	slug       — Market slug (e.g. "will-eth-reach-10k-by-2025")
//	amountUsdc — Amount of USDC to split (e.g. 10 = $10)
//	useRelayer — true for gasless relayer, false for direct on-chain
//
// The private key is read from Config.ClobSigningKeyHex.
// Direct on-chain requires p.cfg.CTFRPCURL to be set.
// Relayer mode requires p.cfg.RelayerAPIKey and p.cfg.RelayerAPIKeyAddr.
func (p *PolymarketConnector) SplitPosition(ctx context.Context, slug string, amountUsdc float64, useRelayer bool) (*SplitPositionResponse, error) {
	privateKey, err := p.ctfPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("polymarket: split: %w", err)
	}

	market, err := p.gamma.FetchMarketBySlug(slug)
	if err != nil {
		return nil, fmt.Errorf("polymarket: split: fetch market: %w", err)
	}

	conditionID := common.HexToHash(market.ConditionID)
	if conditionID == (common.Hash{}) {
		return nil, fmt.Errorf("polymarket: split: invalid condition ID: %s", market.ConditionID)
	}

	collateral := CollateralAddress(p.cfg.CTFChainID)
	amount := new(big.Int).SetInt64(int64(amountUsdc * 1_000_000))
	partition := StandardPartition()
	parentID := common.Hash{}

	req := &SplitPositionRequest{
		CollateralToken:    collateral,
		ParentCollectionID: parentID,
		ConditionID:        conditionID,
		Partition:          partition,
		Amount:             amount,
	}

	if useRelayer {
		if p.cfg.RelayerAPIKey == "" || p.cfg.RelayerAPIKeyAddr == "" {
			return nil, fmt.Errorf("polymarket: split: relayer credentials not configured (set RelayerAPIKey and RelayerAPIKeyAddr)")
		}
		relayer := NewRelayerClient(p.cfg.RelayerAPIKey, p.cfg.RelayerAPIKeyAddr)
		return relayer.SplitPositionViaRelayer(ctx, req, privateKey, market.NegRisk)
	}

	if p.cfg.CTFRPCURL == "" {
		return nil, fmt.Errorf("polymarket: split: RPC URL not configured (set CTFRPCURL)")
	}
	chainID := p.cfg.CTFChainID
	if chainID == 0 {
		chainID = PolygonCTFChainID
	}
	ctfClient, err := NewCTFClient(p.cfg.CTFRPCURL, chainID)
	if err != nil {
		return nil, fmt.Errorf("polymarket: split: create CTF client: %w", err)
	}
	defer ctfClient.Close()
	return ctfClient.SplitPosition(ctx, req, privateKey)
}

// MergePositions merges YES/NO outcome tokens back into USDC for a market.
//
//	slug       — Market slug (e.g. "will-eth-reach-10k-by-2025")
//	amountUsdc — Amount of each outcome token to merge (e.g. 5 = $5 of YES + $5 of NO)
//	useRelayer — true for gasless relayer, false for direct on-chain
//
// The private key is read from Config.ClobSigningKeyHex.
// Direct on-chain requires p.cfg.CTFRPCURL to be set.
// Relayer mode requires p.cfg.RelayerAPIKey and p.cfg.RelayerAPIKeyAddr.
func (p *PolymarketConnector) MergePositions(ctx context.Context, slug string, amountUsdc float64, useRelayer bool) (*MergePositionsResponse, error) {
	privateKey, err := p.ctfPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("polymarket: merge: %w", err)
	}

	market, err := p.gamma.FetchMarketBySlug(slug)
	if err != nil {
		return nil, fmt.Errorf("polymarket: merge: fetch market: %w", err)
	}

	conditionID := common.HexToHash(market.ConditionID)
	if conditionID == (common.Hash{}) {
		return nil, fmt.Errorf("polymarket: merge: invalid condition ID: %s", market.ConditionID)
	}

	collateral := CollateralAddress(p.cfg.CTFChainID)
	amount := new(big.Int).SetInt64(int64(amountUsdc * 1_000_000))
	partition := StandardPartition()
	parentID := common.Hash{}

	req := &MergePositionsRequest{
		CollateralToken:    collateral,
		ParentCollectionID: parentID,
		ConditionID:        conditionID,
		Partition:          partition,
		Amount:             amount,
	}

	if useRelayer {
		if p.cfg.RelayerAPIKey == "" || p.cfg.RelayerAPIKeyAddr == "" {
			return nil, fmt.Errorf("polymarket: merge: relayer credentials not configured (set RelayerAPIKey and RelayerAPIKeyAddr)")
		}
		relayer := NewRelayerClient(p.cfg.RelayerAPIKey, p.cfg.RelayerAPIKeyAddr)
		return relayer.MergePositionsViaRelayer(ctx, req, privateKey, market.NegRisk)
	}

	if p.cfg.CTFRPCURL == "" {
		return nil, fmt.Errorf("polymarket: merge: RPC URL not configured (set CTFRPCURL)")
	}
	chainID := p.cfg.CTFChainID
	if chainID == 0 {
		chainID = PolygonCTFChainID
	}
	ctfClient, err := NewCTFClient(p.cfg.CTFRPCURL, chainID)
	if err != nil {
		return nil, fmt.Errorf("polymarket: merge: create CTF client: %w", err)
	}
	defer ctfClient.Close()
	return ctfClient.MergePositions(ctx, req, privateKey)
}

// RedeemPositions redeems winning outcome tokens for a resolved market.
//
// Always redeems both YES and NO tokens (partition=[1,2]).
// Unlike split/merge, there is no amount — the full balance of each position
// token is redeemed.
//
//	slug       — Market slug (e.g. "will-eth-reach-10k-by-2025")
//	useRelayer — true for gasless relayer, false for direct on-chain
//
// The private key is read from Config.ClobSigningKeyHex.
// Direct on-chain requires p.cfg.CTFRPCURL to be set.
// Relayer mode requires p.cfg.RelayerAPIKey and p.cfg.RelayerAPIKeyAddr.
func (p *PolymarketConnector) RedeemPositions(ctx context.Context, slug string, useRelayer bool) (*RedeemPositionsResponse, error) {
	privateKey, err := p.ctfPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("polymarket: redeem: %w", err)
	}

	market, err := p.gamma.FetchMarketBySlug(slug)
	if err != nil {
		return nil, fmt.Errorf("polymarket: redeem: fetch market: %w", err)
	}

	conditionID := common.HexToHash(market.ConditionID)
	if conditionID == (common.Hash{}) {
		return nil, fmt.Errorf("polymarket: redeem: invalid condition ID: %s", market.ConditionID)
	}

	collateral := CollateralAddress(p.cfg.CTFChainID)
	parentID := common.Hash{}

	req := &RedeemPositionsRequest{
		CollateralToken:    collateral,
		ParentCollectionID: parentID,
		ConditionID:        conditionID,
		Partition:          StandardPartition(), // always redeem both YES and NO
	}

	if useRelayer {
		if p.cfg.RelayerAPIKey == "" || p.cfg.RelayerAPIKeyAddr == "" {
			return nil, fmt.Errorf("polymarket: redeem: relayer credentials not configured (set RelayerAPIKey and RelayerAPIKeyAddr)")
		}
		relayer := NewRelayerClient(p.cfg.RelayerAPIKey, p.cfg.RelayerAPIKeyAddr)
		return relayer.RedeemPositionsViaRelayer(ctx, req, privateKey, market.NegRisk)
	}

	if p.cfg.CTFRPCURL == "" {
		return nil, fmt.Errorf("polymarket: redeem: RPC URL not configured (set CTFRPCURL)")
	}
	chainID := p.cfg.CTFChainID
	if chainID == 0 {
		chainID = PolygonCTFChainID
	}
	ctfClient, err := NewCTFClient(p.cfg.CTFRPCURL, chainID)
	if err != nil {
		return nil, fmt.Errorf("polymarket: redeem: create CTF client: %w", err)
	}
	defer ctfClient.Close()
	return ctfClient.RedeemPositions(ctx, req, privateKey)
}

// ctfPrivateKey parses the CTF private key from ClobSigningKeyHex in Config.
func (p *PolymarketConnector) ctfPrivateKey() (*ecdsa.PrivateKey, error) {
	if p.cfg.ClobSigningKeyHex == "" {
		return nil, fmt.Errorf("ClobSigningKeyHex not set in Config")
	}
	hexStr := strings.TrimPrefix(p.cfg.ClobSigningKeyHex, "0x")
	keyBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("invalid ClobSigningKeyHex: %w", err)
	}
	key, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("invalid ClobSigningKeyHex: %w", err)
	}
	return key, nil
}

// ─────────────────────────────────────────────────────────────
// EIP-712 signing helpers
// ─────────────────────────────────────────────────────────────

// signSendOrder signs the Order inside a SendOrder using EIP-712.
// Uses the executor's signing key. Defaults to non-neg-risk and feeRateBps=0.
func (e *polymarketLiveExecutor) signSendOrder(so *SendOrder) error {
	if e.signingKey == nil {
		return fmt.Errorf("no signing key configured — set ClobSigningKeyHex in Config")
	}

	// Detect neg-risk from token ID or default to false.
	// For now, always non-neg-risk. Override per-market once we have market metadata.
	isNegRisk := false
	feeRateBps := int64(0)

	return SignAndSetOrder(&so.Order, e.signingKey, isNegRisk, feeRateBps)
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func (p *PolymarketConnector) gammaFetch(id, slug string) (*connector.Market, error) {
	var gm *GammaMarket
	var err error

	switch {
	case id != "":
		gm, err = p.gamma.FetchMarketByID(id)
	case slug != "":
		gm, err = p.gamma.FetchMarketBySlug(slug)
	default:
		return nil, fmt.Errorf("polymarket: must provide id or slug")
	}

	if err != nil {
		return nil, err
	}
	return p.gamma.ToConnectorMarket(gm), nil
}

// ─────────────────────────────────────────────────────────────
// Live executor — delegates to the CLOB API
// ─────────────────────────────────────────────────────────────

type polymarketLiveExecutor struct {
	gamma      *GammaClient
	clob       *ClobClient
	signingKey *ecdsa.PrivateKey
}

// PlaceLimitOrders submits a batch of limit orders to the CLOB API.
// Orders are batched in groups of 15 (CLOB max).
//
// Each order requires:
//  1. Convert connector.LimitOrder → CLOB Order (price/size to 6-dec fixed-math)
//  2. EIP-712 sign the Order struct   ← requires go-ethereum (see PROD.md Phase 3)
//  3. Wrap in SendOrder and POST /orders
func (e *polymarketLiveExecutor) PlaceLimitOrders(orders []connector.LimitOrder) (connector.OrderResult, error) {
	if e.clob == nil {
		return connector.OrderResult{}, fmt.Errorf("polymarket LIVE: clob client not initialized")
	}

	if len(orders) == 0 {
		return connector.OrderResult{Success: true}, nil
	}

	// Build SendOrder payloads and sign them.
	sendOrders := make([]SendOrder, 0, len(orders))
	for _, o := range orders {
		so, err := buildSendOrder(o, e.clob)
		if err != nil {
			return connector.OrderResult{}, fmt.Errorf("build order: %w", err)
		}

		// Sign the order.
		if err := e.signSendOrder(so); err != nil {
			return connector.OrderResult{}, fmt.Errorf("sign order for %s: %w", o.OrderID, err)
		}

		sendOrders = append(sendOrders, *so)
	}

	// Post each order individually via /order (V2 single-order endpoint).
	// Batch /orders may have different semantics in V2 — use /order for now.
	var allResults []connector.SingleOrderResult

	for _, so := range sendOrders {
		result, err := e.clob.PostOrder(so)
		if err != nil {
			allResults = append(allResults, connector.SingleOrderResult{
				Success:  false,
				ErrorMsg: err.Error(),
			})
			continue
		}
		allResults = append(allResults, connector.SingleOrderResult{
			OrderID:  result.OrderID,
			Success:  result.Success,
			ErrorMsg: result.ErrorMsg,
		})
	}

	// Overall success = at least one order succeeded.
	overallSuccess := false
	for _, r := range allResults {
		if r.Success {
			overallSuccess = true
			break
		}
	}

	return connector.OrderResult{
		Success: overallSuccess,
		Orders:  allResults,
	}, nil
}

// PlaceMarketOrder submits a FOK (Fill-Or-Kill) market order.
//
// Polymarket market orders use a limit-price FOK order:
//   - BUY:  fills only when price ≤ MarketOrder.Price (slippage protection)
//     MarketOrder.Size = dollar amount to spend (USDC)
//   - SELL: fills only when price ≥ MarketOrder.Price (slippage protection)
//     MarketOrder.Size = number of shares to sell
//
// The entire order fills atomically or cancels — no partial fills.
func (e *polymarketLiveExecutor) PlaceMarketOrder(order connector.MarketOrder) (connector.OrderResult, error) {
	if e.clob == nil {
		return connector.OrderResult{}, fmt.Errorf("polymarket LIVE: clob client not initialized")
	}

	so, err := buildMarketSendOrder(order, e.clob)
	if err != nil {
		return connector.OrderResult{}, fmt.Errorf("build market order: %w", err)
	}

	// Sign the order.
	if err := e.signSendOrder(so); err != nil {
		return connector.OrderResult{}, fmt.Errorf("sign market order: %w", err)
	}

	result, err := e.clob.PostOrder(*so)
	if err != nil {
		return connector.OrderResult{}, fmt.Errorf("post market order: %w", err)
	}

	return connector.OrderResult{
		Success: result.Success,
		Orders: []connector.SingleOrderResult{{
			OrderID:  result.OrderID,
			Success:  result.Success,
			ErrorMsg: result.ErrorMsg,
		}},
	}, nil
}

// CancelOrders cancels multiple orders by their order hashes.
// Uses DELETE /orders (max 1000 per batch).
func (e *polymarketLiveExecutor) CancelOrders(orderIDs []string) error {
	if e.clob == nil {
		return fmt.Errorf("polymarket LIVE: clob client not initialized")
	}

	if len(orderIDs) == 0 {
		return nil
	}

	// Batch in chunks of 1000.
	const batchSize = 1000
	var finalErr error

	for i := 0; i < len(orderIDs); i += batchSize {
		end := i + batchSize
		if end > len(orderIDs) {
			end = len(orderIDs)
		}
		batch := orderIDs[i:end]

		resp, err := e.clob.CancelOrders(batch)
		if err != nil {
			finalErr = err
			slog.Warn("clob: cancel batch failed", "batch_start", i, "err", err)
			continue
		}

		// Log any orders that were not cancelled.
		for id, reason := range resp.NotCanceled {
			slog.Warn("clob: order not cancelled", "order_id", id, "reason", reason)
		}
	}

	return finalErr
}

// buildSendOrder converts a connector.LimitOrder to a CLOB SendOrder.
// The resulting Order will have an empty Signature field — it must be filled
// via EIP-712 signing before submission (see PROD.md Phase 3).
func buildSendOrder(lo connector.LimitOrder, clob *ClobClient) (*SendOrder, error) {
	// Convert price and size to 6-decimal fixed-math strings.
	// Polymarket uses 6 decimals: amount = value * 1_000_000 as string.
	sizeRaw := int64(lo.Size * 1_000_000)
	priceRaw := int64(lo.Price * 1_000_000)

	// Amount semantics (matching the official Go SDK's buildLimit):
	//   BUY:  makerAmount = size * price (USDC spent), takerAmount = size (shares received)
	//   SELL: makerAmount = size (shares sold),       takerAmount = size * price (USDC received)
	var makerAmount, takerAmount string
	switch strings.ToUpper(lo.Side) {
	case "BUY":
		makerAmount = fmt.Sprintf("%d", sizeRaw*priceRaw/1_000_000)
		takerAmount = fmt.Sprintf("%d", sizeRaw)
	case "SELL":
		makerAmount = fmt.Sprintf("%d", sizeRaw)
		takerAmount = fmt.Sprintf("%d", sizeRaw*priceRaw/1_000_000)
	default:
		return nil, fmt.Errorf("invalid side: %s", lo.Side)
	}

	now := time.Now()
	timestampMs := fmt.Sprintf("%d", now.UnixMilli())

	var orderType string
	var expiration string

	if !lo.ExpiresAt.IsZero() {
		expiration = fmt.Sprintf("%d", lo.ExpiresAt.UnixMilli())
		orderType = "GTD"
	} else {
		expiration = "0"
		orderType = "GTC"
	}

	salt := fmt.Sprintf("%d", now.UnixNano()) // use nanosecond timestamp as salt for uniqueness

	order := Order{
		Maker:         clob.makerAddress,
		Signer:        clob.signerAddress,
		TokenID:       lo.AssetID,
		MakerAmount:   makerAmount,
		TakerAmount:   takerAmount,
		Side:          strings.ToUpper(lo.Side),
		Expiration:    expiration,
		Timestamp:     timestampMs,
		Metadata:      "0x0000000000000000000000000000000000000000000000000000000000000000",
		Builder:       "0x0000000000000000000000000000000000000000000000000000000000000000",
		Signature:     "",
		Salt:          salt,
		SignatureType: clob.signatureType,
	}

	so := &SendOrder{
		Order:     order,
		Owner:     clob.ownerUUID,
		OrderType: orderType,
	}

	return so, nil
}

// buildMarketSendOrder converts a connector.MarketOrder to a CLOB SendOrder
// with FOK (Fill-Or-Kill) semantics.
//
// Amount semantics (matching Polymarket SDK's createMarketOrder):
//   - BUY:  order.Size = dollar amount to spend (USDC)
//     makerAmount = USDC spent          = Size * 10^6
//     takerAmount = shares bought       = (Size / Price) * 10^6
//   - SELL: order.Size = number of shares to sell
//     makerAmount = shares sold         = Size * 10^6
//     takerAmount = USDC received       = Size * Price * 10^6
//
// The Price acts as slippage protection — the order only fills at equal or
// better price. FOK means the entire order fills atomically or cancels.
func buildMarketSendOrder(mo connector.MarketOrder, clob *ClobClient) (*SendOrder, error) {
	priceRaw := int64(mo.Price * 1_000_000)

	// Polymarket market-order precision constraints (6-dec fixed math):
	//   makerAmount: max 2 decimal places → truncate last 4 digits
	//   takerAmount: max 4 decimal places → truncate last 2 digits
	const makerTrunc = 10000
	const takerTrunc = 100

	var makerAmount, takerAmount string
	switch strings.ToUpper(mo.Side) {
	case "BUY":
		// mo.Size = dollars to spend
		usdcRaw := int64(mo.Size * 1_000_000)
		sharesRaw := usdcRaw * 1_000_000 / priceRaw // shares = usdc / price
		makerAmount = fmt.Sprintf("%d", (usdcRaw/makerTrunc)*makerTrunc)
		takerAmount = fmt.Sprintf("%d", (sharesRaw/takerTrunc)*takerTrunc)
	case "SELL":
		// mo.Size = number of shares to sell
		sharesRaw := int64(mo.Size * 1_000_000)
		usdcRaw := sharesRaw * priceRaw / 1_000_000 // usdc = shares * price
		makerAmount = fmt.Sprintf("%d", (sharesRaw/makerTrunc)*makerTrunc)
		takerAmount = fmt.Sprintf("%d", (usdcRaw/takerTrunc)*takerTrunc)
	default:
		return nil, fmt.Errorf("invalid side: %s", mo.Side)
	}

	now := time.Now()
	timestampMs := fmt.Sprintf("%d", now.UnixMilli())

	var orderType string
	var expiration string

	if !mo.ExpiresAt.IsZero() {
		expiration = fmt.Sprintf("%d", mo.ExpiresAt.UnixMilli())
		orderType = "GTD"
	} else {
		expiration = "0"
		orderType = "FOK" // Fill-Or-Kill — atomically fill or cancel
	}

	salt := fmt.Sprintf("%d", now.UnixNano())

	order := Order{
		Maker:         clob.makerAddress,
		Signer:        clob.signerAddress,
		TokenID:       mo.AssetID,
		MakerAmount:   makerAmount,
		TakerAmount:   takerAmount,
		Side:          strings.ToUpper(mo.Side),
		Expiration:    expiration,
		Timestamp:     timestampMs,
		Metadata:      "0x0000000000000000000000000000000000000000000000000000000000000000",
		Builder:       "0x0000000000000000000000000000000000000000000000000000000000000000",
		Signature:     "",
		Salt:          salt,
		SignatureType: clob.signatureType,
	}

	so := &SendOrder{
		Order:     order,
		Owner:     clob.ownerUUID,
		OrderType: orderType,
	}

	return so, nil
}

func (e *polymarketLiveExecutor) GetMarket(id, slug string) (*connector.Market, error) {
	if id != "" {
		gm, err := e.gamma.FetchMarketByID(id)
		if err != nil {
			return nil, err
		}
		return e.gamma.ToConnectorMarket(gm), nil
	}
	gm, err := e.gamma.FetchMarketBySlug(slug)
	if err != nil {
		return nil, err
	}
	return e.gamma.ToConnectorMarket(gm), nil
}

func (e *polymarketLiveExecutor) GetResolution(marketID string) (*connector.Resolution, error) {
	gm, err := e.gamma.FetchMarketByID(marketID)
	if err != nil {
		return nil, err
	}
	if gm.Resolution == nil {
		return nil, nil
	}
	res := connector.Resolution(*gm.Resolution)
	return &res, nil
}
