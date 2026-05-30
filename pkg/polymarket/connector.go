package polymarket

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
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

	pc := &PolymarketConnector{
		Connector: connector.New(isLive, &polymarketLiveExecutor{
			gamma: gamma,
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
	// ── Market WS (all modes) ───────────────────────────────
	wsURL := p.cfg.MarketWSURL
	if wsURL == "" {
		wsURL = marketWSSURL
	}

	p.market = NewWSPolymarketMarket(p.Connector)
	if err := p.market.Start(ctx, wsURL); err != nil {
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
	slog.Info("polymarket: connector stopped")
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
// Live executor (stub — real API calls TBD)
// ─────────────────────────────────────────────────────────────

type polymarketLiveExecutor struct {
	gamma *GammaClient
}

func (e *polymarketLiveExecutor) PlaceLimitOrders(orders []connector.LimitOrder) (connector.OrderResult, error) {
	return connector.OrderResult{}, fmt.Errorf("polymarket LIVE PlaceLimitOrders not yet implemented")
}

func (e *polymarketLiveExecutor) CancelOrders(orderIDs []string) error {
	return fmt.Errorf("polymarket LIVE CancelOrders not yet implemented")
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
