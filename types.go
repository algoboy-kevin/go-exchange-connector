// Package connector provides a generic exchange connector framework for
// prediction-market trading systems. It defines the ExchangeConnector
// interface, a reusable Connector base with built-in paper-mode simulation,
// and utilities like BaseWebSocket for WebSocket lifecycle management.
//
// Architecture:
//
//	ExchangeConnector (interface)
//	      │
//	      ├── Connector (base struct + paper simulation)
//	      │       │
//	      │       ├── PolymarketConnector (exchange-specific, private repo)
//	      │       ├── BinanceConnector   (exchange-specific, private repo)
//	      │       └── ...
//	      │
//	      └── MockConnector (testing, built-in)
//
// The Connector struct acts as both an event emitter (market data flows
// through handler fields) and an execution router (PlaceLimitOrders delegates
// to paper simulation or live executor based on isLive).
package connector

import "time"

// ─────────────────────────────────────────────────────────────
// Market types
// ─────────────────────────────────────────────────────────────

// Market represents an exchange-agnostic prediction market.
type Market struct {
	ID          string      `json:"id"`
	Slug        string      `json:"slug"`
	Question    string      `json:"question"`
	ConditionID string      `json:"condition_id"`
	YesAssetID  string      `json:"yes_asset_id"`
	NoAssetID   string      `json:"no_asset_id"`
	Outcomes    []string    `json:"outcomes"`
	TickSize    float64     `json:"tick_size"`
	IsResolved  bool        `json:"is_resolved"`
	Resolution  *Resolution `json:"resolution,omitempty"`

	// ── Optional Gamma metadata (populated for Polymarket markets) ──
	//
	// These describe where a market sits inside its event. Ladder events
	// (e.g. "BTC above $74,000 … $94,000") hold one market per strike; the
	// strike label is GroupItemTitle and EventSlug groups the rungs.

	// GroupItemTitle is the strike/range label, e.g. "80,000".
	GroupItemTitle string `json:"group_item_title,omitempty"`
	// NegRisk is true when the market uses the neg-risk collateral adapter.
	NegRisk bool `json:"neg_risk,omitempty"`
	// NegRiskMarketID is the neg-risk group id (empty for non-neg-risk markets).
	NegRiskMarketID string `json:"neg_risk_market_id,omitempty"`
	// EventSlug / EventTicker identify the enclosing event.
	EventSlug   string `json:"event_slug,omitempty"`
	EventTicker string `json:"event_ticker,omitempty"`
	// Description carries the machine-readable resolution rule
	// (settlement anchor + source) when the exchange publishes one.
	Description string `json:"description,omitempty"`
	// StartDate/EndDate are the trading window open / close. EndDate is the
	// settle instant for the recurring crypto families. Zero when unknown.
	StartDate time.Time `json:"start_date,omitempty"`
	EndDate   time.Time `json:"end_date,omitempty"`
}

// Resolution is the final outcome of a prediction market.
type Resolution string

const (
	ResYes       Resolution = "YES"
	ResNo        Resolution = "NO"
	ResCancelled Resolution = "CANCELLED"
)

// ─────────────────────────────────────────────────────────────
// Order types
// ─────────────────────────────────────────────────────────────

// LimitOrder is an exchange-agnostic limit order request.
type LimitOrder struct {
	OrderID   string    `json:"order_id"`
	AssetID   string    `json:"asset_id"`
	MarketID  string    `json:"market_id"`
	Side      string    `json:"side"` // "BUY" or "SELL"
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	ExpiresAt time.Time `json:"expires_at,omitempty"` // zero = GTC, non-zero = GTD
}

// MarketOrder describes a FOK (Fill-Or-Kill) market order request.
//
// Amount semantics (matching Polymarket's createMarketOrder):
//   - BUY:  Size = dollar amount to spend (USDC)
//     The order fills at price ≤ Price (slippage protection)
//   - SELL: Size = number of shares to sell
//     The order fills at price ≥ Price (slippage protection)
//
// The entire order fills atomically or cancels entirely — no partial fills.
type MarketOrder struct {
	OrderID   string    `json:"order_id"`
	AssetID   string    `json:"asset_id"`
	MarketID  string    `json:"market_id"`
	Side      string    `json:"side"`                 // "BUY" or "SELL"
	Price     float64   `json:"price"`                // worst-price limit (slippage protection)
	Size      float64   `json:"size"`                 // BUY=USDC amount, SELL=shares amount
	ExpiresAt time.Time `json:"expires_at,omitempty"` // zero = no expiry (FOK), non-zero = GTD
}

// OrderResult describes the outcome of a batch order operation.
type OrderResult struct {
	Success  bool                `json:"success"`
	Orders   []SingleOrderResult `json:"orders,omitempty"`
	ErrorMsg string              `json:"error_msg,omitempty"`
}

// SingleOrderResult describes one order's outcome in a batch.
type SingleOrderResult struct {
	OrderID  string `json:"order_id"`
	Success  bool   `json:"success"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

// CancelOrder describes a cancellation request (used with CancelOrders).
// When only OrderID is set, all matches for that order are cancelled.
type CancelOrder struct {
	OrderID string `json:"order_id"`
	AssetID string `json:"asset_id,omitempty"`
}

// CancelOrdersResult describes the outcome of a batch cancel request,
// matching the Polymarket DELETE /orders response format.
type CancelOrdersResult struct {
	Canceled    []string          // successfully canceled order IDs
	NotCanceled map[string]string // orderID → reason (e.g. "Order already matched")
}

// ── Events (PriceChangeEvent, BookSnapshotEvent, TradeEvent, etc.)
// are in events.go — use those for actor dispatch.
