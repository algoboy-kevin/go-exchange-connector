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
	OrderID  string  `json:"order_id"`
	AssetID  string  `json:"asset_id"`
	MarketID string  `json:"market_id"`
	Side     string  `json:"side"` // "BUY" or "SELL"
	Price    float64 `json:"price"`
	Size     float64 `json:"size"`
}

//
type MarketOrder struct {
	OrderID  string  `json:"order_id"`
	AssetID  string  `json:"asset_id"`
	MarketID string  `json:"market_id"`
	Side     string  `json:"side"` // "BUY" or "SELL"
	Price    float64 `json:"price"` // slippage protection
	Size     float64 `json:"size"`
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

// ── Events (PriceChangeEvent, BookSnapshotEvent, TradeEvent, etc.)
// are in events.go — use those for actor dispatch.
