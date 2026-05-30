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
	ID          string   `json:"id"`
	Slug        string   `json:"slug"`
	Question    string   `json:"question"`
	ConditionID string   `json:"condition_id"`
	YesAssetID  string   `json:"yes_asset_id"`
	NoAssetID   string   `json:"no_asset_id"`
	Outcomes    []string `json:"outcomes"`
	TickSize    float64  `json:"tick_size"`
	IsResolved  bool     `json:"is_resolved"`
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

// ─────────────────────────────────────────────────────────────
// Market data types
// ─────────────────────────────────────────────────────────────

// PriceLevel represents a single price level in an orderbook.
type PriceLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// PriceChange describes a single price level change event.
type PriceChange struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"` // "BUY" or "SELL"
	Hash    string `json:"hash,omitempty"`
}

// BookSnapshot is a full orderbook snapshot.
type BookSnapshot struct {
	MarketID  string       `json:"market_id"`
	AssetID   string       `json:"asset_id"`
	Timestamp time.Time    `json:"timestamp"`
	Bids      []PriceLevel `json:"bids"`
	Asks      []PriceLevel `json:"asks"`
}

// Trade is a trade/match event.
type Trade struct {
	AssetID   string    `json:"asset_id"`
	Side      string    `json:"side"` // "BUY" or "SELL"
	Price     string    `json:"price"`
	Size      string    `json:"size"`
	TradeID   string    `json:"trade_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Fill represents a partial or full order fill.
type Fill struct {
	TradeID   string    `json:"trade_id"`
	OrderID   string    `json:"order_id"`
	AssetID   string    `json:"asset_id"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Fee       float64   `json:"fee"`
	Timestamp time.Time `json:"timestamp"`
}

// MarketResolved is a market resolution event.
type MarketResolved struct {
	MarketID       string    `json:"market_id"`
	ConditionID    string    `json:"condition_id"`
	WinningAssetID string    `json:"winning_asset_id"`
	WinningOutcome string    `json:"winning_outcome"`
	Timestamp      time.Time `json:"timestamp"`
}

// TickChange represents a tick size change event.
type TickChange struct {
	AssetID     string `json:"asset_id"`
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
}
