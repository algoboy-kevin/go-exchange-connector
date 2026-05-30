// Package polymarket provides a connector for the Polymarket exchange
// (CLOB API). It handles WebSocket market data (price changes, orderbook
// snapshots, trades, market resolution), authenticated user channels for
// order/trade events, and the Gamma REST API for market metadata.
//
// Usage:
//
//	conn := polymarket.New(false, polymarket.Config{})
//	conn.OnBook = func(snap connector.BookSnapshot) { ... }
//	conn.OnTrade = func(trade connector.Trade) { ... }
//	conn.OnPlacement = func(o connector.LimitOrder) error { ... }
//	conn.Start(ctx)
//	defer conn.Stop()
//	conn.Subscribe([]string{"asset_id_1", "asset_id_2"})
package polymarket

// ─────────────────────────────────────────────────────────────
// Polymarket WebSocket event types
// ─────────────────────────────────────────────────────────────

// WSPriceLevel represents a single price level in a Polymarket orderbook.
type WSPriceLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// PriceChangeWSEvent is a price_change event from Polymarket.
type PriceChangeWSEvent struct {
	EventType    string            `json:"event_type"` // "price_change"
	Market       string            `json:"market"`
	Timestamp    string            `json:"timestamp"`
	PriceChanges []PriceChangeItem `json:"price_changes"`
}

// PriceChangeItem is a single entry in a price_change event.
type PriceChangeItem struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"` // "BUY" or "SELL"
	Hash    string `json:"hash"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

// BookWSEvent is a book snapshot/update event.
type BookWSEvent struct {
	EventType string         `json:"event_type"` // "book"
	Market    string         `json:"market"`
	AssetID   string         `json:"asset_id"`
	Timestamp string         `json:"timestamp"`
	Hash      string         `json:"hash"`
	Bids      []WSPriceLevel `json:"bids"`
	Asks      []WSPriceLevel `json:"asks"`
}

// LastTradePriceWSEvent is a last_trade_price event.
type LastTradePriceWSEvent struct {
	EventType       string `json:"event_type"` // "last_trade_price"
	AssetID         string `json:"asset_id"`
	FeeRateBps      string `json:"fee_rate_bps"`
	Market          string `json:"market"`
	Price           string `json:"price"`
	Side            string `json:"side"` // "BUY" or "SELL"
	Size            string `json:"size"`
	Timestamp       string `json:"timestamp"`
	TransactionHash string `json:"transaction_hash"`
}

// TickSizeChangeWSEvent is a tick_size_change event.
type TickSizeChangeWSEvent struct {
	EventType   string `json:"event_type"` // "tick_size_change"
	AssetID     string `json:"asset_id"`
	Market      string `json:"market"`
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
	Timestamp   string `json:"timestamp"`
}

// MarketResolvedWSEvent is a market_resolved event.
type MarketResolvedWSEvent struct {
	EventType      string `json:"event_type"` // "market_resolved"
	ID             string `json:"id"`
	Market         string `json:"market"`
	AssetsIDs      []string `json:"assets_ids"`
	WinningAssetID string `json:"winning_asset_id"`
	WinningOutcome string `json:"winning_outcome"`
	Timestamp      string `json:"timestamp"`
}

// ─────────────────────────────────────────────────────────────
// User channel types
// ─────────────────────────────────────────────────────────────

// UserAuth contains CLOB API credentials for authenticating the user WS.
type UserAuth struct {
	APIKey     string `json:"apiKey"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

// OrderWSEvent is an order placement, update, or cancellation event.
type OrderWSEvent struct {
	EventType    string `json:"event_type"` // "order"
	ID           string `json:"id"`
	Owner        string `json:"owner"`
	Market       string `json:"market"`
	AssetID      string `json:"asset_id"`
	Side         string `json:"side"` // "BUY" or "SELL"
	OriginalSize string `json:"original_size"`
	SizeMatched  string `json:"size_matched"`
	Price        string `json:"price"`
	Type         string `json:"type"` // "PLACEMENT", "UPDATE", "CANCELLATION"
	Timestamp    string `json:"timestamp"`
	Status       string `json:"status,omitempty"`
	OrderType    string `json:"order_type,omitempty"`
}

// TradeWSEvent is a trade match or status change event.
type TradeWSEvent struct {
	EventType       string `json:"event_type"` // "trade"
	Type            string `json:"type"`       // "TRADE"
	ID              string `json:"id"`
	TakerOrderID    string `json:"taker_order_id"`
	Market          string `json:"market"`
	AssetID         string `json:"asset_id"`
	Side            string `json:"side"` // "BUY" or "SELL"
	Size            string `json:"size"`
	Price           string `json:"price"`
	Status          string `json:"status"` // "MATCHED", "MINED", "CONFIRMED", etc.
	Owner           string `json:"owner"`
	Timestamp       string `json:"timestamp"`
	TransactionHash string `json:"transaction_hash,omitempty"`
}

// ─────────────────────────────────────────────────────────────
// Gamma API types
// ─────────────────────────────────────────────────────────────

// GammaMarket is the normalized representation of a Gamma API market response.
type GammaMarket struct {
	ID          string
	ConditionID string
	Slug        string
	Question    string
	Outcomes    []string
	YesTokenID  string
	NoTokenID   string
	TickSize    float64
	Resolution  *string // "YES", "NO", or nil if unresolved
}

// RawGammaMarket is the raw JSON shape from the Gamma API.
type RawGammaMarket struct {
	ID          string  `json:"id"`
	ConditionID string  `json:"conditionId"`
	Slug        string  `json:"slug"`
	Question    string  `json:"question"`
	Outcomes    string  `json:"outcomes"`      // JSON string: ["Up", "Down"]
	OutcomePrices string `json:"outcomePrices"` // JSON string: ["0.505", "0.495"]
	ClobTokenIDs string  `json:"clobTokenIds"` // JSON string: ["<yesId>", "<noId>"]
	Closed      bool    `json:"closed"`
	TickSize    float64 `json:"orderPriceMinTickSize"`
}

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────

// Config holds Polymarket connection settings.
type Config struct {
	APIKey       string `yaml:"api_key"`
	Secret       string `yaml:"api_secret"`
	Passphrase   string `yaml:"api_passphrase"`
	GammaAPIURL  string `yaml:"gamma_api_url,omitempty"`
	MarketWSURL  string `yaml:"market_ws_url,omitempty"`
	UserWSURL    string `yaml:"user_ws_url,omitempty"`
}
