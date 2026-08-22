package connector

import "time"

// ── Market events (connector → MarketManager) ───────────────

// PriceChangeEvent carries one or more price level changes from the exchange.
type PriceChangeEvent struct {
	SeqID      int64             `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time         `json:"received_at"` // local arrival timestamp
	Market     string            `json:"market"`
	Timestamp  time.Time         `json:"timestamp"`
	Changes    []PriceChangeItem `json:"changes"`
}

type PriceChangeItem struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	Hash    string `json:"hash,omitempty"`
	BestBid string `json:"best_bid,omitempty"`
	BestAsk string `json:"best_ask,omitempty"`
}

// BookSnapshotEvent is a full orderbook snapshot.
type BookSnapshotEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	Market     string    `json:"market"`
	AssetID    string    `json:"asset_id"`
	Timestamp  time.Time `json:"timestamp"`
	Hash       string    `json:"hash,omitempty"`
	Bids       []Level   `json:"bids"`
	Asks       []Level   `json:"asks"`
}

type Level struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// ── User order events (connector → OrderManager) ───────────────

// TradeEvent is a trade/match event from the exchange.
type TradeEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	AssetID    string    `json:"asset_id"`
	TradeID    string    `json:"trade_id,omitempty"`
	Side       string    `json:"side"`
	Size       string    `json:"size"`
	Price      string    `json:"price"`
	Status     string    `json:"status,omitempty"`
	TxHash     string    `json:"tx_hash,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// TickChangeEvent notifies of a tick size change.
type TickChangeEvent struct {
	SeqID       int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt  time.Time `json:"received_at"` // local arrival timestamp
	AssetID     string    `json:"asset_id"`
	Market      string    `json:"market,omitempty"`
	OldTickSize string    `json:"old_tick_size"`
	NewTickSize string    `json:"new_tick_size"`
	Timestamp   time.Time `json:"timestamp"`
}

// ── RTDS (Real-Time Data Service) events ────────────────────

// CryptoPriceEvent is a real-time crypto asset price update from Polymarket's
// RTDS stream. Source distinguishes the feed: "binance" (symbols like
// "btcusdt") or "chainlink" (symbols like "eth/usd").
type CryptoPriceEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	Symbol     string    `json:"symbol"`      // e.g. "btcusdt" or "eth/usd"
	Price      string    `json:"price"`       // decimal string (RTDS "value")
	Timestamp  time.Time `json:"timestamp"`   // exchange timestamp
	Source     string    `json:"source"`      // "binance" or "chainlink"
}

// EquityPriceEvent is a real-time equity/ETF/forex/commodity reference price
// update from the RTDS equity (Pyth) stream, e.g. symbol "aapl".
type EquityPriceEvent struct {
	SeqID            int64     `json:"seq_id"`                       // monotonic sequence for ordered DES replay
	ReceivedAt       time.Time `json:"received_at"`                  // local arrival timestamp
	Symbol           string    `json:"symbol"`                       // e.g. "aapl"
	Price            string    `json:"price"`                        // decimal string (full_accuracy_value preferred)
	Timestamp        time.Time `json:"timestamp"`                    // exchange timestamp
	IsCarriedForward bool      `json:"is_carried_forward,omitempty"` // true when market closed / value carried forward
}

// PriceSnapshotPoint is a single historical price point in a subscribe
// snapshot (used to seed local state before live updates).
type PriceSnapshotPoint struct {
	Timestamp int64   `json:"timestamp"` // ms since epoch
	Value     float64 `json:"value"`
}

// PriceSnapshotEvent carries the historical snapshot that the RTDS stream
// sends immediately after subscribing to a chainlink or equity symbol (the
// preceding ~2 minutes of price data).
type PriceSnapshotEvent struct {
	SeqID      int64                `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time            `json:"received_at"` // local arrival timestamp
	Source     string               `json:"source"`      // "chainlink" or "equity"
	Symbol     string               `json:"symbol"`
	Points     []PriceSnapshotPoint `json:"points"`
	Timestamp  time.Time            `json:"timestamp"` // stream timestamp
}

// MarketResolvedEvent notifies that a prediction market has been resolved.
type MarketResolvedEvent struct {
	SeqID          int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt     time.Time `json:"received_at"` // local arrival timestamp
	MarketID       string    `json:"market_id"`
	ConditionID    string    `json:"condition_id,omitempty"`
	WinningAssetID string    `json:"winning_asset_id"`
	WinningOutcome string    `json:"winning_outcome"`
	Timestamp      time.Time `json:"timestamp"`
}

// OrderReservationEvent is sent to create a pending order before placement.
type OrderReservationEvent struct {
	ClientID  string    `json:"client_id"`
	MarketID  string    `json:"market_id"`
	AssetID   string    `json:"asset_id"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderPlacementEvent is sent when the exchange confirms order placement.
type OrderPlacementEvent struct {
	BrokerID  string    `json:"broker_id"`
	MarketID  string    `json:"market_id"` // condition ID for Polymarket
	AssetID   string    `json:"asset_id"`
	Side      string    `json:"side"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Timestamp time.Time `json:"timestamp"`
}

// TradeStatus represents the on-chain settlement status of a trade.
type TradeStatus string

const (
	TradeMatched   TradeStatus = "MATCHED"   // Matched, sent to executor for on-chain submission
	TradeMined     TradeStatus = "MINED"     // Transaction mined into the blockchain
	TradeConfirmed TradeStatus = "CONFIRMED" // Trade achieved finality, successful
	TradeRetrying  TradeStatus = "RETRYING"  // Transaction failed, being retried
	TradeFailed    TradeStatus = "FAILED"    // Trade failed permanently
)

// OrderFillEvent is sent when an order is partially or fully filled.
type OrderFillEvent struct {
	TradeID   string      `json:"trade_id"`
	BrokerID  string      `json:"broker_id"`
	AssetID   string      `json:"asset_id"`
	Side      string      `json:"side"`
	Price     float64     `json:"price"`
	Size      float64     `json:"size"`
	FeePaid   float64     `json:"fee_paid"`
	Rebates   float64     `json:"rebates"`
	IsMaker   bool        `json:"is_maker"`
	FillZone  string      `json:"fill_zone,omitempty"`
	Mechanism string      `json:"mechanism,omitempty"`
	Status    TradeStatus `json:"status,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// OrderCancelEvent is sent when the exchange confirms cancellation.
type OrderCancelEvent struct {
	BrokerID  string    `json:"broker_id"`
	AssetID   string    `json:"asset_id"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderMergeEvent is sent when YES+NO tokens are merged into USDC.
type OrderMergeEvent struct {
	Success         bool    `json:"success"`
	MarketID        string  `json:"market_id"`
	Amount          float64 `json:"amount"`
	TransactionHash string  `json:"transaction_hash,omitempty"`
}

// OrderSplitEvent is sent when USDC is split into YES+NO tokens.
type OrderSplitEvent struct {
	Success         bool    `json:"success"`
	MarketID        string  `json:"market_id"`
	Amount          float64 `json:"amount"`
	TransactionHash string  `json:"transaction_hash,omitempty"`
}

// OrderRedeemEvent is sent when a resolved position is redeemed.
type OrderRedeemEvent struct {
	Success         bool      `json:"success"`
	AssetID         string    `json:"asset_id"`
	IsWin           bool      `json:"is_win"`
	Amount          float64   `json:"amount"`
	CashReceived    float64   `json:"cash_received"`
	TransactionHash string    `json:"transaction_hash,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
}
