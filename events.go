package connector

import (
	"strings"
	"time"
)

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
// "btcusdt"), "chainlink" (symbols like "eth/usd"), or "chainlink_twap"
// (Chainlink-computed TWAP prices).
type CryptoPriceEvent struct {
	SeqID         int64     `json:"seq_id"`                   // monotonic sequence for ordered DES replay
	ReceivedAt    time.Time `json:"received_at"`              // local arrival timestamp
	Symbol        string    `json:"symbol"`                   // e.g. "btcusdt" or "eth/usd"
	Price         string    `json:"price"`                    // decimal string (RTDS "value")
	Timestamp     time.Time `json:"timestamp"`                // exchange timestamp
	Source        string    `json:"source"`                   // "binance", "chainlink", or "chainlink_twap"
	WindowSeconds int       `json:"window_seconds,omitempty"` // TWAP lookback window (chainlink_twap only)
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

// ── Binance stream events ──────────────────────────────────

// Binance market streams (pkg/binance) dispatch these typed events. Market is
// "spot" (spot) or "perp" (USDⓈ-M perpetual futures); Symbol carries the
// payload casing (e.g. "BTCUSDT").

// BinanceBookTickerEvent is a real-time best bid/ask update from Binance's
// bookTicker stream (spot + perpetual).
type BinanceBookTickerEvent struct {
	SeqID        int64     `json:"seq_id"`         // monotonic sequence for ordered DES replay
	ReceivedAt   time.Time `json:"received_at"`    // local arrival timestamp
	Symbol       string    `json:"symbol"`         // e.g. "BTCUSDT"
	Market       string    `json:"market"`         // "spot" or "perp"
	UpdateID     int64     `json:"update_id"`      // bookTicker update id
	BestBidPrice string    `json:"best_bid_price"` // decimal string
	BestBidQty   string    `json:"best_bid_qty"`   // decimal string
	BestAskPrice string    `json:"best_ask_price"` // decimal string
	BestAskQty   string    `json:"best_ask_qty"`   // decimal string
	Timestamp    time.Time `json:"timestamp"`      // exchange event time (zero if absent)
}

// BinanceAggTradeEvent is an aggregated trade from Binance's aggTrade stream
// (spot + perpetual).
type BinanceAggTradeEvent struct {
	SeqID        int64     `json:"seq_id"`         // monotonic sequence for ordered DES replay
	ReceivedAt   time.Time `json:"received_at"`    // local arrival timestamp
	Symbol       string    `json:"symbol"`         // e.g. "BTCUSDT"
	Market       string    `json:"market"`         // "spot" or "perp"
	TradeID      int64     `json:"trade_id"`       // aggregate trade id
	Price        string    `json:"price"`          // decimal string
	Quantity     string    `json:"quantity"`       // decimal string
	FirstTradeID int64     `json:"first_trade_id"` // first constituent trade id
	LastTradeID  int64     `json:"last_trade_id"`  // last constituent trade id
	IsBuyerMaker bool      `json:"is_buyer_maker"` // buyer was the maker
	Timestamp    time.Time `json:"timestamp"`      // exchange trade time
}

// BinanceDepthEvent is a full order book snapshot for one symbol, maintained
// locally from Binance's diff-depth stream (@depth@100ms) seeded by a REST
// snapshot. Bids are sorted descending (best first), asks ascending.
type BinanceDepthEvent struct {
	SeqID        int64     `json:"seq_id"`         // monotonic sequence for ordered DES replay
	ReceivedAt   time.Time `json:"received_at"`    // local arrival timestamp
	Symbol       string    `json:"symbol"`         // e.g. "BTCUSDT"
	Market       string    `json:"market"`         // "spot" or "perp"
	LastUpdateID int64     `json:"last_update_id"` // book's final update id
	Bids         []Level   `json:"bids"`           // best (highest price) first
	Asks         []Level   `json:"asks"`           // best (lowest price) first
	Timestamp    time.Time `json:"timestamp"`      // exchange event time (zero if absent)
}

// BinanceKlineEvent is an OHLCV candle from Binance's kline stream (spot +
// perpetual). Every kline update for the in-progress candle is dispatched
// (~1/s); IsFinal marks the last update, i.e. the closed candle.
type BinanceKlineEvent struct {
	SeqID       int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt  time.Time `json:"received_at"` // local arrival timestamp
	Symbol      string    `json:"symbol"`      // e.g. "BTCUSDT"
	Market      string    `json:"market"`      // "spot" or "perp"
	Interval    string    `json:"interval"`    // "1m", "5m", "15m", "1h", "4h", "1d"
	OpenTime    time.Time `json:"open_time"`   // kline open time (exchange)
	CloseTime   time.Time `json:"close_time"`  // kline close time (exchange)
	Open        string    `json:"open"`        // decimal string
	High        string    `json:"high"`        // decimal string
	Low         string    `json:"low"`         // decimal string
	Close       string    `json:"close"`       // decimal string
	Volume      string    `json:"volume"`      // base asset volume, decimal string
	QuoteVolume string    `json:"quote_volume"`
	TradeCount  int64     `json:"trade_count"` // number of trades in the kline
	IsFinal     bool      `json:"is_final"`    // Binance "x": candle closed
	Timestamp   time.Time `json:"timestamp"`   // exchange event time
}

// ── Hyperliquid stream events ──────────────────────────────

// Hyperliquid market data (pkg/hyperliquid) dispatches these typed events.
// Coin carries the venue's casing (e.g. "BTC", or "xyz:TSLA" for a HIP-3
// builder-deployed perp) — Hyperliquid coin names are case-sensitive.
//
// Every price, size and volume is a decimal string exactly as the venue sent
// it, never a float, so no precision is lost on the way through.

// HyperliquidLevel is one price level of a Hyperliquid order book. Count is
// the number of resting orders at that price (the venue's "n").
type HyperliquidLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
	Count int    `json:"count"`
}

// HyperliquidBookEvent is an L2 order-book snapshot from the l2Book channel.
// Unlike trades and bbo this channel is paced: the venue pushes a snapshot on
// every block that is at least ~0.5s after the previous one, whether or not
// anything changed. Bids and Asks are best-first, in the venue's order, and as
// deep as the subscription asked for (5 levels with fast, 20 otherwise).
type HyperliquidBookEvent struct {
	SeqID      int64              `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time          `json:"received_at"` // local arrival timestamp
	Coin       string             `json:"coin"`
	Bids       []HyperliquidLevel `json:"bids"`      // best (highest price) first
	Asks       []HyperliquidLevel `json:"asks"`      // best (lowest price) first
	Timestamp  time.Time          `json:"timestamp"` // exchange time (ms epoch on the wire)
}

// HyperliquidTradeEvent is one trade print from the trades channel. The venue
// sends prints in batches (an array per frame), so one frame can produce
// several events sharing a ReceivedAt.
//
// Side is the venue's raw encoding — "B" (buy/bid) or "A" (sell/ask); use
// HyperliquidNormalizeSide for a "buy"/"sell" form. TradeID is the venue's
// 50-bit hash of (buyer order id, seller order id): it is not globally unique,
// so a unique trade key is (Timestamp, Coin, TradeID).
type HyperliquidTradeEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	Coin       string    `json:"coin"`
	Side       string    `json:"side"` // "B" or "A"
	Price      string    `json:"price"`
	Size       string    `json:"size"`
	TradeID    int64     `json:"trade_id"`
	Hash       string    `json:"hash,omitempty"`  // L1 transaction hash
	Users      []string  `json:"users,omitempty"` // [buyer, seller] addresses
	Timestamp  time.Time `json:"timestamp"`       // exchange trade time
}

// HyperliquidBBOEvent is a best bid/offer update from the bbo channel. The
// venue sends one only on a block where the bbo changed, and either side may
// be absent (nil) when there is no resting order on that side.
type HyperliquidBBOEvent struct {
	SeqID      int64             `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time         `json:"received_at"` // local arrival timestamp
	Coin       string            `json:"coin"`
	Bid        *HyperliquidLevel `json:"bid,omitempty"`
	Ask        *HyperliquidLevel `json:"ask,omitempty"`
	Timestamp  time.Time         `json:"timestamp"` // exchange time
}

// HyperliquidAssetCtxEvent is a perp asset context update from the
// activeAssetCtx channel: funding, open interest, and the oracle/mark/mid
// prices, pushed once per block. IsSpot marks the spot variant of the channel,
// which carries none of the perp fields.
//
// This channel has no venue timestamp, so Timestamp mirrors ReceivedAt.
type HyperliquidAssetCtxEvent struct {
	SeqID        int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt   time.Time `json:"received_at"` // local arrival timestamp
	Coin         string    `json:"coin"`
	IsSpot       bool      `json:"is_spot,omitempty"`
	Funding      string    `json:"funding,omitempty"`       // hourly rate, decimal string
	OpenInterest string    `json:"open_interest,omitempty"` // base units
	Premium      string    `json:"premium,omitempty"`
	DayNtlVlm    string    `json:"day_ntl_vlm,omitempty"`
	DayBaseVlm   string    `json:"day_base_vlm,omitempty"`
	PrevDayPx    string    `json:"prev_day_px,omitempty"`
	OraclePx     string    `json:"oracle_px,omitempty"`
	MarkPx       string    `json:"mark_px,omitempty"`
	MidPx        string    `json:"mid_px,omitempty"`
	ImpactPxs    []string  `json:"impact_pxs,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}

// HyperliquidAllMidsEvent carries the mid price of every coin the venue has a
// mid for, from the allMids channel. There is no per-coin subscription: filter
// Mids locally.
//
// Like activeAssetCtx this channel has no venue timestamp, so Timestamp
// mirrors ReceivedAt.
type HyperliquidAllMidsEvent struct {
	SeqID      int64             `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time         `json:"received_at"` // local arrival timestamp
	Mids       map[string]string `json:"mids"`        // coin → mid price (decimal string)
	Timestamp  time.Time         `json:"timestamp"`
}

// HyperliquidCandleEvent is an OHLCV candle from the candle channel. The venue
// pushes an update on each trade inside the in-progress candle, and there is
// no "is final" flag: a changing OpenTime marks the start of the next candle
// (i.e. the previous one closed).
type HyperliquidCandleEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	Coin       string    `json:"coin"`
	Interval   string    `json:"interval"`   // "1m", "5m", "1h", …
	OpenTime   time.Time `json:"open_time"`  // candle open (exchange, ms epoch)
	CloseTime  time.Time `json:"close_time"` // candle close (exchange, ms epoch)
	Open       string    `json:"open"`
	High       string    `json:"high"`
	Low        string    `json:"low"`
	Close      string    `json:"close"`
	Volume     string    `json:"volume"` // base units
	TradeCount int64     `json:"trade_count"`
	Timestamp  time.Time `json:"timestamp"` // local receive time (the payload has none)
}

// HyperliquidErrorEvent is an {"channel":"error"} frame from the venue — in
// practice a refused subscription or a malformed request. It is dispatched as
// well as logged because a refused subscription is otherwise invisible: the
// socket stays healthy and simply never delivers data for that channel.
type HyperliquidErrorEvent struct {
	SeqID      int64     `json:"seq_id"`      // monotonic sequence for ordered DES replay
	ReceivedAt time.Time `json:"received_at"` // local arrival timestamp
	Message    string    `json:"message"`
}

// HyperliquidNormalizeSide maps the venue's trade side encoding to "buy" or
// "sell". The venue documents "B"/"A" but some payloads (and the fills stream)
// use "buy"/"sell"; both are accepted, case-insensitively.
func HyperliquidNormalizeSide(side string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "b", "bid", "buy":
		return "buy", true
	case "a", "ask", "sell":
		return "sell", true
	}
	return "", false
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
