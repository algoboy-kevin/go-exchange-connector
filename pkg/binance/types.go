package binance

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MarketType identifies which Binance market a subscription targets. The two
// supported markets share stream names but use different WebSocket hosts and
// different REST hosts for order-book snapshots.
type MarketType string

const (
	// MarketSpot is the Binance spot market (wss://stream.binance.com:9443).
	MarketSpot MarketType = "spot"
	// MarketPerp is the Binance USDⓈ-M perpetual futures market
	// (wss://fstream.binance.com). Futures market streams are split across
	// two endpoints — /market/ws (aggTrade, kline) and /public/ws
	// (bookTicker, depth) — each on its own connection (see streamClass in
	// binance.go). The legacy /ws + /stream endpoints ack @aggTrade
	// subscriptions but deliver nothing.
	MarketPerp MarketType = "perp"
)

// valid reports whether m is a known market type.
func (m MarketType) valid() bool {
	return m == MarketSpot || m == MarketPerp
}

// restURL returns the REST base URL used for order-book snapshots.
func (m MarketType) restURL() string {
	if m == MarketPerp {
		return perpRESTURL
	}
	return spotRESTURL
}

// streamFor builds a full Binance stream name for a normalized (lowercase)
// symbol and stream type, e.g. "btcusdt@bookTicker".
func streamFor(symbol string, st streamType) string {
	switch st {
	case streamBookTicker:
		return symbol + "@bookTicker"
	case streamAggTrade:
		return symbol + "@aggTrade"
	case streamDepth:
		return symbol + "@depth@100ms"
	case streamKline:
		return symbol + "@kline_1m"
	}
	return ""
}

// klineStream builds a kline stream for an explicit interval, e.g.
// "btcusdt@kline_15m".
func klineStream(symbol, interval string) string {
	return symbol + "@kline_" + interval
}

// partialDepthStream builds a partial book depth stream name, e.g.
// "btcusdt@depth20@100ms". levels is one of Binance's supported partial
// depth levels (5, 10 or 20); speed is "100ms", "500ms" or "1000ms".
func partialDepthStream(symbol string, levels int, speed string) string {
	return fmt.Sprintf("%s@depth%d@%s", symbol, levels, speed)
}

// streamType is the kind of Binance data stream.
type streamType string

const (
	streamBookTicker streamType = "bookTicker"
	streamAggTrade   streamType = "aggTrade"
	streamDepth      streamType = "depth" // diff depth @100ms (book maintained locally)
	streamKline      streamType = "kline"
)

// ─────────────────────────────────────────────────────────────
// Internal WS frame types
// ─────────────────────────────────────────────────────────────

const (
	binanceMethodSubscribe   = "SUBSCRIBE"
	binanceMethodUnsubscribe = "UNSUBSCRIBE"
)

// binanceSubRequest is the message-based subscribe/unsubscribe frame sent to
// the raw /ws endpoint.
type binanceSubRequest struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int64    `json:"id"`
}

// binanceSubAck is the acknowledgement reply for a subscribe/unsubscribe
// frame (result null on success, or an error object).
type binanceSubAck struct {
	ID     int64            `json:"id"`
	Result json.RawMessage  `json:"result,omitempty"`
	Error  *binanceAPIError `json:"error,omitempty"`
}

type binanceAPIError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e *binanceAPIError) Error() string {
	return fmt.Sprintf("binance api error %d: %s", e.Code, e.Msg)
}

// binanceLevel is one price/qty level in a depth payload ("bids"/"asks" are
// arrays of [price, qty] pairs).
type binanceLevel struct {
	Price string
	Qty   string
}

// parseBinanceLevels decodes a depth level array ([["price","qty"],...]) and
// drops empty levels.
func parseBinanceLevels(raw json.RawMessage) []binanceLevel {
	var pairs [][]string
	if err := json.Unmarshal(raw, &pairs); err != nil {
		return nil
	}
	levels := make([]binanceLevel, 0, len(pairs))
	for _, p := range pairs {
		if len(p) < 2 {
			continue
		}
		levels = append(levels, binanceLevel{Price: p[0], Qty: p[1]})
	}
	return levels
}

// normalizeSymbol lowercases and trims a Binance symbol, e.g. "BTCUSDT" →
// "btcusdt" (stream names must be lowercase).
func normalizeSymbol(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
