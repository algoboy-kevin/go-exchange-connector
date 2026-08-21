package connector

import "time"

// ─────────────────────────────────────────────────────────────
// Crypto price types
// ─────────────────────────────────────────────────────────────

// CryptoPriceRequest describes a request to Polymarket's crypto-price API
// for a crypto market's open/close price over a window.
//
// A request can specify the window explicitly (EventStartTime + EndDate), or
// reference a market (MarketID or Slug) from which the connector derives the
// window via the market's Gamma end date:
//
//	eventStartTime = endDate − variantWindow
//
// Symbol and Variant are always required; the caller supplies them (see the
// per-series crypto config in consuming code, e.g. SERIES_CRYPTO_CONFIG).
type CryptoPriceRequest struct {
	// MarketID or Slug identifies the Polymarket market whose end date is
	// used to derive the window. Either may be empty when EventStartTime and
	// EndDate are provided explicitly.
	MarketID string
	Slug     string

	// Symbol is the underlying crypto asset, e.g. "BTC", "ETH", "SOL", "XRP".
	Symbol string

	// Variant is the market interval that determines the window duration:
	// "fiveminute" (5m), "fifteen" (15m), "hourly" (1h), "daily" (24h).
	Variant string

	// EventStartTime is the window start. When zero, it is derived from
	// EndDate and Variant.
	EventStartTime time.Time

	// EndDate is the window end. When zero, it is derived from the market's
	// Gamma end date.
	EndDate time.Time

	// TWAPEnabled requests the time-weighted average price.
	TWAPEnabled bool

	// TWAPLookbackSeconds is the TWAP lookback window in seconds.
	TWAPLookbackSeconds int
}

// CryptoPrice is the response from Polymarket's crypto-price API for a single
// window.
type CryptoPrice struct {
	// OpenPrice is the price at the start of the window.
	OpenPrice float64 `json:"openPrice"`

	// ClosePrice is the price at the end of the window. Null until the window
	// is settled (the current/latest window is still forming), so it's a
	// pointer — consumers use the next window's open price as the previous
	// window's close price when this is nil.
	ClosePrice *float64 `json:"closePrice"`

	// Timestamp is the API response timestamp in ms since epoch.
	Timestamp int64 `json:"timestamp"`

	// Completed is true once the window has settled.
	Completed bool `json:"completed"`

	// Incomplete is true while the window is still forming.
	Incomplete bool `json:"incomplete"`

	// Cached indicates the result came from the API cache.
	Cached bool `json:"cached"`
}
