// Package futures is the Aster perpetuals (fapi.asterdex.com) signed REST client
// and user-data stream.
//
// Aster serves the legacy Binance USDⓈ-M futures layout, so this package does
// not fork pkg/binance/futures: it configures that client for the Aster hosts
// and routes and re-exports its types. The differences, all from the Aster API
// doc recorded in exchange-docs-hub (catalog `aster`, markdown source
// docs.asterdex.com/for-developers/aster-api/api-documentation.md):
//
//   - hosts: REST https://fapi.asterdex.com, WS wss://fstream.asterdex.com;
//   - positions: GET /fapi/v2/positionRisk (Binance uses v3);
//   - user stream: wss://fstream.asterdex.com/ws/<listenKey>, no events filter;
//   - no testnet is documented, so there is no Network option;
//   - signing, order/batch/cancel routes, batch caps (place 5, cancel 10),
//     countdownCancelAll (0 stops the timer), listenKey lifecycle and the
//     ORDER_TRADE_UPDATE / ACCOUNT_UPDATE payloads match Binance.
//
// Limits from the doc: a WS connection lives 24 h, the server pings every 5 min
// (no pong in 15 min closes it), at most 10 inbound msg/s and 200 streams per
// connection, 429 means back off and repeated 429s lead to a 418 IP ban.
// Spot (sapi.asterdex.com, v3 wallet signing) is not implemented here.
package futures

import (
	"net/http"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance"
	bn "github.com/algoboy-kevin/go-exchange-connector/pkg/binance/futures"
)

const (
	// BaseURL is the production REST host.
	BaseURL = "https://fapi.asterdex.com"
	// WSBaseURL is the production WebSocket host.
	WSBaseURL = "wss://fstream.asterdex.com"
	// PositionRiskPath is the positions route Aster documents.
	PositionRiskPath = "/fapi/v2/positionRisk"
)

// Types are shared with the Binance futures client; the payloads are identical.
type (
	Client            = bn.Client
	OrderReq          = bn.OrderReq
	Order             = bn.Order
	BatchItem         = bn.BatchItem
	ExchangeInfo      = bn.ExchangeInfo
	PositionRisk      = bn.PositionRisk
	APIError          = bn.APIError
	ConfigError       = bn.ConfigError
	UserStream        = bn.UserStream
	UserStreamStats   = bn.UserStreamStats
	OrderTradeUpdate  = bn.OrderTradeUpdate
	OrderUpdate       = bn.OrderUpdate
	AccountUpdate     = bn.AccountUpdate
	AccountUpdateData = bn.AccountUpdateData
)

// Error classifiers and sentinels, re-exported from the shared client.
var (
	IsUncertain      = bn.IsUncertain
	IsRateLimited    = bn.IsRateLimited
	IsUnknownOrder   = bn.IsUnknownOrder
	ErrTooManyOrders = bn.ErrTooManyOrders
)

// Config configures a Client. BaseURL defaults to the production host; tests
// point it at an httptest server. RecvWindow defaults to 5 s and HTTP to a
// client with a 10 s timeout. APIKey/APISecret may be empty for public routes.
type Config struct {
	BaseURL    string
	APIKey     string
	APISecret  string
	RecvWindow time.Duration
	HTTP       *http.Client
}

// New builds an Aster futures client.
func New(cfg Config) (*Client, error) {
	base := cfg.BaseURL
	if base == "" {
		base = BaseURL
	}
	return bn.New(bn.Config{
		BaseURL:          base,
		APIKey:           cfg.APIKey,
		APISecret:        cfg.APISecret,
		RecvWindow:       cfg.RecvWindow,
		HTTP:             cfg.HTTP,
		PositionRiskPath: PositionRiskPath,
	})
}

// UserStreamConfig configures a user-data stream. WSURL defaults to the
// production host; KeepAliveInterval defaults to 30 min (a listenKey lives 60).
type UserStreamConfig struct {
	Client              *Client
	WSURL               string
	KeepAliveInterval   time.Duration
	ReconnectIntervalMs int64
}

// NewUserStream builds a user-data stream dialing <WSURL>/ws/<listenKey>.
func NewUserStream(cfg UserStreamConfig) (*UserStream, error) {
	ws := cfg.WSURL
	if ws == "" {
		ws = WSBaseURL
	}
	return bn.NewUserStream(bn.UserStreamConfig{
		Client:              cfg.Client,
		WSURL:               ws,
		KeyInPath:           true,
		KeepAliveInterval:   cfg.KeepAliveInterval,
		ReconnectIntervalMs: cfg.ReconnectIntervalMs,
	})
}

// MarketOptions points the Binance market-data manager (binance.NewWithOptions)
// at Aster's perpetuals streams: one raw endpoint, wss://fstream.asterdex.com/ws,
// carries aggTrade, kline, markPrice, bookTicker and depth; REST depth
// snapshots come from fapi.asterdex.com. Use binance.MarketPerp. Stream names
// must be lowercase and a connection holds at most 200 streams.
func MarketOptions() binance.Options {
	return binance.Options{
		PerpMarketWS: WSBaseURL + "/ws",
		PerpPublicWS: WSBaseURL + "/ws",
		PerpREST:     BaseURL,
	}
}
