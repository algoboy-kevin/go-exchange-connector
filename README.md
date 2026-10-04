# go-exchange-connector

A generic exchange connector framework for prediction-market trading systems. Provides the interface, base implementation with paper-mode simulation, WebSocket utilities, and a mock connector for testing.

## Package Layout

```
go-exchange-connector/
├── connector.go        # ExchangeConnector interface + Connector base struct
├── types.go            # Domain types (Market, LimitOrder, BookSnapshot, etc.)
├── errors.go           # Sentinel errors + domain error types
├── mock.go             # MockConnector — accept-all no-op for testing
├── go.mod
├── README.md
└── pkg/
    ├── binance/
    │   ├── binance.go  # WSBinance — spot + perpetual market data streams
    │   └── types.go    # MarketType, stream names, WS frame types
    ├── hyperliquid/
    │   ├── hyperliquid.go  # WSHyperliquid — perp market data streams
    │   ├── channels.go     # channel registry, validation, subscription frames
    │   ├── types.go        # WS payloads + REST /info types
    │   └── info.go         # InfoClient — /info metadata & snapshots
    └── websocket/
        ├── types.go    # ConnectionStatus, WSOptions, WSocketError, PanicError
        └── websocket.go# BaseWebSocket — reconnect, ping/pong, status tracking
```

## Quick Start

### PAPER mode (simulated execution)

```go
conn := connector.New(false, nil) // isLive=false

// Mount order lifecycle handlers → bridge to your internal system.
conn.OnReservation = func(o connector.LimitOrder) error {
    fmt.Println("reserving:", o.OrderID)
    return nil
}
conn.OnPlacement = func(o connector.LimitOrder) error {
    fmt.Println("placing:", o.OrderID)
    return nil
}
conn.OnCancel = func(orderID string) error {
    fmt.Println("cancelling:", orderID)
    return nil
}

// Mount market data handlers → called by exchange WS code.
conn.OnBook = func(snap connector.BookSnapshot) {
    fmt.Printf("book: %s bids=%d asks=%d\n", snap.MarketID, len(snap.Bids), len(snap.Asks))
}

// When the strategy calls PlaceLimitOrders:
result, err := conn.PlaceLimitOrders(orders)
// Internally calls OnReservation → OnPlacement for each order.
```

### LIVE mode (real exchange API)

```go
exec := &myexchange.LiveExecutor{Client: httpClient}
conn := connector.New(true, exec)

// Mount market data handlers (same as paper mode).
conn.OnBook = func(snap connector.BookSnapshot) { ... }
conn.OnTrade = func(trade connector.Trade) { ... }

// When the strategy calls PlaceLimitOrders:
result, err := conn.PlaceLimitOrders(orders)
// Delegates to exec.PlaceLimitOrders(orders) — the real API call.
```

### Testing with MockConnector

```go
mock := connector.NewMockConnector()
mock.OnEvent = func(ev any) {
    fmt.Printf("event: %T %+v\n", ev, ev)
}

// Use mock anywhere ExchangeConnector is expected:
engine := NewTradingEngine(mock)
```

### RTDS real-time reference prices (Polymarket)

Stream real-time reference prices via the RTDS WebSocket. Supported sources
dispatch as `connector.CryptoPriceEvent` (Binance, Chainlink spot, and Chainlink
TWAP), `connector.EquityPriceEvent` (Pyth equity/forex/commodities), and
`connector.PriceSnapshotEvent` (historical ~2-min snapshot on subscribe, spot
feeds only). Symbol matching is case-insensitive.

```go
conn := polymarket.New(false, polymarket.Config{})
conn.SetDispatcher(func(ev any) {
    switch e := ev.(type) {
    case *connector.CryptoPriceEvent:
        fmt.Printf("%s(%s) = %s win=%ds\n", e.Symbol, e.Source, e.Price, e.WindowSeconds)
    case *connector.EquityPriceEvent:
        fmt.Printf("equity %s = %s\n", e.Symbol, e.Price)
    case *connector.PriceSnapshotEvent:
        fmt.Printf("%s snapshot %s: %d points\n", e.Source, e.Symbol, len(e.Points))
    }
})
conn.Start(ctx)
defer conn.Stop()

// Binance crypto (e.g. btcusdt) — broadcast feed, filtered locally.
conn.SubscribeCryptoPrices(ctx, []string{"btcusdt", "ethusdt"})
// Chainlink crypto feeds (e.g. eth/usd, btc/usd).
conn.SubscribeChainlinkPrices(ctx, []string{"eth/usd", "btc/usd"})
// Chainlink-computed TWAP prices over a 30s or 60s lookback window
// (e.g. btc/usd twap-60s). No snapshot — starts with the next update.
conn.SubscribeChainlinkTWAP(ctx, 60, []string{"btc/usd"})
// Equity / forex / commodities via Pyth (e.g. AAPL, EURUSD).
conn.SubscribeEquityPrices(ctx, []string{"AAPL", "TSLA"})
```

Notes:
- The binance `crypto_prices` topic is a broadcast feed (server-side filters
  do not reliably deliver data), so it's subscribed unfiltered and filtered
  locally.
- Chainlink `full_accuracy_value` is a raw integer scaled by 10¹⁸ — the
  numeric `value` is used for Chainlink prices. TWAP `value` is an exact
  decimal string derived from Chainlink's E18 fixed-point price; keep it as a
  string.
- Chainlink TWAP sends no snapshot: subscriptions start with the next update
  and there is no replay after a disconnect.
- Equity streams may require access/market-hours; updates mark
  `IsCarriedForward` when the market is closed.

### Gamma series discovery (Polymarket)

Discover the currently open and future events of a recurring market series
(Gamma "series", e.g. `btc-up-or-down-daily` or `btc-multi-strikes-weekly`)
without synthesizing market slugs. A ladder event holds one market per strike,
labelled by `GammaMarket.GroupItemTitle`.

```go
conn := polymarket.New(false, polymarket.Config{}, nil)
ctx := context.Background()

// slug → series (cached for 1h; InvalidateSeries(slug) forces a refetch).
series, err := conn.GetSeries(ctx, "btc-up-or-down-daily")
// series[0].ID = "41", series[0].Recurrence = connector.RecurrenceDaily

// Open + future events of that series, each with its nested markets.
openOnly := false
events, err := conn.ListSeriesEvents(ctx, connector.EventQuery{
    SeriesID:  series[0].ID,
    Closed:    &openOnly, // nil = server default (all events)
    Limit:     50,        // default 100, max 500; paginated internally
    Order:     "endDate",
    Ascending: true,
})
for _, ev := range events {
    fmt.Printf("%s settles %s (%d markets)\n", ev.Slug, ev.EndDate, len(ev.Markets))
    for _, m := range ev.Markets {
        outcomes, _ := m.OutcomeList()   // ["Yes", "No"]
        tokens, _ := m.TokenIDList()     // index-aligned with outcomes
        fmt.Printf("  strike=%s yes=%s\n", m.GroupItemTitle, tokens[0])
    }
}

// One event (with nested markets) by slug/ticker.
ev, err := conn.GetEvent(ctx, "bitcoin-above-on-october-3-2026")
```

Notes:
- `outcomes` and `clobTokenIds` arrive as JSON-encoded **strings**; use
  `GammaMarket.OutcomeList()` / `TokenIDList()` (index 0 = YES, 1 = NO).
- `ListSeriesEvents` is live state and is never cached; `GetSeries` is cached
  (`InvalidateSeries` to bust it). Duplicate events across pages are dropped
  and a short page ends the walk.
- Gamma requires a `User-Agent` header (403 without one) — the shared Gamma
  client sets `go-exchange-connector/<version>` on every request.
- Non-2xx responses surface as `*polymarket.GammaHTTPError` (status + URL);
  a slug that matches nothing yields an empty result and a nil error.
- `GetMarket` now also exposes `GroupItemTitle`, `NegRisk`, `NegRiskMarketID`,
  `EventSlug`/`EventTicker`, `Description` (the machine-readable resolution
  rule) and `StartDate`/`EndDate`.

### Binance market data streams (spot + perpetual)

Stream live market data directly from Binance — spot (`stream.binance.com`)
and USDⓈ-M perpetual futures (`fstream.binance.com`). Four streams supported,
with events dispatched as `connector.BinanceBookTickerEvent` (best bid/ask),
`connector.BinanceAggTradeEvent` (trades), `connector.BinanceDepthEvent` (full
order book, maintained locally), and `connector.BinanceKlineEvent` (OHLCV).
Symbol matching is case-insensitive.

```go
base := connector.New(false, nil)
bn := binance.New(base)
bn.SetDispatcher(func(ev any) {
    switch e := ev.(type) {
    case *connector.BinanceBookTickerEvent:
        fmt.Printf("%s(%s) bid=%s ask=%s\n", e.Symbol, e.Market, e.BestBidPrice, e.BestAskPrice)
    case *connector.BinanceAggTradeEvent:
        fmt.Printf("%s(%s) trade %s @ %s\n", e.Symbol, e.Market, e.Quantity, e.Price)
    case *connector.BinanceDepthEvent:
        fmt.Printf("%s(%s) book bids=%d asks=%d\n", e.Symbol, e.Market, len(e.Bids), len(e.Asks))
    case *connector.BinanceKlineEvent:
        fmt.Printf("%s(%s) %s close=%s\n", e.Symbol, e.Market, e.Interval, e.Close)
    }
})
bn.Start(ctx)
defer bn.Stop()

// bookTicker + trades on spot, depth + klines on perpetual:
bn.SubscribeBookTicker(ctx, binance.MarketSpot, []string{"btcusdt", "ethusdt"})
bn.SubscribeTrades(ctx, binance.MarketSpot, []string{"btcusdt"})
bn.SubscribeDepth(ctx, binance.MarketPerp, []string{"btcusdt"})
// Klines validate the interval ("1m", "5m", "15m", "1h", "4h", "1d") and
// reject anything else instead of putting it in the stream name.
if err := bn.SubscribeKlines(ctx, binance.MarketPerp, []string{"btcusdt"}, "5m"); err != nil {
    log.Fatal(err)
}
```

Notes:
- `MarketSpot` and `MarketPerp` are the two supported markets; both connect on
  `Start` and use message-based `SUBSCRIBE`/`UNSUBSCRIBE` frames (raw `/ws`
  endpoint), re-sent on every reconnect.
- Depth books are seeded from a REST snapshot (`limit=1000`) on each connect
  and updated from the `@depth@100ms` diff stream. Books re-snapshot on
  detected gaps. `BinanceDepthEvent.Bids` are sorted descending (best first),
  `Asks` ascending.
- Spot `bookTicker` frames carry no event-type field — the manager detects
  them by shape; the dispatched event carries payload casing (e.g. `BTCUSDT`).
- Klines are delivered for **every** update (~1/s per candle), not only the
  closed one: `BinanceKlineEvent.IsFinal` marks the closing update, and
  `TradeCount`/`Volume`/`QuoteVolume` are decimal strings (no float precision
  loss). `OpenTime` sits on the interval grid; `CloseTime - OpenTime` is the
  interval minus 1 ms.
- Smoke test: `go run ./cmd/test_binance -spot btcusdt -perp btcusdt -trades -depth -kline 1m -duration 12s`.

### Hyperliquid perpetuals market data

Stream live perp market data from Hyperliquid (`wss://api.hyperliquid.xyz/ws`)
and query the venue's `/info` endpoint for metadata and snapshots.

```go
base := connector.New(false, nil)
hl := hyperliquid.New(base)
hl.SetDispatcher(func(ev any) {
    switch e := ev.(type) {
    case *connector.HyperliquidBookEvent:
        fmt.Printf("%s book bids=%d asks=%d best_bid=%s\n", e.Coin, len(e.Bids), len(e.Asks), e.Bids[0].Price)
    case *connector.HyperliquidTradeEvent:
        fmt.Printf("%s trade %s @ %s side=%s\n", e.Coin, e.Size, e.Price, e.Side)
    case *connector.HyperliquidBBOEvent:
        fmt.Printf("%s bbo bid=%v ask=%v\n", e.Coin, e.Bid, e.Ask)
    case *connector.HyperliquidAssetCtxEvent:
        fmt.Printf("%s funding=%s mark=%s oi=%s\n", e.Coin, e.Funding, e.MarkPx, e.OpenInterest)
    case *connector.HyperliquidAllMidsEvent:
        fmt.Printf("mids for %d coins, BTC=%s\n", len(e.Mids), e.Mids["BTC"])
    case *connector.HyperliquidCandleEvent:
        fmt.Printf("%s %s close=%s vol=%s\n", e.Coin, e.Interval, e.Close, e.Volume)
    case *connector.HyperliquidErrorEvent:
        fmt.Printf("venue refused a subscription: %s\n", e.Message)
    }
})
if err := hl.Start(ctx, 0); err != nil { // 0 = 500ms base reconnect delay
    log.Fatal(err)
}
defer hl.Stop()

// Subscribe* never fails because the socket is down: subscriptions are recorded
// and (re-)sent on every successful connect.
hl.SubscribeL2Book(ctx, []string{"BTC", "ETH"}, hyperliquid.SubParams{Fast: true})
hl.SubscribeTrades(ctx, []string{"BTC"})
hl.SubscribeBBO(ctx, []string{"BTC"})
hl.SubscribeActiveAssetCtx(ctx, []string{"BTC"})
hl.SubscribeCandles(ctx, []string{"BTC"}, "1m")
hl.SubscribeAllMids(ctx) // global: filter HyperliquidAllMidsEvent.Mids locally

// One-shot venue metadata (also available verbatim for archiving):
info := hyperliquid.NewInfoClient("")
meta, _ := info.Meta(ctx)            // 234 perps on 2026-10-04
raw, _ := info.MetaRaw(ctx)          // exact response body, for a capture
dexs, _ := info.PerpDexs(ctx)        // [nil, {name:"xyz", …}] — nil = default dex
mctx, _ := info.MetaAndAssetCtxs(ctx)
funding, _ := mctx.FundingRate("BTC")
```

Notes — all verified live on 2026-10-04 with `go run ./cmd/test_hyperliquid`:

- **Wire format is `{"method":"subscribe","subscription":{...}}`, one
  subscription per frame** — *not* the bare `{"type":"l2Book","coin":"BTC"}`
  form shown in `PERP_COLLECTOR_SPEC.md` §1. The bare form is not the venue's
  protocol; the method/subscription form is what the docs and every published
  SDK send, and what this package emits (`BuildSubscriptions` +
  `SubscribeFrame` are exported for a recorder that needs the frames itself).
  The venue replies `{"channel":"subscriptionResponse","data":{…}}` with the
  subscription normalised (`nSigFigs: null, mantissa: null, fast: false`
  filled in), then `{"channel":"<type>","data":<payload>}` for the feed.
- **One connection carries everything.** The venue allows 10 connections and
  1000 subscriptions per IP, so a single socket covers the public feed.
  Subscriptions are re-sent on every reconnect; `SetOnStatusChange` reports
  connected/disconnected.
- **Keepalive is application-level**: `{"method":"ping"}` → `{"channel":"pong"}`
  every 50s (`Options.AppPingIntervalMs`). A WebSocket-level control ping runs
  at 20s for half-open detection (`Options.WSPingIntervalMs`); a 75s live run
  showed no reconnect churn, so the venue answers both. The data-staleness
  watchdog is **off by default** — every channel except `l2Book` is
  event-driven, so silence means "nothing happened".
- **Measured rates** (3 coins, 4 channels, 75s; BTC/ETH/SOL, slow 20-level book):
  `l2Book` 0.2 frames/s/coin at ~1.6 KB, `trades` 0.6 frames/s/coin at ~0.7 KB
  (max 8.3 KB burst), `bbo` 4.5 frames/s/coin at ~144 B, `activeAssetCtx`
  1 frame/s/coin at ~313 B. So `bbo` dominates the *frame count* (71%) while
  `l2Book` + `trades` dominate the *bytes*; all four together are ~1.7 KiB/s
  per coin (~150 MB/day raw across 10 coins, well under the 10 GB budget).
  `allMids` is the largest frame seen: 21.8 KB for 1,237 coins, ~0.2 frames/s.
- **Payload details that matter**: `trades[].side` is `"B"`/`"A"`
  (`connector.HyperliquidNormalizeSide` also accepts `"buy"`/`"sell"`); `bbo`
  sides can be `null`; the `candle` frame is a **single object** despite the
  docs typing it as `Candle[]` (both shapes are accepted); `activeAssetCtx` and
  `allMids` carry no timestamp, so `Timestamp` mirrors `ReceivedAt`; prices and
  sizes stay decimal strings end-to-end (`Num` accepts a bare JSON number too).
- **An `{"channel":"error"}` frame is dispatched as
  `HyperliquidErrorEvent`** as well as logged: a refused subscription is
  otherwise invisible — the socket stays healthy and the channel simply never
  delivers.
- Coin names are case-sensitive (`BTC`, `kPEPE`, `xyz:TSLA` for HIP-3); they are
  validated (whitespace/quotes/backslashes/control characters rejected) because
  they are interpolated into the subscription frame.
- Read limit is 1 MiB per message (`Options.ReadLimitBytes`), ~130× the largest
  frame observed.
- Smoke test / probe: `go run ./cmd/test_hyperliquid -coins BTC,ETH,SOL -duration 75s`
  (add `-info` for the REST client only, `-events` to print every typed event,
  `-fast` for the 5-level book).

## Architecture

### ExchangeConnector interface

```go
type ExchangeConnector interface {
    PlaceLimitOrders(orders []LimitOrder) (OrderResult, error)
    CancelOrders(orderIDs []string) error
    GetMarket(id, slug string) (*Market, error)
    GetResolution(marketID string) (*Resolution, error)
    GetCryptoPrice(req CryptoPriceRequest) (*CryptoPrice, error)
    Subscribe(assetIDs []string)
    Unsubscribe(assetIDs []string)
    SetOnEvent(cb func(any))
    Start(ctx context.Context) error
    Stop()
}
```

Implement this interface for each exchange (Polymarket, Binance, Kalshi, etc.).

### Connector base struct (with paper simulation)

The `Connector` struct implements `ExchangeConnector` with built-in paper-mode simulation:

```
PAPER mode:

  Strategy ──PlaceLimitOrders()──▶ Connector.PlaceLimitOrders()
                                        │
                                        ├── OnReservation(order)  ◀── mounted by app
                                        ├── OnPlacement(order)    ◀── mounted by app
                                        └── OrderResult{success}

LIVE mode:

  Strategy ──PlaceLimitOrders()──▶ Connector.PlaceLimitOrders()
                                        │
                                        └── LiveExecutor.PlaceLimitOrders(orders)
                                              │
                                              └── real exchange API call
```

### Event flow

Market data flows **in reverse** — exchange-specific WebSocket code calls the handler fields:

```
  Exchange WS ──▶ exchange-specific code ──▶ Connector.OnBook(snapshot)
                                              Connector.OnTrade(trade)
                                              Connector.OnPriceChange(marketID, changes)
                                                    │
                                                    └── your application handler
```

### BaseWebSocket (pkg/websocket)

`BaseWebSocket` handles connection lifecycle (dial, reconnect with backoff, ping/pong keepalive, status tracking). Exchange-specific implementations embed it and wire their own message parsing:

```go
import "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"

type MyExchangeWS struct {
    *websocket.BaseWebSocket
    // exchange-specific state
}

func NewMyExchangeWS() *MyExchangeWS {
    m := &MyExchangeWS{}
    m.BaseWebSocket = &websocket.BaseWebSocket{}
    m.OnMessage = m.handleMessage
    m.OnDisconnect = m.onDisconnect
    return m
}

func (m *MyExchangeWS) handleMessage(ctx context.Context, data []byte) error {
    // Parse exchange-specific JSON and call mounted handlers
    var envelope MyEnvelope
    json.Unmarshal(data, &envelope)
    switch envelope.Type {
    case "book":
        connector.OnBook(parseBook(data))
    }
    return nil
}
```

## Creating a New Exchange Connector

1. **Implement `ExchangeConnector`** — create a struct that implements all interface methods
2. **Use `BaseWebSocket`** for WebSocket connection management (reconnection, ping/pong)
3. **Hold a `*Connector`** for paper-mode simulation — delegate `PlaceLimitOrders`/`CancelOrders` to it when `!isLive`
4. **Call handler fields** (`OnBook`, `OnTrade`, etc.) when exchange WS events arrive
5. **Set `LiveExecutor`** on the Connector for LIVE-mode API calls

### Skeleton

```go
package myexchange

import (
    "github.com/algoboy-kevin/go-exchange-connector"
    "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

type MyExchangeConnector struct {
    *connector.Connector
    ws     *websocket.BaseWebSocket
    // ...
}

func New(isLive bool, apiKey string) *MyExchangeConnector {
    c := &MyExchangeConnector{
        Connector: connector.New(isLive, &myLiveExecutor{apiKey: apiKey}),
    }
    return c
}

func (m *MyExchangeConnector) Start(ctx context.Context) error {
    m.ws = &websocket.BaseWebSocket{}
    m.ws.OnConnect = m.onConnect   // send subscription handshake
    m.ws.OnMessage = m.onMessage   // parse JSON → call Connector handlers
    return m.ws.Connect(ctx, "wss://exchange.com/ws", websocket.DefaultWSOptions())
}

func (m *MyExchangeConnector) onMessage(ctx context.Context, data []byte) error {
    // Parse and dispatch to m.OnBook, m.OnTrade, etc.
}
```

## License

MIT
