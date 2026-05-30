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

## Architecture

### ExchangeConnector interface

```go
type ExchangeConnector interface {
    PlaceLimitOrders(orders []LimitOrder) (OrderResult, error)
    CancelOrders(orderIDs []string) error
    GetMarket(id, slug string) (*Market, error)
    GetResolution(marketID string) (*Resolution, error)
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
