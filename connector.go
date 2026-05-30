package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────
// ExchangeConnector (interface)
// ─────────────────────────────────────────────────────────────

// ExchangeConnector defines the interface for exchange interactions.
//
// Implementations handle two concerns:
//   - Market data: subscribing to asset feeds, receiving events via handlers
//   - Order execution: placing and cancelling orders
//
// See Connector for a base implementation with built-in paper-mode
// simulation that implements this interface.
type ExchangeConnector interface {
	// ── Orders ──────────────────────────────────────────────

	// PlaceLimitOrders submits a batch of limit orders.
	// In LIVE mode, sends to the exchange API.
	// In PAPER mode, simulates via the OnReservation → OnPlacement chain.
	PlaceLimitOrders(orders []LimitOrder) (OrderResult, error)

	// CancelOrders cancels a set of orders by ID.
	CancelOrders(orderIDs []string) error

	// ── Market metadata ─────────────────────────────────────

	// GetMarket fetches market metadata by ID or slug.
	GetMarket(id, slug string) (*Market, error)

	// GetResolution checks if a market has resolved, returning the outcome.
	GetResolution(marketID string) (*Resolution, error)

	// ── Subscriptions ───────────────────────────────────────

	// Subscribe starts receiving market data for the given asset IDs.
	Subscribe(assetIDs []string)

	// Unsubscribe stops receiving market data for the given asset IDs.
	Unsubscribe(assetIDs []string)

	// SetOnEvent registers a callback fired for every raw typed event
	// before handler dispatch. Used for recording/monitoring.
	SetOnEvent(cb func(any))

	// SetHandlers configures all event and lifecycle handlers at once.
	SetHandlers(handlers ConnectorHandlers)

	// ── Lifecycle ───────────────────────────────────────────

	// Start initialises the connector (connects WebSockets, etc.).
	Start(ctx context.Context) error

	// Stop shuts down the connector gracefully.
	Stop()
}

// ─────────────────────────────────────────────────────────────
// ConnectorHandlers
// ─────────────────────────────────────────────────────────────

// ConnectorHandlers groups all event and lifecycle callbacks that can be
// mounted on a Connector. Pass to SetHandlers for a clean one-shot setup.
type ConnectorHandlers struct {
	// Market data handlers — called by exchange-specific WS code.
	OnPriceChange    func(marketID string, changes []PriceChange)
	OnBook           func(snapshot BookSnapshot)
	OnTrade          func(trade Trade)
	OnMarketResolved func(ev MarketResolved)
	OnTickChange     func(ev TickChange)

	// Order lifecycle handlers — PAPER mode calls these directly;
	// LIVE mode calls them after exchange WS confirmation.
	OnReservation func(order LimitOrder) error
	OnPlacement   func(order LimitOrder) error
	OnCancel      func(orderID string) error
	OnFill        func(fill Fill)

	// Other handlers.
	OnError   func(err error)
	OnWSOpen  func()
	OnWSClose func()
}

// ─────────────────────────────────────────────────────────────
// LiveExecutor
// ─────────────────────────────────────────────────────────────

// LiveExecutor handles real exchange API calls in LIVE mode.
// Exchange-specific connectors implement this to provide the actual
// HTTP/RPC calls to the exchange.
type LiveExecutor interface {
	PlaceLimitOrders(orders []LimitOrder) (OrderResult, error)
	CancelOrders(orderIDs []string) error
	GetMarket(id, slug string) (*Market, error)
	GetResolution(marketID string) (*Resolution, error)
}

// ─────────────────────────────────────────────────────────────
// Connector (base struct)
// ─────────────────────────────────────────────────────────────

// Connector is a reusable exchange connector base with built-in paper-mode
// simulation. It implements ExchangeConnector.
//
// Usage:
//
//	// PAPER mode — orders are simulated via mounted handlers:
//	conn := connector.New(false, nil)
//	conn.OnReservation = func(o connector.LimitOrder) error { ... }
//	conn.OnPlacement   = func(o connector.LimitOrder) error { ... }
//
//	// LIVE mode — delegates to a LiveExecutor:
//	exec := &polymarket.LiveExecutor{...}
//	conn := connector.New(true, exec)
//
// Market data handlers (OnPriceChange, OnBook, etc.) are called by
// exchange-specific WS code. Mount them to process incoming data:
//
//	conn.OnPriceChange = func(marketID string, changes []connector.PriceChange) {
//	    // forward to internal actor system
//	}
type Connector struct {
	// IsLive controls execution routing.
	//   false → PlaceLimitOrders/CancelOrders simulate via handler chain
	//   true  → delegates to LiveExecutor
	IsLive bool

	// LiveExecutor handles real API calls in LIVE mode.
	// Must be set when IsLive is true.
	Live LiveExecutor

	// Now returns the current time. Defaults to time.Now.
	// Override for backtest mode (e.g. return clock.Now()).
	Now func() time.Time

	// ── Market data handlers ────────────────────────────────
	// Called by exchange-specific WebSocket code when events arrive.

	OnPriceChange    func(marketID string, changes []PriceChange)
	OnBook           func(snapshot BookSnapshot)
	OnTrade          func(trade Trade)
	OnMarketResolved func(ev MarketResolved)
	OnTickChange     func(ev TickChange)

	// ── Order lifecycle handlers ────────────────────────────
	// Mounted by the application to bridge into the internal
	// order management system (OrderManager, Store, etc.).
	//
	// PAPER mode: Connector calls these directly in PlaceLimitOrders.
	// LIVE mode:  exchange WS confirmation calls these after the
	//             exchange confirms the order.

	OnReservation func(order LimitOrder) error
	OnPlacement   func(order LimitOrder) error
	OnCancel      func(orderID string) error
	OnFill        func(fill Fill)

	// ── Other handlers ──────────────────────────────────────

	// OnError is called for non-fatal errors.
	OnError func(err error)

	// OnWSOpen is called when the WebSocket connects.
	OnWSOpen func()

	// OnWSClose is called when the WebSocket disconnects.
	OnWSClose func()

	// ── Internal ────────────────────────────────────────────

	seqID   atomic.Int64
	onEvent func(any)
}

// New creates a new Connector.
//
//	isLive: true  → LIVE mode (requires LiveExecutor)
//	        false → PAPER mode (simulates via handlers)
//	live:   LiveExecutor for LIVE mode, or nil for PAPER mode.
func New(isLive bool, live LiveExecutor) *Connector {
	return &Connector{
		IsLive: isLive,
		Live:   live,
		Now:    time.Now,
	}
}

// ── ExchangeConnector implementation ─────────────────────────

// PlaceLimitOrders submits a batch of limit orders.
//
// In PAPER mode (IsLive=false): simulates order lifecycle by calling
// OnReservation → OnPlacement for each order. Mount these handlers
// to interact with your internal order manager.
//
// In LIVE mode (IsLive=true): delegates to LiveExecutor.PlaceLimitOrders.
func (c *Connector) PlaceLimitOrders(orders []LimitOrder) (OrderResult, error) {
	if c.IsLive {
		if c.Live == nil {
			return OrderResult{Success: false}, fmt.Errorf("not implemented")
		}

		return c.Live.PlaceLimitOrders(orders)
	}

	// ── PAPER mode: simulate order lifecycle ────────────────
	for i, o := range orders {
		if c.OnReservation != nil {
			if err := c.OnReservation(o); err != nil {
				return OrderResult{
					Success:  false,
					ErrorMsg: err.Error(),
				}, err
			}
		}
		if c.OnPlacement != nil {
			if err := c.OnPlacement(o); err != nil {
				return OrderResult{
					Success:  false,
					ErrorMsg: err.Error(),
				}, err
			}
		}

		// Log progress for large batches.
		if i > 0 && i%100 == 0 {
			slog.Debug("paper: placed orders", "count", i+1, "total", len(orders))
		}
	}

	slog.Debug("paper: batch placed",
		"count", len(orders),
		"is_live", false,
	)

	results := make([]SingleOrderResult, len(orders))
	for i, o := range orders {
		results[i] = SingleOrderResult{OrderID: o.OrderID, Success: true}
	}
	return OrderResult{Success: true, Orders: results}, nil
}

// CancelOrders cancels a set of orders.
//
// In PAPER mode: calls OnCancel for each order ID.
// In LIVE mode: delegates to LiveExecutor.CancelOrders.
func (c *Connector) CancelOrders(orderIDs []string) error {
	if c.IsLive {
		if c.Live == nil {
			return fmt.Errorf("not implemented")
		}
		return c.Live.CancelOrders(orderIDs)
	}

	// ── PAPER mode: simulate cancellation ───────────────────
	for _, id := range orderIDs {
		if c.OnCancel != nil {
			if err := c.OnCancel(id); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetMarket fetches market metadata by ID or slug.
// Delegates to LiveExecutor if set.
func (c *Connector) GetMarket(id, slug string) (*Market, error) {
	if c.Live != nil {
		return c.Live.GetMarket(id, slug)
	}
	return nil, fmt.Errorf("not implemented")
}

// GetResolution checks if a market has resolved.
func (c *Connector) GetResolution(marketID string) (*Resolution, error) {
	if c.Live != nil {
		return c.Live.GetResolution(marketID)
	}
	return nil, fmt.Errorf("not implemented")
}

// Subscribe starts receiving market data for the given asset IDs.
// Must be overridden by exchange-specific code that owns the WebSocket.
func (c *Connector) Subscribe(assetIDs []string) {
	slog.Warn("connector: Subscribe not implemented (override in exchange-specific code)")
}

// Unsubscribe stops receiving market data for the given asset IDs.
func (c *Connector) Unsubscribe(assetIDs []string) {
	slog.Warn("connector: Unsubscribe not implemented (override in exchange-specific code)")
}

// SetOnEvent registers a callback for raw typed events.
// The callback receives the same struct that is passed to the handler
// fields (e.g. BookSnapshot, Trade), before handler dispatch.
func (c *Connector) SetOnEvent(cb func(any)) {
	c.onEvent = cb
}

// SetHandlers configures all event and lifecycle handlers at once.
func (c *Connector) SetHandlers(handlers ConnectorHandlers) {
	c.OnPriceChange = handlers.OnPriceChange
	c.OnBook = handlers.OnBook
	c.OnTrade = handlers.OnTrade
	c.OnMarketResolved = handlers.OnMarketResolved
	c.OnTickChange = handlers.OnTickChange
	c.OnReservation = handlers.OnReservation
	c.OnPlacement = handlers.OnPlacement
	c.OnCancel = handlers.OnCancel
	c.OnFill = handlers.OnFill
	c.OnError = handlers.OnError
	c.OnWSOpen = handlers.OnWSOpen
	c.OnWSClose = handlers.OnWSClose
}

// Start initialises the connector. Override in exchange-specific code
// to start WebSocket connections.
func (c *Connector) Start(ctx context.Context) error {
	slog.Info("connector: started",
		"is_live", c.IsLive,
		"has_live_exec", c.Live != nil,
	)
	return nil
}

// Stop shuts down the connector. Override in exchange-specific code.
func (c *Connector) Stop() {
	slog.Info("connector: stopped")
}

// ── Helpers ──────────────────────────────────────────────────

// DispatchEvent fires the OnEvent hook (if set). Call this from
// exchange-specific code after constructing typed event structs.
func (c *Connector) DispatchEvent(ev any) {
	if c.onEvent != nil {
		c.onEvent(ev)
	}
}

// NextSeqID atomically increments and returns the sequence counter.
// Used by exchange-specific code to assign monotonic IDs to events.
func (c *Connector) NextSeqID() int64 {
	return c.seqID.Add(1)
}
