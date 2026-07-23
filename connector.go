package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

	PlaceMarketOrder(order MarketOrder) (OrderResult, error)

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

	// SetDispatcher sets the event dispatcher callback. Exchange-specific
	// code pushes typed events (PriceChangeEvent, BookSnapshotEvent,
	// TradeEvent, OrderPlacementEvent, etc.) through DispatchEvent, which
	// routes them here. The application mounts a switch to forward events
	// to the appropriate actor/manager.
	SetDispatcher(d func(any))

	// ── CTF (Conditional Token Framework) — async ──────────

	// SplitPosition splits collateral into outcome tokens (e.g. USDC → YES/NO).
	// Returns immediately. Results are dispatched as OrderSplitEvent when confirmed.
	SplitPosition(ctx context.Context, marketID string, amountUsdc float64) error

	// MergePositions merges outcome tokens back into collateral (e.g. YES/NO → USDC).
	// Returns immediately. Results are dispatched as OrderMergeEvent when confirmed.
	MergePositions(ctx context.Context, marketID string, amountUsdc float64) error

	// RedeemPositions redeems winning outcome tokens after market resolution.
	// Returns immediately. Results are dispatched as OrderRedeemEvent when confirmed.
	RedeemPositions(ctx context.Context, marketID string) error

	// ── Lifecycle ───────────────────────────────────────────

	// Start initialises the connector (connects WebSockets, etc.).
	Start(ctx context.Context) error

	// Stop shuts down the connector gracefully.
	Stop()
}

// ─────────────────────────────────────────────────────────────
// LiveExecutor
// ─────────────────────────────────────────────────────────────

// LiveExecutor handles real exchange API calls in LIVE mode.
// Exchange-specific connectors implement this to provide the actual
// HTTP/RPC calls to the exchange.
type LiveExecutor interface {
	PlaceLimitOrders(orders []LimitOrder) (OrderResult, error)
	PlaceMarketOrder(order MarketOrder) (OrderResult, error)
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
//	// PAPER mode — orders are simulated, events dispatched:
//	conn := connector.New(false, nil)
//	conn.SetDispatcher(func(ev any) {
//	    switch e := ev.(type) {
//	    case *connector.OrderPlacementEvent: ...
//	    case *connector.OrderFillEvent: ...
//	    }
//	})
//
//	// LIVE mode — delegates to a LiveExecutor:
//	exec := &polymarket.LiveExecutor{...}
//	conn := connector.New(true, exec)
//
// Exchange-specific WS code pushes typed events through DispatchEvent,
// which routes them to the mounted dispatcher.
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

	// ValidateOrder is an optional callback for PAPER mode.
	// If set, it's called before each order is placed. Return a non-nil error
	// to reject the order (simulating an exchange API rejection). The rejected
	// order will have success=false in the returned OrderResult and no event
	// will be dispatched for it.
	ValidateOrder func(LimitOrder) error

	// ── Internal ────────────────────────────────────────────

	seqID      atomic.Int64
	onDispatch func(any)

	dispatchCh chan any
	dispatchWg sync.WaitGroup
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

// SetValidateOrder sets the optional paper-mode order validation callback.
func (c *Connector) SetValidateOrder(fn func(LimitOrder) error) {
	c.ValidateOrder = fn
}

// IsPaperMode returns true when running in PAPER mode.
func (c *Connector) IsPaperMode() bool {
	return !c.IsLive
}

// ── ExchangeConnector implementation ─────────────────────────

// PlaceLimitOrders submits a batch of limit orders.
//
// In PAPER mode (IsLive=false): validates each order via ValidateOrder
// and returns results. The engine generates synthetic lifecycle events.
//
// In LIVE mode (IsLive=true): delegates to LiveExecutor.PlaceLimitOrders.
func (c *Connector) PlaceLimitOrders(orders []LimitOrder) (OrderResult, error) {
	if c.IsLive {
		if c.Live == nil {
			return OrderResult{Success: false}, fmt.Errorf("not implemented")
		}

		return c.Live.PlaceLimitOrders(orders)
	}

	// ── PAPER mode: validate only (events generated by engine) ──
	results := make([]SingleOrderResult, len(orders))
	for i, o := range orders {
		// Optional pre-placement validation (simulates exchange API rejection)
		if c.ValidateOrder != nil {
			if err := c.ValidateOrder(o); err != nil {
				slog.Debug("paper: order rejected by ValidateOrder",
					"id", o.OrderID, "reason", err)
				results[i] = SingleOrderResult{
					OrderID:  "",
					Success:  false,
					ErrorMsg: err.Error(),
				}
				continue // skip dispatch — no event emitted
			}
		}

		results[i] = SingleOrderResult{OrderID: o.OrderID, Success: true}

		// Log progress for large batches.
		if i > 0 && i%100 == 0 {
			slog.Debug("paper: placed orders", "count", i+1, "total", len(orders))
		}
	}

	slog.Debug("paper: batch placed",
		"count", len(orders),
		"is_live", false,
	)

	return OrderResult{Success: true, Orders: results}, nil
}

// PlaceMarketOrder submits a market order.
//
// In PAPER mode (IsLive=false): validates and returns. The engine
// generates synthetic OrderPlacementEvent + OrderFillEvent.
//
// In LIVE mode (IsLive=true): delegates to LiveExecutor.PlaceMarketOrder.
func (c *Connector) PlaceMarketOrder(order MarketOrder) (OrderResult, error) {
	if c.IsLive {
		if c.Live == nil {
			return OrderResult{Success: false}, fmt.Errorf("not implemented")
		}
		return c.Live.PlaceMarketOrder(order)
	}

	// ── PAPER mode: validate only (events generated by engine) ──
	// Optional pre-placement validation (simulates exchange API rejection)
	if c.ValidateOrder != nil {
		if err := c.ValidateOrder(LimitOrder{
			OrderID:  order.OrderID,
			AssetID:  order.AssetID,
			MarketID: order.MarketID,
			Side:     order.Side,
			Price:    order.Price,
			Size:     order.Size,
		}); err != nil {
			slog.Debug("paper: market order rejected by ValidateOrder",
				"id", order.OrderID, "reason", err)
			return OrderResult{
				Success: true,
				Orders: []SingleOrderResult{{
					OrderID:  "",
					Success:  false,
					ErrorMsg: err.Error(),
				}},
			}, nil
		}
	}

	slog.Debug("paper: market order filled",
		"id", order.OrderID,
		"side", order.Side,
		"size", order.Size,
		"price", order.Price,
	)

	return OrderResult{
		Success: true,
		Orders: []SingleOrderResult{{
			OrderID: order.OrderID,
			Success: true,
		}},
	}, nil
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

	// ── PAPER mode: no-op (events generated by engine) ──────
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

// SetDispatcher sets the event dispatcher callback. All typed events
// (PriceChangeEvent, BookSnapshotEvent, TradeEvent, OrderPlacementEvent,
// etc.) from exchange-specific WS code are routed through DispatchEvent
// to this callback.
func (c *Connector) SetDispatcher(d func(any)) {
	c.onDispatch = d
}

// Start initialises the connector. Override in exchange-specific code
// to start WebSocket connections.
func (c *Connector) Start(ctx context.Context) error {
	// slog.Info("connector: started",
	// 	"is_live", c.IsLive,
	// 	"has_live_exec", c.Live != nil,
	// )

	return nil
}

// Stop shuts down the connector. If async dispatch is enabled, it
// drains the event channel gracefully before returning.
func (c *Connector) Stop() {
	if c.dispatchCh != nil {
		close(c.dispatchCh)
		c.dispatchWg.Wait()
		c.dispatchCh = nil
	}
}

// ── Helpers ──────────────────────────────────────────────────

// DispatchEvent routes an event to the engine.
//
// In asynchronous mode (after EnableAsyncDispatch): pushes to a buffered
// channel and returns immediately — the engine processes events on its
// own goroutine. If the channel is full, the event is dropped with a
// warning.
//
// In synchronous mode (default): calls the dispatcher callback directly
// and blocks until it returns.
func (c *Connector) DispatchEvent(ev any) {
	if c.dispatchCh != nil {
		select {
		case c.dispatchCh <- ev:
		default:
			slog.Warn("connector: dropping event, dispatch channel full")
		}
		return
	}
	if c.onDispatch != nil {
		c.onDispatch(ev)
	}
}

// EnableAsyncDispatch switches DispatchEvent to asynchronous mode.
// Events are pushed into a buffered channel and processed by a
// background goroutine, so the caller never blocks on the engine.
//
// Call this after SetDispatcher but before Start. The buffer size
// controls how many events can queue up before drops occur.
// If bufferSize <= 0, a default of 4096 is used.
func (c *Connector) EnableAsyncDispatch(bufferSize int) {
	if bufferSize <= 0 {
		bufferSize = 4096
	}
	c.dispatchCh = make(chan any, bufferSize)
	c.dispatchWg.Add(1)
	go func() {
		defer c.dispatchWg.Done()
		for ev := range c.dispatchCh {
			if c.onDispatch != nil {
				c.onDispatch(ev)
			}
		}
	}()
	slog.Debug("connector: async dispatch enabled", "buffer", bufferSize)
}

// NextSeqID atomically increments and returns the sequence counter.
// Used by exchange-specific code to assign monotonic IDs to events.
func (c *Connector) NextSeqID() int64 {
	return c.seqID.Add(1)
}
