package contract

import (
	"context"
	"time"
)

// Trader is the core every perpetual adapter implements. Single and batch
// placement and cancellation use the same request shapes, so switching venue
// does not change calling code.
type Trader interface {
	PlaceOrder(ctx context.Context, req OrderRequest) (OrderResult, error)
	// PlaceOrders returns one result per request, in order. Per-item rejections
	// are results, not errors.
	PlaceOrders(ctx context.Context, reqs []OrderRequest) ([]OrderResult, error)
	CancelOrder(ctx context.Context, req CancelRequest) (CancelResult, error)
	// CancelOrders returns one result per request, in order.
	CancelOrders(ctx context.Context, reqs []CancelRequest) ([]CancelResult, error)
	OpenOrders(ctx context.Context, symbol string) ([]Order, error)
}

// BatchLimiter reports the venue's batch sizes. Callers chunk to these limits.
// Adapters reject larger batches with ErrInvalid and send nothing.
type BatchLimiter interface {
	MaxPlaceBatch() int
	MaxCancelBatch() int
}

// CancelAller cancels every open order for a symbol. A venue without a native
// cancel-all is not an adapter here. Callers fall back to OpenOrders + CancelOrders.
type CancelAller interface {
	CancelAll(ctx context.Context, symbol string) error
}

// DeadManSwitch arms a server-side cancel that fires after the timeout unless it
// is renewed. Calling it again renews it. after <= 0 disarms.
// Venues that scope the switch globally (Hyperliquid) ignore symbol.
type DeadManSwitch interface {
	ArmDeadManSwitch(ctx context.Context, symbol string, after time.Duration) error
}

// PositionReader lists open positions. An empty symbol returns all of them.
type PositionReader interface {
	Positions(ctx context.Context, symbol string) ([]Position, error)
}

// AccountReader returns collateral totals.
type AccountReader interface {
	Account(ctx context.Context) (Account, error)
}

// LeverageSetter sets leverage and margin mode for one symbol. An empty mode
// leaves the margin mode unchanged.
type LeverageSetter interface {
	SetLeverage(ctx context.Context, symbol string, leverage int, mode MarginMode) error
}

// Feed manages market-data subscriptions. Event delivery is defined per adapter.
type Feed interface {
	Subscribe(ctx context.Context, subs []Subscription) error
	Unsubscribe(ctx context.Context, subs []Subscription) error
}
