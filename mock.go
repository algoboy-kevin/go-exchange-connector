package connector

import (
	"context"
	"log/slog"
)

// ─────────────────────────────────────────────────────────────
// MockConnector
// ─────────────────────────────────────────────────────────────

// MockConnector is a no-op connector that accepts all orders silently.
// Useful for testing connector implementations in isolation without
// any real exchange or actor system.
//
// Usage:
//
//	mock := connector.NewMockConnector()
//	result, err := mock.PlaceLimitOrders(orders) // always succeeds
//	mock.OnEvent = func(ev any) { fmt.Println(ev) }
type MockConnector struct {
	OnEvent func(any)
}

// NewMockConnector creates a new MockConnector.
func NewMockConnector() *MockConnector {
	return &MockConnector{}
}

func (m *MockConnector) PlaceLimitOrders(orders []LimitOrder) (OrderResult, error) {
	results := make([]SingleOrderResult, len(orders))
	for i, o := range orders {
		slog.Debug("mock: accepted order", "id", o.OrderID, "price", o.Price, "size", o.Size)
		results[i] = SingleOrderResult{OrderID: o.OrderID, Success: true}
	}
	return OrderResult{Success: true, Orders: results}, nil
}

func (m *MockConnector) PlaceMarketOrder(order MarketOrder) (OrderResult, error) {
	slog.Debug("mock: accepted market order", "id", order.OrderID, "side", order.Side, "size", order.Size)
	return OrderResult{
		Success: true,
		Orders: []SingleOrderResult{{
			OrderID: order.OrderID,
			Success: true,
		}},
	}, nil
}

func (m *MockConnector) CancelOrders(orderIDs []string) error {
	for _, id := range orderIDs {
		slog.Debug("mock: cancelled order", "id", id)
	}
	return nil
}

func (m *MockConnector) GetMarket(id, slug string) (*Market, error) {
	return &Market{ID: id, Slug: slug}, nil
}

func (m *MockConnector) GetResolution(marketID string) (*Resolution, error) {
	return nil, nil
}

func (m *MockConnector) Subscribe(assetIDs []string) {
	slog.Debug("mock: subscribed", "assets", assetIDs)
}

func (m *MockConnector) Unsubscribe(assetIDs []string) {
	slog.Debug("mock: unsubscribed", "assets", assetIDs)
}

func (m *MockConnector) SetOnEvent(cb func(any)) {
	m.OnEvent = cb
}

func (m *MockConnector) SetDispatcher(d func(any)) {
	// No-op — mock doesn't route events.
}

func (m *MockConnector) Start(ctx context.Context) error {
	slog.Info("mock: connector started")
	return nil
}

func (m *MockConnector) Stop() {
	slog.Info("mock: connector stopped")
}

// Compile-time interface check.
var _ ExchangeConnector = (*MockConnector)(nil)
