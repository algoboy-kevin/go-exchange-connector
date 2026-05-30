package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/internal/utils"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	gws "github.com/gorilla/websocket"
)

const (
	defaultUserReconnectIntervalMs = 5000
	userWSSURL                     = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
)

// WSPolymarketUserWS manages an authenticated WebSocket connection to the
// Polymarket user channel. It delivers order and trade events for the
// authenticated API key.
type WSPolymarketUserWS struct {
	*ws.BaseWebSocket

	base     *connector.Connector
	auth     UserAuth
	handlers UserHandlers

	mu                          sync.RWMutex
	subscribedMarketIDs         map[string]struct{}
	pendingSubscribeMarketIDs   map[string]struct{}
	pendingUnsubscribeMarketIDs map[string]struct{}

	pingCancel context.CancelFunc
}

// UserHandlers groups all user-provided callbacks for the user channel.
type UserHandlers struct {
	OnOrder   func(events []OrderWSEvent)
	OnTrade   func(events []TradeWSEvent)
	OnWSOpen  func()
	OnWSClose func()
	OnError   func(err error)
}

// NewWSPolymarketUserWS creates a new user channel WebSocket manager.
func NewWSPolymarketUserWS(base *connector.Connector, auth UserAuth, handlers UserHandlers) *WSPolymarketUserWS {
	u := &WSPolymarketUserWS{
		BaseWebSocket:               &ws.BaseWebSocket{},
		base:                        base,
		auth:                        auth,
		handlers:                    handlers,
		subscribedMarketIDs:         make(map[string]struct{}),
		pendingSubscribeMarketIDs:   make(map[string]struct{}),
		pendingUnsubscribeMarketIDs: make(map[string]struct{}),
	}

	u.ShouldConnect = func() bool { return true }
	u.OnConnect = u.onConnect
	u.OnMessage = u.onMessage
	u.OnDisconnect = u.onDisconnect
	u.OnError = func(err error) {
		if u.handlers.OnError != nil {
			u.handlers.OnError(err)
		} else {
			slog.Warn("user WS: error", "err", err)
		}
	}

	return u
}

// Start connects to the user WS. Starts a periodic ping loop.
func (u *WSPolymarketUserWS) Start(ctx context.Context, wsURL string) error {
	opts := ws.DefaultWSOptions()
	opts.ReconnectInterval = defaultUserReconnectIntervalMs

	// Start ping loop.
	pingCtx, pingCancel := context.WithCancel(ctx)
	u.pingCancel = pingCancel
	go u.pingLoop(pingCtx)

	return u.Connect(ctx, wsURL, opts)
}

// Stop shuts down the WebSocket and ping loop.
func (u *WSPolymarketUserWS) Stop() {
	if u.pingCancel != nil {
		u.pingCancel()
	}
	u.Close()
	u.clearSubscriptions()
}

// SubscribeMarkets adds condition IDs to the subscription filter.
func (u *WSPolymarketUserWS) SubscribeMarkets(marketIDs []string) {
	u.mu.Lock()
	for _, id := range marketIDs {
		delete(u.pendingUnsubscribeMarketIDs, id)
		if _, ok := u.subscribedMarketIDs[id]; ok {
			continue
		}
		u.pendingSubscribeMarketIDs[id] = struct{}{}
	}
	u.mu.Unlock()
	u.flushMarketSubscriptions()
}

// UnsubscribeMarkets removes condition IDs from the filter.
func (u *WSPolymarketUserWS) UnsubscribeMarkets(marketIDs []string) {
	u.mu.Lock()
	for _, id := range marketIDs {
		delete(u.pendingSubscribeMarketIDs, id)
		u.pendingUnsubscribeMarketIDs[id] = struct{}{}
	}
	u.mu.Unlock()
	u.flushMarketSubscriptions()
}

// ─────────────────────────────────────────────────────────────
// Ping loop
// ─────────────────────────────────────────────────────────────

func (u *WSPolymarketUserWS) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn := u.Conn()
			if conn != nil {
				conn.WriteMessage(gws.TextMessage, []byte("PING"))
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Market subscription flush
// ─────────────────────────────────────────────────────────────

func (u *WSPolymarketUserWS) flushMarketSubscriptions() {
	u.mu.Lock()
	subIDs := u.pendingSubscribeMarketIDs
	unsubIDs := u.pendingUnsubscribeMarketIDs
	u.pendingSubscribeMarketIDs = make(map[string]struct{})
	u.pendingUnsubscribeMarketIDs = make(map[string]struct{})
	u.mu.Unlock()

	conn := u.Conn()
	if conn == nil {
		u.mu.Lock()
		for id := range subIDs {
			u.pendingSubscribeMarketIDs[id] = struct{}{}
		}
		for id := range unsubIDs {
			u.pendingUnsubscribeMarketIDs[id] = struct{}{}
		}
		u.mu.Unlock()
		return
	}

	if len(unsubIDs) > 0 {
		ids := make([]string, 0, len(unsubIDs))
		for id := range unsubIDs {
			ids = append(ids, id)
		}
		conn.WriteJSON(map[string]any{
			"operation": "unsubscribe",
			"markets":   ids,
		})
		u.mu.Lock()
		for _, id := range ids {
			delete(u.subscribedMarketIDs, id)
		}
		u.mu.Unlock()
	}

	if len(subIDs) > 0 {
		ids := make([]string, 0, len(subIDs))
		for id := range subIDs {
			ids = append(ids, id)
		}
		conn.WriteJSON(map[string]any{
			"operation": "subscribe",
			"markets":   ids,
		})
		u.mu.Lock()
		for _, id := range ids {
			u.subscribedMarketIDs[id] = struct{}{}
		}
		u.mu.Unlock()
	}
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (u *WSPolymarketUserWS) onConnect(ctx context.Context, conn *gws.Conn) error {
	handshake := map[string]any{
		"type": "user",
		"auth": u.auth,
	}
	return conn.WriteJSON(handshake)
}

func (u *WSPolymarketUserWS) onDisconnect(err error) {
	if u.handlers.OnWSClose != nil {
		u.handlers.OnWSClose()
	} else {
		slog.Debug("user WS: disconnected")
	}
}

func (u *WSPolymarketUserWS) onMessage(ctx context.Context, data []byte) error {
	if string(data) == "PONG" {
		return nil
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		var single json.RawMessage
		if err2 := json.Unmarshal(data, &single); err2 != nil {
			return err2
		}
		raw = []json.RawMessage{single}
	}

	for _, r := range raw {
		var envelope struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(r, &envelope); err != nil {
			continue
		}

		switch envelope.EventType {
		case "order":
			u.handleOrder(r)
		case "trade":
			u.handleTrade(r)
		default:
			slog.Debug("user WS: unknown event type", "type", envelope.EventType)
		}
	}
	return nil
}

func (u *WSPolymarketUserWS) handleOrder(raw json.RawMessage) {
	var ev OrderWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("user WS: failed to parse order", "err", err)
		return
	}

	// Forward to the user's OnOrder handler for raw processing.
	if u.handlers.OnOrder != nil {
		u.handlers.OnOrder([]OrderWSEvent{ev})
	}

	ts := utils.SafeParseTimestamp(ev.Timestamp)
	price := safeParseFloat(ev.Price)
	size := safeParseFloat(ev.OriginalSize)

	switch ev.Type {
	case "PLACEMENT":
		u.base.DispatchEvent(&connector.OrderPlacementEvent{
			BrokerID:  ev.ID,
			MarketID:  ev.Market,
			AssetID:   ev.AssetID,
			Side:      ev.Side,
			Price:     price,
			Size:      size,
			Timestamp: ts,
		})
	case "CANCELLATION":
		u.base.DispatchEvent(&connector.OrderCancelEvent{
			BrokerID:  ev.ID,
			AssetID:   ev.AssetID,
			Timestamp: ts,
		})
	}
}

func (u *WSPolymarketUserWS) handleTrade(raw json.RawMessage) {
	var ev TradeWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("user WS: failed to parse trade", "err", err)
		return
	}

	// Forward raw event.
	if u.handlers.OnTrade != nil {
		u.handlers.OnTrade([]TradeWSEvent{ev})
	}

	ts := utils.SafeParseTimestamp(ev.Timestamp)
	price := safeParseFloat(ev.Price)
	size := safeParseFloat(ev.Size)

	u.base.DispatchEvent(&connector.OrderFillEvent{
		TradeID:   ev.ID,
		BrokerID:  ev.TakerOrderID,
		AssetID:   ev.AssetID,
		Side:      ev.Side,
		Price:     price,
		Size:      size,
		Timestamp: ts,
	})
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func (u *WSPolymarketUserWS) clearSubscriptions() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.subscribedMarketIDs = make(map[string]struct{})
	u.pendingSubscribeMarketIDs = make(map[string]struct{})
	u.pendingUnsubscribeMarketIDs = make(map[string]struct{})
}

func safeParseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	var v float64
	_, _ = fmt.Sscanf(s, "%f", &v)
	return v
}
