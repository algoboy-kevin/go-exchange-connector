package polymarket

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/internal/utils"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	defaultReconnectIntervalMs = 500
	defaultPendingFlushMs      = 100
	marketWSSURL               = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)

// WSPolymarketMarket manages a WebSocket connection to the Polymarket CLOB
// market data channel. It tracks subscribed assets, batches pending
// subscribe/unsubscribe operations, and routes events to connector handlers.
type WSPolymarketMarket struct {
	*ws.BaseWebSocket

	base *connector.Connector

	mu                    sync.RWMutex
	subscribedAssetIDs    map[string]struct{}
	pendingSubscribeIDs   map[string]struct{}
	pendingUnsubscribeIDs map[string]struct{}

	pendingFlushInterval time.Duration
	flushTicker          *time.Ticker
	flushCancel          context.CancelFunc

	eventCh           chan []byte
	dispatcherCancel  context.CancelFunc
	dispatcherWorkers int

	eventCount atomic.Int64

	onStatusChange func(ws.ConnectionStatus)
}

// SetOnStatusChange registers a callback that fires whenever the WebSocket
// connection status changes. Useful for tracking uptime, disconnection
// duration, or triggering reconnection logic.
func (pm *WSPolymarketMarket) SetOnStatusChange(fn func(ws.ConnectionStatus)) {
	pm.onStatusChange = fn
}

// NewWSPolymarketMarket creates a new market WebSocket manager.
func NewWSPolymarketMarket(base *connector.Connector, cfg Config) *WSPolymarketMarket {
	flushMs := cfg.PendingFlushMs
	if flushMs <= 0 {
		flushMs = defaultPendingFlushMs
	}

	pm := &WSPolymarketMarket{
		BaseWebSocket:         &ws.BaseWebSocket{},
		base:                  base,
		subscribedAssetIDs:    make(map[string]struct{}),
		pendingSubscribeIDs:   make(map[string]struct{}),
		pendingUnsubscribeIDs: make(map[string]struct{}),
		pendingFlushInterval:  time.Duration(flushMs) * time.Millisecond,
		eventCh:               make(chan []byte, 8192),
	}

	pm.ShouldConnect = func() bool {
		pm.mu.RLock()
		defer pm.mu.RUnlock()
		return len(pm.pendingSubscribeIDs) > 0 || len(pm.subscribedAssetIDs) > 0
	}

	pm.dispatcherWorkers = 4

	pm.OnConnect = pm.onConnect
	pm.OnMessage = pm.onMessage
	pm.OnDisconnect = pm.onDisconnect
	pm.OnError = func(err error) {
		slog.Warn("market WS: error", "err", err)
	}

	return pm
}

// Start sets up the reconnection loop and pending flush timer.
// Actual connection is deferred until subscriptions are queued.
func (pm *WSPolymarketMarket) Start(ctx context.Context, wsURL string, reconnectIntervalMs int64) error {
	flushCtx, flushCancel := context.WithCancel(ctx)
	pm.flushCancel = flushCancel
	pm.flushTicker = time.NewTicker(pm.pendingFlushInterval)
	go pm.flushLoop(flushCtx)

	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	pm.dispatcherCancel = dispatchCancel
	for i := 0; i < pm.dispatcherWorkers; i++ {
		go pm.eventDispatcher(dispatchCtx)
	}

	opts := ws.DefaultWSOptions()
	opts.PingInterval = 5000 // 5s keepalive — server drops idle connections
	if reconnectIntervalMs > 0 {
		opts.ReconnectInterval = reconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultReconnectIntervalMs
	}

	return pm.Connect(ctx, wsURL, opts)
}

// Stop shuts down the WebSocket, stops all timers, and clears state.
func (pm *WSPolymarketMarket) Stop() {
	if pm.dispatcherCancel != nil {
		pm.dispatcherCancel()
	}
	if pm.flushCancel != nil {
		pm.flushCancel()
	}
	if pm.flushTicker != nil {
		pm.flushTicker.Stop()
	}
	pm.Close()
	pm.clearSubscriptions()
}

// Subscribe queues asset IDs and triggers a reconnect so the full
// updated subscription list is sent via the initial handshake.
// Polymarket's market channel does not reliably support incremental
// subscribe operations — a full re-subscribe on reconnect avoids issues.
func (pm *WSPolymarketMarket) Subscribe(ctx context.Context, assetIDs []string) {
	pm.mu.Lock()
	for _, id := range assetIDs {
		delete(pm.pendingUnsubscribeIDs, id)
		if _, ok := pm.subscribedAssetIDs[id]; ok {
			continue
		}
		pm.pendingSubscribeIDs[id] = struct{}{}
	}
	needsReconnect := len(pm.pendingSubscribeIDs) > 0
	pm.mu.Unlock()

	if needsReconnect {
		pm.Disconnect()
	}
}

// Unsubscribe queues asset IDs for removal and triggers a reconnect.
func (pm *WSPolymarketMarket) Unsubscribe(ctx context.Context, assetIDs []string) {
	pm.mu.Lock()
	for _, id := range assetIDs {
		delete(pm.pendingSubscribeIDs, id)
		pm.pendingUnsubscribeIDs[id] = struct{}{}
	}
	needsReconnect := len(pm.pendingUnsubscribeIDs) > 0
	pm.mu.Unlock()

	if needsReconnect {
		pm.Disconnect()
	}
}

// ─────────────────────────────────────────────────────────────
// Flush loop
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) flushLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pm.flushTicker.C:
			pm.flushPending(ctx)
		}
	}
}

func (pm *WSPolymarketMarket) flushPending(ctx context.Context) {
	pm.mu.Lock()
	subIDs := pm.pendingSubscribeIDs
	unsubIDs := pm.pendingUnsubscribeIDs
	pm.pendingSubscribeIDs = make(map[string]struct{})
	pm.pendingUnsubscribeIDs = make(map[string]struct{})
	pm.mu.Unlock()

	conn := pm.Conn()
	if conn == nil {
		pm.mu.Lock()
		for id := range subIDs {
			pm.pendingSubscribeIDs[id] = struct{}{}
		}
		for id := range unsubIDs {
			pm.pendingUnsubscribeIDs[id] = struct{}{}
		}
		pm.mu.Unlock()
		return
	}

	flushUnsub := func(ids []string) {
		msg := unsubscribeMessage{Operation: "unsubscribe", AssetsIDs: ids}
		data, marshalErr := json.Marshal(msg)
		if marshalErr != nil {
			slog.Warn("market WS: failed to marshal unsubscribe", "err", marshalErr)
			return
		}
		if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
			slog.Warn("market WS: failed to send unsubscribe", "err", err)
			pm.mu.Lock()
			for _, id := range ids {
				pm.pendingUnsubscribeIDs[id] = struct{}{}
			}
			pm.mu.Unlock()
			return
		}
		pm.mu.Lock()
		for _, id := range ids {
			delete(pm.subscribedAssetIDs, id)
		}
		pm.mu.Unlock()
	}

	flushSub := func(ids []string) {
		msg := subscribeMessage{Operation: "subscribe", AssetsIDs: ids}
		data, marshalErr := json.Marshal(msg)
		if marshalErr != nil {
			slog.Warn("market WS: failed to marshal subscribe", "err", marshalErr)
			return
		}
		if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
			slog.Warn("market WS: failed to send subscribe", "err", err)
			pm.mu.Lock()
			for _, id := range ids {
				pm.pendingSubscribeIDs[id] = struct{}{}
			}
			pm.mu.Unlock()
			return
		}
		pm.mu.Lock()
		for _, id := range ids {
			pm.subscribedAssetIDs[id] = struct{}{}
		}
		pm.mu.Unlock()
	}

	if len(unsubIDs) > 0 {
		ids := make([]string, 0, len(unsubIDs))
		for id := range unsubIDs {
			ids = append(ids, id)
		}
		flushUnsub(ids)
	}
	if len(subIDs) > 0 {
		ids := make([]string, 0, len(subIDs))
		for id := range subIDs {
			ids = append(ids, id)
		}
		flushSub(ids)
	}
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) onConnect(ctx context.Context, conn *coderws.Conn) error {
	slog.Info("market WS: reconnected")
	if pm.onStatusChange != nil {
		pm.onStatusChange(ws.StatusConnected)
	}
	pm.mu.Lock()
	allIDs := make(map[string]struct{})
	for id := range pm.subscribedAssetIDs {
		allIDs[id] = struct{}{}
	}
	for id := range pm.pendingSubscribeIDs {
		allIDs[id] = struct{}{}
	}
	// Remove any assets that are pending unsubscribe.
	for id := range pm.pendingUnsubscribeIDs {
		delete(allIDs, id)
	}
	pm.subscribedAssetIDs = allIDs
	pm.pendingSubscribeIDs = make(map[string]struct{})
	pm.pendingUnsubscribeIDs = make(map[string]struct{})
	pm.mu.Unlock()

	if len(allIDs) == 0 {
		return nil
	}

	ids := make([]string, 0, len(allIDs))
	for id := range allIDs {
		ids = append(ids, id)
	}
	slog.Info("market WS: subscribed on reconnect", "sub_count", len(ids))

	data, err := json.Marshal(subscriptionMessage{AssetsIDs: ids, Type: "market"})
	if err != nil {
		return err
	}
	return conn.Write(ctx, coderws.MessageText, data)
}

func (pm *WSPolymarketMarket) onDisconnect(err error) {
	pm.mu.Lock()
	for id := range pm.subscribedAssetIDs {
		pm.pendingSubscribeIDs[id] = struct{}{}
	}
	pm.subscribedAssetIDs = make(map[string]struct{})
	pm.mu.Unlock()

	// Extract close info.
	closeCode := coderws.CloseStatus(err)
	if closeCode != -1 {
		slog.Info("market WS: disconnected",
			"reason", err,
			"close_code", closeCode,
		)
	} else {
		slog.Info("market WS: disconnected", "reason", err)
	}

	if pm.onStatusChange != nil {
		pm.onStatusChange(ws.StatusDisconnected)
	}
}

func (pm *WSPolymarketMarket) onMessage(ctx context.Context, data []byte) error {
	select {
	case pm.eventCh <- data:
		return nil
	default:
		slog.Warn("market WS: dropping message, event channel full")
		return nil
	}
}

// ─────────────────────────────────────────────────────────────
// Event handlers — parse Polymarket JSON → dispatch typed events
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) handlePriceChange(raw json.RawMessage) {
	var ev PriceChangeWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse price_change", "err", err)
		return
	}

	pm.mu.RLock()
	items := make([]connector.PriceChangeItem, 0, len(ev.PriceChanges))
	for _, pc := range ev.PriceChanges {
		if _, ok := pm.subscribedAssetIDs[pc.AssetID]; ok {
			items = append(items, connector.PriceChangeItem{
				AssetID: pc.AssetID,
				Price:   pc.Price,
				Size:    pc.Size,
				Side:    pc.Side,
				Hash:    pc.Hash,
				BestBid: pc.BestBid,
				BestAsk: pc.BestAsk,
			})
		}
	}
	pm.mu.RUnlock()

	if len(items) == 0 {
		return
	}

	pm.base.DispatchEvent(&connector.PriceChangeEvent{
		SeqID:      pm.base.NextSeqID(),
		ReceivedAt: pm.base.Now(),
		Market:     ev.Market,
		Timestamp:  utils.SafeParseTimestamp(ev.Timestamp),
		Changes:    items,
	})
}

func (pm *WSPolymarketMarket) handleBook(raw json.RawMessage) {
	var ev BookWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse book", "err", err)
		return
	}
	if !pm.isSubscribed(ev.AssetID) {
		return
	}

	bids := make([]connector.Level, len(ev.Bids))
	for i, l := range ev.Bids {
		bids[i] = connector.Level{Price: l.Price, Size: l.Size}
	}
	asks := make([]connector.Level, len(ev.Asks))
	for i, l := range ev.Asks {
		asks[i] = connector.Level{Price: l.Price, Size: l.Size}
	}

	pm.base.DispatchEvent(&connector.BookSnapshotEvent{
		SeqID:      pm.base.NextSeqID(),
		ReceivedAt: pm.base.Now(),
		Market:     ev.Market,
		AssetID:    ev.AssetID,
		Timestamp:  utils.SafeParseTimestamp(ev.Timestamp),
		Hash:       ev.Hash,
		Bids:       bids,
		Asks:       asks,
	})
}

func (pm *WSPolymarketMarket) handleLastTradePrice(raw json.RawMessage) {
	var ev LastTradePriceWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse last_trade_price", "err", err)
		return
	}
	if !pm.isSubscribed(ev.AssetID) {
		return
	}

	pm.base.DispatchEvent(&connector.TradeEvent{
		SeqID:      pm.base.NextSeqID(),
		ReceivedAt: pm.base.Now(),
		AssetID:    ev.AssetID,
		Side:       ev.Side,
		Size:       ev.Size,
		Price:      ev.Price,
		Timestamp:  utils.SafeParseTimestamp(ev.Timestamp),
	})
}

func (pm *WSPolymarketMarket) handleTickSizeChange(raw json.RawMessage) {
	var ev TickSizeChangeWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse tick_size_change", "err", err)
		return
	}
	if !pm.isSubscribed(ev.AssetID) {
		return
	}

	pm.base.DispatchEvent(&connector.TickChangeEvent{
		SeqID:       pm.base.NextSeqID(),
		ReceivedAt:  pm.base.Now(),
		AssetID:     ev.AssetID,
		Market:      ev.Market,
		OldTickSize: ev.OldTickSize,
		NewTickSize: ev.NewTickSize,
		Timestamp:   utils.SafeParseTimestamp(ev.Timestamp),
	})
}

func (pm *WSPolymarketMarket) handleMarketResolved(raw json.RawMessage) {
	var ev MarketResolvedWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse market_resolved", "err", err)
		return
	}

	pm.base.DispatchEvent(&connector.MarketResolvedEvent{
		SeqID:          pm.base.NextSeqID(),
		ReceivedAt:     pm.base.Now(),
		MarketID:       ev.ID,
		ConditionID:    ev.Market,
		WinningAssetID: ev.WinningAssetID,
		WinningOutcome: ev.WinningOutcome,
		Timestamp:      utils.SafeParseTimestamp(ev.Timestamp),
	})
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) isSubscribed(assetID string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	_, ok := pm.subscribedAssetIDs[assetID]
	return ok
}

func (pm *WSPolymarketMarket) clearSubscriptions() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.subscribedAssetIDs = make(map[string]struct{})
	pm.pendingSubscribeIDs = make(map[string]struct{})
	pm.pendingUnsubscribeIDs = make(map[string]struct{})
}

// eventDispatcher runs in a dedicated goroutine, pulling raw WS messages
// from the buffered channel and processing them asynchronously. This
// decouples socket reading from parsing/dispatching, preventing backpressure
// on the read loop during event bursts.
func (pm *WSPolymarketMarket) eventDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-pm.eventCh:
			pm.processMessage(data)
		}
	}
}

// processMessage parses a raw WebSocket message and dispatches typed events.
// Runs off the read-loop goroutine — safe to block on DispatchEvent.
func (pm *WSPolymarketMarket) processMessage(data []byte) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		var single json.RawMessage
		if err2 := json.Unmarshal(data, &single); err2 != nil {
			slog.Warn("market WS: failed to parse message", "err", err2)
			return
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
		case "price_change":
			pm.handlePriceChange(r)
		case "book":
			pm.handleBook(r)
		case "last_trade_price":
			pm.handleLastTradePrice(r)
		case "tick_size_change":
			pm.handleTickSizeChange(r)
		case "new_market":
			slog.Debug("market WS: new_market event ignored")
		case "market_resolved":
			pm.handleMarketResolved(r)
		default:
			slog.Debug("market WS: unknown event type", "type", envelope.EventType)
		}

		count := pm.eventCount.Add(1)
		if count%10000 == 0 {
			slog.Debug("market WS: events received", "count", count)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Internal WS message types
// ─────────────────────────────────────────────────────────────

type subscriptionMessage struct {
	AssetsIDs []string `json:"assets_ids"`
	Type      string   `json:"type"`
}

type subscribeMessage struct {
	Operation string   `json:"operation"`
	AssetsIDs []string `json:"assets_ids"`
}

type unsubscribeMessage struct {
	Operation string   `json:"operation"`
	AssetsIDs []string `json:"assets_ids"`
}
