package polymarket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	rtdsWSSURL = "wss://ws-live-data.polymarket.com"

	// RTDS topics.
	rtdsCryptoPriceTopic    = "crypto_prices"           // Binance crypto feed
	rtdsChainlinkPriceTopic = "crypto_prices_chainlink" // Chainlink crypto feed
	rtdsEquityPriceTopic    = "equity_prices"           // Pyth equity/forex/commodity feed

	// Chainlink TWAP: the SDK-level topic is prices.crypto.chainlink.twap, but
	// the wire uses one distinct topic per window. State is keyed on the
	// SDK-level topic + window (see subsKey); incoming events carry the wire
	// topic. Subscribing is broadcast within a window — symbols are filtered
	// locally.
	rtdsChainlinkTWAPTopic   = "prices.crypto.chainlink.twap"
	rtdsChainlinkTWAP30Topic = "crypto_prices_twap_thirty"
	rtdsChainlinkTWAP60Topic = "crypto_prices_twap_sixty"

	rtdsActionSubscribe   = "subscribe"
	rtdsActionUnsubscribe = "unsubscribe"
)

// WSPolymarketRTDS manages a WebSocket connection to Polymarket's RTDS
// (Real-Time Data Service) for streaming live reference prices.
//
// Supported sources:
//   - Binance crypto prices (crypto_prices) — broadcast feed, filtered locally.
//   - Chainlink crypto prices (crypto_prices_chainlink) — per-feed filter.
//   - Equity prices (equity_prices, Pyth) — per-symbol filter.
//
// It mirrors the WSPolymarketMarket pattern: connection is deferred until the
// first subscription, subscriptions are re-sent on every (re)connect via
// OnConnect, and parsed events are dispatched through the connector as typed
// connector.CryptoPriceEvent / connector.EquityPriceEvent /
// connector.PriceSnapshotEvent values.
type WSPolymarketRTDS struct {
	*ws.BaseWebSocket

	base *connector.Connector

	subsMu sync.RWMutex
	subs   map[string]*rtdsTopicState // key = topic

	eventCh          chan []byte
	dispatcherCancel context.CancelFunc

	onStatusChange func(ws.ConnectionStatus)
}

// rtdsTopicState holds the locally-subscribed symbols for one RTDS topic.
// windowSeconds is non-zero only for the Chainlink TWAP topic, where the state
// is keyed per window (see subsKey) so 30s and 60s subscriptions coexist.
type rtdsTopicState struct {
	windowSeconds int
	symbols       map[string]struct{}
}

func newRtdsTopicState(windowSeconds int) *rtdsTopicState {
	return &rtdsTopicState{windowSeconds: windowSeconds, symbols: make(map[string]struct{})}
}

// subsKey returns the subscription-state key for a topic. The Chainlink TWAP
// topic is disambiguated by window (e.g. "prices.crypto.chainlink.twap|60");
// all other topics use the bare topic name.
func subsKey(topic string, windowSeconds int) string {
	if windowSeconds > 0 {
		return fmt.Sprintf("%s|%d", topic, windowSeconds)
	}
	return topic
}

// splitSubsKey is the inverse of subsKey.
func splitSubsKey(key string) (topic string, windowSeconds int) {
	if i := strings.LastIndex(key, "|"); i >= 0 {
		topic = key[:i]
		windowSeconds, _ = strconv.Atoi(key[i+1:])
		return topic, windowSeconds
	}
	return key, 0
}

// NewWSPolymarketRTDS creates a new RTDS WebSocket manager.
func NewWSPolymarketRTDS(base *connector.Connector) *WSPolymarketRTDS {
	r := &WSPolymarketRTDS{
		BaseWebSocket: &ws.BaseWebSocket{},
		base:          base,
		subs:          make(map[string]*rtdsTopicState),
		eventCh:       make(chan []byte, 8192),
	}

	r.ShouldConnect = func() bool {
		r.subsMu.RLock()
		defer r.subsMu.RUnlock()
		for _, st := range r.subs {
			if len(st.symbols) > 0 {
				return true
			}
		}
		return false
	}

	r.OnConnect = r.onConnect
	r.OnMessage = r.onMessage
	r.OnDisconnect = r.onDisconnect
	r.OnError = func(err error) {
		slog.Warn("rtds: error", "err", err)
	}

	return r
}

// SetOnStatusChange registers a callback that fires whenever the WebSocket
// connection status changes.
func (r *WSPolymarketRTDS) SetOnStatusChange(fn func(ws.ConnectionStatus)) {
	r.onStatusChange = fn
}

// Start sets up the event dispatcher and starts the (deferred) WebSocket
// connection. The actual connection is made once the first subscription is
// queued.
func (r *WSPolymarketRTDS) Start(ctx context.Context, wsURL string, reconnectIntervalMs int64) error {
	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	r.dispatcherCancel = dispatchCancel
	go r.eventDispatcher(dispatchCtx)

	opts := ws.DefaultWSOptions()
	opts.PingInterval = 5000 // 5s keepalive — server drops idle connections
	if reconnectIntervalMs > 0 {
		opts.ReconnectInterval = reconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultReconnectIntervalMs
	}

	return r.Connect(ctx, wsURL, opts)
}

// Stop shuts down the WebSocket, stops the dispatcher, and clears state.
func (r *WSPolymarketRTDS) Stop() {
	if r.dispatcherCancel != nil {
		r.dispatcherCancel()
	}
	r.Close()
	r.clearSymbols()
}

// ─────────────────────────────────────────────────────────────
// Public subscriptions
// ─────────────────────────────────────────────────────────────

// SubscribeCryptoPrices subscribes to real-time Binance crypto price updates
// for the given symbols (e.g. "BTCUSDT", "ethusdt"). Matching is
// case-insensitive.
//
// Note: the binance crypto_prices topic is a broadcast feed (server-side
// filters do not reliably deliver data), so we subscribe to the whole topic
// once and filter locally.
func (r *WSPolymarketRTDS) SubscribeCryptoPrices(ctx context.Context, symbols []string) {
	first, added := r.addSymbols(rtdsCryptoPriceTopic, symbols)
	if len(added) == 0 || !first {
		return
	}
	r.sendEntries(ctx, rtdsActionSubscribe, binanceEntries())
}

// UnsubscribeCryptoPrices removes the given symbols from the local filter.
// When the last symbol is removed the server-side subscription is cancelled.
func (r *WSPolymarketRTDS) UnsubscribeCryptoPrices(ctx context.Context, symbols []string) {
	last, removed := r.removeSymbols(rtdsCryptoPriceTopic, symbols)
	if len(removed) == 0 || !last {
		return
	}
	r.sendEntries(ctx, rtdsActionUnsubscribe, binanceEntries())
}

// SubscribeChainlinkPrices subscribes to real-time Chainlink crypto price
// updates for the given feeds (e.g. "eth/usd", "btc/usd"). Matching is
// case-insensitive.
func (r *WSPolymarketRTDS) SubscribeChainlinkPrices(ctx context.Context, feeds []string) {
	_, added := r.addSymbols(rtdsChainlinkPriceTopic, feeds)
	if len(added) == 0 {
		return
	}
	r.sendEntries(ctx, rtdsActionSubscribe, chainlinkEntries(added))
}

// UnsubscribeChainlinkPrices removes the given feeds from the Chainlink stream.
func (r *WSPolymarketRTDS) UnsubscribeChainlinkPrices(ctx context.Context, feeds []string) {
	_, removed := r.removeSymbols(rtdsChainlinkPriceTopic, feeds)
	if len(removed) == 0 {
		return
	}
	r.sendEntries(ctx, rtdsActionUnsubscribe, chainlinkEntries(removed))
}

// SubscribeChainlinkTWAP subscribes to Chainlink-computed TWAP prices for the
// given feeds (e.g. "btc/usd", "eth/usd") over the given lookback window (30
// or 60 seconds). Matching is case-insensitive.
//
// Note: this topic sends NO snapshot — subscriptions start with the next
// published update, so there is no replay after a disconnect.
func (r *WSPolymarketRTDS) SubscribeChainlinkTWAP(ctx context.Context, windowSeconds int, symbols []string) {
	first, added := r.addSymbolsWindowed(subsKey(rtdsChainlinkTWAPTopic, windowSeconds), windowSeconds, symbols)
	if len(added) == 0 || !first {
		return
	}
	// The wire topic encodes the window and delivers every symbol — the
	// server-side subscription is per window, not per symbol.
	r.sendEntries(ctx, rtdsActionSubscribe, chainlinkTWAPEntries(windowSeconds))
}

// UnsubscribeChainlinkTWAP removes the given feeds from the Chainlink TWAP
// stream. The server-side (broadcast) subscription is cancelled once the last
// symbol for that window is removed.
func (r *WSPolymarketRTDS) UnsubscribeChainlinkTWAP(ctx context.Context, windowSeconds int, symbols []string) {
	last, removed := r.removeSymbols(subsKey(rtdsChainlinkTWAPTopic, windowSeconds), symbols)
	if len(removed) == 0 || !last {
		return
	}
	r.sendEntries(ctx, rtdsActionUnsubscribe, chainlinkTWAPEntries(windowSeconds))
}

// SubscribeEquityPrices subscribes to real-time equity/ETF/forex/commodity
// prices (Pyth) for the given symbols (e.g. "AAPL", "TSLA", "EURUSD").
// Matching is case-insensitive.
func (r *WSPolymarketRTDS) SubscribeEquityPrices(ctx context.Context, symbols []string) {
	_, added := r.addSymbols(rtdsEquityPriceTopic, symbols)
	if len(added) == 0 {
		return
	}
	r.sendEntries(ctx, rtdsActionSubscribe, equityEntries(added))
}

// UnsubscribeEquityPrices removes the given symbols from the equity stream.
func (r *WSPolymarketRTDS) UnsubscribeEquityPrices(ctx context.Context, symbols []string) {
	_, removed := r.removeSymbols(rtdsEquityPriceTopic, symbols)
	if len(removed) == 0 {
		return
	}
	r.sendEntries(ctx, rtdsActionUnsubscribe, equityEntries(removed))
}

// ─────────────────────────────────────────────────────────────
// Subscription state
// ─────────────────────────────────────────────────────────────

func normalizeRTDSSymbol(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// addSymbols adds symbols to a topic's local filter (window 0). Returns
// whether this was the first symbol (0 → >0 transition) and which symbols were
// newly added.
func (r *WSPolymarketRTDS) addSymbols(topic string, symbols []string) (first bool, added []string) {
	return r.addSymbolsWindowed(topic, 0, symbols)
}

// addSymbolsWindowed adds symbols to a topic's local filter, recording the TWAP
// window (0 for non-TWAP topics). key is the composite subscription key (see
// subsKey). Returns whether this was the first symbol (0 → >0 transition) and
// which symbols were newly added.
func (r *WSPolymarketRTDS) addSymbolsWindowed(key string, windowSeconds int, symbols []string) (first bool, added []string) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	st := r.subs[key]
	if st == nil {
		st = newRtdsTopicState(windowSeconds)
		r.subs[key] = st
	} else {
		st.windowSeconds = windowSeconds
	}
	wasEmpty := len(st.symbols) == 0
	for _, s := range symbols {
		s = normalizeRTDSSymbol(s)
		if s == "" {
			continue
		}
		if _, ok := st.symbols[s]; ok {
			continue
		}
		st.symbols[s] = struct{}{}
		added = append(added, s)
	}
	return wasEmpty && len(st.symbols) > 0, added
}

// removeSymbols removes symbols from a topic's local filter. Returns whether
// this removed the last symbol (>0 → 0 transition) and which were removed.
func (r *WSPolymarketRTDS) removeSymbols(topic string, symbols []string) (last bool, removed []string) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	st := r.subs[topic]
	if st == nil {
		return false, nil
	}
	for _, s := range symbols {
		s = normalizeRTDSSymbol(s)
		if s == "" {
			continue
		}
		if _, ok := st.symbols[s]; !ok {
			continue
		}
		delete(st.symbols, s)
		removed = append(removed, s)
	}
	return len(removed) > 0 && len(st.symbols) == 0, removed
}

func (r *WSPolymarketRTDS) clearSymbols() {
	r.subsMu.Lock()
	r.subs = make(map[string]*rtdsTopicState)
	r.subsMu.Unlock()
}

func (r *WSPolymarketRTDS) isSubscribed(topic, symbol string) bool {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()
	st, ok := r.subs[topic]
	if !ok {
		return false
	}
	_, ok = st.symbols[normalizeRTDSSymbol(symbol)]
	return ok
}

func (r *WSPolymarketRTDS) sendEntries(ctx context.Context, action string, entries []rtdsSubscription) {
	if len(entries) == 0 {
		return
	}
	conn := r.Conn()
	if conn == nil {
		// Not connected — subscriptions are re-sent on the next connect.
		return
	}
	req := rtdsSubscriptionRequest{Action: action, Subscriptions: entries}
	data, err := json.Marshal(req)
	if err != nil {
		slog.Warn("rtds: failed to marshal subscription", "action", action, "err", err)
		return
	}
	if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
		slog.Warn("rtds: failed to send subscription", "action", action, "err", err)
	}
}

// ─────────────────────────────────────────────────────────────
// Subscription entries
// ─────────────────────────────────────────────────────────────

// binanceEntries returns the binance subscription entry (unfiltered broadcast).
func binanceEntries() []rtdsSubscription {
	return []rtdsSubscription{{
		Topic:   rtdsCryptoPriceTopic,
		MsgType: "update",
	}}
}

// chainlinkEntries returns one subscription entry per feed, each filtered by a
// JSON string {"symbol":"<feed>"}.
func chainlinkEntries(feeds []string) []rtdsSubscription {
	return filteredEntries(rtdsChainlinkPriceTopic, feeds)
}

// chainlinkTWAPEntries returns a single broadcast subscription entry for the
// given TWAP window. The wire topic encodes the window (30s or 60s); the server
// delivers every symbol on that window, so symbols are narrowed locally.
func chainlinkTWAPEntries(windowSeconds int) []rtdsSubscription {
	wire := twapWireTopic(windowSeconds)
	if wire == "" {
		return nil
	}
	return []rtdsSubscription{{Topic: wire, MsgType: "update"}}
}

// twapWireTopic maps a TWAP window to its wire topic (empty for unsupported
// windows).
func twapWireTopic(windowSeconds int) string {
	switch windowSeconds {
	case 30:
		return rtdsChainlinkTWAP30Topic
	case 60:
		return rtdsChainlinkTWAP60Topic
	}
	return ""
}

// twapWindow returns the TWAP window encoded in a wire topic (0 if not a TWAP
// wire topic).
func twapWindow(topic string) int {
	switch topic {
	case rtdsChainlinkTWAP30Topic:
		return 30
	case rtdsChainlinkTWAP60Topic:
		return 60
	}
	return 0
}

// equityEntries returns one subscription entry per symbol, each filtered by a
// JSON string {"symbol":"<symbol>"}.
func equityEntries(symbols []string) []rtdsSubscription {
	return filteredEntries(rtdsEquityPriceTopic, symbols)
}

func filteredEntries(topic string, symbols []string) []rtdsSubscription {
	entries := make([]rtdsSubscription, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeRTDSSymbol(s); s == "" {
			continue
		}
		entries = append(entries, rtdsSubscription{
			Topic:   topic,
			MsgType: "*",
			Filters: symbolFilterJSON(s),
		})
	}
	return entries
}

// symbolFilterJSON returns the JSON-string filter value the server expects,
// e.g. {"symbol":"eth/usd"} as a Go string containing JSON.
func symbolFilterJSON(symbol string) string {
	b, err := json.Marshal(map[string]string{"symbol": symbol})
	if err != nil {
		return ""
	}
	return string(b)
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (r *WSPolymarketRTDS) onConnect(ctx context.Context, conn *coderws.Conn) error {
	if r.onStatusChange != nil {
		r.onStatusChange(ws.StatusConnected)
	}

	entries := r.allEntries()
	if len(entries) == 0 {
		return nil
	}

	req := rtdsSubscriptionRequest{Action: rtdsActionSubscribe, Subscriptions: entries}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return conn.Write(ctx, coderws.MessageText, data)
}

// allEntries rebuilds the full subscribe frame from all topic states, in
// deterministic order, for resubscribing after a reconnect.
func (r *WSPolymarketRTDS) allEntries() []rtdsSubscription {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	keys := make([]string, 0, len(r.subs))
	for k := range r.subs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var entries []rtdsSubscription
	for _, key := range keys {
		st := r.subs[key]
		if len(st.symbols) == 0 {
			continue
		}
		topic, window := splitSubsKey(key)
		syms := sortedSymbols(st.symbols)
		switch topic {
		case rtdsCryptoPriceTopic:
			entries = append(entries, binanceEntries()...)
		case rtdsChainlinkTWAPTopic:
			entries = append(entries, chainlinkTWAPEntries(window)...)
		case rtdsChainlinkPriceTopic:
			entries = append(entries, chainlinkEntries(syms)...)
		case rtdsEquityPriceTopic:
			entries = append(entries, equityEntries(syms)...)
		}
	}
	return entries
}

func sortedSymbols(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (r *WSPolymarketRTDS) onDisconnect(err error) {
	if closeCode := coderws.CloseStatus(err); closeCode != -1 {
		slog.Info("rtds: disconnected", "reason", err, "close_code", closeCode)
	} else {
		slog.Info("rtds: disconnected", "reason", err)
	}

	if r.onStatusChange != nil {
		r.onStatusChange(ws.StatusDisconnected)
	}
}

func (r *WSPolymarketRTDS) onMessage(ctx context.Context, data []byte) error {
	select {
	case r.eventCh <- data:
		return nil
	default:
		slog.Warn("rtds: dropping message, event channel full")
		return nil
	}
}

// ─────────────────────────────────────────────────────────────
// Event dispatch
// ─────────────────────────────────────────────────────────────

func (r *WSPolymarketRTDS) eventDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-r.eventCh:
			r.processMessage(data)
		}
	}
}

func (r *WSPolymarketRTDS) processMessage(data []byte) {
	// The server may send empty heartbeat frames — skip them quietly.
	if len(bytes.TrimSpace(data)) == 0 {
		return
	}

	var msg rtdsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Warn("rtds: failed to parse message", "err", err)
		return
	}

	switch msg.Topic {
	case rtdsCryptoPriceTopic:
		switch msg.MsgType {
		case "update":
			r.handleCryptoPrice(msg, "binance", rtdsCryptoPriceTopic)
		case "subscribe":
			// Historical snapshot (chainlink feeds arrive here on subscribe).
			r.handlePriceSnapshot(msg, "chainlink", rtdsChainlinkPriceTopic)
		}
	case rtdsChainlinkPriceTopic:
		r.handleCryptoPrice(msg, "chainlink", rtdsChainlinkPriceTopic)
	case rtdsChainlinkTWAP30Topic, rtdsChainlinkTWAP60Topic:
		r.handleChainlinkTWAP(msg)
	case rtdsEquityPriceTopic:
		switch msg.MsgType {
		case "update":
			r.handleEquityPrice(msg)
		case "subscribe":
			r.handleEquitySnapshot(msg)
		}
	default:
		slog.Debug("rtds: ignoring topic", "topic", msg.Topic)
	}
}

func (r *WSPolymarketRTDS) handleCryptoPrice(msg rtdsMessage, source, topic string) {
	var payload rtdsCryptoPricePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		slog.Warn("rtds: failed to parse crypto price payload", "err", err)
		return
	}
	if !r.isSubscribed(topic, payload.Symbol) {
		return
	}

	// Chainlink's full_accuracy_value is a raw integer scaled by 10^18, so
	// prefer the numeric value there; binance's is a plain decimal.
	preferFullAccuracy := source != "chainlink"
	r.base.DispatchEvent(&connector.CryptoPriceEvent{
		SeqID:      r.base.NextSeqID(),
		ReceivedAt: r.base.Now(),
		Symbol:     payload.Symbol,
		Price:      rtdsPriceValue(payload.rtdsPricePayload, preferFullAccuracy),
		Timestamp:  rtdsEventTime(payload.Timestamp, msg.Timestamp),
		Source:     source,
	})
}

// handleChainlinkTWAP dispatches a Chainlink TWAP update. This topic sends NO
// historical snapshot — the first event is the next published TWAP. The wire
// topic encodes the window; value is an exact decimal string derived from
// Chainlink's E18 fixed-point price and the payload timestamp is the Chainlink
// observation time.
func (r *WSPolymarketRTDS) handleChainlinkTWAP(msg rtdsMessage) {
	window := twapWindow(msg.Topic)
	if window == 0 {
		return
	}
	var payload rtdsChainlinkTWAPPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		slog.Warn("rtds: failed to parse chainlink twap payload", "err", err)
		return
	}
	if !r.isSubscribed(subsKey(rtdsChainlinkTWAPTopic, window), payload.Symbol) {
		return
	}

	r.base.DispatchEvent(&connector.CryptoPriceEvent{
		SeqID:         r.base.NextSeqID(),
		ReceivedAt:    r.base.Now(),
		Symbol:        payload.Symbol,
		Price:         rtdsPriceValue(payload.rtdsPricePayload, false),
		Timestamp:     rtdsEventTime(payload.Timestamp, msg.Timestamp),
		Source:        "chainlink_twap",
		WindowSeconds: window,
	})
}

func (r *WSPolymarketRTDS) handleEquityPrice(msg rtdsMessage) {
	var payload rtdsEquityPricePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		slog.Warn("rtds: failed to parse equity price payload", "err", err)
		return
	}
	if !r.isSubscribed(rtdsEquityPriceTopic, payload.Symbol) {
		return
	}

	r.base.DispatchEvent(&connector.EquityPriceEvent{
		SeqID:            r.base.NextSeqID(),
		ReceivedAt:       r.base.Now(),
		Symbol:           payload.Symbol,
		Price:            rtdsPriceValue(payload.rtdsPricePayload, true),
		Timestamp:        rtdsEventTime(payload.Timestamp, msg.Timestamp),
		IsCarriedForward: payload.IsCarriedForward,
	})
}

// handlePriceSnapshot dispatches the historical snapshot for chainlink feeds.
func (r *WSPolymarketRTDS) handlePriceSnapshot(msg rtdsMessage, source, topic string) {
	var payload rtdsPriceSnapshotPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		slog.Warn("rtds: failed to parse price snapshot payload", "err", err)
		return
	}
	if !r.isSubscribed(topic, payload.Symbol) {
		return
	}

	points := make([]connector.PriceSnapshotPoint, len(payload.Data))
	for i, p := range payload.Data {
		points[i] = connector.PriceSnapshotPoint{Timestamp: p.Timestamp, Value: p.Value}
	}

	r.base.DispatchEvent(&connector.PriceSnapshotEvent{
		SeqID:      r.base.NextSeqID(),
		ReceivedAt: r.base.Now(),
		Source:     source,
		Symbol:     payload.Symbol,
		Points:     points,
		Timestamp:  time.UnixMilli(msg.Timestamp),
	})
}

// handleEquitySnapshot dispatches the historical snapshot for equity symbols.
func (r *WSPolymarketRTDS) handleEquitySnapshot(msg rtdsMessage) {
	r.handlePriceSnapshot(msg, "equity", rtdsEquityPriceTopic)
}

// rtdsEventTime prefers the payload timestamp, falling back to the envelope.
func rtdsEventTime(payloadTS, envelopeTS int64) time.Time {
	if payloadTS == 0 {
		payloadTS = envelopeTS
	}
	return time.UnixMilli(payloadTS)
}

// ─────────────────────────────────────────────────────────────
// Internal RTDS message types
// ─────────────────────────────────────────────────────────────

// rtdsSubscription describes a single RTDS subscription (topic + type).
// Filters carries the topic-specific filter value: nil for the binance
// broadcast feed, a JSON string like {"symbol":"eth/usd"} for
// chainlink/equity, or WindowSeconds+Symbols for the Chainlink TWAP topic.
type rtdsSubscription struct {
	Topic   string `json:"topic"`
	MsgType string `json:"type"`
	Filters any    `json:"filters,omitempty"`
}

// rtdsSubscriptionRequest is the top-level RTDS subscribe/unsubscribe payload.
type rtdsSubscriptionRequest struct {
	Action        string             `json:"action"`
	Subscriptions []rtdsSubscription `json:"subscriptions"`
}

// rtdsMessage is the raw RTDS message wrapper received from the stream.
type rtdsMessage struct {
	Topic     string          `json:"topic"`
	MsgType   string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// rtdsPricePayload is the shared price payload (crypto + equity).
type rtdsPricePayload struct {
	Symbol            string          `json:"symbol"`
	Timestamp         int64           `json:"timestamp"`
	Value             json.RawMessage `json:"value"`
	FullAccuracyValue string          `json:"full_accuracy_value,omitempty"`
}

// rtdsCryptoPricePayload is the payload of a crypto_prices update.
type rtdsCryptoPricePayload struct {
	rtdsPricePayload
}

// rtdsChainlinkTWAPPayload is the wire payload for a Chainlink TWAP update.
// value is an exact decimal string; timestamp is the Chainlink observation
// time. The window is also carried on the wire as window_s (informational — the
// window is already encoded in the wire topic).
type rtdsChainlinkTWAPPayload struct {
	rtdsPricePayload
	WindowSeconds int `json:"window_s,omitempty"`
}

// rtdsEquityPricePayload is the payload of an equity_prices update.
type rtdsEquityPricePayload struct {
	rtdsPricePayload
	ReceivedAt       int64 `json:"received_at,omitempty"`
	IsCarriedForward bool  `json:"is_carried_forward,omitempty"`
}

// rtdsPriceSnapshotPayload is the payload of a subscribe snapshot (chainlink
// and equity). The chainlink snapshot arrives on the crypto_prices topic.
type rtdsPriceSnapshotPayload struct {
	Symbol string                   `json:"symbol"`
	Data   []rtdsPriceSnapshotPoint `json:"data"`
}

type rtdsPriceSnapshotPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// rtdsPriceValue returns the price as a decimal string. When preferFullAccuracy
// is true the full-accuracy field is used (binance/equity — plain decimals);
// otherwise the numeric value is used (chainlink — whose full_accuracy_value is
// a raw integer scaled by 10^18).
func rtdsPriceValue(p rtdsPricePayload, preferFullAccuracy bool) string {
	if preferFullAccuracy && p.FullAccuracyValue != "" {
		return p.FullAccuracyValue
	}
	if len(p.Value) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(p.Value, &s); err == nil {
		return s
	}
	var f json.Number
	if err := json.Unmarshal(p.Value, &f); err == nil {
		return f.String()
	}
	return fmt.Sprintf("%s", p.Value)
}
