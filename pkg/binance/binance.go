package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	spotWSSURL = "wss://stream.binance.com:9443/ws"

	// Binance split the USDⓈ-M futures market streams across two endpoints:
	// market streams (aggTrade, kline) moved to /market/ws, public streams
	// (bookTicker, depth) to /public/ws. The legacy /ws + /stream endpoints
	// still push bookTicker/depth but ack @aggTrade subscriptions while
	// delivering nothing — so perp uses two connections (see streamClass).
	perpMarketWSSURL = "wss://fstream.binance.com/market/ws"
	perpPublicWSSURL = "wss://fstream.binance.com/public/ws"

	spotRESTURL = "https://api.binance.com"
	perpRESTURL = "https://fapi.binance.com"

	defaultReconnectIntervalMs = 1000
)

// WSBinance manages raw WebSocket connections to Binance for both the spot
// and USDⓈ-M perpetual futures markets, streaming market data through the
// connector dispatcher as typed connector.Binance*Event values.
//
// Supported streams (per symbol):
//   - bookTicker — best bid/ask (reference price)
//   - aggTrade   — aggregated trades
//   - depth      — full order book, maintained locally from the diff-depth
//     stream (@depth@100ms) seeded by a REST snapshot
//   - partial depth — top-N order-book snapshots (@depth20@100ms), a passive
//     push from Binance with no local book state
//   - kline      — OHLCV candles
//
// It mirrors the WSPolymarketRTDS pattern: subscriptions are re-sent on every
// (re)connect via OnConnect, parsed events are dispatched through the
// connector, and connection is deferred until there is something to stream.
// streamClass groups streams onto a single WS connection. Spot keeps one
// connection; futures use two because Binance moved market streams
// (trades/klines) and public streams (bookTicker/depth) to different
// endpoints (/market/* and /public/* respectively).
type streamClass string

const (
	classSpot   streamClass = "spot"   // spot: all streams on /ws
	classMarket streamClass = "market" // perp: aggTrade + kline on /market/ws
	classPublic streamClass = "public" // perp: bookTicker + depth on /public/ws
)

// streamClassOf maps a stream name to the connection that carries it.
func streamClassOf(mkt MarketType, stream string) streamClass {
	if mkt == MarketSpot {
		return classSpot
	}
	if strings.HasSuffix(stream, "@bookTicker") ||
		strings.HasSuffix(stream, "@depth@100ms") ||
		isPartialDepthStream(stream) {
		return classPublic
	}
	return classMarket
}

// isPartialDepthStream reports whether s is a partial book depth stream
// (e.g. "btcusdt@depth20@100ms"), as opposed to the diff-depth stream
// ("btcusdt@depth@100ms"). Partial streams have "@depth<digits>@…".
func isPartialDepthStream(s string) bool {
	idx := strings.Index(s, "@depth")
	if idx < 0 {
		return false
	}
	after := s[idx+len("@depth"):]
	if after == "" {
		return false
	}
	return after[0] >= '0' && after[0] <= '9'
}

type WSBinance struct {
	base   *connector.Connector
	http   *http.Client
	rootCx context.Context

	conns map[MarketType]map[streamClass]*binanceConn

	booksMu sync.Mutex
	books   map[MarketType]map[string]*binanceBook

	onStatusChange func(MarketType, ws.ConnectionStatus)
}

// binanceConn is one class's WebSocket connection plus its local stream
// registry and event queue.
type binanceConn struct {
	mkt   MarketType
	class streamClass
	*ws.BaseWebSocket

	owner *WSBinance

	mu      sync.RWMutex
	streams map[string]struct{} // full stream names, e.g. "btcusdt@bookTicker"
	nextID  atomic.Int64

	eventCh        chan []byte
	dispatchCancel context.CancelFunc
}

// wsURL returns the WebSocket endpoint for this connection's market/class.
func (c *binanceConn) wsURL() string {
	if c.mkt == MarketSpot {
		return spotWSSURL
	}
	if c.class == classMarket {
		return perpMarketWSSURL
	}
	return perpPublicWSSURL
}

// New creates a Binance stream manager over a connector base.
func New(base *connector.Connector) *WSBinance {
	b := &WSBinance{
		base:  base,
		http:  &http.Client{Timeout: 10 * time.Second},
		conns: make(map[MarketType]map[streamClass]*binanceConn, 2),
		books: make(map[MarketType]map[string]*binanceBook, 2),
	}
	b.conns[MarketSpot] = map[streamClass]*binanceConn{
		classSpot: newBinanceConn(b, MarketSpot, classSpot),
	}
	b.conns[MarketPerp] = map[streamClass]*binanceConn{
		classMarket: newBinanceConn(b, MarketPerp, classMarket),
		classPublic: newBinanceConn(b, MarketPerp, classPublic),
	}
	return b
}

func newBinanceConn(owner *WSBinance, mkt MarketType, class streamClass) *binanceConn {
	c := &binanceConn{
		mkt:           mkt,
		class:         class,
		BaseWebSocket: &ws.BaseWebSocket{},
		owner:         owner,
		streams:       make(map[string]struct{}),
		eventCh:       make(chan []byte, 8192),
	}

	c.ShouldConnect = func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.streams) > 0
	}
	c.OnConnect = c.onConnect
	c.OnMessage = c.onMessage
	c.OnDisconnect = c.onDisconnect
	c.OnError = func(err error) {
		slog.Warn("binance: ws error", "market", mkt, "class", class, "err", err)
	}
	return c
}

// SetOnStatusChange registers a callback that fires whenever either
// connection's status changes.
func (b *WSBinance) SetOnStatusChange(fn func(MarketType, ws.ConnectionStatus)) {
	b.onStatusChange = fn
}

// SetDispatcher routes events dispatched by the connector (BinanceBookTickerEvent,
// BinanceAggTradeEvent, BinanceDepthEvent, BinanceKlineEvent) to the
// application handler.
func (b *WSBinance) SetDispatcher(d func(any)) {
	b.base.SetDispatcher(d)
}

// Start connects to both spot and perpetual streams and starts the event
// dispatchers. The actual connections are made immediately (mirroring RTDS);
// subscriptions queued before/after Start are re-sent on every connect.
func (b *WSBinance) Start(ctx context.Context, reconnectIntervalMs int64) error {
	b.rootCx = ctx

	for _, byClass := range b.conns {
		for _, c := range byClass {
			dispatchCtx, cancel := context.WithCancel(ctx)
			c.dispatchCancel = cancel
			go b.eventDispatcher(dispatchCtx, c)
		}
	}

	opts := ws.DefaultWSOptions()
	opts.PingInterval = 5000 // 5s keepalive — Binance drops idle connections
	// Depth diff frames (@depth@100ms) on liquid symbols routinely exceed
	// coder/websocket's 32KB default read limit, which force-closes the
	// connection with StatusMessageTooBig ("read limited at 32769 bytes").
	opts.ReadLimit = 1 << 20 // 1 MiB per message
	if reconnectIntervalMs > 0 {
		opts.ReconnectInterval = reconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultReconnectIntervalMs
	}

	for _, mkt := range []MarketType{MarketSpot, MarketPerp} {
		for _, c := range b.conns[mkt] {
			if err := c.Connect(ctx, c.wsURL(), opts); err != nil {
				return fmt.Errorf("binance %s %s: %w", mkt, c.class, err)
			}
		}
	}
	return nil
}

// Stop shuts down both connections, stops the dispatchers, and clears state.
func (b *WSBinance) Stop() {
	for _, byClass := range b.conns {
		for _, c := range byClass {
			if c.dispatchCancel != nil {
				c.dispatchCancel()
			}
			c.Close()
			c.clearStreams()
		}
	}
	b.clearBooks()
}

// ─────────────────────────────────────────────────────────────
// Public subscriptions
// ─────────────────────────────────────────────────────────────

// SubscribeBookTicker streams best bid/ask updates for the given symbols
// (e.g. "BTCUSDT"). Matching is case-insensitive.
func (b *WSBinance) SubscribeBookTicker(ctx context.Context, mkt MarketType, symbols []string) {
	b.subscribe(ctx, mkt, buildStreams(symbols, streamBookTicker))
}

// UnsubscribeBookTicker stops the bookTicker stream for the given symbols.
func (b *WSBinance) UnsubscribeBookTicker(ctx context.Context, mkt MarketType, symbols []string) {
	b.unsubscribe(ctx, mkt, buildStreams(symbols, streamBookTicker))
}

// SubscribeTrades streams aggregated trades for the given symbols.
func (b *WSBinance) SubscribeTrades(ctx context.Context, mkt MarketType, symbols []string) {
	b.subscribe(ctx, mkt, buildStreams(symbols, streamAggTrade))
}

// UnsubscribeTrades stops the aggTrade stream for the given symbols.
func (b *WSBinance) UnsubscribeTrades(ctx context.Context, mkt MarketType, symbols []string) {
	b.unsubscribe(ctx, mkt, buildStreams(symbols, streamAggTrade))
}

// SubscribeDepth streams a locally-maintained full order book for the given
// symbols. The book is seeded with a REST snapshot on every (re)connect and
// updated with @depth@100ms diff events.
func (b *WSBinance) SubscribeDepth(ctx context.Context, mkt MarketType, symbols []string) {
	streams := buildStreams(symbols, streamDepth)
	if len(streams) == 0 {
		return
	}
	c := b.connFor(mkt, streamClassOf(mkt, streams[0]))
	if c == nil {
		return
	}
	_, added := c.addStreams(streams)
	for _, s := range streams {
		b.bookFor(mkt, strings.TrimSuffix(s, "@depth@100ms"))
	}
	if len(added) > 0 {
		c.send(ctx, binanceMethodSubscribe, added)
	}
}

// UnsubscribeDepth stops the depth stream and discards the local book for the
// given symbols.
func (b *WSBinance) UnsubscribeDepth(ctx context.Context, mkt MarketType, symbols []string) {
	streams := buildStreams(symbols, streamDepth)
	if len(streams) == 0 {
		return
	}
	c := b.connFor(mkt, streamClassOf(mkt, streams[0]))
	if c == nil {
		return
	}
	_, removed := c.removeStreams(streams)
	for _, s := range streams {
		symbol := strings.TrimSuffix(s, "@depth@100ms")
		b.booksMu.Lock()
		if m := b.books[mkt]; m != nil {
			delete(m, symbol)
		}
		b.booksMu.Unlock()
	}
	if len(removed) > 0 {
		c.send(ctx, binanceMethodUnsubscribe, removed)
	}
}

// SubscribePartialDepth streams top-N order-book snapshots for the given
// symbols at the given update speed. levels must be one of Binance's
// supported partial depth levels (5, 10 or 20); speed is "100ms", "500ms"
// or "1000ms". Unlike SubscribeDepth, partial depth is a passive snapshot
// pushed by Binance — no REST seeding and no locally-maintained book, so
// memory cost is negligible (top-N only).
func (b *WSBinance) SubscribePartialDepth(ctx context.Context, mkt MarketType, symbols []string, levels int, speed string) {
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeSymbol(s); s == "" {
			continue
		}
		streams = append(streams, partialDepthStream(s, levels, speed))
	}
	b.subscribe(ctx, mkt, streams)
}

// UnsubscribePartialDepth stops the partial book depth stream for the given
// symbols.
func (b *WSBinance) UnsubscribePartialDepth(ctx context.Context, mkt MarketType, symbols []string, levels int, speed string) {
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeSymbol(s); s == "" {
			continue
		}
		streams = append(streams, partialDepthStream(s, levels, speed))
	}
	b.unsubscribe(ctx, mkt, streams)
}

// interval (e.g. "1m", "15m", "1h"). Matching is case-insensitive.
func (b *WSBinance) SubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) {
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeSymbol(s); s == "" {
			continue
		}
		streams = append(streams, klineStream(s, interval))
	}
	b.subscribe(ctx, mkt, streams)
}

// UnsubscribeKlines stops the kline stream for the given symbols/interval.
func (b *WSBinance) UnsubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) {
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeSymbol(s); s == "" {
			continue
		}
		streams = append(streams, klineStream(s, interval))
	}
	b.unsubscribe(ctx, mkt, streams)
}

// subscribe adds streams to their class's connection and sends a SUBSCRIBE
// frame if any were newly added. Streams are routed to the right connection
// by stream class (spot single conn; perp market/public split).
func (b *WSBinance) subscribe(ctx context.Context, mkt MarketType, streams []string) {
	for class, group := range groupStreams(mkt, streams) {
		c := b.connFor(mkt, class)
		if c == nil {
			continue
		}
		_, added := c.addStreams(group)
		if len(added) > 0 {
			c.send(ctx, binanceMethodSubscribe, added)
		}
	}
}

func (b *WSBinance) unsubscribe(ctx context.Context, mkt MarketType, streams []string) {
	for class, group := range groupStreams(mkt, streams) {
		c := b.connFor(mkt, class)
		if c == nil {
			continue
		}
		_, removed := c.removeStreams(group)
		if len(removed) > 0 {
			c.send(ctx, binanceMethodUnsubscribe, removed)
		}
	}
}

// groupStreams buckets stream names by the connection that carries them.
func groupStreams(mkt MarketType, streams []string) map[streamClass][]string {
	groups := make(map[streamClass][]string)
	for _, s := range streams {
		class := streamClassOf(mkt, s)
		groups[class] = append(groups[class], s)
	}
	return groups
}

func (b *WSBinance) connFor(mkt MarketType, class streamClass) *binanceConn {
	if !mkt.valid() {
		slog.Warn("binance: invalid market type", "market", mkt)
		return nil
	}
	return b.conns[mkt][class]
}

func buildStreams(symbols []string, st streamType) []string {
	streams := make([]string, 0, len(symbols))
	for _, s := range symbols {
		if s = normalizeSymbol(s); s == "" {
			continue
		}
		streams = append(streams, streamFor(s, st))
	}
	return streams
}

// ─────────────────────────────────────────────────────────────
// Subscription state
// ─────────────────────────────────────────────────────────────

func (c *binanceConn) addStreams(streams []string) (first bool, added []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wasEmpty := len(c.streams) == 0
	for _, s := range streams {
		if s == "" {
			continue
		}
		if _, ok := c.streams[s]; ok {
			continue
		}
		c.streams[s] = struct{}{}
		added = append(added, s)
	}
	return wasEmpty && len(c.streams) > 0, added
}

func (c *binanceConn) removeStreams(streams []string) (last bool, removed []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range streams {
		if _, ok := c.streams[s]; !ok {
			continue
		}
		delete(c.streams, s)
		removed = append(removed, s)
	}
	return len(removed) > 0 && len(c.streams) == 0, removed
}

func (c *binanceConn) clearStreams() {
	c.mu.Lock()
	c.streams = make(map[string]struct{})
	c.mu.Unlock()
}

func (c *binanceConn) isSubscribed(stream string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.streams[stream]
	return ok
}

// subscribedPartialDepth returns the partial-depth stream name subscribed
// for symbol (e.g. "btcusdt@depth20@100ms"), or "" if none.
func (c *binanceConn) subscribedPartialDepth(symbol string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for s := range c.streams {
		if strings.HasPrefix(s, symbol+"@depth") && isPartialDepthStream(s) {
			return s
		}
	}
	return ""
}

func (c *binanceConn) allStreams() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]string, 0, len(c.streams))
	for k := range c.streams {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// send writes a subscribe/unsubscribe frame if connected. When not connected
// the request is dropped — subscriptions are re-sent on the next connect.
func (c *binanceConn) send(ctx context.Context, method string, streams []string) {
	if len(streams) == 0 {
		return
	}
	conn := c.Conn()
	if conn == nil {
		slog.Info("binance: send DROPPED (conn nil)", "market", c.mkt, "method", method, "streams", streams)
		return
	}
	if err := c.writeSubscribe(ctx, conn, method, streams); err != nil {
		slog.Warn("binance: failed to send subscription", "market", c.mkt, "method", method, "err", err)
	}
}

func (c *binanceConn) writeSubscribe(ctx context.Context, conn *coderws.Conn, method string, streams []string) error {
	slog.Info("binance: writeSubscribe", "market", c.mkt, "method", method, "streams", streams)
	req := binanceSubRequest{Method: method, Params: streams, ID: c.nextID.Add(1)}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	return conn.Write(ctx, coderws.MessageText, data)
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (c *binanceConn) onConnect(ctx context.Context, conn *coderws.Conn) error {
	if c.owner.onStatusChange != nil {
		c.owner.onStatusChange(c.mkt, ws.StatusConnected)
	}

	// Re-send every active subscription.
	if streams := c.allStreams(); len(streams) > 0 {
		slog.Info("binance: onConnect re-sending", "market", c.mkt, "streams", streams)
		if err := c.writeSubscribe(ctx, conn, binanceMethodSubscribe, streams); err != nil {
			return err
		}
	} else {
		slog.Info("binance: onConnect no streams", "market", c.mkt)
	}

	// Seed depth books from REST snapshots on the connection that carries
	// the depth stream (spot single conn; perp public conn). Failures are
	// logged, not fatal — bookTicker/trade/kline streams keep working.
	if c.class == classSpot || c.class == classPublic {
		if err := c.owner.snapshotDepthBooks(ctx, c.mkt); err != nil {
			slog.Warn("binance: depth snapshot failed", "market", c.mkt, "err", err)
		}
	}
	return nil
}

func (c *binanceConn) onDisconnect(err error) {
	if closeCode := coderws.CloseStatus(err); closeCode != -1 {
		slog.Info("binance: disconnected", "market", c.mkt, "reason", err, "close_code", closeCode)
	} else {
		slog.Info("binance: disconnected", "market", c.mkt, "reason", err)
	}

	// Drop stale buffered events and reset depth books; the next connect
	// re-snapshots them.
	for {
		select {
		case <-c.eventCh:
		default:
			goto drained
		}
	}
drained:
	c.owner.resetDepthBooks(c.mkt)

	if c.owner.onStatusChange != nil {
		c.owner.onStatusChange(c.mkt, ws.StatusDisconnected)
	}
}

func (c *binanceConn) onMessage(ctx context.Context, data []byte) error {
	select {
	case c.eventCh <- data:
		return nil
	default:
		slog.Warn("binance: dropping message, event channel full", "market", c.mkt)
		return nil
	}
}

// ─────────────────────────────────────────────────────────────
// Event dispatch
// ─────────────────────────────────────────────────────────────

func (b *WSBinance) eventDispatcher(ctx context.Context, c *binanceConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-c.eventCh:
			b.processMessage(c, data)
		}
	}
}

func (b *WSBinance) processMessage(c *binanceConn, data []byte) {
	if len(bytes.TrimSpace(data)) == 0 {
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		slog.Warn("binance: failed to parse message", "market", c.mkt, "err", err)
		return
	}

	// Subscribe/unsubscribe acknowledgement — ignore (log errors).
	if _, hasID := raw["id"]; hasID {
		var ack binanceSubAck
		if err := json.Unmarshal(data, &ack); err == nil && ack.Error != nil {
			slog.Warn("binance: subscribe error", "market", c.mkt, "err", ack.Error)
		}
		return
	}

	switch jsonString(raw["e"]) {
	case "bookTicker":
		b.handleBookTicker(c, data)
	case "aggTrade":
		b.handleAggTrade(c, data)
	case "kline":
		b.handleKline(c, data)
	case "depthUpdate":
		b.handleDepth(c, data)
	case "":
		// Spot bookTicker has no event-type field; partial-depth frames
		// (unused) are ignored.
		b.handleSpotBookTicker(c, data, raw)
	default:
		slog.Debug("binance: ignoring event", "market", c.mkt, "e", jsonString(raw["e"]))
	}
}

func jsonString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// binanceEventTime prefers the event time, falling back to the transaction
// time, then to the local receive time when neither is present (e.g. spot
// bookTicker frames carry no timestamp fields).
func binanceEventTime(eventMs, txMs int64, fallback time.Time) time.Time {
	if eventMs != 0 {
		return time.UnixMilli(eventMs)
	}
	if txMs != 0 {
		return time.UnixMilli(txMs)
	}
	return fallback
}

// ── BookTicker ──────────────────────────────────────────────

type binanceBookTicker struct {
	Event        string `json:"e"`
	UpdateID     int64  `json:"u"`
	EventTime    int64  `json:"E"`
	TransactTime int64  `json:"T"`
	Symbol       string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

func (b *WSBinance) handleBookTicker(c *binanceConn, data []byte) {
	var ev binanceBookTicker
	if err := json.Unmarshal(data, &ev); err != nil {
		slog.Warn("binance: failed to parse bookTicker", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(streamFor(symbol, streamBookTicker)) {
		return
	}
	b.base.DispatchEvent(&connector.BinanceBookTickerEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		Symbol:       ev.Symbol,
		Market:       string(c.mkt),
		UpdateID:     ev.UpdateID,
		BestBidPrice: ev.BestBidPrice,
		BestBidQty:   ev.BestBidQty,
		BestAskPrice: ev.BestAskPrice,
		BestAskQty:   ev.BestAskQty,
		Timestamp:    binanceEventTime(ev.EventTime, ev.TransactTime, b.base.Now()),
	})
}

// handleSpotBookTicker parses the spot bookTicker frame, which has no event
// type field ("e") — it is identified by the u/s/b/B/a/A string fields.
func (b *WSBinance) handleSpotBookTicker(c *binanceConn, data []byte, raw map[string]json.RawMessage) {
	if _, ok := raw["u"]; !ok {
		return // partial-depth or other frameless message
	}
	b.handleBookTicker(c, data)
}

// ── AggTrade ────────────────────────────────────────────────

type binanceAggTrade struct {
	Event        string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

func (b *WSBinance) handleAggTrade(c *binanceConn, data []byte) {
	var ev binanceAggTrade
	if err := json.Unmarshal(data, &ev); err != nil {
		slog.Warn("binance: failed to parse aggTrade", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(streamFor(symbol, streamAggTrade)) {
		slog.Info("binance: aggTrade dropped, not subscribed", "market", c.mkt, "symbol", ev.Symbol, "stream", streamFor(symbol, streamAggTrade))
		return
	}

	b.base.DispatchEvent(&connector.BinanceAggTradeEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		Symbol:       ev.Symbol,
		Market:       string(c.mkt),
		TradeID:      ev.AggTradeID,
		Price:        ev.Price,
		Quantity:     ev.Quantity,
		FirstTradeID: ev.FirstTradeID,
		LastTradeID:  ev.LastTradeID,
		IsBuyerMaker: ev.IsBuyerMaker,
		Timestamp:    binanceEventTime(ev.EventTime, ev.TradeTime, b.base.Now()),
	})
}

// ── Kline ───────────────────────────────────────────────────

type binanceKlineMsg struct {
	Event     string       `json:"e"`
	EventTime int64        `json:"E"`
	Symbol    string       `json:"s"`
	Kline     binanceKline `json:"k"`
}

type binanceKline struct {
	OpenTime     int64  `json:"t"`
	CloseTime    int64  `json:"T"`
	Symbol       string `json:"s"`
	Interval     string `json:"i"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"L"`
	Open         string `json:"o"`
	Close        string `json:"c"`
	High         string `json:"h"`
	Low          string `json:"l"`
	Volume       string `json:"v"`
	TradeNum     int64  `json:"n"`
	IsFinal      bool   `json:"x"`
	QuoteVolume  string `json:"q"`
	// Consumed explicitly: JSON keys V/Q would otherwise case-insensitively
	// collide with Volume/QuoteVolume (tags v/q) and corrupt both fields.
	TakerBuyVolume      string `json:"V"`
	TakerBuyQuoteVolume string `json:"Q"`
	Ignore              string `json:"B"`
}

func (b *WSBinance) handleKline(c *binanceConn, data []byte) {
	var ev binanceKlineMsg
	if err := json.Unmarshal(data, &ev); err != nil {
		slog.Warn("binance: failed to parse kline", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(klineStream(symbol, ev.Kline.Interval)) {
		return
	}
	b.base.DispatchEvent(&connector.BinanceKlineEvent{
		SeqID:       b.base.NextSeqID(),
		ReceivedAt:  b.base.Now(),
		Symbol:      ev.Symbol,
		Market:      string(c.mkt),
		Interval:    ev.Kline.Interval,
		Open:        ev.Kline.Open,
		High:        ev.Kline.High,
		Low:         ev.Kline.Low,
		Close:       ev.Kline.Close,
		Volume:      ev.Kline.Volume,
		QuoteVolume: ev.Kline.QuoteVolume,
		IsFinal:     ev.Kline.IsFinal,
		OpenTime:    time.UnixMilli(ev.Kline.OpenTime),
		CloseTime:   time.UnixMilli(ev.Kline.CloseTime),
		Timestamp:   binanceEventTime(ev.EventTime, 0, b.base.Now()),
	})
}

// ── Depth (local order book) ────────────────────────────────

type binanceDepthUpdate struct {
	Event         string          `json:"e"`
	EventTime     int64           `json:"E"`
	Symbol        string          `json:"s"`
	FirstUpdateID int64           `json:"U"`
	FinalUpdateID int64           `json:"u"`
	BidsRaw       json.RawMessage `json:"b"`
	AsksRaw       json.RawMessage `json:"a"`
}

func (b *WSBinance) handleDepth(c *binanceConn, data []byte) {
	var upd binanceDepthUpdate
	if err := json.Unmarshal(data, &upd); err != nil {
		slog.Warn("binance: failed to parse depth update", "err", err)
		return
	}
	symbol := normalizeSymbol(upd.Symbol)

	// Partial book depth frames (e.g. @depth20@100ms) are full top-N
	// snapshots — dispatch directly, no local book / REST seeding.
	if c.subscribedPartialDepth(symbol) != "" {
		b.handlePartialDepth(c, &upd, symbol)
		return
	}
	if !c.isSubscribed(streamFor(symbol, streamDepth)) {
		return
	}

	bk := b.bookFor(c.mkt, symbol)
	bk.mu.Lock()

	if !bk.snapshotted {
		// Book not seeded yet — trigger a snapshot and drop this diff.
		bk.mu.Unlock()
		go b.snapshotDepthBook(b.rootCx, c.mkt, symbol)
		return
	}

	if upd.FinalUpdateID <= bk.lastUpdateID {
		// Stale — already applied via a later update.
		bk.mu.Unlock()
		return
	}
	if upd.FirstUpdateID > bk.lastUpdateID+1 {
		// Gap detected — re-seed the book from a fresh snapshot.
		bk.snapshotted = false
		bk.mu.Unlock()
		go b.snapshotDepthBook(b.rootCx, c.mkt, symbol)
		return
	}

	applyLevels(bk.bids, parseBinanceLevels(upd.BidsRaw))
	applyLevels(bk.asks, parseBinanceLevels(upd.AsksRaw))
	bk.lastUpdateID = upd.FinalUpdateID

	bids := bookLevels(bk.bids, false)
	asks := bookLevels(bk.asks, true)
	lastID := bk.lastUpdateID
	bk.mu.Unlock()

	b.base.DispatchEvent(&connector.BinanceDepthEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		Symbol:       upd.Symbol,
		Market:       string(c.mkt),
		LastUpdateID: lastID,
		Bids:         bids,
		Asks:         asks,
		Timestamp:    binanceEventTime(upd.EventTime, 0, b.base.Now()),
	})
}

// handlePartialDepth dispatches a partial book depth snapshot. The payload
// carries only the top-N levels, already sorted by Binance.
func (b *WSBinance) handlePartialDepth(c *binanceConn, upd *binanceDepthUpdate, symbol string) {
	bids := make(map[string]string)
	asks := make(map[string]string)
	applyLevels(bids, parseBinanceLevels(upd.BidsRaw))
	applyLevels(asks, parseBinanceLevels(upd.AsksRaw))
	b.base.DispatchEvent(&connector.BinanceDepthEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		Symbol:       upd.Symbol,
		Market:       string(c.mkt),
		LastUpdateID: upd.FinalUpdateID,
		Bids:         bookLevels(bids, false),
		Asks:         bookLevels(asks, true),
		Timestamp:    binanceEventTime(upd.EventTime, 0, b.base.Now()),
	})
}

// ─────────────────────────────────────────────────────────────
// Order book maintenance
// ─────────────────────────────────────────────────────────────

// binanceBook is the locally-maintained order book for one symbol.
type binanceBook struct {
	mu           sync.Mutex
	snapshotted  bool
	lastUpdateID int64
	bids         map[string]string // price → qty
	asks         map[string]string
}

func (b *WSBinance) bookFor(mkt MarketType, symbol string) *binanceBook {
	b.booksMu.Lock()
	defer b.booksMu.Unlock()
	m := b.books[mkt]
	if m == nil {
		m = make(map[string]*binanceBook)
		b.books[mkt] = m
	}
	bk := m[symbol]
	if bk == nil {
		bk = &binanceBook{
			bids: make(map[string]string),
			asks: make(map[string]string),
		}
		m[symbol] = bk
	}
	return bk
}

func (b *WSBinance) resetDepthBooks(mkt MarketType) {
	b.booksMu.Lock()
	defer b.booksMu.Unlock()
	if m := b.books[mkt]; m != nil {
		for _, bk := range m {
			bk.mu.Lock()
			bk.snapshotted = false
			bk.mu.Unlock()
		}
	}
}

func (b *WSBinance) clearBooks() {
	b.booksMu.Lock()
	b.books = make(map[MarketType]map[string]*binanceBook, 2)
	b.booksMu.Unlock()
}

// depthRESTPath returns the REST depth endpoint path for a market.
func depthRESTPath(mkt MarketType) string {
	if mkt == MarketPerp {
		return "/fapi/v1/depth"
	}
	return "/api/v3/depth"
}

type binanceDepthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// snapshotDepthBooks seeds a fresh order book for every subscribed depth
// symbol on a market. Used on (re)connect.
func (b *WSBinance) snapshotDepthBooks(ctx context.Context, mkt MarketType) error {
	var symbols []string
	class := classSpot
	if mkt != MarketSpot {
		class = classPublic
	}
	c := b.connFor(mkt, class)
	if c == nil {
		return nil
	}
	c.mu.RLock()
	for s := range c.streams {
		if strings.HasSuffix(s, "@depth@100ms") {
			symbols = append(symbols, strings.TrimSuffix(s, "@depth@100ms"))
		}
	}
	c.mu.RUnlock()

	var firstErr error
	for _, symbol := range symbols {
		if err := b.snapshotDepthBook(ctx, mkt, symbol); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// snapshotDepthBook fetches a REST depth snapshot and replaces the local book.
// The book mutex is held across the fetch so diff application is serialized
// with the snapshot.
func (b *WSBinance) snapshotDepthBook(ctx context.Context, mkt MarketType, symbol string) error {
	bk := b.bookFor(mkt, symbol)
	bk.mu.Lock()
	defer bk.mu.Unlock()

	// limit=50 keeps the REST snapshot small (lower weight + smaller payload),
	// which eases rate-limit pressure; deeper levels still arrive via diffs.
	url := fmt.Sprintf("%s%s?symbol=%s&limit=50", mkt.restURL(), depthRESTPath(mkt), strings.ToUpper(symbol))
	resp, err := b.http.Get(url)
	if err != nil {
		return fmt.Errorf("binance %s depth snapshot %s: %w", mkt, symbol, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("binance %s depth snapshot %s: status %d", mkt, symbol, resp.StatusCode)
	}
	var snap binanceDepthSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return fmt.Errorf("binance %s depth snapshot %s: decode: %w", mkt, symbol, err)
	}

	bk.bids = levelMap(snap.Bids)
	bk.asks = levelMap(snap.Asks)
	bk.lastUpdateID = snap.LastUpdateID
	bk.snapshotted = true

	b.base.DispatchEvent(&connector.BinanceDepthEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		Symbol:       strings.ToUpper(symbol),
		Market:       string(mkt),
		LastUpdateID: bk.lastUpdateID,
		Bids:         bookLevels(bk.bids, false),
		Asks:         bookLevels(bk.asks, true),
		Timestamp:    b.base.Now(),
	})
	return nil
}

func levelMap(pairs [][]string) map[string]string {
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		if len(p) < 2 {
			continue
		}
		if p[1] != "0" {
			m[p[0]] = p[1]
		}
	}
	return m
}

func applyLevels(m map[string]string, levels []binanceLevel) {
	for _, l := range levels {
		if l.Qty == "0" || l.Qty == "" {
			delete(m, l.Price)
		} else {
			m[l.Price] = l.Qty
		}
	}
}

// bookLevels renders a book map as sorted levels — bids descending (best
// first), asks ascending (best first). Prices are sorted numerically so the
// best level is truly best regardless of decimal length.
func bookLevels(m map[string]string, ascending bool) []connector.Level {
	levels := make([]connector.Level, 0, len(m))
	for price, qty := range m {
		levels = append(levels, connector.Level{Price: price, Size: qty})
	}
	sort.Slice(levels, func(i, j int) bool {
		cmp := comparePrice(levels[i].Price, levels[j].Price)
		if ascending {
			return cmp < 0
		}
		return cmp > 0
	})
	return levels
}

// comparePrice compares two decimal price strings numerically, falling back to
// string comparison when either fails to parse.
func comparePrice(a, b string) int {
	af, aerr := strconv.ParseFloat(a, 64)
	bf, berr := strconv.ParseFloat(b, 64)
	if aerr == nil && berr == nil {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(a, b)
}
