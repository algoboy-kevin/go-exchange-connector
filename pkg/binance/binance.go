package binance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/algoboy-kevin/go-exchange-connector/internal/framestat"
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

	// defaultEventQueueSize is the depth of the per-connection decoded-frame
	// queue (read loop -> dispatcher) when Options.EventQueueSize is unset.
	defaultEventQueueSize = 8192
)

// Options overrides the Binance endpoints a WSBinance connects to. The zero
// value keeps the production defaults; every field is optional. It exists so
// market data can be pointed at the futures testnet (or a local test server)
// without changing New's signature.
type Options struct {
	// PerpMarketWS is the USDⓈ-M market-stream endpoint that carries
	// aggTrade, kline and markPrice. Default: wss://fstream.binance.com/market/ws
	// (testnet: wss://demo-fstream.binance.com/market/ws).
	PerpMarketWS string
	// PerpPublicWS is the USDⓈ-M public-stream endpoint that carries
	// bookTicker and depth. Default: wss://fstream.binance.com/public/ws
	// (testnet: wss://demo-fstream.binance.com/public/ws).
	PerpPublicWS string
	// PerpREST is the USDⓈ-M REST base used for depth snapshots.
	// Default: https://fapi.binance.com (testnet: https://demo-fapi.binance.com).
	PerpREST string
	// EventQueueSize is the depth of each connection's decoded-frame queue
	// (read loop -> dispatcher). A frame the queue cannot hold is dropped and
	// counted (see Stats / SetOnFrameDropped). Zero or negative selects the
	// default 8192; a negative value is a caller bug and falls back to the
	// default rather than disabling the stream.
	EventQueueSize int

	// Logger, if non-nil, receives every log line this manager and its
	// connections write (connect/disconnect, subscription writes, decode
	// failures, queue-full drops, the websocket layer's reconnect lines).
	// Leave it nil — the default — to log to slog.Default(), resolved at each
	// call so a process that swaps the default keeps working.
	//
	// Use it to tag one instance, e.g.
	// opts.Logger = slog.Default().With("venue", "binance", "role", "market").
	Logger *slog.Logger

	// DepthReseedMinInterval is the floor between two GAP-TRIGGERED depth
	// re-seeds of the same symbol (a diff whose FirstUpdateID skips past the
	// local book, or a diff for a book that is not seeded yet). Zero selects
	// 1 s. It does not apply to the per-connect initial seeding, which still
	// runs once per subscribed symbol per connection.
	DepthReseedMinInterval time.Duration
	// DepthReseedPerMin caps gap-triggered depth re-seeds per minute, shared by
	// ALL symbols and both market types of this manager (not per symbol). Zero
	// selects 300. Set it to floor(IP weight budget / weight of one depth
	// request): a spot or USDⓈ-M depth?limit=50 snapshot costs weight 2, so 300
	// spends 600 of the 2,400/min IP budget and leaves headroom for the rest of
	// the process's REST traffic on that IP. Several managers on one IP share
	// that budget between them; each instance enforces only its own share.
	DepthReseedPerMin int
}

// resolved fills in the production defaults for any empty field.
func (o Options) resolved() Options {
	if o.PerpMarketWS == "" {
		o.PerpMarketWS = perpMarketWSSURL
	}
	if o.PerpPublicWS == "" {
		o.PerpPublicWS = perpPublicWSSURL
	}
	if o.PerpREST == "" {
		o.PerpREST = perpRESTURL
	}
	if o.EventQueueSize <= 0 {
		o.EventQueueSize = defaultEventQueueSize
	}
	if o.DepthReseedMinInterval <= 0 {
		o.DepthReseedMinInterval = defaultDepthReseedMinInterval
	}
	if o.DepthReseedPerMin <= 0 {
		o.DepthReseedPerMin = defaultDepthReseedPerMin
	}
	return o
}

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
	opts   Options

	conns map[MarketType]map[streamClass]*binanceConn

	booksMu sync.Mutex
	books   map[MarketType]map[string]*binanceBook

	onStatusChange func(MarketType, ws.ConnectionStatus)

	// onDisconnectHook receives the error that ended a connection (nil for a
	// deliberate Disconnect()), tagged with the market whose connection
	// dropped. See SetOnDisconnect.
	onDisconnectHook func(MarketType, error)

	// connSeq hands out connection generations. It is shared by every
	// connection this manager owns (spot, perp market, perp public), so a
	// ConnID names exactly one connection instance across the whole manager —
	// the two perp stream classes never share a generation number.
	connSeq atomic.Uint64

	// reseed gates GAP-TRIGGERED depth re-seeds (single-flight per book, a
	// per-symbol minimum interval, a shared per-minute budget, a 429/418 pause);
	// see reseed.go. The per-connect initial seeding does not go through it.
	reseed *depthReseedGate
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

	// fa is the frame accounting (FrameID source, exact read/drop/drain
	// counters, queue depth/high-water, connection generation, drop hook).
	fa framestat.Accounting

	eventCh        chan binanceFrame
	dispatchCancel context.CancelFunc
}

// binanceFrame is a raw venue frame plus its provenance, stamped on the read
// loop before the queue so a frame that is later dropped still consumed a
// FrameID and a read.
type binanceFrame struct {
	data   []byte
	id     uint64    // FrameID (never reset across reconnects)
	connID uint64    // connection generation the frame arrived on
	rx     time.Time // read time; reported as DroppedFrame.At
}

// wsURL returns the WebSocket endpoint for this connection's market/class.
func (c *binanceConn) wsURL() string {
	if c.mkt == MarketSpot {
		return spotWSSURL
	}
	if c.class == classMarket {
		return c.owner.opts.PerpMarketWS
	}
	return c.owner.opts.PerpPublicWS
}

// restURL returns the REST base for a market, honouring the Options override
// for USDⓈ-M futures (used to fetch depth snapshots).
func (b *WSBinance) restURL(mkt MarketType) string {
	if mkt == MarketPerp {
		return b.opts.PerpREST
	}
	return mkt.restURL()
}

// New creates a Binance stream manager over a connector base using the
// production endpoints.
func New(base *connector.Connector) *WSBinance {
	return NewWithOptions(base, Options{})
}

// NewWithOptions creates a Binance stream manager over a connector base with
// overridable USDⓈ-M endpoints (e.g. the futures testnet). Empty Options
// fields fall back to the production defaults, so NewWithOptions(base,
// Options{}) is exactly New(base).
func NewWithOptions(base *connector.Connector, opts Options) *WSBinance {
	b := &WSBinance{
		base:   base,
		http:   &http.Client{Timeout: 10 * time.Second},
		opts:   opts.resolved(),
		conns:  make(map[MarketType]map[streamClass]*binanceConn, 2),
		books:  make(map[MarketType]map[string]*binanceBook, 2),
		reseed: newDepthReseedGate(opts.DepthReseedMinInterval, opts.DepthReseedPerMin),
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

// log returns the logger this manager logs through: Options.Logger when it was
// set, otherwise slog.Default() evaluated at CALL time, so a process that swaps
// the default logger keeps working. Every connection (binanceConn) logs through
// the same logger, which Start also hands to the websocket layer.
func (b *WSBinance) log() *slog.Logger {
	if b.opts.Logger != nil {
		return b.opts.Logger
	}
	return slog.Default()
}

func newBinanceConn(owner *WSBinance, mkt MarketType, class streamClass) *binanceConn {
	c := &binanceConn{
		mkt:           mkt,
		class:         class,
		BaseWebSocket: &ws.BaseWebSocket{},
		owner:         owner,
		streams:       make(map[string]struct{}),
		eventCh:       make(chan binanceFrame, owner.opts.EventQueueSize),
	}
	c.fa.SetQueueCap(owner.opts.EventQueueSize)

	c.ShouldConnect = func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.streams) > 0
	}
	c.OnConnect = c.onConnect
	c.OnMessage = c.onMessage
	c.OnDisconnect = c.onDisconnect
	c.OnError = func(err error) {
		c.log().Warn("binance: ws error", "market", mkt, "class", class, "err", err)
	}
	return c
}

// log routes this connection's lines through its manager's logger, so one
// injected logger captures the connection and the websocket layer it embeds.
// A nil owner (only a hand-built conn in a test fixture) falls back to the
// process default.
func (c *binanceConn) log() *slog.Logger {
	if c.owner != nil {
		return c.owner.log()
	}
	return slog.Default()
}

// SetOnStatusChange registers a callback that fires whenever either
// connection's status changes.
func (b *WSBinance) SetOnStatusChange(fn func(MarketType, ws.ConnectionStatus)) {
	b.onStatusChange = fn
}

// SetOnDisconnect registers a callback that receives the error which ended a
// connection, or nil for a deliberate Disconnect(), tagged with the market
// whose connection dropped. Unlike SetOnStatusChange it carries WHY the
// connection dropped: the peer's close frame reason is in the error message and
// its code is available via websocket.CloseStatus. Note that MarketPerp uses two
// connections (market + public classes), so this can fire once per class. Set it
// before Start.
func (b *WSBinance) SetOnDisconnect(fn func(MarketType, error)) {
	b.onDisconnectHook = fn
}

// SetOnFrameDropped registers a callback invoked once for every frame that
// never reached a dispatcher, on the goroutine that discarded it: the read loop
// for a frame the queue could not hold (DroppedFrame.Cause == DropQueueFull),
// the disconnect path for a frame discarded with the queue
// (DropDrainedOnDisconnect). It applies to every connection of this manager;
// the hook must be cheap (no blocking, no I/O) because it runs on the read loop.
//
// The counters behind Stats() count drops whether or not a hook is set, and the
// hook is only called when one is. FrameID gaps seen by a consumer inside one
// ConnID match the reported drops exactly: every frame read consumes an ID,
// whether it is delivered, dropped or drained.
func (b *WSBinance) SetOnFrameDropped(fn func(connector.DroppedFrame)) {
	for _, byClass := range b.conns {
		for _, c := range byClass {
			c.fa.SetOnDrop(fn)
		}
	}
}

// Stats returns the frame accounting for a market's stream connections.
//
// Spot uses one connection (spot) and USDⓈ-M perpetual uses two (market:
// aggTrade/kline/markPrice; public: bookTicker/depth), so for MarketPerp the
// counters, QueueLen/QueueCap and QueueHighWater are the SUM over both
// connections, Disconnects is the sum, and ConnID is the highest current
// generation of the two. ConnIDs are unique across the whole manager (see
// connSeq), so a DroppedFrame's ConnID always identifies the connection that
// lost the frame. The bool is false for an invalid market type.
func (b *WSBinance) Stats(mkt MarketType) (connector.FrameStats, bool) {
	byClass, ok := b.conns[mkt]
	if !ok {
		return connector.FrameStats{}, false
	}
	var out connector.FrameStats
	for _, c := range byClass {
		out = addFrameStats(out, c.fa.Snapshot())
	}
	return out, true
}

// addFrameStats merges two snapshots of the same market's connections.
func addFrameStats(a, b connector.FrameStats) connector.FrameStats {
	return connector.FrameStats{
		FramesRead:     a.FramesRead + b.FramesRead,
		FramesDropped:  a.FramesDropped + b.FramesDropped,
		FramesDrained:  a.FramesDrained + b.FramesDrained,
		QueueLen:       a.QueueLen + b.QueueLen,
		QueueCap:       a.QueueCap + b.QueueCap,
		QueueHighWater: a.QueueHighWater + b.QueueHighWater,
		ConnID:         max(a.ConnID, b.ConnID),
		Disconnects:    a.Disconnects + b.Disconnects,
	}
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

	opts := b.wsOptions(reconnectIntervalMs)

	for _, mkt := range []MarketType{MarketSpot, MarketPerp} {
		for _, c := range b.conns[mkt] {
			if err := c.Connect(ctx, c.wsURL(), opts); err != nil {
				return fmt.Errorf("binance %s %s: %w", mkt, c.class, err)
			}
		}
	}
	return nil
}

// wsOptions builds the connection options every connection of this manager is
// dialled with. Kept separate from Start, like the other venues' wsOptions, so
// the transport settings (including the logger wiring) can be asserted without
// a live socket.
func (b *WSBinance) wsOptions(reconnectIntervalMs int64) ws.WSOptions {
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
	// Point the BaseWebSocket inside each binanceConn at this manager's logger:
	// its connect/reconnect/drop lines belong to the same instance.
	opts.Logger = b.opts.Logger
	return opts
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

// SubscribeKlines streams kline/candlestick updates for the given symbols at
// the given interval. interval must be one of "1m", "5m", "15m", "1h", "4h",
// "1d" and is matched exactly (Binance stream names are lowercase); anything
// else returns an error and subscribes to nothing.
//
// Binance pushes an update for the in-progress candle about once a second and
// flags the last one with IsFinal when the candle closes. Every update is
// dispatched; consumers filter on BinanceKlineEvent.IsFinal.
func (b *WSBinance) SubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) error {
	streams, err := buildKlineStreams(symbols, interval)
	if err != nil {
		return err
	}
	if !mkt.valid() {
		return fmt.Errorf("binance: invalid market type %q", mkt)
	}
	b.subscribe(ctx, mkt, streams)
	return nil
}

// UnsubscribeKlines stops the kline stream for the given symbols/interval.
// The interval is validated the same way as SubscribeKlines.
func (b *WSBinance) UnsubscribeKlines(ctx context.Context, mkt MarketType, symbols []string, interval string) error {
	streams, err := buildKlineStreams(symbols, interval)
	if err != nil {
		return err
	}
	if !mkt.valid() {
		return fmt.Errorf("binance: invalid market type %q", mkt)
	}
	b.unsubscribe(ctx, mkt, streams)
	return nil
}

// SubscribeMarkPrice streams mark-price updates (<symbol>@markPrice@1s) for the
// given symbols. The stream is only available on USDⓈ-M perpetual futures and
// is carried by the /market connection (see streamClassOf), so it must be
// subscribed with MarketPerp.
func (b *WSBinance) SubscribeMarkPrice(ctx context.Context, mkt MarketType, symbols []string) {
	b.subscribe(ctx, mkt, buildStreams(symbols, streamMarkPrice))
}

// UnsubscribeMarkPrice stops the mark-price stream for the given symbols.
func (b *WSBinance) UnsubscribeMarkPrice(ctx context.Context, mkt MarketType, symbols []string) {
	b.unsubscribe(ctx, mkt, buildStreams(symbols, streamMarkPrice))
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
		b.log().Warn("binance: invalid market type", "market", mkt)
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
		c.log().Info("binance: send DROPPED (conn nil)", "market", c.mkt, "method", method, "streams", streams)
		return
	}
	if err := c.writeSubscribe(ctx, conn, method, streams); err != nil {
		c.log().Warn("binance: failed to send subscription", "market", c.mkt, "method", method, "err", err)
	}
}

func (c *binanceConn) writeSubscribe(ctx context.Context, conn *coderws.Conn, method string, streams []string) error {
	c.log().Info("binance: writeSubscribe", "market", c.mkt, "method", method, "streams", streams)
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
	// A new generation: every frame read from this socket is stamped with it,
	// so a consumer can tell a FrameID gap (local loss) from a reconnect (an
	// unknowable gap). The counter is shared with the other connections of
	// this manager, so generations are unique per connection instance.
	c.fa.SetConnID(c.owner.connSeq.Add(1))

	if c.owner.onStatusChange != nil {
		c.owner.onStatusChange(c.mkt, ws.StatusConnected)
	}

	// Re-send every active subscription.
	if streams := c.allStreams(); len(streams) > 0 {
		c.log().Info("binance: onConnect re-sending", "market", c.mkt, "streams", streams)
		if err := c.writeSubscribe(ctx, conn, binanceMethodSubscribe, streams); err != nil {
			return err
		}
	} else {
		c.log().Info("binance: onConnect no streams", "market", c.mkt)
	}

	// Seed depth books from REST snapshots on the connection that carries
	// the depth stream (spot single conn; perp public conn). Failures are
	// logged, not fatal — bookTicker/trade/kline streams keep working.
	if c.class == classSpot || c.class == classPublic {
		if err := c.owner.snapshotDepthBooks(ctx, c.mkt); err != nil {
			c.log().Warn("binance: depth snapshot failed", "market", c.mkt, "err", err)
		}
	}
	return nil
}

func (c *binanceConn) onDisconnect(err error) {
	if closeCode := coderws.CloseStatus(err); closeCode != -1 {
		c.log().Info("binance: disconnected", "market", c.mkt, "reason", err, "close_code", closeCode)
	} else {
		c.log().Info("binance: disconnected", "market", c.mkt, "reason", err)
	}

	// Drop stale buffered events and reset depth books; the next connect
	// re-snapshots them. Each discarded frame is counted (FramesDrained) and
	// reported to the drop hook, so a consumer's
	// read == delivered + dropped + drained identity holds across the drop.
	for {
		select {
		case f := <-c.eventCh:
			c.fa.Dequeued()
			c.fa.NoteDrop(f.id, f.connID, f.rx, connector.DropDrainedOnDisconnect)
		default:
			goto drained
		}
	}
drained:
	// Count the disconnect after the drain, so an observer that sees
	// Disconnects grow also sees FramesDrained settled.
	c.fa.NoteDisconnect()
	c.owner.resetDepthBooks(c.mkt)

	if fn := c.owner.onDisconnectHook; fn != nil {
		fn(c.mkt, err)
	}
	if c.owner.onStatusChange != nil {
		c.owner.onStatusChange(c.mkt, ws.StatusDisconnected)
	}
}

// onMessage runs on the read-loop goroutine. It stamps FrameID and ConnID here,
// before the queue, so a frame the queue cannot hold still consumes an ID: the
// gaps a consumer sees in delivered FrameIDs are exactly the frames that were
// lost, and FramesRead stays the count of everything the socket delivered.
func (c *binanceConn) onMessage(_ context.Context, data []byte) error {
	id := c.fa.Stamp()
	f := binanceFrame{data: data, id: id, connID: c.fa.ConnID(), rx: time.Now()}
	select {
	case c.eventCh <- f:
		c.fa.Enqueued()
		return nil
	default:
	}
	c.fa.NoteDrop(f.id, f.connID, f.rx, connector.DropQueueFull)
	c.log().Warn("binance: dropping message, event channel full",
		"market", c.mkt, "class", c.class, "frame_id", f.id, "conn_id", f.connID,
		"queue_cap", c.fa.QueueCap())
	return nil
}

// ─────────────────────────────────────────────────────────────
// Event dispatch
// ─────────────────────────────────────────────────────────────

func (b *WSBinance) eventDispatcher(ctx context.Context, c *binanceConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-c.eventCh:
			c.fa.Dequeued()
			b.processMessage(c, f)
		}
	}
}

// processMessage decodes one queued frame and dispatches the typed event(s) it
// carries, copying the frame's provenance (FrameID/ConnID) onto each event.
func (b *WSBinance) processMessage(c *binanceConn, f binanceFrame) {
	data := f.data
	if len(bytes.TrimSpace(data)) == 0 {
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		b.log().Warn("binance: failed to parse message", "market", c.mkt, "err", err)
		return
	}

	// Subscribe/unsubscribe acknowledgement — ignore (log errors).
	if _, hasID := raw["id"]; hasID {
		var ack binanceSubAck
		if err := json.Unmarshal(data, &ack); err == nil && ack.Error != nil {
			b.log().Warn("binance: subscribe error", "market", c.mkt, "err", ack.Error)
		}
		return
	}

	switch jsonString(raw["e"]) {
	case "bookTicker":
		b.handleBookTicker(c, f)
	case "aggTrade":
		b.handleAggTrade(c, f)
	case "kline":
		b.handleKline(c, f)
	case "markPriceUpdate":
		b.handleMarkPrice(c, f)
	case "depthUpdate":
		b.handleDepth(c, f)
	case "":
		// Spot bookTicker has no event-type field; partial-depth frames
		// (unused) are ignored.
		b.handleSpotBookTicker(c, f, raw)
	default:
		b.log().Debug("binance: ignoring event", "market", c.mkt, "e", jsonString(raw["e"]))
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

func (b *WSBinance) handleBookTicker(c *binanceConn, f binanceFrame) {
	var ev binanceBookTicker
	if err := json.Unmarshal(f.data, &ev); err != nil {
		b.log().Warn("binance: failed to parse bookTicker", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(streamFor(symbol, streamBookTicker)) {
		return
	}
	b.base.DispatchEvent(&connector.BinanceBookTickerEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		FrameID:      f.id,
		ConnID:       f.connID,
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
func (b *WSBinance) handleSpotBookTicker(c *binanceConn, f binanceFrame, raw map[string]json.RawMessage) {
	if _, ok := raw["u"]; !ok {
		return // partial-depth or other frameless message
	}
	b.handleBookTicker(c, f)
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

func (b *WSBinance) handleAggTrade(c *binanceConn, f binanceFrame) {
	var ev binanceAggTrade
	if err := json.Unmarshal(f.data, &ev); err != nil {
		b.log().Warn("binance: failed to parse aggTrade", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(streamFor(symbol, streamAggTrade)) {
		b.log().Info("binance: aggTrade dropped, not subscribed", "market", c.mkt, "symbol", ev.Symbol, "stream", streamFor(symbol, streamAggTrade))
		return
	}

	b.base.DispatchEvent(&connector.BinanceAggTradeEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		FrameID:      f.id,
		ConnID:       f.connID,
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

func (b *WSBinance) handleKline(c *binanceConn, f binanceFrame) {
	var ev binanceKlineMsg
	if err := json.Unmarshal(f.data, &ev); err != nil {
		b.log().Warn("binance: failed to parse kline", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(klineStream(symbol, ev.Kline.Interval)) {
		return
	}
	b.base.DispatchEvent(&connector.BinanceKlineEvent{
		SeqID:       b.base.NextSeqID(),
		ReceivedAt:  b.base.Now(),
		FrameID:     f.id,
		ConnID:      f.connID,
		Symbol:      ev.Symbol,
		Market:      string(c.mkt),
		Interval:    ev.Kline.Interval,
		OpenTime:    time.UnixMilli(ev.Kline.OpenTime),
		CloseTime:   time.UnixMilli(ev.Kline.CloseTime),
		Open:        ev.Kline.Open,
		High:        ev.Kline.High,
		Low:         ev.Kline.Low,
		Close:       ev.Kline.Close,
		Volume:      ev.Kline.Volume,
		QuoteVolume: ev.Kline.QuoteVolume,
		TradeCount:  ev.Kline.TradeNum,
		IsFinal:     ev.Kline.IsFinal,
		Timestamp:   binanceEventTime(ev.EventTime, 0, b.base.Now()),
	})
}

// ── Mark price ──────────────────────────────────────────────

// binanceMarkPrice is the <symbol>@markPrice@1s payload. Every field carries an
// explicit tag: the JSON keys are case-sensitive and several differ only by
// case (p/P, i/I, r/R, T/t), so encoding/json must match them exactly — the
// documented indexPrice ("i") must never land in an "I" field, etc.
type binanceMarkPrice struct {
	Event           string `json:"e"` // "markPriceUpdate"
	EventTime       int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	EstSettlePrice  string `json:"P"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

func (b *WSBinance) handleMarkPrice(c *binanceConn, f binanceFrame) {
	var ev binanceMarkPrice
	if err := json.Unmarshal(f.data, &ev); err != nil {
		b.log().Warn("binance: failed to parse markPrice", "err", err)
		return
	}
	symbol := normalizeSymbol(ev.Symbol)
	if !c.isSubscribed(streamFor(symbol, streamMarkPrice)) {
		return
	}
	now := b.base.Now()
	b.base.DispatchEvent(&connector.BinanceMarkPriceEvent{
		SeqID:           b.base.NextSeqID(),
		ReceivedAt:      now,
		FrameID:         f.id,
		ConnID:          f.connID,
		Symbol:          ev.Symbol,
		Market:          string(c.mkt),
		MarkPrice:       ev.MarkPrice,
		IndexPrice:      ev.IndexPrice,
		EstSettlePrice:  ev.EstSettlePrice,
		FundingRate:     ev.FundingRate,
		NextFundingTime: ev.NextFundingTime,
		EventTime:       ev.EventTime,
		RecvTime:        now,
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

func (b *WSBinance) handleDepth(c *binanceConn, f binanceFrame) {
	var upd binanceDepthUpdate
	if err := json.Unmarshal(f.data, &upd); err != nil {
		b.log().Warn("binance: failed to parse depth update", "err", err)
		return
	}
	symbol := normalizeSymbol(upd.Symbol)

	// Partial book depth frames (e.g. @depth20@100ms) are full top-N
	// snapshots — dispatch directly, no local book / REST seeding.
	if c.subscribedPartialDepth(symbol) != "" {
		b.handlePartialDepth(c, f, &upd, symbol)
		return
	}
	if !c.isSubscribed(streamFor(symbol, streamDepth)) {
		return
	}

	bk := b.bookFor(c.mkt, symbol)
	bk.mu.Lock()

	if !bk.snapshotted {
		// Book not seeded yet — trigger a (gated) snapshot and drop this diff.
		bk.mu.Unlock()
		b.triggerDepthReseed(c.mkt, symbol)
		return
	}

	if upd.FinalUpdateID <= bk.lastUpdateID {
		// Stale — already applied via a later update.
		bk.mu.Unlock()
		return
	}
	if upd.FirstUpdateID > bk.lastUpdateID+1 {
		// Gap detected — re-seed the book from a fresh snapshot, through the
		// gate: one fetch in flight per book, one per minimum interval per
		// symbol, a shared per-minute budget, and a pause after a 429/418.
		bk.snapshotted = false
		bk.mu.Unlock()
		b.triggerDepthReseed(c.mkt, symbol)
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
		FrameID:      f.id,
		ConnID:       f.connID,
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
func (b *WSBinance) handlePartialDepth(c *binanceConn, f binanceFrame, upd *binanceDepthUpdate, symbol string) {
	bids := make(map[string]string)
	asks := make(map[string]string)
	applyLevels(bids, parseBinanceLevels(upd.BidsRaw))
	applyLevels(asks, parseBinanceLevels(upd.AsksRaw))
	b.base.DispatchEvent(&connector.BinanceDepthEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		FrameID:      f.id,
		ConnID:       f.connID,
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

// triggerDepthReseed asks the gate for permission to re-seed one book and
// spawns the fetch only if it is granted. Refused triggers are counted by the
// gate (coalesced behind an in-flight fetch, or deferred by the interval /
// budget / pause) and change nothing else: the book stays unseeded and its
// diffs are dropped exactly as before, so the next diff after the gate allows
// one triggers the fetch.
//
// A failed fetch is not retried here: the gate's minimum interval holds the next
// attempt until a later diff, which keeps a venue outage from becoming a retry
// loop. A 429/418 pauses every gap-triggered re-seed for its Retry-After.
func (b *WSBinance) triggerDepthReseed(mkt MarketType, symbol string) {
	key := depthReseedKey{mkt: mkt, symbol: symbol}
	if !b.reseed.admit(key) {
		return
	}
	go func() {
		err := b.snapshotDepthBook(b.rootCx, mkt, symbol)
		var pause time.Duration
		var httpErr *depthSnapshotHTTPError
		if errors.As(err, &httpErr) {
			pause = httpErr.RetryAfter
		}
		if err != nil {
			b.log().Warn("binance: depth re-seed failed", "market", mkt, "symbol", symbol, "err", err)
		}
		if b.reseed.finish(key, err != nil, pause) {
			b.log().Warn("binance: pausing depth re-seeds after a rate-limit response",
				"market", mkt, "symbol", symbol, "retry_after", pause)
		}
	}()
}

// DepthReseedStats reports the gap-triggered depth re-seed gate's ledger: how
// many triggers it saw and how each was resolved. Triggers always equals
// Fetched+Coalesced+Deferred. The per-connect initial seeding is not counted.
func (b *WSBinance) DepthReseedStats() DepthReseedStats {
	return b.reseed.stats()
}

// snapshotDepthBooks seeds a fresh order book for every subscribed depth
// symbol on a market. Used on (re)connect; it does NOT pass through the reseed
// gate — the gate bounds gap-triggered repairs only, so a connect still seeds
// every subscribed symbol exactly once.
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

// depthConnID returns the current connection generation of the connection that
// carries a market's depth stream (spot single connection; perp public class).
func (b *WSBinance) depthConnID(mkt MarketType) uint64 {
	class := classSpot
	if mkt != MarketSpot {
		class = classPublic
	}
	if c := b.connFor(mkt, class); c != nil {
		return c.fa.ConnID()
	}
	return 0
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
	url := fmt.Sprintf("%s%s?symbol=%s&limit=50", b.restURL(mkt), depthRESTPath(mkt), strings.ToUpper(symbol))
	resp, err := b.http.Get(url)
	if err != nil {
		return fmt.Errorf("binance %s depth snapshot %s: %w", mkt, symbol, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &depthSnapshotHTTPError{
			Market:     mkt,
			Symbol:     symbol,
			Status:     resp.StatusCode,
			RetryAfter: retryAfterPause(resp.StatusCode, resp.Header),
		}
	}
	var snap binanceDepthSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return fmt.Errorf("binance %s depth snapshot %s: decode: %w", mkt, symbol, err)
	}

	bk.bids = levelMap(snap.Bids)
	bk.asks = levelMap(snap.Asks)
	bk.lastUpdateID = snap.LastUpdateID
	bk.snapshotted = true

	// The seeded book was not built from a wire frame, so it carries no
	// FrameID (0) — that marks it as a re-seed for a consumer reconciling
	// FrameID gaps — but it does carry the generation of the connection whose
	// depth stream it belongs to.
	b.base.DispatchEvent(&connector.BinanceDepthEvent{
		SeqID:        b.base.NextSeqID(),
		ReceivedAt:   b.base.Now(),
		FrameID:      0,
		ConnID:       b.depthConnID(mkt),
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
