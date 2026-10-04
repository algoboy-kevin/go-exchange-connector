// Package hyperliquid implements a Hyperliquid perpetuals connector: a
// WebSocket stream manager for the venue's public market-data channels and a
// REST /info client for metadata and snapshots.
//
// The stream manager mirrors the other venue packages in this repo (pkg/binance,
// pkg/polymarket): typed events are pushed through connector.DispatchEvent, the
// connection is a websocket.BaseWebSocket with automatic reconnect/backoff, and
// subscriptions are re-sent on every successful (re)connect.
//
//	base := connector.New(false, nil)
//	hl := hyperliquid.New(base)
//	hl.SetDispatcher(func(ev any) {
//	    switch e := ev.(type) {
//	    case *connector.HyperliquidBookEvent:
//	        fmt.Printf("%s book bids=%d asks=%d\n", e.Coin, len(e.Bids), len(e.Asks))
//	    case *connector.HyperliquidTradeEvent:
//	        fmt.Printf("%s trade %s @ %s (%s)\n", e.Coin, e.Size, e.Price, e.Side)
//	    }
//	})
//	if err := hl.Start(ctx, 0); err != nil {
//	    log.Fatal(err)
//	}
//	defer hl.Stop()
//
//	if err := hl.SubscribeL2Book(ctx, []string{"BTC"}, hyperliquid.SubParams{Fast: true}); err != nil {
//	    log.Fatal(err)
//	}
//	hl.SubscribeTrades(ctx, []string{"BTC", "ETH"})
//
// Wire protocol, verified against the venue docs, the sonirico/go-hyperliquid
// SDK, and a live capture (2026-10-04; every channel below was seen arriving):
//
//	subscribe    {"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
//	ack          {"channel":"subscriptionResponse","data":{...normalised subscription...}}
//	data         {"channel":"trades","data":[{...}]}
//	keepalive    {"method":"ping"} → {"channel":"pong"}
//
// One subscription per frame; the ack echoes the subscription with the venue's
// defaults filled in (nSigFigs/mantissa null, fast false).
//
// All subscriptions share one connection: the venue allows 10 connections and
// 1000 subscriptions per IP, so one socket carries the whole public feed
// comfortably, and a single connection means a single reconnect to reason
// about. (A raw recorder that must route frames without parsing needs one
// connection per channel instead — it can build those from
// BuildSubscriptions/SubscribeFrame.)
package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	defaultReconnectIntervalMs = 500
	defaultReadLimitBytes      = 1 << 20 // 1 MiB
	defaultAppPingIntervalMs   = 50_000  // 50s, as used by every published SDK
	defaultWSPingIntervalMs    = 20_000  // 20s WebSocket-level control ping
	eventQueueSize             = 8192
)

// Options configures the WebSocket transport of a stream manager.
type Options struct {
	// URL is the WebSocket endpoint. Empty selects DefaultWSURL
	// (wss://api.hyperliquid.xyz/ws).
	URL string

	// ReconnectIntervalMs is the base reconnect delay; it doubles per
	// consecutive failure up to the BaseWebSocket ceiling. Zero selects 500ms.
	//
	// Keep it well away from zero: the venue allows 30 new connections per
	// minute, so a fast backoff on a venue-side outage would spend the budget
	// on failed dials.
	ReconnectIntervalMs int64

	// ReadLimitBytes caps a single WebSocket message. Zero selects 1 MiB,
	// which is ~50x the largest frame the public channels produce (the allMids
	// snapshot).
	ReadLimitBytes int64

	// AppPingIntervalMs is the interval for the application-level
	// {"method":"ping"} frame, which the venue answers with
	// {"channel":"pong"}. Zero selects 50s; negative disables it.
	AppPingIntervalMs int64

	// WSPingIntervalMs is the interval for the WebSocket-level *control* ping
	// that websocket.BaseWebSocket sends and watches for a pong. Zero selects
	// 20s; negative disables it, leaving the application-level ping as the only
	// keepalive.
	//
	// Hyperliquid is a standard RFC 6455 server and answers control pings, but
	// this is the piece most worth watching in production: if a deployment ever
	// stops answering, the pong watchdog (2× this interval) would force a
	// reconnect every ~60s. The reconnect logs make that obvious, and this knob
	// is the fix.
	WSPingIntervalMs int64

	// DataStaleTimeoutMs enables the data-staleness watchdog, which
	// force-reconnects when no frame at all arrives within the timeout.
	//
	// Zero — the default — disables it, because every public Hyperliquid channel
	// except l2Book is event-driven: bbo is sent only on a block where the bbo
	// changed, trades only when something prints, and activeAssetCtx once per
	// block. Silence therefore means "nothing happened", not "the socket is
	// dead", and a watchdog would manufacture reconnect gaps in a quiet market.
	// Half-open sockets are still caught by the ping/pong keepalive.
	DataStaleTimeoutMs int64
}

// DefaultOptions returns the transport defaults.
func DefaultOptions() Options {
	return Options{
		URL:                 DefaultWSURL,
		ReconnectIntervalMs: defaultReconnectIntervalMs,
		ReadLimitBytes:      defaultReadLimitBytes,
		AppPingIntervalMs:   defaultAppPingIntervalMs,
		WSPingIntervalMs:    defaultWSPingIntervalMs,
	}
}

// WSHyperliquid streams Hyperliquid public market data into the connector
// dispatcher as typed connector.Hyperliquid*Event values.
//
// Lifecycle: New → (SetDispatcher) → Start → Subscribe* → Stop. Subscriptions
// may be made before Start or while disconnected; they are recorded and sent
// on the next successful connect, so a Subscribe call never fails because the
// socket happens to be down.
type WSHyperliquid struct {
	base *connector.Connector
	opts Options
	conn *hlConn

	onStatusChange func(ws.ConnectionStatus)
}

// New creates a stream manager over a connector base, using DefaultOptions.
func New(base *connector.Connector) *WSHyperliquid {
	return NewWithOptions(base, DefaultOptions())
}

// NewWithOptions creates a stream manager with explicit transport options.
// Zero-valued fields fall back to their defaults; negative ping intervals
// disable that ping.
func NewWithOptions(base *connector.Connector, opts Options) *WSHyperliquid {
	if opts.URL == "" {
		opts.URL = DefaultWSURL
	}
	if opts.ReconnectIntervalMs == 0 {
		opts.ReconnectIntervalMs = defaultReconnectIntervalMs
	}
	if opts.ReadLimitBytes == 0 {
		opts.ReadLimitBytes = defaultReadLimitBytes
	}
	if opts.AppPingIntervalMs == 0 {
		opts.AppPingIntervalMs = defaultAppPingIntervalMs
	}
	if opts.WSPingIntervalMs == 0 {
		opts.WSPingIntervalMs = defaultWSPingIntervalMs
	}

	h := &WSHyperliquid{base: base, opts: opts}
	h.conn = newHLConn(h)
	return h
}

// SetDispatcher routes the events dispatched by this manager
// (HyperliquidBookEvent, HyperliquidTradeEvent, HyperliquidBBOEvent,
// HyperliquidAssetCtxEvent, HyperliquidAllMidsEvent, HyperliquidCandleEvent,
// HyperliquidErrorEvent) to the application handler.
func (h *WSHyperliquid) SetDispatcher(d func(any)) {
	h.base.SetDispatcher(d)
}

// SetOnStatusChange registers a callback fired on every connection status
// change (connected / disconnected).
func (h *WSHyperliquid) SetOnStatusChange(fn func(ws.ConnectionStatus)) {
	h.onStatusChange = fn
}

// SetRawFrameHandler registers a callback invoked for every frame exactly as it
// arrived, before it is queued or parsed. It is the "never parse" path: a raw
// recorder can archive the bytes, and a probe can measure them, without this
// package's decoding being able to lose anything.
//
// Two rules, both load-bearing:
//
//   - It runs on the read-loop goroutine. Disk or network I/O inside it
//     throttles frame reads, which makes the venue drop the connection. Do the
//     minimum — copy the slice if you keep it, push it to a buffered channel,
//     return.
//   - The slice (and the rx time) belong to that one frame. coder/websocket
//     allocates a fresh slice per message, so keeping it is safe, but treating
//     it as a shared buffer is not.
//
// Set it before Start. Set both handlers to get raw bytes and typed events —
// the raw hook does not replace parsing.
func (h *WSHyperliquid) SetRawFrameHandler(fn func(frame []byte, rx time.Time)) {
	h.conn.rawMu.Lock()
	h.conn.rawHandler = fn
	h.conn.rawMu.Unlock()
}

// Start dials the venue and starts the event dispatcher. reconnectIntervalMs,
// when positive, overrides Options.ReconnectIntervalMs.
//
// The connection is dialled immediately even with no subscriptions (the venue
// tolerates an idle socket and the ping keepalive holds it open), which keeps
// Start's error contract simple: a returned error is a real dial failure.
func (h *WSHyperliquid) Start(ctx context.Context, reconnectIntervalMs int64) error {
	dispatchCtx, cancel := context.WithCancel(ctx)
	h.conn.dispatchCancel = cancel
	go h.eventDispatcher(dispatchCtx, h.conn)

	opts := ws.DefaultWSOptions()
	opts.ReconnectInterval = h.opts.ReconnectIntervalMs
	if reconnectIntervalMs > 0 {
		opts.ReconnectInterval = reconnectIntervalMs
	}
	opts.ReadLimit = h.opts.ReadLimitBytes
	if h.opts.WSPingIntervalMs > 0 {
		// Control ping + pong watchdog: the only liveness check that survives
		// across an event-driven channel being quiet.
		opts.PingInterval = h.opts.WSPingIntervalMs
	} else {
		opts.PingInterval = -1 // disable the ping loop entirely
	}
	if h.opts.DataStaleTimeoutMs > 0 {
		opts.DataStaleTimeout = h.opts.DataStaleTimeoutMs
	} else {
		opts.DisableDataStaleWatchdog = true
	}

	if err := h.conn.Connect(ctx, h.opts.URL, opts); err != nil {
		cancel()
		return fmt.Errorf("hyperliquid: connect %s: %w", h.opts.URL, err)
	}
	return nil
}

// Stop closes the connection, stops the ping ticker and the dispatcher, and
// clears the subscription registry. Safe to call more than once.
func (h *WSHyperliquid) Stop() {
	c := h.conn
	if c == nil {
		return
	}
	c.stopPing()
	if c.dispatchCancel != nil {
		c.dispatchCancel()
		c.dispatchCancel = nil
	}
	c.Close()
	c.clearSubscriptions()
}

// Status returns the connection status.
func (h *WSHyperliquid) Status() ws.ConnectionStatus {
	return h.conn.Status()
}

// Subscriptions returns the currently registered subscriptions, sorted by key.
func (h *WSHyperliquid) Subscriptions() []Subscription {
	return h.conn.allSubscriptions()
}

// ─────────────────────────────────────────────────────────────
// Subscribe / unsubscribe
// ─────────────────────────────────────────────────────────────

// Subscribe registers and sends subscriptions. Subscriptions already
// registered (same channel, coin and interval) are skipped, so calling this
// twice is harmless.
//
// A subscription registered while the socket is down is not lost: it is sent
// when the connection comes back. The returned error is therefore about the
// request (unknown channel, malformed coin, bad parameters), not about the
// transport.
func (h *WSHyperliquid) Subscribe(ctx context.Context, subs ...Subscription) error {
	for _, sub := range subs {
		if !IsSubscribable(sub.Type) {
			return fmt.Errorf("hyperliquid: unknown channel %q (subscribable: %s)", sub.Type, channelList())
		}
		if err := (SubParams{
			Interval: sub.Interval,
			NSigFigs: sub.NSigFigs,
			Mantissa: sub.Mantissa,
			Fast:     sub.Fast,
		}).validate(sub.Type); err != nil {
			return err
		}
		if sub.Type != ChannelAllMids {
			if err := ValidateCoin(sub.Coin); err != nil {
				return err
			}
		} else if sub.Coin != "" {
			return fmt.Errorf("hyperliquid: channel %s takes no coin (got %q)", sub.Type, sub.Coin)
		}
	}

	for _, sub := range h.conn.addSubs(subs) {
		h.conn.writeSub(ctx, sub, false)
	}
	return nil
}

// Unsubscribe removes subscriptions and sends the unsubscribe frames. The
// venue matches an unsubscribe against the original subscribe message, so the
// previously registered subscription object is replayed rather than a freshly
// built one (which could differ in the l2Book thinning options).
//
// Subscriptions that were never registered are ignored.
func (h *WSHyperliquid) Unsubscribe(ctx context.Context, subs ...Subscription) error {
	for _, sub := range h.conn.removeSubs(subs) {
		h.conn.writeSub(ctx, sub, true)
	}
	return nil
}

// SubscribeL2Book streams L2 book snapshots for the given coins. See SubParams
// for the thinning options; leave them zero to record what the venue defaults
// to.
func (h *WSHyperliquid) SubscribeL2Book(ctx context.Context, coins []string, p SubParams) error {
	return h.subscribeChannel(ctx, ChannelL2Book, coins, p)
}

// UnsubscribeL2Book stops the l2Book stream for the given coins.
func (h *WSHyperliquid) UnsubscribeL2Book(ctx context.Context, coins []string, p SubParams) error {
	return h.unsubscribeChannel(ctx, ChannelL2Book, coins, p)
}

// SubscribeTrades streams trade prints for the given coins. Trade prints are
// the only channel that is genuinely bursty: expect nothing, then a batch.
func (h *WSHyperliquid) SubscribeTrades(ctx context.Context, coins []string) error {
	return h.subscribeChannel(ctx, ChannelTrades, coins, SubParams{})
}

// UnsubscribeTrades stops the trades stream for the given coins.
func (h *WSHyperliquid) UnsubscribeTrades(ctx context.Context, coins []string) error {
	return h.unsubscribeChannel(ctx, ChannelTrades, coins, SubParams{})
}

// SubscribeBBO streams best bid/offer updates for the given coins. The venue
// sends an update only on a block where the bbo changed, so a quiet coin can
// be silent for a long time — this is the channel a "no frames in N seconds"
// alarm would misread.
func (h *WSHyperliquid) SubscribeBBO(ctx context.Context, coins []string) error {
	return h.subscribeChannel(ctx, ChannelBBO, coins, SubParams{})
}

// UnsubscribeBBO stops the bbo stream for the given coins.
func (h *WSHyperliquid) UnsubscribeBBO(ctx context.Context, coins []string) error {
	return h.unsubscribeChannel(ctx, ChannelBBO, coins, SubParams{})
}

// SubscribeActiveAssetCtx streams the perp asset context (funding, open
// interest, oracle/mark/mid) for the given coins, once per block.
func (h *WSHyperliquid) SubscribeActiveAssetCtx(ctx context.Context, coins []string) error {
	return h.subscribeChannel(ctx, ChannelActiveAssetCtx, coins, SubParams{})
}

// UnsubscribeActiveAssetCtx stops the activeAssetCtx stream for the given
// coins.
func (h *WSHyperliquid) UnsubscribeActiveAssetCtx(ctx context.Context, coins []string) error {
	return h.unsubscribeChannel(ctx, ChannelActiveAssetCtx, coins, SubParams{})
}

// SubscribeAllMids streams the mid price of every coin. It takes no coin: to
// narrow the feed, filter HyperliquidAllMidsEvent.Mids locally.
func (h *WSHyperliquid) SubscribeAllMids(ctx context.Context) error {
	return h.subscribeChannel(ctx, ChannelAllMids, nil, SubParams{})
}

// UnsubscribeAllMids stops the allMids stream.
func (h *WSHyperliquid) UnsubscribeAllMids(ctx context.Context) error {
	return h.unsubscribeChannel(ctx, ChannelAllMids, nil, SubParams{})
}

// SubscribeCandles streams OHLCV candles for the given coins at interval (one
// of CandleIntervals()).
func (h *WSHyperliquid) SubscribeCandles(ctx context.Context, coins []string, interval string) error {
	return h.subscribeChannel(ctx, ChannelCandle, coins, SubParams{Interval: interval})
}

// UnsubscribeCandles stops the candle stream for the given coins/interval.
func (h *WSHyperliquid) UnsubscribeCandles(ctx context.Context, coins []string, interval string) error {
	return h.unsubscribeChannel(ctx, ChannelCandle, coins, SubParams{Interval: interval})
}

func (h *WSHyperliquid) subscribeChannel(ctx context.Context, ch Channel, coins []string, p SubParams) error {
	subs, err := BuildSubscriptions(ch, coins, p)
	if err != nil {
		return err
	}
	return h.Subscribe(ctx, subs...)
}

func (h *WSHyperliquid) unsubscribeChannel(ctx context.Context, ch Channel, coins []string, p SubParams) error {
	subs, err := BuildSubscriptions(ch, coins, p)
	if err != nil {
		return err
	}
	return h.Unsubscribe(ctx, subs...)
}

// ─────────────────────────────────────────────────────────────
// Connection
// ─────────────────────────────────────────────────────────────

// hlConn is the single venue connection: one BaseWebSocket plus the
// subscription registry, the decoded-frame queue and the ping ticker.
type hlConn struct {
	*ws.BaseWebSocket
	owner *WSHyperliquid

	mu   sync.RWMutex
	subs map[string]Subscription // keyed by Subscription.Key()

	rawMu      sync.RWMutex
	rawHandler func(frame []byte, rx time.Time)

	pingMu     sync.Mutex
	pingCancel context.CancelFunc

	eventCh        chan frame
	dispatchCancel context.CancelFunc
}

// frame is a raw venue frame plus the local receive time. rx is sampled on the
// read-loop goroutine, before the frame is queued, so queueing delay cannot
// pollute ReceivedAt.
type frame struct {
	data []byte
	rx   time.Time
}

func newHLConn(owner *WSHyperliquid) *hlConn {
	c := &hlConn{
		BaseWebSocket: &ws.BaseWebSocket{},
		owner:         owner,
		subs:          make(map[string]Subscription),
		eventCh:       make(chan frame, eventQueueSize),
	}
	// ShouldConnect only gates the reconnect loop: with nothing subscribed
	// there is no reason to re-dial after a drop.
	c.ShouldConnect = func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return len(c.subs) > 0
	}
	c.OnConnect = c.onConnect
	c.OnMessage = c.onMessage
	c.OnDisconnect = c.onDisconnect
	c.OnError = func(err error) {
		slog.Warn("hyperliquid: ws error", "err", err)
	}
	return c
}

// ─────────────────────────────────────────────────────────────
// Subscription registry
// ─────────────────────────────────────────────────────────────

// addSubs registers subs and returns the ones that were newly added (i.e. the
// ones that need a subscribe frame). nSigFigs/mantissa/fast are not part of
// the identity: re-subscribing the same coin with different thinning is a
// no-op rather than a second stream for the same coin. To change thinning,
// unsubscribe first.
func (c *hlConn) addSubs(subs []Subscription) []Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	var added []Subscription
	for _, sub := range subs {
		key := sub.Key()
		if _, ok := c.subs[key]; ok {
			continue
		}
		c.subs[key] = sub
		added = append(added, sub)
	}
	return added
}

// removeSubs unregisters subs and returns the stored subscription objects to
// replay in unsubscribe frames.
func (c *hlConn) removeSubs(subs []Subscription) []Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	var removed []Subscription
	for _, sub := range subs {
		key := sub.Key()
		stored, ok := c.subs[key]
		if !ok {
			continue
		}
		delete(c.subs, key)
		removed = append(removed, stored)
	}
	return removed
}

func (c *hlConn) hasSubscription(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.subs[key]
	return ok
}

func (c *hlConn) allSubscriptions() []Subscription {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		out = append(out, sub)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

func (c *hlConn) clearSubscriptions() {
	c.mu.Lock()
	c.subs = make(map[string]Subscription)
	c.mu.Unlock()
}

// subscribed reports whether a subscription for ch/coin/interval is currently
// registered. Frames for unsubscribed keys are dropped: after an unsubscribe
// the venue can still have frames in flight, and delivering them would
// resurrect a stream the caller already stopped.
func (h *WSHyperliquid) subscribed(ch Channel, coin, interval string) bool {
	return h.conn.hasSubscription(Subscription{Type: ch, Coin: coin, Interval: interval}.Key())
}

// ─────────────────────────────────────────────────────────────
// Writes
// ─────────────────────────────────────────────────────────────

// writeSub sends one subscribe/unsubscribe frame. A write while disconnected
// is not an error: the registry is the source of truth and OnConnect re-sends
// everything on the next successful dial.
//
// Concurrent writes are safe: coder/websocket serializes frames internally.
func (c *hlConn) writeSub(ctx context.Context, sub Subscription, unsubscribe bool) {
	conn := c.Conn()
	if conn == nil {
		slog.Debug("hyperliquid: subscription deferred (not connected)",
			"subscription", sub.Key(), "unsubscribe", unsubscribe)
		return
	}

	var (
		data []byte
		err  error
	)
	if unsubscribe {
		data, err = UnsubscribeFrame(sub)
	} else {
		data, err = SubscribeFrame(sub)
	}
	if err != nil {
		slog.Warn("hyperliquid: build subscription frame", "subscription", sub.Key(), "err", err)
		return
	}
	if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
		slog.Warn("hyperliquid: write subscription frame",
			"subscription", sub.Key(), "unsubscribe", unsubscribe, "err", err)
		return
	}
	slog.Debug("hyperliquid: subscription frame sent", "subscription", sub.Key(), "unsubscribe", unsubscribe)
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (c *hlConn) onConnect(ctx context.Context, conn *coderws.Conn) error {
	// Re-send every registered subscription. One frame per subscription: the
	// venue takes a single subscription object per frame.
	subs := c.allSubscriptions()
	for _, sub := range subs {
		data, err := SubscribeFrame(sub)
		if err != nil {
			return fmt.Errorf("hyperliquid: build subscribe frame for %s: %w", sub.Key(), err)
		}
		if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
			return fmt.Errorf("hyperliquid: subscribe %s: %w", sub.Key(), err)
		}
	}
	slog.Info("hyperliquid: connected", "url", c.owner.opts.URL, "subscriptions", len(subs))

	c.startPing(ctx, conn)

	if fn := c.owner.onStatusChange; fn != nil {
		fn(ws.StatusConnected)
	}
	return nil
}

// onDisconnect fires on an unexpected drop, but not on a deliberate Close():
// BaseWebSocket suppresses the hook during shutdown. So this is exactly the
// "connection lost" path — stop the ping ticker and drop frames that belong to
// the dead socket.
func (c *hlConn) onDisconnect(err error) {
	c.stopPing()
	c.drainEvents()

	if err != nil {
		slog.Info("hyperliquid: disconnected", "reason", err)
	} else {
		slog.Info("hyperliquid: disconnected")
	}
	if fn := c.owner.onStatusChange; fn != nil {
		fn(ws.StatusDisconnected)
	}
}

// onMessage runs on the read-loop goroutine, so it only hands the frame to the
// raw hook and the queue. The slice is freshly allocated per message by
// coder/websocket and is owned by us; no copy is needed.
func (c *hlConn) onMessage(_ context.Context, data []byte) error {
	rx := time.Now()

	c.rawMu.RLock()
	raw := c.rawHandler
	c.rawMu.RUnlock()
	if raw != nil {
		raw(data, rx)
	}

	select {
	case c.eventCh <- frame{data: data, rx: rx}:
	default:
		slog.Warn("hyperliquid: dropping frame, event queue full")
	}
	return nil
}

func (c *hlConn) drainEvents() {
	for {
		select {
		case <-c.eventCh:
		default:
			return
		}
	}
}

// startPing starts the application-level keepalive. The venue answers
// {"method":"ping"} with {"channel":"pong"}; a write failure means the socket
// is gone, so the connection is dropped and the reconnect loop takes over.
func (c *hlConn) startPing(ctx context.Context, conn *coderws.Conn) {
	interval := time.Duration(c.owner.opts.AppPingIntervalMs) * time.Millisecond
	if interval <= 0 {
		return
	}
	c.stopPing()

	pingCtx, cancel := context.WithCancel(ctx)
	c.pingMu.Lock()
	c.pingCancel = cancel
	c.pingMu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				if err := conn.Write(pingCtx, coderws.MessageText, PingFrame()); err != nil {
					slog.Warn("hyperliquid: ping write failed", "err", err)
					// Only drop the connection if this is still the current
					// one — a stale ticker must not kill a healthy reconnect.
					if c.Conn() == conn {
						c.Disconnect()
					}
					return
				}
			}
		}
	}()
}

func (c *hlConn) stopPing() {
	c.pingMu.Lock()
	cancel := c.pingCancel
	c.pingCancel = nil
	c.pingMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// ─────────────────────────────────────────────────────────────
// Dispatch
// ─────────────────────────────────────────────────────────────

func (h *WSHyperliquid) eventDispatcher(ctx context.Context, c *hlConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-c.eventCh:
			h.processFrame(f)
		}
	}
}

// processFrame routes one venue frame by its `channel` field. Every public
// channel is {"channel":"<name>","data":<payload>}.
func (h *WSHyperliquid) processFrame(f frame) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(f.data, &env); err != nil {
		slog.Warn("hyperliquid: undecodable frame", "err", err, "frame", truncate(string(f.data), 256))
		return
	}

	switch Channel(env.Channel) {
	case ChannelL2Book:
		h.handleL2Book(env.Data, f.rx)
	case ChannelTrades:
		h.handleTrades(env.Data, f.rx)
	case ChannelBBO:
		h.handleBBO(env.Data, f.rx)
	case ChannelActiveAssetCtx:
		h.handleActiveAssetCtx(env.Data, f.rx)
	case ChannelAllMids:
		h.handleAllMids(env.Data, f.rx)
	case ChannelCandle:
		h.handleCandle(env.Data, f.rx)
	case ServerChannelSubscriptionResponse:
		slog.Debug("hyperliquid: subscription acknowledged", "data", truncate(string(env.Data), 256))
	case ServerChannelPong:
		slog.Debug("hyperliquid: pong")
	case ServerChannelError:
		// The only signal that a subscription was refused. Surfaced as an event
		// as well as a log: a caller that only mounts a dispatcher must still be
		// able to notice.
		msg := decodeErrorFrame(env.Data)
		slog.Warn("hyperliquid: venue error frame", "error", msg)
		h.base.DispatchEvent(&connector.HyperliquidErrorEvent{
			SeqID:      h.base.NextSeqID(),
			ReceivedAt: f.rx,
			Message:    msg,
		})
	default:
		slog.Debug("hyperliquid: ignoring channel", "channel", env.Channel)
	}
}

func (h *WSHyperliquid) handleL2Book(data json.RawMessage, rx time.Time) {
	var book L2BookSnapshot
	if err := json.Unmarshal(data, &book); err != nil {
		slog.Warn("hyperliquid: l2Book decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelL2Book, book.Coin, "") {
		slog.Debug("hyperliquid: l2Book frame for unsubscribed coin", "coin", book.Coin)
		return
	}
	h.base.DispatchEvent(&connector.HyperliquidBookEvent{
		SeqID:      h.base.NextSeqID(),
		ReceivedAt: rx,
		Coin:       book.Coin,
		Timestamp:  msTime(book.Time, rx),
		Bids:       eventLevels(book.Bids()),
		Asks:       eventLevels(book.Asks()),
	})
}

func (h *WSHyperliquid) handleTrades(data json.RawMessage, rx time.Time) {
	var trades []Trade
	if err := json.Unmarshal(data, &trades); err != nil {
		slog.Warn("hyperliquid: trades decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	for i := range trades {
		t := trades[i]
		if !h.subscribed(ChannelTrades, t.Coin, "") {
			continue
		}
		h.base.DispatchEvent(&connector.HyperliquidTradeEvent{
			SeqID:      h.base.NextSeqID(),
			ReceivedAt: rx,
			Coin:       t.Coin,
			Side:       t.Side,
			Price:      t.Price.String(),
			Size:       t.Size.String(),
			TradeID:    t.TradeID,
			Hash:       t.Hash,
			Users:      t.Users,
			Timestamp:  msTime(t.Time, rx),
		})
	}
}

func (h *WSHyperliquid) handleBBO(data json.RawMessage, rx time.Time) {
	var bbo BBO
	if err := json.Unmarshal(data, &bbo); err != nil {
		slog.Warn("hyperliquid: bbo decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelBBO, bbo.Coin, "") {
		slog.Debug("hyperliquid: bbo frame for unsubscribed coin", "coin", bbo.Coin)
		return
	}
	ev := &connector.HyperliquidBBOEvent{
		SeqID:      h.base.NextSeqID(),
		ReceivedAt: rx,
		Coin:       bbo.Coin,
		Timestamp:  msTime(bbo.Time, rx),
	}
	if bid := bbo.Bid(); bid != nil {
		lvl := eventLevel(*bid)
		ev.Bid = &lvl
	}
	if ask := bbo.Ask(); ask != nil {
		lvl := eventLevel(*ask)
		ev.Ask = &lvl
	}
	h.base.DispatchEvent(ev)
}

func (h *WSHyperliquid) handleActiveAssetCtx(data json.RawMessage, rx time.Time) {
	var msg struct {
		Coin string          `json:"coin"`
		Ctx  json.RawMessage `json:"ctx"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Warn("hyperliquid: activeAssetCtx decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelActiveAssetCtx, msg.Coin, "") {
		slog.Debug("hyperliquid: activeAssetCtx frame for unsubscribed coin", "coin", msg.Coin)
		return
	}
	var ctx AssetCtx
	if err := json.Unmarshal(msg.Ctx, &ctx); err != nil {
		slog.Warn("hyperliquid: activeAssetCtx context decode failed", "coin", msg.Coin, "err", err)
		return
	}
	// The channel carries the perp context for perps and the spot context for
	// spot assets (docs: WsActiveAssetCtx | WsActiveSpotAssetCtx), and the spot
	// variant has neither funding nor an oracle price. The payload itself
	// carries no discriminator, so probe the fields that only the perp variant
	// has.
	h.base.DispatchEvent(&connector.HyperliquidAssetCtxEvent{
		SeqID:        h.base.NextSeqID(),
		ReceivedAt:   rx,
		Coin:         msg.Coin,
		Timestamp:    rx, // the venue sends no timestamp on this channel
		IsSpot:       ctxIsSpot(msg.Ctx),
		Funding:      ctx.Funding.String(),
		OpenInterest: ctx.OpenInterest.String(),
		Premium:      ctx.Premium.String(),
		DayNtlVlm:    ctx.DayNtlVlm.String(),
		DayBaseVlm:   ctx.DayBaseVlm.String(),
		PrevDayPx:    ctx.PrevDayPx.String(),
		OraclePx:     ctx.OraclePx.String(),
		MarkPx:       ctx.MarkPx.String(),
		MidPx:        ctx.MidPx.String(),
		ImpactPxs:    numsToStrings(ctx.ImpactPxs),
	})
}

func (h *WSHyperliquid) handleAllMids(data json.RawMessage, rx time.Time) {
	var mids AllMids
	if err := json.Unmarshal(data, &mids); err != nil {
		slog.Warn("hyperliquid: allMids decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelAllMids, "", "") {
		return
	}
	out := make(map[string]string, len(mids.Mids))
	for coin, mid := range mids.Mids {
		out[coin] = mid.String()
	}
	h.base.DispatchEvent(&connector.HyperliquidAllMidsEvent{
		SeqID:      h.base.NextSeqID(),
		ReceivedAt: rx,
		Timestamp:  rx, // this channel carries no timestamp either
		Mids:       out,
	})
}

func (h *WSHyperliquid) handleCandle(data json.RawMessage, rx time.Time) {
	candles, err := decodeCandles(data)
	if err != nil {
		slog.Warn("hyperliquid: candle decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	for _, c := range candles {
		if !h.subscribed(ChannelCandle, c.Coin, c.Interval) {
			continue
		}
		h.base.DispatchEvent(&connector.HyperliquidCandleEvent{
			SeqID:      h.base.NextSeqID(),
			ReceivedAt: rx,
			Coin:       c.Coin,
			Interval:   c.Interval,
			OpenTime:   msTime(c.OpenTime, rx),
			CloseTime:  msTime(c.CloseTime, rx),
			Open:       c.Open.String(),
			High:       c.High.String(),
			Low:        c.Low.String(),
			Close:      c.Close.String(),
			Volume:     c.Volume.String(),
			TradeCount: c.TradeCount,
			Timestamp:  rx,
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Decode helpers
// ─────────────────────────────────────────────────────────────

// decodeCandles accepts either shape the candle channel might use. The venue
// docs type the payload as Candle[], while at least one published SDK decodes a
// single object — and a shape that fails to decode is a frame nobody sees.
func decodeCandles(raw json.RawMessage) ([]Candle, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}
	if trimmed[0] == '[' {
		var out []Candle
		if err := json.Unmarshal(trimmed, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	var one Candle
	if err := json.Unmarshal(trimmed, &one); err != nil {
		return nil, err
	}
	return []Candle{one}, nil
}

// ctxIsSpot reports whether an asset context is the spot variant, i.e. it
// carries neither of the perp-only fields.
func ctxIsSpot(raw json.RawMessage) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	_, hasFunding := probe["funding"]
	_, hasOraclePx := probe["oraclePx"]
	return !hasFunding && !hasOraclePx
}

// decodeErrorFrame renders an {"channel":"error"} payload, which the venue
// sends as a bare JSON string but could equally send as an object.
func decodeErrorFrame(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// msTime converts a millisecond epoch into a time.Time, falling back to the
// local receive time when the venue omitted the field.
func msTime(ms int64, fallback time.Time) time.Time {
	if ms <= 0 {
		return fallback
	}
	return time.UnixMilli(ms)
}

func eventLevel(l L2Level) connector.HyperliquidLevel {
	return connector.HyperliquidLevel{
		Price: l.Price.String(),
		Size:  l.Size.String(),
		Count: l.Count,
	}
}

func eventLevels(levels []L2Level) []connector.HyperliquidLevel {
	if len(levels) == 0 {
		return nil
	}
	out := make([]connector.HyperliquidLevel, len(levels))
	for i, l := range levels {
		out[i] = eventLevel(l)
	}
	return out
}

func numsToStrings(nums []Num) []string {
	if len(nums) == 0 {
		return nil
	}
	out := make([]string, len(nums))
	for i, n := range nums {
		out[i] = n.String()
	}
	return out
}
