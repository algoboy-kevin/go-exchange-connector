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
	//
	// The two ACCOUNT channels (orderUpdates, userFills) are event-driven in the
	// strongest sense — a desk that is not trading sees nothing for minutes or
	// hours — so a connection that carries ONLY user channels must leave this at
	// 0. The driver is expected to run a SEPARATE WSHyperliquid instance for the
	// user channels, with DefaultOptions (this stays 0); the market-data
	// instance may enable the watchdog for the l2Book stream it carries.
	DataStaleTimeoutMs int64

	// SubscribePaceMs is the minimum gap between consecutive subscribe frames
	// sent by Subscribe, SubscribeMany and the reconnect replay. Zero — the
	// default — leaves subscribe frames unpaced (and the reconnect replay
	// inline in the connect hook, as before).
	//
	// The venue caps WS traffic at 2000 messages/min per IP; 40 (=1500
	// msgs/min) is the recommended value for a consumer that subscribes
	// hundreds of channels. When positive, the reconnect replay runs on its
	// own goroutine so reading starts immediately; see SubscribeMany.
	SubscribePaceMs int64

	// EventQueueSize is the depth of the decoded-frame queue between the read
	// loop and the dispatcher. Zero (and any negative value) selects the
	// default of 8192.
	//
	// The queue is the only place where market frames can be lost locally: a
	// full queue drops the frame, counts it (Stats.FramesDropped), reports it
	// to SetOnFrameDropped and — on an account-bearing connection — fails the
	// connection so the reconnect and gap-event machinery can bound the damage.
	// Size it for the slowest dispatcher you expect, not the average one.
	EventQueueSize int
}

// DefaultOptions returns the transport defaults.
func DefaultOptions() Options {
	return Options{
		URL:                 DefaultWSURL,
		ReconnectIntervalMs: defaultReconnectIntervalMs,
		ReadLimitBytes:      defaultReadLimitBytes,
		AppPingIntervalMs:   defaultAppPingIntervalMs,
		WSPingIntervalMs:    defaultWSPingIntervalMs,
		EventQueueSize:      eventQueueSize,
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

	// onDisconnectHook receives the error that ended the connection (nil for a
	// deliberate Disconnect()). See SetOnDisconnect.
	onDisconnectHook func(err error)
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
	if opts.EventQueueSize <= 0 {
		// Negative is a caller bug, not a request for an unbuffered queue: an
		// unbuffered queue would drop nearly every frame, so fall back to the
		// default rather than silently disabling the stream.
		opts.EventQueueSize = eventQueueSize
	}

	h := &WSHyperliquid{base: base, opts: opts}
	h.conn = newHLConn(h)
	return h
}

// SetDispatcher routes the events dispatched by this manager
// (HyperliquidBookEvent, HyperliquidTradeEvent, HyperliquidBBOEvent,
// HyperliquidAssetCtxEvent, HyperliquidAllMidsEvent, HyperliquidCandleEvent,
// HyperliquidOrderUpdateEvent, HyperliquidUserFillsEvent,
// HyperliquidErrorEvent) to the application handler.
func (h *WSHyperliquid) SetDispatcher(d func(any)) {
	h.base.SetDispatcher(d)
}

// SetOnStatusChange registers a callback fired on every connection status
// change (connected / disconnected).
func (h *WSHyperliquid) SetOnStatusChange(fn func(ws.ConnectionStatus)) {
	h.onStatusChange = fn
}

// SetOnDisconnect registers a callback that receives the error which ended the
// connection, or nil for a deliberate Disconnect(). Unlike SetOnStatusChange it
// carries WHY the connection dropped: the peer's close frame reason is in the
// error message and its code is available via websocket.CloseStatus. That is
// the difference between "the socket blipped" and "the venue refused us" —
// e.g. "Cannot open more than 15 connections." for a per-IP connection cap,
// which is otherwise only visible in a log line. Set it before Start.
func (h *WSHyperliquid) SetOnDisconnect(fn func(err error)) {
	h.onDisconnectHook = fn
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

// SetOnFrameDropped registers a callback invoked once for every frame that
// never reaches the dispatcher: the queue was full (DropQueueFull, called on
// the read-loop goroutine) or the connection dropped with frames still queued
// (DropDrainedOnDisconnect, called from the disconnect path). Set it before
// Start, like the other hooks.
//
// The hook is the per-frame signal; Stats() is the aggregate. On a market-only
// connection a drop is survivable and only needs counting (FrameID gaps in the
// delivered events show it too). On an account-bearing connection a queue-full
// drop fails the connection and the reconnect is announced by a
// HyperliquidGapEvent.
//
// Keep it cheap: it runs on the read loop (a blocking or I/O-bound hook
// throttles frame reads, which is how a connector talks itself into a drop) and
// it runs once per drained frame during disconnect handling.
func (h *WSHyperliquid) SetOnFrameDropped(fn func(DroppedFrame)) {
	h.conn.fa.setOnDrop(fn)
}

// Stats returns the frame accounting snapshot: frames read, dropped and
// drained, the decode queue's depth/capacity/high-water, the current connection
// generation and the disconnect count. Lock-free and safe from any goroutine.
func (h *WSHyperliquid) Stats() Stats {
	return h.conn.fa.snapshot(h.conn.sm.connID.Load())
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
	// Cancel a paced reconnect replay before closing the socket, and again
	// after: a dial racing Close can still run onConnect once. Close waits for
	// the reconnect loop, so after it returns no further onConnect can run.
	first := c.dropConnState(nil)
	c.Close()
	second := c.dropConnState(nil)
	waitConnState(first)
	waitConnState(second)
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
// registered (same channel, coin, user and interval) are skipped, so calling
// this twice is harmless.
//
// A subscription registered while the socket is down is not lost: it is sent
// when the connection comes back. The returned error is therefore about the
// request (unknown channel, malformed coin or user address, bad parameters),
// not about the transport.
func (h *WSHyperliquid) Subscribe(ctx context.Context, subs ...Subscription) error {
	norm, err := normalizeSubscriptions(subs)
	if err != nil {
		return err
	}
	for _, sub := range h.conn.addSubs(norm) {
		h.conn.writeSub(ctx, sub, false)
	}
	return nil
}

// normalizeSubscriptions validates subs and returns a normalized copy: user
// addresses are trimmed and lowercased so one account collapses to one
// canonical key. The caller's slice is not mutated. Any invalid subscription
// fails the whole batch.
func normalizeSubscriptions(subs []Subscription) ([]Subscription, error) {
	norm := make([]Subscription, len(subs))
	copy(norm, subs)
	for i := range norm {
		sub := &norm[i]
		if !IsSubscribable(sub.Type) {
			return nil, fmt.Errorf("hyperliquid: unknown channel %q (subscribable: %s)", sub.Type, channelList())
		}
		switch {
		case isUserChannel(sub.Type):
			// Keyed by user, not coin: a coin would be a caller mixing the two
			// shapes.
			if sub.Coin != "" {
				return nil, fmt.Errorf("hyperliquid: channel %s takes no coin (got %q)", sub.Type, sub.Coin)
			}
			user, err := NormalizeUser(sub.User)
			if err != nil {
				return nil, err
			}
			sub.User = user
		case sub.Type == ChannelAllMids:
			if sub.Coin != "" {
				return nil, fmt.Errorf("hyperliquid: channel %s takes no coin (got %q)", sub.Type, sub.Coin)
			}
		default:
			if err := ValidateCoin(sub.Coin); err != nil {
				return nil, err
			}
		}
		if err := (SubParams{
			Interval: sub.Interval,
			NSigFigs: sub.NSigFigs,
			Mantissa: sub.Mantissa,
			Fast:     sub.Fast,
			User:     sub.User,
		}).validate(sub.Type); err != nil {
			return nil, err
		}
	}
	return norm, nil
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

// SubscribeOrderUpdates streams the account's own order lifecycle updates
// (open, filled, canceled, rejected, …) for user, a 0x-prefixed address. It is
// keyed by user, not coin.
//
// This channel is event-driven: a quiet account produces no frames for
// arbitrarily long stretches, so a connection that carries only user channels
// must NOT enable the data-staleness watchdog — leave Options.DataStaleTimeoutMs
// at its default of 0 (see Options.DataStaleTimeoutMs). The WebSocket-level
// ping/pong keepalive still catches a half-open socket.
func (h *WSHyperliquid) SubscribeOrderUpdates(ctx context.Context, user string) error {
	return h.subscribeChannel(ctx, ChannelOrderUpdates, nil, SubParams{User: user})
}

// UnsubscribeOrderUpdates stops the orderUpdates stream for user.
func (h *WSHyperliquid) UnsubscribeOrderUpdates(ctx context.Context, user string) error {
	return h.unsubscribeChannel(ctx, ChannelOrderUpdates, nil, SubParams{User: user})
}

// SubscribeUserFills streams the account's fills for user, a 0x-prefixed
// address. Keyed by user, not coin. The first frame after subscribing is a
// snapshot of recent fills (IsSnapshot=true on the event); later frames are
// incremental.
//
// Like orderUpdates this channel is event-driven, so a user-only connection
// must leave Options.DataStaleTimeoutMs at 0.
func (h *WSHyperliquid) SubscribeUserFills(ctx context.Context, user string) error {
	return h.subscribeChannel(ctx, ChannelUserFills, nil, SubParams{User: user})
}

// UnsubscribeUserFills stops the userFills stream for user.
func (h *WSHyperliquid) UnsubscribeUserFills(ctx context.Context, user string) error {
	return h.unsubscribeChannel(ctx, ChannelUserFills, nil, SubParams{User: user})
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

	// fa is the frame accounting and the drop hook (provenance.go): FrameIDs,
	// FramesRead/Dropped/Drained, queue depth and high-water, the drop hook and
	// the loss accumulated since the last gap event.
	fa frameAccounting

	// sm is the C4 subscription machinery: connection id, per-connection
	// sent set + paced replay, the shared pacing gate and the ack trackers
	// (subscribe.go).
	sm subState
}

// frame is a raw venue frame plus the local receive time. rx is sampled on the
// read-loop goroutine, before the frame is queued, so queueing delay cannot
// pollute ReceivedAt. connID is the connection generation the frame arrived
// on (see WSHyperliquid.ConnID).
type frame struct {
	data   []byte
	rx     time.Time
	connID uint64
	id     uint64 // FrameID stamped in onMessage before the queue (0 when built by tests)

	// gap is non-nil for the synthetic frame that carries a HyperliquidGapEvent
	// through this same queue (never a wire frame; consumes no FrameID).
	gap *gapFrame
}

func newHLConn(owner *WSHyperliquid) *hlConn {
	c := &hlConn{
		BaseWebSocket: &ws.BaseWebSocket{},
		owner:         owner,
		subs:          make(map[string]Subscription),
		eventCh:       make(chan frame, owner.opts.EventQueueSize),
	}
	c.fa.queueCap = owner.opts.EventQueueSize
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
// Subscribe frames go through the pacing gate (Options.SubscribePaceMs) and
// the per-connection sent set, so a subscription is sent at most once per
// connection even while a reconnect replay is running.
//
// Concurrent writes are safe: coder/websocket serializes frames internally.
func (c *hlConn) writeSub(ctx context.Context, sub Subscription, unsubscribe bool) {
	if unsubscribe {
		c.writeUnsubscribe(ctx, sub)
		return
	}
	out, err := c.sendSubscribe(ctx, nil, sub, nil)
	switch {
	case err != nil:
		slog.Warn("hyperliquid: write subscription frame",
			"subscription", sub.Key(), "unsubscribe", false, "err", err)
	case out == sendDeferred:
		slog.Debug("hyperliquid: subscription deferred (not connected)",
			"subscription", sub.Key(), "unsubscribe", false)
	case out == sendSent:
		slog.Debug("hyperliquid: subscription frame sent", "subscription", sub.Key(), "unsubscribe", false)
	}
}

// writeUnsubscribe sends one unsubscribe frame on the current connection and
// forgets that the subscription was sent on it.
func (c *hlConn) writeUnsubscribe(ctx context.Context, sub Subscription) {
	c.mu.Lock()
	st := c.sm.cur
	if st != nil {
		delete(st.sent, sub.Key())
	}
	c.mu.Unlock()
	if st == nil {
		slog.Debug("hyperliquid: subscription deferred (not connected)",
			"subscription", sub.Key(), "unsubscribe", true)
		return
	}

	data, err := UnsubscribeFrame(sub)
	if err != nil {
		slog.Warn("hyperliquid: build subscription frame", "subscription", sub.Key(), "err", err)
		return
	}
	if err := st.conn.Write(ctx, coderws.MessageText, data); err != nil {
		slog.Warn("hyperliquid: write subscription frame",
			"subscription", sub.Key(), "unsubscribe", true, "err", err)
		return
	}
	slog.Debug("hyperliquid: subscription frame sent", "subscription", sub.Key(), "unsubscribe", true)
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (c *hlConn) onConnect(ctx context.Context, conn *coderws.Conn) error {
	// New connection generation first: frames read on this socket are
	// stamped with it, so acks can be attributed to the right connection.
	st, subs := c.installConnState(ctx, conn)

	// No-silent-loss ordering: this hook runs before BaseWebSocket starts the
	// read loop, so a gap event enqueued here is ahead of every frame of the new
	// generation in the dispatcher's FIFO. It is enqueued after the connection
	// generation is installed (so it carries the new ConnID) and before the
	// subscription replay (which only writes; nothing is read yet).
	c.enqueueGap(st.id)

	if c.paceGap() > 0 {
		// Paced replay on its own goroutine: this hook runs before
		// BaseWebSocket starts the read loop, so sleeping here would stall
		// reading while the venue is already pushing data.
		go c.replay(st, subs)
	} else {
		// Unpaced: re-send every registered subscription inline, as before.
		// One frame per subscription: the venue takes a single subscription
		// object per frame.
		close(st.done)
		for _, sub := range subs {
			data, err := SubscribeFrame(sub)
			if err != nil {
				c.dropConnState(st)
				return fmt.Errorf("hyperliquid: build subscribe frame for %s: %w", sub.Key(), err)
			}
			if err := conn.Write(ctx, coderws.MessageText, data); err != nil {
				c.dropConnState(st)
				return fmt.Errorf("hyperliquid: subscribe %s: %w", sub.Key(), err)
			}
		}
	}
	slog.Info("hyperliquid: connected", "url", c.owner.opts.URL, "subscriptions", len(subs),
		"conn_id", st.id, "paced", c.paceGap() > 0)

	c.startPing(ctx, conn)

	if fn := c.owner.onStatusChange; fn != nil {
		fn(ws.StatusConnected)
	}
	return nil
}

// enqueueGap queues the synthetic HyperliquidGapEvent frame for a reconnected
// account-bearing connection. It is a no-op for the first connection (nothing
// has been lost yet) and for market-only connections (where loss is counted but
// the stream self-heals: the next snapshot supersedes it).
//
// If the queue is full at this instant — possible when the dispatcher is still
// working through the previous generation — it is drained first. Those frames
// belong to a dead socket and are counted as drained on the way out, so
// discarding them here cannot hide a loss: it is added to the gap event's
// LostFrames. Making room guarantees the gap notice is never the thing that
// gets dropped.
func (c *hlConn) enqueueGap(connID uint64) {
	if connID <= 1 || !c.accountBearing() {
		return
	}
	if len(c.eventCh) >= cap(c.eventCh) {
		c.drainEvents()
	}
	prev := connID - 1
	cause := connector.HyperliquidGapDisconnect
	if c.fa.overflowed.Swap(false) {
		cause = connector.HyperliquidGapQueueOverflow
	}
	f := frame{
		rx:     time.Now(),
		connID: connID,
		gap: &gapFrame{
			prevConnID:      prev,
			lostFrames:      c.fa.lostSinceGap.Swap(0),
			cause:           cause,
			accountChannels: true,
		},
	}
	select {
	case c.eventCh <- f:
		c.fa.enqueued()
	default:
		// Unreachable in practice: the queue was just emptied and only this
		// goroutine enqueues before the read loop starts. Count it anyway rather
		// than lose the notice silently.
		slog.Error("hyperliquid: could not enqueue gap event, event queue full",
			"conn_id", connID, "lost_frames", f.gap.lostFrames)
	}
}

// onDisconnect fires on an unexpected drop, but not on a deliberate Close():
// BaseWebSocket suppresses the hook during shutdown. So this is exactly the
// "connection lost" path — stop the ping ticker and the paced replay, and drop
// frames that belong to the dead socket.
func (c *hlConn) onDisconnect(err error) {
	c.stopPing()
	c.dropConnState(nil)
	// Frames that were read but never decoded die here, account frames
	// included. Count them (and report each to the drop hook) so the reconnect
	// can tell the consumer how much it lost; the store below lets onConnect
	// label the next gap event. The drain runs before the disconnect is counted
	// so an observer that sees Disconnects grow also sees FramesDrained settled.
	c.drainEvents()
	c.fa.disc.Add(1)

	if err != nil {
		slog.Info("hyperliquid: disconnected", "reason", err)
	} else {
		slog.Info("hyperliquid: disconnected")
	}
	if fn := c.owner.onDisconnectHook; fn != nil {
		fn(err)
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
	// FrameID is stamped here, on the read loop and before the queue, so a
	// frame that is later dropped still consumes an ID: the gaps a consumer
	// sees in delivered FrameIDs are exactly the frames that never made it, and
	// FramesRead stays the count of everything the socket delivered.
	id := c.fa.frameSeq.Add(1)
	c.fa.read.Add(1)
	connID := c.sm.connID.Load()

	c.rawMu.RLock()
	raw := c.rawHandler
	c.rawMu.RUnlock()
	if raw != nil {
		raw(data, rx)
	}

	f := frame{data: data, rx: rx, connID: connID, id: id}
	select {
	case c.eventCh <- f:
		c.fa.enqueued()
		return nil
	default:
	}

	c.fa.noteDrop(f, DropQueueFull)
	if !c.accountBearing() {
		// Market data: losing a frame is bad but harmless (the next l2Book
		// snapshot supersedes it), and the drop is counted and reported.
		slog.Warn("hyperliquid: dropping frame, event queue full",
			"frame_id", id, "conn_id", connID, "queue_cap", c.fa.cap())
		return nil
	}

	// Account channel: a dropped frame is a lost fill or order update, and the
	// next frame does not repair it. Do not keep reading into a queue that
	// cannot keep up — fail the connection so the reconnect loop re-establishes
	// it, and let onConnect announce the loss with a gap event before any frame
	// of the new generation is decoded.
	c.fa.overflowed.Store(true)
	slog.Warn("hyperliquid: failing account connection, event queue full",
		"frame_id", id, "conn_id", connID, "queue_cap", c.fa.cap(),
		"dropped", c.fa.dropped.Load(), "drained", c.fa.drained.Load())
	c.Disconnect()
	return nil
}

// accountBearing reports whether any account channel (orderUpdates, userFills)
// is registered, i.e. whether a lost frame is a lost account event rather than
// a superseded market snapshot.
func (c *hlConn) accountBearing() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, sub := range c.subs {
		if isUserChannel(sub.Type) {
			return true
		}
	}
	return false
}

// drainEvents discards every queued frame, counting each one and reporting it to
// the drop hook. Called on the disconnect path (and by the connect path when it
// must make room for a gap event), so nothing here may block.
func (c *hlConn) drainEvents() {
	for {
		select {
		case f := <-c.eventCh:
			c.fa.dequeued()
			c.fa.noteDrop(f, DropDrainedOnDisconnect)
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
			c.fa.dequeued()
			h.processFrame(f)
		}
	}
}

// processFrame routes one venue frame by its `channel` field. Every public
// channel is {"channel":"<name>","data":<payload>}.
func (h *WSHyperliquid) processFrame(f frame) {
	if f.gap != nil {
		// Synthetic: the reconnect notice of an account-bearing connection. It
		// travels this FIFO so it is dispatched before any frame of the new
		// connection generation (enqueueGap runs before the read loop starts).
		h.base.DispatchEvent(&connector.HyperliquidGapEvent{
			SeqID:           h.base.NextSeqID(),
			ReceivedAt:      f.rx,
			FrameID:         f.id, // 0: no wire frame behind it
			ConnID:          f.connID,
			PrevConnID:      f.gap.prevConnID,
			Cause:           f.gap.cause,
			LostFrames:      f.gap.lostFrames,
			AccountChannels: f.gap.accountChannels,
		})
		return
	}

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
		h.handleL2Book(env.Data, f)
	case ChannelTrades:
		h.handleTrades(env.Data, f)
	case ChannelBBO:
		h.handleBBO(env.Data, f)
	case ChannelActiveAssetCtx:
		h.handleActiveAssetCtx(env.Data, f)
	case ChannelAllMids:
		h.handleAllMids(env.Data, f)
	case ChannelCandle:
		h.handleCandle(env.Data, f)
	case ChannelOrderUpdates:
		h.handleOrderUpdates(env.Data, f)
	case ChannelUserFills:
		h.handleUserFills(env.Data, f)
	case ServerChannelSubscriptionResponse:
		slog.Debug("hyperliquid: subscription acknowledged", "data", truncate(string(env.Data), 256))
		h.handleSubscriptionResponse(env.Data, f)
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
			FrameID:    f.id,
			ConnID:     f.connID,
			Message:    msg,
		})
		h.attributeErrorFrame(msg, f)
	default:
		slog.Debug("hyperliquid: ignoring channel", "channel", env.Channel)
	}
}

func (h *WSHyperliquid) handleL2Book(data json.RawMessage, f frame) {
	var book L2BookSnapshot
	if err := json.Unmarshal(data, &book); err != nil {
		slog.Warn("hyperliquid: l2Book decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelL2Book, book.Coin, "") {
		slog.Debug("hyperliquid: l2Book frame for unsubscribed coin", "coin", book.Coin)
		return
	}
	ts, synth := msTimeSynth(book.Time, f.rx)
	h.base.DispatchEvent(&connector.HyperliquidBookEvent{
		SeqID:                h.base.NextSeqID(),
		ReceivedAt:           f.rx,
		FrameID:              f.id,
		ConnID:               f.connID,
		Coin:                 book.Coin,
		Timestamp:            ts,
		TimestampSynthesized: synth,
		Bids:                 eventLevels(book.Bids()),
		Asks:                 eventLevels(book.Asks()),
	})
}

func (h *WSHyperliquid) handleTrades(data json.RawMessage, f frame) {
	var trades []Trade
	if err := json.Unmarshal(data, &trades); err != nil {
		slog.Warn("hyperliquid: trades decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	// BatchIndex is the print's position in the frame's array and BatchLen the
	// array's length, so a consumer can tell "these N prints are one venue
	// event" from "these N prints are N events" — the difference between a
	// batch that is complete and one whose tail was cut off by a decode error
	// or an unsubscribed coin (indexes then skip).
	batchLen := len(trades)
	for i := range trades {
		t := trades[i]
		if !h.subscribed(ChannelTrades, t.Coin, "") {
			continue
		}
		ts, synth := msTimeSynth(t.Time, f.rx)
		h.base.DispatchEvent(&connector.HyperliquidTradeEvent{
			SeqID:                h.base.NextSeqID(),
			ReceivedAt:           f.rx,
			FrameID:              f.id,
			ConnID:               f.connID,
			Coin:                 t.Coin,
			Side:                 t.Side,
			Price:                t.Price.String(),
			Size:                 t.Size.String(),
			TradeID:              t.TradeID,
			Hash:                 t.Hash,
			Users:                t.Users,
			Timestamp:            ts,
			TimestampSynthesized: synth,
			BatchIndex:           i,
			BatchLen:             batchLen,
		})
	}
}

func (h *WSHyperliquid) handleBBO(data json.RawMessage, f frame) {
	var bbo BBO
	if err := json.Unmarshal(data, &bbo); err != nil {
		slog.Warn("hyperliquid: bbo decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	if !h.subscribed(ChannelBBO, bbo.Coin, "") {
		slog.Debug("hyperliquid: bbo frame for unsubscribed coin", "coin", bbo.Coin)
		return
	}
	ts, synth := msTimeSynth(bbo.Time, f.rx)
	ev := &connector.HyperliquidBBOEvent{
		SeqID:                h.base.NextSeqID(),
		ReceivedAt:           f.rx,
		FrameID:              f.id,
		ConnID:               f.connID,
		Coin:                 bbo.Coin,
		Timestamp:            ts,
		TimestampSynthesized: synth,
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

func (h *WSHyperliquid) handleActiveAssetCtx(data json.RawMessage, f frame) {
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
	// The venue documents one activeAssetCtx subscription serving both
	// variants, with ctx typed PerpsAssetCtx (funding, openInterest, oraclePx)
	// or SpotAssetCtx (the spot-only circulatingSupply). Classify from the
	// positive spot marker; absence of perp fields is not evidence of anything
	// (GMS ledger D6: a perp ctx that omits funding/oraclePx used to be
	// misfiled as spot).
	h.base.DispatchEvent(&connector.HyperliquidAssetCtxEvent{
		SeqID:        h.base.NextSeqID(),
		ReceivedAt:   f.rx,
		FrameID:      f.id,
		ConnID:       f.connID,
		Coin:         msg.Coin,
		Timestamp:    f.rx, // the venue sends no timestamp on this channel
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

func (h *WSHyperliquid) handleAllMids(data json.RawMessage, f frame) {
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
		ReceivedAt: f.rx,
		FrameID:    f.id,
		ConnID:     f.connID,
		Timestamp:  f.rx, // this channel carries no timestamp either
		Mids:       out,
	})
}

func (h *WSHyperliquid) handleCandle(data json.RawMessage, f frame) {
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
			ReceivedAt: f.rx,
			FrameID:    f.id,
			ConnID:     f.connID,
			Coin:       c.Coin,
			Interval:   c.Interval,
			OpenTime:   msTime(c.OpenTime, f.rx),
			CloseTime:  msTime(c.CloseTime, f.rx),
			Open:       c.Open.String(),
			High:       c.High.String(),
			Low:        c.Low.String(),
			Close:      c.Close.String(),
			Volume:     c.Volume.String(),
			TradeCount: c.TradeCount,
			Timestamp:  f.rx,
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Decode helpers
// ─────────────────────────────────────────────────────────────

// handleOrderUpdates decodes an orderUpdates frame and dispatches one event per
// element. The venue's data is an array of {order, status, statusTimestamp}
// objects; the frame carries no user, so it is dispatched regardless of the
// subscription registry — the spec for these account channels is that a frame
// is always decoded, unlike the coin channels where an unsubscribed coin is
// dropped. A user-channel socket is dedicated to a single account, so a frame
// that arrives on it belongs to that account.
func (h *WSHyperliquid) handleOrderUpdates(data json.RawMessage, f frame) {
	if nullOrEmpty(data) {
		return
	}
	var updates []OrderUpdate
	if err := json.Unmarshal(data, &updates); err != nil {
		slog.Warn("hyperliquid: orderUpdates decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	for i := range updates {
		u := updates[i]
		h.base.DispatchEvent(&connector.HyperliquidOrderUpdateEvent{
			SeqID:           h.base.NextSeqID(),
			ReceivedAt:      f.rx,
			FrameID:         f.id,
			ConnID:          f.connID,
			Coin:            u.Order.Coin,
			Side:            u.Order.Side,
			LimitPx:         u.Order.LimitPx.String(),
			Sz:              u.Order.Sz.String(),
			OrigSz:          u.Order.OrigSz.String(),
			Oid:             u.Order.Oid,
			Timestamp:       u.Order.Timestamp,
			Cloid:           u.Order.Cloid,
			Status:          u.Status,
			StatusTimestamp: u.StatusTimestamp,
		})
	}
}

// handleUserFills decodes a userFills frame and dispatches one event carrying
// the whole batch. The first frame after subscribing is the account's fill
// snapshot (IsSnapshot=true); later frames are incremental. Like orderUpdates
// the frame is always decoded, never gated on the subscription registry.
func (h *WSHyperliquid) handleUserFills(data json.RawMessage, f frame) {
	if nullOrEmpty(data) {
		return
	}
	var msg UserFills
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Warn("hyperliquid: userFills decode failed", "err", err, "data", truncate(string(data), 256))
		return
	}
	fills := make([]connector.HyperliquidFill, 0, len(msg.Fills))
	for _, f := range msg.Fills {
		fills = append(fills, connector.HyperliquidFill{
			Coin:          f.Coin,
			Px:            f.Px.String(),
			Sz:            f.Sz.String(),
			Side:          f.Side,
			Dir:           f.Dir,
			ClosedPnl:     f.ClosedPnl.String(),
			Hash:          f.Hash,
			Fee:           f.Fee.String(),
			FeeToken:      f.FeeToken,
			StartPosition: f.StartPosition.String(),
			Time:          f.Time,
			Oid:           f.Oid,
			Tid:           f.Tid,
			Crossed:       f.Crossed,
			Cloid:         f.Cloid,
		})
	}
	h.base.DispatchEvent(&connector.HyperliquidUserFillsEvent{
		SeqID:      h.base.NextSeqID(),
		ReceivedAt: f.rx,
		FrameID:    f.id,
		ConnID:     f.connID,
		User:       msg.User,
		IsSnapshot: msg.IsSnapshot,
		Fills:      fills,
	})
}

// nullOrEmpty reports whether a frame's data payload is absent or JSON null.
// json.Unmarshal of "null" into a struct leaves it zero with no error, so a
// handler must check before dispatching an empty event.
func nullOrEmpty(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) == 0 || string(trimmed) == "null"
}

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

// ctxIsSpot reports whether an asset context is the spot variant, from the
// positive spot marker the venue documents: SpotAssetCtx carries
// circulatingSupply, PerpsAssetCtx does not (see the H2 report for the docs
// provenance). Anything without the marker is reported as perp — the previous
// heuristic ("no funding and no oraclePx therefore spot") silently mislabelled
// perp contexts that omitted those fields.
func ctxIsSpot(raw json.RawMessage) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	_, hasSpotMarker := probe["circulatingSupply"]
	return hasSpotMarker
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
	t, _ := msTimeSynth(ms, fallback)
	return t
}

// msTimeSynth is msTime plus the provenance flag: synthesized is true exactly
// when the fallback was used, so a consumer can tell an exchange timestamp from
// a local receive time without comparing clocks.
func msTimeSynth(ms int64, fallback time.Time) (t time.Time, synthesized bool) {
	if ms <= 0 {
		return fallback, true
	}
	return time.UnixMilli(ms), false
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
