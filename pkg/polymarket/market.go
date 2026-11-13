package polymarket

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/internal/framestat"
	"github.com/algoboy-kevin/go-exchange-connector/internal/utils"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	defaultReconnectIntervalMs = 500
	defaultPendingFlushMs      = 100
	marketWSSURL               = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

	// defaultMarketEventQueueSize is the depth of the market channel's decoded
	// frame queue (read loop -> dispatchers) when SetEventQueueSize is unused.
	defaultMarketEventQueueSize = 16384

	// reconnectOnDropMinInterval is the floor between two drop-triggered
	// reconnects. A queue-full burst fires one reconnect per window, never a
	// loop: the reconnect itself is only useful if the rebuilt handshake
	// snapshot can be consumed, so hammering the venue would be counter-
	// productive.
	reconnectOnDropMinInterval = 10 * time.Second
)

// WSPolymarketMarket manages a WebSocket connection to the Polymarket CLOB
// market data channel. It tracks subscribed assets, batches pending
// subscribe/unsubscribe operations, and routes events to connector handlers.
type WSPolymarketMarket struct {
	*ws.BaseWebSocket

	base *connector.Connector

	subMu                 sync.RWMutex
	pendingMu             sync.Mutex
	subscribedAssetIDs    map[string]struct{}
	pendingSubscribeIDs   map[string]struct{}
	pendingUnsubscribeIDs map[string]struct{}

	pendingFlushInterval time.Duration
	flushTicker          *time.Ticker
	flushCancel          context.CancelFunc

	eventCh           chan marketFrame
	dispatcherCancel  context.CancelFunc
	dispatcherWorkers int

	// fa is the frame accounting: FrameID source, exact read/drop counters
	// (this stream never drains, so FramesDrained stays 0), queue depth/high-
	// water, connection generation, drop hook.
	fa framestat.Accounting

	// connSeq hands out connection generations for ConnID.
	connSeq atomic.Uint64

	// reconnectOnDrop (opt-in, SetReconnectOnDrop) makes a queue-full drop
	// force one reconnect so the consumer's book is rebuilt from the handshake
	// snapshot; lastReconnectDrop rate-limits it to one per
	// reconnectOnDropMinInterval.
	reconnectOnDrop   bool
	lastReconnectDrop atomic.Int64 // unix ms of the last drop-triggered reconnect

	eventCount  atomic.Int64
	dropped     atomic.Int64 // total dropped messages (queue full); mirrors Stats().FramesDropped
	lastDropLog atomic.Int64 // unix ms of last rate-limited drop warning

	onStatusChange func(ws.ConnectionStatus)

	// onDisconnectHook receives the error that ended the connection (nil for a
	// deliberate Disconnect()). See SetOnDisconnect.
	onDisconnectHook func(err error)

	latencyTracker    latencyTracker
	latencyCancel     context.CancelFunc
	latencyLogEnabled bool

	// readLimit caps a single inbound message; resolved from
	// Config.ReadLimitBytes (0 → 1 MiB default).
	readLimit int64

	writeMu sync.Mutex // serializes WS writes (onConnect + flushPending)
}

// marketFrame is a raw venue frame plus its provenance, stamped on the read
// loop before the queue so a frame that is later dropped still consumed a
// FrameID and a read.
type marketFrame struct {
	data   []byte
	id     uint64    // FrameID (never reset across reconnects)
	connID uint64    // connection generation the frame arrived on
	rx     time.Time // read time; reported as DroppedFrame.At
}

// SetEventQueueSize sets the depth of the market channel's decoded-frame queue
// (read loop -> dispatchers). Zero or negative restores the default 16384. Call
// it before Start; changing the queue while dispatchers are running would race
// with them.
func (pm *WSPolymarketMarket) SetEventQueueSize(n int) {
	if n <= 0 {
		n = defaultMarketEventQueueSize
	}
	pm.eventCh = make(chan marketFrame, n)
	pm.fa.SetQueueCap(n)
}

// SetReconnectOnDrop is the OPT-IN recovery for a queue-full drop (default
// off). The market channel's price_change frames are incremental deltas: a
// dropped delta leaves the consumer's book silently wrong, and nothing in the
// stream re-sends a delta. With this on, one drop forces one reconnect (the
// existing ReconnectMarket path) so the handshake re-delivers a full `book`
// snapshot for every subscribed asset, at most once per 10s
// (reconnectOnDropMinInterval) so a sustained overload cannot become a
// reconnect loop. Call it before Start.
//
// Residual risk that remains with the option on: a delta that was already lost
// is not re-sent, so a consumer must reconcile its book against the
// BookSnapshotEvent the new connection's handshake delivers.
func (pm *WSPolymarketMarket) SetReconnectOnDrop(on bool) {
	pm.reconnectOnDrop = on
}

// SetOnFrameDropped registers a callback invoked once for every frame that
// never reached a dispatcher, on the goroutine that discarded it: the read loop
// for a frame the queue could not hold (DropQueueFull). This stream never
// discards queued frames on disconnect, so DropDrainedOnDisconnect is never
// reported here and FramesDrained stays 0. It must be cheap (no blocking, no
// I/O): the call runs on the read loop. Counters count drops whether or not a
// hook is set.
func (pm *WSPolymarketMarket) SetOnFrameDropped(fn func(connector.DroppedFrame)) {
	pm.fa.SetOnDrop(fn)
}

// Stats returns the market channel's frame accounting. FramesDropped counts the
// same queue-full drops as the internal dropped counter (they are incremented
// together); FramesRead counts every frame the socket delivered, including
// frames this adapter did not turn into a typed event. FramesDrained is always
// 0 on this stream: queued frames are never discarded on disconnect, they are
// dispatched after it with the ConnID of the generation that read them.
func (pm *WSPolymarketMarket) Stats() connector.FrameStats {
	return pm.fa.Snapshot()
}

// SetOnStatusChange registers a callback that fires whenever the WebSocket
// connection status changes. Useful for tracking uptime, disconnection
// duration, or triggering reconnection logic.
func (pm *WSPolymarketMarket) SetOnStatusChange(fn func(ws.ConnectionStatus)) {
	pm.onStatusChange = fn
}

// SetOnDisconnect registers a callback that receives the error which ended the
// connection, or nil for a deliberate Disconnect(). Unlike SetOnStatusChange it
// carries WHY the connection dropped: the peer's close frame reason is in the
// error message and its code is available via websocket.CloseStatus — e.g. a
// 1001 GoingAway for a server-side idle close, or the venue's policy-close
// reason for a refused subscription. Set it before Start.
func (pm *WSPolymarketMarket) SetOnDisconnect(fn func(err error)) {
	pm.onDisconnectHook = fn
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
		eventCh:               make(chan marketFrame, defaultMarketEventQueueSize),
		readLimit:             resolveReadLimit(cfg.ReadLimitBytes),
	}
	pm.fa.SetQueueCap(defaultMarketEventQueueSize)

	pm.ShouldConnect = func() bool {
		pm.pendingMu.Lock()
		hasPending := len(pm.pendingSubscribeIDs) > 0
		pm.pendingMu.Unlock()
		pm.subMu.RLock()
		hasSubscribed := len(pm.subscribedAssetIDs) > 0
		pm.subMu.RUnlock()
		return hasPending || hasSubscribed
	}

	workers := cfg.DispatcherWorkers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	pm.dispatcherWorkers = workers
	pm.latencyLogEnabled = cfg.LatencyLogEnabled

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

	if pm.latencyLogEnabled {
		latencyCtx, latencyCancel := context.WithCancel(ctx)
		pm.latencyCancel = latencyCancel
		go pm.latencyLogger(latencyCtx)
	}

	return pm.Connect(ctx, wsURL, pm.wsOptions(reconnectIntervalMs))
}

// wsOptions builds the connection options for the market channel. Kept
// separate from Start so the transport settings can be asserted without a live
// socket.
func (pm *WSPolymarketMarket) wsOptions(reconnectIntervalMs int64) ws.WSOptions {
	opts := ws.DefaultWSOptions()
	opts.PingInterval = 5000 // 5s keepalive — server drops idle connections
	// The market channel is event-driven (price_change / last_trade_price
	// only) and can legitimately be quiet for >5s between updates, so it
	// doesn't inherit the tight 5s streaming default for DataStaleTimeout.
	opts.DataStaleTimeout = 30000
	// …and even 30s is wrong for markets that simply have no trades: the
	// channel emits price_change only when a market actually trades, so an
	// illiquid btc-updown-5m window produces zero data for its whole 5m life
	// (verified live 2026-08-26: an active market with a 0.5/0.51 quote had 0
	// trades → 0 price_change events → the 30s staleness watchdog force-
	// reconnected every 30s forever, and a clean reconnect + handshake still
	// delivered nothing because the market wasn't trading). The ping/pong
	// watchdog still catches genuinely dead sockets, so disable the staleness
	// watchdog here.
	opts.DisableDataStaleWatchdog = true
	if reconnectIntervalMs > 0 {
		opts.ReconnectInterval = reconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultReconnectIntervalMs
	}
	// A full-depth book snapshot routinely exceeds coder/websocket's 32 KiB
	// default; an oversized frame fails the read and closes the socket, and the
	// reconnect re-pulls the same snapshot — a drop/reconnect loop that loses
	// exactly the deepest, most liquid books.
	opts.ReadLimit = pm.readLimit
	return opts
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
	if pm.latencyLogEnabled && pm.latencyCancel != nil {
		pm.latencyCancel()
	}
	pm.Close()
	pm.clearSubscriptions()
}

// Subscribe queues asset IDs for subscription and pushes the full updated
// asset set to the server over the live connection — no reconnect needed.
// The market channel honors the handshake form {assets_ids, type,
// custom_feature_enabled} as a full REPLACEMENT of the subscription set over
// an established connection (verified live 2026-08-24). This replaces the old
// forced-reconnect approach, which produced a "constant 3 disconnects per
// market" during rotation (each costing ~1s of market data + a fresh chance
// to hit a real disconnect).
func (pm *WSPolymarketMarket) Subscribe(ctx context.Context, assetIDs []string) {
	pm.subMu.Lock()
	pm.pendingMu.Lock()
	for _, id := range assetIDs {
		delete(pm.pendingUnsubscribeIDs, id)
		if _, ok := pm.subscribedAssetIDs[id]; ok {
			continue
		}
		pm.pendingSubscribeIDs[id] = struct{}{}
	}
	pm.pendingMu.Unlock()
	pm.subMu.Unlock()

	pm.flushPending(ctx)
}

// Unsubscribe queues asset IDs for removal and pushes the full updated asset
// set over the live connection. The incremental operation:"unsubscribe" frame
// is NOT honored by the market channel (verified live 2026-08-24), so removal
// relies on the full-set replace in flushPending.
func (pm *WSPolymarketMarket) Unsubscribe(ctx context.Context, assetIDs []string) {
	pm.pendingMu.Lock()
	for _, id := range assetIDs {
		delete(pm.pendingSubscribeIDs, id)
		pm.pendingUnsubscribeIDs[id] = struct{}{}
	}
	pm.pendingMu.Unlock()

	pm.flushPending(ctx)
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
	conn := pm.Conn()
	if conn == nil {
		// Not connected — the pending set is applied by onConnect on the next
		// reconnect.
		return
	}

	pm.subMu.Lock()
	pm.pendingMu.Lock()
	// Compute the diff against the currently-subscribed set and apply it
	// locally, so the server only ever receives the changed assets.
	var toAdd, toRemove []string
	for id := range pm.pendingSubscribeIDs {
		if _, ok := pm.subscribedAssetIDs[id]; !ok {
			pm.subscribedAssetIDs[id] = struct{}{}
			toAdd = append(toAdd, id)
		}
	}
	for id := range pm.pendingUnsubscribeIDs {
		if _, ok := pm.subscribedAssetIDs[id]; ok {
			delete(pm.subscribedAssetIDs, id)
			toRemove = append(toRemove, id)
		}
	}
	pm.pendingSubscribeIDs = make(map[string]struct{})
	pm.pendingUnsubscribeIDs = make(map[string]struct{})
	changed := len(toAdd) > 0 || len(toRemove) > 0
	pm.pendingMu.Unlock()
	pm.subMu.Unlock()

	if !changed {
		return
	}

	// Send the incremental operation frames over the live connection. The
	// market channel REJECTS the handshake form {assets_ids,type,...} when it
	// is re-sent over an established connection (plain-text "INVALID
	// OPERATION", verified live 2026-08-24) — add/remove must use the
	// operation frames. This avoids a reconnect on every market rotation.
	var firstErr error
	pm.writeMu.Lock()
	defer pm.writeMu.Unlock()
	if len(toAdd) > 0 {
		if err := writeOp(ctx, conn, "subscribe", toAdd); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if len(toRemove) > 0 {
		if err := writeOp(ctx, conn, "unsubscribe", toRemove); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		slog.Warn("market WS: failed to send subscription update", "err", firstErr)
		// Re-queue so the next flush (or reconnect, via onDisconnect) retries.
		pm.pendingMu.Lock()
		for _, id := range toAdd {
			pm.pendingSubscribeIDs[id] = struct{}{}
		}
		for _, id := range toRemove {
			pm.pendingUnsubscribeIDs[id] = struct{}{}
		}
		pm.pendingMu.Unlock()
	}
}

// writeOp sends a single incremental subscribe/unsubscribe frame.
func writeOp(ctx context.Context, conn *coderws.Conn, operation string, ids []string) error {
	msg := struct {
		Operation string   `json:"operation"`
		AssetsIDs []string `json:"assets_ids"`
	}{Operation: operation, AssetsIDs: ids}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, coderws.MessageText, data)
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) onConnect(ctx context.Context, conn *coderws.Conn) error {
	// A new generation: every frame read from this socket is stamped with it,
	// so a consumer can tell a FrameID gap (local loss) from a reconnect (an
	// unknowable gap) and can discard events of a generation it abandoned.
	pm.fa.SetConnID(pm.connSeq.Add(1))

	if pm.onStatusChange != nil {
		pm.onStatusChange(ws.StatusConnected)
	}
	pm.subMu.Lock()
	pm.pendingMu.Lock()
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
	pm.pendingMu.Unlock()
	pm.subMu.Unlock()

	if len(allIDs) == 0 {
		return nil
	}

	ids := make([]string, 0, len(allIDs))
	for id := range allIDs {
		ids = append(ids, id)
	}

	data, err := json.Marshal(subscriptionMessage{AssetsIDs: ids, Type: "market", CustomFeatureEnabled: true})
	if err != nil {
		return err
	}
	pm.writeMu.Lock()
	defer pm.writeMu.Unlock()
	return conn.Write(ctx, coderws.MessageText, data)
}

func (pm *WSPolymarketMarket) onDisconnect(err error) {
	// Queued frames are NOT discarded here: frames already read from the socket
	// are real venue data that the dispatchers still deliver, in order, after
	// the reconnect. A price_change of the dead generation then arrives before
	// the next connection's handshake `book` snapshot, which resets the
	// consumer's book anyway; losing it instead would be a silent loss of
	// already-received data. FramesDrained therefore stays 0 on this stream.
	pm.fa.NoteDisconnect()

	pm.subMu.Lock()
	pm.pendingMu.Lock()
	for id := range pm.subscribedAssetIDs {
		pm.pendingSubscribeIDs[id] = struct{}{}
	}
	pm.subscribedAssetIDs = make(map[string]struct{})
	pm.pendingMu.Unlock()
	pm.subMu.Unlock()

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

	if pm.onDisconnectHook != nil {
		pm.onDisconnectHook(err)
	}
	if pm.onStatusChange != nil {
		pm.onStatusChange(ws.StatusDisconnected)
	}
}

// onMessage runs on the read-loop goroutine. FrameID and ConnID are stamped
// here, before the queue, so a frame the queue cannot hold still consumes an
// ID: FrameID gaps a consumer sees inside one ConnID are exactly the drops
// reported by Stats()/the drop hook.
func (pm *WSPolymarketMarket) onMessage(_ context.Context, data []byte) error {
	id := pm.fa.Stamp()
	f := marketFrame{data: data, id: id, connID: pm.fa.ConnID(), rx: time.Now()}
	select {
	case pm.eventCh <- f:
		pm.fa.Enqueued()
		return nil
	default:
	}
	// Queue full — the "slow consumer" precursor. Track a total and rate-limit
	// the warning so a sustained overload doesn't spam logs. pm.dropped stays
	// the legacy counter and is incremented alongside the shared accounting, so
	// Stats().FramesDropped and pm.dropped always agree.
	pm.dropped.Add(1)
	pm.fa.NoteDrop(f.id, f.connID, f.rx, connector.DropQueueFull)
	if now := time.Now().UnixMilli(); now-pm.lastDropLog.Load() >= 5000 {
		pm.lastDropLog.Store(now)
		slog.Warn("market WS: queue full, dropping frames",
			"frame_id", f.id, "conn_id", f.connID,
			"queue_cap", pm.fa.QueueCap(), "dropped_total", pm.dropped.Load())
	}
	if pm.reconnectOnDrop {
		pm.reconnectAfterDrop(f)
	}
	return nil
}

// reconnectAfterDrop runs the ReconnectOnDrop recovery: at most one reconnect
// per reconnectOnDropMinInterval (10s), decided with a CAS so two drops racing
// in the same window cannot both trigger one. The reconnect is the same
// primitive ReconnectMarket uses (Disconnect -> re-dial -> handshake), so the
// next connection re-sends the full subscription set and the venue answers with
// a fresh `book` snapshot per asset.
func (pm *WSPolymarketMarket) reconnectAfterDrop(f marketFrame) {
	now := time.Now().UnixMilli()
	last := pm.lastReconnectDrop.Load()
	if last != 0 && now-last < reconnectOnDropMinInterval.Milliseconds() {
		return
	}
	if !pm.lastReconnectDrop.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("market WS: queue full, reconnecting so the book is rebuilt from the handshake snapshot",
		"frame_id", f.id, "conn_id", f.connID, "dropped_total", pm.dropped.Load())
	pm.Disconnect()
}

// ─────────────────────────────────────────────────────────────
// Event handlers — parse Polymarket JSON → dispatch typed events
// ─────────────────────────────────────────────────────────────

func (pm *WSPolymarketMarket) handlePriceChange(f marketFrame, raw json.RawMessage) {
	var ev PriceChangeWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse price_change", "err", err)
		return
	}

	exchangeTS := utils.SafeParseTimestamp(ev.Timestamp)

	pm.subMu.RLock()
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
	pm.subMu.RUnlock()

	if len(items) == 0 {
		return
	}

	now := pm.base.Now()
	pm.latencyTracker.record(now.Sub(exchangeTS))

	pm.base.DispatchEvent(&connector.PriceChangeEvent{
		SeqID:      pm.base.NextSeqID(),
		ReceivedAt: now,
		FrameID:    f.id,
		ConnID:     f.connID,
		Market:     ev.Market,
		Timestamp:  exchangeTS,
		Changes:    items,
	})
}

func (pm *WSPolymarketMarket) handleBook(f marketFrame, raw json.RawMessage) {
	var ev BookWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse book", "err", err)
		return
	}
	if !pm.isSubscribed(ev.AssetID) {
		return
	}

	exchangeTS := utils.SafeParseTimestamp(ev.Timestamp)
	now := pm.base.Now()
	pm.latencyTracker.record(now.Sub(exchangeTS))

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
		ReceivedAt: now,
		FrameID:    f.id,
		ConnID:     f.connID,
		Market:     ev.Market,
		AssetID:    ev.AssetID,
		Timestamp:  exchangeTS,
		Hash:       ev.Hash,
		Bids:       bids,
		Asks:       asks,
	})
}

func (pm *WSPolymarketMarket) handleLastTradePrice(f marketFrame, raw json.RawMessage) {
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
		FrameID:    f.id,
		ConnID:     f.connID,
		AssetID:    ev.AssetID,
		Side:       ev.Side,
		Size:       ev.Size,
		Price:      ev.Price,
		Timestamp:  utils.SafeParseTimestamp(ev.Timestamp),
	})
}

func (pm *WSPolymarketMarket) handleTickSizeChange(f marketFrame, raw json.RawMessage) {
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
		FrameID:     f.id,
		ConnID:      f.connID,
		AssetID:     ev.AssetID,
		Market:      ev.Market,
		OldTickSize: ev.OldTickSize,
		NewTickSize: ev.NewTickSize,
		Timestamp:   utils.SafeParseTimestamp(ev.Timestamp),
	})
}

func (pm *WSPolymarketMarket) handleMarketResolved(f marketFrame, raw json.RawMessage) {
	var ev MarketResolvedWSEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		slog.Warn("market WS: failed to parse market_resolved", "err", err)
		return
	}

	pm.base.DispatchEvent(&connector.MarketResolvedEvent{
		SeqID:          pm.base.NextSeqID(),
		ReceivedAt:     pm.base.Now(),
		FrameID:        f.id,
		ConnID:         f.connID,
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
	pm.subMu.RLock()
	defer pm.subMu.RUnlock()
	_, ok := pm.subscribedAssetIDs[assetID]
	return ok
}

func (pm *WSPolymarketMarket) clearSubscriptions() {
	pm.subMu.Lock()
	pm.pendingMu.Lock()
	pm.subscribedAssetIDs = make(map[string]struct{})
	pm.pendingSubscribeIDs = make(map[string]struct{})
	pm.pendingUnsubscribeIDs = make(map[string]struct{})
	pm.pendingMu.Unlock()
	pm.subMu.Unlock()
}

// ─────────────────────────────────────────────────────────────
// Latency monitoring
// ─────────────────────────────────────────────────────────────

// latencyTracker keeps a rolling window of the last 100 event latencies
// (difference between local receive time and exchange timestamp) for
// price_change and book events. Report is called every 5s from the
// latencyLogger goroutine.
type latencyTracker struct {
	mu      sync.Mutex
	samples [100]time.Duration
	count   int
	idx     int // next write position (ring buffer)
}

func (lt *latencyTracker) record(d time.Duration) {
	lt.mu.Lock()
	lt.samples[lt.idx] = d
	lt.idx = (lt.idx + 1) % len(lt.samples)
	if lt.count < len(lt.samples) {
		lt.count++
	}
	lt.mu.Unlock()
}

func (lt *latencyTracker) report() (time.Duration, int) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.count == 0 {
		return 0, 0
	}
	var total int64
	for i := 0; i < lt.count; i++ {
		total += int64(lt.samples[i])
	}
	return time.Duration(total / int64(lt.count)), lt.count
}

func (pm *WSPolymarketMarket) latencyLogger(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			avg, count := pm.latencyTracker.report()
			if count > 0 {
				slog.Info("market WS: event latency",
					"avg_ms", avg.Milliseconds(),
					"samples", count,
				)
			}
		}
	}
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
		case f := <-pm.eventCh:
			pm.fa.Dequeued()
			pm.processMessage(f)
		}
	}
}

// processMessage parses a raw WebSocket message and dispatches typed events.
// Runs off the read-loop goroutine — safe to block on DispatchEvent.
// truncateBytes returns a log-safe prefix of a raw message so protocol
// anomalies (e.g. a plain-text server error) are visible in the log without
// dumping large or binary frames.
func truncateBytes(b []byte) string {
	const max = 200
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}

func (pm *WSPolymarketMarket) processMessage(f marketFrame) {
	data := f.data
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		var single json.RawMessage
		if err2 := json.Unmarshal(data, &single); err2 != nil {
			slog.Warn("market WS: failed to parse message",
				"err", err2,
				"raw", truncateBytes(data))
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
			pm.handlePriceChange(f, r)
		case "book":
			pm.handleBook(f, r)
		case "last_trade_price":
			pm.handleLastTradePrice(f, r)
		case "tick_size_change":
			pm.handleTickSizeChange(f, r)
		case "new_market":
			slog.Debug("market WS: new_market event ignored")
		case "market_resolved":
			pm.handleMarketResolved(f, r)
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
	AssetsIDs            []string `json:"assets_ids"`
	Type                 string   `json:"type"`
	CustomFeatureEnabled bool     `json:"custom_feature_enabled"`
}
