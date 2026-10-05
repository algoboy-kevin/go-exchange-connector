package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	coderws "github.com/coder/websocket"
)

// BaseWebSocket manages a WebSocket connection with automatic reconnection
// and status tracking.
//
// Embed this struct in an exchange-specific manager and set the hook functions
// (OnConnect, OnMessage, OnDisconnect, ShouldConnect) to implement custom
// behaviour — similar to a template method pattern but using Go composition
// with function fields.
//
// Basic usage:
//
//	ws := &websocket.BaseWebSocket{}
//	ws.OnMessage = func(ctx context.Context, data []byte) error {
//	    fmt.Println("received:", string(data))
//	    return nil
//	}
//	ws.Connect(ctx, "wss://example.com/ws", websocket.DefaultWSOptions())
//	defer ws.Close()
type BaseWebSocket struct {
	// ── Hooks (set before Connect) ──────────────────────────

	// OnConnect is called immediately after a successful dial, before the
	// read loop starts. Use it to send handshake/subscription messages.
	// If it returns an error, the connection is closed and reconnection
	// is attempted.
	OnConnect func(ctx context.Context, conn *coderws.Conn) error

	// OnMessage is called for every text or binary message received.
	// Return an error to log it; the read loop continues unless the
	// WebSocket itself reports a read error.
	OnMessage func(ctx context.Context, data []byte) error

	// OnDisconnect is called when the connection drops (read error or
	// clean close). Use it to re-queue active subscriptions. err carries the
	// read error — for a peer close frame the code is available via CloseStatus
	// and the reason is in the message — and is nil for a deliberate
	// Disconnect().
	OnDisconnect func(err error)

	// OnError is called for non-fatal errors (handler panics, bad data).
	OnError func(err error)

	// ShouldConnect returns true if a connection attempt should proceed.
	// Return false to skip (e.g. when there are no pending subscriptions).
	// If nil, defaults to always true.
	ShouldConnect func() bool

	// ── Config (set by Connect) ─────────────────────────────

	url  string
	opts WSOptions

	// ── Internal state ──────────────────────────────────────

	mu     sync.RWMutex
	conn   *coderws.Conn
	status ConnectionStatus
	cancel context.CancelFunc // cancels the entire WS goroutine tree
	done   chan struct{}      // closed when all goroutines exit

	// lastDataAt is the wall-clock time of the last DATA message received,
	// used by the data-staleness watchdog. Empty text heartbeat frames are
	// ignored so a server that only delivers keepalives is still caught.
	lastDataMu sync.RWMutex
	lastDataAt time.Time

	// connEstablishedAt is when the current connection became usable (set by
	// dialSync once the dial and the OnConnect hook both succeeded). When the
	// connection drops, reconnLoop compares the connection's lifetime against
	// MinStableConnectionMs to decide whether it was a success or a flap (see
	// nextDelayAfterDrop). The zero time means no connection has been
	// established since the last drop was accounted for.
	connEstablishedAt time.Time
}

// defaultMinStableConnectionMs is the stability window used when
// WSOptions.MinStableConnectionMs is zero: a connection must stay up at least
// this long before it counts as a successful reconnect that resets the backoff.
const defaultMinStableConnectionMs = 5000

// Connect establishes the WebSocket connection and starts the read loop and
// reconnection watcher. It blocks until the initial dial succeeds or fails.
//
// If the connection drops, BaseWebSocket automatically re-dials after
// opts.ReconnectInterval. Call Close() to permanently shut it down.
func (b *BaseWebSocket) Connect(ctx context.Context, url string, opts WSOptions) error {
	// Store the URL for reconnection.
	b.url = url

	// Sanitise options.
	if opts.ReconnectInterval <= 0 {
		opts.ReconnectInterval = 2000 // 2s default
	}
	if opts.ReconnectMaxInterval <= 0 {
		opts.ReconnectMaxInterval = 30000 // 30s default
	}
	if opts.ConnectionTimeout <= 0 {
		opts.ConnectionTimeout = 5000 // 5s default
	}
	// PongTimeout is derived from the EFFECTIVE PingInterval (RTDS overrides
	// it after taking the defaults), so a silent/half-open socket is declared
	// unresponsive within ~one missed ping + one pong wait. If PingInterval is
	// 0 the ping loop is disabled and PongTimeout is irrelevant.
	if opts.PongTimeout <= 0 {
		opts.PongTimeout = 2 * opts.PingInterval
	}
	if opts.DataStaleTimeout <= 0 {
		opts.DataStaleTimeout = 5000 // 5s default
	}
	if opts.MinStableConnectionMs == 0 {
		opts.MinStableConnectionMs = defaultMinStableConnectionMs
	}
	b.opts = opts

	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.done = make(chan struct{})

	if err := b.dialSync(ctx); err != nil {
		cancel()
		return err
	}

	// Initialize the data-staleness clock so a freshly-connected socket isn't
	// instantly considered stale (lastData() starts at the zero time, which
	// would make time.Since(lastData) effectively infinite). Data messages
	// refresh it; if none arrive within DataStaleTimeout the watchdog fires.
	b.setLastData(time.Now())

	// Start the read loop, reconnection watcher, and data-staleness watchdog.
	go b.readLoop(ctx)
	go b.reconnLoop(ctx)
	go b.dataStaleWatchdog(ctx)

	slog.Info("websocket: connected", "url", url)
	return nil
}

// Close permanently shuts down the WebSocket, stops all goroutines, and
// cancels any pending reconnection. Safe to call multiple times.
func (b *BaseWebSocket) Close() {
	b.mu.Lock()
	hasCancel := b.cancel != nil
	if b.conn != nil {
		b.conn.Close(coderws.StatusNormalClosure, "shutdown")
		b.conn = nil
	}
	b.mu.Unlock()

	if hasCancel {
		b.cancel()
		<-b.done
	}
}

// Disconnect closes the current WebSocket connection without shutting down
// the reconnection loop. The reconnLoop will automatically reconnect.
// Unlike Close(), this does not permanently terminate the WebSocket.
func (b *BaseWebSocket) Disconnect() {
	b.mu.Lock()
	conn := b.conn
	b.conn = nil
	b.status = StatusDisconnected
	b.mu.Unlock()

	if conn != nil {
		// CloseNow, not Close: coder/websocket's Close performs a close
		// handshake and blocks up to ~10s waiting for the peer's close frame.
		// Against a silent/half-open peer that would stall OnDisconnect and
		// the reconnLoop — exactly the failure this self-heal is recovering
		// from. CloseNow drops the connection immediately, unblocking the
		// read loop.
		conn.CloseNow()
	}

	// Fire OnDisconnect so subscriptions get re-queued.
	b.safeCallOnDisconnect(nil)
}

// Status returns the current connection status.
func (b *BaseWebSocket) Status() ConnectionStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

// Conn returns the underlying WebSocket connection, or nil if not connected.
// Use this for sending messages.
func (b *BaseWebSocket) Conn() *coderws.Conn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.conn
}

// SendText sends a text message over the WebSocket. Returns an error if not connected.
func (b *BaseWebSocket) SendText(ctx context.Context, data string) error {
	conn := b.Conn()
	if conn == nil {
		return ErrNotConnected
	}
	return conn.Write(ctx, coderws.MessageText, []byte(data))
}

// SendJSON sends data as a JSON text message.
func (b *BaseWebSocket) SendJSON(ctx context.Context, v any) error {
	conn := b.Conn()
	if conn == nil {
		return ErrNotConnected
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.Write(ctx, coderws.MessageText, data)
}

// ─────────────────────────────────────────────────────────────
// Internal: dial
// ─────────────────────────────────────────────────────────────

// dialSync performs a single dial attempt and fires the OnConnect hook.
func (b *BaseWebSocket) dialSync(ctx context.Context) error {
	b.setStatus(StatusConnecting)

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Duration(b.opts.ConnectionTimeout)*time.Millisecond)
	conn, _, err := coderws.Dial(dialCtx, b.url, nil)
	dialCancel()

	if err != nil {
		b.setStatus(StatusDisconnected)
		return err
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	b.setStatus(StatusConnected)

	// Apply the configured message read limit before the read loop starts.
	// coder/websocket's default (32KB) is too small for some exchange
	// payloads and would force-close the connection with StatusMessageTooBig.
	if b.opts.ReadLimit != 0 {
		conn.SetReadLimit(b.opts.ReadLimit)
	}

	// Start keepalive pings.
	b.startPingLoop(ctx, conn)

	// Fire the OnConnect hook.
	if b.OnConnect != nil {
		if err := b.OnConnect(ctx, conn); err != nil {
			b.closeConn()
			b.setStatus(StatusDisconnected)
			return err
		}
	}

	// Record when this connection became usable. reconnLoop measures the
	// connection's lifetime against MinStableConnectionMs when it drops.
	b.setConnEstablishedAt(time.Now())
	return nil
}

// ─────────────────────────────────────────────────────────────
// Internal: read loop
// ─────────────────────────────────────────────────────────────

func (b *BaseWebSocket) readLoop(ctx context.Context) {
	// Capture the connection at the start so cleanup only closes this
	// specific connection — not a replacement set by a concurrent dialSync.
	conn := b.Conn()
	if conn == nil {
		return
	}
	cleanup := func() {
		b.mu.Lock()
		if b.conn == conn {
			conn.Close(coderws.StatusNormalClosure, "readloop-exit")
			b.conn = nil
			if b.status != StatusDisconnected {
				b.status = StatusDisconnected
			}
		}
		b.mu.Unlock()
	}
	defer cleanup()

	// Single choke point: every read-loop exit reports the disconnect via
	// OnDisconnect — clean close, EOF, or unexpected error — except a
	// deliberate shutdown (context cancelled via Close()) and stale loops
	// (a newer connection already replaced us). This guarantees the
	// exchange-level onDisconnect hook (status-change event, disconnect log,
	// subscription re-queue) always fires when the connection drops.
	var exitErr error
	var shutdown bool
	defer func() {
		if shutdown {
			return // deliberate Close() — no disconnect event
		}
		b.mu.RLock()
		connIsCurrent := b.conn == conn
		alreadyDisconnected := b.status == StatusDisconnected
		b.mu.RUnlock()
		if connIsCurrent && !alreadyDisconnected {
			b.safeCallOnDisconnect(exitErr)
		}
	}()

	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			// Context cancelled — clean shutdown requested via Close().
			if ctx.Err() != nil {
				shutdown = true
				return
			}

			// Log close frame details. 1001 (GoingAway) is sent by some
			// exchanges (e.g. Polymarket's market WS) to close idle connections
			// with no active subscriptions — expected and self-healed by the
			// reconnLoop, so keep it quiet. Other codes are worth a Warn.
			if code := coderws.CloseStatus(err); code != -1 {
				if code == coderws.StatusGoingAway {
					slog.Debug("websocket: server closed connection (going away)",
						"code", code, "reason", err.Error())
				} else {
					slog.Warn("websocket: server closed connection",
						"code", code, "reason", err.Error())
				}
			}

			exitErr = err
			return
		}

		if b.OnMessage != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.safeCallOnError(fmt.Errorf("message handler panic: %v", r))
					}
				}()
				if err := b.OnMessage(ctx, msg); err != nil {
					b.safeCallOnError(err)
				}
			}()
		}
		// Record data arrival for the staleness watchdog. Empty frames (some
		// servers send empty text heartbeats, e.g. RTDS) are skipped so a
		// socket that only delivers keepalives is still treated as stale.
		if len(msg) > 0 {
			b.setLastData(time.Now())
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Internal: reconnection loop
// ─────────────────────────────────────────────────────────────

func (b *BaseWebSocket) reconnLoop(ctx context.Context) {
	defer close(b.done)

	baseDelay := time.Duration(b.opts.ReconnectInterval) * time.Millisecond
	maxDelay := time.Duration(b.opts.ReconnectMaxInterval) * time.Millisecond
	minStable := time.Duration(b.opts.MinStableConnectionMs) * time.Millisecond
	delay := baseDelay

	for {
		// While connected, poll at the base interval so a drop is redialled
		// promptly. The backoff state itself is not reset here — it is decided
		// below, from how long the connection actually lived.
		wait := delay
		if b.Status() == StatusConnected {
			wait = baseDelay
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if b.Status() == StatusConnected {
			continue
		}

		if !b.shouldConnect() {
			continue
		}

		// The previous connection has dropped. Whether that counts as a success
		// (reset the backoff) or a flap (escalate it) is decided from the
		// connection's lifetime — never from the mere fact that the dial
		// succeeded.
		if established := b.establishedAt(); !established.IsZero() {
			b.setConnEstablishedAt(time.Time{})
			lived := time.Since(established)
			var flapped bool
			delay, flapped = nextDelayAfterDrop(delay, baseDelay, maxDelay, minStable, lived)
			if flapped {
				slog.Warn("websocket: connection flapping, backing off",
					"url", b.url,
					"lived", lived.Round(time.Millisecond).String(),
					"next_backoff", delay.String(),
				)
				// Loop back to actually wait the escalated delay. Dialling here
				// would let the cycle run at the poll interval instead, which is
				// the storm this backoff exists to prevent.
				continue
			}
		}

		slog.Debug("websocket: reconnecting", "url", b.url, "delay", delay.String())
		if err := b.dialSync(ctx); err != nil {
			delay = nextBackoff(delay, maxDelay)
			slog.Warn("websocket: reconnection failed", "error", err, "next_backoff", delay.String())
			continue
		}

		// The dial succeeded and OnConnect ran. Restart the read loop; the
		// delay for the next drop is decided above, from this connection's
		// lifetime.
		go b.readLoop(ctx)
	}
}

// nextDelayAfterDrop returns the reconnect delay to use after a connection that
// stayed up for `lived` has dropped, plus whether that drop counts as a flap.
//
// A connection that outlived minStable is a success, so the backoff restarts
// from base. A shorter one is a flap — the venue accepted the handshake and
// then closed the socket (refused subscription, per-IP connection cap, policy
// close) — so the delay escalates instead of resetting. Without that
// distinction such a close resets the backoff on every cycle and the reconnect
// loop degrades into a hot storm that spends the venue's new-connection budget
// and deepens the rejection it is reacting to. minStable <= 0 disables the
// check, restoring the pre-0.7.2 behaviour.
func nextDelayAfterDrop(current, base, max, minStable, lived time.Duration) (delay time.Duration, flapped bool) {
	if minStable > 0 && lived < minStable {
		return nextBackoff(current, max), true
	}
	return base, false
}

// nextBackoff returns the next reconnect delay: d doubled, capped at max.
func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}

// ─────────────────────────────────────────────────────────────
// Internal: ping loop
// ─────────────────────────────────────────────────────────────

// startPingLoop starts a background goroutine that sends WebSocket-level
// ping frames at the configured interval. This keeps the connection alive
// through proxies, load balancers, and server-side idle timeouts.
//
// The loop exits when the context is cancelled (shutdown) or when a ping
// fails (connection dropped). A new ping loop is started automatically
// after each successful reconnection.
func (b *BaseWebSocket) startPingLoop(ctx context.Context, conn *coderws.Conn) {
	if b.opts.PingInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(b.opts.PingInterval) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Per-ping deadline: coder/websocket's Ping waits on its ctx for
				// a pong. Without a deadline this blocks forever on a dead/
				// half-open socket, so the connection would never reconnect.
				pingCtx, cancel := context.WithTimeout(ctx, time.Duration(b.opts.PongTimeout)*time.Millisecond)
				err := conn.Ping(pingCtx)
				cancel()
				if err != nil {
					// Peer didn't pong in time — unresponsive. Only act if this
					// is still the ACTIVE connection: a stale loop left over from
					// a pre-reconnect socket must not kill a newer connection
					// installed by a concurrent dialSync (mirror the readLoop
					// guard).
					if b.Conn() == conn {
						slog.Warn("websocket: pong timeout, forcing reconnect", "err", err)
						b.Disconnect() // closes conn, fires OnDisconnect, reconnLoop redials
					}
					return
				}
			}
		}
	}()
}

// ─────────────────────────────────────────────────────────────
// Internal: helpers
// ─────────────────────────────────────────────────────────────

func (b *BaseWebSocket) shouldConnect() bool {
	if b.ShouldConnect != nil {
		return b.ShouldConnect()
	}
	return true
}

func (b *BaseWebSocket) setStatus(s ConnectionStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.status = s
}

// setConnEstablishedAt records when the current connection became usable.
func (b *BaseWebSocket) setConnEstablishedAt(t time.Time) {
	b.mu.Lock()
	b.connEstablishedAt = t
	b.mu.Unlock()
}

// establishedAt returns when the current connection became usable, or the
// zero time if no connection has been established since the last drop was
// accounted for.
func (b *BaseWebSocket) establishedAt() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connEstablishedAt
}

func (b *BaseWebSocket) closeConn() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		b.conn.Close(coderws.StatusGoingAway, "close")
		b.conn = nil
	}
}

func (b *BaseWebSocket) safeCallOnDisconnect(err error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("websocket: OnDisconnect panic", "err", r)
			}
		}()
		if b.OnDisconnect != nil {
			b.OnDisconnect(err)
		}
	}()
}

func (b *BaseWebSocket) safeCallOnError(err error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("websocket: OnError panic", "err", r)
			}
		}()
		if b.OnError != nil {
			b.OnError(err)
		}
	}()
}

func (b *BaseWebSocket) setLastData(t time.Time) {
	b.lastDataMu.Lock()
	b.lastDataAt = t
	b.lastDataMu.Unlock()
}

func (b *BaseWebSocket) lastData() time.Time {
	b.lastDataMu.RLock()
	defer b.lastDataMu.RUnlock()
	return b.lastDataAt
}

// dataStaleWatchdog force-reconnects if no DATA message arrives within
// opts.DataStaleTimeout, even while control frames (pings/pongs) still flow.
// Guards against a server that keeps the socket alive but silently drops our
// subscription. Exits when the root context is cancelled (shutdown).
func (b *BaseWebSocket) dataStaleWatchdog(ctx context.Context) {
	// Event-driven channels can be legitimately silent (e.g. a market with no
	// trades produces no price_change events). The ping/pong watchdog already
	// catches genuinely dead sockets, so the staleness watchdog is only a
	// heuristic for streaming feeds with a regular cadence — skip it entirely
	// when the caller opts out.
	if b.opts.DisableDataStaleWatchdog {
		return
	}
	interval := time.Duration(b.opts.PingInterval) * time.Millisecond
	if interval <= 0 {
		interval = 15 * time.Second
	}
	timeout := time.Duration(b.opts.DataStaleTimeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if b.Status() != StatusConnected {
				continue
			}
			if since := time.Since(b.lastData()); since >= timeout {
				slog.Warn("websocket: no data, forcing reconnect", "age", since.String())
				b.Disconnect()
			}
		}
	}
}
