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
	// clean close). Use it to re-queue active subscriptions.
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
}

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
	if opts.ConnectionTimeout <= 0 {
		opts.ConnectionTimeout = 5000 // 5s default
	}
	b.opts = opts

	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.done = make(chan struct{})

	if err := b.dialSync(ctx); err != nil {
		cancel()
		return err
	}

	// Start the read loop and reconnection watcher.
	go b.readLoop(ctx)
	go b.reconnLoop(ctx)

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
	if b.conn != nil {
		b.conn.Close(coderws.StatusGoingAway, "reconnect")
		b.conn = nil
	}
	b.status = StatusDisconnected
	b.mu.Unlock()

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

	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			// Context cancelled — clean shutdown requested via Close().
			if ctx.Err() != nil {
				return
			}

			// Log close frame details.
			if code := coderws.CloseStatus(err); code != -1 {
				slog.Warn("websocket: server closed connection",
					"code", code, "reason", err.Error())
			}

			// Only fire OnDisconnect if the connection wasn't already
			// intentionally disconnected (e.g. by Disconnect()). This
			// prevents the double-fire race between Disconnect() and
			// a dying readLoop goroutine.
			b.mu.RLock()
			alreadyDisconnected := b.status == StatusDisconnected
			b.mu.RUnlock()

			if !alreadyDisconnected {
				b.safeCallOnDisconnect(err)
			}
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
	}
}

// ─────────────────────────────────────────────────────────────
// Internal: reconnection loop
// ─────────────────────────────────────────────────────────────

func (b *BaseWebSocket) reconnLoop(ctx context.Context) {
	reconnTicker := time.NewTicker(time.Duration(b.opts.ReconnectInterval) * time.Millisecond)
	defer func() {
		reconnTicker.Stop()
		close(b.done)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnTicker.C:
		}

		// If already connected, skip this tick.
		if b.Status() == StatusConnected {
			continue
		}

		if !b.shouldConnect() {
			continue
		}

		slog.Debug("websocket: reconnecting", "url", b.url)
		if err := b.dialSync(ctx); err != nil {
			slog.Warn("websocket: reconnection failed", "error", err)
			continue
		}

		// Successful reconnection — restart the read loop.
		go b.readLoop(ctx)
	}
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
				if err := conn.Ping(ctx); err != nil {
					// Connection died — ping loop exits naturally.
					// A new one will start after reconnection.
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

func (b *BaseWebSocket) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("websocket: handler panic", "error", r)
		}
	}()
	fn()
}
