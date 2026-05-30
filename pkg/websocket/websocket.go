package websocket

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// BaseWebSocket manages a WebSocket connection with automatic reconnection,
// ping/pong keepalive, and status tracking.
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
	OnConnect func(ctx context.Context, conn *websocket.Conn) error

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
	conn   *websocket.Conn
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
		b.conn.Close()
		b.conn = nil
	}
	b.mu.Unlock()

	if hasCancel {
		b.cancel()
		<-b.done
	}
}

// Status returns the current connection status.
func (b *BaseWebSocket) Status() ConnectionStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

// Conn returns the underlying WebSocket connection, or nil if not connected.
// Use this for sending messages.
func (b *BaseWebSocket) Conn() *websocket.Conn {
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
	return conn.WriteMessage(websocket.TextMessage, []byte(data))
}

// SendJSON sends data as a JSON text message. Equivalent to conn.WriteJSON.
func (b *BaseWebSocket) SendJSON(ctx context.Context, v any) error {
	conn := b.Conn()
	if conn == nil {
		return ErrNotConnected
	}
	return conn.WriteJSON(v)
}

// ─────────────────────────────────────────────────────────────
// Internal: dial
// ─────────────────────────────────────────────────────────────

// dialSync performs a single dial attempt and fires the OnConnect hook.
func (b *BaseWebSocket) dialSync(ctx context.Context) error {
	b.setStatus(StatusConnecting)

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Duration(b.opts.ConnectionTimeout)*time.Millisecond)
	conn, _, err := websocket.DefaultDialer.DialContext(dialCtx, b.url, nil)
	dialCancel()

	if err != nil {
		b.setStatus(StatusDisconnected)
		return err
	}

	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	b.setStatus(StatusConnected)

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
	defer b.reconnCleanup()

	for {
		conn := b.Conn()
		if conn == nil {
			return
		}

		// Check context before blocking on read.
		if ctx.Err() != nil {
			return
		}

		if err := conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			return
		}

		// gorilla/websocket panics with "repeated read on failed websocket
		// connection" if the conn was closed by another goroutine between
		// our Conn() call and ReadMessage. Catch the panic gracefully.
		var msg []byte
		readErr := safeCall(func() error {
			var err error
			_, msg, err = conn.ReadMessage()
			return err
		})

		if readErr != nil {
			// Timeout — check if we should shut down.
			if ctx.Err() != nil {
				return
			}
			// Ignore timeout errors, keep reading.
			if websocket.IsCloseError(readErr, websocket.CloseNormalClosure,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				b.safeCallOnDisconnect(readErr)
				return
			}
			if _, ok := readErr.(*websocket.CloseError); ok {
				b.safeCallOnDisconnect(readErr)
				return
			}
			// Network error — connection is dead.
			if !isTimeoutError(readErr) {
				b.safeCallOnDisconnect(readErr)
				return
			}
			// Timeout only — continue loop.
			continue
		}

		if b.OnMessage != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						b.safeCallOnError(&PanicError{Value: r})
					}
				}()
				if err := b.OnMessage(ctx, msg); err != nil {
					b.safeCallOnError(err)
				}
			}()
		}
	}
}

func isTimeoutError(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

// safeCall executes fn and recovers any panic, returning it as an error.
// This is needed because gorilla/websocket panics on repeated reads after
// the connection is closed by another goroutine.
func safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r}
		}
	}()
	return fn()
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
		b.conn.Close()
		b.conn = nil
	}
}

// reconnCleanup is called when the read loop exits. It closes the connection
// so the reconnection loop notices and triggers a re-dial.
func (b *BaseWebSocket) reconnCleanup() {
	b.closeConn()
	b.setStatus(StatusDisconnected)
}

func (b *BaseWebSocket) safeCallOnDisconnect(err error) {
	b.safeCall(func() {
		if b.OnDisconnect != nil {
			b.OnDisconnect(err)
		}
	})
}

func (b *BaseWebSocket) safeCallOnError(err error) {
	b.safeCall(func() {
		if b.OnError != nil {
			b.OnError(err)
		}
	})
}

func (b *BaseWebSocket) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("websocket: handler panic", "error", r)
		}
	}()
	fn()
}
