// Package websocket provides a reusable WebSocket connection base with
// automatic reconnection, ping/pong keepalive, and status tracking.
//
// Exchange-specific implementations embed BaseWebSocket and wire their own
// message handlers, subscription logic, and protocol details.
//
// Usage:
//
//	ws := &websocket.BaseWebSocket{}
//	ws.OnMessage = func(ctx context.Context, data []byte) error {
//	    fmt.Println("received:", string(data))
//	    return nil
//	}
//	ws.Connect(ctx, "wss://example.com/ws", websocket.DefaultWSOptions())
//	defer ws.Close()
package websocket

import "context"

// ─────────────────────────────────────────────────────────────
// Connection status
// ─────────────────────────────────────────────────────────────

// ConnectionStatus represents the state of a WebSocket connection.
type ConnectionStatus string

const (
	StatusDisconnected ConnectionStatus = "disconnected"
	StatusConnecting   ConnectionStatus = "connecting"
	StatusConnected    ConnectionStatus = "connected"
)

// ─────────────────────────────────────────────────────────────
// Options
// ─────────────────────────────────────────────────────────────

// WSOptions configures a BaseWebSocket.
type WSOptions struct {
	// ReconnectInterval is how often to check for reconnection.
	// Default: 5s.
	ReconnectInterval int64 `json:"reconnect_interval_ms,omitempty"`

	// ReconnectMaxInterval caps the exponential reconnect backoff in
	// milliseconds. After each consecutive failed reconnect the interval is
	// doubled, up to this maximum; a successful connection resets it to
	// ReconnectInterval. Default: 30s.
	ReconnectMaxInterval int64 `json:"reconnect_max_interval_ms,omitempty"`

	// MinStableConnectionMs is how long a connection must survive before it
	// counts as a successful reconnect that resets the backoff to
	// ReconnectInterval. A venue that accepts the handshake and then closes the
	// socket immediately — a refused subscription, a per-IP connection cap, a
	// policy close — produces a "successful" dial every cycle, which would
	// reset the backoff and turn the reconnect loop into a hot storm that
	// spends the venue's new-connection budget and deepens the rejection it is
	// reacting to. Connections shorter than this escalate the backoff instead.
	// Default: 5s. Negative disables the check (every successful dial resets
	// the backoff, the pre-0.7.2 behaviour). Keep the default well below any
	// venue idle-close interval so a server that closes idle sockets on a fixed
	// cadence is not mistaken for a flapping one.
	MinStableConnectionMs int64 `json:"min_stable_connection_ms,omitempty"`

	// ConnectionTimeout is how long to wait for the initial dial to succeed.
	// Default: 30s.
	ConnectionTimeout int64 `json:"connection_timeout_ms,omitempty"`

	// PingInterval is how often to send a WebSocket-level ping keepalive.
	// If zero, coder/websocket's default of 30s is used.
	PingInterval int64 `json:"ping_interval_ms,omitempty"`

	// PongTimeout is how long to wait for a pong after sending a ping before
	// declaring the connection unresponsive and forcing a reconnect. A silent,
	// half-open socket delivers no pong; without this the ping loop blocks
	// forever (it waits on the root context) and a hung connection never
	// reconnects. If zero, defaults to 2 * PingInterval at Connect time.
	PongTimeout int64 `json:"pong_timeout_ms,omitempty"`

	// DataStaleTimeout is how long without receiving any DATA message before
	// the connection is declared stale and force-reconnected, even while
	// control frames (pings/pongs) still flow. Catches a server that keeps the
	// socket alive but silently drops our subscription. Empty heartbeat frames
	// do not count as data. If zero, defaults to 5s at Connect time. Pick a
	// value safely above the feed's expected cadence.
	DataStaleTimeout int64 `json:"data_stale_timeout_ms,omitempty"`

	// DisableDataStaleWatchdog turns off the data-staleness watchdog entirely.
	// Use for event-driven channels where markets can legitimately be silent
	// for extended periods (e.g. Polymarket's market channel emits price_change
	// only when a market actually trades — a quiet, illiquid market produces no
	// data for minutes, which the staleness watchdog would otherwise misread as
	// a dead connection and force-reconnect every DataStaleTimeout, forever).
	// Connection liveness is still guaranteed by the ping/pong watchdog, so a
	// genuinely dead socket is still caught and reconnected.
	DisableDataStaleWatchdog bool `json:"disable_data_stale_watchdog,omitempty"`

	// ReadLimit is the max number of bytes to read for a single message.
	// If zero, coder/websocket's default of 32768 bytes applies. Set to -1
	// to disable the limit entirely. Raise this for exchanges that send
	// large frames (e.g. Binance @depth@100ms diffs), which exceed 32KB and
	// force-close the connection with StatusMessageTooBig.
	ReadLimit int64 `json:"read_limit_bytes,omitempty"`

	// ── Optional control hooks (nil / false = historical behaviour) ──

	// DialGate, if non-nil, is consulted before EVERY dial attempt: the
	// initial Connect and every reconnect. It may block (a rate limiter's
	// Take) and must respect ctx — Close() cancels it. A non-nil error means
	// "do not dial now":
	//
	//   - Connect returns the error wrapped, so errors.Is(err, sentinel)
	//     matches the gate's own error;
	//   - the reconnect loop treats it as a failed attempt: it logs, escalates
	//     the backoff exactly like a failed dial (see ReconnectMaxInterval) and
	//     waits before trying again, so a persistent refusal can never spin
	//     into a tight loop. Cancelling ctx stops the loop.
	//
	// Use it to keep one shared IP/connection budget across every venue the
	// process talks to, and to hold redials while an upstream supervisor is
	// restarting.
	//
	// The field is a func, so it is excluded from JSON.
	DialGate func(ctx context.Context) error `json:"-"`

	// SendGate, if non-nil, is consulted before every outbound frame this
	// package writes: WebSocket ping frames (see PingInterval) and the
	// SendText/SendJSON helpers. n is the payload size in bytes of the frame
	// about to be written (0 for control frames such as ping).
	// The gate is called exactly once per outbound frame, so a message-count
	// budget (for example a per-IP messages/minute limiter) should count calls
	// and treat n as informational.
	//
	// A non-nil error SKIPS that frame only — it never closes the connection
	// and never stops the ping loop by itself. The denial is reported through
	// OnError, and SendText/SendJSON also return it to the caller (a write
	// helper must not claim success for a frame it never wrote).
	//
	// Writes made directly on the connection returned by Conn() (every
	// exchange package's own subscribe/replay/ping code) are NOT covered by
	// this gate.
	//
	// The field is a func, so it is excluded from JSON.
	SendGate func(ctx context.Context, n int) error `json:"-"`

	// NoReconnect disables automatic reconnection: after a drop the status
	// becomes (and stays) disconnected, OnDisconnect fires once with the read
	// error and nothing redials. Close() still returns promptly and stops the
	// remaining goroutines. Use it when the caller owns the supervisor
	// (go-mm-driver, GMS: stop the adapter, back off, dial a new one) and a
	// second reconnect policy underneath that one would fight it.
	NoReconnect bool `json:"no_reconnect,omitempty"`
}

// DefaultWSOptions returns sensible defaults for a production WebSocket connection.
func DefaultWSOptions() WSOptions {
	return WSOptions{
		ReconnectInterval:    5000,  // 5s
		ReconnectMaxInterval: 30000, // 30s
		ConnectionTimeout:    30000, // 30s
		PingInterval:         20000, // 20s
	}
}

// ─────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────

// ErrNotConnected is returned when attempting to send on a closed WebSocket.
var ErrNotConnected = &WSocketError{"not connected"}

// WSocketError is an error type for WebSocket operations.
type WSocketError struct {
	Msg string
}

func (e *WSocketError) Error() string { return "websocket: " + e.Msg }
