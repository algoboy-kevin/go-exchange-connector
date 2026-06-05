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

	// ConnectionTimeout is how long to wait for the initial dial to succeed.
	// Default: 30s.
	ConnectionTimeout int64 `json:"connection_timeout_ms,omitempty"`

	// PingInterval is how often to send a ping keepalive.
	// If zero, no periodic ping is sent.
	PingInterval int64 `json:"ping_interval_ms,omitempty"`

	// PingMessage is the raw bytes sent as a ping.
	// Default: nil (uses gorilla's built-in ping).
	PingMessage []byte `json:"-"`
}

// DefaultWSOptions returns sensible defaults for a production WebSocket connection.
func DefaultWSOptions() WSOptions {
	return WSOptions{
		ReconnectInterval: 5000,  // 5s
		ConnectionTimeout: 30000, // 30s
		PingInterval:      20000, // 20s
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
