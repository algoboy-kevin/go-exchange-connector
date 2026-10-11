package futures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

const (
	// Private user-data stream endpoints (USDⓈ-M futures).
	mainnetPrivateWS = "wss://fstream.binance.com/private"
	testnetPrivateWS = "wss://demo-fstream.binance.com/private"

	defaultKeepAliveInterval = 30 * time.Minute
	defaultUserReconnectMs   = 1000
)

// defaultUserStreamEvents is the events query parameter Binance's split USDⓈ-M
// private stream requires: without it the endpoint delivers nothing on prod.
var defaultUserStreamEvents = []string{"ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE", "listenKeyExpired"}

// UserStreamConfig configures a UserStream.
type UserStreamConfig struct {
	// Client is the signed REST client used to create/keep alive/close the
	// listen key. Required.
	Client *Client
	// Network selects the default private WS host when WSURL is empty
	// (mainnet wss://fstream.binance.com/private, testnet
	// wss://demo-fstream.binance.com/private).
	Network Network
	// WSURL overrides the private WS base URL entirely, e.g.
	// "wss://fstream.binance.com/private". Tests point it at an httptest
	// server.
	WSURL string
	// KeepAliveInterval is how often the listen key is extended with a PUT.
	// Default 30m (Binance expires a key after 60 minutes of silence).
	KeepAliveInterval time.Duration
	// ReconnectIntervalMs is the base reconnect delay for the underlying
	// socket, in milliseconds. Default 1000.
	ReconnectIntervalMs int64
	// Events overrides the URL's events parameter. Default
	// ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/listenKeyExpired.
	Events []string
}

// UserStream is a Binance USDⓈ-M user-data stream (order/account updates) built
// on websocket.BaseWebSocket, which owns the reconnect/backoff/ping machinery.
//
// The socket URL embeds the listen key, and BaseWebSocket dials the URL it was
// given, so a *new* key cannot be applied to a live socket: when the key is
// rotated (a listenKeyExpired event, or a keepalive that failed) the stream
// closes the old BaseWebSocket and connects a fresh one with the new URL. For
// an ordinary transport drop nothing is needed — BaseWebSocket redials the same
// URL itself.
type UserStream struct {
	cfg    UserStreamConfig
	client *Client
	wsBase string
	events []string

	mu        sync.Mutex
	base      *ws.BaseWebSocket
	url       string
	listenKey string
	running   bool

	rootCtx    context.Context
	rootCancel context.CancelFunc
	kaCancel   context.CancelFunc
	rekeyMu    sync.Mutex

	onOrderUpdate   func(OrderTradeUpdate)
	onAccountUpdate func(AccountUpdate)
	onStatus        func(up bool, err error)

	connects    atomic.Int64
	reconnects  atomic.Int64
	disconnects atomic.Int64
	msgs        atomic.Int64
	bytes       atomic.Int64
	lastMsgAt   atomic.Int64 // unix nanoseconds
	lastErr     atomic.Value // string
}

// NewUserStream validates cfg and builds a UserStream (it does not connect).
func NewUserStream(cfg UserStreamConfig) (*UserStream, error) {
	if cfg.Client == nil {
		return nil, &ConfigError{Reason: "user stream requires a REST client"}
	}
	base := strings.TrimRight(cfg.WSURL, "/")
	if base == "" {
		switch cfg.Network {
		case "", Mainnet:
			base = mainnetPrivateWS
		case Testnet:
			base = testnetPrivateWS
		default:
			return nil, &ConfigError{Reason: "unknown network " + string(cfg.Network)}
		}
	}
	keepAlive := cfg.KeepAliveInterval
	if keepAlive <= 0 {
		keepAlive = defaultKeepAliveInterval
	}
	events := cfg.Events
	if len(events) == 0 {
		events = append([]string(nil), defaultUserStreamEvents...)
	}
	cfg.KeepAliveInterval = keepAlive
	return &UserStream{
		cfg:    cfg,
		client: cfg.Client,
		wsBase: base,
		events: events,
	}, nil
}

// SetOnOrderUpdate registers the ORDER_TRADE_UPDATE callback. Set it before
// Start.
func (u *UserStream) SetOnOrderUpdate(fn func(OrderTradeUpdate)) {
	u.mu.Lock()
	u.onOrderUpdate = fn
	u.mu.Unlock()
}

// SetOnAccountUpdate registers the ACCOUNT_UPDATE callback. Set it before
// Start.
func (u *UserStream) SetOnAccountUpdate(fn func(AccountUpdate)) {
	u.mu.Lock()
	u.onAccountUpdate = fn
	u.mu.Unlock()
}

// SetOnStatus registers the connection-status callback (up=true after a
// successful connect, false with the reason after a drop or a failed rekey).
// Set it before Start.
func (u *UserStream) SetOnStatus(fn func(up bool, err error)) {
	u.mu.Lock()
	u.onStatus = fn
	u.mu.Unlock()
}

// UserStreamStats is a snapshot of a UserStream's health counters.
type UserStreamStats struct {
	Msgs        int64     // messages received
	Bytes       int64     // bytes received
	LastMsgAt   time.Time // arrival time of the last message (zero if none)
	Reconnects  int64     // connects after the first (drops + listen-key rekeys)
	Disconnects int64     // connections lost
	LastError   string    // last transport/handler error ("" if none)
}

// Stats returns the stream's health counters.
func (u *UserStream) Stats() UserStreamStats {
	var last time.Time
	if ns := u.lastMsgAt.Load(); ns > 0 {
		last = time.Unix(0, ns)
	}
	lastErr, _ := u.lastErr.Load().(string)
	return UserStreamStats{
		Msgs:        u.msgs.Load(),
		Bytes:       u.bytes.Load(),
		LastMsgAt:   last,
		Reconnects:  u.reconnects.Load(),
		Disconnects: u.disconnects.Load(),
		LastError:   lastErr,
	}
}

// Start creates a listen key, connects the socket, and begins the keepalive
// timer. It returns once the initial dial succeeds (or its error).
func (u *UserStream) Start(ctx context.Context) error {
	u.mu.Lock()
	if u.running {
		u.mu.Unlock()
		return errors.New("futures: user stream already started")
	}
	u.running = true
	u.rootCtx, u.rootCancel = context.WithCancel(ctx)
	u.mu.Unlock()

	if err := u.dial(u.rootCtx); err != nil {
		u.mu.Lock()
		u.running = false
		u.mu.Unlock()
		u.rootCancel()
		return err
	}

	kaCtx, kaCancel := context.WithCancel(u.rootCtx)
	u.mu.Lock()
	u.kaCancel = kaCancel
	u.mu.Unlock()
	go u.keepAliveLoop(kaCtx)
	return nil
}

// Stop closes the socket and invalidates the listen key. Safe to call twice.
func (u *UserStream) Stop() {
	u.mu.Lock()
	if !u.running {
		u.mu.Unlock()
		return
	}
	u.running = false
	kaCancel := u.kaCancel
	base := u.base
	u.base = nil
	cancel := u.rootCancel
	u.mu.Unlock()

	if kaCancel != nil {
		kaCancel()
	}
	if base != nil {
		base.Close()
	}
	if cancel != nil {
		cancel()
	}

	// Best effort, bounded: the root context is already cancelled.
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	_ = u.client.CloseListenKey(ctx)
}

// URL returns the current stream URL (embedded listen key included) — useful
// for logging; never log the key itself in production.
func (u *UserStream) URL() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.url
}

// createKey fetches a fresh listen key. Binance returns the SAME key when the
// existing one is still valid, so callers must not assume it changed.
func (u *UserStream) createKey(ctx context.Context) (string, error) {
	key, err := u.client.NewListenKey(ctx)
	if err != nil {
		return "", err
	}
	if key == "" {
		return "", errors.New("futures: empty listen key from REST")
	}
	return key, nil
}

// dial fetches a listen key and (re)connects the socket to its URL, replacing
// any existing BaseWebSocket. The old socket is closed first so its read loop,
// reconnect loop and watchdog all stop before a new one starts.
func (u *UserStream) dial(ctx context.Context) error {
	key, err := u.createKey(ctx)
	if err != nil {
		return err
	}
	return u.connect(ctx, key)
}

// connect (re)connects with an explicit listen key.
func (u *UserStream) connect(ctx context.Context, key string) error {
	u.mu.Lock()
	if !u.running {
		u.mu.Unlock()
		return errors.New("futures: user stream stopped")
	}
	old := u.base
	u.base = nil
	u.listenKey = key
	u.url = u.buildURL(key)
	streamURL := u.url
	u.mu.Unlock()

	if old != nil {
		old.Close()
	}

	b := &ws.BaseWebSocket{}
	b.OnConnect = u.onConnect
	b.OnMessage = u.onMessage
	b.OnDisconnect = u.onDisconnect
	b.OnError = u.setLastErr

	opts := ws.DefaultWSOptions()
	// An idle account legitimately sends nothing for hours; the staleness
	// watchdog would force a reconnect on every quiet period.
	opts.DisableDataStaleWatchdog = true
	opts.PingInterval = 5000 // Binance drops idle connections
	opts.ReadLimit = 1 << 20 // 1 MiB per message
	if u.cfg.ReconnectIntervalMs > 0 {
		opts.ReconnectInterval = u.cfg.ReconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultUserReconnectMs
	}

	if err := b.Connect(ctx, streamURL, opts); err != nil {
		return err
	}

	u.mu.Lock()
	stopped := !u.running
	if !stopped {
		u.base = b
	}
	u.mu.Unlock()
	if stopped {
		// Stop() won the race while this dial was in flight — don't leak the
		// socket we just opened.
		b.Close()
		return errors.New("futures: user stream stopped during connect")
	}
	return nil
}

// buildURL renders <private base>/ws?listenKey=…&events=…. The events value is
// slash-separated; url.Values percent-encodes "/" as %2F, which is equivalent
// but not what Binance documents, so the literal slash is restored afterwards
// (a "/" is legal inside a query value).
func (u *UserStream) buildURL(key string) string {
	v := url.Values{}
	v.Set("listenKey", key)
	v.Set("events", strings.Join(u.events, "/"))
	q := strings.ReplaceAll(v.Encode(), "%2F", "/")
	return u.wsBase + "/ws?" + q
}

// Events returns the events parameter the stream subscribes to.
func (u *UserStream) Events() []string { return append([]string(nil), u.events...) }

// ─────────────────────────────────────────────────────────────
// Rekey: a rotated listen key needs a new socket
// ─────────────────────────────────────────────────────────────

// rekey obtains a (possibly new) listen key and reconnects with it. It is
// called on a listenKeyExpired event and after a keepalive error, and is
// serialized so a burst of triggers rotates once.
func (u *UserStream) rekey(reason string) {
	u.rekeyMu.Lock()
	defer u.rekeyMu.Unlock()

	u.mu.Lock()
	running := u.running
	ctx := u.rootCtx
	u.mu.Unlock()
	if !running || ctx == nil {
		return
	}

	// Retry with backoff until a key is obtained and the socket is connected:
	// a single failed attempt (REST hiccup) must not leave the exec stream
	// dead until the next keepalive tick (30 min).
	backoff := rekeyRetryMin
	for {
		err := u.rekeyOnce(ctx, reason)
		if err == nil {
			return
		}
		u.setLastErr(err)
		u.fireStatus(false, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		u.mu.Lock()
		running := u.running
		u.mu.Unlock()
		if !running {
			return
		}
		if backoff *= 2; backoff > rekeyRetryMax {
			backoff = rekeyRetryMax
		}
	}
}

// Rekey retry backoff bounds (vars so tests can shrink them).
var (
	rekeyRetryMin = time.Second
	rekeyRetryMax = 30 * time.Second
)

func (u *UserStream) rekeyOnce(ctx context.Context, reason string) error {
	key, err := u.createKey(ctx)
	if err != nil {
		return fmt.Errorf("futures: rekey after %s: %w", reason, err)
	}
	if err := u.connect(ctx, key); err != nil {
		return fmt.Errorf("futures: reconnect after %s: %w", reason, err)
	}
	return nil
}

// keepAliveLoop extends the listen key on a fixed interval. A failed PUT means
// the key is gone, so it rotates immediately.
func (u *UserStream) keepAliveLoop(ctx context.Context) {
	t := time.NewTicker(u.cfg.KeepAliveInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := u.client.KeepAliveListenKey(ctx); err != nil {
				u.setLastErr(fmt.Errorf("futures: listen key keepalive: %w", err))
				u.fireStatus(false, err)
				u.rekey("keepalive error")
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (u *UserStream) onConnect(_ context.Context, _ *coderws.Conn) error {
	if u.connects.Add(1) > 1 {
		u.reconnects.Add(1)
	}
	u.fireStatus(true, nil)
	return nil
}

func (u *UserStream) onDisconnect(err error) {
	u.disconnects.Add(1)
	if err != nil {
		u.setLastErr(err)
	}
	u.fireStatus(false, err)
}

func (u *UserStream) onMessage(_ context.Context, data []byte) error {
	u.msgs.Add(1)
	u.bytes.Add(int64(len(data)))
	u.lastMsgAt.Store(time.Now().UnixNano())
	u.handleMessage(data)
	return nil
}

// userStreamEnvelope is the leading part of every user-data frame — enough to
// route it. Every top-level key that these frames carry is declared with an
// explicit tag: the keys differ only by case ("e" vs "E") and encoding/json
// falls back to a case-INSENSITIVE match when there is no exact one, so a
// struct holding only "e" would try to read the numeric "E" into a string and
// fail the whole frame.
type userStreamEnvelope struct {
	Event           string `json:"e"`
	EventTime       int64  `json:"E"`
	TransactionTime int64  `json:"T"`
}

// handleMessage parses one user-stream frame and invokes the matching callback.
// It is deliberately transport-free so the payload decoding can be tested
// without a server.
func (u *UserStream) handleMessage(data []byte) {
	var env userStreamEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		u.setLastErr(fmt.Errorf("futures: user stream decode: %w", err))
		return
	}

	switch env.Event {
	case "ORDER_TRADE_UPDATE":
		var ev OrderTradeUpdate
		if err := json.Unmarshal(data, &ev); err != nil {
			u.setLastErr(fmt.Errorf("futures: ORDER_TRADE_UPDATE decode: %w", err))
			return
		}
		if fn := u.orderUpdateFn(); fn != nil {
			fn(ev)
		}
	case "ACCOUNT_UPDATE":
		var ev AccountUpdate
		if err := json.Unmarshal(data, &ev); err != nil {
			u.setLastErr(fmt.Errorf("futures: ACCOUNT_UPDATE decode: %w", err))
			return
		}
		if fn := u.accountUpdateFn(); fn != nil {
			fn(ev)
		}
	case "listenKeyExpired":
		// The socket is about to be closed by the venue; rotate the key and
		// reconnect on a fresh socket.
		go u.rekey("listenKeyExpired")
	}
}

// ─────────────────────────────────────────────────────────────
// Hook plumbing
// ─────────────────────────────────────────────────────────────

func (u *UserStream) orderUpdateFn() func(OrderTradeUpdate) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.onOrderUpdate
}

func (u *UserStream) accountUpdateFn() func(AccountUpdate) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.onAccountUpdate
}

func (u *UserStream) fireStatus(up bool, err error) {
	u.mu.Lock()
	fn := u.onStatus
	u.mu.Unlock()
	if fn != nil {
		fn(up, err)
	}
}

func (u *UserStream) setLastErr(err error) {
	if err == nil {
		return
	}
	u.lastErr.Store(err.Error())
}
