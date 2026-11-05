package spot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

// User-data stream over the WebSocket API.
//
// Which mechanism is current (decided from the docs):
//
//   - The REST listenKey flow (POST/PUT/DELETE /api/v3/userDataStream and the
//     userDataStream.start/ping/stop WS methods) is deprecated (changelog line
//     26370: "Receiving user data streams on wss://stream.binance.com:9443
//     using a listenKey is now deprecated") and its documentation was removed
//     (line 25918: "all documentation related with listenKey for use on
//     wss://stream.binance.com has been removed"). It is not implemented here.
//   - userDataStream.subscribe needs an authenticated session, and
//     session.logon needs an Ed25519 key (testnet WS API doc, line 116952-116960;
//     changelog line 26376).
//   - userDataStream.subscribe.signature works "without needing to login first.
//     This also doesn't require an Ed25519 API Key, and can work with any API
//     Key type" (changelog line 26092), so it is the one used with HMAC keys.
//     Request: {"id","method":"userDataStream.subscribe.signature","params":
//     {"apiKey","timestamp","signature","recvWindow"?,"filter"?}} (lines
//     117165-117190). The signature is the hex HMAC-SHA256 of the params
//     except signature, sorted alphabetically by name and joined as k=v with
//     "&" (WS API signing, lines 20529-20580; test vector in the tests). A
//     filter, if present, is part of the signed string with sorted keys and no
//     whitespace (line 117079).
//
// Each frame is {"subscriptionId":N,"event":{...}} (line 20086). The response
// to the subscribe request is {"id","status":200,"result":{"subscriptionId":N}}
// or {"id","status":4xx,"error":{"code","msg"}} (lines 19945, 117165-117190).
//
// Connection facts (line 19796-19820): the endpoint is
// wss://ws-api.binance.com:443/ws-api/v3 (Testnet = Demo Mode wss://demo-ws-api.binance.com/ws-api/v3);
// a single connection is only valid for 24 HOURS ("expect to be disconnected
// after the 24-hour mark"); a serverShutdown event announces a server stop and
// the disconnect that follows (line 20124); the server pings every 20 s and
// disconnects when no pong arrives within a minute (coder/websocket answers
// pings automatically inside the read loop). At most 300 connections per
// attempt per 5 minutes per IP (line 20137).
//
// Reconnect: pkg/websocket.BaseWebSocket redials with backoff after any drop,
// including the 24 h cut. OnConnect re-sends a freshly signed subscribe request
// on EVERY new connection, so the subscription is re-established with no extra
// code. Events produced while the socket was down are NOT replayed: the status
// callback reports up=true only after the subscribe is acknowledged, and the
// caller must reconcile (OpenOrders / GetOrder by client order id) on every
// up=true after the first.

const (
	mainnetWSAPI = "wss://ws-api.binance.com:443/ws-api/v3"
	testnetWSAPI = "wss://demo-ws-api.binance.com/ws-api/v3"

	methodSubscribeSignature = "userDataStream.subscribe.signature"

	defaultUserReconnectMs = 1000
	defaultAckTimeout      = 10 * time.Second
)

// UserStreamConfig configures a UserStream.
type UserStreamConfig struct {
	// Client supplies the API key, secret, recvWindow and the SyncTime offset.
	// Required, with credentials.
	Client *Client
	// Network selects the default WS API host when WSURL is empty.
	Network Network
	// WSURL overrides the WS API URL entirely (tests use an httptest server).
	WSURL string
	// EventTypes, if non-empty, subscribes with filter {"e": EventTypes} (e.g.
	// "executionReport", "outboundAccountPosition"). Default: all events.
	EventTypes []string
	// ReconnectIntervalMs is the base reconnect delay in ms. Default 1000.
	ReconnectIntervalMs int64
	// AckTimeout bounds how long Start waits for the subscribe response.
	// Default 10s.
	AckTimeout time.Duration
}

// UserStream is a Binance spot user-data stream (order/account updates) over
// the WebSocket API with an HMAC key, built on websocket.BaseWebSocket.
type UserStream struct {
	cfg    UserStreamConfig
	client *Client
	wsURL  string

	mu      sync.Mutex
	base    *ws.BaseWebSocket
	running bool
	pending string     // id of the in-flight subscribe request on the current connection
	ackCh   chan error // Start waits on it; buffered, written at most once per Start
	started bool       // first ack delivered (Start returned)
	subID   int64
	subbed  bool

	rootCtx    context.Context
	rootCancel context.CancelFunc

	onExecution func(ExecutionReport)
	onPosition  func(OutboundAccountPosition)
	onBalance   func(BalanceUpdate)
	onStatus    func(up bool, err error)

	reqSeq      atomic.Int64
	connects    atomic.Int64
	reconnects  atomic.Int64
	disconnects atomic.Int64
	msgs        atomic.Int64
	bytes       atomic.Int64
	shutdowns   atomic.Int64
	terminated  atomic.Int64
	lastMsgAt   atomic.Int64
	lastErr     atomic.Value // string
}

// NewUserStream validates cfg and builds a UserStream (it does not connect).
func NewUserStream(cfg UserStreamConfig) (*UserStream, error) {
	if cfg.Client == nil {
		return nil, &ConfigError{Reason: "user stream requires a client"}
	}
	if !cfg.Client.hasCredentials() {
		return nil, &ConfigError{Reason: "user stream requires APIKey and APISecret"}
	}
	url := strings.TrimSpace(cfg.WSURL)
	if url == "" {
		switch cfg.Network {
		case "", Mainnet:
			url = mainnetWSAPI
		case Testnet:
			url = testnetWSAPI
		default:
			return nil, &ConfigError{Reason: "unknown network " + string(cfg.Network)}
		}
	}
	for _, e := range cfg.EventTypes {
		if e == "" || strings.ContainsAny(e, "\"\\ \t\r\n") {
			return nil, &ConfigError{Reason: fmt.Sprintf("bad event type %q", e)}
		}
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	return &UserStream{cfg: cfg, client: cfg.Client, wsURL: url}, nil
}

// SetOnExecutionReport registers the order-update callback. Set before Start.
func (u *UserStream) SetOnExecutionReport(fn func(ExecutionReport)) {
	u.mu.Lock()
	u.onExecution = fn
	u.mu.Unlock()
}

// SetOnAccountPosition registers the outboundAccountPosition callback.
func (u *UserStream) SetOnAccountPosition(fn func(OutboundAccountPosition)) {
	u.mu.Lock()
	u.onPosition = fn
	u.mu.Unlock()
}

// SetOnBalanceUpdate registers the balanceUpdate callback.
func (u *UserStream) SetOnBalanceUpdate(fn func(BalanceUpdate)) {
	u.mu.Lock()
	u.onBalance = fn
	u.mu.Unlock()
}

// SetOnStatus registers the status callback: up=true once a subscription is
// acknowledged (on every reconnect too), up=false with the reason after a drop
// or a rejected subscribe. Set before Start.
func (u *UserStream) SetOnStatus(fn func(up bool, err error)) {
	u.mu.Lock()
	u.onStatus = fn
	u.mu.Unlock()
}

// UserStreamStats is a snapshot of health counters.
type UserStreamStats struct {
	Msgs        int64
	Bytes       int64
	LastMsgAt   time.Time
	Reconnects  int64 // connects after the first
	Disconnects int64
	Shutdowns   int64 // serverShutdown events seen
	Terminated  int64 // eventStreamTerminated events seen
	LastError   string
}

// Stats returns the health counters.
func (u *UserStream) Stats() UserStreamStats {
	var last time.Time
	if ns := u.lastMsgAt.Load(); ns > 0 {
		last = time.Unix(0, ns)
	}
	le, _ := u.lastErr.Load().(string)
	return UserStreamStats{
		Msgs: u.msgs.Load(), Bytes: u.bytes.Load(), LastMsgAt: last,
		Reconnects: u.reconnects.Load(), Disconnects: u.disconnects.Load(),
		Shutdowns: u.shutdowns.Load(), Terminated: u.terminated.Load(),
		LastError: le,
	}
}

// SubscriptionID returns the subscriptionId of the current subscription and
// whether one is active.
func (u *UserStream) SubscriptionID() (int64, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.subID, u.subbed
}

// Start connects, sends the signed subscribe request, and returns once the
// venue acknowledges it (or fails). A rejected subscribe at Start is returned
// as an *APIError and the stream is stopped.
func (u *UserStream) Start(ctx context.Context) error {
	u.mu.Lock()
	if u.running {
		u.mu.Unlock()
		return errors.New("spot: user stream already started")
	}
	u.running = true
	u.started = false
	u.ackCh = make(chan error, 1)
	ackCh := u.ackCh
	u.rootCtx, u.rootCancel = context.WithCancel(ctx)
	rootCtx := u.rootCtx
	u.mu.Unlock()

	b := &ws.BaseWebSocket{}
	b.OnConnect = u.onConnect
	b.OnMessage = u.onMessage
	b.OnDisconnect = u.onDisconnect
	b.OnError = u.setLastErr

	opts := ws.DefaultWSOptions()
	// An idle account legitimately sends nothing for hours; liveness is the
	// ping/pong watchdog's job.
	opts.DisableDataStaleWatchdog = true
	opts.ReadLimit = 1 << 20
	if u.cfg.ReconnectIntervalMs > 0 {
		opts.ReconnectInterval = u.cfg.ReconnectIntervalMs
	} else {
		opts.ReconnectInterval = defaultUserReconnectMs
	}

	u.mu.Lock()
	u.base = b
	u.mu.Unlock()

	fail := func(err error) error {
		u.Stop()
		return err
	}
	if err := b.Connect(rootCtx, u.wsURL, opts); err != nil {
		return fail(err)
	}
	select {
	case err := <-ackCh:
		if err != nil {
			return fail(err)
		}
		return nil
	case <-time.After(u.cfg.AckTimeout):
		return fail(errors.New("spot: user stream subscribe not acknowledged in " + u.cfg.AckTimeout.String()))
	case <-rootCtx.Done():
		return fail(rootCtx.Err())
	}
}

// Stop closes the socket (which ends the subscription). Safe to call twice.
func (u *UserStream) Stop() {
	u.mu.Lock()
	if !u.running {
		u.mu.Unlock()
		return
	}
	u.running = false
	base := u.base
	u.base = nil
	cancel := u.rootCancel
	u.subbed = false
	u.mu.Unlock()
	if base != nil {
		base.Close()
	}
	if cancel != nil {
		cancel()
	}
}

// ─────────────────────────────────────────────────────────────
// Subscribe request
// ─────────────────────────────────────────────────────────────

// filterJSON renders {"e":[...]} with sorted keys and no whitespace, or "".
func (u *UserStream) filterJSON() string {
	if len(u.cfg.EventTypes) == 0 {
		return ""
	}
	types := append([]string(nil), u.cfg.EventTypes...)
	sort.Strings(types) // order inside the array is not significant; keep it deterministic
	b, _ := json.Marshal(map[string][]string{"e": types})
	return string(b)
}

// subscribePayload returns the string to sign: every param except signature,
// sorted alphabetically by name, joined k=v with "&" (lines 20529-20580). The
// names that can occur sort as apiKey < filter < recvWindow < timestamp.
func subscribePayload(apiKey, filter string, recvWindowMs, timestamp int64) string {
	parts := []string{"apiKey=" + apiKey}
	if filter != "" {
		parts = append(parts, "filter="+filter)
	}
	parts = append(parts, "recvWindow="+strconv.FormatInt(recvWindowMs, 10), "timestamp="+strconv.FormatInt(timestamp, 10))
	return strings.Join(parts, "&")
}

// buildSubscribe builds the request frame and returns it with its id.
func (u *UserStream) buildSubscribe() (id string, frame []byte, err error) {
	c := u.client
	id = "sub-" + strconv.FormatInt(u.reqSeq.Add(1), 10)
	ts := c.now()
	recv := c.recvWindow.Milliseconds()
	filter := u.filterJSON()
	sig := signHex(c.apiSecret, subscribePayload(c.apiKey, filter, recv, ts))
	params := map[string]any{
		"apiKey":     c.apiKey,
		"timestamp":  ts,
		"recvWindow": recv,
		"signature":  sig,
	}
	if filter != "" {
		params["filter"] = json.RawMessage(filter)
	}
	frame, err = json.Marshal(map[string]any{"id": id, "method": methodSubscribeSignature, "params": params})
	return id, frame, err
}

// ─────────────────────────────────────────────────────────────
// BaseWebSocket hooks
// ─────────────────────────────────────────────────────────────

func (u *UserStream) onConnect(ctx context.Context, conn *coderws.Conn) error {
	if u.connects.Add(1) > 1 {
		u.reconnects.Add(1)
	}
	id, frame, err := u.buildSubscribe()
	if err != nil {
		return err
	}
	u.mu.Lock()
	u.pending = id
	u.subbed = false
	u.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(wctx, coderws.MessageText, frame)
}

func (u *UserStream) onDisconnect(err error) {
	u.disconnects.Add(1)
	u.mu.Lock()
	u.subbed = false
	u.pending = ""
	u.mu.Unlock()
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

// frame is a WS API frame: a response (id/status/result|error) or an event
// (subscriptionId/event). Every key is tagged exactly.
type frame struct {
	ID             string          `json:"id"`
	Status         int             `json:"status"`
	Result         *subResult      `json:"result"`
	Error          *frameError     `json:"error"`
	SubscriptionID *int64          `json:"subscriptionId"`
	Event          json.RawMessage `json:"event"`
}

type subResult struct {
	SubscriptionID int64 `json:"subscriptionId"`
}

type frameError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// eventHead routes an event by its type. "e" and "E" differ only by case, so
// both are declared (see userstream_types.go).
type eventHead struct {
	Type string `json:"e"`
	Time int64  `json:"E"`
}

// handleMessage routes one frame. It is transport-free so decoding can be
// tested without a server.
func (u *UserStream) handleMessage(data []byte) {
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		u.setLastErr(fmt.Errorf("spot: user stream decode: %w", err))
		return
	}
	if len(f.Event) > 0 {
		u.handleEvent(f.Event)
		return
	}
	if f.ID != "" {
		u.handleResponse(f)
	}
}

func (u *UserStream) handleResponse(f frame) {
	u.mu.Lock()
	if f.ID != u.pending {
		u.mu.Unlock()
		return
	}
	u.pending = ""
	if f.Status == 200 && f.Error == nil {
		if f.Result != nil {
			u.subID = f.Result.SubscriptionID
		}
		u.subbed = true
		first := !u.started
		u.started = true
		ack := u.ackCh
		u.mu.Unlock()
		if first && ack != nil {
			ack <- nil
		}
		u.fireStatus(true, nil)
		return
	}
	first := !u.started
	ack := u.ackCh
	base := u.base
	u.mu.Unlock()

	ae := &APIError{HTTPStatus: f.Status, Msg: "user data subscribe rejected"}
	if f.Error != nil {
		ae.Code, ae.Msg = f.Error.Code, f.Error.Msg
	}
	u.setLastErr(ae)
	u.fireStatus(false, ae)
	if first {
		if ack != nil {
			ack <- ae // Start returns it and stops the stream
		}
		return
	}
	// A re-subscribe after a reconnect was rejected (e.g. -1021 clock skew):
	// drop the connection so BaseWebSocket redials with backoff and signs a
	// fresh request. Off the read goroutine.
	if base != nil {
		go base.Disconnect()
	}
}

func (u *UserStream) handleEvent(raw json.RawMessage) {
	var head eventHead
	if err := json.Unmarshal(raw, &head); err != nil {
		u.setLastErr(fmt.Errorf("spot: user stream event decode: %w", err))
		return
	}
	switch head.Type {
	case "executionReport":
		var ev ExecutionReport
		if err := json.Unmarshal(raw, &ev); err != nil {
			u.setLastErr(fmt.Errorf("spot: executionReport decode: %w", err))
			return
		}
		u.mu.Lock()
		fn := u.onExecution
		u.mu.Unlock()
		if fn != nil {
			fn(ev)
		}
	case "outboundAccountPosition":
		var ev OutboundAccountPosition
		if err := json.Unmarshal(raw, &ev); err != nil {
			u.setLastErr(fmt.Errorf("spot: outboundAccountPosition decode: %w", err))
			return
		}
		u.mu.Lock()
		fn := u.onPosition
		u.mu.Unlock()
		if fn != nil {
			fn(ev)
		}
	case "balanceUpdate":
		var ev BalanceUpdate
		if err := json.Unmarshal(raw, &ev); err != nil {
			u.setLastErr(fmt.Errorf("spot: balanceUpdate decode: %w", err))
			return
		}
		u.mu.Lock()
		fn := u.onBalance
		u.mu.Unlock()
		if fn != nil {
			fn(ev)
		}
	case "serverShutdown":
		// The server is about to disconnect us; BaseWebSocket redials and
		// onConnect re-subscribes.
		u.shutdowns.Add(1)
	case "eventStreamTerminated":
		// The subscription ended without the connection closing (e.g. an
		// unsubscribe). Resubscribe by dropping the connection.
		u.terminated.Add(1)
		u.mu.Lock()
		u.subbed = false
		base := u.base
		running := u.running
		u.mu.Unlock()
		u.fireStatus(false, errors.New("spot: eventStreamTerminated"))
		if running && base != nil {
			go base.Disconnect()
		}
	}
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
