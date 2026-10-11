package futures

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Fake servers
// ─────────────────────────────────────────────────────────────

// restStub is a listenKey REST stub. It answers POST /fapi/v1/listenKey with
// keys[call] (clamping at the last entry, so a single-entry list models Binance
// returning the SAME key while the old one is still valid), PUT with the
// configured status, and DELETE with 200.
type restStub struct {
	keys            []string
	keepAliveStatus int
	keepAliveBody   string
	failPost        map[int]bool // 1-based POST call numbers answered with HTTP 500

	mu             sync.Mutex
	newKeyCalls    int
	keepAliveCalls int
	closeCalls     int
	lastMethod     string
}

func (s *restStub) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.lastMethod = r.Method
	switch r.Method {
	case http.MethodPost:
		s.newKeyCalls++
		if s.failPost[s.newKeyCalls] {
			s.mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":-1001,"msg":"Internal error"}`))
			return
		}
		idx := s.newKeyCalls - 1
		if idx >= len(s.keys) {
			idx = len(s.keys) - 1
		}
		key := ""
		if len(s.keys) > 0 {
			key = s.keys[idx]
		}
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{"listenKey":"` + key + `"}`))
	case http.MethodPut:
		s.keepAliveCalls++
		status := s.keepAliveStatus
		body := s.keepAliveBody
		s.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			if body == "" {
				body = `{"code":-1125,"msg":"This listenKey does not exist."}`
			}
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	case http.MethodDelete:
		s.closeCalls++
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	default:
		s.mu.Unlock()
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *restStub) counts() (newKeys, keepAlives, closes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.newKeyCalls, s.keepAliveCalls, s.closeCalls
}

func newRestStub(t *testing.T, stub *restStub) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(stub.handler))
	t.Cleanup(srv.Close)
	return srv
}

// wsTap is a WebSocket test server that records the listen key each connection
// arrived with and optionally runs per-connection behaviour.
type wsTap struct {
	onConn func(idx int, c *coderws.Conn)

	mu     sync.Mutex
	conns  int
	closed int
	keys   []string
	query  []string

	accepted chan struct{}
}

func newWSTap() *wsTap {
	return &wsTap{accepted: make(chan struct{}, 32)}
}

func (s *wsTap) handler(w http.ResponseWriter, r *http.Request) {
	c, err := coderws.Accept(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.conns++
	idx := s.conns
	s.keys = append(s.keys, r.URL.Query().Get("listenKey"))
	s.query = append(s.query, r.URL.RawQuery)
	s.mu.Unlock()

	select {
	case s.accepted <- struct{}{}:
	default:
	}

	if s.onConn != nil {
		s.onConn(idx, c)
	}
	// Keep the handler (and the hijacked connection) alive until the client
	// goes away; coder/websocket processes control frames inside Read.
	for {
		if _, _, err := c.Read(context.Background()); err != nil {
			s.mu.Lock()
			s.closed++
			s.mu.Unlock()
			return
		}
	}
}

// closedCount returns how many connections the client has torn down.
func (s *wsTap) closedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *wsTap) snapshot() (conns int, keys, query []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns, append([]string(nil), s.keys...), append([]string(nil), s.query...)
}

func newWSTapServer(t *testing.T, tap *wsTap) (wsURL string, srv *httptest.Server) {
	t.Helper()
	srv = httptest.NewServer(http.HandlerFunc(tap.handler))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), srv
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}

func testRESTClient(t *testing.T, srvURL string) *Client {
	t.Helper()
	c, err := New(Config{BaseURL: srvURL, APIKey: "TESTKEY", APISecret: testSecret})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// newDecodeStream builds a UserStream with no live socket; handleMessage is
// exercised directly.
func newDecodeStream(t *testing.T) *UserStream {
	t.Helper()
	u, err := NewUserStream(UserStreamConfig{
		Client: testRESTClient(t, "http://127.0.0.1:0"),
		WSURL:  "ws://127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	return u
}

// ─────────────────────────────────────────────────────────────
// URL + config
// ─────────────────────────────────────────────────────────────

func TestUserStreamBuildURL(t *testing.T) {
	c := testRESTClient(t, "http://127.0.0.1:0")

	u, err := NewUserStream(UserStreamConfig{Client: c})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	want := "wss://fstream.binance.com/private/ws?events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/listenKeyExpired&listenKey=KEY1"
	if got := u.buildURL("KEY1"); got != want {
		t.Errorf("mainnet URL:\n got %s\nwant %s", got, want)
	}
	if evs := u.Events(); len(evs) != 3 || evs[0] != "ORDER_TRADE_UPDATE" || evs[2] != "listenKeyExpired" {
		t.Errorf("default events = %v", evs)
	}

	tt, err := NewUserStream(UserStreamConfig{Client: c, Network: Testnet})
	if err != nil {
		t.Fatalf("NewUserStream(testnet): %v", err)
	}
	if got := tt.buildURL("K"); !strings.HasPrefix(got, "wss://demo-fstream.binance.com/private/ws?") {
		t.Errorf("testnet URL = %q", got)
	}

	over, err := NewUserStream(UserStreamConfig{Client: c, WSURL: "wss://demo-fstream.binance.com/private"})
	if err != nil {
		t.Fatalf("NewUserStream(WSURL): %v", err)
	}
	if got := over.buildURL("K"); !strings.HasPrefix(got, "wss://demo-fstream.binance.com/private/ws?") {
		t.Errorf("WSURL override not used: %q", got)
	}

	custom, err := NewUserStream(UserStreamConfig{Client: c, Events: []string{"ORDER_TRADE_UPDATE"}})
	if err != nil {
		t.Fatalf("NewUserStream(Events): %v", err)
	}
	if got := custom.buildURL("K"); !strings.Contains(got, "events=ORDER_TRADE_UPDATE&") {
		t.Errorf("custom events not used: %q", got)
	}

	if _, err := NewUserStream(UserStreamConfig{}); err == nil {
		t.Error("NewUserStream without a client = nil error, want error")
	}
	if _, err := NewUserStream(UserStreamConfig{Client: c, Network: "devnet"}); err == nil {
		t.Error("NewUserStream with an unknown network = nil error, want error")
	}
}

// TestUserStreamURLSeenByServer proves the live dial carries the listen key and
// the three events (with a literal slash, as Binance documents).
func TestUserStreamURLSeenByServer(t *testing.T) {
	stub := &restStub{keys: []string{"KEY-URL"}}
	rest := newRestStub(t, stub)

	tap := newWSTap()
	wsURL, _ := newWSTapServer(t, tap)

	u, err := NewUserStream(UserStreamConfig{
		Client:              testRESTClient(t, rest.URL),
		WSURL:               wsURL,
		ReconnectIntervalMs: 20,
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()

	waitFor(t, 2*time.Second, "first connection", func() bool {
		n, _, _ := tap.snapshot()
		return n >= 1
	})
	_, keys, query := tap.snapshot()
	if keys[0] != "KEY-URL" {
		t.Errorf("listen key seen by server = %q, want KEY-URL", keys[0])
	}
	q := query[0]
	if !strings.Contains(q, "events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/listenKeyExpired") {
		t.Errorf("query missing the three literal-slash events: %q", q)
	}
	if !strings.Contains(q, "listenKey=KEY-URL") {
		t.Errorf("query missing listenKey: %q", q)
	}
}

// ─────────────────────────────────────────────────────────────
// ORDER_TRADE_UPDATE fixtures
// ─────────────────────────────────────────────────────────────

// TestUserStreamOrderTradeUpdateFixtures decodes one frame per exec type and
// pins every field, including the case-only-different keys (s/S, x/X, l/L,
// n/N, and the top-level T vs the order's t).
func TestUserStreamOrderTradeUpdateFixtures(t *testing.T) {
	u := newDecodeStream(t)

	var mu sync.Mutex
	var got []OrderTradeUpdate

	u.SetOnOrderUpdate(func(ev OrderTradeUpdate) {
		mu.Lock()
		got = append(got, ev)
		mu.Unlock()
	})

	frames := []string{
		// NEW
		`{"e":"ORDER_TRADE_UPDATE","T":1568879465651,"E":1568879465650,"o":{"s":"BTCUSDT","c":"TEST-NEW","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"70000","ap":"0","x":"NEW","X":"NEW","i":1001,"l":"0","z":"0","L":"0","n":"0","N":"","t":-1,"m":false,"R":false,"ps":"BOTH"}}`,
		// TRADE (maker fill with commission)
		`{"e":"ORDER_TRADE_UPDATE","T":1568879465656,"E":1568879465655,"o":{"s":"BTCUSDT","c":"TEST-TRADE","S":"SELL","o":"LIMIT","f":"GTC","q":"0.001","p":"60000.00","ap":"59999.50","x":"TRADE","X":"PARTIALLY_FILLED","i":1002,"l":"0.0005","z":"0.0005","L":"59999.50","n":"0.00120000","N":"USDT","t":987654321,"T":1568879465656,"m":true,"R":true,"ps":"BOTH"}}`,
		// CANCELED (nothing filled)
		`{"e":"ORDER_TRADE_UPDATE","T":1568879465659,"E":1568879465658,"o":{"s":"BTCUSDT","c":"TEST-CANCEL","S":"BUY","o":"LIMIT","f":"GTC","q":"0.002","p":"55000.00","ap":"0","x":"CANCELED","X":"CANCELED","i":1003,"l":"0","z":"0.002","L":"0","n":"0","N":"","t":-1,"m":false,"R":false,"ps":"LONG"}}`,
		// EXPIRED (with expiry reason)
		`{"e":"ORDER_TRADE_UPDATE","T":1568879465661,"E":1568879465660,"o":{"s":"ETHUSDT","c":"TEST-EXP","S":"BUY","o":"LIMIT","f":"GTC","q":"0.5","p":"3000.00","ap":"0","x":"EXPIRED","X":"EXPIRED","i":1004,"l":"0","z":"0","L":"0","n":"0","N":"","t":-1,"m":false,"R":false,"ps":"BOTH","er":"INSUFFICIENT_MARGIN"}}`,
	}
	for _, f := range frames {
		u.handleMessage([]byte(f))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("decoded %d order updates, want 4 (lastErr=%q)", len(got), u.Stats().LastError)
	}

	newEv := got[0]
	if newEv.Event != "ORDER_TRADE_UPDATE" || newEv.EventTime != 1568879465650 || newEv.TransactionTime != 1568879465651 {
		t.Errorf("bad NEW envelope: %+v", newEv)
	}
	if o := newEv.Order; o.Symbol != "BTCUSDT" || o.Side != "BUY" || o.Type != "LIMIT" || o.TimeInForce != "GTC" ||
		o.OrigQty != "0.001" || o.Price != "70000" || o.OrderID != 1001 ||
		o.ExecType != "NEW" || o.Status != "NEW" || o.TradeID != -1 {
		t.Errorf("bad NEW order: %+v", o)
	}

	tr := got[1].Order
	if tr.Symbol != "BTCUSDT" || tr.Side != "SELL" {
		t.Errorf("s/S collision: symbol=%q side=%q (want BTCUSDT/SELL)", tr.Symbol, tr.Side)
	}
	if tr.ExecType != "TRADE" || tr.Status != "PARTIALLY_FILLED" {
		t.Errorf("x/X collision: exec=%q status=%q", tr.ExecType, tr.Status)
	}
	if tr.LastQty != "0.0005" || tr.LastPrice != "59999.50" {
		t.Errorf("l/L collision: lastQty=%q lastPrice=%q", tr.LastQty, tr.LastPrice)
	}
	if tr.Commission != "0.00120000" || tr.CommissionAsset != "USDT" {
		t.Errorf("n/N collision: commission=%q asset=%q", tr.Commission, tr.CommissionAsset)
	}
	if tr.TradeID != 987654321 {
		t.Errorf("t (tradeId) = %d, want 987654321 — was it overwritten by the order's T?", tr.TradeID)
	}
	if tr.OrderTime != 1568879465656 {
		t.Errorf("order T = %d, want 1568879465656", tr.OrderTime)
	}
	if !tr.Maker || !tr.ReduceOnly {
		t.Errorf("maker/reduceOnly = %v/%v, want true/true", tr.Maker, tr.ReduceOnly)
	}
	if tr.CumQty != "0.0005" {
		t.Errorf("CumQty = %q, want 0.0005", tr.CumQty)
	}
	if tr.AvgPrice != "59999.50" {
		t.Errorf("AvgPrice = %q", tr.AvgPrice)
	}
	if got[1].TransactionTime != 1568879465656 {
		t.Errorf("top-level T = %d, want 1568879465656", got[1].TransactionTime)
	}

	if c := got[2].Order; c.ExecType != "CANCELED" || c.CumQty != "0.002" || c.Status != "CANCELED" || c.PositionSide != "LONG" {
		t.Errorf("bad CANCELED order: %+v", c)
	}
	if e := got[3].Order; e.ExecType != "EXPIRED" || e.ExpiryReason != "INSUFFICIENT_MARGIN" || e.Symbol != "ETHUSDT" {
		t.Errorf("bad EXPIRED order: %+v", e)
	}
	if u.Stats().LastError != "" {
		t.Errorf("LastError = %q, want empty", u.Stats().LastError)
	}
}

// TestUserStreamAccountUpdate decodes an ACCOUNT_UPDATE with balances and
// positions.
func TestUserStreamAccountUpdate(t *testing.T) {
	u := newDecodeStream(t)

	var got AccountUpdate
	var called bool
	u.SetOnAccountUpdate(func(ev AccountUpdate) {
		got = ev
		called = true
	})

	frame := `{"e":"ACCOUNT_UPDATE","T":1568879465651,"E":1568879465650,"a":{"m":"ORDER","B":[{"a":"USDT","wb":"122624.12345678","cw":"130.00000000"}],"P":[{"s":"BTCUSDT","pa":"0.001","ep":"60000.00","ps":"LONG"}]}}`
	u.handleMessage([]byte(frame))

	if !called {
		t.Fatal("OnAccountUpdate was not called (lastErr=" + u.Stats().LastError + ")")
	}
	if got.Event != "ACCOUNT_UPDATE" || got.EventTime != 1568879465650 || got.Time != 1568879465651 {
		t.Errorf("bad envelope: %+v", got)
	}
	if got.Account.Reason != "ORDER" {
		t.Errorf("reason = %q, want ORDER", got.Account.Reason)
	}
	if len(got.Account.Balances) != 1 {
		t.Fatalf("balances = %d, want 1", len(got.Account.Balances))
	}
	b := got.Account.Balances[0]
	if b.Asset != "USDT" || b.WalletBalance != "122624.12345678" || b.CrossWalletBalance != "130.00000000" {
		t.Errorf("bad balance: %+v", b)
	}
	if len(got.Account.Positions) != 1 {
		t.Fatalf("positions = %d, want 1", len(got.Account.Positions))
	}
	p := got.Account.Positions[0]
	if p.Symbol != "BTCUSDT" || p.PositionAmount != "0.001" || p.EntryPrice != "60000.00" || p.PositionSide != "LONG" {
		t.Errorf("bad position: %+v", p)
	}
}

// TestUserStreamBadFrameRecordsError keeps a malformed frame from panicking and
// surfaces it through Stats.
func TestUserStreamBadFrameRecordsError(t *testing.T) {
	u := newDecodeStream(t)
	u.SetOnOrderUpdate(func(OrderTradeUpdate) { t.Error("callback ran for a malformed frame") })

	u.handleMessage([]byte(`{"e":"ORDER_TRADE_UPDATE","o":"not-an-object"}`))
	if u.Stats().LastError == "" {
		t.Fatal("LastError empty after a malformed frame")
	}
}

// ─────────────────────────────────────────────────────────────
// Lifecycle
// ─────────────────────────────────────────────────────────────

// TestUserStreamStartStopAndStats drives a real (local) socket: start, receive
// one frame, verify the counters, then stop and verify the listen key is
// released.
func TestUserStreamStartStopAndStats(t *testing.T) {
	stub := &restStub{keys: []string{"KEY-LIFE"}}
	rest := newRestStub(t, stub)

	tap := newWSTap()
	tap.onConn = func(idx int, c *coderws.Conn) {
		_ = c.Write(context.Background(), coderws.MessageText, []byte(`{"e":"ACCOUNT_UPDATE","E":1,"T":1,"a":{"m":"ORDER"}}`))
	}
	wsURL, _ := newWSTapServer(t, tap)

	u, err := NewUserStream(UserStreamConfig{
		Client:              testRESTClient(t, rest.URL),
		WSURL:               wsURL,
		ReconnectIntervalMs: 20,
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}

	var mu sync.Mutex
	var statuses []bool
	u.SetOnStatus(func(up bool, _ error) {
		mu.Lock()
		statuses = append(statuses, up)
		mu.Unlock()
	})
	var accounts atomic.Int64
	u.SetOnAccountUpdate(func(AccountUpdate) { accounts.Add(1) })

	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, 2*time.Second, "the account update", func() bool {
		s := u.Stats()
		return s.Msgs >= 1 && s.Bytes > 0 && accounts.Load() >= 1
	})
	st := u.Stats()
	if st.Msgs < 1 || st.Bytes <= 0 || st.LastMsgAt.IsZero() {
		t.Errorf("bad stats after one frame: %+v", st)
	}
	if st.Reconnects != 0 {
		t.Errorf("Reconnects = %d on a first connect, want 0", st.Reconnects)
	}
	mu.Lock()
	first := append([]bool(nil), statuses...)
	mu.Unlock()
	if len(first) == 0 || !first[0] {
		t.Errorf("status callbacks = %v, want the first one up=true", first)
	}

	u.Stop()

	_, _, closes := stub.counts()
	if closes != 1 {
		t.Errorf("CloseListenKey calls = %d, want 1", closes)
	}
	if u.Stats().Disconnects != 0 {
		t.Errorf("Disconnects = %d after a deliberate Stop, want 0", u.Stats().Disconnects)
	}
	// The socket is gone: the server's read loop must see the connection close.
	waitFor(t, 2*time.Second, "the server to notice the close", func() bool {
		return tap.closedCount() >= 1
	})

	// Stop is idempotent.
	u.Stop()
}

// TestUserStreamCountsReconnectOnDrop proves a transport drop is reconnected by
// the base socket and counted (excluding the first connect).
func TestUserStreamCountsReconnectOnDrop(t *testing.T) {
	stub := &restStub{keys: []string{"KEY-DROP"}}
	rest := newRestStub(t, stub)

	tap := newWSTap()
	tap.onConn = func(idx int, c *coderws.Conn) {
		if idx == 1 {
			// Drop the first connection shortly after it is established.
			go func() {
				time.Sleep(20 * time.Millisecond)
				_ = c.Close(coderws.StatusNormalClosure, "test drop")
			}()
		}
	}
	wsURL, _ := newWSTapServer(t, tap)

	u, err := NewUserStream(UserStreamConfig{
		Client:              testRESTClient(t, rest.URL),
		WSURL:               wsURL,
		ReconnectIntervalMs: 20,
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()

	waitFor(t, 3*time.Second, "a reconnect", func() bool {
		s := u.Stats()
		return s.Reconnects >= 1 && s.Disconnects >= 1
	})
	conns, keys, _ := tap.snapshot()
	if conns < 2 {
		t.Errorf("server saw %d connections, want >= 2", conns)
	}
	for i, k := range keys {
		if k != "KEY-DROP" {
			t.Errorf("connection %d used key %q, want KEY-DROP (no rekey on a plain drop)", i, k)
		}
	}
}

// TestUserStreamListenKeyExpiredRekeys drives the listenKeyExpired path: the
// stream must rotate the key and reconnect on a new socket. Both a genuinely
// new key and Binance returning the same key are covered.
func TestUserStreamListenKeyExpiredRekeys(t *testing.T) {
	cases := []struct {
		name string
		keys []string
	}{
		{"new key", []string{"KEY-1", "KEY-2"}},
		{"same key", []string{"KEY-SAME"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &restStub{keys: tc.keys}
			rest := newRestStub(t, stub)

			tap := newWSTap()
			tap.onConn = func(idx int, c *coderws.Conn) {
				if idx == 1 {
					_ = c.Write(context.Background(), coderws.MessageText, []byte(`{"e":"listenKeyExpired"}`))
				}
			}
			wsURL, _ := newWSTapServer(t, tap)

			u, err := NewUserStream(UserStreamConfig{
				Client:              testRESTClient(t, rest.URL),
				WSURL:               wsURL,
				ReconnectIntervalMs: 20,
			})
			if err != nil {
				t.Fatalf("NewUserStream: %v", err)
			}
			if err := u.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer u.Stop()

			waitFor(t, 3*time.Second, "the rekeyed connection", func() bool {
				n, _, _ := tap.snapshot()
				return n >= 2
			})
			_, keys, _ := tap.snapshot()
			wantKey := tc.keys[len(tc.keys)-1]
			if keys[1] != wantKey {
				t.Errorf("reconnected with key %q, want %q", keys[1], wantKey)
			}
			if newKeys, _, _ := stub.counts(); newKeys != 2 {
				t.Errorf("NewListenKey calls = %d, want 2", newKeys)
			}
			if rc := u.Stats().Reconnects; rc < 1 {
				t.Errorf("Reconnects = %d, want >= 1", rc)
			}
		})
	}
}

// TestUserStreamKeepAliveErrorRekeys proves a failed keepalive PUT rotates the
// key and reconnects.
func TestUserStreamKeepAliveErrorRekeys(t *testing.T) {
	stub := &restStub{
		keys:            []string{"KEY-KA-1", "KEY-KA-2"},
		keepAliveStatus: http.StatusBadRequest,
		keepAliveBody:   `{"code":-1125,"msg":"This listenKey does not exist."}`,
	}
	rest := newRestStub(t, stub)

	tap := newWSTap()
	wsURL, _ := newWSTapServer(t, tap)

	u, err := NewUserStream(UserStreamConfig{
		Client:              testRESTClient(t, rest.URL),
		WSURL:               wsURL,
		ReconnectIntervalMs: 20,
		KeepAliveInterval:   20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()

	waitFor(t, 3*time.Second, "the rekeyed connection", func() bool {
		n, _, _ := tap.snapshot()
		return n >= 2
	})
	_, keys, _ := tap.snapshot()
	if keys[1] != "KEY-KA-2" {
		t.Errorf("reconnected with key %q, want KEY-KA-2", keys[1])
	}
	if _, ka, _ := stub.counts(); ka < 1 {
		t.Error("no keepalive PUT was sent")
	}
	if u.Stats().LastError == "" {
		t.Error("LastError empty after a keepalive failure")
	}
}

// TestUserStreamStartTwiceRejected guards the double-start path.
func TestUserStreamStartTwiceRejected(t *testing.T) {
	stub := &restStub{keys: []string{"KEY-1"}}
	rest := newRestStub(t, stub)
	tap := newWSTap()
	wsURL, _ := newWSTapServer(t, tap)

	u, err := NewUserStream(UserStreamConfig{
		Client:              testRESTClient(t, rest.URL),
		WSURL:               wsURL,
		ReconnectIntervalMs: 20,
	})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()

	if err := u.Start(context.Background()); err == nil {
		t.Error("second Start = nil error, want error")
	}
	if n, _, _ := stub.counts(); n != 1 {
		t.Errorf("NewListenKey calls = %d, want 1", n)
	}
}

// A failed rekey (listen-key POST 500) is retried with backoff instead of
// leaving the exec stream dead until the next keepalive tick.
func TestUserStreamRekeyRetriesAfterFailure(t *testing.T) {
	oldMin, oldMax := rekeyRetryMin, rekeyRetryMax
	rekeyRetryMin, rekeyRetryMax = 10*time.Millisecond, 20*time.Millisecond
	defer func() { rekeyRetryMin, rekeyRetryMax = oldMin, oldMax }()

	stub := &restStub{keys: []string{"KEY-1", "KEY-1", "KEY-2"}, failPost: map[int]bool{2: true}}
	rest := newRestStub(t, stub)
	tap := newWSTap()
	tap.onConn = func(idx int, c *coderws.Conn) {
		if idx == 1 {
			_ = c.Write(context.Background(), coderws.MessageText, []byte(`{"e":"listenKeyExpired"}`))
		}
	}
	wsURL, _ := newWSTapServer(t, tap)
	u, err := NewUserStream(UserStreamConfig{Client: testRESTClient(t, rest.URL), WSURL: wsURL, ReconnectIntervalMs: 20})
	if err != nil {
		t.Fatalf("NewUserStream: %v", err)
	}
	if err := u.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()
	waitFor(t, 3*time.Second, "reconnect after a failed rekey", func() bool {
		n, _, _ := tap.snapshot()
		return n >= 2
	})
	_, keys, _ := tap.snapshot()
	if keys[1] != "KEY-2" {
		t.Errorf("reconnected with key %q, want KEY-2", keys[1])
	}
	if newKeys, _, _ := stub.counts(); newKeys != 3 {
		t.Errorf("NewListenKey calls = %d, want 3 (start, failed rekey, retry)", newKeys)
	}
}
