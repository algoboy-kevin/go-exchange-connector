package spot

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The key and secret below are the example values printed in the Binance docs
// (llms-full.txt lines 117953-117990 and 20529-20600). They are published
// documentation values, not credentials.
const (
	docKey    = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
	docSecret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
)

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// rec is one recorded request.
type rec struct {
	method, path, rawQuery, apiKey string
}

// fake is an httptest REST server answering from a handler and recording
// requests.
type fake struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []rec
	hits atomic.Int64
}

func newFake(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *fake {
	t.Helper()
	f := &fake{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		f.mu.Lock()
		f.reqs = append(f.reqs, rec{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("X-MBX-APIKEY")})
		f.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fake) last() rec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[len(f.reqs)-1]
}

func (f *fake) client(t *testing.T) *Client {
	t.Helper()
	c, err := New(Config{BaseURL: f.srv.URL, APIKey: docKey, APISecret: docSecret})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func serveJSON(body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

func serveFile(t *testing.T, name string) func(http.ResponseWriter, *http.Request) {
	return serveJSON(string(testdata(t, name)))
}

func serveStatus(status int, body string, hdr map[string]string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range hdr {
			w.Header().Set(k, v)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// ─────────────────────────────────────────────────────────────
// Signing
// ─────────────────────────────────────────────────────────────

func TestSignHexDocVectors(t *testing.T) {
	// REST: testnet REST doc "HMAC Keys", Step 2 (line 118040-118055).
	rest := "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
	if got, want := signHex(docSecret, rest), "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"; got != want {
		t.Fatalf("REST vector: got %s want %s", got, want)
	}
	// Non-ASCII symbol, percent-encoded before signing (same section).
	enc := "symbol=%EF%BC%91%EF%BC%92%EF%BC%93%EF%BC%94%EF%BC%95%EF%BC%96&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
	if got, want := signHex(docSecret, enc), "e1353ec6b14d888f1164ae9af8228a3dbd508bc82eb867db8ab6046442f33ef3"; got != want {
		t.Fatalf("non-ASCII REST vector: got %s want %s", got, want)
	}
	// params.encode must produce exactly that percent-encoded string.
	p := newParams()
	for _, kv := range [][2]string{{"symbol", "１２３４５６"}, {"side", "BUY"}, {"type", "LIMIT"}, {"timeInForce", "GTC"}, {"quantity", "1"}, {"price", "0.1"}, {"recvWindow", "5000"}, {"timestamp", "1499827319559"}} {
		p.add(kv[0], kv[1])
	}
	if got := p.encode(); got != enc {
		t.Fatalf("encode:\n got %s\nwant %s", got, enc)
	}
	// WebSocket API: sorted params, WS API signing section (line 20565-20580).
	ws := "apiKey=" + docKey + "&price=52000.00&quantity=0.01000000&recvWindow=100&side=SELL&symbol=BTCUSDT&timeInForce=GTC&timestamp=1645423376532&type=LIMIT"
	if got, want := signHex(docSecret, ws), "aa1b5712c094bc4e57c05a1a5c1fd8d88dcd628338ea863fec7b88e59fe2db24"; got != want {
		t.Fatalf("WS vector: got %s want %s", got, want)
	}
}

func TestParamsKeepInsertionOrder(t *testing.T) {
	p := newParams()
	p.add("z", "1")
	p.add("a", "2")
	p.add("m", "3")
	p.add("a", "4") // replace keeps position
	if got := p.encode(); got != "z=1&a=4&m=3" {
		t.Fatalf("got %s", got)
	}
}

func TestSignedRequestShape(t *testing.T) {
	f := newFake(t, serveFile(t, "new_order_result.json"))
	c := f.client(t)
	_, err := c.NewOrder(context.Background(), OrderReq{
		Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC,
		Quantity: "0.001", Price: "50000.10", NewClientOrderID: "my-id_1",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := f.last()
	if r.method != http.MethodPost || r.path != "/api/v3/order" {
		t.Fatalf("route %s %s", r.method, r.path)
	}
	if r.apiKey != docKey {
		t.Fatalf("X-MBX-APIKEY %q", r.apiKey)
	}
	// Order of parameters is the insertion order, then recvWindow, timestamp, signature.
	var keys []string
	for _, kv := range strings.Split(r.rawQuery, "&") {
		keys = append(keys, strings.SplitN(kv, "=", 2)[0])
	}
	want := []string{"symbol", "side", "type", "timeInForce", "quantity", "price", "newClientOrderId", "newOrderRespType", "recvWindow", "timestamp", "signature"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("params order %v want %v", keys, want)
	}
	// signature is last and equals HMAC-SHA256 of everything before it.
	i := strings.LastIndex(r.rawQuery, "&signature=")
	if i < 0 {
		t.Fatal("no signature")
	}
	payload, sig := r.rawQuery[:i], r.rawQuery[i+len("&signature="):]
	m := hmac.New(sha256.New, []byte(docSecret))
	m.Write([]byte(payload))
	if sig != hex.EncodeToString(m.Sum(nil)) {
		t.Fatalf("signature mismatch for payload %q", payload)
	}
	q, _ := url.ParseQuery(r.rawQuery)
	if q.Get("newOrderRespType") != "RESULT" || q.Get("recvWindow") != "5000" {
		t.Fatalf("defaults: %v", q)
	}
	ts, _ := strconv.ParseInt(q.Get("timestamp"), 10, 64)
	if d := time.Now().UnixMilli() - ts; d < -2000 || d > 5000 {
		t.Fatalf("timestamp off by %d ms", d)
	}
}

func TestLimitMakerSendsNoTimeInForce(t *testing.T) {
	f := newFake(t, serveFile(t, "new_order_result.json"))
	_, err := f.client(t).NewOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimitMaker, Quantity: "0.001", Price: "1000"})
	if err != nil {
		t.Fatal(err)
	}
	q, _ := url.ParseQuery(f.last().rawQuery)
	if q.Get("type") != "LIMIT_MAKER" || q.Has("timeInForce") {
		t.Fatalf("query %v", q)
	}
}

func TestUnsignedPublicRequestsCarryNoSignature(t *testing.T) {
	f := newFake(t, serveFile(t, "book_ticker.json"))
	c, _ := New(Config{BaseURL: f.srv.URL})
	if _, err := c.BookTicker(context.Background(), "LTCBTC"); err != nil {
		t.Fatal(err)
	}
	r := f.last()
	if r.rawQuery != "symbol=LTCBTC" || r.path != "/api/v3/ticker/bookTicker" || r.method != http.MethodGet {
		t.Fatalf("%+v", r)
	}
	// A signed call without credentials fails locally and sends nothing.
	before := f.hits.Load()
	_, err := c.Account(context.Background(), false)
	var ce *ConfigError
	if !errors.As(err, &ce) || f.hits.Load() != before {
		t.Fatalf("err=%v hits=%d", err, f.hits.Load()-before)
	}
}

func TestNewConfig(t *testing.T) {
	c, err := New(Config{Network: Testnet})
	if err != nil || c.BaseURL() != "https://testnet.binance.vision" {
		t.Fatalf("%v %v", c, err)
	}
	c, _ = New(Config{})
	if c.BaseURL() != "https://api.binance.com" {
		t.Fatal(c.BaseURL())
	}
	c, _ = New(Config{Network: Testnet, BaseURL: "http://x/"})
	if c.BaseURL() != "http://x" {
		t.Fatal("BaseURL should override and be trimmed")
	}
	if _, err := New(Config{Network: "nope"}); err == nil {
		t.Fatal("unknown network accepted")
	}
	if _, err := New(Config{RecvWindow: 61 * time.Second}); err == nil {
		t.Fatal("recvWindow > 60s accepted")
	}
}

func TestSyncTimeOffsetAppliesToTimestamp(t *testing.T) {
	var serverAhead int64 = 60_000
	f := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v3/time" {
			_, _ = w.Write([]byte(`{"serverTime":` + strconv.FormatInt(time.Now().UnixMilli()+serverAhead, 10) + `}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	c := f.client(t)
	if err := c.SyncTime(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.TestOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC, Quantity: "1", Price: "1"}); err != nil {
		t.Fatal(err)
	}
	q, _ := url.ParseQuery(f.last().rawQuery)
	ts, _ := strconv.ParseInt(q.Get("timestamp"), 10, 64)
	if d := ts - time.Now().UnixMilli() - serverAhead; d < -1000 || d > 1000 {
		t.Fatalf("timestamp not offset: delta %d", d)
	}
}

// ─────────────────────────────────────────────────────────────
// Routes
// ─────────────────────────────────────────────────────────────

func TestRoutes(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		body   string
		method string
		path   string
		query  []string // substrings that must appear
		call   func(c *Client) error
	}{
		{"order", "new_order_ack.json", "POST", "/api/v3/order", []string{"symbol=BTCUSDT"}, func(c *Client) error {
			_, err := c.NewOrder(ctx, OrderReq{Symbol: "BTCUSDT", Side: SideSell, Type: TypeLimit, TimeInForce: TIFIOC, Quantity: "1", Price: "2"})
			return err
		}},
		{"order/test", "new_order_ack.json", "POST", "/api/v3/order/test", []string{"timeInForce=FOK"}, func(c *Client) error {
			return c.TestOrder(ctx, OrderReq{Symbol: "BTCUSDT", Side: SideSell, Type: TypeLimit, TimeInForce: TIFFOK, Quantity: "1", Price: "2"})
		}},
		{"cancel by id", "cancel_order.json", "DELETE", "/api/v3/order", []string{"orderId=4"}, func(c *Client) error {
			_, err := c.CancelOrder(ctx, CancelReq{Symbol: "LTCBTC", OrderID: 4})
			return err
		}},
		{"cancel by client id", "cancel_order.json", "DELETE", "/api/v3/order", []string{"origClientOrderId=myOrder1"}, func(c *Client) error {
			_, err := c.CancelOrder(ctx, CancelReq{Symbol: "LTCBTC", OrigClientOrderID: "myOrder1"})
			return err
		}},
		{"cancel all", "cancel_all.json", "DELETE", "/api/v3/openOrders", []string{"symbol=BTCUSDT"}, func(c *Client) error {
			_, err := c.CancelAll(ctx, "BTCUSDT")
			return err
		}},
		{"get order", "query_order.json", "GET", "/api/v3/order", []string{"origClientOrderId=myOrder1"}, func(c *Client) error {
			_, err := c.GetOrder(ctx, CancelReq{Symbol: "LTCBTC", OrigClientOrderID: "myOrder1"})
			return err
		}},
		{"open orders", "open_orders.json", "GET", "/api/v3/openOrders", []string{"symbol=LTCBTC"}, func(c *Client) error {
			_, err := c.OpenOrders(ctx, "LTCBTC")
			return err
		}},
		{"my trades", "my_trades.json", "GET", "/api/v3/myTrades", []string{"symbol=BNBBTC", "orderId=100234", "limit=5"}, func(c *Client) error {
			_, err := c.MyTrades(ctx, MyTradesReq{Symbol: "BNBBTC", OrderID: 100234, Limit: 5})
			return err
		}},
		{"account", "account.json", "GET", "/api/v3/account", []string{"omitZeroBalances=true"}, func(c *Client) error {
			_, err := c.Account(ctx, true)
			return err
		}},
		{"exchangeInfo", "exchange_info.json", "GET", "/api/v3/exchangeInfo", []string{"symbol=ETHBTC"}, func(c *Client) error {
			_, err := c.ExchangeInfo(ctx, "ETHBTC")
			return err
		}},
		{"time", "server_time.json", "GET", "/api/v3/time", nil, func(c *Client) error { return c.SyncTime(ctx) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, serveFile(t, tc.body))
			if err := tc.call(f.client(t)); err != nil {
				t.Fatal(err)
			}
			r := f.last()
			if r.method != tc.method || r.path != tc.path {
				t.Fatalf("got %s %s want %s %s", r.method, r.path, tc.method, tc.path)
			}
			for _, s := range tc.query {
				if !strings.Contains(r.rawQuery, s) {
					t.Fatalf("query %q lacks %q", r.rawQuery, s)
				}
			}
			if f.hits.Load() != 1 {
				t.Fatalf("%d requests, want exactly 1", f.hits.Load())
			}
		})
	}
}

func TestExchangeInfoMultipleSymbols(t *testing.T) {
	f := newFake(t, serveFile(t, "exchange_info.json"))
	if _, err := f.client(t).ExchangeInfo(context.Background(), "BTCUSDT", "ETHBTC"); err != nil {
		t.Fatal(err)
	}
	q, _ := url.ParseQuery(f.last().rawQuery)
	if q.Get("symbols") != `["BTCUSDT","ETHBTC"]` {
		t.Fatalf("symbols=%q", q.Get("symbols"))
	}
}

func TestNoBatchSurface(t *testing.T) {
	// Spot has no batch place/cancel. Guard against inventing one: the
	// exported Client methods must not include a Batch* name.
	typ := reflect.TypeOf(&Client{})
	for i := 0; i < typ.NumMethod(); i++ {
		if n := typ.Method(i).Name; strings.Contains(strings.ToLower(n), "batch") {
			t.Fatalf("spot Client has a batch method %s; the venue has none", n)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Local validation: nothing is sent
// ─────────────────────────────────────────────────────────────

func TestLocalValidationSendsNothing(t *testing.T) {
	f := newFake(t, serveJSON(`{}`))
	c := f.client(t)
	ctx := context.Background()
	good := OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC, Quantity: "1", Price: "2"}
	mut := func(fn func(*OrderReq)) OrderReq { r := good; fn(&r); return r }
	bad := map[string]OrderReq{
		"no symbol":            mut(func(r *OrderReq) { r.Symbol = "" }),
		"symbol with &":        mut(func(r *OrderReq) { r.Symbol = "BTC&x" }),
		"bad side":             mut(func(r *OrderReq) { r.Side = "buy" }),
		"market type":          mut(func(r *OrderReq) { r.Type = "MARKET" }),
		"LIMIT no tif":         mut(func(r *OrderReq) { r.TimeInForce = "" }),
		"LIMIT tif typo":       mut(func(r *OrderReq) { r.TimeInForce = "gtc" }),
		"LIMIT_MAKER with tif": mut(func(r *OrderReq) { r.Type = TypeLimitMaker }),
		"qty zero":             mut(func(r *OrderReq) { r.Quantity = "0" }),
		"qty 0.000":            mut(func(r *OrderReq) { r.Quantity = "0.000" }),
		"qty negative":         mut(func(r *OrderReq) { r.Quantity = "-1" }),
		"qty exponent":         mut(func(r *OrderReq) { r.Quantity = "1e3" }),
		"qty empty":            mut(func(r *OrderReq) { r.Quantity = "" }),
		"price empty":          mut(func(r *OrderReq) { r.Price = "" }),
		"price spaces":         mut(func(r *OrderReq) { r.Price = " 2" }),
		"client id too long":   mut(func(r *OrderReq) { r.NewClientOrderID = strings.Repeat("a", 37) }),
		"client id bad chars":  mut(func(r *OrderReq) { r.NewClientOrderID = "a b" }),
		"client id has dot":    mut(func(r *OrderReq) { r.NewClientOrderID = "a.b" }),
		"bad resp type":        mut(func(r *OrderReq) { r.NewOrderRespType = "FULLY" }),
	}
	for name, req := range bad {
		if _, err := c.NewOrder(ctx, req); !IsValidation(err) {
			t.Errorf("NewOrder %s: err=%v, want validation error", name, err)
		}
		if err := c.TestOrder(ctx, req); !IsValidation(err) {
			t.Errorf("TestOrder %s: err=%v, want validation error", name, err)
		}
	}
	// LIMIT_MAKER good form passes validation.
	if err := (OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimitMaker, Quantity: "1", Price: "2"}).Validate(); err != nil {
		t.Errorf("valid LIMIT_MAKER rejected: %v", err)
	}
	badCancel := map[string]CancelReq{
		"no symbol":      {OrderID: 1},
		"no identifier":  {Symbol: "BTCUSDT"},
		"negative id":    {Symbol: "BTCUSDT", OrderID: -1},
		"bad client id":  {Symbol: "BTCUSDT", OrigClientOrderID: "a b"},
		"bad new cancel": {Symbol: "BTCUSDT", OrderID: 1, NewClientOrderID: strings.Repeat("x", 40)},
	}
	for name, req := range badCancel {
		if _, err := c.CancelOrder(ctx, req); !IsValidation(err) {
			t.Errorf("CancelOrder %s: %v", name, err)
		}
		if _, err := c.GetOrder(ctx, req); !IsValidation(err) && name != "bad new cancel" {
			t.Errorf("GetOrder %s: %v", name, err)
		}
	}
	if _, err := c.CancelAll(ctx, ""); !IsValidation(err) {
		t.Errorf("CancelAll empty symbol: %v", err)
	}
	if _, err := c.OpenOrders(ctx, "BTC USDT"); !IsValidation(err) {
		t.Errorf("OpenOrders bad symbol: %v", err)
	}
	badTrades := map[string]MyTradesReq{
		"no symbol":        {},
		"limit too big":    {Symbol: "BTCUSDT", Limit: 1001},
		"order + time":     {Symbol: "BTCUSDT", OrderID: 1, StartTime: 5},
		"fromId + time":    {Symbol: "BTCUSDT", FromID: 1, EndTime: 5},
		"window over 24h":  {Symbol: "BTCUSDT", StartTime: 1, EndTime: 1 + 24*3600*1000 + 1},
		"end before start": {Symbol: "BTCUSDT", StartTime: 10, EndTime: 5},
		"negative order":   {Symbol: "BTCUSDT", OrderID: -3},
	}
	for name, req := range badTrades {
		if _, err := c.MyTrades(ctx, req); !IsValidation(err) {
			t.Errorf("MyTrades %s: %v", name, err)
		}
	}
	if _, err := c.ExchangeInfo(ctx, "BTCUSDT", ""); !IsValidation(err) {
		t.Errorf("ExchangeInfo empty symbol: %v", err)
	}
	if n := f.hits.Load(); n != 0 {
		t.Fatalf("%d HTTP requests were sent for locally invalid input", n)
	}
}

// ─────────────────────────────────────────────────────────────
// Errors and classes
// ─────────────────────────────────────────────────────────────

func TestErrorClasses(t *testing.T) {
	ctx := context.Background()
	order := OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC, Quantity: "1", Price: "2"}
	type want struct{ uncertain, rate, banned, waf, unknown, ts, sig bool }
	cases := []struct {
		name   string
		status int
		body   string
		hdr    map[string]string
		want   want
		after  func(t *testing.T, err error)
	}{
		{"500", 500, `{"code":-1000,"msg":"UNKNOWN"}`, nil, want{uncertain: true}, nil},
		{"502 html", 502, `<html>bad gateway</html>`, nil, want{uncertain: true}, func(t *testing.T, err error) {
			var ae *APIError
			if !errors.As(err, &ae) || ae.Code != 0 || !strings.Contains(ae.Msg, "bad gateway") {
				t.Fatalf("%+v", ae)
			}
		}},
		{"503 timeout -1007", 503, `{"code":-1007,"msg":"Timeout waiting for response from backend server. Send status unknown; execution status unknown."}`, nil, want{uncertain: true}, nil},
		{"400 with -1007", 400, `{"code":-1007,"msg":"Timeout"}`, nil, want{uncertain: true}, nil},
		{"400 with -1006", 400, `{"code":-1006,"msg":"Unexpected response"}`, nil, want{uncertain: true}, nil},
		{"429", 429, `{"code":-1003,"msg":"Too many requests"}`, map[string]string{"Retry-After": "7"}, want{rate: true}, func(t *testing.T, err error) {
			var ae *APIError
			errors.As(err, &ae)
			if ae.RetryAfter != 7*time.Second {
				t.Fatalf("RetryAfter %v", ae.RetryAfter)
			}
		}},
		{"-1015 too many orders", 429, `{"code":-1015,"msg":"Too many new orders"}`, nil, want{rate: true}, nil},
		{"418", 418, `{"code":-1003,"msg":"Way too many requests; IP banned"}`, map[string]string{"Retry-After": "120"}, want{banned: true}, func(t *testing.T, err error) {
			var ae *APIError
			errors.As(err, &ae)
			if ae.RetryAfter != 120*time.Second {
				t.Fatalf("RetryAfter %v", ae.RetryAfter)
			}
		}},
		{"403 WAF", 403, `{"code":0,"msg":"WAF"}`, nil, want{waf: true}, nil},
		{"-2010 post-only reject", 400, `{"code":-2010,"msg":"Order would immediately match and take."}`, nil, want{}, nil},
		{"-2013", 400, `{"code":-2013,"msg":"Order does not exist."}`, nil, want{unknown: true}, nil},
		{"-2011 unknown", 400, `{"code":-2011,"msg":"Unknown order sent."}`, nil, want{unknown: true}, nil},
		{"-2011 other", 400, `{"code":-2011,"msg":"Order was not canceled due to cancel restrictions."}`, nil, want{}, nil},
		{"-1021", 400, `{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}`, nil, want{ts: true}, nil},
		{"-1022", 400, `{"code":-1022,"msg":"Signature for this request is not valid."}`, nil, want{sig: true}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, serveStatus(tc.status, tc.body, tc.hdr))
			_, err := f.client(t).NewOrder(ctx, order)
			if err == nil {
				t.Fatal("no error")
			}
			got := want{IsUncertain(err), IsRateLimited(err), IsBanned(err), IsWAFBlocked(err), IsUnknownOrder(err), IsTimestampError(err), IsInvalidSignature(err)}
			if got != tc.want {
				t.Fatalf("classes %+v want %+v (err %v)", got, tc.want, err)
			}
			if f.hits.Load() != 1 {
				t.Fatalf("%d requests: the client must not retry", f.hits.Load())
			}
			var ae *APIError
			if !errors.As(err, &ae) || ae.HTTPStatus != tc.status {
				t.Fatalf("not an APIError with status %d: %v", tc.status, err)
			}
			if tc.after != nil {
				tc.after(t, err)
			}
		})
	}
}

func TestTransportAndContextErrorsAreUncertain(t *testing.T) {
	f := newFake(t, serveJSON(`{}`))
	c := f.client(t)
	f.srv.Close() // connection refused
	_, err := c.NewOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC, Quantity: "1", Price: "2"})
	if !IsUncertain(err) {
		t.Fatalf("dial error should be uncertain: %v", err)
	}

	// Timeout: the server never answers within the client timeout.
	block := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(block); slow.Close() })
	c2, _ := New(Config{BaseURL: slow.URL, APIKey: docKey, APISecret: docSecret, HTTP: &http.Client{Timeout: 100 * time.Millisecond}})
	if _, err := c2.CancelOrder(context.Background(), CancelReq{Symbol: "BTCUSDT", OrderID: 1}); !IsUncertain(err) {
		t.Fatalf("timeout should be uncertain: %v", err)
	}
	// A cancelled context counts too.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c2.CancelOrder(ctx, CancelReq{Symbol: "BTCUSDT", OrderID: 1}); !IsUncertain(err) {
		t.Fatalf("cancelled ctx should be uncertain: %v", err)
	}
	// Validation errors are not uncertain.
	if IsUncertain(invalidf("x")) || IsUncertain(nil) {
		t.Fatal("validation/nil must not be uncertain")
	}
}

func TestUsageHeaders(t *testing.T) {
	f := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "42")
		w.Header().Set("X-MBX-ORDER-COUNT-10S", "3")
		w.Header().Set("x-mbx-order-count-1d", "117")
		_, _ = w.Write([]byte(`{}`))
	})
	c := f.client(t)
	if err := c.TestOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: SideBuy, Type: TypeLimit, TimeInForce: TIFGTC, Quantity: "1", Price: "2"}); err != nil {
		t.Fatal(err)
	}
	u := c.Usage()
	if u.UsedWeight["1M"] != 42 || u.OrderCount["10S"] != 3 || u.OrderCount["1D"] != 117 {
		t.Fatalf("%+v", u)
	}
	u.UsedWeight["1M"] = 0 // a copy
	if c.Usage().UsedWeight["1M"] != 42 {
		t.Fatal("Usage must return a copy")
	}
	// Usage is captured on error responses too.
	g := newFake(t, serveStatus(429, `{"code":-1003,"msg":"x"}`, map[string]string{"X-MBX-USED-WEIGHT-1M": "6001"}))
	c2 := g.client(t)
	_, _ = c2.OpenOrders(context.Background(), "BTCUSDT")
	if c2.Usage().UsedWeight["1M"] != 6001 {
		t.Fatalf("%+v", c2.Usage())
	}
}

// ─────────────────────────────────────────────────────────────
// Doc payload round trips
// ─────────────────────────────────────────────────────────────

// keysCovered fails when a key present in the doc payload would be dropped on
// a round trip through v: re-marshalling v must contain every key, spelled
// exactly (encoding/json matches case-insensitively when decoding, so a
// mis-cased tag would pass a plain decode).
func keysCovered(t *testing.T, name string, raw []byte, v any) {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var src, dst any
	if err := json.Unmarshal(raw, &src); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &dst); err != nil {
		t.Fatal(err)
	}
	var walk func(path string, s, d any)
	walk = func(path string, s, d any) {
		switch sv := s.(type) {
		case map[string]any:
			dm, ok := d.(map[string]any)
			if !ok {
				t.Errorf("%s: %s: shape mismatch", name, path)
				return
			}
			for k, x := range sv {
				y, ok := dm[k]
				if !ok {
					t.Errorf("%s: key %s.%s is not carried by the struct (exact-case tag missing)", name, path, k)
					continue
				}
				walk(path+"."+k, x, y)
			}
		case []any:
			da, ok := d.([]any)
			if !ok || len(da) != len(sv) {
				t.Errorf("%s: %s: array mismatch", name, path)
				return
			}
			for i := range sv {
				walk(path+"["+strconv.Itoa(i)+"]", sv[i], da[i])
			}
		default:
			if s == nil && d == "" { // JSON null into a string field (e.g. executionReport N)
				return
			}
			if s != d {
				t.Errorf("%s: %s: value %v became %v", name, path, s, d)
			}
		}
	}
	walk("$", src, dst)
}

func TestDocPayloadRoundTrips(t *testing.T) {
	ctx := context.Background()
	t.Run("order responses", func(t *testing.T) {
		for _, name := range []string{"new_order_ack.json", "new_order_result.json", "new_order_full.json", "query_order.json", "cancel_order.json"} {
			raw := testdata(t, name)
			var o Order
			if err := json.Unmarshal(raw, &o); err != nil {
				t.Fatal(name, err)
			}
			// Order carries zero-valued extras; check only that every source key survives.
			keysCovered(t, name, raw, o)
		}
		var full Order
		_ = json.Unmarshal(testdata(t, "new_order_full.json"), &full)
		if len(full.Fills) != 5 || full.Fills[1].Price != "3999.00000000" || full.Fills[1].TradeID != 57 || full.CummulativeQuoteQty != "10.00000000" {
			t.Fatalf("%+v", full)
		}
	})
	t.Run("my trades", func(t *testing.T) {
		raw := testdata(t, "my_trades.json")
		var tr []Trade
		if err := json.Unmarshal(raw, &tr); err != nil || len(tr) != 1 {
			t.Fatal(err)
		}
		keysCovered(t, "my_trades", raw, tr)
		if tr[0].QuoteQty != "48.000012" || !tr[0].IsBuyer || tr[0].IsMaker {
			t.Fatalf("%+v", tr[0])
		}
	})
	t.Run("account", func(t *testing.T) {
		raw := testdata(t, "account.json")
		var a Account
		if err := json.Unmarshal(raw, &a); err != nil {
			t.Fatal(err)
		}
		keysCovered(t, "account", raw, a)
		if a.Balance("BTC").Free != "4723846.89208129" || a.Balance("XRP").Free != "0" || a.CommissionRates.Maker != "0.00150000" {
			t.Fatalf("%+v", a)
		}
	})
	t.Run("book ticker", func(t *testing.T) {
		raw := testdata(t, "book_ticker.json")
		var b BookTicker
		_ = json.Unmarshal(raw, &b)
		keysCovered(t, "book_ticker", raw, b)
	})
	t.Run("cancel all splits orders and order lists", func(t *testing.T) {
		f := newFake(t, serveFile(t, "cancel_all.json"))
		res, err := f.client(t).CancelAll(ctx, "BTCUSDT")
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Orders) != 2 || len(res.OrderLists) != 1 {
			t.Fatalf("%d orders %d lists", len(res.Orders), len(res.OrderLists))
		}
		if res.Orders[0].OrderID != 11 || res.Orders[0].OrigClientOrderID != "E6APeyTJvkMvLMYMqu1KQ4" || res.Orders[0].Status != "CANCELED" {
			t.Fatalf("%+v", res.Orders[0])
		}
		l := res.OrderLists[0]
		if l.OrderListID != 1929 || len(l.Orders) != 2 || len(l.OrderReports) != 2 || l.OrderReports[1].Type != "LIMIT_MAKER" {
			t.Fatalf("%+v", l)
		}
	})
	t.Run("cancel all empty and unknown", func(t *testing.T) {
		f := newFake(t, serveJSON(`[]`))
		res, err := f.client(t).CancelAll(ctx, "BTCUSDT")
		if err != nil || len(res.Orders) != 0 || len(res.OrderLists) != 0 {
			t.Fatalf("%v %+v", err, res)
		}
		g := newFake(t, serveStatus(400, `{"code":-2011,"msg":"Unknown order sent."}`, nil))
		if _, err := g.client(t).CancelAll(ctx, "BTCUSDT"); !IsUnknownOrder(err) {
			t.Fatalf("%v", err)
		}
	})
	t.Run("exchangeInfo rules", func(t *testing.T) {
		f := newFake(t, serveFile(t, "exchange_info.json"))
		info, err := f.client(t).ExchangeInfo(ctx, "ETHBTC")
		if err != nil {
			t.Fatal(err)
		}
		s := info.Symbols[0]
		r, err := s.Rules()
		if err != nil {
			t.Fatal(err)
		}
		// NOTIONAL (10.0) wins over MIN_NOTIONAL (0.001); both use the field minNotional.
		if r.TickSize != "0.00000100" || r.StepSize != "0.00100000" || r.MinNotional != "10.00000000" || r.MaxNotional != "10000.00000000" || r.MinQty != "0.00100000" || r.BidMultiplierDown != "0.2" {
			t.Fatalf("%+v", r)
		}
		if !s.SupportsOrderType("LIMIT_MAKER") || s.SupportsOrderType("ICEBERG") {
			t.Fatal("orderTypes")
		}
		if info.RateLimits[0].RateLimitType != "REQUEST_WEIGHT" {
			t.Fatalf("%+v", info.RateLimits)
		}
	})
	t.Run("rules need tick and step", func(t *testing.T) {
		if _, err := (Symbol{Symbol: "X"}).Rules(); err == nil {
			t.Fatal("empty symbol accepted")
		}
		if _, err := (Symbol{Symbol: "X", Filters: []Filter{{FilterType: "PRICE_FILTER", TickSize: "0.1"}}}).Rules(); err == nil {
			t.Fatal("missing stepSize accepted")
		}
	})
}

func TestMIN_NOTIONALAlone(t *testing.T) {
	s := Symbol{Symbol: "X", Filters: []Filter{
		{FilterType: "PRICE_FILTER", TickSize: "0.01"}, {FilterType: "LOT_SIZE", StepSize: "0.001"},
		{FilterType: "MIN_NOTIONAL", MinNotional: "5.0"}, {FilterType: "PERCENT_PRICE", MultiplierDown: "0.7"},
	}}
	r, err := s.Rules()
	if err != nil || r.MinNotional != "5.0" || r.BidMultiplierDown != "0.7" {
		t.Fatalf("%v %+v", err, r)
	}
}

func TestErrorsString(t *testing.T) {
	e := &APIError{HTTPStatus: 400, Code: -2010, Msg: "nope"}
	if !strings.Contains(e.Error(), "-2010") || !strings.Contains(e.Error(), "nope") {
		t.Fatal(e.Error())
	}
	e2 := &APIError{HTTPStatus: 418, Msg: "x", RetryAfter: 2 * time.Second}
	if !strings.Contains(e2.Error(), "retry after 2s") {
		t.Fatal(e2.Error())
	}
}

// sortedKeys is used by user stream tests.
func sortedKeys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
