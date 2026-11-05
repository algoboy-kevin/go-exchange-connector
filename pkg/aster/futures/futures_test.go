package futures

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

const secret = "test-secret"

// verifySigned checks the Binance-style signature: last param, HMAC-SHA256 of
// everything before it, and the key header.
func verifySigned(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Header.Get("X-MBX-APIKEY") != "test-key" {
		t.Errorf("X-MBX-APIKEY = %q", r.Header.Get("X-MBX-APIKEY"))
	}
	q := r.URL.RawQuery
	i := strings.LastIndex(q, "&signature=")
	if i < 0 {
		t.Fatalf("no trailing signature in %q", q)
	}
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(q[:i]))
	if got, want := q[i+len("&signature="):], hex.EncodeToString(m.Sum(nil)); got != want {
		t.Errorf("signature = %s, want %s", got, want)
	}
	if !strings.Contains(q, "timestamp=") {
		t.Errorf("missing timestamp in %q", q)
	}
}

func newTestClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{BaseURL: srv.URL, APIKey: "test-key", APISecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDefaultBaseURL(t *testing.T) {
	c, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c.BaseURL() != "https://fapi.asterdex.com" {
		t.Errorf("BaseURL = %q", c.BaseURL())
	}
}

// Aster serves positionRisk at v2; Binance's v3 route must not be used.
func TestPositionRiskUsesV2(t *testing.T) {
	var path string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		verifySigned(t, r)
		_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","positionAmt":"-10.000","entryPrice":"6.0","markPrice":"6679.5","unRealizedProfit":"-1156.4","positionSide":"SHORT","leverage":"20"}]`))
	})
	rows, err := c.PositionRisk(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if path != "/fapi/v2/positionRisk" {
		t.Errorf("path = %s, want /fapi/v2/positionRisk", path)
	}
	if len(rows) != 1 || rows[0].PositionAmt != "-10.000" || rows[0].PositionSide != "SHORT" {
		t.Errorf("rows = %+v", rows)
	}
}

func TestNewOrderSignedAndShaped(t *testing.T) {
	var gotPath, gotMethod string
	var q url.Values
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		verifySigned(t, r)
		q = r.URL.Query()
		_, _ = w.Write([]byte(`{"clientOrderId":"cid1","orderId":22542179,"status":"NEW","symbol":"BTCUSDT","side":"BUY","type":"LIMIT","timeInForce":"GTX","origQty":"0.001","price":"50000"}`))
	})
	o, err := c.NewOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT", TimeInForce: "GTX", Quantity: "0.001", Price: "50000", NewClientOrderID: "cid1"})
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/fapi/v1/order" {
		t.Errorf("%s %s", gotMethod, gotPath)
	}
	for k, v := range map[string]string{"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "timeInForce": "GTX", "quantity": "0.001", "price": "50000", "newClientOrderId": "cid1"} {
		if q.Get(k) != v {
			t.Errorf("param %s = %q, want %q", k, q.Get(k), v)
		}
	}
	if o.OrderID != 22542179 || o.Status != "NEW" {
		t.Errorf("order = %+v", o)
	}
}

func TestBatchCapsAreLocal(t *testing.T) {
	c := newTestClient(t, func(http.ResponseWriter, *http.Request) { t.Error("request sent") })
	if _, err := c.BatchOrders(context.Background(), make([]OrderReq, 6)); !errors.Is(err, ErrTooManyOrders) {
		t.Errorf("6 orders: err = %v, want ErrTooManyOrders", err)
	}
	if _, err := c.CancelBatch(context.Background(), "BTCUSDT", make([]string, 11)); !errors.Is(err, ErrTooManyOrders) {
		t.Errorf("11 cancels: err = %v, want ErrTooManyOrders", err)
	}
}

func TestErrorClassification(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/order":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":-1007,"msg":"Timeout waiting for response from backend server."}`))
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests."}`))
		}
	})
	_, err := c.NewOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT", TimeInForce: "GTC", Quantity: "1", Price: "1"})
	if !IsUncertain(err) {
		t.Errorf("5xx: IsUncertain(%v) = false", err)
	}
	_, err = c.OpenOrders(context.Background(), "BTCUSDT")
	if !IsRateLimited(err) {
		t.Errorf("429: IsRateLimited(%v) = false", err)
	}
}

// The user stream must dial /ws/<listenKey> with no query (Aster's layout) and
// deliver ORDER_TRADE_UPDATE using the documented payload.
func TestUserStreamDialsKeyInPath(t *testing.T) {
	var mu sync.Mutex
	var wsURI string
	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/listenKey", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MBX-APIKEY") != "test-key" {
			t.Errorf("listenKey without key header")
		}
		if r.URL.RawQuery != "" {
			t.Errorf("listenKey is unsigned, got query %q", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"listenKey":"LK123"}`))
	})
	mux.HandleFunc("/ws/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		wsURI = r.URL.RequestURI()
		mu.Unlock()
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		frame := `{"e":"ORDER_TRADE_UPDATE","E":1568879465651,"T":1568879465650,"o":{"s":"BTCUSDT","c":"TEST","S":"SELL","o":"LIMIT","f":"GTX","q":"0.001","p":"7100","ap":"0","x":"NEW","X":"NEW","i":8886774,"l":"0","z":"0","L":"0","t":0,"m":false,"R":false,"ps":"BOTH","T":1568879465651}}`
		_ = conn.Write(r.Context(), coderws.MessageText, []byte(frame))
		<-r.Context().Done()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c, err := New(Config{BaseURL: srv.URL, APIKey: "test-key", APISecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	u, err := NewUserStream(UserStreamConfig{Client: c, WSURL: "ws" + strings.TrimPrefix(srv.URL, "http"), ReconnectIntervalMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan OrderTradeUpdate, 1)
	u.SetOnOrderUpdate(func(e OrderTradeUpdate) {
		select {
		case got <- e:
		default:
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := u.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer u.Stop()
	select {
	case e := <-got:
		if e.Order.OrderID != 8886774 || e.Order.Status != "NEW" || e.Order.TimeInForce != "GTX" || e.Order.ClientOrderID != "TEST" {
			t.Errorf("event = %+v", e.Order)
		}
	case <-ctx.Done():
		t.Fatal("no ORDER_TRADE_UPDATE delivered")
	}
	mu.Lock()
	defer mu.Unlock()
	if wsURI != "/ws/LK123" {
		t.Errorf("dialed %q, want /ws/LK123", wsURI)
	}
}

func TestMarketOptions(t *testing.T) {
	o := MarketOptions()
	if o.PerpMarketWS != "wss://fstream.asterdex.com/ws" || o.PerpPublicWS != o.PerpMarketWS || o.PerpREST != "https://fapi.asterdex.com" {
		t.Errorf("MarketOptions = %+v", o)
	}
}
