package bnspot

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/spot"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

// Documentation example key/secret (llms-full.txt line 117957); not credentials.
const (
	docKey    = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A"
	docSecret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
)

type srv struct {
	hits atomic.Int64
	mu   sync.Mutex
	qs   []url.Values
}

// newAdapter starts a stub whose handler runs for every request and returns an
// adapter pointed at it.
func newAdapter(t *testing.T, h func(n int, r *http.Request, w http.ResponseWriter), opts ...Option) (*Adapter, *srv) {
	t.Helper()
	s := &srv{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(s.hits.Add(1))
		s.mu.Lock()
		s.qs = append(s.qs, r.URL.Query())
		s.mu.Unlock()
		h(n, r, w)
	}))
	t.Cleanup(ts.Close)
	c, err := spot.New(spot.Config{BaseURL: ts.URL, APIKey: docKey, APISecret: docSecret})
	if err != nil {
		t.Fatal(err)
	}
	return New(c, opts...), s
}

func (s *srv) query(i int) url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qs[i]
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func always(status int, body string) func(int, *http.Request, http.ResponseWriter) {
	return func(_ int, _ *http.Request, w http.ResponseWriter) { writeJSON(w, status, body) }
}

func orderBody(id int, status, executed, quote string) string {
	return `{"symbol":"BTCUSDT","orderId":` + strconv.Itoa(id) + `,"orderListId":-1,"clientOrderId":"c","transactTime":1,"price":"100","origQty":"1","executedQty":"` + executed +
		`","cummulativeQuoteQty":"` + quote + `","status":"` + status + `","timeInForce":"GTC","type":"LIMIT","side":"BUY"}`
}

var good = contract.OrderRequest{Symbol: "BTCUSDT", Side: contract.Buy, Price: "100", Qty: "1", TIF: contract.GTC, ClientID: "cid-1"}

func withTIF(r contract.OrderRequest, t contract.TIF, id string) contract.OrderRequest {
	r.TIF, r.ClientID = t, id
	return r
}

// ─────────────────────────────────────────────────────────────
// Mapping
// ─────────────────────────────────────────────────────────────

func TestTIFMapping(t *testing.T) {
	cases := []struct {
		in       contract.TIF
		typ, tif string
	}{
		{contract.GTC, "LIMIT", "GTC"},
		{contract.IOC, "LIMIT", "IOC"},
		{contract.PostOnly, "LIMIT_MAKER", ""},
	}
	for _, c := range cases {
		typ, tif, err := orderTypeAndTIF(c.in)
		if err != nil || typ != c.typ || tif != c.tif {
			t.Errorf("%s -> %q %q %v, want %q %q", c.in, typ, tif, err, c.typ, c.tif)
		}
	}
	if _, _, err := orderTypeAndTIF("FOK"); !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("FOK is not in the contract; want ErrInvalid, got %v", err)
	}
}

func TestToOrderReq(t *testing.T) {
	n, err := toOrderReq(withTIF(good, contract.PostOnly, "post-1"))
	if err != nil {
		t.Fatal(err)
	}
	if n.Type != "LIMIT_MAKER" || n.TimeInForce != "" || n.NewClientOrderID != "post-1" || n.NewOrderRespType != "RESULT" || n.Side != "BUY" || n.Quantity != "1" || n.Price != "100" {
		t.Fatalf("%+v", n)
	}
	n, _ = toOrderReq(withTIF(good, contract.IOC, ""))
	if n.Type != "LIMIT" || n.TimeInForce != "IOC" || n.NewClientOrderID != "" {
		t.Fatalf("%+v", n)
	}
}

func TestToOrderReqRejectsLocally(t *testing.T) {
	ro := good
	ro.ReduceOnly = true
	_, err := toOrderReq(ro)
	if !errors.Is(err, contract.ErrUnsupported) || !errors.Is(err, contract.ErrInvalid) {
		t.Fatalf("reduce-only: want ErrUnsupported and ErrInvalid, got %v", err)
	}
	mut := func(fn func(*contract.OrderRequest)) contract.OrderRequest { r := good; fn(&r); return r }
	bad := map[string]contract.OrderRequest{
		"zero price":     mut(func(r *contract.OrderRequest) { r.Price = "0" }),
		"exp qty":        mut(func(r *contract.OrderRequest) { r.Qty = "1e-3" }),
		"bad tif":        withTIF(good, "FOK", "x"),
		"client id len":  withTIF(good, contract.GTC, strings.Repeat("a", 37)),
		"client id char": withTIF(good, contract.GTC, "a b"),
		"no symbol":      mut(func(r *contract.OrderRequest) { r.Symbol = "" }),
		"bad side":       mut(func(r *contract.OrderRequest) { r.Side = "X" }),
	}
	for name, r := range bad {
		if _, err := toOrderReq(r); !errors.Is(err, contract.ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", name, err)
		}
	}
}

func TestMapStatus(t *testing.T) {
	cases := map[string]contract.Status{
		"NEW": contract.Resting, "PARTIALLY_FILLED": contract.Resting, "FILLED": contract.Filled,
		"REJECTED": contract.Rejected, "EXPIRED": contract.Rejected, "CANCELED": contract.Rejected, "EXPIRED_IN_MATCH": contract.Rejected,
		"PENDING_NEW": contract.Unknown, "": contract.Unknown, "WEIRD": contract.Unknown,
	}
	for in, want := range cases {
		if got := mapStatus(in); got != want {
			t.Errorf("mapStatus(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestBatchLimiter(t *testing.T) {
	a := New(nil)
	if a.MaxPlaceBatch() != 5 || a.MaxCancelBatch() != 5 {
		t.Fatalf("%d %d", a.MaxPlaceBatch(), a.MaxCancelBatch())
	}
}

// ─────────────────────────────────────────────────────────────
// PlaceOrder
// ─────────────────────────────────────────────────────────────

func TestPlaceOrderMapsResult(t *testing.T) {
	a, s := newAdapter(t, always(200, orderBody(28, "PARTIALLY_FILLED", "0.50000000", "49.00000000")))
	res, err := a.PlaceOrder(context.Background(), good)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != contract.Resting || res.OrderID != "28" || res.ClientID != "cid-1" || res.FilledQty != "0.50000000" || res.AvgPrice != "98.00000000" {
		t.Fatalf("%+v", res)
	}
	if res.SentAt.IsZero() || res.AckAt.Before(res.SentAt) {
		t.Fatalf("timestamps %v %v", res.SentAt, res.AckAt)
	}
	q := s.query(0)
	if q.Get("type") != "LIMIT" || q.Get("timeInForce") != "GTC" || q.Get("newClientOrderId") != "cid-1" || q.Get("newOrderRespType") != "RESULT" {
		t.Fatalf("%v", q)
	}
}

func TestPlaceOrderPostOnlyIsLimitMaker(t *testing.T) {
	a, s := newAdapter(t, always(200, orderBody(1, "NEW", "0.00000000", "0.00000000")))
	res, err := a.PlaceOrder(context.Background(), withTIF(good, contract.PostOnly, "p1"))
	if err != nil || res.Status != contract.Resting || res.AvgPrice != "" {
		t.Fatalf("%+v %v", res, err)
	}
	q := s.query(0)
	if q.Get("type") != "LIMIT_MAKER" || q.Has("timeInForce") {
		t.Fatalf("post-only must be LIMIT_MAKER with no timeInForce: %v", q)
	}
}

func TestPlaceOrderRejections(t *testing.T) {
	// -2010 "Order would immediately match and take." is a definite refusal: Rejected, no error.
	a, _ := newAdapter(t, always(400, `{"code":-2010,"msg":"Order would immediately match and take."}`))
	res, err := a.PlaceOrder(context.Background(), withTIF(good, contract.PostOnly, "p"))
	if err != nil || res.Status != contract.Rejected || res.Reason != "Order would immediately match and take." || res.ClientID != "p" {
		t.Fatalf("%+v %v", res, err)
	}
	// Status-level reject inside a 200.
	b, _ := newAdapter(t, always(200, orderBody(5, "EXPIRED", "0.00000000", "0.00000000")))
	res, err = b.PlaceOrder(context.Background(), withTIF(good, contract.IOC, "i"))
	if err != nil || res.Status != contract.Rejected || res.Reason != "EXPIRED" {
		t.Fatalf("%+v %v", res, err)
	}
	c, _ := newAdapter(t, always(200, orderBody(5, "SOMETHING_NEW", "0", "0")))
	res, err = c.PlaceOrder(context.Background(), good)
	if err != nil || res.Status != contract.Unknown || res.Reason == "" {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestPlaceOrderUncertainIsUnknown(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"500":   {500, `{"code":-1000,"msg":"x"}`},
		"-1007": {503, `{"code":-1007,"msg":"Timeout waiting for response from backend server."}`},
	} {
		a, _ := newAdapter(t, always(tc.status, tc.body))
		res, err := a.PlaceOrder(context.Background(), good)
		if res.Status != contract.Unknown || res.ClientID != "cid-1" || !errors.Is(err, contract.ErrUncertain) {
			t.Errorf("%s: %+v %v", name, res, err)
		}
		var ae *spot.APIError
		if !errors.As(err, &ae) {
			t.Errorf("%s: native error not wrapped: %v", name, err)
		}
	}
}

func TestPlaceOrderCallLevelErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"429":  {429, `{"code":-1003,"msg":"Too many requests"}`},
		"418":  {418, `{"code":-1003,"msg":"banned"}`},
		"403":  {403, `{"code":0,"msg":"WAF"}`},
		"1021": {400, `{"code":-1021,"msg":"Timestamp outside recvWindow"}`},
		"1022": {400, `{"code":-1022,"msg":"Signature not valid"}`},
		"2015": {401, `{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`},
	} {
		a, _ := newAdapter(t, always(tc.status, tc.body))
		res, err := a.PlaceOrder(context.Background(), good)
		if err == nil || errors.Is(err, contract.ErrUncertain) {
			t.Errorf("%s: want a non-uncertain call error, got %v", name, err)
		}
		var ae *spot.APIError
		if !errors.As(err, &ae) || ae.HTTPStatus != tc.status {
			t.Errorf("%s: native error not wrapped: %v", name, err)
		}
		if res.Status == contract.Resting || res.Status == contract.Filled {
			t.Errorf("%s: %+v", name, res)
		}
	}
}

func TestPlaceOrderLocalErrorsSendNothing(t *testing.T) {
	a, s := newAdapter(t, always(200, `{}`))
	ro := good
	ro.ReduceOnly = true
	if _, err := a.PlaceOrder(context.Background(), ro); !errors.Is(err, contract.ErrUnsupported) {
		t.Fatal(err)
	}
	bad := good
	bad.Price = "-1"
	if _, err := a.PlaceOrder(context.Background(), bad); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal(err)
	}
	if s.hits.Load() != 0 {
		t.Fatalf("%d requests", s.hits.Load())
	}
}

// ─────────────────────────────────────────────────────────────
// PlaceOrders
// ─────────────────────────────────────────────────────────────

func batch(ids ...string) []contract.OrderRequest {
	out := make([]contract.OrderRequest, len(ids))
	for i, id := range ids {
		out[i] = good
		out[i].ClientID = id
	}
	return out
}

func TestPlaceOrdersSequentialInOrder(t *testing.T) {
	a, s := newAdapter(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 2 {
			writeJSON(w, 400, `{"code":-2010,"msg":"Account has insufficient balance for requested action."}`)
			return
		}
		writeJSON(w, 200, orderBody(10+n, "NEW", "0", "0"))
	})
	res, err := a.PlaceOrders(context.Background(), batch("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 3 {
		t.Fatalf("%d results", len(res))
	}
	if res[0].Status != contract.Resting || res[0].OrderID != "11" || res[0].ClientID != "a" ||
		res[1].Status != contract.Rejected || res[1].ClientID != "b" || !strings.Contains(res[1].Reason, "insufficient balance") ||
		res[2].Status != contract.Resting || res[2].OrderID != "13" || res[2].ClientID != "c" {
		t.Fatalf("%+v", res)
	}
	if s.hits.Load() != 3 {
		t.Fatalf("%d requests; the item after a rejection must still be sent", s.hits.Load())
	}
	for i, id := range []string{"a", "b", "c"} { // sequential, in request order
		if s.query(i).Get("newClientOrderId") != id {
			t.Fatalf("request %d carried %q", i, s.query(i).Get("newClientOrderId"))
		}
	}
}

func TestPlaceOrdersStopsOnUncertain(t *testing.T) {
	a, s := newAdapter(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 2 {
			writeJSON(w, 503, `{"code":-1007,"msg":"Timeout"}`)
			return
		}
		writeJSON(w, 200, orderBody(10+n, "NEW", "0", "0"))
	})
	res, err := a.PlaceOrders(context.Background(), batch("a", "b", "c", "d"))
	if !errors.Is(err, contract.ErrUncertain) {
		t.Fatalf("err %v", err)
	}
	if len(res) != 4 {
		t.Fatalf("%d results", len(res))
	}
	if res[0].Status != contract.Resting {
		t.Fatalf("%+v", res[0])
	}
	if res[1].Status != contract.Unknown || res[1].ClientID != "b" {
		t.Fatalf("%+v", res[1])
	}
	for _, r := range res[2:] {
		if r.Status != contract.Unknown || !strings.HasPrefix(r.Reason, "not sent") || !r.SentAt.IsZero() {
			t.Fatalf("unsent item: %+v", r)
		}
	}
	if res[2].ClientID != "c" || res[3].ClientID != "d" {
		t.Fatalf("%+v", res)
	}
	if s.hits.Load() != 2 {
		t.Fatalf("%d requests; nothing may be sent after an unknown outcome", s.hits.Load())
	}
}

func TestPlaceOrdersStopsOnRateLimit(t *testing.T) {
	a, s := newAdapter(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 2 {
			writeJSON(w, 429, `{"code":-1003,"msg":"Too many requests"}`)
			return
		}
		writeJSON(w, 200, orderBody(10+n, "NEW", "0", "0"))
	})
	res, err := a.PlaceOrders(context.Background(), batch("a", "b", "c"))
	if err == nil || errors.Is(err, contract.ErrUncertain) || !spot.IsRateLimited(err) {
		t.Fatalf("err %v", err)
	}
	if len(res) != 3 || res[0].Status != contract.Resting ||
		res[1].Status != contract.Rejected || !strings.HasPrefix(res[1].Reason, "not sent") ||
		res[2].Status != contract.Rejected || !strings.HasPrefix(res[2].Reason, "not sent") {
		t.Fatalf("%+v", res)
	}
	if s.hits.Load() != 2 {
		t.Fatalf("%d requests", s.hits.Load())
	}
}

func TestPlaceOrdersValidatesAllBeforeSending(t *testing.T) {
	a, s := newAdapter(t, always(200, `{}`))
	reqs := batch("a", "b", "c")
	reqs[2].ReduceOnly = true // third one is unsupported
	if _, err := a.PlaceOrders(context.Background(), reqs); !errors.Is(err, contract.ErrUnsupported) || !errors.Is(err, contract.ErrInvalid) {
		t.Fatalf("%v", err)
	}
	reqs = batch("a", "b")
	reqs[1].Qty = "0"
	if _, err := a.PlaceOrders(context.Background(), reqs); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := a.PlaceOrders(context.Background(), nil); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal("empty batch accepted")
	}
	if _, err := a.PlaceOrders(context.Background(), batch("1", "2", "3", "4", "5", "6")); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal("batch over the local cap accepted")
	}
	if s.hits.Load() != 0 {
		t.Fatalf("%d requests were sent for invalid batches", s.hits.Load())
	}
}

// ─────────────────────────────────────────────────────────────
// Cancel
// ─────────────────────────────────────────────────────────────

const cancelBody = `{"symbol":"BTCUSDT","origClientOrderId":"cid-1","orderId":4,"orderListId":-1,"clientOrderId":"x","transactTime":1,"price":"1","origQty":"1","executedQty":"0","cummulativeQuoteQty":"0","status":"CANCELED","timeInForce":"GTC","type":"LIMIT","side":"BUY"}`

func TestCancelOrderPrefersOrderID(t *testing.T) {
	a, s := newAdapter(t, func(_ int, r *http.Request, w http.ResponseWriter) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v3/order" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, 200, cancelBody)
	})
	res, err := a.CancelOrder(context.Background(), contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "4", ClientID: "cid-1"})
	if err != nil || !res.Canceled || res.OrderID != "4" || res.ClientID != "cid-1" {
		t.Fatalf("%+v %v", res, err)
	}
	q := s.query(0)
	if q.Get("orderId") != "4" || q.Has("origClientOrderId") {
		t.Fatalf("OrderID must win and be the only id sent: %v", q)
	}
	res, err = a.CancelOrder(context.Background(), contract.CancelRequest{Symbol: "BTCUSDT", ClientID: "cid-1"})
	if err != nil || !res.Canceled || res.OrderID != "4" {
		t.Fatalf("%+v %v", res, err)
	}
	if q := s.query(1); q.Get("origClientOrderId") != "cid-1" || q.Has("orderId") {
		t.Fatalf("%v", q)
	}
}

func TestCancelOrderOutcomes(t *testing.T) {
	a, _ := newAdapter(t, always(400, `{"code":-2011,"msg":"Unknown order sent."}`))
	res, err := a.CancelOrder(context.Background(), contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "9"})
	if err != nil || res.Canceled || res.Reason != "Unknown order sent." {
		t.Fatalf("%+v %v", res, err)
	}
	b, _ := newAdapter(t, always(502, `bad gateway`))
	res, err = b.CancelOrder(context.Background(), contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "9"})
	if !errors.Is(err, contract.ErrUncertain) || res.Canceled || res.OrderID != "9" {
		t.Fatalf("%+v %v", res, err)
	}
	c, _ := newAdapter(t, always(429, `{"code":-1003,"msg":"x"}`))
	if _, err = c.CancelOrder(context.Background(), contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "9"}); !spot.IsRateLimited(err) {
		t.Fatalf("%v", err)
	}
}

func TestCancelOrderLocalErrors(t *testing.T) {
	a, s := newAdapter(t, always(200, cancelBody))
	for name, r := range map[string]contract.CancelRequest{
		"no symbol":     {OrderID: "1"},
		"no id":         {Symbol: "BTCUSDT"},
		"order id text": {Symbol: "BTCUSDT", OrderID: "abc"},
		"order id zero": {Symbol: "BTCUSDT", OrderID: "0"},
		"order id neg":  {Symbol: "BTCUSDT", OrderID: "-5"},
		"client id bad": {Symbol: "BTCUSDT", ClientID: "a b"},
	} {
		if _, err := a.CancelOrder(context.Background(), r); !errors.Is(err, contract.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if s.hits.Load() != 0 {
		t.Fatalf("%d requests", s.hits.Load())
	}
}

func TestCancelOrdersSequential(t *testing.T) {
	a, s := newAdapter(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 2 {
			writeJSON(w, 400, `{"code":-2011,"msg":"Unknown order sent."}`)
			return
		}
		writeJSON(w, 200, cancelBody)
	})
	reqs := []contract.CancelRequest{
		{Symbol: "BTCUSDT", OrderID: "4"},
		{Symbol: "ETHUSDT", ClientID: "e-1"}, // other symbol and id kind: allowed, each is its own request
		{Symbol: "BTCUSDT", OrderID: "6"},
	}
	res, err := a.CancelOrders(context.Background(), reqs)
	if err != nil || len(res) != 3 {
		t.Fatalf("%v %+v", err, res)
	}
	if !res[0].Canceled || res[1].Canceled || res[1].Reason != "Unknown order sent." || res[1].ClientID != "e-1" || !res[2].Canceled {
		t.Fatalf("%+v", res)
	}
	if s.hits.Load() != 3 {
		t.Fatalf("%d requests", s.hits.Load())
	}
	if s.query(1).Get("symbol") != "ETHUSDT" || s.query(1).Get("origClientOrderId") != "e-1" {
		t.Fatalf("%v", s.query(1))
	}
}

func TestCancelOrdersStopsOnUncertain(t *testing.T) {
	a, s := newAdapter(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 1 {
			writeJSON(w, 500, `{"code":-1000,"msg":"x"}`)
			return
		}
		writeJSON(w, 200, cancelBody)
	})
	res, err := a.CancelOrders(context.Background(), []contract.CancelRequest{{Symbol: "BTCUSDT", OrderID: "1"}, {Symbol: "BTCUSDT", OrderID: "2"}, {Symbol: "BTCUSDT", ClientID: "z"}})
	if !errors.Is(err, contract.ErrUncertain) || len(res) != 3 {
		t.Fatalf("%v %+v", err, res)
	}
	if res[0].Canceled || res[1].Canceled || !strings.HasPrefix(res[1].Reason, "not sent") || res[2].ClientID != "z" || !strings.HasPrefix(res[2].Reason, "not sent") {
		t.Fatalf("%+v", res)
	}
	if s.hits.Load() != 1 {
		t.Fatalf("%d requests", s.hits.Load())
	}
}

func TestCancelOrdersValidation(t *testing.T) {
	a, s := newAdapter(t, always(200, cancelBody))
	if _, err := a.CancelOrders(context.Background(), nil); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal(err)
	}
	six := make([]contract.CancelRequest, 6)
	for i := range six {
		six[i] = contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "1"}
	}
	if _, err := a.CancelOrders(context.Background(), six); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal("over cap accepted")
	}
	if _, err := a.CancelOrders(context.Background(), []contract.CancelRequest{{Symbol: "BTCUSDT", OrderID: "1"}, {Symbol: "BTCUSDT"}}); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal("invalid item accepted")
	}
	if s.hits.Load() != 0 {
		t.Fatalf("%d requests", s.hits.Load())
	}
}

// ─────────────────────────────────────────────────────────────
// CancelAll, OpenOrders, Account
// ─────────────────────────────────────────────────────────────

func TestCancelAll(t *testing.T) {
	a, s := newAdapter(t, func(_ int, r *http.Request, w http.ResponseWriter) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v3/openOrders" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		writeJSON(w, 200, `[`+cancelBody+`]`)
	})
	if err := a.CancelAll(context.Background(), "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if s.query(0).Get("symbol") != "BTCUSDT" {
		t.Fatal(s.query(0))
	}
	if err := a.CancelAll(context.Background(), ""); !errors.Is(err, contract.ErrInvalid) {
		t.Fatal(err)
	}
	// Nothing open (-2011) is success.
	b, _ := newAdapter(t, always(400, `{"code":-2011,"msg":"Unknown order sent."}`))
	if err := b.CancelAll(context.Background(), "BTCUSDT"); err != nil {
		t.Fatalf("nothing-open must be success, got %v", err)
	}
	c, _ := newAdapter(t, always(503, `x`))
	if err := c.CancelAll(context.Background(), "BTCUSDT"); !errors.Is(err, contract.ErrUncertain) {
		t.Fatal(err)
	}
	d, _ := newAdapter(t, always(400, `{"code":-1121,"msg":"Invalid symbol."}`))
	if err := d.CancelAll(context.Background(), "NOPE"); err == nil || errors.Is(err, contract.ErrUncertain) {
		t.Fatalf("a real rejection must surface: %v", err)
	}
}

func TestOpenOrders(t *testing.T) {
	a, s := newAdapter(t, always(200, `[{"symbol":"LTCBTC","orderId":1,"orderListId":-1,"clientOrderId":"myOrder1","price":"0.1","origQty":"1.0","executedQty":"0.4","cummulativeQuoteQty":"0.04","status":"PARTIALLY_FILLED","timeInForce":"GTC","type":"LIMIT","side":"SELL","time":1499827319000,"updateTime":1499827319559,"isWorking":true}]`))
	got, err := a.OpenOrders(context.Background(), "LTCBTC")
	if err != nil || len(got) != 1 {
		t.Fatalf("%v %+v", err, got)
	}
	o := got[0]
	if o.Symbol != "LTCBTC" || o.OrderID != "1" || o.ClientID != "myOrder1" || o.Side != contract.Sell || o.Price != "0.1" || o.Qty != "1.0" || o.FilledQty != "0.4" || o.ReduceOnly || !o.Timestamp.Equal(time.UnixMilli(1499827319559)) {
		t.Fatalf("%+v", o)
	}
	if s.query(0).Get("symbol") != "LTCBTC" {
		t.Fatal(s.query(0))
	}
	if _, err := a.OpenOrders(context.Background(), ""); !errors.Is(err, contract.ErrInvalid) || s.hits.Load() != 1 {
		t.Fatalf("%v hits=%d", err, s.hits.Load())
	}
}

func TestAccount(t *testing.T) {
	body := `{"updateTime":1700000000000,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1.0","locked":"0.0"},{"asset":"USDT","free":"100.25000000","locked":"20.50000000"}]}`
	a, s := newAdapter(t, func(_ int, r *http.Request, w http.ResponseWriter) {
		if r.URL.Path != "/api/v3/account" {
			t.Errorf("path %s", r.URL.Path)
		}
		writeJSON(w, 200, body)
	})
	acc, err := a.Account(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if acc.Currency != "USDT" || acc.Equity != "120.75000000" || acc.Available != "100.25000000" || !acc.At.Equal(time.UnixMilli(1700000000000)) {
		t.Fatalf("%+v", acc)
	}
	if s.query(0).Has("omitZeroBalances") {
		t.Fatal("balances must not be hidden")
	}
	b, _ := newAdapter(t, always(200, body), WithAccountAsset("BTC"))
	acc, _ = b.Account(context.Background())
	if acc.Currency != "BTC" || acc.Equity != "1.0" || acc.Available != "1.0" {
		t.Fatalf("%+v", acc)
	}
	c, _ := newAdapter(t, always(200, body), WithAccountAsset("DOGE"))
	acc, _ = c.Account(context.Background())
	if acc.Equity != "0" || acc.Available != "0" {
		t.Fatalf("absent asset should be zero: %+v", acc)
	}
}
