package futures

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestBatchOrdersMixed verifies a 200 response containing one order and one
// error object decodes per-item (the error is not a Go error of the call).
func TestBatchOrdersMixed(t *testing.T) {
	const resp = `[
	  {"orderId":111,"clientOrderId":"c1","symbol":"BTCUSDT","side":"BUY","status":"NEW","price":"60000.0","origQty":"0.001","executedQty":"0","avgPrice":"0.0","timeInForce":"GTC","reduceOnly":false,"updateTime":1700000000000},
	  {"code":-5022,"msg":"Due to the order could not be executed as maker, the Post Only order will be rejected. The order will not be recorded in the order history"}
	]`
	var method, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		_, _ = w.Write([]byte(resp))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	items, err := c.BatchOrders(context.Background(), []OrderReq{
		{Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT", TimeInForce: "GTC", Quantity: "0.001", Price: "60000.0", NewClientOrderID: "c1"},
		{Symbol: "BTCUSDT", Side: "SELL", Type: "LIMIT", TimeInForce: "GTX", Quantity: "0.001", Price: "70000.0", NewClientOrderID: "c2"},
	})
	if err != nil {
		t.Fatalf("BatchOrders returned a Go error for a 200 batch: %v", err)
	}
	if method != http.MethodPost || path != "/fapi/v1/batchOrders" {
		t.Fatalf("request = %s %s", method, path)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	if items[0].Order == nil || items[0].Err != nil {
		t.Fatalf("item 0: %+v", items[0])
	}
	if items[0].Order.OrderID != 111 || items[0].Order.ClientOrderID != "c1" {
		t.Fatalf("item 0 order wrong: %+v", items[0].Order)
	}
	if items[1].Err == nil || items[1].Order != nil {
		t.Fatalf("item 1: %+v", items[1])
	}
	if items[1].Err.Code != -5022 {
		t.Fatalf("item 1 code = %d, want -5022", items[1].Err.Code)
	}
}

// TestBatchOrderParamEncoding checks batchOrders is URL-encoded JSON whose
// field names match the single-order parameters (reduceOnly as a string).
func TestBatchOrderParamEncoding(t *testing.T) {
	var batchParam, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		batchParam = r.URL.Query().Get("batchOrders")
		_, _ = w.Write([]byte(`[{"orderId":1}]`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	if _, err := c.BatchOrders(context.Background(), []OrderReq{{
		Symbol: "ETHUSDT", Side: "SELL", Type: "LIMIT", TimeInForce: "GTC",
		Quantity: "1.5", Price: "3000.10", ReduceOnly: true, NewClientOrderID: "x9",
	}}); err != nil {
		t.Fatalf("BatchOrders: %v", err)
	}
	var decoded []map[string]string
	if err := json.Unmarshal([]byte(batchParam), &decoded); err != nil {
		t.Fatalf("batchOrders param is not JSON: %q (%v)", batchParam, err)
	}
	if len(decoded) != 1 {
		t.Fatalf("decoded %d orders", len(decoded))
	}
	o := decoded[0]
	want := map[string]string{
		"symbol": "ETHUSDT", "side": "SELL", "type": "LIMIT", "timeInForce": "GTC",
		"quantity": "1.5", "price": "3000.10", "reduceOnly": "true",
		"newClientOrderId": "x9", "newOrderRespType": "ACK",
	}
	for k, v := range want {
		if o[k] != v {
			t.Errorf("batch order[%s] = %q, want %q", k, o[k], v)
		}
	}
	if !containsAll(query, "batchOrders=", "signature=") {
		t.Errorf("query missing batchOrders/signature: %q", query)
	}
}

// TestBatchOrdersTooManyRejectedLocally checks >5 orders never reaches the
// server.
func TestBatchOrdersTooManyRejectedLocally(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	reqs := make([]OrderReq, 6)
	for i := range reqs {
		reqs[i] = OrderReq{Symbol: "BTCUSDT", Side: "BUY", Type: "MARKET", Quantity: "0.001"}
	}
	items, err := c.BatchOrders(context.Background(), reqs)
	if err == nil {
		t.Fatal("expected local error for 6 orders")
	}
	if !errors.Is(err, ErrTooManyOrders) {
		t.Fatalf("error = %v, want ErrTooManyOrders", err)
	}
	if items != nil {
		t.Fatalf("items = %v, want nil", items)
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("server hit %d times, want 0", got)
	}
}

// TestCancelBatchPerItemError checks per-item -2011 decoding and the
// origClientOrderIdList parameter.
func TestCancelBatchPerItemError(t *testing.T) {
	var method, path, symbol, list string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		q := r.URL.Query()
		symbol, list = q.Get("symbol"), q.Get("origClientOrderIdList")
		_, _ = w.Write([]byte(`[{"code":-2011,"msg":"Unknown order sent."}]`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	items, err := c.CancelBatch(context.Background(), "BTCUSDT", []string{"a1", "a2"})
	if err != nil {
		t.Fatalf("CancelBatch: %v", err)
	}
	if method != http.MethodDelete || path != "/fapi/v1/batchOrders" {
		t.Fatalf("request = %s %s", method, path)
	}
	if symbol != "BTCUSDT" {
		t.Errorf("symbol = %q", symbol)
	}
	var ids []string
	if err := json.Unmarshal([]byte(list), &ids); err != nil || len(ids) != 2 || ids[0] != "a1" {
		t.Errorf("origClientOrderIdList = %q (%v)", list, err)
	}
	if len(items) != 1 || items[0].Err == nil {
		t.Fatalf("items = %+v", items)
	}
	if items[0].Err.Code != -2011 {
		t.Fatalf("code = %d, want -2011", items[0].Err.Code)
	}
	if !IsUnknownOrder(items[0].Err) {
		t.Fatalf("IsUnknownOrder(-2011) = false")
	}
}

// TestCancelBatchTooMany checks the >10 local rejection.
func TestCancelBatchTooMany(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	ids := make([]string, 11)
	for i := range ids {
		ids[i] = "id"
	}
	if _, err := c.CancelBatch(context.Background(), "BTCUSDT", ids); !errors.Is(err, ErrTooManyOrders) {
		t.Fatalf("err = %v, want ErrTooManyOrders", err)
	}
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("server hit %d times, want 0", got)
	}
}

// TestCancelOrder checks the single-cancel parameters and response.
func TestCancelOrder(t *testing.T) {
	var method, path, symbol, ocid string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		symbol, ocid = r.URL.Query().Get("symbol"), r.URL.Query().Get("origClientOrderId")
		_, _ = w.Write([]byte(`{"orderId":55,"clientOrderId":"c1","status":"CANCELED","symbol":"BTCUSDT"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	o, err := c.CancelOrder(context.Background(), "BTCUSDT", "c1")
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if method != http.MethodDelete || path != "/fapi/v1/order" {
		t.Fatalf("request = %s %s", method, path)
	}
	if symbol != "BTCUSDT" || ocid != "c1" {
		t.Fatalf("params symbol=%q ocid=%q", symbol, ocid)
	}
	if o.OrderID != 55 || o.Status != "CANCELED" {
		t.Fatalf("order = %+v", o)
	}
}

// TestCancelAllAndCountdown checks endpoint/param wiring.
func TestCancelAllAndCountdown(t *testing.T) {
	type call struct{ method, path, symbol, countdown string }
	var calls []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		calls = append(calls, call{r.Method, r.URL.Path, q.Get("symbol"), q.Get("countdownTime")})
		_, _ = w.Write([]byte(`{"code":200,"msg":"success"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	if err := c.CancelAll(context.Background(), "BTCUSDT"); err != nil {
		t.Fatalf("CancelAll: %v", err)
	}
	if err := c.CountdownCancelAll(context.Background(), "BTCUSDT", 30000); err != nil {
		t.Fatalf("CountdownCancelAll: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %d", len(calls))
	}
	if calls[0].method != http.MethodDelete || calls[0].path != "/fapi/v1/allOpenOrders" || calls[0].symbol != "BTCUSDT" {
		t.Errorf("CancelAll call = %+v", calls[0])
	}
	if calls[1].method != http.MethodPost || calls[1].path != "/fapi/v1/countdownCancelAll" || calls[1].countdown != "30000" {
		t.Errorf("Countdown call = %+v", calls[1])
	}
}

// TestOpenOrdersAndLeverage checks the remaining simple endpoints.
func TestOpenOrdersAndLeverage(t *testing.T) {
	var symbolAll, symbolFiltered, levSymbol, lev string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch r.URL.Path {
		case "/fapi/v1/openOrders":
			if q.Get("symbol") == "" {
				symbolAll = "ALL"
			} else {
				symbolFiltered = q.Get("symbol")
			}
			_, _ = w.Write([]byte(`[{"orderId":2,"symbol":"BTCUSDT"}]`))
		case "/fapi/v1/leverage":
			levSymbol, lev = q.Get("symbol"), q.Get("leverage")
			_, _ = w.Write([]byte(`{"symbol":"BTCUSDT","leverage":10}`))
		}
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	all, err := c.OpenOrders(context.Background(), "")
	if err != nil || len(all) != 1 {
		t.Fatalf("OpenOrders(all): %v %+v", err, all)
	}
	if symbolAll != "ALL" {
		t.Errorf("symbol \"\" did not produce an all-symbols query")
	}
	if _, err := c.OpenOrders(context.Background(), "ETHUSDT"); err != nil {
		t.Fatalf("OpenOrders(ETHUSDT): %v", err)
	}
	if symbolFiltered != "ETHUSDT" {
		t.Errorf("openOrders symbol = %q", symbolFiltered)
	}
	if err := c.SetLeverage(context.Background(), "BTCUSDT", 10); err != nil {
		t.Fatalf("SetLeverage: %v", err)
	}
	if levSymbol != "BTCUSDT" || lev != "10" {
		t.Errorf("leverage params = %q/%q", levSymbol, lev)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
