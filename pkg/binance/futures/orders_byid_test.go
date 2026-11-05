package futures

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCancelOrderByIDAndGetOrder(t *testing.T) {
	var method, path string
	var q url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, q = r.Method, r.URL.Path, r.URL.Query()
		_, _ = w.Write([]byte(`{"orderId":283194212,"clientOrderId":"c1","symbol":"BTCUSDT","status":"CANCELED"}`))
	}))
	defer srv.Close()
	c := testClient(t, srv.URL)

	o, err := c.CancelOrderByID(context.Background(), "BTCUSDT", 283194212)
	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodDelete || path != "/fapi/v1/order" || q.Get("orderId") != "283194212" || q.Get("origClientOrderId") != "" {
		t.Errorf("cancel: %s %s %v", method, path, q)
	}
	if o.Status != "CANCELED" || o.OrderID != 283194212 {
		t.Errorf("order = %+v", o)
	}

	if _, err := c.GetOrder(context.Background(), "BTCUSDT", 0, "c1"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodGet || path != "/fapi/v1/order" || q.Get("origClientOrderId") != "c1" || q.Get("orderId") != "" {
		t.Errorf("get by client id: %s %s %v", method, path, q)
	}
	if _, err := c.GetOrder(context.Background(), "BTCUSDT", 7, "c1"); err != nil {
		t.Fatal(err)
	}
	if q.Get("orderId") != "7" || q.Get("origClientOrderId") != "" {
		t.Errorf("order id must win: %v", q)
	}
}

func TestCancelBatchByIDEncodingAndLimits(t *testing.T) {
	var raw string
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		raw = r.URL.Query().Get("orderIdList")
		_, _ = w.Write([]byte(`[{"orderId":1,"status":"CANCELED","symbol":"BTCUSDT"},{"code":-2011,"msg":"Unknown order sent."}]`))
	}))
	defer srv.Close()
	c := testClient(t, srv.URL)

	items, err := c.CancelBatchByID(context.Background(), "BTCUSDT", []int64{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if raw != "[1,2]" {
		t.Errorf("orderIdList = %q, want [1,2]", raw)
	}
	if len(items) != 2 || items[0].Err != nil || items[1].Err == nil || !IsUnknownOrder(items[1].Err) {
		t.Errorf("items = %+v", items)
	}
	calls = 0
	if _, err := c.CancelBatchByID(context.Background(), "BTCUSDT", make([]int64, 11)); !errors.Is(err, ErrTooManyOrders) {
		t.Errorf("11 ids: err = %v", err)
	}
	if _, err := c.CancelBatchByID(context.Background(), "BTCUSDT", []int64{0}); err == nil {
		t.Error("zero id accepted")
	}
	if _, err := c.CancelOrderByID(context.Background(), "", 5); err == nil || !strings.Contains(err.Error(), "symbol") {
		t.Errorf("empty symbol: %v", err)
	}
	if calls != 0 {
		t.Errorf("%d requests sent for locally invalid input", calls)
	}
}
