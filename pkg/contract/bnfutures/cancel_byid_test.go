package bnfutures

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/futures"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

func TestCancelByOrderID(t *testing.T) {
	var gotOID, gotCID, gotList string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		gotOID, gotCID, gotList = q.Get("orderId"), q.Get("origClientOrderId"), q.Get("orderIdList")
		if r.URL.Path == "/fapi/v1/batchOrders" {
			_, _ = w.Write([]byte(`[{"orderId":11,"status":"CANCELED"},{"code":-2011,"msg":"Unknown order sent."}]`))
			return
		}
		_, _ = w.Write([]byte(`{"orderId":11,"status":"CANCELED"}`))
	}))
	defer srv.Close()
	c, err := futures.New(futures.Config{BaseURL: srv.URL, APIKey: "k", APISecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	a := New(c)
	ctx := context.Background()

	res, err := a.CancelOrder(ctx, contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "11", ClientID: "ignored"})
	if err != nil || !res.Canceled || res.OrderID != "11" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if gotOID != "11" || gotCID != "" {
		t.Errorf("order id must win: orderId=%q origClientOrderId=%q", gotOID, gotCID)
	}

	rs, err := a.CancelOrders(ctx, []contract.CancelRequest{{Symbol: "BTCUSDT", OrderID: "11"}, {Symbol: "BTCUSDT", OrderID: "12"}})
	if err != nil {
		t.Fatal(err)
	}
	if gotList != "[11,12]" || !rs[0].Canceled || rs[1].Canceled || rs[1].Reason == "" {
		t.Errorf("list=%q results=%+v", gotList, rs)
	}

	_, err = a.CancelOrders(ctx, []contract.CancelRequest{{Symbol: "BTCUSDT", OrderID: "11"}, {Symbol: "BTCUSDT", ClientID: "c"}})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("mixed batch: err = %v, want ErrInvalid", err)
	}
	_, err = a.CancelOrder(ctx, contract.CancelRequest{Symbol: "BTCUSDT", OrderID: "abc"})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("bad order id: err = %v, want ErrInvalid", err)
	}
}
