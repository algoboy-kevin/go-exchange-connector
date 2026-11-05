package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// Recorded payloads from the Hyperliquid exchange-endpoint docs (checked
// 2026-11-05 via exchange-docs-hub, llms-full.txt "Place an order", "Cancel
// order(s)", "Cancel order(s) by cloid"). These pin the documented key names
// and the documented per-item responses. No network.

// docs example: 200 OK Successful Response (resting).
const docsPlaceResting = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77738308}}]}}}`

// docs example: 200 OK Error Response.
const docsPlaceError = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order must have minimum value of $10."}]}}}`

// docs example: 200 OK Successful Response (filled).
const docsPlaceFilled = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.02","avgPx":"1891.4","oid":77747314}}]}}}`

// docs example: cancel success and cancel error.
const docsCancelOK = `{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}`
const docsCancelErr = `{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"error":"Order was never placed, already canceled, or filled."}]}}}`

func TestDocsPlaceResponseExamplesMap(t *testing.T) {
	cases := []struct {
		name string
		body string
		want func(s OrderStatus) bool
	}{
		{"resting", docsPlaceResting, func(s OrderStatus) bool {
			return s.Resting && !s.Filled && s.Oid == 77738308 && s.Error == ""
		}},
		{"error", docsPlaceError, func(s OrderStatus) bool {
			return !s.Resting && !s.Filled && s.Error == "Order must have minimum value of $10."
		}},
		{"filled", docsPlaceFilled, func(s OrderStatus) bool {
			return s.Filled && !s.Resting && s.Oid == 77747314 && s.TotalSz == "0.02" && s.AvgPx == "1891.4" && s.Error == ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setExchange(tc.body, http.StatusOK)
			st, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifAlo}})
			if err != nil {
				t.Fatalf("Place: %v (per-item result must not be a call error)", err)
			}
			if len(st) != 1 || !tc.want(st[0]) {
				t.Fatalf("status = %+v, does not match the documented %s example", st, tc.name)
			}
		})
	}
}

func TestDocsCancelResponseExamplesMap(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(docsCancelOK, http.StatusOK)
	res, err := c.CancelByOid(context.Background(), []OidCancel{{Coin: "BTC", Oid: 77738308}})
	if err != nil {
		t.Fatalf("CancelByOid: %v", err)
	}
	if len(res) != 1 || !res[0].OK || res[0].Error != "" {
		t.Fatalf("documented success example mapped to %+v", res)
	}

	v.setExchange(docsCancelErr, http.StatusOK)
	res, err = c.CancelByOid(context.Background(), []OidCancel{{Coin: "BTC", Oid: 77738308}})
	if err != nil {
		t.Fatalf("CancelByOid (error example): %v", err)
	}
	if len(res) != 1 || res[0].OK || res[0].Error != "Order was never placed, already canceled, or filled." {
		t.Fatalf("documented error example mapped to %+v", res)
	}
}

// Request shape: the documented key names, and no optional false flags.
func TestDocsOrderRequestShape(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(placeResponse(`{"resting":{"oid":77738308}}`), http.StatusOK)
	if _, err := c.Place(context.Background(), []Order{
		{Coin: "BTC", IsBuy: true, Px: "100000", Sz: "0.001", Tif: TifAlo, Cloid: testCloid1},
	}); err != nil {
		t.Fatalf("Place: %v", err)
	}
	var env struct {
		Action map[string]json.RawMessage `json:"action"`
		Nonce  json.RawMessage            `json:"nonce"`
	}
	if err := json.Unmarshal(lastExchangeBody(t, v), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range []string{"type", "orders", "grouping"} {
		if _, ok := env.Action[k]; !ok {
			t.Errorf("action missing documented key %q", k)
		}
	}
	var orders []map[string]json.RawMessage
	if err := json.Unmarshal(env.Action["orders"], &orders); err != nil || len(orders) != 1 {
		t.Fatalf("orders = %s (err %v)", env.Action["orders"], err)
	}
	for _, k := range []string{"a", "b", "p", "s", "r", "t", "c"} {
		if _, ok := orders[0][k]; !ok {
			t.Errorf("order item missing documented key %q: %v", k, orders[0])
		}
	}
	if _, ok := env.Action["builder"]; ok {
		t.Error("builder present without a builder fee: documented as Optional")
	}
	if env.Nonce == nil {
		t.Error("nonce missing (documented as required)")
	}
}

func TestDocsCancelRequestShapes(t *testing.T) {
	type envelope struct {
		Action map[string]json.RawMessage `json:"action"`
	}

	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(docsCancelOK, http.StatusOK)
	if _, err := c.CancelByOid(context.Background(), []OidCancel{{Coin: "BTC", Oid: 77738308}}); err != nil {
		t.Fatalf("CancelByOid: %v", err)
	}
	var oidEnv envelope
	if err := json.Unmarshal(lastExchangeBody(t, v), &oidEnv); err != nil {
		t.Fatalf("decode oid cancel: %v", err)
	}
	if string(oidEnv.Action["type"]) != `"cancel"` {
		t.Errorf("oid cancel type = %s, want \"cancel\"", oidEnv.Action["type"])
	}
	var oidCancels []map[string]json.RawMessage
	if err := json.Unmarshal(oidEnv.Action["cancels"], &oidCancels); err != nil || len(oidCancels) != 1 {
		t.Fatalf("oid cancels = %s (err %v)", oidEnv.Action["cancels"], err)
	}
	for _, k := range []string{"a", "o"} {
		if _, ok := oidCancels[0][k]; !ok {
			t.Errorf("oid cancel item missing documented key %q: %v", k, oidCancels[0])
		}
	}
	if _, ok := oidEnv.Action["f"]; ok {
		t.Error("f present: documented as skipped when false")
	}

	v.setExchange(`{"status":"ok","response":{"type":"cancelByCloid","data":{"statuses":["success"]}}}`, http.StatusOK)
	if _, err := c.CancelByCloid(context.Background(), []CancelReq{{Coin: "BTC", Cloid: testCloid1}}); err != nil {
		t.Fatalf("CancelByCloid: %v", err)
	}
	var clEnv envelope
	if err := json.Unmarshal(lastExchangeBody(t, v), &clEnv); err != nil {
		t.Fatalf("decode cloid cancel: %v", err)
	}
	if string(clEnv.Action["type"]) != `"cancelByCloid"` {
		t.Errorf("cloid cancel type = %s, want \"cancelByCloid\"", clEnv.Action["type"])
	}
	var clCancels []map[string]json.RawMessage
	if err := json.Unmarshal(clEnv.Action["cancels"], &clCancels); err != nil || len(clCancels) != 1 {
		t.Fatalf("cloid cancels = %s (err %v)", clEnv.Action["cancels"], err)
	}
	for _, k := range []string{"asset", "cloid"} {
		if _, ok := clCancels[0][k]; !ok {
			t.Errorf("cloid cancel item missing documented key %q: %v", k, clCancels[0])
		}
	}
}
