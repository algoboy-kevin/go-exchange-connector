package hyperliquid

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestInfoServer stands up an /info double. It records the last request body
// and replies with body, so tests can assert both directions of the contract.
func newTestInfoServer(t *testing.T, status int, body string) (*InfoClient, *[]map[string]any, *[]http.Header) {
	t.Helper()
	var bodies []map[string]any
	var headers []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		bodies = append(bodies, decoded)
		headers = append(headers, r.Header.Clone())

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewInfoClient(srv.URL), &bodies, &headers
}

func TestInfoClientMeta(t *testing.T) {
	c, bodies, headers := newTestInfoServer(t, http.StatusOK, `{
		"universe": [
			{"name":"BTC","szDecimals":5,"maxLeverage":40,"onlyIsolated":false},
			{"name":"ETH","szDecimals":4,"maxLeverage":25}
		],
		"marginTables": [[50, {"description":"test","marginTiers":[{"lowerBound":"0","maxLeverage":40}]}]],
		"collateralToken": 0
	}`)

	meta, err := c.Meta(context.Background())
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if len(meta.Universe) != 2 || meta.Universe[0].Name != "BTC" || meta.Universe[0].SzDecimals != 5 ||
		meta.Universe[0].MaxLeverage != 40 || meta.Universe[1].Name != "ETH" {
		t.Errorf("bad universe: %+v", meta.Universe)
	}
	if len(meta.MarginTables) != 1 {
		t.Errorf("margin tables not preserved: %+v", meta.MarginTables)
	}

	// Asset ids are the universe index — the property the connector relies on.
	if id, ok := meta.AssetID("ETH"); !ok || id != 1 {
		t.Errorf("AssetID(ETH) = %d, %v; want 1, true", id, ok)
	}
	if _, ok := meta.AssetID("SOL"); ok {
		t.Error("AssetID(SOL) = ok, want not found")
	}

	// The request must be the documented {"type":"meta"} body, with a UA.
	if len(*bodies) != 1 || (*bodies)[0]["type"] != "meta" {
		t.Errorf("bad request body: %+v", *bodies)
	}
	if ua := (*headers)[0].Get("User-Agent"); !strings.HasPrefix(ua, "go-exchange-connector/") {
		t.Errorf("User-Agent = %q", ua)
	}
}

// TestInfoClientMetaRaw pins the raw path a recorder uses: the body must come
// back byte-identical, because it is archived verbatim.
func TestInfoClientMetaRaw(t *testing.T) {
	// Deliberately unusual formatting/key order: a re-marshal would change it.
	verbatim := `{"collateralToken":0,  "universe":[{"maxLeverage":40,"name":"BTC","szDecimals":5}]}`
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, verbatim)

	raw, err := c.MetaRaw(context.Background())
	if err != nil {
		t.Fatalf("MetaRaw: %v", err)
	}
	if string(raw) != verbatim {
		t.Errorf("MetaRaw is not verbatim:\n got  %s\n want %s", raw, verbatim)
	}
	if (*bodies)[0]["type"] != "meta" {
		t.Errorf("bad request body: %+v", (*bodies)[0])
	}
}

func TestInfoClientPerpDexs(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusOK,
		`[null,{"name":"xyz","fullName":"XYZ DEX","deployer":"0xdeployer","oracleUpdater":null,"feeRecipient":"0xfee"}]`)

	dexs, err := c.PerpDexs(context.Background())
	if err != nil {
		t.Fatalf("PerpDexs: %v", err)
	}
	if len(dexs) != 2 {
		t.Fatalf("got %d dexs, want 2", len(dexs))
	}
	if dexs[0] != nil {
		t.Errorf("first dex = %+v, want nil (the default dex)", dexs[0])
	}
	if dexs[1].Name != "xyz" || dexs[1].FullName != "XYZ DEX" || dexs[1].OracleUpdater != nil ||
		dexs[1].FeeRecipient == nil || *dexs[1].FeeRecipient != "0xfee" {
		t.Errorf("bad dex: %+v", dexs[1])
	}
}

func TestInfoClientMetaAndAssetCtxs(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, `[
		{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":40},{"name":"ETH","szDecimals":4,"maxLeverage":25}]},
		[
			{"funding":"0.0000125","openInterest":"1234.5","oraclePx":"121000.0","markPx":"121001.0","midPx":"121000.75","dayNtlVlm":"1.0","prevDayPx":"120000.0","premium":"0.0001","impactPxs":["120990.0"]},
			{"funding":"-0.0000050","openInterest":"500.0","oraclePx":"4500.0","markPx":"4501.0","midPx":"4500.5","dayNtlVlm":"2.0","prevDayPx":"4400.0","premium":"0.0002","impactPxs":["4490.0"]}
		]
	]`)

	out, err := c.MetaAndAssetCtxs(context.Background())
	if err != nil {
		t.Fatalf("MetaAndAssetCtxs: %v", err)
	}
	if len(out.Ctxs) != 2 {
		t.Fatalf("got %d contexts, want 2", len(out.Ctxs))
	}
	if (*bodies)[0]["type"] != "metaAndAssetCtxs" {
		t.Errorf("bad request body: %+v", (*bodies)[0])
	}

	// Ctxs is index-aligned with Universe — the property CoinContext relies on.
	ctx, ok := out.CoinContext("ETH")
	if !ok || ctx.Funding != "-0.0000050" {
		t.Errorf("CoinContext(ETH) = %+v, %v", ctx, ok)
	}
	if rate, ok := out.FundingRate("BTC"); !ok || rate != "0.0000125" {
		t.Errorf("FundingRate(BTC) = %q, %v", rate, ok)
	}
	if _, ok := out.CoinContext("SOL"); ok {
		t.Error("CoinContext(SOL) = ok, want not found")
	}
}

func TestInfoClientMetaAndAssetCtxsBadTuple(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusOK, `[{"universe":[]}]`)
	if _, err := c.MetaAndAssetCtxs(context.Background()); err == nil {
		t.Error("MetaAndAssetCtxs on a 1-element tuple = nil error, want error")
	}
}

func TestInfoClientAllMids(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusOK, `{"mids":{"BTC":"121000.5","ETH":"4500.25"}}`)

	mids, err := c.AllMids(context.Background())
	if err != nil {
		t.Fatalf("AllMids: %v", err)
	}
	if len(mids) != 2 || mids["BTC"] != "121000.5" {
		t.Errorf("bad mids: %+v", mids)
	}
}

func TestInfoClientL2Snapshot(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK,
		`{"coin":"BTC","time":1759536000123,"levels":[[{"px":"121000.0","sz":"0.5","n":3}],[{"px":"121000.5","sz":"0.2","n":1}]]}`)

	book, err := c.L2Snapshot(context.Background(), " BTC ")
	if err != nil {
		t.Fatalf("L2Snapshot: %v", err)
	}
	if book.Coin != "BTC" || len(book.Bids()) != 1 || len(book.Asks()) != 1 {
		t.Errorf("bad book: %+v", book)
	}
	if book.Bids()[0] != (L2Level{Price: "121000.0", Size: "0.5", Count: 3}) {
		t.Errorf("bad bid: %+v", book.Bids()[0])
	}
	if (*bodies)[0]["coin"] != "BTC" {
		t.Errorf("coin not normalized in request: %+v", (*bodies)[0])
	}

	// A malformed coin must fail before the request goes out.
	before := len(*bodies)
	if _, err := c.L2Snapshot(context.Background(), `BT"C`); err == nil {
		t.Error("L2Snapshot with a malformed coin = nil error, want error")
	}
	if len(*bodies) != before {
		t.Error("L2Snapshot sent a request for a malformed coin")
	}
}

func TestInfoClientCandleSnapshot(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK,
		`[{"t":1759535940000,"T":1759535999999,"s":"BTC","i":"1m","o":"121000.0","c":"121010.0","h":"121020.0","l":"120990.0","v":"12.5","n":321}]`)

	candles, err := c.CandleSnapshot(context.Background(), "BTC", "1m", 1759535940000, 0)
	if err != nil {
		t.Fatalf("CandleSnapshot: %v", err)
	}
	if len(candles) != 1 || candles[0].Close != "121010.0" || candles[0].TradeCount != 321 {
		t.Errorf("bad candles: %+v", candles)
	}

	req, ok := (*bodies)[0]["req"].(map[string]any)
	if !ok {
		t.Fatalf("request has no req object: %+v", (*bodies)[0])
	}
	if req["coin"] != "BTC" || req["interval"] != "1m" || req["startTime"] != float64(1759535940000) {
		t.Errorf("bad candle request: %+v", req)
	}
	// endTime<=0 means "now", so it must be filled in rather than sent as 0.
	if end, _ := req["endTime"].(float64); end <= 0 {
		t.Errorf("endTime not defaulted: %+v", req)
	}

	if _, err := c.CandleSnapshot(context.Background(), "BTC", "2m", 1, 2); err == nil {
		t.Error("CandleSnapshot with an unsupported interval = nil error, want error")
	}
}

func TestInfoClientHTTPError(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusTooManyRequests, `{"error":"rate limited"}`)

	_, err := c.Meta(context.Background())
	if err == nil {
		t.Fatal("Meta on a 429 = nil error, want error")
	}
	httpErr, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("error type = %T, want *HTTPError", err)
	}
	if httpErr.StatusCode != http.StatusTooManyRequests || !strings.Contains(httpErr.Body, "rate limited") {
		t.Errorf("bad HTTPError: %+v", httpErr)
	}
}

// TestInfoClientNonJSONBody covers the proxy-error-page case: /info is one URL,
// so a misconfigured proxy answering 200 text/html would otherwise surface much
// later as an unmarshal error deep inside a decoder.
func TestInfoClientNonJSONBody(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusOK, `<html><body>gateway</body></html>`)

	_, err := c.Meta(context.Background())
	if err == nil {
		t.Fatal("Meta on a non-JSON body = nil error, want error")
	}
	httpErr, ok := err.(*HTTPError)
	if !ok {
		t.Fatalf("error type = %T, want *HTTPError", err)
	}
	if !strings.Contains(httpErr.Body, "not JSON") {
		t.Errorf("bad HTTPError body: %q", httpErr.Body)
	}
}

func TestInfoClientPostMarshalError(t *testing.T) {
	c := NewInfoClient("")
	// A channel cannot be marshalled to JSON: the error must surface before any
	// request is attempted.
	if _, err := c.Post(context.Background(), make(chan int)); err == nil {
		t.Error("Post with an unmarshalable body = nil error, want error")
	}
}
