package futures

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// testSecret is the secret from the Binance signing documentation, reused so
// the signature vector is reproduced verbatim.
const testSecret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"

// testClient builds a Client pointing at srv with a fixed key/secret.
func testClient(t *testing.T, srvURL string) *Client {
	t.Helper()
	c, err := New(Config{BaseURL: srvURL, APIKey: "TESTKEY", APISecret: testSecret})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// TestSignVector pins signQuery to the documented Binance vector.
func TestSignVector(t *testing.T) {
	const payload = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
	const want = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
	if got := signQuery(testSecret, payload); got != want {
		t.Fatalf("signature mismatch:\n got %s\nwant %s", got, want)
	}
}

// TestSignedQueryShape checks that a signed request carries recvWindow and
// timestamp, ends with a correct signature, and sends the API key header.
func TestSignedQueryShape(t *testing.T) {
	var captured *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Clone(context.Background())
		_, _ = w.Write([]byte(`{"orderId":1,"symbol":"BTCUSDT"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	if _, err := c.NewOrder(context.Background(), OrderReq{
		Symbol: "BTCUSDT", Side: "BUY", Type: "LIMIT", TimeInForce: "GTC",
		Quantity: "0.001", Price: "60000.0",
	}); err != nil {
		t.Fatalf("NewOrder: %v", err)
	}
	if captured == nil {
		t.Fatal("server never received the request")
	}
	if got := captured.Header.Get("X-MBX-APIKEY"); got != "TESTKEY" {
		t.Fatalf("api key header = %q, want TESTKEY", got)
	}
	q := captured.URL.RawQuery
	if !strings.Contains(q, "recvWindow=5000") {
		t.Errorf("query missing recvWindow=5000: %q", q)
	}
	if !strings.Contains(q, "timestamp=") {
		t.Errorf("query missing timestamp: %q", q)
	}
	idx := strings.Index(q, "&signature=")
	if idx < 0 {
		t.Fatalf("query has no &signature=: %q", q)
	}
	pre, sig := q[:idx], q[idx+len("&signature="):]
	if strings.Contains(sig, "&") {
		t.Errorf("signature is not the last parameter: %q", q)
	}
	if !strings.HasSuffix(q, "&signature="+sig) {
		t.Errorf("query does not end with signature: %q", q)
	}
	if got := signQuery(testSecret, pre); got != sig {
		t.Errorf("signature does not match signed prefix:\n got %s\nwant %s", sig, got)
	}
	// The signed prefix must itself end with the timestamp it signs.
	if !strings.Contains(pre, "timestamp=") {
		t.Errorf("signed prefix missing timestamp: %q", pre)
	}
}

// TestTimeOffsetApplied verifies SyncTime's offset shifts the signed timestamp
// toward the (deliberately offset) server clock.
func TestTimeOffsetApplied(t *testing.T) {
	const serverTime = int64(1_700_000_000_000) // fixed, ~2023
	var capturedTS atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/fapi/v1/time", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"serverTime":1700000000000}`))
	})
	mux.HandleFunc("/fapi/v1/order", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("timestamp"); v != "" {
			if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
				capturedTS.Store(ts)
			}
		}
		_, _ = w.Write([]byte(`{"orderId":1}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := testClient(t, srv.URL)
	if err := c.SyncTime(context.Background()); err != nil {
		t.Fatalf("SyncTime: %v", err)
	}
	if _, err := c.NewOrder(context.Background(), OrderReq{Symbol: "BTCUSDT", Side: "BUY", Type: "MARKET", Quantity: "0.001"}); err != nil {
		t.Fatalf("NewOrder: %v", err)
	}
	ts := capturedTS.Load()
	if diff := math.Abs(float64(ts - serverTime)); diff > 5000 {
		t.Fatalf("timestamp %d not within 5s of serverTime %d (offset not applied)", ts, serverTime)
	}
}

// TestUsageHeaders verifies the three rate-limit headers are captured.
func TestUsageHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-MBX-USED-WEIGHT-1M", "1234")
		w.Header().Set("X-MBX-ORDER-COUNT-10S", "7")
		w.Header().Set("X-MBX-ORDER-COUNT-1M", "42")
		_, _ = w.Write([]byte(`{"timezone":"UTC","symbols":[]}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	if _, err := c.ExchangeInfo(context.Background()); err != nil {
		t.Fatalf("ExchangeInfo: %v", err)
	}
	u := c.Usage()
	if u.UsedWeight1M != 1234 || u.OrderCount10s != 7 || u.OrderCount1M != 42 {
		t.Fatalf("Usage() = %+v", u)
	}
}

// TestListenKeyUnsigned checks the listenKey endpoints carry the API key header
// but no query string, signature or timestamp.
func TestListenKeyUnsigned(t *testing.T) {
	var hits int
	seenKey := make([]string, 0, 3)
	seenQuery := make([]string, 0, 3)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		seenKey = append(seenKey, r.Header.Get("X-MBX-APIKEY"))
		seenQuery = append(seenQuery, r.URL.RawQuery)
		if r.URL.RawQuery != "" {
			t.Errorf("%s %s: unexpected query %q", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		switch r.Method {
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"listenKey":"LISTEN-KEY-1"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	key, err := c.NewListenKey(context.Background())
	if err != nil {
		t.Fatalf("NewListenKey: %v", err)
	}
	if key != "LISTEN-KEY-1" {
		t.Fatalf("listen key = %q, want LISTEN-KEY-1", key)
	}
	if err := c.KeepAliveListenKey(context.Background()); err != nil {
		t.Fatalf("KeepAliveListenKey: %v", err)
	}
	if err := c.CloseListenKey(context.Background()); err != nil {
		t.Fatalf("CloseListenKey: %v", err)
	}
	if hits != 3 {
		t.Fatalf("expected 3 listenKey calls, got %d", hits)
	}
	for i, k := range seenKey {
		if k != "TESTKEY" {
			t.Errorf("call %d: api key header = %q, want TESTKEY", i, k)
		}
	}
	for i, q := range seenQuery {
		if q != "" {
			t.Errorf("call %d: query = %q, want empty", i, q)
		}
	}
}

const exchangeInfoFixture = `{
  "timezone": "UTC",
  "serverTime": 1700000000000,
  "rateLimits": [
    {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400},
    {"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1200}
  ],
  "symbols": [
    {
      "symbol": "BTCUSDT",
      "status": "TRADING",
      "contractType": "PERPETUAL",
      "pricePrecision": 1,
      "quantityPrecision": 3,
      "filters": [
        {"filterType":"PRICE_FILTER","minPrice":"556.80","maxPrice":"4529764","tickSize":"0.10"},
        {"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},
        {"filterType":"MIN_NOTIONAL","notional":"100"}
      ]
    },
    {
      "symbol": "ETHUSDT",
      "status": "TRADING",
      "contractType": "PERPETUAL",
      "pricePrecision": 2,
      "quantityPrecision": 3,
      "filters": [
        {"filterType":"PRICE_FILTER","tickSize":"0.01"},
        {"filterType":"LOT_SIZE","stepSize":"0.001","minQty":"0.001"}
      ]
    }
  ]
}`

func TestExchangeInfoDecodes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(exchangeInfoFixture))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	info, err := c.ExchangeInfo(context.Background())
	if err != nil {
		t.Fatalf("ExchangeInfo: %v", err)
	}
	if len(info.Symbols) != 2 {
		t.Fatalf("symbols = %d, want 2", len(info.Symbols))
	}
	if len(info.RateLimits) != 2 || info.RateLimits[0].Limit != 2400 {
		t.Fatalf("rateLimits not decoded: %+v", info.RateLimits)
	}
	btc := info.Symbols[0]
	if btc.Symbol != "BTCUSDT" || btc.Status != "TRADING" || btc.ContractType != "PERPETUAL" {
		t.Fatalf("BTCUSDT header wrong: %+v", btc)
	}
	if btc.PricePrecision != 1 || btc.QuantityPrecision != 3 {
		t.Fatalf("BTCUSDT precision wrong: %+v", btc)
	}
	byType := map[string]Filter{}
	for _, f := range btc.Filters {
		byType[f.FilterType] = f
	}
	if got := byType["PRICE_FILTER"].TickSize; got != "0.10" {
		t.Errorf("tickSize = %q, want 0.10", got)
	}
	if got := byType["LOT_SIZE"].StepSize; got != "0.001" {
		t.Errorf("stepSize = %q, want 0.001", got)
	}
	if got := byType["LOT_SIZE"].MinQty; got != "0.001" {
		t.Errorf("minQty = %q, want 0.001", got)
	}
	if got := byType["MIN_NOTIONAL"].Notional; got != "100" {
		t.Errorf("notional = %q, want 100", got)
	}
}

func TestPositionRiskDecodes(t *testing.T) {
	const fixture = `[
	  {"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"0.010","entryPrice":"60000.50","markPrice":"60123.40","unRealizedProfit":"1.2290","leverage":"20"}
	]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fixture))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL)
	pr, err := c.PositionRisk(context.Background())
	if err != nil {
		t.Fatalf("PositionRisk: %v", err)
	}
	if len(pr) != 1 {
		t.Fatalf("len = %d, want 1", len(pr))
	}
	p := pr[0]
	if p.Symbol != "BTCUSDT" || p.PositionSide != "BOTH" {
		t.Fatalf("symbol/side wrong: %+v", p)
	}
	if p.PositionAmt != "0.010" || p.EntryPrice != "60000.50" || p.MarkPrice != "60123.40" {
		t.Fatalf("decimals not strings: %+v", p)
	}
	if p.UnRealizedProfit != "1.2290" || p.Leverage != "20" {
		t.Fatalf("profit/leverage wrong: %+v", p)
	}
}

// TestNewRejectsUnknownNetwork guards the config validation path.
func TestNewRejectsUnknownNetwork(t *testing.T) {
	if _, err := New(Config{Network: "devnet"}); err == nil {
		t.Fatal("expected error for unknown network")
	}
	if c, err := New(Config{}); err != nil || c.BaseURL() != mainnetBaseURL {
		t.Fatalf("default network: base=%q err=%v", c.BaseURL(), err)
	}
	if c, err := New(Config{Network: Testnet}); err != nil || c.BaseURL() != testnetBaseURL {
		t.Fatalf("testnet: base=%q err=%v", c.BaseURL(), err)
	}
}
