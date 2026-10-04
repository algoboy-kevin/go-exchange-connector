package hyperliquid

import (
	"context"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// newTestHyperliquid builds a stream manager over a real connector base and
// captures every dispatched event. Nothing connects: the frame handlers are
// exercised directly, the same way the Binance tests do it.
func newTestHyperliquid(t *testing.T) (*WSHyperliquid, *[]any) {
	t.Helper()
	base := connector.New(false, nil)
	var got []any
	base.SetDispatcher(func(ev any) {
		got = append(got, ev)
	})
	return New(base), &got
}

// feed hands one raw venue frame to the parser with a fixed receive time.
func feed(h *WSHyperliquid, rx time.Time, raw string) {
	h.processFrame(frame{data: []byte(raw), rx: rx})
}

func TestSubscribeBeforeStartIsRegistered(t *testing.T) {
	h, _ := newTestHyperliquid(t)
	ctx := context.Background()

	if err := h.SubscribeTrades(ctx, []string{"BTC", "ETH"}); err != nil {
		t.Fatalf("SubscribeTrades: %v", err)
	}
	// Subscribing the same coin again is a no-op, not a duplicate.
	if err := h.SubscribeTrades(ctx, []string{"BTC"}); err != nil {
		t.Fatalf("SubscribeTrades (repeat): %v", err)
	}
	if got := len(h.Subscriptions()); got != 2 {
		t.Fatalf("got %d subscriptions, want 2: %+v", got, h.Subscriptions())
	}
	if h.Status() == ws.StatusConnected {
		t.Errorf("status = %v, want not connected before Start", h.Status())
	}

	// Invalid requests must not register anything.
	if err := h.SubscribeTrades(ctx, []string{"BTC:bad coin"}); err == nil {
		t.Error("SubscribeTrades with a malformed coin = nil error, want error")
	}
	if err := h.SubscribeCandles(ctx, []string{"BTC"}, "2m"); err == nil {
		t.Error("SubscribeCandles with an unsupported interval = nil error, want error")
	}

	// Unsubscribing replays the stored subscription and empties the registry.
	if err := h.UnsubscribeTrades(ctx, []string{"BTC", "ETH"}); err != nil {
		t.Fatalf("UnsubscribeTrades: %v", err)
	}
	if got := len(h.Subscriptions()); got != 0 {
		t.Fatalf("got %d subscriptions after unsubscribe, want 0", got)
	}
}

func TestParseL2Book(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{Fast: true}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}

	rx := time.Now()
	feed(h, rx, `{"channel":"l2Book","data":{"coin":"BTC","time":1759536000123,"levels":[[{"px":"121000.0","sz":"0.5","n":3},{"px":"120999.5","sz":"1.25","n":1}],[{"px":"121000.5","sz":"0.2","n":2}]]}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.HyperliquidBookEvent)
	if !ok {
		t.Fatalf("expected *HyperliquidBookEvent, got %T", (*got)[0])
	}
	if ev.Coin != "BTC" || ev.SeqID <= 0 {
		t.Errorf("bad event header: %+v", ev)
	}
	if !ev.Timestamp.Equal(time.UnixMilli(1759536000123)) {
		t.Errorf("Timestamp = %v, want the payload ms epoch", ev.Timestamp)
	}
	if !ev.ReceivedAt.Equal(rx) {
		t.Errorf("ReceivedAt = %v, want the read-loop time %v", ev.ReceivedAt, rx)
	}
	wantBids := []connector.HyperliquidLevel{
		{Price: "121000.0", Size: "0.5", Count: 3},
		{Price: "120999.5", Size: "1.25", Count: 1},
	}
	if len(ev.Bids) != len(wantBids) {
		t.Fatalf("got %d bids, want %d", len(ev.Bids), len(wantBids))
	}
	for i, want := range wantBids {
		if ev.Bids[i] != want {
			t.Errorf("bid[%d] = %+v, want %+v", i, ev.Bids[i], want)
		}
	}
	if len(ev.Asks) != 1 || ev.Asks[0].Price != "121000.5" || ev.Asks[0].Count != 2 {
		t.Errorf("bad asks: %+v", ev.Asks)
	}
	// The exact decimal text must survive the round trip.
	if ev.Bids[0].Price != "121000.0" {
		t.Errorf("price lost its decimal text: %q", ev.Bids[0].Price)
	}
}

// TestParseL2BookNumericPrices covers the docs/wire disagreement: some fields
// are documented as JSON numbers. A frame that fails to decode is a frame the
// caller never sees, so both encodings must be accepted.
func TestParseL2BookNumericPrices(t *testing.T) {
	h, got := newTestHyperliquid(t)
	_ = h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{})

	feed(h, time.Now(), `{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":121000.5,"sz":0.5,"n":3}],[{"px":121001,"sz":0.25,"n":1}]]}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.HyperliquidBookEvent)
	if ev.Bids[0].Price != "121000.5" || ev.Bids[0].Size != "0.5" {
		t.Errorf("bad bid: %+v", ev.Bids[0])
	}
	if ev.Asks[0].Price != "121001" {
		t.Errorf("bad ask price: %q", ev.Asks[0].Price)
	}
}

func TestParseTradesBatch(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeTrades(context.Background(), []string{"BTC"}); err != nil {
		t.Fatalf("SubscribeTrades: %v", err)
	}

	rx := time.Now()
	feed(h, rx, `{"channel":"trades","data":[`+
		`{"coin":"BTC","side":"B","px":"121000.5","sz":"0.01","hash":"0xabc","time":1759536000456,"tid":123456789,"users":["0xbuyer","0xseller"]},`+
		`{"coin":"BTC","side":"A","px":"121000.0","sz":"0.02","hash":"0xdef","time":1759536000457,"tid":123456790,"users":["0xb","0xs"]}]}`)

	if len(*got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(*got))
	}
	first := (*got)[0].(*connector.HyperliquidTradeEvent)
	if first.Coin != "BTC" || first.Side != "B" || first.Price != "121000.5" ||
		first.Size != "0.01" || first.TradeID != 123456789 || first.Hash != "0xabc" {
		t.Errorf("bad trade: %+v", first)
	}
	if len(first.Users) != 2 || first.Users[0] != "0xbuyer" {
		t.Errorf("bad users: %+v", first.Users)
	}
	if !first.Timestamp.Equal(time.UnixMilli(1759536000456)) || !first.ReceivedAt.Equal(rx) {
		t.Errorf("bad trade times: ts=%v rx=%v", first.Timestamp, first.ReceivedAt)
	}
	second := (*got)[1].(*connector.HyperliquidTradeEvent)
	if second.SeqID <= first.SeqID {
		t.Errorf("SeqID not monotonic: %d then %d", first.SeqID, second.SeqID)
	}
	if sb, ok := connector.HyperliquidNormalizeSide(second.Side); !ok || sb != "sell" {
		t.Errorf("HyperliquidNormalizeSide(%q) = %q, %v", second.Side, sb, ok)
	}
}

func TestParseBBOWithNullSide(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeBBO(context.Background(), []string{"BTC"}); err != nil {
		t.Fatalf("SubscribeBBO: %v", err)
	}

	feed(h, time.Now(), `{"channel":"bbo","data":{"coin":"BTC","time":1759536000789,"bbo":[{"px":"121000.0","sz":"0.5","n":3},null]}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.HyperliquidBBOEvent)
	if ev.Bid == nil || ev.Bid.Price != "121000.0" || ev.Bid.Count != 3 {
		t.Errorf("bad bid: %+v", ev.Bid)
	}
	if ev.Ask != nil {
		t.Errorf("ask = %+v, want nil for a null side", ev.Ask)
	}

	// Both sides present.
	feed(h, time.Now(), `{"channel":"bbo","data":{"coin":"BTC","time":1759536000790,"bbo":[{"px":"121000.0","sz":"0.5","n":3},{"px":"121000.5","sz":"0.1","n":1}]}}`)
	ev = (*got)[1].(*connector.HyperliquidBBOEvent)
	if ev.Bid == nil || ev.Ask == nil || ev.Ask.Price != "121000.5" {
		t.Errorf("bad two-sided bbo: bid=%+v ask=%+v", ev.Bid, ev.Ask)
	}
}

func TestParseActiveAssetCtxPerp(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeActiveAssetCtx(context.Background(), []string{"BTC"}); err != nil {
		t.Fatalf("SubscribeActiveAssetCtx: %v", err)
	}

	rx := time.Now()
	feed(h, rx, `{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0000125","openInterest":"1234.5","prevDayPx":"120000.0","dayNtlVlm":"987654321.0","premium":"0.0001","oraclePx":"121000.0","markPx":"121001.0","midPx":"121000.75","impactPxs":["120990.0","121010.0"]}}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.HyperliquidAssetCtxEvent)
	if ev.Coin != "BTC" || ev.IsSpot {
		t.Errorf("bad header: %+v", ev)
	}
	if ev.Funding != "0.0000125" || ev.OpenInterest != "1234.5" || ev.OraclePx != "121000.0" ||
		ev.MarkPx != "121001.0" || ev.MidPx != "121000.75" || ev.Premium != "0.0001" {
		t.Errorf("bad ctx: %+v", ev)
	}
	if len(ev.ImpactPxs) != 2 || ev.ImpactPxs[1] != "121010.0" {
		t.Errorf("bad impact prices: %+v", ev.ImpactPxs)
	}
	// This channel carries no venue timestamp: Timestamp falls back to rx.
	if !ev.Timestamp.Equal(rx) {
		t.Errorf("Timestamp = %v, want the receive time %v", ev.Timestamp, rx)
	}
}

func TestParseActiveAssetCtxSpotVariant(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeActiveAssetCtx(context.Background(), []string{"PURR"}); err != nil {
		t.Fatalf("SubscribeActiveAssetCtx: %v", err)
	}

	feed(h, time.Now(), `{"channel":"activeAssetCtx","data":{"coin":"PURR","ctx":{"dayNtlVlm":"1000.0","prevDayPx":"0.5","markPx":"0.51","midPx":"0.505","circulatingSupply":"1000000"}}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.HyperliquidAssetCtxEvent)
	if !ev.IsSpot {
		t.Errorf("IsSpot = false, want true for a context with no funding/oraclePx: %+v", ev)
	}
	if ev.MarkPx != "0.51" || ev.Funding != "" {
		t.Errorf("bad spot ctx: %+v", ev)
	}
}

func TestParseAllMids(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeAllMids(context.Background()); err != nil {
		t.Fatalf("SubscribeAllMids: %v", err)
	}

	feed(h, time.Now(), `{"channel":"allMids","data":{"mids":{"BTC":"121000.5","ETH":"4500.25"}}}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.HyperliquidAllMidsEvent)
	if len(ev.Mids) != 2 || ev.Mids["BTC"] != "121000.5" || ev.Mids["ETH"] != "4500.25" {
		t.Errorf("bad mids: %+v", ev.Mids)
	}
}

// TestParseCandle accepts both shapes the venue might use: the docs type the
// candle payload as an array, while at least one SDK decodes a single object.
func TestParseCandle(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeCandles(context.Background(), []string{"BTC"}, "1m"); err != nil {
		t.Fatalf("SubscribeCandles: %v", err)
	}

	feed(h, time.Now(), `{"channel":"candle","data":[{"t":1759535940000,"T":1759535999999,"s":"BTC","i":"1m","o":"121000.0","c":"121010.0","h":"121020.0","l":"120990.0","v":"12.5","n":321}]}`)
	feed(h, time.Now(), `{"channel":"candle","data":{"t":1759536000000,"T":1759536059999,"s":"BTC","i":"1m","o":"121010.0","c":"121005.0","h":"121015.0","l":"121000.0","v":"3.25","n":40}}`)

	if len(*got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(*got))
	}
	first := (*got)[0].(*connector.HyperliquidCandleEvent)
	if first.Coin != "BTC" || first.Interval != "1m" || first.Open != "121000.0" ||
		first.High != "121020.0" || first.Low != "120990.0" || first.Close != "121010.0" ||
		first.Volume != "12.5" || first.TradeCount != 321 {
		t.Errorf("bad candle: %+v", first)
	}
	if !first.OpenTime.Equal(time.UnixMilli(1759535940000)) || !first.CloseTime.Equal(time.UnixMilli(1759535999999)) {
		t.Errorf("bad candle times: open=%v close=%v", first.OpenTime, first.CloseTime)
	}

	second := (*got)[1].(*connector.HyperliquidCandleEvent)
	if !second.OpenTime.Equal(time.UnixMilli(1759536000000)) || second.TradeCount != 40 {
		t.Errorf("bad object-shaped candle: %+v", second)
	}
}

func TestUnsubscribedCoinIgnored(t *testing.T) {
	h, got := newTestHyperliquid(t)
	// Subscribe to ETH, then feed a BTC frame (and an unsubscribed channel).
	if err := h.SubscribeL2Book(context.Background(), []string{"ETH"}, SubParams{}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}
	feed(h, time.Now(), `{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}`)
	feed(h, time.Now(), `{"channel":"trades","data":[{"coin":"ETH","side":"B","px":"1","sz":"1","time":1,"tid":1}]}`)
	feed(h, time.Now(), `{"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[{"px":"1","sz":"1","n":1},null]}}`)

	if len(*got) != 0 {
		t.Fatalf("expected no events, got %d: %+v", len(*got), *got)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	h, got := newTestHyperliquid(t)
	ctx := context.Background()
	if err := h.SubscribeTrades(ctx, []string{"BTC"}); err != nil {
		t.Fatalf("SubscribeTrades: %v", err)
	}
	raw := `{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1}]}`
	feed(h, time.Now(), raw)
	if len(*got) != 1 {
		t.Fatalf("expected 1 event before unsubscribe, got %d", len(*got))
	}
	if err := h.UnsubscribeTrades(ctx, []string{"BTC"}); err != nil {
		t.Fatalf("UnsubscribeTrades: %v", err)
	}
	// A frame already in flight must not resurrect the stream.
	feed(h, time.Now(), raw)
	if len(*got) != 1 {
		t.Fatalf("got %d events after unsubscribe, want 1 (in-flight frame dropped)", len(*got))
	}
}

func TestErrorFrameDispatchesEvent(t *testing.T) {
	h, got := newTestHyperliquid(t)

	feed(h, time.Now(), `{"channel":"error","data":"Invalid subscription: unknown type"}`)

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.HyperliquidErrorEvent)
	if !ok {
		t.Fatalf("expected *HyperliquidErrorEvent, got %T", (*got)[0])
	}
	if ev.Message != "Invalid subscription: unknown type" || ev.SeqID <= 0 || ev.ReceivedAt.IsZero() {
		t.Errorf("bad error event: %+v", ev)
	}
}

func TestAckAndPongProduceNoEvent(t *testing.T) {
	h, got := newTestHyperliquid(t)
	feed(h, time.Now(), `{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}`)
	feed(h, time.Now(), `{"channel":"pong"}`)
	feed(h, time.Now(), `{"channel":"somethingNew","data":{}}`)

	if len(*got) != 0 {
		t.Fatalf("expected no events, got %d: %+v", len(*got), *got)
	}
}

func TestUndecodableFrameDoesNotPanic(t *testing.T) {
	h, got := newTestHyperliquid(t)
	feed(h, time.Now(), `not json at all`)
	feed(h, time.Now(), ``)
	// A frame whose payload is the wrong shape logs and drops, never panics.
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}
	feed(h, time.Now(), `{"channel":"l2Book","data":"not a book"}`)
	feed(h, time.Now(), `{"channel":"trades","data":{"not":"an array"}}`)
	feed(h, time.Now(), `{"channel":"candle","data":123}`)

	if len(*got) != 0 {
		t.Fatalf("expected no events, got %d: %+v", len(*got), *got)
	}
}

func TestNumDecoding(t *testing.T) {
	tests := []struct {
		in   string
		want string
		bad  bool
	}{
		{`"0.0000125"`, "0.0000125", false},
		{`121000.5`, "121000.5", false},
		{`-3`, "-3", false},
		{`null`, "", false},
		{`"97500.5"`, "97500.5", false},
		{`"abc"`, "abc", false}, // a string is passed through; only bare numbers are validated
		{`{}`, "", true},
	}
	for _, tc := range tests {
		var n Num
		err := n.UnmarshalJSON([]byte(tc.in))
		if tc.bad {
			if err == nil {
				t.Errorf("UnmarshalJSON(%s) = nil error, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("UnmarshalJSON(%s) = %v", tc.in, err)
			continue
		}
		if n.String() != tc.want {
			t.Errorf("UnmarshalJSON(%s) = %q, want %q", tc.in, n, tc.want)
		}
	}

	var n Num
	if err := n.UnmarshalJSON([]byte(`"1.5"`)); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if f, err := n.Float64(); err != nil || f != 1.5 {
		t.Errorf("Float64(%q) = %v, %v", n, f, err)
	}
	var empty Num
	if f, err := empty.Float64(); err != nil || f != 0 {
		t.Errorf("Float64(empty) = %v, %v", f, err)
	}
	if !empty.IsZero() {
		t.Error("IsZero(empty) = false")
	}
	// Marshal must round-trip the venue's decimal text as a JSON string.
	b, err := n.MarshalJSON()
	if err != nil || string(b) != `"1.5"` {
		t.Errorf("MarshalJSON = %s, %v", b, err)
	}
}

func TestMsTimeFallsBack(t *testing.T) {
	fallback := time.Now()
	if got := msTime(0, fallback); !got.Equal(fallback) {
		t.Errorf("msTime(0) = %v, want fallback", got)
	}
	if got := msTime(1759536000123, fallback); !got.Equal(time.UnixMilli(1759536000123)) {
		t.Errorf("msTime = %v", got)
	}
}

func TestNewWithOptionsDefaults(t *testing.T) {
	base := connector.New(false, nil)
	h := NewWithOptions(base, Options{URL: "", WSPingIntervalMs: -1, AppPingIntervalMs: -1})

	if h.opts.URL != DefaultWSURL {
		t.Errorf("URL = %q, want %q", h.opts.URL, DefaultWSURL)
	}
	if h.opts.ReconnectIntervalMs != defaultReconnectIntervalMs ||
		h.opts.ReadLimitBytes != defaultReadLimitBytes {
		t.Errorf("defaults not applied: %+v", h.opts)
	}
	// Negative ping intervals mean "disabled" and must survive the defaults.
	if h.opts.WSPingIntervalMs != -1 || h.opts.AppPingIntervalMs != -1 {
		t.Errorf("negative ping intervals overwritten: %+v", h.opts)
	}
	if h.opts.DataStaleTimeoutMs != 0 {
		t.Errorf("DataStaleTimeoutMs = %d, want 0 (watchdog off by default)", h.opts.DataStaleTimeoutMs)
	}
}
