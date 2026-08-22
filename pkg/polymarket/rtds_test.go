package polymarket

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func TestNormalizeRTDSSymbol(t *testing.T) {
	cases := map[string]string{
		"BTCUSDT":   "btcusdt",
		"btcusdt":   "btcusdt",
		" EthUSDT ": "ethusdt",
		"ETH/USD":   "eth/usd",
		"":          "",
	}
	for in, want := range cases {
		if got := normalizeRTDSSymbol(in); got != want {
			t.Errorf("normalizeRTDSSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRTDSSubscriptionJSONShape(t *testing.T) {
	// Binance: broadcast, no filters.
	req := rtdsSubscriptionRequest{Action: rtdsActionSubscribe, Subscriptions: binanceEntries()}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"action":"subscribe","subscriptions":[{"topic":"crypto_prices","type":"update"}]}`
	if string(data) != want {
		t.Errorf("binance JSON mismatch:\n got  %s\n want %s", data, want)
	}

	// Chainlink: one entry per feed, JSON-string filter.
	req = rtdsSubscriptionRequest{Action: rtdsActionSubscribe, Subscriptions: chainlinkEntries([]string{"eth/usd", "btc/usd"})}
	data, _ = json.Marshal(req)
	want = `{"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"eth/usd\"}"},{"topic":"crypto_prices_chainlink","type":"*","filters":"{\"symbol\":\"btc/usd\"}"}]}`
	if string(data) != want {
		t.Errorf("chainlink JSON mismatch:\n got  %s\n want %s", data, want)
	}

	// Equity: one entry per symbol, JSON-string filter.
	req = rtdsSubscriptionRequest{Action: rtdsActionSubscribe, Subscriptions: equityEntries([]string{"AAPL"})}
	data, _ = json.Marshal(req)
	want = `{"action":"subscribe","subscriptions":[{"topic":"equity_prices","type":"*","filters":"{\"symbol\":\"aapl\"}"}]}`
	if string(data) != want {
		t.Errorf("equity JSON mismatch:\n got  %s\n want %s", data, want)
	}

	// Chainlink TWAP: one broadcast entry per window; the wire topic encodes
	// the window (no filters/window/symbols fields on the wire).
	req = rtdsSubscriptionRequest{Action: rtdsActionSubscribe, Subscriptions: chainlinkTWAPEntries(60)}
	data, _ = json.Marshal(req)
	want = `{"action":"subscribe","subscriptions":[{"topic":"crypto_prices_twap_sixty","type":"update"}]}`
	if string(data) != want {
		t.Errorf("chainlink twap JSON mismatch:\n got  %s\n want %s", data, want)
	}
}

// newTestRTDSClient builds an RTDS client over a real connector base and
// captures every dispatched event.
func newTestRTDSClient(t *testing.T) (*WSPolymarketRTDS, *[]any) {
	t.Helper()
	base := connector.New(false, nil)
	var got []any
	base.SetDispatcher(func(ev any) {
		got = append(got, ev)
	})
	return NewWSPolymarketRTDS(base), &got
}

func TestRTDSProcessBinancePrice(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeCryptoPrices(context.Background(), []string{"BTCUSDT"})

	raw := `{"topic":"crypto_prices","type":"update","timestamp":1787312179781,"payload":{"symbol":"BTCUSDT","timestamp":1787312179781,"value":76578.37804472372,"full_accuracy_value":"76578.37804472372"}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.CryptoPriceEvent)
	if !ok {
		t.Fatalf("expected *CryptoPriceEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Source != "binance" || ev.Price != "76578.37804472372" {
		t.Errorf("got symbol=%q source=%q price=%q", ev.Symbol, ev.Source, ev.Price)
	}
	if !ev.Timestamp.Equal(time.UnixMilli(1787312179781)) || ev.SeqID <= 0 || ev.ReceivedAt.IsZero() {
		t.Errorf("bad time/seq fields: ts=%v seq=%d", ev.Timestamp, ev.SeqID)
	}
}

func TestRTDSProcessChainlinkPrice(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeChainlinkPrices(context.Background(), []string{"eth/usd"})

	// Chainlink full_accuracy_value is a raw integer scaled by 10^18 — the
	// numeric "value" is the actual price.
	raw := `{"topic":"crypto_prices_chainlink","type":"update","timestamp":1787378918006,"payload":{"symbol":"eth/usd","timestamp":1787378917000,"value":2434.24334265,"full_accuracy_value":"2434243342650000000000"}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.CryptoPriceEvent)
	if !ok {
		t.Fatalf("expected *CryptoPriceEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "eth/usd" || ev.Source != "chainlink" {
		t.Errorf("got symbol=%q source=%q", ev.Symbol, ev.Source)
	}
	if ev.Price != "2434.24334265" {
		t.Errorf("Price = %q, want 2434.24334265 (numeric value, not scaled full_accuracy)", ev.Price)
	}
}

func TestRTDSProcessChainlinkSnapshot(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeChainlinkPrices(context.Background(), []string{"eth/usd"})

	// Chainlink snapshot arrives on the crypto_prices topic with type subscribe.
	raw := `{"topic":"crypto_prices","type":"subscribe","timestamp":1787378917136,"payload":{"symbol":"eth/usd","data":[{"timestamp":1787378858000,"value":2431.19},{"timestamp":1787378860000,"value":2431.21}]}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.PriceSnapshotEvent)
	if !ok {
		t.Fatalf("expected *PriceSnapshotEvent, got %T", (*got)[0])
	}
	if ev.Source != "chainlink" || ev.Symbol != "eth/usd" || len(ev.Points) != 2 {
		t.Errorf("got source=%q symbol=%q points=%d", ev.Source, ev.Symbol, len(ev.Points))
	}
	if ev.Points[0].Timestamp != 1787378858000 || ev.Points[0].Value != 2431.19 {
		t.Errorf("bad first point: %+v", ev.Points[0])
	}
}

func TestRTDSProcessChainlinkTWAP(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeChainlinkTWAP(context.Background(), 60, []string{"btc/usd"})

	// Incoming events carry the per-window wire topic and window_s in the
	// payload.
	raw := `{"topic":"crypto_prices_twap_sixty","type":"update","timestamp":1787378918006,"payload":{"symbol":"btc/usd","timestamp":1787378917000,"value":"72913.515000000000000000","window_s":60}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.CryptoPriceEvent)
	if !ok {
		t.Fatalf("expected *CryptoPriceEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "btc/usd" || ev.Source != "chainlink_twap" || ev.WindowSeconds != 60 {
		t.Errorf("got symbol=%q source=%q window=%d", ev.Symbol, ev.Source, ev.WindowSeconds)
	}
	// value is an exact decimal string, not converted to a float.
	if ev.Price != "72913.515000000000000000" {
		t.Errorf("Price = %q, want exact decimal string", ev.Price)
	}
	if !ev.Timestamp.Equal(time.UnixMilli(1787378917000)) {
		t.Errorf("Timestamp = %v, want payload observation time", ev.Timestamp)
	}
}

func TestRTDSProcessChainlinkTWAP30(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeChainlinkTWAP(context.Background(), 30, []string{"eth/usd"})

	raw := `{"topic":"crypto_prices_twap_thirty","type":"update","timestamp":1,"payload":{"symbol":"eth/usd","value":"3000.5","window_s":30}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.CryptoPriceEvent)
	if ev.WindowSeconds != 30 || ev.Source != "chainlink_twap" {
		t.Errorf("got window=%d source=%q", ev.WindowSeconds, ev.Source)
	}
}

func TestRTDSChainlinkTWAPWindowIsolation(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeChainlinkTWAP(context.Background(), 60, []string{"btc/usd"})

	// A 30s-window update (different wire topic) is not subscribed → no event.
	raw := `{"topic":"crypto_prices_twap_thirty","type":"update","timestamp":1,"payload":{"symbol":"btc/usd","value":"1.5","window_s":30}}`
	r.processMessage([]byte(raw))

	if len(*got) != 0 {
		t.Fatalf("expected 0 events (window isolation), got %d", len(*got))
	}
}

func TestRTDSChainlinkTWAPEntries(t *testing.T) {
	// 60s window → crypto_prices_twap_sixty, type update (broadcast, no
	// window/symbols fields on the wire).
	entries := chainlinkTWAPEntries(60)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Topic != rtdsChainlinkTWAP60Topic || e.MsgType != "update" || e.Filters != nil {
		t.Errorf("bad 60s entry: %+v", e)
	}
	// 30s window → crypto_prices_twap_thirty.
	entries = chainlinkTWAPEntries(30)
	if len(entries) != 1 || entries[0].Topic != rtdsChainlinkTWAP30Topic || entries[0].MsgType != "update" {
		t.Errorf("bad 30s entry: %+v", entries)
	}
	// Unsupported window → nil.
	if got := chainlinkTWAPEntries(45); got != nil {
		t.Errorf("expected nil for unsupported window, got %v", got)
	}
}

func TestRTDSProcessEquityPrice(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeEquityPrices(context.Background(), []string{"AAPL"})

	raw := `{"topic":"equity_prices","type":"update","timestamp":1782753357257,"payload":{"symbol":"aapl","timestamp":1782753357213,"value":189.42,"full_accuracy_value":"189.4217","received_at":1782753357220,"is_carried_forward":true}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.EquityPriceEvent)
	if !ok {
		t.Fatalf("expected *EquityPriceEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "aapl" || ev.Price != "189.4217" || !ev.IsCarriedForward {
		t.Errorf("got symbol=%q price=%q carried=%v", ev.Symbol, ev.Price, ev.IsCarriedForward)
	}
}

func TestRTDSProcessEquitySnapshot(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeEquityPrices(context.Background(), []string{"aapl"})

	raw := `{"topic":"equity_prices","type":"subscribe","timestamp":1782753357257,"payload":{"symbol":"aapl","data":[{"timestamp":1782753297000,"value":189.38},{"timestamp":1782753357000,"value":189.42}]}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.PriceSnapshotEvent)
	if !ok {
		t.Fatalf("expected *PriceSnapshotEvent, got %T", (*got)[0])
	}
	if ev.Source != "equity" || ev.Symbol != "aapl" || len(ev.Points) != 2 {
		t.Errorf("got source=%q symbol=%q points=%d", ev.Source, ev.Symbol, len(ev.Points))
	}
}

func TestRTDSTopicIsolation(t *testing.T) {
	r, got := newTestRTDSClient(t)
	// Subscribe to binance only; a chainlink symbol must not be dispatched.
	r.SubscribeCryptoPrices(context.Background(), []string{"ethusdt"})

	raw := `{"topic":"crypto_prices_chainlink","type":"update","timestamp":1,"payload":{"symbol":"eth/usd","value":2434.2}}`
	r.processMessage([]byte(raw))

	if len(*got) != 0 {
		t.Fatalf("expected 0 events (topic isolation), got %d", len(*got))
	}
}

func TestRTDSFiltersUnsubscribedSymbols(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeCryptoPrices(context.Background(), []string{"ethusdt"})

	raw := `{"topic":"crypto_prices","type":"update","timestamp":1,"payload":{"symbol":"btcusdt","value":76578.37}}`
	r.processMessage([]byte(raw))

	if len(*got) != 0 {
		t.Fatalf("expected 0 events for unsubscribed symbol, got %d", len(*got))
	}
}

func TestRTDSFallbackEnvelopeTimestamp(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt"})

	raw := `{"topic":"crypto_prices","type":"update","timestamp":1000,"payload":{"symbol":"btcusdt","value":10.5}}`
	r.processMessage([]byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.CryptoPriceEvent)
	if !ev.Timestamp.Equal(time.UnixMilli(1000)) {
		t.Errorf("Timestamp = %v, want epoch 1000ms", ev.Timestamp)
	}
}

func TestRTDSIgnoresEmptyFrames(t *testing.T) {
	r, got := newTestRTDSClient(t)
	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt"})

	r.processMessage([]byte(""))
	r.processMessage([]byte(" \n\t "))
	r.processMessage([]byte(`{"topic":"other","type":"x","payload":{}}`))

	if len(*got) != 0 {
		t.Fatalf("expected 0 events for empty/ignored frames, got %d", len(*got))
	}
}

func TestRTDSAddRemoveSymbols(t *testing.T) {
	r, _ := newTestRTDSClient(t)

	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt", "BTCUSDT", "ethusdt"})
	if got := r.subscribedCount(rtdsCryptoPriceTopic); got != 2 {
		t.Fatalf("expected 2 unique binance symbols, got %d", got)
	}
	if !r.isSubscribed(rtdsCryptoPriceTopic, "BTCUSDT") || !r.isSubscribed(rtdsCryptoPriceTopic, "ethusdt") {
		t.Error("expected both symbols subscribed (case-insensitive)")
	}

	r.UnsubscribeCryptoPrices(context.Background(), []string{"btcusdt"})
	if r.isSubscribed(rtdsCryptoPriceTopic, "btcusdt") {
		t.Error("expected btcusdt unsubscribed")
	}
	if !r.isSubscribed(rtdsCryptoPriceTopic, "ethusdt") {
		t.Error("expected ethusdt still subscribed")
	}
}

func TestRTDSFirstLastTransitions(t *testing.T) {
	r, _ := newTestRTDSClient(t)

	first, added := r.addSymbols(rtdsCryptoPriceTopic, []string{"btcusdt", "btcusdt", "ethusdt"})
	if !first || len(added) != 2 {
		t.Errorf("first=%v added=%d, want first=true added=2", first, len(added))
	}

	first, added = r.addSymbols(rtdsCryptoPriceTopic, []string{"solusdt"})
	if first || len(added) != 1 {
		t.Errorf("first=%v added=%d, want first=false added=1", first, len(added))
	}

	last, removed := r.removeSymbols(rtdsCryptoPriceTopic, []string{"btcusdt"})
	if last || len(removed) != 1 {
		t.Errorf("last=%v removed=%d, want last=false removed=1", last, len(removed))
	}

	last, removed = r.removeSymbols(rtdsCryptoPriceTopic, []string{"ethusdt", "solusdt", "ethusdt"})
	if !last || len(removed) != 2 {
		t.Errorf("last=%v removed=%d, want last=true removed=2", last, len(removed))
	}
}

func TestRTDSFilteredEntriesPerSymbol(t *testing.T) {
	// Chainlink: one entry per feed, each with its own JSON-string filter.
	// Dedup happens upstream in addSymbols; this is a pure mapper.
	entries := chainlinkEntries([]string{"eth/usd", "btc/usd"})
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	for _, e := range entries {
		if e.Topic != rtdsChainlinkPriceTopic || e.MsgType != "*" {
			t.Errorf("bad entry: %+v", e)
		}
		if e.Filters == nil {
			t.Error("expected filters")
		}
	}
}

func TestRTDSPriceValue(t *testing.T) {
	// Full accuracy preferred (binance/equity).
	p := rtdsPricePayload{Value: json.RawMessage(`76578.37`), FullAccuracyValue: "76578.37000000"}
	if got := rtdsPriceValue(p, true); got != "76578.37000000" {
		t.Errorf("full-accuracy case = %q", got)
	}

	// Chainlink: full accuracy is a scaled integer — use the numeric value.
	p = rtdsPricePayload{Value: json.RawMessage(`2434.24334265`), FullAccuracyValue: "2434243342650000000000"}
	if got := rtdsPriceValue(p, false); got != "2434.24334265" {
		t.Errorf("chainlink case = %q", got)
	}

	// Numeric value.
	p = rtdsPricePayload{Value: json.RawMessage(`76980.05`)}
	if got := rtdsPriceValue(p, true); got != "76980.05" {
		t.Errorf("numeric case = %q", got)
	}

	// Quoted string value.
	p = rtdsPricePayload{Value: json.RawMessage(`"76980.05"`)}
	if got := rtdsPriceValue(p, true); got != "76980.05" {
		t.Errorf("string case = %q", got)
	}

	// Empty.
	if got := rtdsPriceValue(rtdsPricePayload{}, true); got != "" {
		t.Errorf("empty case = %q", got)
	}
}

func (r *WSPolymarketRTDS) subscribedCount(topic string) int {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()
	st, ok := r.subs[topic]
	if !ok {
		return 0
	}
	return len(st.symbols)
}

// Ensure the subscription request JSON round-trips (Filters any).
func TestRTDSSubscriptionRoundTrip(t *testing.T) {
	sub := rtdsSubscription{Topic: rtdsChainlinkPriceTopic, MsgType: "*", Filters: `{"symbol":"eth/usd"}`}
	data, err := json.Marshal(sub)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"filters":"{\"symbol\":\"eth/usd\"}"`) {
		t.Errorf("unexpected marshal: %s", data)
	}
}
