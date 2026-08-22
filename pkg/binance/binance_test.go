package binance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func TestNormalizeBinanceSymbol(t *testing.T) {
	cases := map[string]string{
		"BTCUSDT":   "btcusdt",
		"btcusdt":   "btcusdt",
		" EthUSDT ": "ethusdt",
		"":          "",
	}
	for in, want := range cases {
		if got := normalizeSymbol(in); got != want {
			t.Errorf("normalizeSymbol(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStreamNames(t *testing.T) {
	cases := map[string]string{
		streamFor("btcusdt", streamBookTicker): "btcusdt@bookTicker",
		streamFor("btcusdt", streamAggTrade):   "btcusdt@aggTrade",
		streamFor("btcusdt", streamDepth):      "btcusdt@depth@100ms",
		streamFor("btcusdt", streamKline):      "btcusdt@kline_1m",
		klineStream("ethusdt", "15m"):          "ethusdt@kline_15m",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("stream name = %q, want %q", got, want)
		}
	}
}

func TestBinanceSubscribeJSONShape(t *testing.T) {
	req := binanceSubRequest{Method: binanceMethodSubscribe, Params: []string{"btcusdt@bookTicker", "btcusdt@aggTrade"}, ID: 7}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"method":"SUBSCRIBE","params":["btcusdt@bookTicker","btcusdt@aggTrade"],"id":7}`
	if string(data) != want {
		t.Errorf("subscribe JSON mismatch:\n got  %s\n want %s", data, want)
	}
}

// newTestBinance builds a Binance stream manager over a real connector base
// and captures every dispatched event.
func newTestBinance(t *testing.T) (*WSBinance, *[]any) {
	t.Helper()
	base := connector.New(false, nil)
	var got []any
	base.SetDispatcher(func(ev any) {
		got = append(got, ev)
	})
	return New(base), &got
}

func TestBinanceParseFuturesBookTicker(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeBookTicker(context.Background(), MarketPerp, []string{"btcusdt"})

	// Futures bookTicker includes the event type field.
	raw := `{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}`
	b.processMessage(b.conns[MarketPerp], []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceBookTickerEvent)
	if !ok {
		t.Fatalf("expected *BinanceBookTickerEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Market != "perp" ||
		ev.BestBidPrice != "25.35190000" || ev.BestAskPrice != "25.36520000" ||
		ev.BestBidQty != "31.21000000" || ev.BestAskQty != "40.66000000" ||
		ev.UpdateID != 400900217 {
		t.Errorf("bad bookTicker: %+v", ev)
	}
	if !ev.Timestamp.Equal(time.UnixMilli(1568014460893)) || ev.SeqID <= 0 || ev.ReceivedAt.IsZero() {
		t.Errorf("bad time/seq fields: ts=%v seq=%d", ev.Timestamp, ev.SeqID)
	}
}

func TestBinanceParseSpotBookTicker(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeBookTicker(context.Background(), MarketSpot, []string{"btcusdt"})

	// Spot bookTicker has no event type field.
	raw := `{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}`
	b.processMessage(b.conns[MarketSpot], []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceBookTickerEvent)
	if !ok {
		t.Fatalf("expected *BinanceBookTickerEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Market != "spot" || ev.BestBidPrice != "25.35190000" {
		t.Errorf("bad spot bookTicker: %+v", ev)
	}
}

func TestBinanceUnsubscribedSymbolIgnored(t *testing.T) {
	b, got := newTestBinance(t)
	// Subscribe to ethusdt but feed a btcusdt frame.
	b.SubscribeBookTicker(context.Background(), MarketSpot, []string{"ethusdt"})
	raw := `{"u":1,"s":"BTCUSDT","b":"1.0","B":"1","a":"1.1","A":"1"}`
	b.processMessage(b.conns[MarketSpot], []byte(raw))
	if len(*got) != 0 {
		t.Fatalf("expected no event for unsubscribed symbol, got %d", len(*got))
	}
}

func TestBinanceParseAggTrade(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeTrades(context.Background(), MarketPerp, []string{"btcusdt"})

	raw := `{"e":"aggTrade","E":1628843331742,"s":"BTCUSDT","a":105688535,"p":"46063.00","q":"0.005","f":188354417,"l":188354417,"T":1628843331590,"m":false}`
	b.processMessage(b.conns[MarketPerp], []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceAggTradeEvent)
	if !ok {
		t.Fatalf("expected *BinanceAggTradeEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Market != "perp" || ev.TradeID != 105688535 ||
		ev.Price != "46063.00" || ev.Quantity != "0.005" ||
		ev.FirstTradeID != 188354417 || ev.LastTradeID != 188354417 || ev.IsBuyerMaker {
		t.Errorf("bad aggTrade: %+v", ev)
	}
}

func TestBinanceParseKline(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeKlines(context.Background(), MarketSpot, []string{"btcusdt"}, "1m")

	// Real Binance frame (includes f/L/V/Q/B fields) — L (last trade id) must
	// not collide case-insensitively with the Low price field (tag "l").
	raw := `{"e":"kline","E":1787380868016,"s":"BTCUSDT","k":{"t":1787380860000,"T":1787380919999,"s":"BTCUSDT","i":"1m","f":6601225492,"L":6601226390,"o":"77560.64000000","c":"77539.56000000","h":"77568.87000000","l":"77539.56000000","v":"2.89566000","n":899,"x":false,"q":"224590.84896810","V":"1.51980000","Q":"117873.71400110","B":"0"}}`
	b.processMessage(b.conns[MarketSpot], []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceKlineEvent)
	if !ok {
		t.Fatalf("expected *BinanceKlineEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Market != "spot" || ev.Interval != "1m" ||
		ev.Open != "77560.64000000" || ev.Close != "77539.56000000" ||
		ev.High != "77568.87000000" || ev.Low != "77539.56000000" ||
		ev.Volume != "2.89566000" || ev.QuoteVolume != "224590.84896810" || ev.IsFinal {
		t.Errorf("bad kline: %+v", ev)
	}
	if !ev.OpenTime.Equal(time.UnixMilli(1787380860000)) || !ev.CloseTime.Equal(time.UnixMilli(1787380919999)) {
		t.Errorf("bad kline times: open=%v close=%v", ev.OpenTime, ev.CloseTime)
	}
}

func TestBinanceApplyLevelsAndBookLevels(t *testing.T) {
	m := map[string]string{"100.0": "1.0", "99.0": "2.0"}
	applyLevels(m, []binanceLevel{
		{Price: "100.0", Qty: "0"}, // removal
		{Price: "101.0", Qty: "5.0"},
	})
	if _, ok := m["100.0"]; ok {
		t.Errorf("expected 100.0 removed")
	}
	if m["101.0"] != "5.0" {
		t.Errorf("101.0 qty = %q, want 5.0", m["101.0"])
	}

	// Bids descending (best first), asks ascending (best first).
	bids := bookLevels(map[string]string{"100.0": "1.0", "99.0": "2.0", "101.0": "3.0"}, false)
	if bids[0].Price != "101.0" || bids[2].Price != "99.0" {
		t.Errorf("bids not descending: %+v", bids)
	}
	asks := bookLevels(map[string]string{"101.0": "1.0", "102.0": "2.0", "100.0": "3.0"}, true)
	if asks[0].Price != "100.0" || asks[2].Price != "102.0" {
		t.Errorf("asks not ascending: %+v", asks)
	}
}

func TestBinanceHandleDepth(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeDepth(context.Background(), MarketSpot, []string{"btcusdt"})

	// Seed the book (normally done by the REST snapshot).
	bk := b.bookFor(MarketSpot, "btcusdt")
	bk.mu.Lock()
	bk.snapshotted = true
	bk.lastUpdateID = 100
	bk.bids = map[string]string{"100.0": "1.0", "99.0": "2.0"}
	bk.asks = map[string]string{"101.0": "1.5"}
	bk.mu.Unlock()

	// Diff update: U == last+1, so it applies.
	raw := `{"e":"depthUpdate","E":1628843331742,"s":"BTCUSDT","U":101,"u":101,"b":[["100.0","0"],["99.5","4.0"]],"a":[["101.0","1.0"]]}`
	b.processMessage(b.conns[MarketSpot], []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceDepthEvent)
	if !ok {
		t.Fatalf("expected *BinanceDepthEvent, got %T", (*got)[0])
	}
	if ev.Market != "spot" || ev.LastUpdateID != 101 {
		t.Errorf("bad depth event: %+v", ev)
	}
	if len(ev.Bids) != 2 || ev.Bids[0].Price != "99.5" || ev.Bids[1].Price != "99.0" {
		t.Errorf("bids not updated correctly: %+v", ev.Bids)
	}
	if len(ev.Asks) != 1 || ev.Asks[0].Price != "101.0" || ev.Asks[0].Size != "1.0" {
		t.Errorf("asks not updated correctly: %+v", ev.Asks)
	}
}

func TestBinanceDepthStaleUpdateIgnored(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeDepth(context.Background(), MarketPerp, []string{"btcusdt"})

	bk := b.bookFor(MarketPerp, "btcusdt")
	bk.mu.Lock()
	bk.snapshotted = true
	bk.lastUpdateID = 100
	bk.bids = map[string]string{"100.0": "1.0"}
	bk.asks = map[string]string{"101.0": "1.0"}
	bk.mu.Unlock()

	// FinalUpdateID <= lastUpdateID → stale, ignored.
	raw := `{"e":"depthUpdate","s":"BTCUSDT","U":99,"u":100,"b":[["100.0","5.0"]],"a":[]}`
	b.processMessage(b.conns[MarketPerp], []byte(raw))

	if len(*got) != 0 {
		t.Fatalf("expected no event for stale update, got %d", len(*got))
	}
}
