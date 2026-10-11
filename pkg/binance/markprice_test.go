package binance

import (
	"context"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// TestMarkPriceStreamNameAndRouting pins the stream name and proves the
// markPrice stream is carried by the perp /market connection, not /public.
func TestMarkPriceStreamNameAndRouting(t *testing.T) {
	if got := streamFor("btcusdt", streamMarkPrice); got != "btcusdt@markPrice@1s" {
		t.Errorf("streamFor(markPrice) = %q, want btcusdt@markPrice@1s", got)
	}
	if got := streamClassOf(MarketPerp, "btcusdt@markPrice@1s"); got != classMarket {
		t.Errorf("perp markPrice class = %q, want %q", got, classMarket)
	}
	// Sanity: the public class still owns bookTicker/depth.
	if got := streamClassOf(MarketPerp, "btcusdt@bookTicker"); got != classPublic {
		t.Errorf("perp bookTicker class = %q, want %q", got, classPublic)
	}

	b, _ := newTestBinance(t)
	b.SubscribeMarkPrice(context.Background(), MarketPerp, []string{"BTCUSDT"})

	market := b.connFor(MarketPerp, classMarket).allStreams()
	if len(market) != 1 || market[0] != "btcusdt@markPrice@1s" {
		t.Errorf("market conn streams = %v, want [btcusdt@markPrice@1s]", market)
	}
	if pub := b.connFor(MarketPerp, classPublic).allStreams(); len(pub) != 0 {
		t.Errorf("markPrice leaked onto the public conn: %v", pub)
	}

	b.UnsubscribeMarkPrice(context.Background(), MarketPerp, []string{"btcusdt"})
	if s := b.connFor(MarketPerp, classMarket).allStreams(); len(s) != 0 {
		t.Errorf("streams after unsubscribe = %v, want none", s)
	}
}

// TestBinanceParseMarkPrice decodes a real markPrice@1s frame, including the
// case-sensitive p/P, i and r/T fields.
func TestBinanceParseMarkPrice(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeMarkPrice(context.Background(), MarketPerp, []string{"BTCUSDT"})

	raw := `{"e":"markPriceUpdate","E":1787380860000,"s":"BTCUSDT","p":"77539.56000000","i":"77540.10000000","P":"77500.00000000","r":"0.00010000","T":1787383200000}`
	b.processMessage(b.connFor(MarketPerp, classMarket), []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev, ok := (*got)[0].(*connector.BinanceMarkPriceEvent)
	if !ok {
		t.Fatalf("expected *BinanceMarkPriceEvent, got %T", (*got)[0])
	}
	if ev.Symbol != "BTCUSDT" || ev.Market != "perp" {
		t.Errorf("bad symbol/market: %+v", ev)
	}
	if ev.MarkPrice != "77539.56000000" {
		t.Errorf("MarkPrice = %q (p must not land in P)", ev.MarkPrice)
	}
	if ev.IndexPrice != "77540.10000000" {
		t.Errorf("IndexPrice = %q", ev.IndexPrice)
	}
	if ev.EstSettlePrice != "77500.00000000" {
		t.Errorf("EstSettlePrice = %q (P must not land in p)", ev.EstSettlePrice)
	}
	if ev.FundingRate != "0.00010000" {
		t.Errorf("FundingRate = %q", ev.FundingRate)
	}
	if ev.NextFundingTime != 1787383200000 {
		t.Errorf("NextFundingTime = %d, want 1787383200000 (T)", ev.NextFundingTime)
	}
	if ev.EventTime != 1787380860000 {
		t.Errorf("EventTime = %d, want 1787380860000 (E)", ev.EventTime)
	}
	if ev.SeqID <= 0 || ev.ReceivedAt.IsZero() || ev.RecvTime.IsZero() {
		t.Errorf("bad seq/receive fields: seq=%d received=%v recv=%v", ev.SeqID, ev.ReceivedAt, ev.RecvTime)
	}
	if !ev.RecvTime.Equal(ev.ReceivedAt) {
		t.Errorf("RecvTime %v != ReceivedAt %v", ev.RecvTime, ev.ReceivedAt)
	}
}

// TestBinanceParseMarkPriceNegativeFunding proves a negative funding rate
// survives as an exact string (no float round-trip).
func TestBinanceParseMarkPriceNegativeFunding(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeMarkPrice(context.Background(), MarketPerp, []string{"ETHUSDT"})

	raw := `{"e":"markPriceUpdate","E":1787380860000,"s":"ETHUSDT","p":"3000.10","i":"3000.20","P":"3000.00","r":"-0.00012345","T":1787383200000}`
	b.processMessage(b.connFor(MarketPerp, classMarket), []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(*got))
	}
	ev := (*got)[0].(*connector.BinanceMarkPriceEvent)
	if ev.FundingRate != "-0.00012345" {
		t.Errorf("FundingRate = %q, want -0.00012345", ev.FundingRate)
	}
	if ev.MarkPrice != "3000.10" || ev.IndexPrice != "3000.20" || ev.EstSettlePrice != "3000.00" {
		t.Errorf("bad prices: %+v", ev)
	}
}

// TestBinanceMarkPriceUnsubscribedIgnored drops a frame for a symbol that was
// never subscribed.
func TestBinanceMarkPriceUnsubscribedIgnored(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeMarkPrice(context.Background(), MarketPerp, []string{"ethusdt"})

	raw := `{"e":"markPriceUpdate","E":1,"s":"BTCUSDT","p":"1.0","i":"1.0","P":"1.0","r":"0.0","T":2}`
	b.processMessage(b.connFor(MarketPerp, classMarket), []byte(raw))

	if len(*got) != 0 {
		t.Fatalf("expected no event for unsubscribed symbol, got %d", len(*got))
	}
}

// TestMarkPriceTimestampUnaffectedByKlinePath is a guard that the markPrice
// handler does not go through the kline/depth paths: a markPrice frame must
// produce exactly one MarkPrice event and nothing else.
func TestMarkPriceExclusiveDispatch(t *testing.T) {
	b, got := newTestBinance(t)
	b.SubscribeMarkPrice(context.Background(), MarketPerp, []string{"BTCUSDT"})
	b.SubscribeTrades(context.Background(), MarketPerp, []string{"BTCUSDT"})

	raw := `{"e":"markPriceUpdate","E":1787380860000,"s":"BTCUSDT","p":"77539.56","i":"77540.10","P":"77500.00","r":"0.00000000","T":1787383200000}`
	b.processMessage(b.connFor(MarketPerp, classMarket), []byte(raw))

	if len(*got) != 1 {
		t.Fatalf("expected exactly 1 event, got %d", len(*got))
	}
	if _, ok := (*got)[0].(*connector.BinanceMarkPriceEvent); !ok {
		t.Fatalf("got %T, want *BinanceMarkPriceEvent", (*got)[0])
	}
	if got := len(*got); got != 1 {
		t.Fatalf("markPrice frame produced %d events", got)
	}
}
