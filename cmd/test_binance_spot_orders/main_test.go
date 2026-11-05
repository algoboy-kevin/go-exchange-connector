package main

import (
	"testing"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/spot"
)

func btcSymbol(minNotional string) spot.Symbol {
	return spot.Symbol{
		Symbol: "BTCUSDT", Status: "TRADING", QuoteAsset: "USDT",
		Filters: []spot.Filter{
			{FilterType: "PRICE_FILTER", MinPrice: "0.01000000", MaxPrice: "1000000.00000000", TickSize: "0.01000000"},
			{FilterType: "LOT_SIZE", MinQty: "0.00001000", MaxQty: "9000.00000000", StepSize: "0.00001000"},
			{FilterType: "NOTIONAL", MinNotional: minNotional, ApplyMinToMarket: true},
			{FilterType: "PERCENT_PRICE_BY_SIDE", BidMultiplierUp: "5", BidMultiplierDown: "0.2", AskMultiplierUp: "5", AskMultiplierDown: "0.2"},
		},
	}
}

func TestBuildPlan(t *testing.T) {
	p, err := buildPlan(btcSymbol("5.00000000"), "100000.00", "100000.02")
	if err != nil {
		t.Fatal(err)
	}
	// 80% of mid 100000.01 = 80000.008 -> tick 0.01 down = 80000.00
	if p.price != "80000.00" {
		t.Fatalf("price %s", p.price)
	}
	// target = max(5,6)*1.1 = 6.6 ; 6.6/80000 = 0.0000825 -> step 0.00001 up = 0.00009
	if p.qty != "0.00009" {
		t.Fatalf("qty %s", p.qty)
	}
	if p.notional != "7.2000000" && p.notional != "7.20000" && p.notional != "7.2" {
		t.Logf("notional %s", p.notional)
	}
	if c, _ := spot.CmpDecimal(p.notional, "7.2"); c != 0 {
		t.Fatalf("notional %s", p.notional)
	}
}

func TestBuildPlanUsesSymbolMinimumWhenAboveFloor(t *testing.T) {
	p, err := buildPlan(btcSymbol("10.00000000"), "100000.00", "100000.02")
	if err != nil {
		t.Fatal(err)
	}
	// target 11 ; 11/80000 = 0.00013750 -> 0.00014
	if p.qty != "0.00014" {
		t.Fatalf("qty %s", p.qty)
	}
	if c, _ := spot.CmpDecimal(p.notional, "15"); c > 0 {
		t.Fatalf("over cap: %s", p.notional)
	}
}

func TestBuildPlanRefusesOverCap(t *testing.T) {
	// A coarse step makes the rounded-up notional exceed the 15 USD cap.
	s := btcSymbol("5.00000000")
	s.Filters[1].StepSize = "0.001"
	s.Filters[1].MinQty = "0.001"
	if _, err := buildPlan(s, "100000.00", "100000.02"); err == nil {
		t.Fatal("0.001 BTC at 80000 is 80 USD: must be refused")
	}
	// The symbol minimum itself above the cap.
	if _, err := buildPlan(btcSymbol("20.00000000"), "100000.00", "100000.02"); err == nil {
		t.Fatal("minNotional 20 cannot fit under the 15 USD cap")
	}
}

func TestBuildPlanRejectsBrokenRules(t *testing.T) {
	s := btcSymbol("5")
	s.Filters = s.Filters[:1] // no LOT_SIZE
	if _, err := buildPlan(s, "100", "101"); err == nil {
		t.Fatal("missing stepSize accepted")
	}
	if _, err := buildPlan(btcSymbol("5"), "x", "101"); err == nil {
		t.Fatal("bad bid accepted")
	}
}

func TestPercentile(t *testing.T) {
	ds := []time.Duration{5, 1, 4, 2, 3}
	if percentile(ds, 0.5) != 3 || percentile(ds, 0.95) != 5 || percentile(nil, 0.5) != 0 {
		t.Fatal("percentile")
	}
}

func TestEventPredicates(t *testing.T) {
	if !isNewEv(wsEvent{execType: "NEW", status: "NEW"}) || isNewEv(wsEvent{execType: "TRADE", status: "NEW"}) {
		t.Fatal("isNewEv")
	}
	if !isCancelEv(wsEvent{execType: "CANCELED", status: "CANCELED"}) || isCancelEv(wsEvent{execType: "NEW", status: "NEW"}) {
		t.Fatal("isCancelEv")
	}
}
