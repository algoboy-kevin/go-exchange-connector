package connector

import (
	"encoding/json"
	"testing"
	"time"
)

// Gamma wire frames below are trimmed copies of live responses
// (GET /events?series_id=45&closed=false, GET /events?slug=bitcoin-above-on-october-3-2026).
const gammaEventWire = `{
	"id": "1086127",
	"ticker": "bitcoin-above-on-october-3-2026",
	"slug": "bitcoin-above-on-october-3-2026",
	"title": "Bitcoin above $74,000 on October 3?",
	"description": "This market will resolve...",
	"seriesSlug": "btc-multi-strikes-weekly",
	"startDate": "2026-09-26T16:00:15Z",
	"endDate": "2026-10-03T16:00:00Z",
	"closed": false,
	"active": true,
	"negRisk": false,
	"tags": [{"id": "235", "slug": "bitcoin"}, {"id": "102264", "slug": "weekly"}],
	"markets": [
		{
			"id": "4985048",
			"slug": "bitcoin-above-74k-on-october-3-2026",
			"question": "Will the price of Bitcoin be above $74,000 on October 3?",
			"conditionId": "0xcondition",
			"groupItemTitle": "74,000",
			"outcomes": "[\"Yes\", \"No\"]",
			"clobTokenIds": "[\"82008503\", \"22087255\"]",
			"description": "This market will resolve to \"Yes\" if the Binance 1 minute candle...",
			"startDate": "2026-09-26T16:00:15Z",
			"endDate": "2026-10-03T16:00:00Z",
			"closed": false,
			"active": true,
			"negRisk": false,
			"negRiskMarketID": null
		},
		{
			"id": "4985049",
			"slug": "bitcoin-above-76k-on-october-3-2026",
			"groupItemTitle": "76,000",
			"outcomes": "[\"Yes\", \"No\"]",
			"clobTokenIds": "[\"82008504\", \"22087256\"]",
			"closed": false,
			"active": true,
			"negRisk": false,
			"negRiskMarketID": "0xnegrisk"
		}
	]
}`

func TestGammaEventUnmarshal(t *testing.T) {
	var ev GammaEvent
	if err := json.Unmarshal([]byte(gammaEventWire), &ev); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	if ev.ID != "1086127" || ev.Slug != "bitcoin-above-on-october-3-2026" {
		t.Errorf("bad event identity: %+v", ev)
	}
	if ev.SeriesSlug != "btc-multi-strikes-weekly" {
		t.Errorf("SeriesSlug = %q, want btc-multi-strikes-weekly", ev.SeriesSlug)
	}
	if ev.Closed || !ev.Active || ev.NegRisk {
		t.Errorf("bad event flags: closed=%v active=%v negRisk=%v", ev.Closed, ev.Active, ev.NegRisk)
	}
	if want := time.Date(2026, 9, 26, 16, 0, 15, 0, time.UTC); !ev.StartDate.Equal(want) {
		t.Errorf("StartDate = %v, want %v", ev.StartDate, want)
	}
	if want := time.Date(2026, 10, 3, 16, 0, 0, 0, time.UTC); !ev.EndDate.Equal(want) {
		t.Errorf("EndDate = %v, want %v", ev.EndDate, want)
	}
	if len(ev.Tags) != 2 || ev.Tags[1].Slug != "weekly" {
		t.Errorf("bad tags: %+v", ev.Tags)
	}
	// A ladder event holds one market per strike.
	if len(ev.Markets) != 2 {
		t.Fatalf("markets = %d, want 2", len(ev.Markets))
	}
	if ev.Markets[0].GroupItemTitle != "74,000" || ev.Markets[1].GroupItemTitle != "76,000" {
		t.Errorf("bad GroupItemTitle: %q, %q", ev.Markets[0].GroupItemTitle, ev.Markets[1].GroupItemTitle)
	}
	// negRiskMarketID is null in the wire format → empty string, not an error.
	if ev.Markets[0].NegRiskMarketID != "" {
		t.Errorf("NegRiskMarketID = %q, want empty", ev.Markets[0].NegRiskMarketID)
	}
	if ev.Markets[1].NegRiskMarketID != "0xnegrisk" {
		t.Errorf("NegRiskMarketID = %q, want 0xnegrisk", ev.Markets[1].NegRiskMarketID)
	}
}

func TestGammaEventMarketLists(t *testing.T) {
	var ev GammaEvent
	if err := json.Unmarshal([]byte(gammaEventWire), &ev); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}

	m := ev.Markets[0]
	outcomes, err := m.OutcomeList()
	if err != nil {
		t.Fatalf("OutcomeList: %v", err)
	}
	if len(outcomes) != 2 || outcomes[0] != "Yes" || outcomes[1] != "No" {
		t.Errorf("outcomes = %v, want [Yes No]", outcomes)
	}

	tokens, err := m.TokenIDList()
	if err != nil {
		t.Fatalf("TokenIDList: %v", err)
	}
	// Index-aligned with outcomes: 0 = YES, 1 = NO.
	if len(tokens) != len(outcomes) {
		t.Fatalf("token ids = %v, outcomes = %v (not index-aligned)", tokens, outcomes)
	}
	if tokens[0] != "82008503" || tokens[1] != "22087255" {
		t.Errorf("token ids = %v", tokens)
	}
}

func TestGammaMarketListEdgeCases(t *testing.T) {
	// Absent fields are not errors.
	var empty GammaMarket
	outcomes, err := empty.OutcomeList()
	if err != nil || outcomes != nil {
		t.Errorf("OutcomeList(empty) = %v, %v; want nil, nil", outcomes, err)
	}
	tokens, err := empty.TokenIDList()
	if err != nil || tokens != nil {
		t.Errorf("TokenIDList(empty) = %v, %v; want nil, nil", tokens, err)
	}

	// Malformed JSON-string arrays surface the decode error.
	bad := GammaMarket{Outcomes: "Yes,No", ClobTokenIDs: `{"yes":"1"}`}
	if _, err := bad.OutcomeList(); err == nil {
		t.Error("OutcomeList(malformed) = nil error, want error")
	}
	if _, err := bad.TokenIDList(); err == nil {
		t.Error("TokenIDList(malformed) = nil error, want error")
	}
}
