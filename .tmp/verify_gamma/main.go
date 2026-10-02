// Command verify_gamma is a dev-time live check of the v0.6.0 series-discovery
// API against the real Gamma API. Not part of the module's build
// (`go build ./...` skips dot-directories).
//
//	go run ./.tmp/verify_gamma
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
)

func check(label string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ %s: %v\n", label, err)
		os.Exit(1)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn := polymarket.New(false, polymarket.Config{}, nil)

	// 1. Series lookup by slug (cached).
	series, err := conn.GetSeries(ctx, "btc-up-or-down-daily")
	check("GetSeries", err)
	for _, s := range series {
		fmt.Printf("✅ GetSeries id=%s slug=%s ticker=%s recurrence=%s active=%v closed=%v\n",
			s.ID, s.Slug, s.Ticker, s.Recurrence, s.Active, s.Closed)
	}

	// 2. Miss → empty, nil error.
	missing, err := conn.GetSeries(ctx, "does-not-exist-xyz")
	check("GetSeries(miss)", err)
	fmt.Printf("✅ GetSeries(miss) → %d series, err=%v\n", len(missing), err)

	// 3. Cached second call + explicit invalidation.
	if _, err := conn.GetSeries(ctx, "btc-up-or-down-daily"); err != nil {
		check("GetSeries(cached)", err)
	}
	conn.InvalidateSeries("btc-up-or-down-daily")
	fmt.Println("✅ GetSeries cached + InvalidateSeries ok")

	// 4. Open + future events of the multi-strikes weekly series (id 45).
	openOnly := false
	events, err := conn.ListSeriesEvents(ctx, connector.EventQuery{
		SeriesID:  "45",
		Closed:    &openOnly,
		Limit:     50,
		Order:     "endDate",
		Ascending: true,
	})
	check("ListSeriesEvents", err)
	fmt.Printf("✅ ListSeriesEvents series=45 → %d open events\n", len(events))
	for _, ev := range events {
		strikes := map[string]bool{}
		tokenCounts := map[int]int{}
		for _, mk := range ev.Markets {
			strikes[mk.GroupItemTitle] = true
			tokens, err := mk.TokenIDList()
			check("TokenIDList", err)
			tokenCounts[len(tokens)]++
		}
		fmt.Printf("   %-42s endDate=%s markets=%2d distinctStrikes=%2d tokenCounts=%v\n",
			ev.Slug, ev.EndDate.Format(time.RFC3339), len(ev.Markets), len(strikes), tokenCounts)
	}

	// 5. Single event with nested markets + index-aligned token ids.
	ev, err := conn.GetEvent(ctx, "bitcoin-above-on-october-3-2026")
	check("GetEvent", err)
	if ev == nil {
		fmt.Fprintln(os.Stderr, "❌ GetEvent → not found")
		os.Exit(1)
	}
	fmt.Printf("✅ GetEvent %s seriesSlug=%s start=%s end=%s closed=%v markets=%d tags=%d\n",
		ev.Slug, ev.SeriesSlug, ev.StartDate.Format(time.RFC3339), ev.EndDate.Format(time.RFC3339),
		ev.Closed, len(ev.Markets), len(ev.Tags))

	rung := ev.Markets[0]
	outcomes, err := rung.OutcomeList()
	check("OutcomeList", err)
	tokens, err := rung.TokenIDList()
	check("TokenIDList", err)
	fmt.Printf("✅ rung groupItemTitle=%q outcomes=%v tokens=%d aligned=%v\n",
		rung.GroupItemTitle, outcomes, len(tokens), len(tokens) == len(outcomes))
	fmt.Printf("   description=%q\n", truncate(rung.Description, 90))

	// 6. GetMarket now carries the strike / event / resolution-rule metadata.
	m, err := conn.GetMarket("", rung.Slug)
	check("GetMarket", err)
	fmt.Printf("✅ GetMarket groupItemTitle=%q negRisk=%v negRiskMarketID=%q eventSlug=%q eventTicker=%q endDate=%s\n",
		m.GroupItemTitle, m.NegRisk, m.NegRiskMarketID, m.EventSlug, m.EventTicker, m.EndDate.Format(time.RFC3339))

	fmt.Println("🎉 live Gamma checks passed")
}
