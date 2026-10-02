// Command test_rotate validates the production PolymarketConnector path after
// the "no forced reconnect on Subscribe/Unsubscribe" change (see market.go):
//
//	Subscribe/Unsubscribe now push the full desired asset set over the live
//	connection (full-set replace) instead of calling Disconnect() and
//	reconnecting. A full rotation cycle should therefore produce ZERO
//	disconnects, eliminating the "constant 3 disconnects per market" seen in
//	the 2026-08-24 collector recordings.
//
// It drives the REAL connector API (polymarket.New → Start → SetDispatcher →
// SetOnMarketStatusChange → Subscribe/Unsubscribe), resolving two consecutive
// active btc_5m markets, and counts market-WS disconnect events per phase.
//
// Usage (no auth needed):
//
//	go run ./cmd/test_rotate/
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

const (
	gammaURL   = "https://gamma-api.polymarket.com"
	seriesSlug = "btc-updown-5m-%d"
	phaseWait  = 6 * time.Second
)

// ─────────────────────────────────────────────────────────────
// Market resolution
// ─────────────────────────────────────────────────────────────

func resolvePair() (a, b *polymarket.GammaMarket, err error) {
	gamma := polymarket.NewGammaClient(gammaURL)
	interval := 5 * time.Minute
	for attempt := 0; attempt < 5; attempt++ {
		aligned := time.Now().Truncate(interval)
		pairs := [][2]int64{
			{aligned.Unix(), aligned.Add(interval).Unix()},
			{aligned.Add(-interval).Unix(), aligned.Unix()},
		}
		for _, p := range pairs {
			ga, errA := gamma.FetchMarketBySlug(fmt.Sprintf(seriesSlug, p[0]))
			gb, errB := gamma.FetchMarketBySlug(fmt.Sprintf(seriesSlug, p[1]))
			if errA == nil && errB == nil {
				return ga, gb, nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return nil, nil, fmt.Errorf("could not resolve two consecutive active btc_5m markets")
}

// ─────────────────────────────────────────────────────────────
// Event / disconnect counters
// ─────────────────────────────────────────────────────────────

type observer struct {
	mu          sync.Mutex
	byAsset     map[string]int
	disconnects int
	connects    int
}

func newObserver() *observer { return &observer{byAsset: map[string]int{}} }

func (o *observer) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.byAsset = map[string]int{}
}

func (o *observer) onEvent(ev any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch e := ev.(type) {
	case *connector.PriceChangeEvent:
		for _, ch := range e.Changes {
			o.byAsset[ch.AssetID]++
		}
	case *connector.BookSnapshotEvent:
		o.byAsset[e.AssetID]++
	case *connector.TradeEvent:
		o.byAsset[e.AssetID]++
	}
}

func (o *observer) onStatus(s ws.ConnectionStatus) {
	switch s {
	case ws.StatusDisconnected:
		o.mu.Lock()
		o.disconnects++
		o.mu.Unlock()
		fmt.Printf("    ⚠ STATUS DISCONNECTED (count=%d)\n", o.disconnects)
	case ws.StatusConnected:
		o.mu.Lock()
		o.connects++
		o.mu.Unlock()
	}
}

func (o *observer) counts(assetIDs []string) (n int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range assetIDs {
		n += o.byAsset[id]
	}
	return
}

func (o *observer) disc() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.disconnects
}

// ─────────────────────────────────────────────────────────────
// Results table
// ─────────────────────────────────────────────────────────────

type phase struct {
	label  string
	pass   bool
	detail string
}

func printTable(phases []phase) {
	wLabel, wDetail := 40, 52
	sep := func() {
		fmt.Print("+", strings.Repeat("-", wLabel+2), "+", strings.Repeat("-", 8), "+", strings.Repeat("-", wDetail+2), "+\n")
	}
	sep()
	fmt.Printf("| %-*s | %-*s | %-*s |\n", wLabel, "PHASE", 6, "PASS", wDetail, "OBSERVED")
	sep()
	for _, p := range phases {
		icon := "✅"
		if !p.pass {
			icon = "❌"
		}
		fmt.Printf("| %-*s | %-*s | %-*s |\n", wLabel, p.label, 6, icon, wDetail, p.detail)
	}
	sep()

	passed := 0
	for _, p := range phases {
		if p.pass {
			passed++
		}
	}
	fmt.Printf("\n%d/%d phases passed. ", passed, len(phases))
	if passed == len(phases) {
		fmt.Println("=> ZERO disconnects across the full rotation cycle: the forced reconnect is gone.")
	} else {
		fmt.Println("=> Disconnects still occurred during rotation — investigate.")
	}
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fmt.Println("== Resolving two consecutive active btc_5m markets ==")
	gmA, gmB, err := resolvePair()
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌", err)
		os.Exit(1)
	}
	assetA := []string{gmA.YesTokenID, gmA.NoTokenID}
	assetB := []string{gmB.YesTokenID, gmB.NoTokenID}
	fmt.Printf("  A: %s (yes=%s… no=%s…)\n", gmA.Slug, short(gmA.YesTokenID), short(gmA.NoTokenID))
	fmt.Printf("  B: %s (yes=%s… no=%s…)\n", gmB.Slug, short(gmB.YesTokenID), short(gmB.NoTokenID))

	fmt.Println("\n== Starting PolymarketConnector (paper) ==")
	cfg := polymarket.Config{ReconnectIntervalMs: 500}
	conn := polymarket.New(false, cfg, nil)
	if err := conn.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "❌ start:", err)
		os.Exit(1)
	}
	defer conn.Stop()

	obs := newObserver()
	conn.SetDispatcher(obs.onEvent)
	conn.SetOnMarketStatusChange(obs.onStatus) // after Start (market WS created there)

	// warm up the connection with A so phase 0 has data
	conn.Subscribe(assetA)
	time.Sleep(2 * time.Second)

	var phases []phase

	// ── R0: Subscribe(A) — expect A to flow, 0 disconnects ──
	fmt.Println("\n== R0  Subscribe(A) ==")
	obs.reset()
	conn.Subscribe(assetA)
	time.Sleep(phaseWait)
	a, b := obs.counts(assetA), obs.counts(assetB)
	d := obs.disc()
	phases = append(phases, phase{"R0 Subscribe(A): A flows, no disconnect", d == 0 && a > 0, fmt.Sprintf("A=%d B=%d disc=%d", a, b, d)})

	// ── R1: Subscribe(B) — expect A+B, 0 disconnects ────────
	fmt.Println("\n== R1  Subscribe(B) (incremental add over live conn) ==")
	obs.reset()
	conn.Subscribe(assetB)
	time.Sleep(phaseWait)
	a, b = obs.counts(assetA), obs.counts(assetB)
	d = obs.disc()
	phases = append(phases, phase{"R1 Subscribe(B): B flows, no disconnect", d == 0 && a > 0 && b > 0, fmt.Sprintf("A=%d B=%d disc=%d", a, b, d)})

	// ── R2: Unsubscribe(A) — expect B only, 0 disconnects ───
	fmt.Println("\n== R2  Unsubscribe(A) (incremental remove over live conn) ==")
	obs.reset()
	conn.Unsubscribe(assetA)
	time.Sleep(phaseWait)
	a, b = obs.counts(assetA), obs.counts(assetB)
	d = obs.disc()
	phases = append(phases, phase{"R2 Unsubscribe(A): A stops, B flows, no disconnect", d == 0 && a == 0 && b > 0, fmt.Sprintf("A=%d B=%d disc=%d", a, b, d)})

	fmt.Println("\n== Summary ==")
	printTable(phases)
}

func short(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16]
}
