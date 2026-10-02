// Command test_incr answers a single question:
//
//	Does Polymarket's market channel support INCREMENTAL subscribe/unsubscribe
//	over a live connection — or must we reconnect (full handshake) every time
//	the asset set changes?
//
// Background
// ----------
// The trading-core collector records rolling markets. On every rotation it
// calls Subscribe(next) / Unsubscribe(prev), and the connector's
// WSPolymarketMarket.Subscribe/Unsubscribe each force a full WebSocket
// reconnect (pm.Disconnect()) so the updated asset list is re-sent via the
// initial handshake. The rationale comment (added in v0.3.6 "fix: add asset
// bug") says the channel "does not reliably support incremental subscribe
// operations".
//
// That forced reconnect is the "constant 3 disconnects per market" observed
// in the 2026-08-24 recordings — each one costs ~1s of market data and is a
// fresh chance to hit a real failure (slow-consumer kick, EOF).
//
// This harness connects RAW to the market WS and empirically verifies, on a
// SINGLE connection, whether:
//
//	P0  full-handshake subscribe to A              -> events for A flow
//	P1  INCREMENTAL subscribe to B (operation)     -> events for B flow, A keeps flowing
//	P2  INCREMENTAL unsubscribe A (operation)      -> A stops, B keeps flowing
//	P3  handshake-form full replace to B-only      -> A stays silent (replacement semantics)
//
// If P1/P2 pass, the forced reconnect is unnecessary and the connector can be
// changed to do incremental subscribe/unsubscribe (killing the constant-3).
//
// Usage (no auth needed):
//
//	go run ./cmd/test_incr/
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	coderws "github.com/coder/websocket"
)

const (
	marketWSURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	gammaURL    = "https://gamma-api.polymarket.com"
	seriesSlug  = "btc-updown-5m-%d" // btc_5m slug format, epoch-aligned
	phaseWait   = 8 * time.Second    // observation window per phase
)

// ─────────────────────────────────────────────────────────────
// Wire message types (mirror of the connector's unexported structs)
// ─────────────────────────────────────────────────────────────

// subscriptionMessage is the FULL handshake form (replaces the whole set).
type subscriptionMessage struct {
	AssetsIDs            []string `json:"assets_ids"`
	Type                 string   `json:"type"`
	CustomFeatureEnabled bool     `json:"custom_feature_enabled"`
}

// subscribeMessage / unsubscribeMessage are the INCREMENTAL operation forms.
type subscribeMessage struct {
	Operation string   `json:"operation"`
	AssetsIDs []string `json:"assets_ids"`
}

type unsubscribeMessage struct {
	Operation string   `json:"operation"`
	AssetsIDs []string `json:"assets_ids"`
}

// ─────────────────────────────────────────────────────────────
// Event counter
// ─────────────────────────────────────────────────────────────

type counter struct {
	mu       sync.Mutex
	byAsset  map[string]int // assetID -> event count
	rawMsgs  int
	errors   int
	lastType string
}

func newCounter() *counter { return &counter{byAsset: map[string]int{}} }

func (c *counter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byAsset = map[string]int{}
	c.rawMsgs = 0
	c.errors = 0
	c.lastType = ""
}

func (c *counter) note(assetIDs []string, msgType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rawMsgs++
	c.lastType = msgType
	for _, id := range assetIDs {
		c.byAsset[id]++
	}
}

func (c *counter) countFor(assetIDs []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, id := range assetIDs {
		n += c.byAsset[id]
	}
	return n
}

func (c *counter) totals() (raw, errs int, lastType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rawMsgs, c.errors, c.lastType
}

// assetIDsIn extracts asset IDs from a raw market-channel message (handles a
// single object or an array of envelopes).
func assetIDsIn(raw []byte) []string {
	var docs []json.RawMessage
	if err := json.Unmarshal(raw, &docs); err != nil {
		docs = []json.RawMessage{raw}
	}
	var out []string
	for _, d := range docs {
		var env struct {
			EventType string `json:"event_type"`
			AssetID   string `json:"asset_id"`
			Changes   []struct {
				AssetID string `json:"asset_id"`
			} `json:"changes"`
		}
		if err := json.Unmarshal(d, &env); err != nil {
			continue
		}
		if env.AssetID != "" {
			out = append(out, env.AssetID)
		}
		for _, ch := range env.Changes {
			if ch.AssetID != "" {
				out = append(out, ch.AssetID)
			}
		}
		if len(out) == 0 && env.EventType != "" {
			// A message with no asset ids (e.g. market_resolved) still counts.
			out = append(out, "\x00"+env.EventType)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────
// Market resolution
// ─────────────────────────────────────────────────────────────

// resolveMarketPair returns two consecutive, currently-active btc_5m markets
// (current+next, falling back to prev+current) with retry, so the test always
// has a streaming market pair.
func resolveMarketPair(ctx context.Context, gamma *polymarket.GammaClient) (a, b *polymarket.GammaMarket, err error) {
	interval := 5 * time.Minute
	for attempt := 0; attempt < 4; attempt++ {
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
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, nil, fmt.Errorf("could not resolve two consecutive active btc_5m markets")
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
	wLabel, wDetail := 34, 66
	sep := func() {
		fmt.Print("+", strings.Repeat("-", wLabel+2), "+",
			strings.Repeat("-", 8), "+",
			strings.Repeat("-", wDetail+2), "+\n")
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
		fmt.Println("=> Incremental subscribe/unsubscribe WORKS. The forced reconnect can be dropped.")
	} else {
		fmt.Println("=> Incremental is NOT reliable on the market channel; the full-handshake reconnect stays.")
	}
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fmt.Println("== Resolving two consecutive active btc_5m markets ==")
	gamma := polymarket.NewGammaClient(gammaURL)
	gmA, gmB, err := resolveMarketPair(ctx, gamma)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌", err)
		os.Exit(1)
	}
	assetA := []string{gmA.YesTokenID, gmA.NoTokenID}
	assetB := []string{gmB.YesTokenID, gmB.NoTokenID}
	fmt.Printf("  A: market %s (%s)\n     yes=%s…\n     no =%s…\n", gmA.ID, gmA.Slug, short(gmA.YesTokenID), short(gmA.NoTokenID))
	fmt.Printf("  B: market %s (%s)\n     yes=%s…\n     no =%s…\n", gmB.ID, gmB.Slug, short(gmB.YesTokenID), short(gmB.NoTokenID))

	fmt.Println("\n== Connecting to market WS ==")
	conn, _, err := coderws.Dial(ctx, marketWSURL, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ dial:", err)
		os.Exit(1)
	}
	defer conn.Close(coderws.StatusNormalClosure, "done")
	fmt.Println("  connected:", marketWSURL)

	c := newCounter()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, msg, err := conn.Read(ctx)
			if err != nil {
				fmt.Println("  [read loop ended]", err)
				return
			}
			c.note(assetIDsIn(msg), "")
		}
	}()

	send := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return conn.Write(ctx, coderws.MessageText, b)
	}

	observe := func(label string, during time.Duration) (int, int) {
		time.Sleep(during)
		a := c.countFor(assetA)
		b := c.countFor(assetB)
		raw, errs, last := c.totals()
		fmt.Printf("    %-46s A=%5d  B=%5d  msgs=%5d errs=%d last=%s\n", label, a, b, raw, errs, last)
		return a, b
	}

	var phases []phase

	// ── P0: full handshake subscribe to A ──────────────────
	fmt.Println("\n== P0  full-handshake subscribe A ==")
	if err := send(subscriptionMessage{AssetsIDs: assetA, Type: "market", CustomFeatureEnabled: true}); err != nil {
		fmt.Fprintln(os.Stderr, "❌ send:", err)
		os.Exit(1)
	}
	c.reset()
	a, b := observe("P0 window (A should flow)", phaseWait)
	phases = append(phases, phase{"P0 full subscribe A -> A flows", a > 0, fmt.Sprintf("A=%d B=%d", a, b)})

	// ── P1: incremental subscribe B over the SAME connection ─
	fmt.Println("\n== P1  incremental subscribe B (operation:\"subscribe\", NO reconnect) ==")
	if err := send(subscribeMessage{Operation: "subscribe", AssetsIDs: assetB}); err != nil {
		fmt.Fprintln(os.Stderr, "❌ send:", err)
		os.Exit(1)
	}
	c.reset()
	a, b = observe("P1 window (A AND B should flow)", phaseWait)
	phases = append(phases, phase{"P1 incremental subscribe B -> B flows", a > 0 && b > 0, fmt.Sprintf("A=%d B=%d", a, b)})

	// ── P2: incremental unsubscribe A over the SAME connection ─
	fmt.Println("\n== P2  incremental unsubscribe A (operation:\"unsubscribe\", NO reconnect) ==")
	if err := send(unsubscribeMessage{Operation: "unsubscribe", AssetsIDs: assetA}); err != nil {
		fmt.Fprintln(os.Stderr, "❌ send:", err)
		os.Exit(1)
	}
	c.reset()
	a, b = observe("P2 window (A should STOP, B flows)", phaseWait)
	phases = append(phases, phase{"P2 incremental unsubscribe A -> A stops", a == 0 && b > 0, fmt.Sprintf("A=%d B=%d", a, b)})

	// ── P3 (diagnostic): handshake-form full replace to B-only ─
	fmt.Println("\n== P3  handshake-form full replace -> B only (replacement semantics) ==")
	if err := send(subscriptionMessage{AssetsIDs: assetB, Type: "market", CustomFeatureEnabled: true}); err != nil {
		fmt.Fprintln(os.Stderr, "❌ send:", err)
		os.Exit(1)
	}
	c.reset()
	a, b = observe("P3 window (A stays silent if replace semantics)", 6*time.Second)
	phases = append(phases, phase{"P3 handshake form replaces set", a == 0 && b > 0, fmt.Sprintf("A=%d B=%d", a, b)})

	fmt.Println("\n== Summary ==")
	printTable(phases)

	select {
	case <-readDone:
	case <-ctx.Done():
	}
}

func short(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16]
}
