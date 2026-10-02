package main

// Command test_market_stability reproduces the collector's rolling-market
// subscription pattern on a RAW coder/websocket connection to Polymarket's
// market channel, and reports whether the stream stalls.
//
// Motivation: the production collector (trading-core) showed the market WS
// force-reconnecting every ~30s on a Digital Ocean server after the first
// market rotation (data stopped ~11s after Subscribe(next); even a full
// reconnect + handshake did NOT restore data flow). This harness mirrors that
// exact sequence at the raw-protocol level so we can see what the server
// actually sends (price_change, new_market broadcasts, empty heartbeats) and
// WHEN it stops.
//
// Phases (boundary-aware: R1/R2 straddle the 5m boundary so B is actually live):
//
//	R0  handshake subscribe to A (current window)          -> expect A to flow
//	R1  hold until boundary−15s, INCREMENTAL subscribe B   -> KEY: does B start flowing at the
//	    boundary while A (now expired) stops? collector died ~11s after this step.
//	R2  INCREMENTAL unsubscribe A (B only)                 -> expect B to keep flowing
//	R3  full reconnect: close + redial + handshake B-only  -> does B still flow post-reconnect?
//
// Usage (no auth needed):
//
//	go run ./cmd/test_market_stability/            # full rotation, 45s/phase
//	go run ./cmd/test_market_stability/ -dur 0     # R0 only (quick single-market check)
//	go run ./cmd/test_market_stability/ -phases R0,R1

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
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
)

// ─────────────────────────────────────────────────────────────
// Wire message types (mirror of the connector's unexported structs)
// ─────────────────────────────────────────────────────────────

type subscriptionMessage struct {
	AssetsIDs            []string `json:"assets_ids"`
	Type                 string   `json:"type"`
	CustomFeatureEnabled bool     `json:"custom_feature_enabled"`
}

type opMessage struct {
	Operation string   `json:"operation"`
	AssetsIDs []string `json:"assets_ids"`
}

// ─────────────────────────────────────────────────────────────
// Observer — counts raw messages, types, per-asset events, stalls
// ─────────────────────────────────────────────────────────────

type observer struct {
	mu sync.Mutex

	rawMsgs      int            // every frame received (incl. empty)
	nonEmptyMsgs int            // frames with len>0
	emptyMsgs    int            // zero-length heartbeat frames
	types        map[string]int // event_type histogram ("" = no event_type)
	byAsset      map[string]int // assetID -> event count (asset_id or changes[].asset_id)
	lastNonEmpty time.Time      // wall time of last len>0 frame

	stalls int // times the stream went >30s without a non-empty frame
}

func newObserver() *observer {
	return &observer{types: map[string]int{}, byAsset: map[string]int{}}
}

func (o *observer) observe(raw []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rawMsgs++
	if len(raw) == 0 {
		o.emptyMsgs++
		return
	}
	o.nonEmptyMsgs++
	o.lastNonEmpty = time.Now()

	for _, env := range eventTypesIn(raw) {
		o.types[env]++
	}
	for _, id := range assetIDsIn(raw) {
		o.byAsset[id]++
	}
}

func (o *observer) snapshot() obsSnap {
	o.mu.Lock()
	defer o.mu.Unlock()
	return obsSnap{
		raw:          o.rawMsgs,
		nonEmpty:     o.nonEmptyMsgs,
		empty:        o.emptyMsgs,
		types:        copyMap(o.types),
		byAsset:      copyMap(o.byAsset),
		stalls:       o.stalls,
		lastNonEmpty: o.lastNonEmpty,
	}
}

func (o *observer) countsFor(ids []string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, id := range ids {
		n += o.byAsset[id]
	}
	return n
}

type obsSnap struct {
	raw, nonEmpty, empty, stalls int
	types                        map[string]int
	byAsset                      map[string]int
	lastNonEmpty                 time.Time
}

func copyMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// eventTypesIn extracts the event_type of every envelope in a frame.
func eventTypesIn(raw []byte) []string {
	var docs []json.RawMessage
	if err := json.Unmarshal(raw, &docs); err != nil {
		docs = []json.RawMessage{raw}
	}
	var out []string
	for _, d := range docs {
		var env struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(d, &env); err != nil {
			continue
		}
		if env.EventType == "" {
			env.EventType = "(none)"
		}
		out = append(out, env.EventType)
	}
	return out
}

// assetIDsIn extracts asset IDs referenced by a frame (asset_id or
// price_changes[].asset_id), falling back to a marker for event-only frames.
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
			} `json:"price_changes"`
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
			out = append(out, "["+env.EventType+"]")
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────
// Market resolution (same as test_incr)
// ─────────────────────────────────────────────────────────────

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

func short(s string) string {
	if len(s) <= 14 {
		return s
	}
	return s[:14]
}

// ─────────────────────────────────────────────────────────────
// Connection helper
// ─────────────────────────────────────────────────────────────

func dial(ctx context.Context) (*coderws.Conn, error) {
	conn, _, err := coderws.Dial(ctx, marketWSURL, nil)
	return conn, err
}

func sendHandshake(ctx context.Context, conn *coderws.Conn, ids []string) error {
	msg := subscriptionMessage{AssetsIDs: ids, Type: "market", CustomFeatureEnabled: true}
	data, _ := json.Marshal(msg)
	return conn.Write(ctx, coderws.MessageText, data)
}

func sendOp(ctx context.Context, conn *coderws.Conn, op string, ids []string) error {
	msg := opMessage{Operation: op, AssetsIDs: ids}
	data, _ := json.Marshal(msg)
	return conn.Write(ctx, coderws.MessageText, data)
}

// ─────────────────────────────────────────────────────────────
// Phase runner
// ─────────────────────────────────────────────────────────────

func observe(ctx context.Context, conn *coderws.Conn, obs *observer, label string, dur time.Duration, idsA, idsB []string, send func() error) {
	fmt.Printf("\n═══ %s (obs %s) ═══\n", label, dur.Round(time.Second))
	if send != nil {
		if err := send(); err != nil {
			fmt.Printf("  ❌ send failed: %v\n", err)
			return
		}
	}

	deadline := time.After(dur)
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			printSnap(obs, start, idsA, idsB, true)
			return
		case <-tick.C:
			printSnap(obs, start, idsA, idsB, false)
		}
	}
}

func printSnap(obs *observer, start time.Time, idsA, idsB []string, final bool) {
	s := obs.snapshot()
	a := obs.countsFor(idsA)
	b := obs.countsFor(idsB)
	age := time.Since(s.lastNonEmpty)
	if s.nonEmpty > 0 && age > 30*time.Second {
		obs.mu.Lock()
		obs.stalls++
		obs.mu.Unlock()
	}
	m := " "
	if final {
		m = "■"
	}
	fmt.Printf("%s [%4.0fs] %s A=%d B=%d raw=%d types=%v stall=%s\n",
		m, time.Since(start).Seconds(), time.Now().UTC().Format("15:04:05"),
		a, b, s.raw, s.types, age.Round(time.Second))
}

// ─────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────

func main() {
	dur := flag.Duration("dur", 45*time.Second, "observation duration per phase (0 = R0 only)")
	phasesFlag := flag.String("phases", "R0,R1,R2,R3", "comma-separated phases to run")
	soloSlug := flag.String("solo", "", "probe a single market by slug (subscribe only it, observe -dur)")
	flag.Parse()

	want := map[string]bool{}
	for _, p := range strings.Split(*phasesFlag, ",") {
		want[strings.TrimSpace(p)] = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if *soloSlug != "" {
		runSolo(ctx, *soloSlug, *dur)
		return
	}

	fmt.Println("== Resolving current + next btc_5m markets ==")
	gmA, gmB, err := resolveMarketPair(ctx, polymarket.NewGammaClient(gammaURL))
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌", err)
		os.Exit(1)
	}
	assetA := []string{gmA.YesTokenID, gmA.NoTokenID}
	assetB := []string{gmB.YesTokenID, gmB.NoTokenID}
	fmt.Printf("  A: %s yes=%s… no=%s…\n", gmA.Slug, short(gmA.YesTokenID), short(gmA.NoTokenID))
	fmt.Printf("  B: %s yes=%s… no=%s…\n", gmB.Slug, short(gmB.YesTokenID), short(gmB.NoTokenID))
	nextBoundary := time.Unix(slugEpoch(gmB.Slug), 0).UTC()
	toB := time.Until(nextBoundary)
	fmt.Printf("  B (next) window start %s (in %s)\n", nextBoundary.Format("15:04:05"), toB.Round(time.Second))

	if *dur <= 0 {
		*dur = 45 * time.Second // default even in R0-only quick mode
	}
	fmt.Printf("  per-phase observation: %v\n", *dur)

	conn, err := dial(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ dial:", err)
		os.Exit(1)
	}
	defer conn.CloseNow()

	obs := newObserver()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, msg, err := conn.Read(ctx)
			if err != nil {
				fmt.Printf("  ⚠ read ended: %v\n", err)
				return
			}
			obs.observe(msg)
		}
	}()

	if want["R0"] {
		r0 := time.Duration(*dur)
		if keep := toB - 15*time.Second; keep > 0 && keep < r0 {
			r0 = keep
		}
		if r0 > 0 {
			observe(ctx, conn, obs, "R0 handshake subscribe A (current, active)", r0, assetA, assetB,
				func() error { return sendHandshake(ctx, conn, assetA) })
		}
	}

	if want["R1"] {
		// Hold until 15s before the boundary (B's window start), then subscribe B.
		if wait := toB - 15*time.Second; wait > 0 {
			fmt.Printf("\n  …hold %s until boundary−15s (B's window opens)…\n", wait.Round(time.Second))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
		observe(ctx, conn, obs, "R1 INCREMENTAL subscribe B (cross boundary — KEY)", *dur+30*time.Second,
			assetA, assetB, func() error { return sendOp(ctx, conn, "subscribe", assetB) })
	}

	if want["R2"] {
		observe(ctx, conn, obs, "R2 INCREMENTAL unsubscribe A (B only)", *dur, assetA, assetB,
			func() error { return sendOp(ctx, conn, "unsubscribe", assetA) })
	}

	if want["R3"] {
		fmt.Printf("\n═══ R3  full reconnect: close + redial + handshake B-only ═══\n")
		conn.Close(coderws.StatusNormalClosure, "phase")
		<-readDone
		newConn, err := dial(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "  ❌ redial:", err)
			os.Exit(1)
		}
		conn = newConn
		obs = newObserver()
		readDone = make(chan struct{})
		go func() {
			defer close(readDone)
			for {
				_, msg, err := conn.Read(ctx)
				if err != nil {
					fmt.Printf("  ⚠ read ended: %v\n", err)
					return
				}
				obs.observe(msg)
			}
		}()
		observe(ctx, conn, obs, "R3 handshake subscribe B only (post-reconnect)", *dur+30*time.Second,
			assetA, assetB, func() error { return sendHandshake(ctx, conn, assetB) })
	}

	// Final summary
	s := obs.snapshot()
	fmt.Printf("\n== Summary ==\n")
	fmt.Printf("  A events: %d  B events: %d\n", obs.countsFor(assetA), obs.countsFor(assetB))
	fmt.Printf("  raw=%d nonempty=%d empty=%d type_hist=%v stalls(>30s)=%d\n",
		s.raw, s.nonEmpty, s.empty, s.types, s.stalls)
}

// slugEpoch parses the unix window-start timestamp out of a btc_5m slug
// (e.g. "btc-updown-5m-1787728800" -> 1787728800).
func slugEpoch(slug string) int64 {
	n, err := strconv.ParseInt(strings.TrimPrefix(slug, "btc-updown-5m-"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// runSolo subscribes to a single market by slug and observes its raw flow,
// to check whether an active market's price data is actually delivered.
func runSolo(ctx context.Context, slug string, dur time.Duration) {
	fmt.Printf("== Solo probe: %s ==\n", slug)
	gamma := polymarket.NewGammaClient(gammaURL)
	gm, err := gamma.FetchMarketBySlug(slug)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌", err)
		os.Exit(1)
	}
	ids := []string{gm.YesTokenID, gm.NoTokenID}
	fmt.Printf("  yes=%s… no=%s…\n", short(gm.YesTokenID), short(gm.NoTokenID))
	if dur <= 0 {
		dur = 60 * time.Second
	}

	conn, err := dial(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "❌ dial:", err)
		os.Exit(1)
	}
	defer conn.CloseNow()

	obs := newObserver()
	go func() {
		for {
			_, msg, err := conn.Read(ctx)
			if err != nil {
				fmt.Printf("  ⚠ read ended: %v\n", err)
				return
			}
			obs.observe(msg)
		}
	}()

	observe(ctx, conn, obs, "SOLO handshake subscribe "+slug, dur, ids, nil,
		func() error { return sendHandshake(ctx, conn, ids) })

	s := obs.snapshot()
	fmt.Printf("\n== Solo summary ==\n")
	fmt.Printf("  market events: %d  raw=%d empty=%d type_hist=%v stalls(>30s)=%d\n",
		obs.countsFor(ids), s.raw, s.empty, s.types, s.stalls)
}
