// Command test_hyperliquid validates the Hyperliquid market-data streams and
// doubles as the probe for the open questions in PERP_COLLECTOR_SPEC.md §8.
//
// It connects to the venue's public WebSocket, subscribes to the requested
// channels for the requested coins, prints each dispatched event, and reports
// per-channel statistics measured on the RAW frames (frame rate, bytes/s,
// largest frame) — which is what decides droplet size, whether 1 MiB of read
// limit is enough, and how much of the volume bbo really is.
//
// Usage:
//
//	# l2Book + trades + bbo + activeAssetCtx for 5 coins, 60s:
//	go run ./cmd/test_hyperliquid -coins BTC,ETH,SOL,HYPE,DOGE -duration 60s
//
//	# Focused probes:
//	go run ./cmd/test_hyperliquid -coins BTC -channels bbo -duration 60s
//	go run ./cmd/test_hyperliquid -info                 # /info meta + perpDexs only
//	go run ./cmd/test_hyperliquid -coins BTC -fast -duration 30s
//
// What the report answers:
//
//	§8.1 subscription frame shape   frames arrive at all ⇒ method+subscription is right
//	§8.2 app-level ping             a run longer than the ping interval that never reconnects
//	§8.3 bbo frame rate             frames/s per channel (bbo is the volume wildcard)
//	§8.5 ReadLimit headroom         "largest frame" vs the 1 MiB limit
//	§8.6 trades[].side encoding     the distinct side values observed
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// channelStat accumulates the raw-frame measurements for one channel.
type channelStat struct {
	frames    int64
	bytes     int64
	maxFrame  int
	maxKey    string // coin/interval of the largest frame
	sample    []byte
	sampleAt  time.Time
	firstSeen time.Time
	lastSeen  time.Time
}

func (s *channelStat) observe(key string, n int, raw []byte, rx time.Time) {
	s.frames++
	s.bytes += int64(n)
	if n > s.maxFrame {
		s.maxFrame = n
		s.maxKey = key
	}
	if s.sample == nil {
		s.sample = append([]byte(nil), raw...)
		s.sampleAt = rx
	}
	if s.firstSeen.IsZero() {
		s.firstSeen = rx
	}
	s.lastSeen = rx
}

type stats struct {
	mu       sync.Mutex
	channels map[string]*channelStat
	keys     map[string]map[string]struct{} // channel → coin keys seen
	sides    map[string]int64               // trades side values observed
	errors   []string
}

func newStats() *stats {
	return &stats{
		channels: make(map[string]*channelStat),
		keys:     make(map[string]map[string]struct{}),
		sides:    make(map[string]int64),
	}
}

// observe is called on the read-loop goroutine, so it stays cheap: one shallow
// decode of the envelope plus map updates under a lock.
func (s *stats) observe(raw []byte, rx time.Time) {
	var env struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.channels[env.Channel]
	if st == nil {
		st = &channelStat{}
		s.channels[env.Channel] = st
	}
	key := keyOf(env.Channel, env.Data)
	if env.Channel == "trades" {
		var trades []struct {
			Side string `json:"side"`
		}
		if json.Unmarshal(env.Data, &trades) == nil {
			for _, t := range trades {
				s.sides[t.Side]++
			}
		}
	}
	if env.Channel == "error" {
		s.errors = append(s.errors, string(raw))
	}
	seen := s.keys[env.Channel]
	if seen == nil {
		seen = make(map[string]struct{})
		s.keys[env.Channel] = seen
	}
	seen[key] = struct{}{}
	st.observe(key, len(raw), raw, rx)
}

// keyOf extracts the coin (+interval) a payload belongs to; an empty string for
// broadcast channels.
func keyOf(channel string, data json.RawMessage) string {
	probe := struct {
		Coin     string `json:"coin"`
		Symbol   string `json:"s"` // candle uses "s" for the coin
		Interval string `json:"i"`
		Mids     any    `json:"mids"`
	}{}
	if err := json.Unmarshal(data, &probe); err != nil {
		// trades is an array: peek at its first element.
		var arr []struct {
			Coin string `json:"coin"`
		}
		if json.Unmarshal(data, &arr) == nil && len(arr) > 0 {
			return arr[0].Coin
		}
		return ""
	}
	if probe.Mids != nil {
		return "all coins"
	}
	coin := probe.Coin
	if coin == "" {
		coin = probe.Symbol
	}
	if probe.Interval != "" {
		return coin + "/" + probe.Interval
	}
	return coin
}

func (s *stats) report(period time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println("\n── per-channel raw-frame stats ──")
	names := make([]string, 0, len(s.channels))
	for name := range s.channels {
		names = append(names, name)
	}
	sortStrings(names)
	if len(names) == 0 {
		fmt.Println("  (no frames received)")
	}
	for _, name := range names {
		st := s.channels[name]
		window := st.lastSeen.Sub(st.firstSeen).Seconds()
		if window <= 0 {
			window = 1
		}
		rate := float64(st.frames) / window
		fmt.Printf("  %-18s frames=%-7d %.2f frames/s  %.1f KiB/s  avg=%.0f B  max=%d B (%s)\n",
			name, st.frames, rate,
			float64(st.bytes)/window/1024,
			float64(st.bytes)/float64(st.frames),
			st.maxFrame, st.maxKey)
		keys := make([]string, 0, len(s.keys[name]))
		for k := range s.keys[name] {
			if k != "" {
				keys = append(keys, k)
			}
		}
		sortStrings(keys)
		if len(keys) > 0 {
			fmt.Printf("      keys: %s\n", strings.Join(keys, ", "))
		}
		if st.sample != nil && *showSample {
			fmt.Printf("      sample: %s\n", truncate(string(st.sample), 600))
		}
	}

	if len(s.sides) > 0 {
		parts := make([]string, 0, len(s.sides))
		for side, n := range s.sides {
			parts = append(parts, fmt.Sprintf("%q×%d", side, n))
		}
		sortStrings(parts)
		fmt.Printf("\n  trades side encoding: %s\n", strings.Join(parts, ", "))
	} else {
		fmt.Println("\n  trades side encoding: (no trades seen)")
	}

	if len(s.errors) > 0 {
		fmt.Println("\n── venue error frames ──")
		for _, e := range s.errors {
			fmt.Printf("  %s\n", truncate(e, 300))
		}
	}

	fmt.Printf("\n  raw frames: %d over %s\n", s.total(), period.Round(time.Millisecond))
}

func (s *stats) total() int64 {
	var n int64
	for _, st := range s.channels {
		n += st.frames
	}
	return n
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j] < ss[j-1]; j-- {
			ss[j], ss[j-1] = ss[j-1], ss[j]
		}
	}
}

var (
	showSample = flag.Bool("sample", true, "Print the first raw frame seen per channel")
	counters   = flag.Bool("events", false, "Also print every dispatched typed event")
)

func main() {
	coinsFlag := flag.String("coins", "BTC", "Comma-separated coins (e.g. BTC,ETH,HYPE)")
	channelsFlag := flag.String("channels", "l2Book,trades,bbo,activeAssetCtx",
		"Comma-separated channels: l2Book,trades,bbo,activeAssetCtx,allMids,candle")
	intervalFlag := flag.String("candle-interval", "1m", "Candle interval (for the candle channel)")
	fastFlag := flag.Bool("fast", false, "l2Book: request 5 levels instead of 20")
	infoFlag := flag.Bool("info", false, "Only exercise the REST /info client (meta, perpDexs)")
	infoURLFlag := flag.String("info-url", "", "Override the /info URL")
	statsFlag := flag.Duration("stats", 0, "Print the stats report every N (0 = only at the end)")
	durationFlag := flag.Duration("duration", 15*time.Second, "How long to stream")
	verbose := flag.Bool("verbose", false, "Enable debug logging")
	flag.Parse()

	if *verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *durationFlag)
	defer cancel()

	if *infoFlag {
		checkInfo(ctx, *infoURLFlag)
		return
	}

	coins := splitList(*coinsFlag)
	channels := splitList(*channelsFlag)
	if len(coins) == 0 || len(channels) == 0 {
		fmt.Fprintln(os.Stderr, "❌ -coins and -channels must not be empty")
		os.Exit(1)
	}

	st := newStats()

	base := connector.New(false, nil)
	hl := hyperliquid.New(base)
	hl.SetRawFrameHandler(st.observe)
	hl.SetDispatcher(func(ev any) {
		if !*counters {
			return
		}
		printEvent(ev)
	})
	hl.SetOnStatusChange(func(status ws.ConnectionStatus) {
		fmt.Printf("[HL] connection: %s\n", status)
	})

	if err := hl.Start(ctx, 0); err != nil {
		fmt.Fprintf(os.Stderr, "❌ failed to start hyperliquid: %v\n", err)
		os.Exit(1)
	}
	defer hl.Stop()

	params := hyperliquid.SubParams{Fast: *fastFlag}
	for _, ch := range channels {
		switch hyperliquid.Channel(ch) {
		case hyperliquid.ChannelL2Book:
			must(hl.SubscribeL2Book(ctx, coins, params))
		case hyperliquid.ChannelTrades:
			must(hl.SubscribeTrades(ctx, coins))
		case hyperliquid.ChannelBBO:
			must(hl.SubscribeBBO(ctx, coins))
		case hyperliquid.ChannelActiveAssetCtx:
			must(hl.SubscribeActiveAssetCtx(ctx, coins))
		case hyperliquid.ChannelAllMids:
			must(hl.SubscribeAllMids(ctx))
		case hyperliquid.ChannelCandle:
			must(hl.SubscribeCandles(ctx, coins, *intervalFlag))
		default:
			fmt.Fprintf(os.Stderr, "❌ unknown channel %q (subscribable: %s)\n",
				ch, joinChannels(hyperliquid.SubscribableChannels()))
			os.Exit(1)
		}
	}
	fmt.Printf("[HL] subscribed %d subscription(s), streaming for %s…\n",
		len(hl.Subscriptions()), durationFlag.String())

	if *statsFlag > 0 {
		ticker := time.NewTicker(*statsFlag)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					st.report(*statsFlag)
				}
			}
		}()
	}

	<-ctx.Done()
	// Give queued frames a moment to be parsed and dispatched before reporting.
	time.Sleep(300 * time.Millisecond)
	st.report(*durationFlag)
}

// checkInfo exercises the REST client: the verbatim meta/perpDexs dumps a
// recorder makes, plus the typed decodes.
func checkInfo(ctx context.Context, url string) {
	info := hyperliquid.NewInfoClient(url)

	raw, err := info.MetaRaw(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ meta: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[HL] /info meta: %d bytes, valid JSON: %v\n", len(raw), json.Valid(raw))

	meta, err := info.Meta(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ meta (typed): %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[HL] universe: %d perps\n", len(meta.Universe))
	for i, a := range meta.Universe {
		if i < 5 {
			fmt.Printf("      [%d] %-10s szDecimals=%d maxLeverage=%d\n", i, a.Name, a.SzDecimals, a.MaxLeverage)
		}
	}
	if len(meta.Universe) > 5 {
		fmt.Printf("      … %d more\n", len(meta.Universe)-5)
	}

	if dexs, err := info.PerpDexs(ctx); err == nil {
		fmt.Printf("[HL] perp dexs: %d entries (first is the default, nil on the wire)\n", len(dexs))
	} else {
		fmt.Printf("[HL] perp dexs: %v\n", err)
	}

	ctxs, err := info.MetaAndAssetCtxs(ctx)
	if err != nil {
		fmt.Printf("[HL] metaAndAssetCtxs: %v\n", err)
		return
	}
	if funding, ok := ctxs.FundingRate("BTC"); ok {
		fmt.Printf("[HL] BTC funding: %s, mark: %s\n", funding, mustContext(ctxs, "BTC").MarkPx)
	}
}

func mustContext(m *hyperliquid.MetaAndAssetCtxs, coin string) *hyperliquid.AssetCtx {
	ctx, _ := m.CoinContext(coin)
	if ctx == nil {
		return &hyperliquid.AssetCtx{}
	}
	return ctx
}

func printEvent(ev any) {
	switch e := ev.(type) {
	case *connector.HyperliquidBookEvent:
		fmt.Printf("[HL] l2Book %s bids=%d asks=%d best_bid=%s best_ask=%s\n",
			e.Coin, len(e.Bids), len(e.Asks), firstPrice(e.Bids), firstPrice(e.Asks))
	case *connector.HyperliquidTradeEvent:
		fmt.Printf("[HL] trade %s side=%s px=%s sz=%s tid=%d\n", e.Coin, e.Side, e.Price, e.Size, e.TradeID)
	case *connector.HyperliquidBBOEvent:
		fmt.Printf("[HL] bbo %s bid=%s ask=%s\n", e.Coin, levelPrice(e.Bid), levelPrice(e.Ask))
	case *connector.HyperliquidAssetCtxEvent:
		fmt.Printf("[HL] ctx %s funding=%s mark=%s oracle=%s oi=%s\n",
			e.Coin, e.Funding, e.MarkPx, e.OraclePx, e.OpenInterest)
	case *connector.HyperliquidAllMidsEvent:
		fmt.Printf("[HL] allMids coins=%d\n", len(e.Mids))
	case *connector.HyperliquidCandleEvent:
		fmt.Printf("[HL] candle %s/%s O=%s H=%s L=%s C=%s V=%s n=%d\n",
			e.Coin, e.Interval, e.Open, e.High, e.Low, e.Close, e.Volume, e.TradeCount)
	case *connector.HyperliquidErrorEvent:
		fmt.Printf("[HL] ERROR %s\n", e.Message)
	}
}

func firstPrice(levels []connector.HyperliquidLevel) string {
	if len(levels) == 0 {
		return "-"
	}
	return levels[0].Price
}

func levelPrice(l *connector.HyperliquidLevel) string {
	if l == nil {
		return "-"
	}
	return l.Price
}

func joinChannels(chs []hyperliquid.Channel) string {
	parts := make([]string, len(chs))
	for i, c := range chs {
		parts[i] = string(c)
	}
	return strings.Join(parts, ", ")
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}
}
