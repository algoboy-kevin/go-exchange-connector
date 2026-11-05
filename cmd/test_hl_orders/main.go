// Command test_hl_orders is a gated Hyperliquid testnet check for the order
// path: read-only /info checks, and (only with -live) a tiny post-only order
// far from the book, its cancel by oid and by cloid, and WebSocket
// orderUpdates correlation by oid.
//
// Safety:
//   - Mainnet is refused outright. Only -network testnet is accepted.
//   - Any mutation requires -live AND both HL_TESTNET_AGENT_KEY and
//     HL_TESTNET_ACCOUNT. Missing values are reported, never fabricated.
//   - The key and env values are read by name and never printed. The
//     Polymarket PRIVATE_KEY/FUNDER in .env are never read by this tool.
//   - Orders are post-only (Alo), priced 30% below mid (buy), so they cannot
//     fill. Anything still resting is cancelled on exit.
//
// Usage:
//
//	go run ./cmd/test_hl_orders -network testnet                 # read-only
//	HL_TESTNET_AGENT_KEY=... HL_TESTNET_ACCOUNT=0x... \
//	  go run ./cmd/test_hl_orders -network testnet -live -samples 5
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/algoboy-kevin/go-common/decimal"
	connector "github.com/algoboy-kevin/go-exchange-connector"
	"github.com/algoboy-kevin/go-exchange-connector/internal/hlnet"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid/exchange"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// infoURL is the REST /info endpoint for the selected network, set in run()
// from internal/hlnet before any request is made.
var infoURL string

const (
	minDistance    = 0.20 // order price must be at least 20% from mid
	minNotional    = 10.0 // venue minimum order value, USDC (docs error table)
	notionalTarget = 11.0 // size is sized to this, to clear the minimum with margin
	defaultOffset  = 0.30 // default price: mid * (1 - 0.30) for a buy
)

var addrRE = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

var (
	flagNetwork        = flag.String("network", "testnet", "testnet (default) or mainnet")
	flagLive           = flag.Bool("live", false, "required to send any order or cancel")
	flagConfirmMainnet = flag.Bool("confirm-mainnet", false, "required with -live -network mainnet: places real orders")
	flagCoin           = flag.String("coin", "BTC", "perp coin")
	flagPx             = flag.String("px", "", "limit price (default: mid*0.70, rounded per venue rules)")
	flagSz             = flag.String("sz", "", "size (default: smallest size clearing the $10 minimum)")
	flagTif            = flag.String("tif", "Alo", "Alo|Ioc|Gtc; live mode requires Alo")
	flagSamples        = flag.Int("samples", 3, "live place/cancel round trips")
)

// ---------- read-only /info ----------

// readOnly runs the read-only checks through the connector client. It makes no
// order requests. Returns the size decimals for the coin and whether meta has it.
func readOnly(ctx context.Context, c *hl.Client, env hlnet.Env, account string) (szDecimals int, ok bool) {
	fmt.Printf("== read-only checks (%s /info, %s) ==\n", env.Network, env.AccountVar)
	m, err := c.Meta(ctx)
	if err != nil {
		fmt.Printf("meta: FAIL %v\n", err)
	} else {
		szDecimals, ok = szDecimalsFrom(m, *flagCoin)
		fmt.Printf("meta: OK universe=%d coin=%s found=%v szDecimals=%d\n", len(m.Universe), *flagCoin, ok, szDecimals)
	}
	if !ok {
		fmt.Printf("meta: coin %q not in universe\n", *flagCoin)
	}

	if account == "" {
		fmt.Printf("account reads: SKIPPED (%s not set)\n", env.AccountVar)
		return szDecimals, ok
	}

	// Account mode decides where collateral lives. Unified and portfolio accounts
	// hold one balance in the spot clearinghouse; perps-only numbers are not meaningful.
	mode, err := c.AccountMode(ctx)
	if err != nil {
		fmt.Printf("accountMode: FAIL %v\n", err)
		mode = ""
	} else {
		fmt.Printf("accountMode: %s\n", mode)
	}

	if oo, err := c.OpenOrders(ctx); err != nil {
		fmt.Printf("openOrders: FAIL %v\n", err)
	} else {
		fmt.Printf("openOrders: OK count=%d\n", len(oo))
	}

	if sb, err := c.SpotUSDCBalance(ctx); err != nil {
		fmt.Printf("spotBalances: FAIL %v\n", err)
	} else {
		fmt.Printf("spotBalance: USDC total=%s hold=%s\n", sb.Total, sb.Hold)
	}

	if st, err := c.AccountState(ctx); err != nil {
		fmt.Printf("perpState: FAIL %v\n", err)
	} else {
		note := ""
		if mode == hl.AccountModeUnified {
			note = " (perp view only; collateral is in spot balance under unified mode)"
		}
		fmt.Printf("perpState: accountValue=%s withdrawable=%s%s\n", st.AccountValue, st.Withdrawable, note)
	}
	return szDecimals, ok
}

// szDecimalsFrom returns the size decimals for coin from venue meta.
func szDecimalsFrom(m *hl.Meta, coin string) (int, bool) {
	for _, a := range m.Universe {
		if a.Name == coin {
			return a.SzDecimals, true
		}
	}
	return 0, false
}

// parseFloat returns 0 for unparseable input; callers treat 0 as missing.
func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// ---------- WebSocket event store ----------

// evStore keeps every orderUpdates event, keyed by oid, in arrival order.
// Matching is by oid only: never by "first event on the stream".
type evStore struct {
	mu   sync.Mutex
	byID map[int64][]connector.HyperliquidOrderUpdateEvent
}

func (s *evStore) add(e connector.HyperliquidOrderUpdateEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[e.Oid] = append(s.byID[e.Oid], e)
}

// waitFor returns the first event for oid that satisfies pred, within timeout.
func (s *evStore) waitFor(oid int64, pred func(connector.HyperliquidOrderUpdateEvent) bool, timeout time.Duration) (connector.HyperliquidOrderUpdateEvent, bool) {
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		for _, e := range s.byID[oid] {
			if pred(e) {
				s.mu.Unlock()
				return e, true
			}
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			return connector.HyperliquidOrderUpdateEvent{}, false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------- percentiles ----------

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	return s[max(idx, 0)]
}

func fmtMs(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }

func summarize(name string, ds []time.Duration) {
	if len(ds) == 0 {
		fmt.Printf("%-22s n=0 (no samples)\n", name)
		return
	}
	fmt.Printf("%-22s n=%d p50=%s p95=%s\n", name, len(ds), fmtMs(percentile(ds, 0.50)), fmtMs(percentile(ds, 0.95)))
}

// ---------- live run ----------

type runState struct {
	c           *hl.Client
	store       *evStore
	coin        string
	outstanding map[int64]bool
	mu          sync.Mutex
}

func (r *runState) track(oid int64)   { r.mu.Lock(); r.outstanding[oid] = true; r.mu.Unlock() }
func (r *runState) untrack(oid int64) { r.mu.Lock(); delete(r.outstanding, oid); r.mu.Unlock() }

// cleanup cancels anything this run left resting, on a fresh context.
func (r *runState) cleanup() {
	r.mu.Lock()
	var oids []int64
	for oid := range r.outstanding {
		oids = append(oids, oid)
	}
	r.mu.Unlock()
	if len(oids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var reqs []hl.OidCancel
	for _, oid := range oids {
		reqs = append(reqs, hl.OidCancel{Coin: r.coin, Oid: oid})
	}
	res, err := r.c.CancelByOid(ctx, reqs)
	fmt.Printf("cleanup: cancel %d leftover order(s): err=%v results=%+v\n", len(oids), err, res)
}

func openOrderOids(ctx context.Context, c *hl.Client) (map[int64]bool, error) {
	rows, err := c.OpenOrders(ctx)
	if err != nil {
		return nil, err
	}
	m := make(map[int64]bool, len(rows))
	for _, o := range rows {
		m[o.Oid] = true
	}
	return m, nil
}

func waitRest(ctx context.Context, c *hl.Client, oid int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		m, err := openOrderOids(ctx, c)
		if err == nil && m[oid] {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func isOpen(e connector.HyperliquidOrderUpdateEvent) bool     { return e.Status == "open" }
func isCanceled(e connector.HyperliquidOrderUpdateEvent) bool { return e.Status == "canceled" }

func runLive(ctx context.Context, env hlnet.Env, account, agentKey string, szDec int) int {
	fmt.Printf("\n== live %s order round trips ==\n", env.Network)
	c, err := hl.New(ctx, hl.Config{Network: env.Network, AgentKey: env.AgentKey, Account: env.Account})
	if err != nil {
		fmt.Printf("client FAIL: %v\n", err)
		return 1
	}
	px, sz, err := planOrder(ctx, c, szDec)
	if err != nil {
		fmt.Printf("live not run: %v\n", err)
		return 2
	}
	fmt.Printf("plan: %s BUY post-only(Alo) px=%s sz=%s (notional >= $%.0f)\n", *flagCoin, px, sz, minNotional)

	store := &evStore{byID: map[int64][]connector.HyperliquidOrderUpdateEvent{}}
	base := connector.New(false, nil)
	opts := hyperliquid.DefaultOptions()
	opts.URL = env.WSURL
	h := hyperliquid.NewWithOptions(base, opts)
	h.SetDispatcher(func(ev any) {
		if e, ok := ev.(*connector.HyperliquidOrderUpdateEvent); ok {
			store.add(*e)
		}
	})
	wsCtx, wsCancel := context.WithCancel(ctx)
	defer wsCancel()
	if err := h.Start(wsCtx, 0); err != nil {
		fmt.Printf("ws: FAIL %v\n", err)
		return 1
	}
	defer h.Stop()
	if err := h.SubscribeOrderUpdates(ctx, account); err != nil {
		fmt.Printf("ws subscribe: FAIL %v\n", err)
		return 1
	}
	if !waitStatus(h, ws.StatusConnected, 15*time.Second) {
		fmt.Println("ws: FAIL not connected within 15s")
		return 1
	}
	fmt.Printf("ws: connected to %s, subscribed orderUpdates for master account\n", env.WSURL)

	run := &runState{c: c, store: store, coin: *flagCoin, outstanding: map[int64]bool{}}
	defer run.cleanup()

	var placeSend, placeWS, cancelSend, cancelWS []time.Duration
	failures := 0

	for i := 1; i <= *flagSamples; i++ {
		order := hl.Order{Coin: *flagCoin, IsBuy: true, Px: px, Sz: sz, Tif: hl.TifAlo}
		t0 := time.Now()
		st, err := c.Place(ctx, []hl.Order{order})
		ack := time.Now()
		if err != nil {
			fmt.Printf("sample %d: place FAIL %v\n", i, err)
			return 1
		}
		if len(st) != 1 || st[0].Error != "" || !st[0].Resting || st[0].Oid == 0 {
			fmt.Printf("sample %d: place not resting: %+v\n", i, st)
			failures++
			continue
		}
		oid := st[0].Oid
		run.track(oid)
		if !waitRest(ctx, c, oid, 15*time.Second) {
			fmt.Printf("sample %d: oid %d not seen in openOrders within 15s\n", i, oid)
			failures++
		}
		ev, ok := store.waitFor(oid, isOpen, 10*time.Second)
		if !ok {
			fmt.Printf("sample %d: no orderUpdates(open) for oid %d within 10s\n", i, oid)
		}

		t1 := time.Now()
		cs, err := c.CancelByOid(ctx, []hl.OidCancel{{Coin: *flagCoin, Oid: oid}})
		cack := time.Now()
		if err != nil || len(cs) != 1 || !cs[0].OK {
			fmt.Printf("sample %d: cancel FAIL err=%v status=%+v\n", i, err, cs)
			failures++
			continue
		}
		run.untrack(oid)
		cev, cok := store.waitFor(oid, isCanceled, 10*time.Second)
		if !cok {
			fmt.Printf("sample %d: no orderUpdates(canceled) for oid %d within 10s\n", i, oid)
		}
		open, oerr := openOrderOids(ctx, c)
		gone := oerr == nil && !open[oid]

		placeSend = append(placeSend, ack.Sub(t0))
		cancelSend = append(cancelSend, cack.Sub(t1))
		if ok {
			placeWS = append(placeWS, ev.ReceivedAt.Sub(ack))
		}
		if cok {
			cancelWS = append(cancelWS, cev.ReceivedAt.Sub(cack))
		}
		// A WS latency can be negative: the event may land before the REST ack
		// returns. It is reported as measured, not clamped.
		fmt.Printf("sample %d: oid=%d place send->ack=%s ack->ws(open)=%s | cancel send->ack=%s ack->ws(canceled)=%s | gone-from-openOrders=%v\n",
			i, oid, fmtMs(ack.Sub(t0)), fmtDur(ev, ok, ack), fmtMs(cack.Sub(t1)), fmtDur(cev, cok, cack), gone)
	}

	batchOK := runBatch(ctx, run, store, px, sz)
	cloidOK := runCloid(ctx, run, store, px, sz)

	fmt.Println("\n== latency over samples (ms) ==")
	summarize("place send->ack", placeSend)
	summarize("place ack->ws(open)", placeWS)
	summarize("cancel send->ack", cancelSend)
	summarize("cancel ack->ws(cancel)", cancelWS)
	fmt.Printf("samples=%d failures=%d batch(place2+cancel2)=%v cloid(place+cancelByCloid)=%v\n", *flagSamples, failures, batchOK, cloidOK)
	if failures > 0 || !batchOK || !cloidOK {
		return 1
	}
	return 0
}

func fmtDur(ev connector.HyperliquidOrderUpdateEvent, ok bool, ref time.Time) string {
	if !ok {
		return "NO-EVENT"
	}
	return fmtMs(ev.ReceivedAt.Sub(ref))
}

func waitStatus(h *hyperliquid.WSHyperliquid, want ws.ConnectionStatus, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if h.Status() == want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// planOrder computes the price and size, or validates the flags against the
// 20% distance and $10 minimum rules.
func planOrder(ctx context.Context, c *hl.Client, szDec int) (string, string, error) {
	mids, err := c.Mids(ctx)
	if err != nil {
		return "", "", fmt.Errorf("mid price: %w", err)
	}
	mid := parseFloat(mids[*flagCoin])
	if mid <= 0 {
		return "", "", fmt.Errorf("mid price: no mid for %s", *flagCoin)
	}
	px := *flagPx
	if px == "" {
		px = decimal.Sig(mid*(1-defaultOffset), 5, 6-szDec)
	}
	pxF, err := strconv.ParseFloat(px, 64)
	if err != nil || pxF <= 0 {
		return "", "", fmt.Errorf("invalid -px %q", px)
	}
	if dist := math.Abs(pxF/mid - 1); dist < minDistance {
		return "", "", fmt.Errorf("px %s is %.1f%% from mid %.2f; need >= %.0f%%", px, dist*100, mid, minDistance*100)
	}
	sz := *flagSz
	if sz == "" {
		sz = decimal.CeilStep(notionalTarget/pxF, szDec)
	}
	szF, err := strconv.ParseFloat(sz, 64)
	if err != nil || szF <= 0 {
		return "", "", fmt.Errorf("invalid -sz %q", sz)
	}
	if pxF*szF < minNotional {
		return "", "", fmt.Errorf("notional %.2f < $%.0f minimum", pxF*szF, minNotional)
	}
	return px, sz, nil
}

// runBatch: two post-only orders in one action, then both cancelled by oid in
// one action. Verifies per-item statuses and batch ids.
func runBatch(ctx context.Context, r *runState, store *evStore, px, sz string) bool {
	orders := []hl.Order{
		{Coin: *flagCoin, IsBuy: true, Px: px, Sz: sz, Tif: hl.TifAlo},
		{Coin: *flagCoin, IsBuy: true, Px: px, Sz: sz, Tif: hl.TifAlo},
	}
	st, err := r.c.Place(ctx, orders)
	if err != nil || len(st) != 2 {
		fmt.Printf("batch: place FAIL err=%v n=%d\n", err, len(st))
		return false
	}
	var cancels []hl.OidCancel
	for _, s := range st {
		if s.Error != "" || !s.Resting || s.Oid == 0 {
			fmt.Printf("batch: item not resting: %+v\n", s)
			return false
		}
		r.track(s.Oid)
		cancels = append(cancels, hl.OidCancel{Coin: *flagCoin, Oid: s.Oid})
	}
	cs, err := r.c.CancelByOid(ctx, cancels)
	if err != nil || len(cs) != 2 {
		fmt.Printf("batch: cancel FAIL err=%v n=%d\n", err, len(cs))
		return false
	}
	for i, c := range cs {
		if !c.OK {
			fmt.Printf("batch: cancel item %d not ok: %s\n", i, c.Error)
			return false
		}
		r.untrack(cancels[i].Oid)
	}
	fmt.Printf("batch: 2 placed (oids %d,%d) and 2 cancelled in one action each\n", st[0].Oid, st[1].Oid)
	return true
}

// runCloid: place with a client id, cancel by cloid, confirm gone.
func runCloid(ctx context.Context, r *runState, store *evStore, px, sz string) bool {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		fmt.Printf("cloid: random: %v\n", err)
		return false
	}
	cloid := "0x" + hex.EncodeToString(b)
	st, err := r.c.Place(ctx, []hl.Order{{Coin: *flagCoin, IsBuy: true, Px: px, Sz: sz, Tif: hl.TifAlo, Cloid: cloid}})
	if err != nil || len(st) != 1 || st[0].Error != "" || !st[0].Resting {
		fmt.Printf("cloid: place FAIL err=%v status=%+v\n", err, st)
		return false
	}
	oid := st[0].Oid
	r.track(oid)
	cs, err := r.c.CancelByCloid(ctx, []hl.CancelReq{{Coin: *flagCoin, Cloid: cloid}})
	if err != nil || len(cs) != 1 || !cs[0].OK {
		fmt.Printf("cloid: cancelByCloid FAIL err=%v status=%+v\n", err, cs)
		return false
	}
	r.untrack(oid)
	open, _ := openOrderOids(ctx, r.c)
	fmt.Printf("cloid: placed oid=%d cloid=%s..., cancelled by cloid, gone-from-openOrders=%v\n", oid, cloid[:10], !open[oid])
	return !open[oid]
}

func main() {
	flag.Parse()
	os.Exit(run())
}

func run() int {
	env, err := hlnet.Resolve(*flagNetwork)
	if err != nil {
		fmt.Printf("REFUSED: %v\n", err)
		return 2
	}
	if env.Network == "mainnet" && *flagLive && !*flagConfirmMainnet {
		fmt.Println("REFUSED: mainnet live orders need -confirm-mainnet (this places real orders on production).")
		return 2
	}
	infoURL = env.InfoURL
	if tif := *flagTif; tif != hl.TifAlo && tif != hl.TifIoc && tif != hl.TifGtc {
		fmt.Printf("invalid -tif %q (want Alo, Ioc or Gtc)\n", tif)
		return 2
	}
	if *flagLive && *flagTif != hl.TifAlo {
		fmt.Println("REFUSED: -live requires -tif Alo (post-only), so the order cannot take liquidity.")
		return 2
	}
	if *flagSamples < 1 {
		*flagSamples = 1
	}

	agentKey := env.AgentKey
	account := env.Account
	fmt.Printf("network=%s live=%v coin=%s samples=%d\n", env.Network, *flagLive, *flagCoin, *flagSamples)
	fmt.Printf("env %s set=%v; env %s set=%v (values never printed)\n", env.AgentVar, agentKey != "", env.AccountVar, account != "")
	if account != "" && !hlnet.ValidAddr(account) {
		fmt.Printf("%s is not a 0x + 40 hex address; no account reads or orders.\n", env.AccountVar)
		account = ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	c, err := hl.New(ctx, hl.Config{Network: env.Network, AgentKey: agentKey, Account: account})
	if err != nil {
		fmt.Printf("client FAIL: %v\n", err) // ConfigError text carries shape only
		return 1
	}
	szDec, metaOK := readOnly(ctx, c, env, account)
	if !*flagLive {
		if agentKey == "" {
			fmt.Println("live not run: key absent (HL_TESTNET_AGENT_KEY not set); no -live requested")
		} else {
			fmt.Println("live not run: -live not given (read-only mode)")
		}
		return 0
	}
	if agentKey == "" {
		fmt.Println("live not run: key absent (HL_TESTNET_AGENT_KEY not set). Nothing sent.")
		return 3
	}
	if account == "" {
		fmt.Println("live not run: HL_TESTNET_ACCOUNT absent or invalid. Nothing sent.")
		return 3
	}
	if !metaOK {
		fmt.Printf("live not run: coin missing from %s meta; refusing to guess a size grid.\n", env.Network)
		return 3
	}
	return runLive(ctx, env, account, agentKey, szDec)
}
