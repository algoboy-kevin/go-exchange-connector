// Command test_binance_spot_orders is a gated Binance SPOT order checker and
// latency probe. Network is testnet (default) or mainnet.
//
// Without -live (default) it sends nothing signed and reads no keys: public
// exchangeInfo (symbol filters) and public bookTicker, then it prints the
// planned order (post-only LIMIT_MAKER BUY 20% below mid) and
// "dry run: -live not given. Nothing sent.".
//
// With -live it refuses unless the symbol has no open orders and the quote
// balance covers the order. It then, in this order:
//
//  1. POST /api/v3/order/test with the planned order (validates signature,
//     recvWindow and the filters; nothing is placed),
//  2. starts the user-data stream (WS API userDataStream.subscribe.signature),
//  3. per sample: places the LIMIT_MAKER BUY far below the book with a unique
//     newClientOrderId, checks it in openOrders, waits for the executionReport
//     NEW (matched by order id only), cancels by origClientOrderId, waits for
//     the executionReport CANCELED, and checks the order is gone.
//
// Every exit path after the preconditions runs a deferred cancel-all on the
// symbol (skipped after an HTTP 418 ban).
//
// Gates and exits:
//
//	0  all checks PASS (or the dry run is OK)
//	1  a check FAILed or a fatal error
//	2  REFUSED: -live on mainnet without -confirm-mainnet; or open orders /
//	   insufficient quote balance on the symbol
//	3  REFUSED: -live and a key variable is unset (its NAME is printed)
//
// Keys come only from the environment, through internal/bnnet:
// BINANCE_SPOT_{TESTNET,MAINNET}_API_{KEY,SECRET}. Values are never printed.
//
// Safety: an UNKNOWN result (spot.IsUncertain) is reconciled through
// openOrders and never resent; HTTP 429 stops the run; there is no retry.
// Spot has no batch endpoint, so there is no batch step.
//
// Usage:
//
//	go run ./cmd/test_binance_spot_orders                                  # dry run, testnet host
//	BINANCE_SPOT_TESTNET_API_KEY=... BINANCE_SPOT_TESTNET_API_SECRET=... \
//	  go run ./cmd/test_binance_spot_orders -live -samples 3
//	go run ./cmd/test_binance_spot_orders -network mainnet -live -confirm-mainnet
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/internal/bnnet"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/spot"
)

const (
	planDistanceNum = 80  // planned price = mid * 80 / 100 (20% below mid)
	planDistanceDen = 100 //
	minNotionalUSD  = "6" // floor on top of the symbol minimum
	notionalMargin  = 11  // target = max(min, floor) * 11 / 10
	maxNotionalUSD  = "15"
	wsWait          = 10 * time.Second
	restWait        = 15 * time.Second
)

var (
	flagNetwork        = flag.String("network", "testnet", "testnet or mainnet")
	flagLive           = flag.Bool("live", false, "required to send anything signed (order test, orders, cancels)")
	flagConfirmMainnet = flag.Bool("confirm-mainnet", false, "required with -live -network mainnet")
	flagSymbol         = flag.String("symbol", "BTCUSDT", "symbol, e.g. BTCUSDT")
	flagSamples        = flag.Int("samples", 1, "live place/cancel round trips")
	flagVerbose        = flag.Bool("verbose", false, "print extra detail for each step")
)

func main() {
	flag.Parse()
	os.Exit(run())
}

// ---------- helpers ----------

func fmtMs(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000) }

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	return s[max(idx, 0)]
}

func summarize(name string, ds []time.Duration) {
	if len(ds) == 0 {
		fmt.Printf("%-24s n=0 (no samples)\n", name)
		return
	}
	fmt.Printf("%-24s n=%d p50=%s p95=%s\n", name, len(ds), fmtMs(percentile(ds, 0.50)), fmtMs(percentile(ds, 0.95)))
}

func vlog(format string, args ...any) {
	if *flagVerbose {
		fmt.Printf("  [v] "+format+"\n", args...)
	}
}

// ---------- check bookkeeping ----------

type app struct {
	c        *spot.Client
	symbol   string
	quote    string
	network  spot.Network
	store    *evStore
	failures int
	banned   bool // HTTP 418 seen: no further requests, not even cleanup
}

func (a *app) check(name string, ok bool, detail string) bool {
	sfx := ""
	if detail != "" {
		sfx = " — " + detail
	}
	if ok {
		fmt.Printf("PASS %s%s\n", name, sfx)
	} else {
		a.failures++
		fmt.Printf("FAIL %s%s\n", name, sfx)
	}
	return ok
}

// noteErr records rate-limit state: 418 sets banned (no further requests);
// 429 is reported and the caller stops. There is no retry loop in this tool.
func (a *app) noteErr(err error) {
	switch {
	case spot.IsBanned(err):
		a.banned = true
		fmt.Println("HTTP 418: IP ban. Aborting; no further requests will be sent.")
	case spot.IsRateLimited(err):
		fmt.Println("HTTP 429: rate limited. Stopping; no retry.")
	}
}

// ---------- user-stream event store ----------

type wsEvent struct {
	execType string
	status   string
	at       time.Time
}

// evStore keeps executionReports keyed by order id. Matching is by order id
// only, so an unrelated event is never accepted.
type evStore struct {
	mu   sync.Mutex
	byID map[int64][]wsEvent
}

func newStore() *evStore { return &evStore{byID: map[int64][]wsEvent{}} }

func (s *evStore) add(oid int64, e wsEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[oid] = append(s.byID[oid], e)
}

func (s *evStore) waitFor(ctx context.Context, oid int64, pred func(wsEvent) bool, timeout time.Duration) (wsEvent, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		s.mu.Lock()
		for _, e := range s.byID[oid] {
			if pred(e) {
				s.mu.Unlock()
				return e, true
			}
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return wsEvent{}, false
		case <-deadline.C:
			return wsEvent{}, false
		case <-tick.C:
		}
	}
}

func isNewEv(e wsEvent) bool { return e.execType == spot.ExecNew && e.status == spot.StatusNew }
func isCancelEv(e wsEvent) bool {
	return e.execType == spot.ExecCanceled && e.status == spot.StatusCanceled
}

// ---------- plan ----------

// plan is the order this tool would send, computed with exact decimals.
type plan struct {
	price, qty, notional string
	rules                spot.Rules
	quote                string
}

// buildPlan: price = 80% of mid rounded DOWN to tickSize (further from mid);
// qty = target notional / price rounded UP to stepSize, where target is
// max(symbol minNotional, 6 USD) + 10%. It fails when the result would break
// minQty/maxQty/minPrice/maxPrice, the percent-price floor, or exceed 15 USD.
func buildPlan(sym spot.Symbol, bid, ask string) (plan, error) {
	r, err := sym.Rules()
	if err != nil {
		return plan{}, err
	}
	if r.TickSize == "" || r.StepSize == "" || zero(r.TickSize) || zero(r.StepSize) {
		return plan{}, fmt.Errorf("symbol %s: missing tickSize %q / stepSize %q", sym.Symbol, r.TickSize, r.StepSize)
	}
	mid, err := spot.MidPrice(bid, ask, 12)
	if err != nil {
		return plan{}, err
	}
	raw, err := spot.ScaleDecimal(mid, planDistanceNum, planDistanceDen, 12)
	if err != nil {
		return plan{}, err
	}
	px, err := spot.RoundDown(raw, r.TickSize)
	if err != nil {
		return plan{}, err
	}
	if !spot.ValidPositiveDecimal(px) {
		return plan{}, fmt.Errorf("planned price %q is not positive", px)
	}
	if r.MinPrice != "" && !zero(r.MinPrice) {
		if c, _ := spot.CmpDecimal(px, r.MinPrice); c < 0 {
			return plan{}, fmt.Errorf("planned price %s below minPrice %s", px, r.MinPrice)
		}
	}
	if r.MaxPrice != "" && !zero(r.MaxPrice) {
		if c, _ := spot.CmpDecimal(px, r.MaxPrice); c > 0 {
			return plan{}, fmt.Errorf("planned price %s above maxPrice %s", px, r.MaxPrice)
		}
	}
	if r.BidMultiplierDown != "" && !zero(r.BidMultiplierDown) {
		// The reference is the venue's weighted average price, which this tool
		// does not read; the mid stands in for it.
		low, err := spot.MulDecimal(mid, r.BidMultiplierDown)
		if err == nil {
			if c, _ := spot.CmpDecimal(px, low); c < 0 {
				return plan{}, fmt.Errorf("planned price %s below the percent-price floor ~%s", px, low)
			}
		}
	}
	floor := minNotionalUSD
	if r.MinNotional != "" && !zero(r.MinNotional) {
		if c, _ := spot.CmpDecimal(r.MinNotional, floor); c > 0 {
			floor = r.MinNotional
		}
	}
	target, err := spot.ScaleDecimal(floor, notionalMargin, 10, 8)
	if err != nil {
		return plan{}, err
	}
	rawQty, err := spot.DivDecimal(target, px, 12)
	if err != nil {
		return plan{}, err
	}
	qty, err := spot.RoundUp(rawQty, r.StepSize)
	if err != nil {
		return plan{}, err
	}
	if r.MinQty != "" && !zero(r.MinQty) {
		if c, _ := spot.CmpDecimal(qty, r.MinQty); c < 0 {
			if qty, err = spot.RoundUp(r.MinQty, r.StepSize); err != nil {
				return plan{}, err
			}
		}
	}
	if r.MaxQty != "" && !zero(r.MaxQty) {
		if c, _ := spot.CmpDecimal(qty, r.MaxQty); c > 0 {
			return plan{}, fmt.Errorf("planned qty %s above maxQty %s", qty, r.MaxQty)
		}
	}
	notional, err := spot.MulDecimal(px, qty)
	if err != nil {
		return plan{}, err
	}
	if c, _ := spot.CmpDecimal(notional, floor); c < 0 {
		return plan{}, fmt.Errorf("planned notional %s below %s", notional, floor)
	}
	if c, _ := spot.CmpDecimal(notional, maxNotionalUSD); c > 0 {
		return plan{}, fmt.Errorf("planned notional %s above the %s USD cap (stepSize %s too coarse for this price)", notional, maxNotionalUSD, r.StepSize)
	}
	return plan{price: px, qty: qty, notional: notional, rules: r, quote: sym.QuoteAsset}, nil
}

func zero(s string) bool { c, err := spot.CmpDecimal(s, "0"); return err == nil && c == 0 }

func (p plan) req(typ, cloid string) spot.OrderReq {
	return spot.OrderReq{Symbol: "", Side: spot.SideBuy, Type: typ, Quantity: p.qty, Price: p.price, NewClientOrderID: cloid, NewOrderRespType: spot.RespRESULT}
}

// ---------- REST helpers ----------

func (a *app) findOpen(cloid string) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), restWait)
	defer cancel()
	rows, err := a.c.OpenOrders(ctx, a.symbol)
	if err != nil {
		a.noteErr(err)
		return 0, false, err
	}
	for _, o := range rows {
		if o.ClientOrderID == cloid {
			return o.OrderID, true, nil
		}
	}
	return 0, false, nil
}

// waitOpen polls openOrders until oid is present (want=true) or absent.
func (a *app) waitOpen(ctx context.Context, oid int64, want bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		rows, err := a.c.OpenOrders(ctx, a.symbol)
		if err != nil {
			a.noteErr(err)
			return false
		}
		present := false
		for _, o := range rows {
			if o.OrderID == oid {
				present = true
				break
			}
		}
		if present == want {
			return true
		}
		if a.banned || time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (a *app) waitEmpty(ctx context.Context, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for {
		rows, err := a.c.OpenOrders(ctx, a.symbol)
		if err != nil {
			a.noteErr(err)
			return -1, false
		}
		if len(rows) == 0 {
			return 0, true
		}
		if a.banned || time.Now().After(deadline) {
			return len(rows), false
		}
		select {
		case <-ctx.Done():
			return len(rows), false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ---------- live sample ----------

type sampleRes struct {
	placeSend, placeWS, cancelSend, cancelWS time.Duration
	hasPlaceWS, hasCancelWS                  bool
}

func (a *app) sample(ctx context.Context, i int, p plan, cloid string) (sampleRes, bool) {
	var r sampleRes
	req := p.req(spot.TypeLimitMaker, cloid)
	req.Symbol = a.symbol
	t0 := time.Now()
	o, err := a.c.NewOrder(ctx, req)
	ack := time.Now()
	r.placeSend = ack.Sub(t0)
	if err != nil {
		a.noteErr(err)
		if spot.IsUncertain(err) {
			oid, found, rerr := a.findOpen(cloid)
			detail := fmt.Sprintf("reconcile openOrders: found=%v oid=%d", found, oid)
			if rerr != nil {
				detail = "reconcile openOrders failed: " + rerr.Error()
			}
			a.check(fmt.Sprintf("sample %d place ack", i), false, "UNKNOWN: "+err.Error()+"; "+detail+"; not resending")
			return r, false
		}
		a.check(fmt.Sprintf("sample %d place ack", i), false, err.Error())
		return r, false
	}
	oid := o.OrderID
	okAck := oid != 0 && o.ClientOrderID == cloid && o.Status == spot.StatusNew
	if !a.check(fmt.Sprintf("sample %d place ack", i), okAck, fmt.Sprintf("oid=%d status=%s send->ack=%s", oid, o.Status, fmtMs(r.placeSend))) {
		return r, false
	}
	vlog("cloid=%s", cloid)

	okOpen := a.waitOpen(ctx, oid, true, restWait)
	a.check(fmt.Sprintf("sample %d in openOrders", i), okOpen, fmt.Sprintf("oid=%d", oid))

	if ev, ok := a.store.waitFor(ctx, oid, isNewEv, wsWait); ok {
		r.placeWS, r.hasPlaceWS = ev.at.Sub(ack), true
		a.check(fmt.Sprintf("sample %d ws NEW", i), true, fmt.Sprintf("oid=%d ack->ws=%s", oid, fmtMs(r.placeWS)))
	} else {
		a.check(fmt.Sprintf("sample %d ws NEW", i), false, fmt.Sprintf("no executionReport NEW for oid=%d within %s", oid, wsWait))
	}

	t1 := time.Now()
	co, err := a.c.CancelOrder(ctx, spot.CancelReq{Symbol: a.symbol, OrigClientOrderID: cloid})
	cack := time.Now()
	r.cancelSend = cack.Sub(t1)
	if err != nil {
		a.noteErr(err)
		a.check(fmt.Sprintf("sample %d cancel ack", i), false, err.Error())
		return r, false
	}
	okCancel := co != nil && co.OrderID == oid && co.Status == spot.StatusCanceled
	a.check(fmt.Sprintf("sample %d cancel ack", i), okCancel, fmt.Sprintf("oid=%d send->ack=%s", oid, fmtMs(r.cancelSend)))

	okWSC := false
	if ev, ok := a.store.waitFor(ctx, oid, isCancelEv, wsWait); ok {
		r.cancelWS, r.hasCancelWS = ev.at.Sub(cack), true
		okWSC = true
		a.check(fmt.Sprintf("sample %d ws CANCELED", i), true, fmt.Sprintf("oid=%d ack->ws=%s", oid, fmtMs(r.cancelWS)))
	} else {
		a.check(fmt.Sprintf("sample %d ws CANCELED", i), false, fmt.Sprintf("no executionReport CANCELED for oid=%d within %s", oid, wsWait))
	}

	gone := a.waitOpen(ctx, oid, false, restWait)
	a.check(fmt.Sprintf("sample %d gone from openOrders", i), gone, fmt.Sprintf("oid=%d", oid))
	return r, okCancel && okOpen && okWSC && gone
}

// ---------- cleanup ----------

// cleanup runs on every exit path once live orders are allowed. It is skipped
// after a 418 ban. "Nothing open" (-2011) is not a failure.
func (a *app) cleanup() {
	if a.banned {
		fmt.Println("cleanup: SKIPPED CancelAll (HTTP 418 IP ban). Check open orders on the account manually.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), restWait)
	defer cancel()
	res, err := a.c.CancelAll(ctx, a.symbol)
	switch {
	case err == nil:
		fmt.Printf("cleanup: CancelAll %s sent, cancelled=%d\n", a.symbol, len(res.Orders))
	case spot.IsUnknownOrder(err):
		fmt.Printf("cleanup: CancelAll %s: nothing open\n", a.symbol)
	default:
		a.noteErr(err)
		fmt.Printf("cleanup: CancelAll %s FAIL: %v\n", a.symbol, err)
		return
	}
	rows, err := a.c.OpenOrders(ctx, a.symbol)
	if err != nil {
		a.noteErr(err)
		fmt.Printf("cleanup: openOrders check FAIL: %v\n", err)
		return
	}
	fmt.Printf("cleanup: openOrders on %s now=%d\n", a.symbol, len(rows))
}

// ---------- run ----------

func run() int {
	if *flagSamples < 1 {
		*flagSamples = 1
	}
	symbol := strings.ToUpper(strings.TrimSpace(*flagSymbol))
	env, err := bnnet.Resolve(bnnet.Spot, *flagNetwork)
	if err != nil {
		fmt.Printf("FAIL %v\n", err)
		return 1
	}
	fmt.Printf("venue=%s network=%s live=%v confirm-mainnet=%v symbol=%s samples=%d\n",
		env.RESTURL, env.Network, *flagLive, *flagConfirmMainnet, symbol, *flagSamples)

	if *flagLive && env.Network == "mainnet" && !*flagConfirmMainnet {
		fmt.Println("REFUSED: -live on mainnet needs -confirm-mainnet (this places real orders). Nothing sent.")
		return 2
	}
	fmt.Printf("env %s set=%v; env %s set=%v (values never printed)\n", env.KeyVar, env.Key != "", env.SecretVar, env.Secret != "")
	if *flagLive {
		if name := env.MissingVar(); name != "" {
			fmt.Printf("REFUSED: missing env %s; -live needs both keys. Nothing sent.\n", name)
			return 3
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancelTimeout := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelTimeout()

	cfg := spot.Config{Network: spot.Network(env.Network)}
	if *flagLive { // the dry run never loads keys into the client
		cfg.APIKey, cfg.APISecret = env.Key, env.Secret
	}
	c, err := spot.New(cfg)
	if err != nil {
		fmt.Printf("client FAIL: %v\n", err)
		return 1
	}
	a := &app{c: c, symbol: symbol, network: cfg.Network, store: newStore()}

	fmt.Println("== public checks ==")
	ei, err := c.ExchangeInfo(ctx, symbol)
	if err != nil {
		a.noteErr(err)
		a.check("exchangeInfo", false, err.Error())
		return 1
	}
	var sym *spot.Symbol
	for i := range ei.Symbols {
		if ei.Symbols[i].Symbol == symbol {
			sym = &ei.Symbols[i]
		}
	}
	if sym == nil {
		a.check("exchangeInfo", false, "symbol "+symbol+" not in the response")
		return 1
	}
	rules, err := sym.Rules()
	if err != nil {
		a.check("exchangeInfo", false, err.Error())
		return 1
	}
	a.check("exchangeInfo", true, fmt.Sprintf("%s status=%s tickSize=%s stepSize=%s minQty=%s minNotional=%s bidMultiplierDown=%s",
		symbol, sym.Status, rules.TickSize, rules.StepSize, rules.MinQty, rules.MinNotional, rules.BidMultiplierDown))
	if sym.Status != "TRADING" {
		a.check("symbol trading", false, "status="+sym.Status)
	}
	if !sym.SupportsOrderType(spot.TypeLimitMaker) {
		a.check("LIMIT_MAKER allowed", false, "orderTypes="+strings.Join(sym.OrderTypes, ","))
	}
	a.quote = sym.QuoteAsset

	bt, err := c.BookTicker(ctx, symbol)
	if err != nil {
		a.noteErr(err)
		a.check("bookTicker", false, err.Error())
		return 1
	}
	a.check("bookTicker", true, fmt.Sprintf("bid=%s ask=%s", bt.BidPrice, bt.AskPrice))

	p, err := buildPlan(*sym, bt.BidPrice, bt.AskPrice)
	if err != nil {
		a.check("plan", false, err.Error())
		return 1
	}
	a.check("plan notional", true, fmt.Sprintf("notional=%s (cap %s)", p.notional, maxNotionalUSD))
	fmt.Printf("plan: BUY LIMIT_MAKER (post-only) %s px=%s qty=%s notional=%s %s\n", symbol, p.price, p.qty, p.notional, sym.QuoteAsset)

	if !*flagLive {
		if a.failures > 0 {
			return 1
		}
		fmt.Println("dry run: -live not given. Nothing sent.")
		return 0
	}
	return runLive(ctx, a, p)
}

func runLive(ctx context.Context, a *app, p plan) int {
	if err := a.c.SyncTime(ctx); err != nil {
		a.noteErr(err)
		fmt.Printf("time sync: FAIL %v\n", err)
		return 1
	}

	fmt.Println("== live preconditions ==")
	oo, err := a.c.OpenOrders(ctx, a.symbol)
	if err != nil {
		a.noteErr(err)
		a.check("precondition openOrders", false, err.Error())
		return 1
	}
	if len(oo) != 0 {
		fmt.Printf("REFUSED: %d open order(s) already on %s. Nothing sent.\n", len(oo), a.symbol)
		return 2
	}
	acc, err := a.c.Account(ctx, true)
	if err != nil {
		a.noteErr(err)
		a.check("precondition account", false, err.Error())
		return 1
	}
	free := acc.Balance(a.quote).Free
	if free == "" {
		free = "0"
	}
	if c, err := spot.CmpDecimal(free, p.notional); err != nil || c < 0 {
		fmt.Printf("REFUSED: free %s balance %s is below the order notional %s. Nothing sent.\n", a.quote, free, p.notional)
		return 2
	}
	fmt.Printf("precondition: no open orders on %s; free %s %s covers notional %s\n", a.symbol, a.quote, free, p.notional)

	// From here on, any exit cancels all open orders on the symbol.
	defer a.cleanup()

	// Test order: validates signature, recvWindow and filters; places nothing.
	tr := p.req(spot.TypeLimitMaker, "")
	tr.Symbol = a.symbol
	if err := a.c.TestOrder(ctx, tr); err != nil {
		a.noteErr(err)
		a.check("test order (POST /api/v3/order/test)", false, err.Error())
		return 1
	}
	a.check("test order (POST /api/v3/order/test)", true, "accepted, nothing placed")

	us, err := spot.NewUserStream(spot.UserStreamConfig{Client: a.c, Network: a.network, EventTypes: []string{"executionReport"}})
	if err != nil {
		a.check("user stream new", false, err.Error())
		return 1
	}
	us.SetOnExecutionReport(func(ev spot.ExecutionReport) {
		a.store.add(ev.OrderID, wsEvent{execType: ev.ExecType, status: ev.Status, at: time.Now()})
	})
	if err := us.Start(ctx); err != nil {
		a.noteErr(err)
		a.check("user stream start", false, err.Error())
		return 1
	}
	defer us.Stop()
	a.check("user stream start", true, "subscribed to executionReport (userDataStream.subscribe.signature)")

	fmt.Printf("== live %s order round trips ==\n", a.symbol)
	stamp := time.Now().UnixMilli()
	var placeSend, placeWS, cancelSend, cancelWS []time.Duration
	allOK := true
	for i := 1; i <= *flagSamples; i++ {
		cloid := fmt.Sprintf("bnspot-lat-%d-s%d", stamp, i)
		r, ok := a.sample(ctx, i, p, cloid)
		placeSend = append(placeSend, r.placeSend)
		cancelSend = append(cancelSend, r.cancelSend)
		if r.hasPlaceWS {
			placeWS = append(placeWS, r.placeWS)
		}
		if r.hasCancelWS {
			cancelWS = append(cancelWS, r.cancelWS)
		}
		if !ok {
			allOK = false
			fmt.Printf("stopping after sample %d: a step failed\n", i)
			break
		}
		if a.banned || ctx.Err() != nil {
			allOK = false
			break
		}
	}
	if allOK && !a.banned && ctx.Err() == nil {
		n, empty := a.waitEmpty(ctx, restWait)
		a.check("final openOrders empty", empty, fmt.Sprintf("remaining=%d", n))
	}

	fmt.Println("\n== latency over samples (ms) ==")
	summarize("place send->ack", placeSend)
	summarize("place ack->ws(NEW)", placeWS)
	summarize("cancel send->ack", cancelSend)
	summarize("cancel ack->ws(CANCELED)", cancelWS)
	fmt.Printf("samples=%d failures=%d\n", len(placeSend), a.failures)
	if a.failures > 0 || !allOK {
		return 1
	}
	return 0
}
