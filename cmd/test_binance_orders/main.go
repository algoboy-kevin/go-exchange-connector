// Command test_binance_orders is a gated Binance USDⓈ-M futures order-latency
// checker. -network testnet|mainnet (default testnet) picks the endpoints and the
// key variables together through internal/bnnet: BINANCE_FUTURES_TESTNET_API_KEY /
// _API_SECRET or BINANCE_FUTURES_MAINNET_API_KEY / _API_SECRET. Nothing is sent
// without -live; mainnet with -live also needs -confirm-mainnet.
//
// Without -live (default) it is read-only: public exchangeInfo and bookTicker
// for the symbol, the planned post-only order is printed, and if both keys are
// set, positionRisk and openOrders are read too.
//
// With -live it refuses unless the symbol has no open orders and a flat
// position. It then starts the private user-data stream and, per sample, places
// one GTX (post-only) BUY far below the book with a unique newClientOrderId,
// checks it in openOrders and on the WebSocket (matched by order id only),
// cancels it by client order id, and checks it is gone. Once, it places and
// cancels a batch of two. Any order still resting is cancelled (CancelAll on the
// symbol) on every exit path.
//
// Safety:
//   - Keys come only from the environment. Values are never printed; neither is
//     the listen key.
//   - An UNKNOWN result (IsUncertain) is reconciled through openOrders and never
//     resent blindly; the run then stops.
//   - HTTP 429 stops the run (no retry). HTTP 418 (IP ban) aborts at once and
//     skips the cleanup call.
//
// Exit codes: 0 all checks PASS (or dry run OK); 1 a check FAILed or a fatal
// error; 2 refused by a flag gate; 3 refused for a missing env key.
//
// Usage:
//
//	go run ./cmd/test_binance_orders                          # read-only, testnet
//	set -a; source .env; set +a
//	go run ./cmd/test_binance_orders -live -samples 3         # testnet
//	go run ./cmd/test_binance_orders -network mainnet -live -confirm-mainnet
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/internal/bnnet"
	bn "github.com/algoboy-kevin/go-exchange-connector/pkg/binance/futures"
)

const (
	planDistance     = 0.20 // planned price is 20% below mid
	minNotionalFloor = 6.0  // USD, floor applied on top of the venue minNotional
	wsWait           = 10 * time.Second
	restWait         = 15 * time.Second
)

var (
	flagLive           = flag.Bool("live", false, "required to send any order or cancel")
	flagNetwork        = flag.String("network", "testnet", "testnet or mainnet")
	flagConfirmMainnet = flag.Bool("confirm-mainnet", false, "required with -live on mainnet")
	flagCoin           = flag.String("coin", "BTCUSDT", "symbol, e.g. BTCUSDT")
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

// decimalsOf returns the number of significant decimals in a venue step string
// such as "0.00100000" (3) or "1.00000000" (0).
func decimalsOf(s string) int {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return 0
	}
	return len(strings.TrimRight(s[i+1:], "0"))
}

func fmtStep(v float64, stepS string) string {
	return strconv.FormatFloat(v, 'f', decimalsOf(stepS), 64)
}

func vlog(format string, args ...any) {
	if *flagVerbose {
		fmt.Printf("  [v] "+format+"\n", args...)
	}
}

// ---------- check bookkeeping ----------

type app struct {
	c        *bn.Client
	env      bnnet.Env
	symbol   string
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

// noteErr records rate-limit state. 418 sets banned; 429 is reported and the
// caller stops (there is no retry loop anywhere in this tool).
func (a *app) noteErr(err error) {
	var ae *bn.APIError
	if !errors.As(err, &ae) {
		return
	}
	switch ae.HTTPStatus {
	case 418:
		a.banned = true
		fmt.Println("HTTP 418: IP ban. Aborting; no further requests will be sent.")
	case 429:
		fmt.Println("HTTP 429: rate limited. Stopping; no retry.")
	}
}

// ---------- WebSocket event store ----------

type wsEvent struct {
	execType string
	status   string
	at       time.Time
}

// evStore keeps user-stream order events keyed by order id. Matching is by
// order id only, so an unrelated event is never accepted.
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

// waitFor returns the first event for oid satisfying pred, within timeout or
// until ctx is done.
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

func isOpenEv(e wsEvent) bool   { return e.execType == "NEW" && e.status == "NEW" }
func isCancelEv(e wsEvent) bool { return e.execType == "CANCELED" && e.status == "CANCELED" }

// ---------- REST helpers ----------

// symInfo is the subset of exchangeInfo this tool needs.
type symInfo struct {
	status      string
	tick, step  float64
	tickS       string
	stepS       string
	minNotional float64 // 0 when the MIN_NOTIONAL filter carries no notional
}

func (a *app) symbolInfo(ctx context.Context) (symInfo, error) {
	ei, err := a.c.ExchangeInfo(ctx)
	if err != nil {
		return symInfo{}, err
	}
	for _, s := range ei.Symbols {
		if s.Symbol != a.symbol {
			continue
		}
		var si symInfo
		si.status = s.Status
		var notionalS string
		for _, f := range s.Filters {
			switch f.FilterType {
			case "PRICE_FILTER":
				si.tickS = f.TickSize
			case "LOT_SIZE":
				si.stepS = f.StepSize
			case "MIN_NOTIONAL":
				notionalS = f.Notional
			}
		}
		tick, err1 := strconv.ParseFloat(si.tickS, 64)
		step, err2 := strconv.ParseFloat(si.stepS, 64)
		if err1 != nil || err2 != nil || tick <= 0 || step <= 0 {
			return symInfo{}, fmt.Errorf("symbol %s: missing or invalid tickSize %q / stepSize %q", a.symbol, si.tickS, si.stepS)
		}
		si.tick, si.step = tick, step
		if notionalS != "" {
			if v, err := strconv.ParseFloat(notionalS, 64); err == nil {
				si.minNotional = v
			}
		}
		return si, nil
	}
	return symInfo{}, fmt.Errorf("symbol %s not in exchangeInfo", a.symbol)
}

// bookMid reads GET /fapi/v1/ticker/bookTicker (public) and returns the mid.
func (a *app) bookMid(ctx context.Context) (float64, error) {
	q := url.Values{}
	q.Set("symbol", a.symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.env.RESTURL+"/fapi/v1/ticker/bookTicker?"+q.Encode(), nil)
	if err != nil {
		return 0, err
	}
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 400 {
		return 0, &bn.APIError{HTTPStatus: resp.StatusCode, Msg: "bookTicker"}
	}
	var bt struct {
		BidPrice string `json:"bidPrice"`
		AskPrice string `json:"askPrice"`
	}
	if err := json.Unmarshal(body, &bt); err != nil {
		return 0, err
	}
	bid, err1 := strconv.ParseFloat(bt.BidPrice, 64)
	ask, err2 := strconv.ParseFloat(bt.AskPrice, 64)
	if err1 != nil || err2 != nil || bid <= 0 || ask <= 0 {
		return 0, fmt.Errorf("bookTicker for %s: unusable bid/ask", a.symbol)
	}
	return (bid + ask) / 2, nil
}

// findOpen lists openOrders and reports whether a client order id is present.
// Used only to reconcile an UNKNOWN result; it uses its own context so it still
// works after ctrl-c.
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

// waitOpen polls openOrders until oid is present (want=true) or absent
// (want=false), or the timeout passes.
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

// waitEmpty polls openOrders until the symbol has no open orders.
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

// sample runs one place -> openOrders -> WS open -> cancel -> WS canceled ->
// gone round trip. Returns false on the first failed step; the caller stops.
func (a *app) sample(ctx context.Context, i int, px, qty, cloid string) (sampleRes, bool) {
	var r sampleRes
	req := bn.OrderReq{
		Symbol:           a.symbol,
		Side:             "BUY",
		Type:             "LIMIT",
		TimeInForce:      "GTX",
		Quantity:         qty,
		Price:            px,
		NewClientOrderID: cloid,
	}
	t0 := time.Now()
	o, err := a.c.NewOrder(ctx, req)
	ack := time.Now()
	r.placeSend = ack.Sub(t0)
	if err != nil {
		a.noteErr(err)
		if bn.IsUncertain(err) {
			// UNKNOWN: reconcile only; never resend. The run stops here and
			// the cleanup CancelAll removes anything that did rest.
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
	okAck := oid != 0 && o.ClientOrderID == cloid && o.Status == "NEW"
	if !a.check(fmt.Sprintf("sample %d place ack", i), okAck, fmt.Sprintf("oid=%d status=%s send->ack=%s", oid, o.Status, fmtMs(r.placeSend))) {
		return r, false
	}
	vlog("cloid=%s", cloid)

	okOpen := a.waitOpen(ctx, oid, true, restWait)
	a.check(fmt.Sprintf("sample %d in openOrders", i), okOpen, fmt.Sprintf("oid=%d", oid))

	if ev, ok := a.store.waitFor(ctx, oid, isOpenEv, wsWait); ok {
		r.placeWS, r.hasPlaceWS = ev.at.Sub(ack), true
		a.check(fmt.Sprintf("sample %d ws open", i), true, fmt.Sprintf("oid=%d ack->ws=%s", oid, fmtMs(r.placeWS)))
	} else {
		a.check(fmt.Sprintf("sample %d ws open", i), false, fmt.Sprintf("no ORDER_TRADE_UPDATE NEW for oid=%d within %s", oid, wsWait))
	}

	t1 := time.Now()
	co, err := a.c.CancelOrder(ctx, a.symbol, cloid)
	cack := time.Now()
	r.cancelSend = cack.Sub(t1)
	if err != nil {
		a.noteErr(err)
		a.check(fmt.Sprintf("sample %d cancel ack", i), false, err.Error())
		return r, false
	}
	okCancel := co != nil && co.OrderID == oid && co.Status == "CANCELED"
	a.check(fmt.Sprintf("sample %d cancel ack", i), okCancel, fmt.Sprintf("oid=%d send->ack=%s", oid, fmtMs(r.cancelSend)))

	okWSC := false
	if ev, ok := a.store.waitFor(ctx, oid, isCancelEv, wsWait); ok {
		r.cancelWS, r.hasCancelWS = ev.at.Sub(cack), true
		okWSC = true
		a.check(fmt.Sprintf("sample %d ws canceled", i), true, fmt.Sprintf("oid=%d ack->ws=%s", oid, fmtMs(r.cancelWS)))
	} else {
		a.check(fmt.Sprintf("sample %d ws canceled", i), false, fmt.Sprintf("no ORDER_TRADE_UPDATE CANCELED for oid=%d within %s", oid, wsWait))
	}

	gone := a.waitOpen(ctx, oid, false, restWait)
	a.check(fmt.Sprintf("sample %d gone from openOrders", i), gone, fmt.Sprintf("oid=%d", oid))

	return r, okCancel && okOpen && okWSC && gone
}

// batch places two GTX orders in one action, then cancels both by client id in
// one action.
func (a *app) batch(ctx context.Context, px, qty string, stamp int64) bool {
	c1 := fmt.Sprintf("bnf-lat-%d-b1", stamp)
	c2 := fmt.Sprintf("bnf-lat-%d-b2", stamp)
	mk := func(cloid string) bn.OrderReq {
		return bn.OrderReq{Symbol: a.symbol, Side: "BUY", Type: "LIMIT", TimeInForce: "GTX", Quantity: qty, Price: px, NewClientOrderID: cloid}
	}
	items, err := a.c.BatchOrders(ctx, []bn.OrderReq{mk(c1), mk(c2)})
	if err != nil {
		a.noteErr(err)
		if bn.IsUncertain(err) {
			_, f1, _ := a.findOpen(c1)
			_, f2, _ := a.findOpen(c2)
			a.check("batch place 2", false, fmt.Sprintf("UNKNOWN: %v; reconcile found1=%v found2=%v; not resending", err, f1, f2))
			return false
		}
		return a.check("batch place 2", false, err.Error())
	}
	byCloid := map[string]bn.BatchItem{}
	for _, it := range items {
		if it.Order != nil {
			byCloid[it.Order.ClientOrderID] = it
		}
	}
	placedOK := len(items) == 2
	var oids [2]int64
	for k, cl := range []string{c1, c2} {
		it, ok := byCloid[cl]
		if !ok || it.Err != nil || it.Order == nil || it.Order.OrderID == 0 || it.Order.Status != "NEW" {
			placedOK = false
			detail := "item missing"
			if ok && it.Err != nil {
				detail = it.Err.Error()
			}
			a.check("batch place 2", false, fmt.Sprintf("cloid %d: %s", k+1, detail))
			return false
		}
		oids[k] = it.Order.OrderID
	}
	a.check("batch place 2", placedOK, fmt.Sprintf("oids=%d,%d", oids[0], oids[1]))

	cancels, err := a.c.CancelBatch(ctx, a.symbol, []string{c1, c2})
	if err != nil {
		a.noteErr(err)
		a.check("batch cancel 2 by cloid", false, err.Error())
		return false
	}
	okC := len(cancels) == 2
	for _, it := range cancels {
		if it.Err != nil || it.Order == nil || it.Order.Status != "CANCELED" {
			okC = false
		}
	}
	return a.check("batch cancel 2 by cloid", okC, fmt.Sprintf("items=%d", len(cancels)))
}

// ---------- cleanup ----------

// cleanup runs on every exit path once live orders are allowed. It cancels all
// open orders on the symbol with a fresh context. It is skipped after a 418 IP
// ban, because any further request would be against the ban.
func (a *app) cleanup() {
	if a.banned {
		fmt.Println("cleanup: SKIPPED CancelAll (HTTP 418 IP ban). Check open orders on the account manually.")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), restWait)
	defer cancel()
	if err := a.c.CancelAll(ctx, a.symbol); err != nil {
		a.noteErr(err)
		fmt.Printf("cleanup: CancelAll %s FAIL: %v\n", a.symbol, err)
		return
	}
	fmt.Printf("cleanup: CancelAll %s sent\n", a.symbol)
	rows, err := a.c.OpenOrders(ctx, a.symbol)
	if err != nil {
		a.noteErr(err)
		fmt.Printf("cleanup: openOrders check FAIL: %v\n", err)
		return
	}
	fmt.Printf("cleanup: openOrders on %s now=%d\n", a.symbol, len(rows))
}

// privateWS is the Binance USDⓈ-M private stream base for the network.
func privateWS(env bnnet.Env) string { return env.WSURL + "/private" }

// ---------- run ----------

func run() int {
	if *flagSamples < 1 {
		*flagSamples = 1
	}
	symbol := strings.ToUpper(strings.TrimSpace(*flagCoin))
	env, err := bnnet.Resolve(bnnet.Futures, *flagNetwork)
	if err != nil {
		fmt.Printf("REFUSED: %v. Nothing sent.\n", err)
		return 2
	}
	fmt.Printf("venue=%s network=%s live=%v confirm-mainnet=%v symbol=%s samples=%d\n",
		env.RESTURL, env.Network, *flagLive, *flagConfirmMainnet, symbol, *flagSamples)

	if *flagLive && env.Network == "mainnet" && !*flagConfirmMainnet {
		fmt.Println("REFUSED: -network mainnet with -live needs -confirm-mainnet (real orders). Nothing sent.")
		return 2
	}

	apiKey, apiSecret := env.Key, env.Secret
	envKey, envSecret := env.KeyVar, env.SecretVar
	fmt.Printf("env %s set=%v; env %s set=%v (values never printed)\n", envKey, apiKey != "", envSecret, apiSecret != "")
	if *flagLive {
		if miss := env.MissingVar(); miss != "" {
			fmt.Printf("REFUSED: missing env %s; -live needs both keys. Nothing sent.\n", miss)
			return 3
		}
	}
	hasKeys := apiKey != "" && apiSecret != ""
	if !hasKeys && (apiKey != "" || apiSecret != "") {
		fmt.Printf("keys incomplete: account reads SKIPPED (need both %s and %s)\n", envKey, envSecret)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancelTimeout := context.WithTimeout(ctx, 10*time.Minute)
	defer cancelTimeout()

	c, err := bn.New(bn.Config{BaseURL: env.RESTURL, APIKey: apiKey, APISecret: apiSecret})
	if err != nil {
		fmt.Printf("client FAIL: %v\n", err)
		return 1
	}
	a := &app{c: c, env: env, symbol: symbol, store: newStore()}

	if hasKeys || *flagLive {
		if err := c.SyncTime(ctx); err != nil {
			a.noteErr(err)
			fmt.Printf("time sync: FAIL %v\n", err)
			if *flagLive {
				return 1
			}
		} else {
			vlog("time offset synced")
		}
	}

	fmt.Println("== public checks ==")
	si, err := a.symbolInfo(ctx)
	if err != nil {
		a.noteErr(err)
		a.check("exchangeInfo", false, err.Error())
		return 1
	}
	notionalNote := "absent from response; using $" + strconv.FormatFloat(minNotionalFloor, 'f', 0, 64) + " floor only"
	if si.minNotional > 0 {
		notionalNote = strconv.FormatFloat(si.minNotional, 'f', -1, 64)
	}
	a.check("exchangeInfo", true, fmt.Sprintf("%s status=%s tickSize=%s stepSize=%s minNotional=%s", symbol, si.status, si.tickS, si.stepS, notionalNote))
	if si.status != "TRADING" {
		a.check("symbol trading", false, "status="+si.status)
	}

	mid, err := a.bookMid(ctx)
	if err != nil {
		a.noteErr(err)
		a.check("bookTicker", false, err.Error())
		return 1
	}
	a.check("bookTicker", true, fmt.Sprintf("mid=%s", strconv.FormatFloat(mid, 'f', -1, 64)))

	// Plan: price 20% below mid, rounded DOWN to tick (further from mid, so the
	// 20% rule holds); quantity rounded UP to step so notional >= max(minNotional, $6).
	pxF := math.Floor((mid*(1-planDistance))/si.tick+1e-9) * si.tick
	px := fmtStep(pxF, si.tickS)
	pxF, _ = strconv.ParseFloat(px, 64)
	floorN := math.Max(si.minNotional, minNotionalFloor)
	qtyF := math.Ceil(floorN/pxF/si.step-1e-9) * si.step
	qty := fmtStep(qtyF, si.stepS)
	qtyF, _ = strconv.ParseFloat(qty, 64)
	distOK := 1-pxF/mid >= planDistance-1e-9
	notionalOK := pxF*qtyF >= floorN
	if !a.check("plan distance >= 20%", distOK, fmt.Sprintf("px=%s is %.2f%% below mid", px, (1-pxF/mid)*100)) {
		return 1
	}
	if !a.check("plan notional", notionalOK, fmt.Sprintf("notional=%.4f >= %.2f", pxF*qtyF, floorN)) {
		return 1
	}
	fmt.Printf("plan: BUY LIMIT GTX (post-only) %s px=%s qty=%s notional=%.4f USD\n", symbol, px, qty, pxF*qtyF)

	if hasKeys {
		fmt.Println("== account reads ==")
		rows, err := c.PositionRisk(ctx)
		if err != nil {
			a.noteErr(err)
			a.check("positionRisk", false, err.Error())
			return 1
		}
		flat, posDesc := symbolFlat(rows, symbol)
		a.check("positionRisk read", true, posDesc)
		oo, err := c.OpenOrders(ctx, symbol)
		if err != nil {
			a.noteErr(err)
			a.check("openOrders read", false, err.Error())
			return 1
		}
		a.check("openOrders read", true, fmt.Sprintf("count on %s=%d", symbol, len(oo)))
		vlog("flat=%v", flat)
	}

	if !*flagLive {
		if a.failures > 0 {
			return 1
		}
		fmt.Println("dry run: -live not given. Nothing sent.")
		return 0
	}

	return runLive(ctx, a, symbol, px, qty)
}

// symbolFlat reports whether every position row for symbol has zero size, and
// a short description. A row that does not parse counts as not flat.
func symbolFlat(rows []bn.PositionRisk, symbol string) (bool, string) {
	flat := true
	seen := false
	for _, r := range rows {
		if r.Symbol != symbol {
			continue
		}
		seen = true
		v, err := strconv.ParseFloat(r.PositionAmt, 64)
		if err != nil || v != 0 {
			flat = false
		}
	}
	if !seen {
		return true, fmt.Sprintf("positionAmt for %s: no row (treated as flat)", symbol)
	}
	return flat, fmt.Sprintf("flat=%v for %s", flat, symbol)
}

func runLive(ctx context.Context, a *app, symbol, px, qty string) int {
	fmt.Println("== live preconditions ==")
	rows, err := a.c.PositionRisk(ctx)
	if err != nil {
		a.noteErr(err)
		a.check("precondition positionRisk", false, err.Error())
		return 1
	}
	if flat, desc := symbolFlat(rows, symbol); !flat {
		fmt.Printf("REFUSED: not flat on %s (%s). Nothing sent.\n", symbol, desc)
		return 2
	}
	oo, err := a.c.OpenOrders(ctx, symbol)
	if err != nil {
		a.noteErr(err)
		a.check("precondition openOrders", false, err.Error())
		return 1
	}
	if len(oo) != 0 {
		fmt.Printf("REFUSED: %d open order(s) already on %s. Nothing sent.\n", len(oo), symbol)
		return 2
	}
	fmt.Println("precondition: flat and no open orders on", symbol)

	// From here on, any exit cancels all open orders on the symbol.
	defer a.cleanup()

	us, err := bn.NewUserStream(bn.UserStreamConfig{Client: a.c, WSURL: privateWS(a.env)})
	if err != nil {
		a.check("user stream new", false, err.Error())
		return 1
	}
	us.SetOnOrderUpdate(func(ev bn.OrderTradeUpdate) {
		a.store.add(ev.Order.OrderID, wsEvent{execType: ev.Order.ExecType, status: ev.Order.Status, at: time.Now()})
	})
	if err := us.Start(ctx); err != nil {
		a.noteErr(err)
		a.check("user stream start", false, err.Error()) // error text carries no key
		return 1
	}
	defer us.Stop()
	a.check("user stream start", true, "subscribed to ORDER_TRADE_UPDATE (listen key not printed)")

	fmt.Printf("== live %s order round trips ==\n", symbol)
	stamp := time.Now().UnixMilli()
	var placeSend, placeWS, cancelSend, cancelWS []time.Duration
	allSamplesOK := true
	for i := 1; i <= *flagSamples; i++ {
		cloid := fmt.Sprintf("bnf-lat-%d-s%d", stamp, i)
		r, ok := a.sample(ctx, i, px, qty, cloid)
		placeSend = append(placeSend, r.placeSend)
		cancelSend = append(cancelSend, r.cancelSend)
		if r.hasPlaceWS {
			placeWS = append(placeWS, r.placeWS)
		}
		if r.hasCancelWS {
			cancelWS = append(cancelWS, r.cancelWS)
		}
		if !ok {
			allSamplesOK = false
			fmt.Printf("stopping after sample %d: a step failed\n", i)
			break
		}
		if a.banned || ctx.Err() != nil {
			allSamplesOK = false
			break
		}
	}

	batchOK := false
	if allSamplesOK && !a.banned && ctx.Err() == nil {
		batchOK = a.batch(ctx, px, qty, stamp)
	}

	if allSamplesOK && batchOK && !a.banned && ctx.Err() == nil {
		n, empty := a.waitEmpty(ctx, restWait)
		a.check("final openOrders empty", empty, fmt.Sprintf("remaining=%d", n))
	}

	fmt.Println("\n== latency over samples (ms) ==")
	summarize("place send->ack", placeSend)
	summarize("place ack->ws(open)", placeWS)
	summarize("cancel send->ack", cancelSend)
	summarize("cancel ack->ws(cancel)", cancelWS)
	fmt.Printf("samples=%d failures=%d batch=%v\n", len(placeSend), a.failures, batchOK)
	if a.failures > 0 || !allSamplesOK || !batchOK {
		return 1
	}
	return 0
}
