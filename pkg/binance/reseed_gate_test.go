package binance

// reseed_gate_test.go covers the gap-triggered depth re-seed gate (H4c):
// single-flight per book, a per-symbol minimum interval, a shared per-minute
// budget, and the 429/418 Retry-After pause. Everything is loopback: the depth
// stream comes from loVenue, the REST snapshot from a counting fake server.
//
// The venue publishes diffs whose FirstUpdateID jumps (U == u, with u advancing
// by two), and the fake snapshot serves a frozen lastUpdateId of 0, so every
// processed diff is a gap: that is the sustained-overload shape the H4a soak
// measured as 2,201-2,766 REST calls in 60 s for one symbol.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

// reseedREST is a loopback depth-snapshot endpoint. It counts every request,
// tracks per-symbol concurrency (single-flight evidence), and can serve 429/418
// with a Retry-After header.
type reseedREST struct {
	url string
	srv *httptest.Server

	mu          sync.Mutex
	calls       int
	bySymbol    map[string]int
	inflight    map[string]int
	maxInflight map[string]int
	seq         int64
	frozen      int64 // >= 0: serve this lastUpdateId; -1: serve seq
	status      int   // 0: 200
	retryAfter  string
	delay       time.Duration
}

func newReseedREST(t *testing.T) *reseedREST {
	t.Helper()
	d := &reseedREST{
		bySymbol:    map[string]int{},
		inflight:    map[string]int{},
		maxInflight: map[string]int{},
		frozen:      0,
	}
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sym := strings.ToUpper(r.URL.Query().Get("symbol"))
		d.mu.Lock()
		d.calls++
		d.bySymbol[sym]++
		d.inflight[sym]++
		if d.inflight[sym] > d.maxInflight[sym] {
			d.maxInflight[sym] = d.inflight[sym]
		}
		status, retry, delay := d.status, d.retryAfter, d.delay
		last := d.frozen
		if last < 0 {
			last = d.seq
		}
		d.mu.Unlock()

		defer func() {
			d.mu.Lock()
			d.inflight[sym]--
			d.mu.Unlock()
		}()
		if delay > 0 {
			time.Sleep(delay)
		}
		if status != 0 {
			if retry != "" {
				w.Header().Set("Retry-After", retry)
			}
			w.WriteHeader(status)
			return
		}
		if last < 0 {
			last = 0
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"lastUpdateId":%d,"bids":[["1.0","%d"],["0.5","7"]],"asks":[["2.0","3"]]}`, last, last)
	}))
	t.Cleanup(d.srv.Close)
	d.url = d.srv.URL
	return d
}

func (d *reseedREST) setSeq(v int64) {
	d.mu.Lock()
	d.seq = v
	d.mu.Unlock()
}

func (d *reseedREST) setFrozen(v int64) {
	d.mu.Lock()
	d.frozen = v
	d.mu.Unlock()
}

func (d *reseedREST) setStatus(status int, retryAfter string) {
	d.mu.Lock()
	d.status = status
	d.retryAfter = retryAfter
	d.mu.Unlock()
}

func (d *reseedREST) stats() (calls int, bySymbol map[string]int, maxInflight map[string]int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	bySymbol = make(map[string]int, len(d.bySymbol))
	for k, v := range d.bySymbol {
		bySymbol[k] = v
	}
	maxInflight = make(map[string]int, len(d.maxInflight))
	for k, v := range d.maxInflight {
		maxInflight[k] = v
	}
	return d.calls, bySymbol, maxInflight
}

// reseedDepthDiff builds a diff-depth frame whose FirstUpdateID jumps to u
// (U == u), so it is a gap for any book whose lastUpdateID is below u-1.
func reseedDepthDiff(symbol string, u int64) string {
	return fmt.Sprintf(`{"e":"depthUpdate","E":1568014460893,"s":%q,"U":%d,"u":%d,"b":[["1.0","%d"]],"a":[]}`,
		symbol, u, u, u)
}

// reseedSink records every depth event delivered, under a mutex, so the test
// can read the last one without swapping the dispatcher on a live stream.
type reseedSink struct {
	mu    sync.Mutex
	n     int
	last  *connector.BinanceDepthEvent
	seeds int
}

func (s *reseedSink) dispatch(ev any) {
	e, ok := ev.(*connector.BinanceDepthEvent)
	if !ok {
		return
	}
	s.mu.Lock()
	s.n++
	if e.FrameID == 0 {
		s.seeds++
	}
	s.last = e
	s.mu.Unlock()
}

func (s *reseedSink) snapshot() (n, seeds int, last *connector.BinanceDepthEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n, s.seeds, s.last
}

// newReseedLoopback is newLoopbackBinance with caller-supplied Options (the gate
// knobs) — the shared helper pins the default Options. Logs are discarded: the
// flood scenarios produce thousands of queue-full warnings, and writing those
// lines would dominate the timings the tests measure.
func newReseedLoopback(t *testing.T, v *loVenue, rest string, opts Options, d func(any), subscribe func(*WSBinance)) *WSBinance {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	opts.PerpMarketWS = v.url
	opts.PerpPublicWS = v.url
	opts.PerpREST = rest
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.EventQueueSize <= 0 {
		opts.EventQueueSize = 4096
	}
	b := NewWithOptions(base, opts)
	c := b.connFor(MarketPerp, classPublic)
	if subscribe != nil {
		subscribe(b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.rootCx = ctx
	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	c.dispatchCancel = dispatchCancel
	go b.eventDispatcher(dispatchCtx, c)

	o := ws.DefaultWSOptions()
	o.PingInterval = 20000
	o.ReconnectInterval = 20
	if err := c.Connect(ctx, c.wsURL(), o); err != nil {
		t.Fatalf("loopback connect: %v", err)
	}
	t.Cleanup(b.Stop)
	return b
}

// TestDepthReseedRateIsBounded is the H4c D1 guard: a sustained gap flood must
// not turn into one REST snapshot per diff. The bound is derived from the gate
// options: the first trigger is served immediately, then at most one per
// minimum interval over the measured burst, plus one for boundary slack.
func TestDepthReseedRateIsBounded(t *testing.T) {
	const (
		symbol   = "BTCUSDT"
		burstFor = 2500 * time.Millisecond
	)
	minInterval := 1 * time.Second // the documented default: Options zero value
	v := newLoVenue(t)
	rest := newReseedREST(t)
	rest.delay = 5 * time.Millisecond // a real REST round trip is not instant
	sink := &reseedSink{}

	b := newReseedLoopback(t, v, rest.url, Options{}, sink.dispatch,
		func(b *WSBinance) { b.SubscribeDepth(nil, MarketPerp, []string{symbol}) })

	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })
	waitForBinance(t, "the initial REST seed", 5*time.Second, func() bool {
		calls, _, _ := rest.stats()
		return calls >= 1
	})
	initialCalls, _, _ := rest.stats()

	// Sustained gap flood on one symbol: U jumps by two every frame, so the
	// diff is always ahead of lastUpdateID+1 and the book stays unseeded.
	u := int64(1000)
	start := time.Now()
	frames := 0
	for time.Since(start) < burstFor {
		u += 2
		v.write(t, 0, reseedDepthDiff(symbol, u))
		rest.setSeq(u)
		frames++
		if frames%8 == 0 {
			time.Sleep(100 * time.Microsecond) // ~a real diff stream, not a syscall race
		}
	}
	elapsed := time.Since(start)
	if frames < 500 {
		t.Fatalf("only %d gap diffs written in %v: the burst is too small to prove anything", frames, elapsed)
	}
	callsAfterBurst, _, maxInflight := rest.stats()
	burstCalls := callsAfterBurst - initialCalls

	bound := ceilDiv(elapsed, minInterval) + 2 // one immediate + one per interval + slack
	st := b.DepthReseedStats()
	t.Logf("reseed: frames=%d elapsed=%v rest_calls_after_burst=%d bound=%d stats=%+v", frames, elapsed, burstCalls, bound, st)
	if burstCalls > bound {
		t.Fatalf("gap flood caused %d REST snapshots for %d diffs in %v: bound is %d (1 + ceil(elapsed/%v) + 1)",
			burstCalls, frames, elapsed, bound, minInterval)
	}
	if maxInflight[symbol] > 1 {
		t.Fatalf("%d concurrent snapshots for %s: single-flight per book is broken", maxInflight[symbol], symbol)
	}
	if st.Triggers != st.Fetched+st.Coalesced+st.Deferred {
		t.Fatalf("triggers %d != fetched %d + coalesced %d + deferred %d", st.Triggers, st.Fetched, st.Coalesced, st.Deferred)
	}
	if st.Triggers < uint64(frames)/2 {
		t.Fatalf("only %d triggers for %d gap diffs: the burst did not reach the gate", st.Triggers, frames)
	}
	// Coalescing is not asserted here: snapshotDepthBook holds the book mutex
	// across its HTTP GET, so the dispatcher is usually blocked (on bk.mu) for
	// exactly as long as a fetch is in flight and cannot reach the gate to be
	// coalesced. The single-flight rule is pinned deterministically in
	// TestDepthReseedGateSingleFlightAndInterval, and the concurrency cap is
	// checked above via maxInflight.
	if st.Deferred == 0 {
		t.Fatalf("no trigger was deferred by the interval/budget: the gate did not limit anything")
	}

	// Convergence after the burst: let the backlog drain first, then serve the
	// venue's live sequence and publish contiguous diffs until the consumer's
	// book reaches them.
	waitForBinance(t, "the queued backlog to drain", 10*time.Second, func() bool {
		st, ok := b.Stats(MarketPerp)
		return ok && st.QueueLen == 0
	})
	rest.setFrozen(-1)
	before, _, _ := rest.stats()
	deadline := time.Now().Add(10 * time.Second)
	for {
		u++
		v.write(t, 0, reseedDepthDiff(symbol, u))
		rest.setSeq(u)
		now, _, _ := rest.stats()
		if now > before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no re-seed happened after the burst: the book never re-seeded")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 8; i++ {
		u++
		v.write(t, 0, reseedDepthDiff(symbol, u))
		rest.setSeq(u)
		time.Sleep(10 * time.Millisecond)
	}
	wantSeq := u
	waitForBinance(t, "the consumer to reach the venue's leading edge", 10*time.Second, func() bool {
		_, _, last := sink.snapshot()
		return last != nil && last.LastUpdateID == wantSeq
	})
	_, seeds, last := sink.snapshot()
	bids := map[string]string{}
	for _, l := range last.Bids {
		bids[l.Price] = l.Size
	}
	if bids["1.0"] != fmt.Sprint(wantSeq) || bids["0.5"] != "7" {
		t.Fatalf("converged book %v, want 1.0=%d 0.5=7", bids, wantSeq)
	}
	n, _, _ := sink.snapshot()
	t.Logf("reseed: delivered=%d seeds=%d final=%d", n, seeds, wantSeq)
}

// TestDepthReseedSharedBudgetAndPerSymbolFlight proves the budget is shared by
// every symbol of one manager (not per symbol) while single-flight stays per
// book, and that both remain consistent with the ledger.
func TestDepthReseedSharedBudgetAndPerSymbolFlight(t *testing.T) {
	const (
		a        = "BTCUSDT"
		bsym     = "ETHUSDT"
		perMin   = 5
		burstFor = 600 * time.Millisecond
	)
	v := newLoVenue(t)
	rest := newReseedREST(t)
	rest.delay = 10 * time.Millisecond
	sink := &reseedSink{}

	b := newReseedLoopback(t, v, rest.url,
		Options{DepthReseedMinInterval: time.Millisecond, DepthReseedPerMin: perMin},
		sink.dispatch,
		func(b *WSBinance) { b.SubscribeDepth(nil, MarketPerp, []string{a, bsym}) })

	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })
	// Two initial seeds (one per subscribed symbol) happen on connect; they are
	// not gated and must not be, so they are excluded from the burst counts.
	waitForBinance(t, "both initial seeds", 5*time.Second, func() bool {
		calls, _, _ := rest.stats()
		return calls >= 2
	})
	initial, baseBy, _ := rest.stats()

	u := int64(1000)
	start := time.Now()
	frames := 0
	for time.Since(start) < burstFor {
		u += 2
		v.write(t, 0, reseedDepthDiff(a, u))
		v.write(t, 0, reseedDepthDiff(bsym, u))
		rest.setSeq(u)
		frames += 2
		if frames%8 == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	after, bySymbol, maxInflight := rest.stats()
	for k, v := range baseBy {
		bySymbol[k] -= v
	}
	st := b.DepthReseedStats()
	t.Logf("budget: frames=%d rest_calls=%d by_symbol=%v max_inflight=%v stats=%+v", frames, after-initial, bySymbol, maxInflight, st)

	if burst := after - initial; burst > perMin {
		t.Fatalf("%d gap-triggered snapshots for two symbols, budget is %d per minute shared by all symbols", burst, perMin)
	}
	if maxInflight[a] > 1 || maxInflight[bsym] > 1 {
		t.Fatalf("concurrent snapshots per symbol = %v: single-flight is per book", maxInflight)
	}
	if bySymbol[a] == 0 || bySymbol[bsym] == 0 {
		t.Fatalf("per-symbol fetch counts %v: one symbol never got a re-seed, so the budget was not shared fairly", bySymbol)
	}
	if st.Triggers != st.Fetched+st.Coalesced+st.Deferred {
		t.Fatalf("triggers %d != fetched %d + coalesced %d + deferred %d", st.Triggers, st.Fetched, st.Coalesced, st.Deferred)
	}
	if st.Fetched > perMin {
		t.Fatalf("gate admitted %d fetches, budget is %d", st.Fetched, perMin)
	}
	if st.Deferred == 0 {
		t.Fatalf("no trigger was deferred by the exhausted budget: the budget did not bind")
	}
}

// TestDepthReseedPausesOnRateLimit proves a 429 with Retry-After stops every
// gap-triggered re-seed for that long (and no longer), and that the book still
// converges once the pause expires.
func TestDepthReseedPausesOnRateLimit(t *testing.T) {
	const symbol = "BTCUSDT"
	v := newLoVenue(t)
	rest := newReseedREST(t)
	sink := &reseedSink{}

	b := newReseedLoopback(t, v, rest.url,
		Options{DepthReseedMinInterval: time.Millisecond, DepthReseedPerMin: 1000},
		sink.dispatch,
		func(b *WSBinance) { b.SubscribeDepth(nil, MarketPerp, []string{symbol}) })

	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })
	waitForBinance(t, "the initial REST seed", 5*time.Second, func() bool {
		calls, _, _ := rest.stats()
		return calls >= 1
	})
	rest.setStatus(429, "1") // Retry-After: 1 second

	u := int64(1000)
	writeGaps := func(forDur time.Duration) {
		deadline := time.Now().Add(forDur)
		for time.Now().Before(deadline) {
			u += 2
			v.write(t, 0, reseedDepthDiff(symbol, u))
			rest.setSeq(u)
			time.Sleep(100 * time.Microsecond)
		}
	}

	writeGaps(200 * time.Millisecond)
	waitForBinance(t, "the failing fetch", 5*time.Second, func() bool {
		return b.DepthReseedStats().Failed == 1
	})
	pausedAt, _, _ := rest.stats()
	writeGaps(400 * time.Millisecond)
	during, _, _ := rest.stats()
	if during != pausedAt {
		t.Fatalf("%d further snapshot calls during the Retry-After pause (was %d): the 429 pause did not stop re-seeds",
			during-pausedAt, pausedAt)
	}
	if st := b.DepthReseedStats(); st.Paused != 1 || st.Failed != 1 {
		t.Fatalf("stats after the 429 = %+v, want Paused=1 Failed=1", st)
	}

	// After the 1 s Retry-After elapses, a later diff must re-seed again.
	rest.setStatus(0, "")
	resumeDeadline := time.Now().Add(5 * time.Second)
	resumed := false
	for time.Now().Before(resumeDeadline) {
		u += 2
		v.write(t, 0, reseedDepthDiff(symbol, u))
		rest.setSeq(u)
		if now, _, _ := rest.stats(); now > during {
			resumed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !resumed {
		t.Fatalf("re-seeds never resumed after the Retry-After pause expired: %+v", b.DepthReseedStats())
	}
	rest.setFrozen(-1)
	for i := 0; i < 40; i++ {
		u++
		v.write(t, 0, reseedDepthDiff(symbol, u))
		rest.setSeq(u)
		time.Sleep(10 * time.Millisecond)
		if _, _, last := sink.snapshot(); last != nil && last.LastUpdateID == u {
			break
		}
	}
	wantSeq := u
	final := b.DepthReseedStats()
	t.Logf("429: stats_after_resume=%+v converged_seq=%d", final, wantSeq)
	if final.Fetched < 2 {
		t.Fatalf("only %d fetches: re-seeds never resumed after the pause (%+v)", final.Fetched, final)
	}
	_, _, last := sink.snapshot()
	if last == nil || last.LastUpdateID < wantSeq-2 {
		t.Fatalf("book did not converge after resuming: last update id %v, want ~%d", last, wantSeq)
	}
}

// ceilDiv returns ceil(d/unit) as an int.
func ceilDiv(d, unit time.Duration) int {
	if unit <= 0 {
		return 0
	}
	return int((d + unit - 1) / unit)
}

// TestDepthReseedGateSingleFlightAndInterval pins the two per-book rules
// deterministically (fake clock, no sockets): one fetch in flight per book, and
// the per-symbol minimum interval after it finishes.
func TestDepthReseedGateSingleFlightAndInterval(t *testing.T) {
	now := time.Unix(1700000000, 0)
	g := newDepthReseedGate(50*time.Millisecond, 100)
	g.now = func() time.Time { return now }

	btc := depthReseedKey{mkt: MarketPerp, symbol: "btcusdt"}
	eth := depthReseedKey{mkt: MarketPerp, symbol: "ethusdt"}

	if !g.admit(btc) {
		t.Fatalf("first trigger was refused: %+v", g.stats())
	}
	if g.admit(btc) {
		t.Fatalf("a second trigger was admitted while the first was in flight")
	}
	if st := g.stats(); st.Coalesced != 1 {
		t.Fatalf("coalesced = %d, want 1 (%+v)", st.Coalesced, st)
	}
	// A different book is not blocked by that flight.
	if !g.admit(eth) {
		t.Fatalf("an independent book was refused while another was in flight")
	}
	g.finish(eth, false, 0)
	g.finish(btc, false, 0)

	// Minimum interval: right after a fetch started, the same book is deferred;
	// once the interval has elapsed it is admitted again.
	if g.admit(btc) {
		t.Fatalf("the per-symbol minimum interval did not defer a trigger")
	}
	now = now.Add(60 * time.Millisecond)
	if !g.admit(btc) {
		t.Fatalf("the trigger after the minimum interval was still refused: %+v", g.stats())
	}
	g.finish(btc, false, 0)

	st := g.stats()
	if st.Triggers != st.Fetched+st.Coalesced+st.Deferred {
		t.Fatalf("ledger does not balance: %+v", st)
	}
	if st.Fetched != 3 || st.Coalesced != 1 || st.Deferred != 1 {
		t.Fatalf("ledger = %+v, want Fetched=3 Coalesced=1 Deferred=1", st)
	}
	t.Logf("single-flight+interval ledger: %+v", st)
}

// TestDepthReseedGateBudget pins the shared one-minute budget: it counts every
// symbol of the manager together, and a sliding window reopens it.
func TestDepthReseedGateBudget(t *testing.T) {
	now := time.Unix(1700000000, 0)
	g := newDepthReseedGate(time.Nanosecond, 3)
	g.now = func() time.Time { return now }

	keys := []depthReseedKey{
		{mkt: MarketPerp, symbol: "btcusdt"},
		{mkt: MarketPerp, symbol: "ethusdt"},
		{mkt: MarketSpot, symbol: "btcusdt"},
	}
	for _, k := range keys {
		if !g.admit(k) {
			t.Fatalf("budget refused %v while under the limit: %+v", k, g.stats())
		}
		g.finish(k, false, 0)
		now = now.Add(time.Millisecond)
	}
	if g.admit(keys[0]) {
		t.Fatalf("a 4th fetch inside one minute was admitted: budget is 3 (%+v)", g.stats())
	}
	now = now.Add(time.Minute + time.Second) // the window slides past the first three
	if !g.admit(keys[0]) {
		t.Fatalf("the sliding window did not reopen the budget: %+v", g.stats())
	}
	g.finish(keys[0], false, 0)
	if st := g.stats(); st.Triggers != st.Fetched+st.Coalesced+st.Deferred {
		t.Fatalf("ledger does not balance: %+v", st)
	}
	t.Logf("budget ledger: %+v", g.stats())
}

// TestDepthReseedGatePause pins the 429/418 pause: it stops every book, it is
// counted once per pause (an extension does count), it expires on its own, and
// a transport failure never pauses anything.
func TestDepthReseedGatePause(t *testing.T) {
	now := time.Unix(1700000000, 0)
	g := newDepthReseedGate(time.Millisecond, 100)
	g.now = func() time.Time { return now }

	btc := depthReseedKey{mkt: MarketPerp, symbol: "btcusdt"}
	eth := depthReseedKey{mkt: MarketPerp, symbol: "ethusdt"}
	if !g.admit(btc) {
		t.Fatalf("first trigger refused: %+v", g.stats())
	}
	g.finish(btc, true, 2*time.Second) // 429 with Retry-After: 2

	now = now.Add(time.Second)
	if g.admit(eth) {
		t.Fatalf("a trigger was admitted inside the Retry-After pause")
	}
	// A second rate-limit response inside the pause does not re-count it.
	if !g.admit(btc) && g.stats().Paused != 1 {
		t.Fatalf("pause counted twice inside one pause: %+v", g.stats())
	}
	now = now.Add(2 * time.Second)
	if !g.admit(eth) {
		t.Fatalf("triggers were still refused after the pause expired: %+v", g.stats())
	}
	g.finish(eth, true, 0) // transport failure: no pause
	now = now.Add(10 * time.Millisecond)
	if !g.admit(eth) {
		t.Fatalf("a transport failure paused the gate; only 429/418 may: %+v", g.stats())
	}
	g.finish(eth, false, 0)

	st := g.stats()
	if st.Triggers != st.Fetched+st.Coalesced+st.Deferred {
		t.Fatalf("ledger does not balance: %+v", st)
	}
	if st.Paused != 1 || st.Failed != 2 {
		t.Fatalf("stats = %+v, want Paused=1 Failed=2", st)
	}
	t.Logf("pause ledger: %+v", st)
}

// TestRetryAfterPauseParsing pins the header handling: only 429/418 pause, the
// delta-seconds form is honoured, and anything else falls back to 60 s.
func TestRetryAfterPauseParsing(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "17")
	if got := retryAfterPause(http.StatusTooManyRequests, h); got != 17*time.Second {
		t.Fatalf("429 Retry-After: 17 -> %v, want 17s", got)
	}
	if got := retryAfterPause(418, h); got != 17*time.Second {
		t.Fatalf("418 Retry-After: 17 -> %v, want 17s", got)
	}
	if got := retryAfterPause(http.StatusOK, h); got != 0 {
		t.Fatalf("200 -> %v, want no pause", got)
	}
	if got := retryAfterPause(http.StatusInternalServerError, h); got != 0 {
		t.Fatalf("500 -> %v, want no pause", got)
	}
	h.Set("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT")
	if got := retryAfterPause(http.StatusTooManyRequests, h); got != defaultDepthReseedPause {
		t.Fatalf("HTTP-date Retry-After -> %v, want the %v default", got, defaultDepthReseedPause)
	}
	h.Del("Retry-After")
	if got := retryAfterPause(http.StatusTooManyRequests, h); got != defaultDepthReseedPause {
		t.Fatalf("missing Retry-After -> %v, want the %v default", got, defaultDepthReseedPause)
	}
}
