package binance

// soak_test.go is H4a's chaos soak for the Binance depth stream. Skipped unless
// CONNECTOR_SOAK=1:
//
//	CONNECTOR_SOAK=1 CONNECTOR_SOAK_SECONDS=60 CONNECTOR_SOAK_SEED=2 \
//	    go test -race -run Soak ./pkg/binance
//
// Loopback only: a fake stream endpoint plus a fake REST depth snapshot (both
// 127.0.0.1). The venue publishes a deterministic diff sequence (each diff sets
// bid 1.0 to its own sequence number, so the book at sequence u is exactly
// {1.0: u, 0.5: 7} and can be checked from any single event), while the soak
// drops sockets, sends bursts and runs a slow consumer against a tiny queue.
//
// Asserts: the consumer's book converges to the venue's final book; EVERY
// delivered diff is contiguous with the one before it (a divergence must have
// been healed by a re-seed, which is the only way a non-seed event can be
// out of sequence); each event's own book is consistent with its
// LastUpdateID; FramesRead == delivered + dropped + drained + no-event frames,
// with the no-event frames explained as re-seed triggers/coalesced reads; and
// the H3 identity for depth — FrameID gaps == FramesDropped + re-seeds — holds
// up to those explained frames, which are reported separately.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Environment / PRNG / leak helpers
// ─────────────────────────────────────────────────────────────

func soakParams(t *testing.T) (seed int64, dur time.Duration) {
	t.Helper()
	if os.Getenv("CONNECTOR_SOAK") != "1" {
		t.Skip("chaos soak: set CONNECTOR_SOAK=1 to run")
	}
	seed = 1
	if s := os.Getenv("CONNECTOR_SOAK_SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatalf("CONNECTOR_SOAK_SEED=%q: %v", s, err)
		}
		seed = v
	}
	secs := 20
	if s := os.Getenv("CONNECTOR_SOAK_SECONDS"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			t.Fatalf("CONNECTOR_SOAK_SECONDS=%q: want a positive integer", s)
		}
		secs = v
	}
	t.Logf("soak: seed=%d duration=%ds", seed, secs)
	return seed, time.Duration(secs) * time.Second
}

type soakRNG struct {
	mu sync.Mutex
	s  uint64
}

func newSoakRNG(seed int64) *soakRNG {
	s := uint64(seed)*0x9E3779B97F4A7C15 + 0x2545F4914F6CDD1D
	if s == 0 {
		s = 0x9E3779B97F4A7C15
	}
	return &soakRNG{s: s}
}

func (r *soakRNG) next() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s ^= r.s << 13
	r.s ^= r.s >> 7
	r.s ^= r.s << 17
	return r.s
}

func (r *soakRNG) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func (r *soakRNG) duration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(r.next()%uint64(max-min))
}

func soakAssertNoLeak(t *testing.T, what string, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			t.Logf("%s: goroutines=%d (baseline %d) after teardown", what, n, baseline)
			return
		}
		if time.Now().After(deadline) {
			var b strings.Builder
			_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
			t.Fatalf("%s: goroutines=%d after teardown, baseline=%d\n%s", what, n, baseline, b.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ─────────────────────────────────────────────────────────────
// Live fake depth REST: always the venue's current book
// ─────────────────────────────────────────────────────────────

// snapshotLookback is how far behind the leading edge the fake REST snapshot
// sits, in diff sequence numbers.
const snapshotLookback = 3

// soakDepthREST serves {"lastUpdateId": seq, bids: [[1.0, seq],[0.5, 7]]}
// where seq is the last diff the venue published. That is exactly the venue's
// book at that sequence, so a re-seed can never disagree with the stream.
type soakDepthREST struct {
	seq   atomic.Int64
	calls atomic.Int64
	// lookback is how far behind the leading edge the fake snapshot sits. It
	// starts at snapshotLookback (a venue snapshot that lags the stream) and is
	// set to 0 for the convergence phase, where the stream must be able to
	// continue contiguously from the snapshot.
	lookback atomic.Int64
	url      string
	srv      *httptest.Server
}

func newSoakDepthREST(t *testing.T) *soakDepthREST {
	t.Helper()
	d := &soakDepthREST{}
	d.lookback.Store(snapshotLookback)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.calls.Add(1)
		// A real snapshot lags the leading edge of the stream by a few
		// messages. Returning exactly the leading edge would make every queued
		// diff stale and the connector would re-seed on every one of them; a
		// small lookback keeps the documented flow (snapshot at S, diffs S+1..
		// applied) intact while still leaving a few frames to be coalesced.
		s := d.seq.Load() - d.lookback.Load()
		if s < 0 {
			s = 0
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"lastUpdateId":%d,"bids":[["1.0","%d"],["0.5","7"]],"asks":[["2.0","3"]]}`, s, s)
	}))
	d.srv = srv
	t.Cleanup(srv.Close)
	d.url = srv.URL
	return d
}

func (d *soakDepthREST) close() { d.srv.Close() }

// ─────────────────────────────────────────────────────────────
// Book model (what a consumer builds from the typed events)
// ─────────────────────────────────────────────────────────────

type soakBook struct {
	bids map[string]string
	asks map[string]string
}

func newSoakBook() *soakBook {
	return &soakBook{bids: map[string]string{}, asks: map[string]string{}}
}

func (b *soakBook) replace(levels []connector.Level, dst map[string]string) {
	for k := range dst {
		delete(dst, k)
	}
	for _, l := range levels {
		dst[l.Price] = l.Size
	}
}

func (b *soakBook) equal(o *soakBook) bool {
	if len(b.bids) != len(o.bids) || len(b.asks) != len(o.asks) {
		return false
	}
	for k, v := range b.bids {
		if o.bids[k] != v {
			return false
		}
	}
	for k, v := range b.asks {
		if o.asks[k] != v {
			return false
		}
	}
	return true
}

func (b *soakBook) String() string {
	bids := make([]string, 0, len(b.bids))
	for k, v := range b.bids {
		bids = append(bids, k+"="+v)
	}
	sort.Strings(bids)
	asks := make([]string, 0, len(b.asks))
	for k, v := range b.asks {
		asks = append(asks, k+"="+v)
	}
	sort.Strings(asks)
	return "bids[" + strings.Join(bids, ",") + "] asks[" + strings.Join(asks, ",") + "]"
}

// ─────────────────────────────────────────────────────────────
// Scenario
// ─────────────────────────────────────────────────────────────

func TestSoakBinanceDepth(t *testing.T) {
	seed, dur := soakParams(t)
	rng := newSoakRNG(seed + 11)
	scenarioStart := time.Now()

	const queue = 8
	v := newLoVenue(t)
	rest := newSoakDepthREST(t)
	// Baseline after the two fake servers exist: their accept loops are part of
	// the environment, not of the manager under test.
	baseline := runtime.NumGoroutine()

	var mu sync.Mutex
	var events []*connector.BinanceDepthEvent
	consumer := newSoakBook()
	d := func(ev any) {
		e, ok := ev.(*connector.BinanceDepthEvent)
		if !ok {
			return
		}
		mu.Lock()
		events = append(events, e)
		consumer.replace(e.Bids, consumer.bids)
		consumer.replace(e.Asks, consumer.asks)
		n := len(events)
		mu.Unlock()
		// A slow consumer with occasional stalls: the 8-slot queue fills and
		// frames are dropped, which is what forces the re-seed path.
		if rng.intn(25) == 0 {
			time.Sleep(rng.duration(10*time.Millisecond, 60*time.Millisecond))
		} else if n%3 == 0 {
			time.Sleep(time.Duration(rng.intn(2)) * time.Millisecond)
		}
	}

	b, _ := newLoopbackBinance(t, v, rest.url, queue, d, func(b *WSBinance) {
		b.SubscribeDepth(nil, MarketPerp, []string{"BTCUSDT"})
	})
	_ = b
	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })
	waitForBinance(t, "the initial REST seed", 5*time.Second, func() bool {
		return rest.calls.Load() >= 1
	})

	var droppedMu sync.Mutex
	var droppedFrames []connector.DroppedFrame
	b.SetOnFrameDropped(func(f connector.DroppedFrame) {
		droppedMu.Lock()
		droppedFrames = append(droppedFrames, f)
		droppedMu.Unlock()
	})

	stopAt := time.Now().Add(dur)
	var seq int64 // last sequence published by the venue
	var injectedDrops int
	var muSeq sync.Mutex
	publish := func(s int64) {
		muSeq.Lock()
		seq = s
		muSeq.Unlock()
		rest.seq.Store(s)
	}
	// soakWrite is v.write without the t.Fatalf: a dead socket is expected here.
	soakWrite := func(gen int, frame string) bool {
		c := v.conn(gen)
		if c == nil {
			return false
		}
		return c.Write(context.Background(), coderws.MessageText, []byte(frame)) == nil
	}

	// The last 1.5 s are calm: pacing only, no bursts. The self-heal is "on the
	// next diff", so a venue that goes quiet right after a loss legitimately
	// leaves the book stale (documented in H3); the calm tail is what lets the
	// convergence claim be checked instead of that residual.
	calmFrom := stopAt.Add(-1500 * time.Millisecond)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for time.Now().Before(stopAt) {
			gen := v.numConns() - 1
			if gen < 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			burst := 1
			if time.Now().Before(calmFrom) && rng.intn(100) < 8 {
				burst = 5 + rng.intn(25)
			}
			for i := 0; i < burst && time.Now().Before(stopAt); i++ {
				muSeq.Lock()
				u := seq + 1
				muSeq.Unlock()
				if !soakWrite(gen, depthDiff(u)) {
					// The write failed, so the venue did not publish this diff:
					// the leading edge only advances for frames that reached a
					// socket. (A real venue's book state exists independently of
					// the subscriber, but the snapshot's job here is to be
					// consistent with what was actually streamed.)
					time.Sleep(time.Millisecond)
					break
				}
				muSeq.Lock()
				if u > seq {
					seq = u
				}
				muSeq.Unlock()
				publish(u)
				if burst == 1 {
					if time.Now().Before(calmFrom) {
						time.Sleep(rng.duration(1*time.Millisecond, 4*time.Millisecond))
					} else {
						time.Sleep(5 * time.Millisecond)
					}
				}
			}
			if time.Now().Before(calmFrom) && rng.intn(100) < 3 {
				time.Sleep(rng.duration(5*time.Millisecond, 30*time.Millisecond))
			}
		}
	}()
	dropDone := make(chan struct{})
	// A generation is dropped at most once: closing an already dead socket
	// produces no Disconnects increment, so counting it would make
	// "Disconnects >= injected drops" fail for a reason the connector does not
	// control.
	var dropMu sync.Mutex
	droppedGen := map[int]bool{}
	go func() {
		defer close(dropDone)
		for time.Now().Before(stopAt) {
			time.Sleep(rng.duration(700*time.Millisecond, 3*time.Second))
			if time.Until(stopAt) < 700*time.Millisecond {
				return
			}
			gen := v.numConns() - 1
			if gen < 0 {
				continue
			}
			dropMu.Lock()
			if droppedGen[gen] {
				dropMu.Unlock()
				continue
			}
			droppedGen[gen] = true
			dropMu.Unlock()
			v.closeGen(gen)
			injectedDrops++
		}
	}()
	<-writerDone
	<-dropDone
	// Drain: wait for the queue to empty and the counters to stop moving.
	deadline := time.Now().Add(10 * time.Second)
	prev, _ := b.Stats(MarketPerp)
	for {
		stats, _ := b.Stats(MarketPerp)
		if stats.QueueLen == 0 && stats.FramesRead == prev.FramesRead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not settle: %+v", stats)
		}
		prev = stats
		time.Sleep(100 * time.Millisecond)
	}
	// Convergence after the chaos. Pre-H4c every gap frame re-seeded instantly,
	// so the book tracked the leading edge while the venue kept publishing; the
	// gate now allows ONE gap repair per minimum interval, and a venue that keeps
	// publishing faster than this (deliberately slow) consumer re-gaps the book
	// faster than the gate may heal it. So the phase is split in two:
	//
	//   1. repair: keep triggering with paced diffs until the gate admits a
	//      re-seed (at most one minimum interval of waiting);
	//   2. converge: stop publishing, let the queue drain, then send the tail one
	//      diff at a time and require each to be delivered and applied — every
	//      diff after a seed is contiguous with it, so the consumer's book must
	//      reach the venue's final book.
	//
	// The reconnect backoff is exponential up to ReconnectMaxInterval (30 s
	// default) and only resets after a connection that survives; a run with ten
	// drops can legitimately be waiting most of half a minute for the next dial,
	// during which no frame can be published at all. 45 s covers that cap.
	recoverDeadline := time.Now().Add(45 * time.Second)
	appliedSeq := func() int64 {
		mu.Lock()
		defer mu.Unlock()
		if n := len(events); n > 0 {
			return events[n-1].LastUpdateID
		}
		return -1
	}
	seedCalls := rest.calls.Load()
	for {
		if rest.calls.Load() > seedCalls {
			break // a gap repair was admitted after the burst
		}
		if time.Now().After(recoverDeadline) {
			stNow, _ := b.Stats(MarketPerp)
			t.Fatalf("no re-seed was admitted after the burst within 45s: applied=%d conns=%d connID=%d reads=%d dropped=%d rest=%d gate=%+v",
				appliedSeq(), v.numConns(), stNow.ConnID, stNow.FramesRead, stNow.FramesDropped, rest.calls.Load(), b.DepthReseedStats())
		}
		gen := v.numConns() - 1
		if gen < 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		muSeq.Lock()
		u := seq + 1
		muSeq.Unlock()
		if !soakWrite(gen, depthDiff(u)) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		muSeq.Lock()
		if u > seq {
			seq = u
		}
		muSeq.Unlock()
		publish(u)
		time.Sleep(10 * time.Millisecond)
	}
	// Drain: nothing new is published, so the consumer reaches the last frame it
	// was sent (or the book is still unseeded and the gate needs another
	// interval — the deadline allows for that).
	drainDeadline := time.Now().Add(20 * time.Second)
	for {
		muSeq.Lock()
		leading := seq
		muSeq.Unlock()
		if appliedSeq() == leading {
			break
		}
		if time.Now().After(drainDeadline) {
			stNow, _ := b.Stats(MarketPerp)
			t.Logf("depth: drain stopped at applied=%d published=%d (queue=%d) — continuing with the paced tail",
				appliedSeq(), leading, stNow.QueueLen)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// From here on the venue's snapshot is the leading edge itself, so the diff
	// stream can continue contiguously from whatever a seed returns.
	rest.lookback.Store(0)

	// Settle: a slow (100 ms) contiguous stream, which this consumer can absorb,
	// until the applied sequence meets the leading edge. A dropped frame leaves
	// one gap and the gate may defer its repair by up to the minimum interval
	// (1 s default), so the loop allows 30 s — the claim is that the book
	// converges once the flood stops, not that it converges frame for frame.
	convergeDeadline := time.Now().Add(30 * time.Second)
	nextConvergeLog := time.Now().Add(5 * time.Second)
	for {
		muSeq.Lock()
		leading := seq
		muSeq.Unlock()
		if appliedSeq() == leading {
			break
		}
		if time.Now().After(convergeDeadline) {
			stNow, _ := b.Stats(MarketPerp)
			t.Fatalf("consumer's book never caught up: applied=%d leading=%d conns=%d connID=%d reads=%d dropped=%d queue=%d rest=%d gate=%+v",
				appliedSeq(), leading, v.numConns(), stNow.ConnID, stNow.FramesRead, stNow.FramesDropped, stNow.QueueLen, rest.calls.Load(), b.DepthReseedStats())
		}
		if time.Now().After(nextConvergeLog) {
			nextConvergeLog = time.Now().Add(5 * time.Second)
			stNow, _ := b.Stats(MarketPerp)
			t.Logf("depth: converging: applied=%d published=%d conns=%d connID=%d queue=%d gate=%+v",
				appliedSeq(), leading, v.numConns(), stNow.ConnID, stNow.QueueLen, b.DepthReseedStats())
		}
		gen := v.numConns() - 1
		if gen < 0 {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		muSeq.Lock()
		u := seq + 1
		muSeq.Unlock()
		if !soakWrite(gen, depthDiff(u)) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		muSeq.Lock()
		if u > seq {
			seq = u
		}
		muSeq.Unlock()
		publish(u)
		time.Sleep(100 * time.Millisecond)
	}
	muSeq.Lock()
	finalSeq := seq
	muSeq.Unlock()

	st, okStats := b.Stats(MarketPerp)
	if !okStats {
		t.Fatalf("Stats(MarketPerp) reported no connection")
	}
	// The re-seed gate's ledger, read before Stop so it describes the run: every
	// gap-triggered re-seed the stream asked for and how it was resolved.
	rs := b.DepthReseedStats()
	stopStart := time.Now()
	b.Stop()
	stopTook := time.Since(stopStart)
	mu.Lock()
	evs := append([]*connector.BinanceDepthEvent(nil), events...)
	gotBook := &soakBook{bids: map[string]string{}, asks: map[string]string{}}
	for k, val := range consumer.bids {
		gotBook.bids[k] = val
	}
	for k, val := range consumer.asks {
		gotBook.asks[k] = val
	}
	mu.Unlock()
	droppedMu.Lock()
	droppedFs := append([]connector.DroppedFrame(nil), droppedFrames...)
	droppedMu.Unlock()

	// ── observed numbers ──
	var seeds int
	var deliveredIDs []uint64
	prevID := int64(0)
	contiguous := true
	consistent := true
	for _, e := range evs {
		if e.FrameID == 0 {
			seeds++
			prevID = e.LastUpdateID
			continue
		}
		deliveredIDs = append(deliveredIDs, e.FrameID)
		if e.LastUpdateID != prevID+1 {
			contiguous = false
		}
		prevID = e.LastUpdateID
		bids := map[string]string{}
		for _, l := range e.Bids {
			bids[l.Price] = l.Size
		}
		if bids["1.0"] != strconv.FormatInt(e.LastUpdateID, 10) || bids["0.5"] != "7" {
			consistent = false
		}
	}
	queueFull, drainedHook := 0, 0
	for _, f := range droppedFs {
		switch f.Cause {
		case connector.DropQueueFull:
			queueFull++
		case connector.DropDrainedOnDisconnect:
			drainedHook++
		default:
			t.Fatalf("drop hook reported undocumented cause %q", f.Cause)
		}
	}
	read := int(st.FramesRead)
	delivered := len(deliveredIDs)
	// noEvent: read frames that produced no event and were not counted as lost:
	// re-seed triggers and the frames coalesced by a snapshot that came back
	// ahead of them. They are invisible individually, so they are bounded and
	// reported, and the identity is asserted with them as an explicit term.
	noEvent := read - delivered - int(st.FramesDropped) - int(st.FramesDrained)
	reseeds := seeds
	if reseeds > 0 {
		reseeds-- // the first seed is the on-connect one, not a repair
	}
	present := make(map[uint64]bool, delivered)
	for _, id := range deliveredIDs {
		present[id] = true
	}
	gaps := 0
	for id := uint64(1); id <= uint64(read); id++ {
		if !present[id] {
			gaps++
		}
	}

	venueBook := &soakBook{bids: map[string]string{"1.0": strconv.FormatInt(finalSeq, 10), "0.5": "7"}, asks: map[string]string{"2.0": "3"}}

	t.Logf("depth: dur=%v reads=%d delivered=%d seeds=%d reseeds=%d dropped=%d drained=%d no_event=%d gaps=%d rest_calls=%d disconnects=%d connID=%d accepts=%d drops=%d gate=%+v close=%v",
		dur, read, delivered, seeds, reseeds, st.FramesDropped, st.FramesDrained, noEvent, gaps, rest.calls.Load(), st.Disconnects, st.ConnID, v.numConns(), injectedDrops, rs, stopTook)
	t.Logf("depth: final venue book %s / consumer book %s", venueBook, gotBook)

	// ── assertions ──
	if read != delivered+int(st.FramesDropped)+int(st.FramesDrained)+noEvent {
		t.Fatalf("FramesRead %d != delivered %d + dropped %d + drained %d + no_event %d",
			read, delivered, st.FramesDropped, st.FramesDrained, noEvent)
	}
	if noEvent < 0 {
		t.Fatalf("negative no_event term: %d (frames were counted twice)", noEvent)
	}
	if queueFull != int(st.FramesDropped) {
		t.Fatalf("drop hook reported %d queue_full drops, Stats.FramesDropped = %d", queueFull, st.FramesDropped)
	}
	if drainedHook != int(st.FramesDrained) {
		t.Fatalf("drop hook reported %d drained frames, Stats.FramesDrained = %d", drainedHook, st.FramesDrained)
	}
	// Convergence at every step: a delivered diff is always contiguous with the
	// previous one (any divergence is healed by a re-seed, which resets the
	// sequence); and every event's own book matches its LastUpdateID.
	if !contiguous {
		t.Fatalf("a delivered diff was not contiguous with its predecessor (a divergence window was not healed by a re-seed)")
	}
	if !consistent {
		t.Fatalf("a delivered event's book disagreed with its own LastUpdateID (diffs were applied to a stale book)")
	}
	// H3's documented depth identity: consumer-visible FrameID gaps are exactly
	// dropped + drained + the frames that produced no event (re-seed triggers
	// and coalesced reads), and every re-seed accounts for at least one of them.
	if gaps != int(st.FramesDropped)+int(st.FramesDrained)+noEvent {
		t.Fatalf("FrameID gaps %d != dropped %d + drained %d + no_event %d", gaps, st.FramesDropped, st.FramesDrained, noEvent)
	}
	// H4c: the no-event frames are triggers plus stale skips, not re-seeds.
	// Every frame that found the book unseeded or gapped is a trigger and
	// produces no event, whether the gate spawned its fetch or deferred it; the
	// only other no-event source is a diff the fresh snapshot had already
	// overtaken (at most snapshotLookback per seed); the +4 absorbs the boundary
	// at the start/end of the run. The gate's ledger is the authority here, so
	// the identity is asserted against it.
	if noEvent < int(rs.Triggers) {
		t.Fatalf("no_event frames %d < %d gate triggers: a trigger frame must produce no event", noEvent, rs.Triggers)
	}
	if staleBound := int(rs.Triggers) + (snapshotLookback+1)*seeds + 4; noEvent > staleBound {
		t.Fatalf("no_event frames %d exceed triggers %d + stale skips (%d seeds at lookback %d) + 4 = %d",
			noEvent, rs.Triggers, seeds, snapshotLookback, staleBound)
	}
	// The gate's own ledger must balance, and every admitted fetch must be a
	// REST call that actually happened (the initial per-connect seeding is not
	// in the ledger, so it is added back from the number of accepts).
	if rs.Triggers != rs.Fetched+rs.Coalesced+rs.Deferred {
		t.Fatalf("gate ledger does not balance: %+v", rs)
	}
	// H4c D1: the REST call rate is bounded by the OPTIONS, not by the frame
	// rate. Calls are: one initial seed per accept (ungated) + one per admitted
	// gap-triggered re-seed, which the shared budget caps at DepthReseedPerMin
	// per minute of run.
	perMin := b.opts.DepthReseedPerMin
	minInterval := b.opts.DepthReseedMinInterval
	calls := rest.calls.Load()
	if ident := int64(v.numConns()) + int64(rs.Fetched); calls > ident+2 {
		t.Fatalf("%d REST calls but only %d accepts + %d admitted re-seeds (+2 slack): the gate did not account for every fetch",
			calls, v.numConns(), rs.Fetched)
	}
	budgetBound := int64(v.numConns()) + int64(perMin)*(int64(dur/time.Minute)+1) + 2
	if calls > budgetBound {
		t.Fatalf("%d REST snapshots in %v exceed the options' bound %d (accepts %d + %d/min for %v)",
			calls, dur, budgetBound, v.numConns(), perMin, dur)
	}
	if rs.Fetched > 0 && minInterval <= 0 {
		t.Fatalf("gate counted %d gap-triggered re-seeds with a zero minimum interval: options were not resolved", rs.Fetched)
	}
	// Final convergence: the consumer's book equals the venue's final book.
	if !gotBook.equal(venueBook) {
		t.Fatalf("consumer book %s != venue book %s after draining", gotBook, venueBook)
	}
	if reseeds == 0 && st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped and no re-seed happened in %v — the chaos never exercised the self-heal path", dur)
	}
	if st.Disconnects < uint64(injectedDrops) {
		t.Fatalf("Disconnects %d < injected drops %d", st.Disconnects, injectedDrops)
	}
	// One generation per drop is expected. Only the LAST drop may still be
	// waiting for its next scheduled dial (exponential flap backoff, capped at
	// 30 s, with a drop every 0.7-3 s), so ConnID >= injectedDrops is required
	// and ConnID == injectedDrops+1 is the clean case, reported when it is not.
	if st.ConnID < uint64(injectedDrops) {
		t.Fatalf("ConnID %d after %d injected drops and %d accepts", st.ConnID, injectedDrops, v.numConns())
	}
	if st.ConnID != uint64(injectedDrops)+1 {
		t.Logf("depth: ConnID=%d with %d drops and %d accepts: the last drop's reconnect is still in backoff at teardown",
			st.ConnID, injectedDrops, v.numConns())
	}
	if injectedDrops > 0 && rest.calls.Load() < 1 {
		t.Fatalf("injected %d drops but the REST seed was never called", injectedDrops)
	}
	dialBound := injectedDrops + int(dur/(20*time.Millisecond)) + 16
	if conns := v.numConns(); conns > dialBound {
		t.Fatalf("%d dials for %d drops in %v (bound %d)", conns, injectedDrops, dur, dialBound)
	}
	if stopTook > 2*time.Second {
		t.Fatalf("Stop() took %v (> 2s)", stopTook)
	}
	for i := 0; i < v.numConns(); i++ {
		v.closeGen(i)
	}
	rest.close()
	soakAssertNoLeak(t, "depth", baseline)
	t.Logf("depth: wall=%v", time.Since(scenarioStart))
}
