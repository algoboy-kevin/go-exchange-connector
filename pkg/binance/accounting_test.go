package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Helpers: loopback venue, fake depth REST, waiting
// ─────────────────────────────────────────────────────────────

// loVenue is a loopback Binance stream endpoint. It accepts connections,
// drains everything the client writes (subscribe/ping), and lets the test
// write frames on a generation at will, so the exact interleaving of reads and
// drops is under the test's control.
type loVenue struct {
	url string

	mu    sync.Mutex
	conns []*coderws.Conn
}

func newLoVenue(t *testing.T) *loVenue {
	t.Helper()
	v := &loVenue{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		v.conns = append(v.conns, c)
		v.mu.Unlock()
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		v.mu.Lock()
		conns := append([]*coderws.Conn(nil), v.conns...)
		v.mu.Unlock()
		for _, c := range conns {
			_ = c.CloseNow()
		}
	})
	v.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return v
}

func (v *loVenue) conn(gen int) *coderws.Conn {
	v.mu.Lock()
	defer v.mu.Unlock()
	if gen >= len(v.conns) {
		return nil
	}
	return v.conns[gen]
}

func (v *loVenue) numConns() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

// write sends one frame on generation gen; a write error fails the test because
// the frame the test believes it sent would never have been read.
func (v *loVenue) write(t *testing.T, gen int, frame string) {
	t.Helper()
	c := v.conn(gen)
	if c == nil {
		t.Fatalf("write on generation %d: no such connection", gen)
	}
	if err := c.Write(context.Background(), coderws.MessageText, []byte(frame)); err != nil {
		t.Fatalf("write on generation %d: %v", gen, err)
	}
}

// closeGen drops the venue side of a connection.
func (v *loVenue) closeGen(gen int) {
	v.mu.Lock()
	var c *coderws.Conn
	if gen < len(v.conns) {
		c = v.conns[gen]
	}
	v.mu.Unlock()
	if c != nil {
		_ = c.CloseNow()
	}
}

// depthREST is a fake Binance REST depth endpoint. The first snapshot (the
// on-connect seed) is lastUpdateId 100; every later snapshot is lastUpdateId
// 200 and carries a marker level the seed does not have, so a test can tell a
// re-seed from the initial seed.
type depthREST struct {
	calls    atomic.Int64
	lastSeed atomic.Int64
	url      string
}

func newDepthREST(t *testing.T) *depthREST {
	t.Helper()
	d := &depthREST{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := d.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if n == 1 {
			d.lastSeed.Store(100)
			fmt.Fprint(w, `{"lastUpdateId":100,"bids":[["1.0","100"],["0.5","7"]],"asks":[["2.0","3"]]}`)
			return
		}
		d.lastSeed.Store(200)
		fmt.Fprint(w, `{"lastUpdateId":200,"bids":[["1.0","200"],["0.5","7"],["0.25","9"]],"asks":[["2.0","3"]]}`)
	}))
	t.Cleanup(srv.Close)
	d.url = srv.URL
	return d
}

func waitForBinance(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// newLoopbackBinance starts a WSBinance whose perp "public" connection (the one
// that carries bookTicker and diff-depth) points at the loopback venue and whose
// perp REST base points at the fake snapshot server. Only that connection is
// dialled: the spot and perp "market" endpoints keep their production URLs and
// are never connected, so no test can reach a real venue.
func newLoopbackBinance(t *testing.T, v *loVenue, rest string, queue int, d func(any), subscribe func(*WSBinance)) (*WSBinance, *binanceConn) {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	b := NewWithOptions(base, Options{
		PerpMarketWS:   v.url,
		PerpPublicWS:   v.url,
		PerpREST:       rest,
		EventQueueSize: queue,
	})
	c := b.connFor(MarketPerp, classPublic)

	// Subscriptions are registered before the dial: onConnect re-sends them and,
	// for depth, takes the REST seed for every subscribed symbol.
	if subscribe != nil {
		subscribe(b)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b.rootCx = ctx

	dispatchCtx, dispatchCancel := context.WithCancel(ctx)
	c.dispatchCancel = dispatchCancel
	go b.eventDispatcher(dispatchCtx, c)

	opts := ws.DefaultWSOptions()
	opts.PingInterval = 20000 // longer than any test: no keepalive noise
	opts.ReconnectInterval = 20
	if err := c.Connect(ctx, c.wsURL(), opts); err != nil {
		t.Fatalf("loopback connect: %v", err)
	}
	t.Cleanup(b.Stop)
	return b, c
}

// ─────────────────────────────────────────────────────────────
// Options / Stats surface
// ─────────────────────────────────────────────────────────────

// TestBinanceEventQueueSizeOption pins the resolution rules: 0 and negative
// fall back to the 8192 default (a negative is a caller bug, not a request for
// an unbuffered queue that would drop nearly every frame), and Stats reports
// the resolved value. MarketPerp has two connections, so its reported caps are
// the sum of both.
func TestBinanceEventQueueSizeOption(t *testing.T) {
	if got := (Options{}).resolved().EventQueueSize; got != defaultEventQueueSize {
		t.Errorf("zero value resolved EventQueueSize = %d, want %d", got, defaultEventQueueSize)
	}
	if got := (Options{EventQueueSize: -5}).resolved().EventQueueSize; got != defaultEventQueueSize {
		t.Errorf("negative EventQueueSize resolved to %d, want %d", got, defaultEventQueueSize)
	}
	if got := (Options{EventQueueSize: 32}).resolved().EventQueueSize; got != 32 {
		t.Errorf("EventQueueSize 32 resolved to %d, want 32", got)
	}

	base := connector.New(false, nil)
	// Default: spot = 8192 (one connection), perp = 2 × 8192.
	def := New(base)
	if st, ok := def.Stats(MarketSpot); !ok || st.QueueCap != defaultEventQueueSize {
		t.Errorf("default spot QueueCap = %d (ok=%v), want %d", st.QueueCap, ok, defaultEventQueueSize)
	}
	if st, ok := def.Stats(MarketPerp); !ok || st.QueueCap != 2*defaultEventQueueSize {
		t.Errorf("default perp QueueCap = %d (ok=%v), want %d", st.QueueCap, ok, 2*defaultEventQueueSize)
	}
	// Fresh manager: nothing read, nothing dropped, generation 0.
	if st, _ := def.Stats(MarketPerp); st.FramesRead != 0 || st.FramesDropped != 0 || st.ConnID != 0 || st.Disconnects != 0 {
		t.Errorf("fresh manager stats = %+v, want zeroed", st)
	}

	small := NewWithOptions(base, Options{EventQueueSize: 32})
	if st, _ := small.Stats(MarketSpot); st.QueueCap != 32 {
		t.Errorf("spot QueueCap = %d, want 32", st.QueueCap)
	}
	if st, _ := small.Stats(MarketPerp); st.QueueCap != 64 {
		t.Errorf("perp QueueCap = %d, want 64 (2 connections × 32)", st.QueueCap)
	}

	if _, ok := def.Stats(MarketType("futures")); ok {
		t.Error("Stats(invalid market) reported ok, want false")
	}
}

// TestBinanceFrameAndConnIDOnEveryEvent proves every typed event these streams
// dispatch copies the frame's provenance, at the decode seam where the frame's
// ID and generation are already attached.
func TestBinanceFrameAndConnIDOnEveryEvent(t *testing.T) {
	b, got := newTestBinance(t)
	ctx := context.Background()
	b.SubscribeBookTicker(ctx, MarketPerp, []string{"btcusdt"})
	b.SubscribePartialDepth(ctx, MarketPerp, []string{"btcusdt"}, 20, "100ms")
	b.SubscribeTrades(ctx, MarketPerp, []string{"btcusdt"})
	if err := b.SubscribeKlines(ctx, MarketPerp, []string{"btcusdt"}, "1m"); err != nil {
		t.Fatalf("SubscribeKlines: %v", err)
	}
	b.SubscribeMarkPrice(ctx, MarketPerp, []string{"btcusdt"})

	public := b.connFor(MarketPerp, classPublic)
	market := b.connFor(MarketPerp, classMarket)
	cases := []struct {
		name string
		conn *binanceConn
		raw  string
	}{
		{"bookTicker", public, `{"e":"bookTicker","u":400900217,"E":1568014460893,"s":"BTCUSDT","b":"25.3519","B":"31.21","a":"25.3652","A":"40.66"}`},
		{"partialDepth", public, `{"e":"depthUpdate","E":1568014460893,"s":"BTCUSDT","U":1,"u":2,"b":[["25.0","1"]],"a":[]}`},
		{"aggTrade", market, `{"e":"aggTrade","E":1568014460893,"s":"BTCUSDT","a":5933014,"p":"0.001","q":"100","f":100,"l":105,"T":1568014460891,"m":true}`},
		{"kline", market, `{"e":"kline","E":1568014460893,"s":"BTCUSDT","k":{"t":1568014440000,"T":1568014499999,"i":"1m","o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":false}}`},
		{"markPrice", market, `{"e":"markPriceUpdate","E":1568014460893,"s":"BTCUSDT","p":"25.35","i":"25.36","P":"25.00","r":"0.0001","T":1568014460893}`},
	}
	for i, tc := range cases {
		before := len(*got)
		f := binanceFrame{data: []byte(tc.raw), id: uint64(100 + i), connID: 7}
		b.processMessage(tc.conn, f)
		if len(*got) != before+1 {
			t.Fatalf("%s: dispatched %d events, want 1", tc.name, len(*got)-before)
		}
		ev := (*got)[len(*got)-1]
		var fid, cid uint64
		switch e := ev.(type) {
		case *connector.BinanceBookTickerEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.BinanceDepthEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.BinanceAggTradeEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.BinanceKlineEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.BinanceMarkPriceEvent:
			fid, cid = e.FrameID, e.ConnID
		default:
			t.Fatalf("%s: unexpected event type %T", tc.name, ev)
		}
		if fid != uint64(100+i) || cid != 7 {
			t.Errorf("%s: FrameID=%d ConnID=%d, want %d/7", tc.name, fid, cid, 100+i)
		}
	}

}

// ─────────────────────────────────────────────────────────────
// Exact accounting under a slow consumer
// ─────────────────────────────────────────────────────────────

func bookTickerFrame(seq int64) string {
	return fmt.Sprintf(`{"e":"bookTicker","u":%d,"E":1568014460893,"s":"BTCUSDT","b":"%d","B":"1","a":"%d","A":"1"}`,
		seq, 1000+seq, 2000+seq)
}

// TestBinanceSlowConsumerExactDropAccounting floods the perp public connection
// with numbered bookTicker frames while the dispatcher is stalled and a tiny
// queue is full, then proves the loss is exactly accounted:
//
//	FramesRead == delivered + dropped + drained
//	FrameID gaps seen by the consumer == FramesDropped == hook calls
//
// and that no frame is reported dropped whose event was delivered.
func TestBinanceSlowConsumerExactDropAccounting(t *testing.T) {
	const total = 200
	const queue = 16

	v := newLoVenue(t)
	var mu sync.Mutex
	delivered := make(map[uint64]int) // FrameID -> times delivered
	droppedIDs := make(map[uint64]int)
	gate := make(chan struct{})

	d := func(ev any) {
		e, ok := ev.(*connector.BinanceBookTickerEvent)
		if !ok {
			return
		}
		mu.Lock()
		delivered[e.FrameID]++
		n := len(delivered)
		mu.Unlock()
		if n == 1 && e.FrameID == 1 {
			<-gate // stall the dispatcher on the first event
		}
	}

	b, _ := newLoopbackBinance(t, v, "http://127.0.0.1:1", queue, d, func(b *WSBinance) {
		b.SetOnFrameDropped(func(df connector.DroppedFrame) {
			mu.Lock()
			droppedIDs[df.FrameID]++
			mu.Unlock()
		})
		b.SubscribeBookTicker(context.Background(), MarketPerp, []string{"BTCUSDT"})
	})

	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, bookTickerFrame(i))
		time.Sleep(200 * time.Microsecond) // the reader keeps up; the dispatcher does not
	}
	waitForBinance(t, "all frames read", 5*time.Second, func() bool {
		st, _ := b.Stats(MarketPerp)
		return st.FramesRead == total
	})

	st, _ := b.Stats(MarketPerp)
	if st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped; the test did not exercise the drop path (stats %+v)", st)
	}
	if st.QueueHighWater != queue || st.QueueCap != queue*2 { // perp = 2 connections
		t.Errorf("QueueHighWater=%d QueueCap=%d, want %d/%d", st.QueueHighWater, st.QueueCap, queue, queue*2)
	}

	close(gate)
	waitForBinance(t, "the queue to drain", 5*time.Second, func() bool {
		st, _ := b.Stats(MarketPerp)
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		return st.QueueLen == 0 && n == int(total-st.FramesDropped)
	})

	st, _ = b.Stats(MarketPerp)
	mu.Lock()
	defer mu.Unlock()

	if st.FramesDrained != 0 {
		t.Errorf("FramesDrained = %d on a live connection, want 0", st.FramesDrained)
	}
	if int(st.FramesRead) != len(delivered)+int(st.FramesDropped) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d", st.FramesRead, len(delivered), st.FramesDropped)
	}
	// FrameID gaps == drops: every ID in 1..total is either delivered or
	// reported dropped, never both.
	var missing []uint64
	for i := uint64(1); i <= total; i++ {
		if _, ok := delivered[i]; !ok {
			missing = append(missing, i)
		}
	}
	if uint64(len(missing)) != st.FramesDropped {
		t.Fatalf("FrameID gaps = %d (%v), FramesDropped = %d", len(missing), missing, st.FramesDropped)
	}
	for _, id := range missing {
		if droppedIDs[id] != 1 {
			t.Errorf("frame %d missing from delivery but hook calls = %d, want 1", id, droppedIDs[id])
		}
	}
	for id := range delivered {
		if droppedIDs[id] != 0 {
			t.Errorf("frame %d was delivered AND reported dropped", id)
		}
	}
	if len(droppedIDs) != len(missing) {
		t.Errorf("hook reported %d frames, want %d", len(droppedIDs), len(missing))
	}
	if st.ConnID != 1 || st.Disconnects != 0 {
		t.Errorf("market flood changed the connection state: ConnID=%d Disconnects=%d", st.ConnID, st.Disconnects)
	}
}

// TestBinanceDrainOnDisconnectIsCounted covers the other loss path: frames
// queued behind a stalled dispatcher when the venue drops the socket are
// counted as drained and reported to the hook, so the identity holds across the
// reconnect.
func TestBinanceDrainOnDisconnectIsCounted(t *testing.T) {
	const total = 40
	const queue = 16

	v := newLoVenue(t)
	var mu sync.Mutex
	delivered := 0
	drainedIDs := make(map[uint64]int)
	gate := make(chan struct{})

	d := func(ev any) {
		if _, ok := ev.(*connector.BinanceBookTickerEvent); !ok {
			return
		}
		mu.Lock()
		delivered++
		first := delivered == 1
		mu.Unlock()
		if first {
			<-gate
		}
	}

	b, _ := newLoopbackBinance(t, v, "http://127.0.0.1:1", queue, d, func(b *WSBinance) {
		b.SetOnFrameDropped(func(df connector.DroppedFrame) {
			if df.Cause != connector.DropDrainedOnDisconnect {
				return
			}
			mu.Lock()
			drainedIDs[df.FrameID]++
			mu.Unlock()
		})
		b.SubscribeBookTicker(context.Background(), MarketPerp, []string{"BTCUSDT"})
	})
	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, bookTickerFrame(i))
		time.Sleep(200 * time.Microsecond)
	}
	waitForBinance(t, "all frames read", 5*time.Second, func() bool {
		st, _ := b.Stats(MarketPerp)
		return st.FramesRead == total
	})

	v.closeGen(0) // venue drops the socket with the queue full
	waitForBinance(t, "the disconnect to be counted", 5*time.Second, func() bool {
		st, _ := b.Stats(MarketPerp)
		return st.Disconnects >= 1 && st.FramesDrained > 0
	})
	close(gate)

	st, _ := b.Stats(MarketPerp)
	mu.Lock()
	defer mu.Unlock()
	if st.FramesDrained != queue {
		t.Errorf("FramesDrained = %d, want %d (the whole queue)", st.FramesDrained, queue)
	}
	if len(drainedIDs) != int(st.FramesDrained) {
		t.Errorf("hook reported %d drained frames, want %d", len(drainedIDs), st.FramesDrained)
	}
	waitForBinance(t, "the queue to drain", 5*time.Second, func() bool {
		s, _ := b.Stats(MarketPerp)
		return s.QueueLen == 0
	})
	st, _ = b.Stats(MarketPerp)
	got := delivered
	if int(st.FramesRead) != got+int(st.FramesDropped)+int(st.FramesDrained) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d + drained %d",
			st.FramesRead, got, st.FramesDropped, st.FramesDrained)
	}
	waitForBinance(t, "the reconnect to establish a new generation", 5*time.Second, func() bool {
		s, _ := b.Stats(MarketPerp)
		return s.ConnID >= 2
	})
}

// ─────────────────────────────────────────────────────────────
// Depth-gap self-heal (deliverable 2's stateful claim)
// ─────────────────────────────────────────────────────────────

// depthDiff builds a diff-depth frame that sets one bid level to qty=u.
func depthDiff(u int64) string {
	return fmt.Sprintf(`{"e":"depthUpdate","E":1568014460893,"s":"BTCUSDT","U":%d,"u":%d,"b":[["1.0","%d"]],"a":[]}`, u, u, u)
}

// TestBinanceDepthDropSelfHeals proves the stateful claim: a diff-depth frame
// dropped because the queue was full is detected at the next diff
// (FirstUpdateID > lastUpdateID+1), the book is re-seeded from the REST
// snapshot, and the event stream after the re-seed is consistent (the seeded
// levels are there and later diffs apply on top).
//
// The drop happens *after* the book is snapshotted, which is the only case
// where a lost frame can leave stale state; a drop before the seed is benign
// because the handler re-requests a snapshot for every diff while unsnapshotted.
func TestBinanceDepthDropSelfHeals(t *testing.T) {
	const queue = 8
	const burst = 20 // diffs 101..120

	v := newLoVenue(t)
	rest := newDepthREST(t)

	var mu sync.Mutex
	var events []*connector.BinanceDepthEvent
	gate := make(chan struct{})
	d := func(ev any) {
		e, ok := ev.(*connector.BinanceDepthEvent)
		if !ok {
			return
		}
		mu.Lock()
		events = append(events, e)
		n := len(events)
		mu.Unlock()
		if n == 3 { // seed, diff 101, diff 102 -> stall here
			<-gate
		}
	}

	b, _ := newLoopbackBinance(t, v, rest.url, queue, d, func(b *WSBinance) {
		b.SubscribeDepth(context.Background(), MarketPerp, []string{"BTCUSDT"})
	})
	waitForBinance(t, "the loopback connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	// The on-connect seed must have landed before the diffs start.
	waitForBinance(t, "the initial REST seed", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 1 && events[0].FrameID == 0 && events[0].LastUpdateID == 100
	})

	for i := int64(101); i < 101+burst; i++ {
		v.write(t, 0, depthDiff(i))
	}
	waitForBinance(t, "the burst to be read", 5*time.Second, func() bool {
		st, _ := b.Stats(MarketPerp)
		return st.FramesRead == burst
	})

	st, _ := b.Stats(MarketPerp)
	if st.FramesDropped == 0 {
		t.Fatalf("no diff frame was dropped; the test did not exercise the self-heal path (stats %+v)", st)
	}

	close(gate)
	waitForBinance(t, "the queued diffs to apply", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.FrameID > 0 && e.LastUpdateID == 110 {
				return true
			}
		}
		return false
	})

	// One frame past the hole: this is what reveals the loss.
	v.write(t, 0, depthDiff(121))
	waitForBinance(t, "the re-seed", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.FrameID == 0 && e.LastUpdateID == 200 {
				return true
			}
		}
		return false
	})

	// Diffs on top of the re-seeded book must apply normally and keep the
	// seeded levels: the stream after the re-seed is consistent.
	for i := int64(201); i <= 205; i++ {
		v.write(t, 0, depthDiff(i))
		time.Sleep(2 * time.Millisecond)
	}
	waitForBinance(t, "post-seed diffs to apply", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.LastUpdateID == 205 {
				return true
			}
		}
		return false
	})

	mu.Lock()
	defer mu.Unlock()

	st, _ = b.Stats(MarketPerp)
	deliveredFrames := map[uint64]bool{}
	var reseedIdx = -1
	for i, e := range events {
		if e.ConnID != 1 {
			t.Errorf("event LastUpdateID=%d ConnID=%d, want 1", e.LastUpdateID, e.ConnID)
		}
		if e.FrameID == 0 {
			if e.LastUpdateID == 200 && reseedIdx < 0 {
				reseedIdx = i
			}
			continue
		}
		deliveredFrames[e.FrameID] = true
	}
	if reseedIdx < 0 {
		t.Fatalf("no REST re-seed event observed; events=%d", len(events))
	}

	// Identity. Every frame read is one of:
	//   - delivered (it produced an event), or
	//   - reported dropped / drained, or
	//   - a self-heal TRIGGER: the diff whose FirstUpdateID proved the hole.
	// The trigger frame is consumed by design (its levels are superseded by the
	// snapshot that replaces the book), so it is neither delivered nor a loss —
	// it is visible as the FrameID-0 re-seed event that follows it. This test
	// sent exactly one such frame (u=121); a consumer counts them by counting
	// re-seeds, and must not read a FrameID gap as pure loss on a depth stream.
	const triggers = 1
	if int(st.FramesRead) != len(deliveredFrames)+int(st.FramesDropped)+int(st.FramesDrained)+triggers {
		t.Errorf("FramesRead %d != delivered frames %d + dropped %d + drained %d + triggers %d",
			st.FramesRead, len(deliveredFrames), st.FramesDropped, st.FramesDrained, triggers)
	}
	var gaps []uint64
	for i := uint64(1); i <= st.FramesRead; i++ {
		if !deliveredFrames[i] {
			gaps = append(gaps, i)
		}
	}
	if uint64(len(gaps)) != st.FramesDropped+triggers {
		t.Errorf("FrameID gaps = %d (%v), want dropped %d + triggers %d", len(gaps), gaps, st.FramesDropped, triggers)
	}

	// Ordering: the re-seed comes after everything before the hole and before
	// every post-seed event.
	var post []*connector.BinanceDepthEvent
	for _, e := range events[reseedIdx+1:] {
		if e.LastUpdateID >= 201 {
			post = append(post, e)
		}
	}
	if len(post) == 0 {
		t.Fatal("no depth event after the re-seed")
	}
	for i, e := range post {
		want := int64(201 + i)
		if e.LastUpdateID != want {
			t.Errorf("post-seed event %d has LastUpdateID=%d, want %d (events after the re-seed must be contiguous)", i, e.LastUpdateID, want)
		}
		if e.FrameID == 0 {
			t.Errorf("post-seed event %d has FrameID 0, want the wire frame it came from", i)
		}
		seedLevel, diffLevel := false, false
		for _, l := range e.Bids {
			if l.Price == "0.25" && l.Size == "9" {
				seedLevel = true
			}
			if l.Price == "1.0" && l.Size == strconv.FormatInt(e.LastUpdateID, 10) {
				diffLevel = true
			}
		}
		if !seedLevel {
			t.Errorf("post-seed event LastUpdateID=%d lost the seeded level 0.25/9: book=%v", e.LastUpdateID, e.Bids)
		}
		if !diffLevel {
			t.Errorf("post-seed event LastUpdateID=%d did not apply its diff at 1.0: book=%v", e.LastUpdateID, e.Bids)
		}
	}
}
