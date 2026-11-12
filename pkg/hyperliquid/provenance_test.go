package hyperliquid

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Helpers: loopback venue, event recorder, waiting
// ─────────────────────────────────────────────────────────────

// scriptVenue is a loopback WebSocket venue that runs a per-connection script.
// It reads (and discards) everything the client writes, so subscription frames
// never block, and it counts the data frames each generation managed to write.
type scriptVenue struct {
	url string

	mu      sync.Mutex
	conns   []*coderws.Conn
	writes  map[int]int // data frames successfully written, per generation
	readErr map[int]error
}

func newScriptVenue(t *testing.T, script func(gen int, c *coderws.Conn, v *scriptVenue)) *scriptVenue {
	t.Helper()
	v := &scriptVenue{writes: map[int]int{}, readErr: map[int]error{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		gen := len(v.conns)
		v.conns = append(v.conns, c)
		v.mu.Unlock()
		go script(gen, c, v)
		// Drain client frames (subscribe/ping) so its writes never block.
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				v.mu.Lock()
				v.readErr[gen] = err
				v.mu.Unlock()
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(v.closeAll)
	v.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return v
}

func (v *scriptVenue) closeAll() {
	v.mu.Lock()
	conns := append([]*coderws.Conn(nil), v.conns...)
	v.mu.Unlock()
	for _, c := range conns {
		_ = c.CloseNow()
	}
}

func (v *scriptVenue) conn(gen int) *coderws.Conn {
	v.mu.Lock()
	defer v.mu.Unlock()
	if gen >= len(v.conns) {
		return nil
	}
	return v.conns[gen]
}

func (v *scriptVenue) written(gen int) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.writes[gen]
}

// writeFrames writes frames on c, pacing them so the client's read loop keeps
// up with the socket (the client's *dispatcher* is what the tests stall, not
// its reader). It records how many writes succeeded and stops at the first
// error — a write after the client dropped the socket must not count as sent.
func (v *scriptVenue) writeFrames(gen int, c *coderws.Conn, frames []string, pace time.Duration) {
	n := 0
	for _, raw := range frames {
		if err := c.Write(context.Background(), coderws.MessageText, []byte(raw)); err != nil {
			break
		}
		n++
		if pace > 0 {
			time.Sleep(pace)
		}
	}
	v.mu.Lock()
	v.writes[gen] = n
	v.mu.Unlock()
}

// eventRecorder stores every dispatched event in dispatch order.
type eventRecorder struct {
	mu     sync.Mutex
	events []any
}

func (r *eventRecorder) add(ev any) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *eventRecorder) snapshot() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.events...)
}

func (r *eventRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// newLoopback starts a stream manager against a script venue with the given
// queue depth. The manager is stopped at cleanup.
func newLoopback(t *testing.T, v *scriptVenue, queue int, d func(any), configure func(h *WSHyperliquid)) *WSHyperliquid {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	opts := DefaultOptions()
	opts.URL = v.url
	opts.EventQueueSize = queue
	opts.ReconnectIntervalMs = 10
	// Silence both keepalives: a ping frame would consume FrameIDs and make the
	// exact accounting harder to read.
	opts.AppPingIntervalMs = -1
	opts.WSPingIntervalMs = -1
	opts.DataStaleTimeoutMs = 0
	h := NewWithOptions(base, opts)
	if configure != nil {
		configure(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := h.Start(ctx, 10); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(h.Stop)
	return h
}

// ─────────────────────────────────────────────────────────────
// Checkpoint 1: provenance and exact accounting
// ─────────────────────────────────────────────────────────────

// eventRef extracts the H2 provenance fields from any Hyperliquid event and
// fails the test for a type that has none. It doubles as a compile-time check
// that every event type carries them.
func eventRef(t *testing.T, ev any) (frameID, connID uint64) {
	t.Helper()
	switch e := ev.(type) {
	case *connector.HyperliquidBookEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidTradeEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidBBOEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidAssetCtxEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidAllMidsEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidCandleEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidOrderUpdateEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidUserFillsEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidErrorEvent:
		return e.FrameID, e.ConnID
	case *connector.HyperliquidGapEvent:
		return e.FrameID, e.ConnID
	default:
		t.Fatalf("event %T has no H2 provenance fields", ev)
		return 0, 0
	}
}

// TestFrameAndConnIDOnEveryEvent feeds one frame per public/private channel and
// asserts every dispatched event carries the frame's ID and connection
// generation — including the error frame, which is easy to forget because it is
// built outside the handle* family.
func TestFrameAndConnIDOnEveryEvent(t *testing.T) {
	h, got := newTestHyperliquid(t)
	ctx := context.Background()
	for _, err := range []error{
		h.SubscribeL2Book(ctx, []string{"BTC"}, SubParams{}),
		h.SubscribeTrades(ctx, []string{"BTC"}),
		h.SubscribeBBO(ctx, []string{"BTC"}),
		h.SubscribeActiveAssetCtx(ctx, []string{"BTC"}),
		h.SubscribeAllMids(ctx),
		h.SubscribeCandles(ctx, []string{"BTC"}, "1m"),
		h.SubscribeOrderUpdates(ctx, testUser),
		h.SubscribeUserFills(ctx, testUser),
	} {
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	const wantFrame, wantConn = 42, 7
	frames := []string{
		`{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"1","sz":"1","n":1}],[]]}}`,
		`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1}]}`,
		`{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1","sz":"1","n":1},null]}}`,
		`{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.00001","oraclePx":"1","openInterest":"1"}}}`,
		`{"channel":"allMids","data":{"mids":{"BTC":"1"}}}`,
		`{"channel":"candle","data":[{"t":1,"T":2,"s":"BTC","i":"1m","o":"1","c":"1","h":"1","l":"1","v":"1","n":1}]}`,
		`{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"1","sz":"1","oid":1,"timestamp":1,"origSz":"1"},"status":"open","statusTimestamp":1}]}`,
		`{"channel":"userFills","data":{"user":"` + testUser + `","fills":[{"coin":"BTC","px":"1","sz":"1","side":"B","dir":"Open Long","closedPnl":"0","fee":"0","startPosition":"0","time":1,"oid":1,"tid":1}]}}`,
		`{"channel":"error","data":"Invalid subscription"}`,
	}
	for i, raw := range frames {
		h.processFrame(frame{data: []byte(raw), rx: time.Now(), id: wantFrame, connID: wantConn})
		if len(*got) <= i {
			t.Fatalf("frame %d produced no event: %s", i, raw)
		}
	}
	if len(*got) != len(frames) {
		t.Fatalf("events = %d, want %d: %+v", len(*got), len(frames), *got)
	}
	for i, ev := range *got {
		fid, cid := eventRef(t, ev)
		if fid != wantFrame || cid != wantConn {
			t.Errorf("event %d (%T): FrameID/ConnID = %d/%d, want %d/%d", i, ev, fid, cid, wantFrame, wantConn)
		}
	}
}

// TestEventQueueSizeOption pins the option's contract: zero is the default,
// a negative value is a caller bug that must fall back to the default (an
// unbuffered queue would drop nearly everything), and a positive value is used
// verbatim.
func TestEventQueueSizeOption(t *testing.T) {
	tests := []struct {
		name string
		opt  int
		want int
	}{
		{"zero is the default", 0, eventQueueSize},
		{"negative falls back to the default", -5, eventQueueSize},
		{"positive is used verbatim", 32, 32},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.EventQueueSize = tc.opt
			h := NewWithOptions(connector.New(false, nil), opts)
			st := h.Stats()
			if st.QueueCap != tc.want {
				t.Errorf("Stats().QueueCap = %d, want %d", st.QueueCap, tc.want)
			}
			if got := cap(h.conn.eventCh); got != tc.want {
				t.Errorf("queue capacity = %d, want %d", got, tc.want)
			}
			if st.QueueLen != 0 || st.FramesRead != 0 || st.FramesDropped != 0 || st.FramesDrained != 0 || st.QueueHighWater != 0 {
				t.Errorf("a fresh manager is not zeroed: %+v", st)
			}
			if st.ConnID != 0 {
				t.Errorf("ConnID = %d before the first connect, want 0", st.ConnID)
			}
		})
	}
}

// TestOnFrameDroppedQueueFull exercises the drop site directly: it fills the
// decode queue and asserts the next frame is dropped, counted and reported with
// the exact frame it discarded.
func TestOnFrameDroppedQueueFull(t *testing.T) {
	opts := DefaultOptions()
	opts.EventQueueSize = 2
	h := NewWithOptions(connector.New(false, nil), opts)
	h.conn.sm.connID.Store(3)

	var mu sync.Mutex
	var drops []DroppedFrame
	h.SetOnFrameDropped(func(df DroppedFrame) {
		mu.Lock()
		drops = append(drops, df)
		mu.Unlock()
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := h.conn.onMessage(ctx, []byte(`{"channel":"pong"}`)); err != nil {
			t.Fatalf("onMessage(%d): %v", i, err)
		}
	}
	if err := h.conn.onMessage(ctx, []byte(`{"channel":"pong"}`)); err != nil {
		t.Fatalf("onMessage(dropped): %v", err)
	}

	st := h.Stats()
	if st.FramesRead != 3 || st.FramesDropped != 1 || st.FramesDrained != 0 {
		t.Errorf("Stats = %+v, want read 3 / dropped 1 / drained 0", st)
	}
	if st.QueueLen != 2 || st.QueueHighWater != 2 || st.QueueCap != 2 {
		t.Errorf("queue accounting = len %d, high %d, cap %d; want 2/2/2", st.QueueLen, st.QueueHighWater, st.QueueCap)
	}
	if len(drops) != 1 {
		t.Fatalf("drop hook calls = %d, want 1", len(drops))
	}
	d := drops[0]
	if d.FrameID != 3 {
		t.Errorf("DroppedFrame.FrameID = %d, want 3 (the third frame read)", d.FrameID)
	}
	if d.ConnID != 3 || d.Cause != DropQueueFull || d.At.IsZero() {
		t.Errorf("DroppedFrame = %+v, want conn 3 / cause %q / non-zero At", d, DropQueueFull)
	}

	// A market-only connection keeps reading: nothing else was dispatched and
	// the connection was not failed.
	if st.Disconnects != 0 || st.ConnID != 3 {
		t.Errorf("market-only drop changed connection state: %+v", st)
	}

	// No hook: the same drop must be counted without panicking.
	h2 := NewWithOptions(connector.New(false, nil), opts)
	for i := 0; i < 3; i++ {
		if err := h2.conn.onMessage(ctx, []byte(`{"channel":"pong"}`)); err != nil {
			t.Fatalf("onMessage without hook: %v", err)
		}
	}
	if got := h2.Stats().FramesDropped; got != 1 {
		t.Errorf("FramesDropped without a hook = %d, want 1", got)
	}
}

// ── loopback accounting ──────────────────────────────────────

func bookFrame(i int) string {
	// `time` is distinct per frame so a consumer can correlate payload and
	// provenance; the levels never change.
	return fmt.Sprintf(`{"channel":"l2Book","data":{"coin":"BTC","time":%d,"levels":[[{"px":"100.0","sz":"1","n":1}],[{"px":"101.0","sz":"1","n":1}]]}}`, 1_700_000_000_000+int64(i))
}

func bookFrames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = bookFrame(i)
	}
	return out
}

// TestSlowConsumerExactDropAccounting is checkpoint 1's discriminating test: a
// stalled dispatcher plus a 16-slot queue against a 200-frame flood. Every
// number a consumer can observe must agree —
//
//	FramesDropped == the FrameID gap it can see == drop-hook calls
//	FramesRead    == delivered + dropped + drained
//	queue high-water == capacity (i.e. the queue really did fill)
//
// and the connection must NOT be dropped: this is a market-only connection,
// where a lost snapshot is superseded by the next one.
func TestSlowConsumerExactDropAccounting(t *testing.T) {
	const total, qcap = 200, 16
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)

	rec := &eventRecorder{}
	var hookMu sync.Mutex
	var hookDrops []DroppedFrame

	v := newScriptVenue(t, func(gen int, c *coderws.Conn, v *scriptVenue) {
		if gen != 0 {
			return // a market-only connection must not reconnect here
		}
		v.writeFrames(gen, c, bookFrames(total), 200*time.Microsecond)
	})

	h := newLoopback(t, v, qcap, func(ev any) {
		rec.add(ev) // record in dispatch order, then stall
		<-gate
	}, func(h *WSHyperliquid) {
		h.SetOnFrameDropped(func(df DroppedFrame) {
			hookMu.Lock()
			hookDrops = append(hookDrops, df)
			hookMu.Unlock()
		})
	})
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}

	waitUntil(t, "the flood to be read", 15*time.Second, func() bool {
		return h.Stats().FramesRead == total
	})

	full := h.Stats()
	if full.FramesDropped == 0 {
		t.Fatalf("no frame was dropped: the test proves nothing (stats %+v)", full)
	}
	if full.QueueLen != qcap || full.QueueHighWater != qcap {
		t.Fatalf("queue len/high-water = %d/%d, want %d/%d (the queue must be full while the dispatcher is stalled)",
			full.QueueLen, full.QueueHighWater, qcap, qcap)
	}
	if full.ConnID != 1 || full.Disconnects != 0 {
		t.Fatalf("market-only connection changed generation on a drop: %+v", full)
	}

	openGate()
	waitUntil(t, "the queue to drain", 15*time.Second, func() bool {
		st := h.Stats()
		return st.QueueLen == 0 && rec.count() == int(st.FramesRead-st.FramesDropped-st.FramesDrained)
	})

	st := h.Stats()
	t.Logf("market flood at rest: read=%d delivered=%d dropped=%d drained=%d cap=%d high-water=%d conn=%d",
		st.FramesRead, rec.count(), st.FramesDropped, st.FramesDrained, st.QueueCap, st.QueueHighWater, st.ConnID)
	if st.FramesDrained != 0 {
		t.Errorf("FramesDrained = %d, want 0: no disconnect happened", st.FramesDrained)
	}
	if st.Disconnects != 0 || st.ConnID != 1 {
		t.Errorf("Stats = %+v: a market-only flood must not fail the connection", st)
	}
	if got := st.FramesRead - st.FramesDropped - st.FramesDrained; got != uint64(rec.count()) {
		t.Errorf("delivered+dropped+drained = %d, delivered events = %d (read %d, dropped %d)",
			got, rec.count(), st.FramesRead, st.FramesDropped)
	}
	if st.QueueHighWater != qcap {
		t.Errorf("QueueHighWater = %d, want the capacity %d", st.QueueHighWater, qcap)
	}

	// The FrameID gap a consumer can see must equal the drop count, and the
	// frame IDs it did see must be the ones that were not dropped.
	seen := make(map[uint64]bool, total)
	for _, ev := range rec.snapshot() {
		b, ok := ev.(*connector.HyperliquidBookEvent)
		if !ok {
			t.Fatalf("unexpected event %T in a market-only stream", ev)
		}
		if b.ConnID != 1 {
			t.Errorf("event ConnID = %d, want 1", b.ConnID)
		}
		if b.FrameID == 0 || b.FrameID > total {
			t.Errorf("FrameID = %d outside 1..%d", b.FrameID, total)
		}
		if seen[b.FrameID] {
			t.Errorf("FrameID %d delivered twice", b.FrameID)
		}
		seen[b.FrameID] = true
	}
	missing := 0
	for id := uint64(1); id <= total; id++ {
		if !seen[id] {
			missing++
		}
	}
	if uint64(missing) != st.FramesDropped {
		t.Errorf("FrameID gaps seen by the consumer = %d, FramesDropped = %d", missing, st.FramesDropped)
	}

	hookMu.Lock()
	drops := append([]DroppedFrame(nil), hookDrops...)
	hookMu.Unlock()
	if uint64(len(drops)) != st.FramesDropped {
		t.Errorf("drop hook calls = %d, FramesDropped = %d", len(drops), st.FramesDropped)
	}
	hookIDs := make(map[uint64]bool, len(drops))
	for _, d := range drops {
		if d.Cause != DropQueueFull {
			t.Errorf("DroppedFrame cause = %q, want %q", d.Cause, DropQueueFull)
		}
		if d.ConnID != 1 {
			t.Errorf("DroppedFrame.ConnID = %d, want 1", d.ConnID)
		}
		if seen[d.FrameID] {
			t.Errorf("hook reported FrameID %d as dropped but it was delivered", d.FrameID)
		}
		hookIDs[d.FrameID] = true
	}
	for id := uint64(1); id <= total; id++ {
		if !seen[id] && !hookIDs[id] {
			t.Errorf("FrameID %d is missing from the consumer but was never reported dropped", id)
		}
	}
}

// TestDrainOnDisconnectIsCounted: the frames that were queued when the socket
// died must be counted (and reported), not silently discarded. This is the
// pre-existing loss path that checkpoint 3 turns into a gap event on account
// channels; on a market connection it stays counted loss.
func TestDrainOnDisconnectIsCounted(t *testing.T) {
	const total, qcap = 40, 16
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)

	rec := &eventRecorder{}
	var hookMu sync.Mutex
	var drainedFrames []DroppedFrame

	wrote := make(chan struct{})
	v := newScriptVenue(t, func(gen int, c *coderws.Conn, v *scriptVenue) {
		if gen != 0 {
			return
		}
		v.writeFrames(gen, c, bookFrames(total), 200*time.Microsecond)
		close(wrote)
	})

	h := newLoopback(t, v, qcap, func(ev any) {
		rec.add(ev)
		<-gate
	}, func(h *WSHyperliquid) {
		h.SetOnFrameDropped(func(df DroppedFrame) {
			hookMu.Lock()
			if df.Cause == DropDrainedOnDisconnect {
				drainedFrames = append(drainedFrames, df)
			}
			hookMu.Unlock()
		})
	})
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}

	waitUntil(t, "the flood to be read", 15*time.Second, func() bool {
		return h.Stats().FramesRead == total
	})
	<-wrote
	// The venue drops the socket; the client's queued frames die with it.
	if c := v.conn(0); c != nil {
		_ = c.CloseNow()
	}
	waitUntil(t, "the disconnect to be accounted", 15*time.Second, func() bool {
		return h.Stats().Disconnects >= 1
	})

	atDrop := h.Stats()
	if atDrop.FramesDrained != qcap {
		t.Errorf("FramesDrained = %d, want %d (the queue was full when the socket died)", atDrop.FramesDrained, qcap)
	}

	openGate()
	waitUntil(t, "the queue to drain and the connection to come back", 15*time.Second, func() bool {
		st := h.Stats()
		return st.ConnID >= 2 && st.QueueLen == 0 &&
			rec.count() == int(st.FramesRead-st.FramesDropped-st.FramesDrained)
	})

	st := h.Stats()
	if st.FramesRead != total {
		t.Errorf("FramesRead = %d, want %d", st.FramesRead, total)
	}
	if got := st.FramesRead - st.FramesDropped - st.FramesDrained; got != uint64(rec.count()) {
		t.Errorf("read - dropped - drained = %d, delivered = %d", got, rec.count())
	}
	hookMu.Lock()
	n := len(drainedFrames)
	hookMu.Unlock()
	if uint64(n) != st.FramesDrained {
		t.Errorf("DropDrainedOnDisconnect hook calls = %d, FramesDrained = %d", n, st.FramesDrained)
	}
	// The reconnect happened (subscription registry is non-empty) and the new
	// generation carries no queued leftovers.
	if st.ConnID < 2 {
		t.Errorf("ConnID = %d after a drop, want >= 2", st.ConnID)
	}
	if st.QueueLen != 0 {
		t.Errorf("QueueLen = %d at rest, want 0", st.QueueLen)
	}
}

// ─────────────────────────────────────────────────────────────
// Checkpoint 2: explicit markers instead of heuristics
// ─────────────────────────────────────────────────────────────

// TestTimestampSynthesized pins the flag against the data: true exactly when
// the venue omitted its time field, and never a change to Timestamp itself.
func TestTimestampSynthesized(t *testing.T) {
	h, got := newTestHyperliquid(t)
	ctx := context.Background()
	for _, err := range []error{
		h.SubscribeL2Book(ctx, []string{"BTC"}, SubParams{}),
		h.SubscribeBBO(ctx, []string{"BTC"}),
		h.SubscribeTrades(ctx, []string{"BTC"}),
	} {
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}

	const ms = int64(1_759_536_000_123)
	want := time.UnixMilli(ms)

	rx := time.Now()
	h.processFrame(frame{data: []byte(fmt.Sprintf(`{"channel":"l2Book","data":{"coin":"BTC","time":%d,"levels":[[],[]]}}`, ms)), rx: rx})
	h.processFrame(frame{data: []byte(`{"channel":"l2Book","data":{"coin":"BTC","levels":[[],[]]}}`), rx: rx})
	h.processFrame(frame{data: []byte(fmt.Sprintf(`{"channel":"bbo","data":{"coin":"BTC","time":%d,"bbo":[null,null]}}`, ms)), rx: rx})
	h.processFrame(frame{data: []byte(`{"channel":"bbo","data":{"coin":"BTC","bbo":[null,null]}}`), rx: rx})
	h.processFrame(frame{data: []byte(fmt.Sprintf(`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":%d,"tid":1}]}`, ms)), rx: rx})
	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","tid":2}]}`), rx: rx})

	if len(*got) != 6 {
		t.Fatalf("events = %d, want 6", len(*got))
	}
	check := func(name string, ev any, wantSynth bool) {
		t.Helper()
		var ts time.Time
		var synth bool
		switch e := ev.(type) {
		case *connector.HyperliquidBookEvent:
			ts, synth = e.Timestamp, e.TimestampSynthesized
		case *connector.HyperliquidBBOEvent:
			ts, synth = e.Timestamp, e.TimestampSynthesized
		case *connector.HyperliquidTradeEvent:
			ts, synth = e.Timestamp, e.TimestampSynthesized
		default:
			t.Fatalf("%s: unexpected event %T", name, ev)
		}
		if synth != wantSynth {
			t.Errorf("%s: TimestampSynthesized = %v, want %v", name, synth, wantSynth)
		}
		if wantSynth && !ts.Equal(rx) {
			t.Errorf("%s: synthesized Timestamp = %v, want the receive time %v", name, ts, rx)
		}
		if !wantSynth && !ts.Equal(want) {
			t.Errorf("%s: Timestamp = %v, want the payload ms epoch %v", name, ts, want)
		}
	}
	check("book with time", (*got)[0], false)
	check("book without time", (*got)[1], true)
	check("bbo with time", (*got)[2], false)
	check("bbo without time", (*got)[3], true)
	check("trade with time", (*got)[4], false)
	check("trade without time", (*got)[5], true)
}

// TestTradeBatchIndexLen pins the frame-relative grouping of trade prints: one
// venue frame can print several trades, and a consumer that reacts per-event
// must be able to tell "print 2 of 5" from "a lone print".
func TestTradeBatchIndexLen(t *testing.T) {
	h, got := newTestHyperliquid(t)
	ctx := context.Background()
	if err := h.SubscribeTrades(ctx, []string{"BTC", "ETH"}); err != nil {
		t.Fatalf("SubscribeTrades: %v", err)
	}

	// Three prints in one frame: two BTC (subscribed) and one ETH (subscribed).
	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[` +
		`{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1},` +
		`{"coin":"BTC","side":"A","px":"2","sz":"1","time":2,"tid":2},` +
		`{"coin":"ETH","side":"B","px":"3","sz":"1","time":3,"tid":3}]}`), rx: time.Now()})
	// A lone print is its own batch of one.
	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":4,"tid":4}]}`), rx: time.Now()})
	// A print for a coin we are not subscribed to is skipped, but the batch
	// length still describes the frame so the skip is visible.
	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[` +
		`{"coin":"BTC","side":"B","px":"1","sz":"1","time":5,"tid":5},` +
		`{"coin":"SOL","side":"B","px":"1","sz":"1","time":6,"tid":6},` +
		`{"coin":"ETH","side":"B","px":"1","sz":"1","time":7,"tid":7}]}`), rx: time.Now()})

	want := []struct {
		tid        int64
		idx, total int
	}{
		{1, 0, 3}, {2, 1, 3}, {3, 2, 3},
		{4, 0, 1},
		{5, 0, 3}, {7, 2, 3},
	}
	trades := make([]*connector.HyperliquidTradeEvent, 0, len(*got))
	for _, ev := range *got {
		trades = append(trades, ev.(*connector.HyperliquidTradeEvent))
	}
	if len(trades) != len(want) {
		t.Fatalf("trade events = %d, want %d: %+v", len(trades), len(want), trades)
	}
	for i, w := range want {
		if trades[i].TradeID != w.tid || trades[i].BatchIndex != w.idx || trades[i].BatchLen != w.total {
			t.Errorf("trade %d = tid %d idx %d len %d, want tid %d idx %d len %d",
				i, trades[i].TradeID, trades[i].BatchIndex, trades[i].BatchLen, w.tid, w.idx, w.total)
		}
	}
}

// TestAssetCtxSpotPerpFromPayloadEvidence is the D6 regression: the old
// heuristic classified "no funding and no oraclePx" as spot, so a perp context
// that omitted those two fields was silently delivered as spot. The venue
// documents the positive spot marker (SpotAssetCtx.circulatingSupply), so that
// is what the classifier must use.
func TestAssetCtxSpotPerpFromPayloadEvidence(t *testing.T) {
	h, got := newTestHyperliquid(t)
	ctx := context.Background()
	if err := h.SubscribeActiveAssetCtx(ctx, []string{"BTC", "PURR"}); err != nil {
		t.Fatalf("SubscribeActiveAssetCtx: %v", err)
	}

	tests := []struct {
		name string
		raw  string
		spot bool
	}{
		{
			name: "perp with the documented perp fields",
			raw:  `{"coin":"BTC","ctx":{"funding":"0.0000125","openInterest":"1234.5","oraclePx":"121000.0","markPx":"121001.0","dayNtlVlm":"1","prevDayPx":"1"}}`,
			spot: false,
		},
		{
			name: "perp missing funding AND oraclePx is still a perp",
			raw:  `{"coin":"BTC","ctx":{"openInterest":"1234.5","markPx":"121001.0","midPx":"121000.5","dayNtlVlm":"1","prevDayPx":"1"}}`,
			spot: false,
		},
		{
			name: "perp with no perp-only field at all is still a perp",
			raw:  `{"coin":"BTC","ctx":{"markPx":"121001.0","dayNtlVlm":"1","prevDayPx":"1"}}`,
			spot: false,
		},
		{
			name: "spot with the documented spot marker",
			raw:  `{"coin":"PURR","ctx":{"dayNtlVlm":"1000.0","prevDayPx":"0.5","markPx":"0.51","midPx":"0.505","circulatingSupply":"1000000"}}`,
			spot: true,
		},
		{
			name: "spot that also carries a stray funding field is still spot",
			raw:  `{"coin":"PURR","ctx":{"dayNtlVlm":"1","prevDayPx":"1","markPx":"1","circulatingSupply":"5","funding":"0.00001"}}`,
			spot: true,
		},
	}
	for i, tc := range tests {
		before := len(*got)
		coin := "BTC"
		if strings.Contains(tc.raw, "PURR") {
			coin = "PURR"
		}
		h.processFrame(frame{data: []byte(fmt.Sprintf(`{"channel":"activeAssetCtx","data":%s}`, tc.raw)), rx: time.Now()})
		if len(*got) != before+1 {
			t.Fatalf("%s: no event dispatched", tc.name)
		}
		ev := (*got)[before].(*connector.HyperliquidAssetCtxEvent)
		if ev.Coin != coin {
			t.Errorf("%s: coin = %q, want %q", tc.name, ev.Coin, coin)
		}
		if ev.IsSpot != tc.spot {
			t.Errorf("%s: IsSpot = %v, want %v (event %+v)", tc.name, ev.IsSpot, tc.spot, ev)
		}
		_ = i
	}
}

// ─────────────────────────────────────────────────────────────
// Checkpoint 3: no silent loss on account channels
// ─────────────────────────────────────────────────────────────

func userFillFrame(tid int64, coin string) string {
	return fmt.Sprintf(`{"channel":"userFills","data":{"user":"%s","fills":[{"coin":"%s","px":"100.0","sz":"1.0","side":"B","dir":"Open Long","closedPnl":"0","hash":"0x1","oid":%d,"crossed":true,"fee":"0.01","startPosition":"0","time":%d,"tid":%d}]}}`,
		testUser, coin, tid, 1_700_000_000_000+tid, tid)
}

// TestAccountOverflowFailsConnectionAndGaps is item 11a: a stalled consumer plus
// a full queue on an account-bearing connection must fail the connection, and
// the reconnect must announce the loss with a gap event that is dispatched
// before any frame of the new generation. Every fill the venue sent is either
// delivered or covered by that notice; nothing disappears between the two.
func TestAccountOverflowFailsConnectionAndGaps(t *testing.T) {
	const gen1Frames, gen2Frames, qcap = 40, 5, 16
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)

	rec := &eventRecorder{}
	var discMu sync.Mutex
	var discStats []Stats

	v := newScriptVenue(t, func(gen int, c *coderws.Conn, v *scriptVenue) {
		switch gen {
		case 0:
			frames := make([]string, gen1Frames)
			for i := range frames {
				frames[i] = userFillFrame(int64(1000+i), "BTC")
			}
			v.writeFrames(gen, c, frames, 200*time.Microsecond)
		case 1:
			time.Sleep(20 * time.Millisecond)
			frames := make([]string, gen2Frames)
			for i := range frames {
				frames[i] = userFillFrame(int64(2000+i), "ETH")
			}
			v.writeFrames(gen, c, frames, 200*time.Microsecond)
		}
	})

	h := newLoopback(t, v, qcap, func(ev any) {
		rec.add(ev)
		<-gate
	}, func(h *WSHyperliquid) {
		h.SetOnDisconnect(func(err error) {
			discMu.Lock()
			discStats = append(discStats, h.Stats())
			discMu.Unlock()
		})
	})
	if err := h.SubscribeUserFills(context.Background(), testUser); err != nil {
		t.Fatalf("SubscribeUserFills: %v", err)
	}

	waitUntil(t, "the account connection to be re-established", 20*time.Second, func() bool {
		return h.Stats().ConnID >= 2
	})
	openGate()
	waitUntil(t, "the gap notice and the new generation's fills", 20*time.Second, func() bool {
		var gaps, gen2 int
		for _, ev := range rec.snapshot() {
			switch e := ev.(type) {
			case *connector.HyperliquidGapEvent:
				gaps++
			case *connector.HyperliquidUserFillsEvent:
				if e.ConnID == 2 {
					gen2++
				}
			}
		}
		return gaps == 1 && gen2 == gen2Frames && h.Stats().QueueLen == 0
	})

	// ── ordering: all of generation 1, then the gap, then generation 2 ──
	events := rec.snapshot()
	gapIdx := -1
	var gap *connector.HyperliquidGapEvent
	var gen1Tids, gen2Tids []int64
	lastGen1, firstGen2 := -1, -1
	for i, ev := range events {
		switch e := ev.(type) {
		case *connector.HyperliquidGapEvent:
			if gapIdx >= 0 {
				t.Fatalf("two gap events for one reconnect: %+v", events)
			}
			gapIdx, gap = i, e
		case *connector.HyperliquidUserFillsEvent:
			for _, f := range e.Fills {
				if e.ConnID == 1 {
					gen1Tids = append(gen1Tids, f.Tid)
					lastGen1 = i
				} else if e.ConnID == 2 {
					gen2Tids = append(gen2Tids, f.Tid)
					if firstGen2 < 0 {
						firstGen2 = i
					}
				}
			}
		default:
			t.Fatalf("unexpected event %T", ev)
		}
	}
	if gap == nil {
		t.Fatal("no HyperliquidGapEvent after an account queue overflow")
	}
	if lastGen1 > gapIdx {
		t.Errorf("a generation-1 fill (index %d) was dispatched after the gap (index %d)", lastGen1, gapIdx)
	}
	if firstGen2 >= 0 && firstGen2 < gapIdx {
		t.Errorf("a generation-2 fill (index %d) was dispatched before the gap (index %d)", firstGen2, gapIdx)
	}
	if gap.Cause != connector.HyperliquidGapQueueOverflow {
		t.Errorf("gap cause = %q, want %q", gap.Cause, connector.HyperliquidGapQueueOverflow)
	}
	if !gap.AccountChannels {
		t.Error("gap.AccountChannels = false on an account-bearing connection")
	}
	if gap.PrevConnID != 1 || gap.ConnID != 2 {
		t.Errorf("gap PrevConnID/ConnID = %d/%d, want 1/2", gap.PrevConnID, gap.ConnID)
	}
	if gap.FrameID != 0 {
		t.Errorf("gap.FrameID = %d, want 0 (synthetic, consumes no wire frame)", gap.FrameID)
	}
	if gap.SeqID <= 0 {
		t.Errorf("gap.SeqID = %d, want a monotonic sequence number", gap.SeqID)
	}

	t.Logf("gap: cause=%s prev=%d conn=%d lost=%d account=%v seq=%d",
		gap.Cause, gap.PrevConnID, gap.ConnID, gap.LostFrames, gap.AccountChannels, gap.SeqID)
	t.Logf("generation 1 delivered tids=%v", gen1Tids)
	t.Logf("generation 2 delivered tids=%v", gen2Tids)

	// ── generation 1: exact accounting, and fills are a contiguous prefix ──
	discMu.Lock()
	if len(discStats) == 0 {
		discMu.Unlock()
		t.Fatal("OnDisconnect never fired: the overflow did not fail the connection")
	}
	at1 := discStats[0]
	discMu.Unlock()

	t.Logf("generation 1 at the overflow disconnect: read=%d dropped=%d drained=%d cap=%d high-water=%d",
		at1.FramesRead, at1.FramesDropped, at1.FramesDrained, at1.QueueCap, at1.QueueHighWater)
	if at1.ConnID != 1 {
		t.Errorf("disconnect happened on ConnID %d, want 1", at1.ConnID)
	}
	if at1.FramesDropped == 0 {
		t.Error("FramesDropped = 0 at the overflow disconnect")
	}
	if at1.FramesDrained != qcap {
		t.Errorf("FramesDrained = %d at the overflow disconnect, want %d (the full queue)", at1.FramesDrained, qcap)
	}
	if got := int64(at1.FramesRead) - int64(len(gen1Tids)) - int64(at1.FramesDropped+at1.FramesDrained); got != 0 {
		t.Errorf("generation 1: read %d, delivered %d, dropped %d, drained %d: %d frames unaccounted for",
			at1.FramesRead, len(gen1Tids), at1.FramesDropped, at1.FramesDrained, got)
	}
	if want := at1.FramesDropped + at1.FramesDrained; gap.LostFrames != want {
		t.Errorf("gap.LostFrames = %d, want %d (dropped+drained on generation 1)", gap.LostFrames, want)
	}
	if gap.LostFrames < qcap {
		t.Errorf("gap.LostFrames = %d, want at least the queue capacity %d", gap.LostFrames, qcap)
	}
	if written := v.written(0); written-len(gen1Tids) < int(gap.LostFrames) {
		// Wire-level loss (frames written but never read because the socket was
		// closed) can only make the true loss larger; the notice must never
		// claim more than the venue sent.
		t.Errorf("gap.LostFrames = %d exceeds written-delivered = %d", gap.LostFrames, written-len(gen1Tids))
	}

	// The delivered fills must be a contiguous run of tids from the start of the
	// stream: a hole before the gap would be a silent loss.
	if len(gen1Tids) == 0 {
		t.Fatal("no generation-1 fill was delivered: ordering cannot be checked")
	}
	for i := 1; i < len(gen1Tids); i++ {
		if gen1Tids[i] != gen1Tids[i-1]+1 {
			t.Errorf("generation-1 fills are not contiguous: %v (silent hole before the gap)", gen1Tids)
			break
		}
	}
	if gen1Tids[0] != 1000 {
		t.Errorf("first delivered tid = %d, want 1000", gen1Tids[0])
	}

	// ── generation 2 is intact after the gap ──
	if len(gen2Tids) != gen2Frames {
		t.Fatalf("generation-2 fills = %d, want %d", len(gen2Tids), gen2Frames)
	}
	for i, tid := range gen2Tids {
		if tid != int64(2000+i) {
			t.Errorf("generation-2 tid[%d] = %d, want %d", i, tid, 2000+i)
		}
	}

	// ── end state: the whole run balances ──
	st := h.Stats()
	delivered := len(gen1Tids) + len(gen2Tids) // one fill per frame in this test
	if got := st.FramesRead - st.FramesDropped - st.FramesDrained; got != uint64(delivered) {
		t.Errorf("read %d - dropped %d - drained %d = %d, delivered fills = %d",
			st.FramesRead, st.FramesDropped, st.FramesDrained, got, delivered)
	}
	if st.Disconnects != 1 {
		t.Errorf("Disconnects = %d, want 1 (only the overflow generation should have died)", st.Disconnects)
	}
	if st.ConnID != 2 {
		t.Errorf("ConnID = %d at rest, want 2", st.ConnID)
	}
}

// TestAccountDisconnectGapsWithNoLocalLoss is item 11b: a plain socket drop with
// an idle consumer loses nothing that was queued, but the account connection
// still announces the generation change — because the client cannot know what
// the venue sent while it was down, and a consumer that reconciles only on
// "LostFrames > 0" would silently miss that window.
func TestAccountDisconnectGapsWithNoLocalLoss(t *testing.T) {
	const gen1Frames, gen2Frames = 5, 2
	rec := &eventRecorder{}
	var mu sync.Mutex
	gen1Delivered := 0

	drop := make(chan struct{})
	v := newScriptVenue(t, func(gen int, c *coderws.Conn, v *scriptVenue) {
		switch gen {
		case 0:
			frames := make([]string, gen1Frames)
			for i := range frames {
				frames[i] = userFillFrame(int64(3000+i), "BTC")
			}
			v.writeFrames(gen, c, frames, 0)
			<-drop
			_ = c.CloseNow()
		case 1:
			time.Sleep(20 * time.Millisecond)
			frames := make([]string, gen2Frames)
			for i := range frames {
				frames[i] = userFillFrame(int64(4000+i), "ETH")
			}
			v.writeFrames(gen, c, frames, 0)
		}
	})

	h := newLoopback(t, v, 64, func(ev any) {
		mu.Lock()
		if _, ok := ev.(*connector.HyperliquidUserFillsEvent); ok {
			gen1Delivered++
		}
		mu.Unlock()
		rec.add(ev)
	}, nil)
	if err := h.SubscribeUserFills(context.Background(), testUser); err != nil {
		t.Fatalf("SubscribeUserFills: %v", err)
	}

	waitUntil(t, "the first generation's fills", 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gen1Delivered == gen1Frames && h.Stats().QueueLen == 0
	})
	close(drop)

	waitUntil(t, "the gap notice and the second generation", 20*time.Second, func() bool {
		var gaps, gen2 int
		for _, ev := range rec.snapshot() {
			switch e := ev.(type) {
			case *connector.HyperliquidGapEvent:
				gaps++
			case *connector.HyperliquidUserFillsEvent:
				if e.ConnID == 2 {
					gen2++
				}
			}
		}
		return gaps == 1 && gen2 == gen2Frames
	})

	events := rec.snapshot()
	gapIdx, firstGen2 := -1, -1
	var gap *connector.HyperliquidGapEvent
	for i, ev := range events {
		switch e := ev.(type) {
		case *connector.HyperliquidGapEvent:
			gapIdx, gap = i, e
		case *connector.HyperliquidUserFillsEvent:
			if e.ConnID == 2 && firstGen2 < 0 {
				firstGen2 = i
			}
		}
	}
	if gap == nil {
		t.Fatal("no gap event after a reconnect of an account-bearing connection")
	}
	t.Logf("gap: cause=%s prev=%d conn=%d lost=%d", gap.Cause, gap.PrevConnID, gap.ConnID, gap.LostFrames)
	if gap.Cause != connector.HyperliquidGapDisconnect {
		t.Errorf("gap cause = %q, want %q", gap.Cause, connector.HyperliquidGapDisconnect)
	}
	if gap.LostFrames != 0 {
		t.Errorf("gap.LostFrames = %d, want 0 (nothing was queued when the socket dropped)", gap.LostFrames)
	}
	if gap.PrevConnID != 1 || gap.ConnID != 2 {
		t.Errorf("gap PrevConnID/ConnID = %d/%d, want 1/2", gap.PrevConnID, gap.ConnID)
	}
	if gapIdx < gen1Frames {
		t.Errorf("gap dispatched at index %d, before the first generation's %d fills", gapIdx, gen1Frames)
	}
	if firstGen2 >= 0 && firstGen2 < gapIdx {
		t.Errorf("generation-2 fill at index %d precedes the gap at %d", firstGen2, gapIdx)
	}
}

// TestMarketOnlyFloodKeepsCountedLoss is item 11c: the same flood on a
// market-only connection must keep the pre-H2 behaviour — no connection failure,
// no gap event, exact counters — because the next book snapshot supersedes the
// lost one and a forced reconnect would manufacture a gap where none matters.
func TestMarketOnlyFloodKeepsCountedLoss(t *testing.T) {
	const total, qcap = 120, 8
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)

	rec := &eventRecorder{}
	v := newScriptVenue(t, func(gen int, c *coderws.Conn, v *scriptVenue) {
		if gen != 0 {
			return
		}
		v.writeFrames(gen, c, bookFrames(total), 200*time.Microsecond)
	})

	h := newLoopback(t, v, qcap, func(ev any) {
		rec.add(ev)
		<-gate
	}, nil)
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		t.Fatalf("SubscribeL2Book: %v", err)
	}

	waitUntil(t, "the flood to be read", 15*time.Second, func() bool {
		return h.Stats().FramesRead == total
	})
	openGate()
	waitUntil(t, "the queue to drain", 15*time.Second, func() bool {
		st := h.Stats()
		return st.QueueLen == 0 && rec.count() == int(st.FramesRead-st.FramesDropped-st.FramesDrained)
	})

	st := h.Stats()
	if st.FramesDropped == 0 {
		t.Fatalf("no drop happened: %+v", st)
	}
	if st.Disconnects != 0 || st.ConnID != 1 {
		t.Errorf("market-only connection was failed: %+v", st)
	}
	if st.FramesDrained != 0 {
		t.Errorf("FramesDrained = %d, want 0", st.FramesDrained)
	}
	for _, ev := range rec.snapshot() {
		if g, ok := ev.(*connector.HyperliquidGapEvent); ok {
			t.Fatalf("market-only connection produced a gap event: %+v", g)
		}
	}
	if uint64(rec.count())+st.FramesDropped != st.FramesRead {
		t.Errorf("delivered %d + dropped %d != read %d", rec.count(), st.FramesDropped, st.FramesRead)
	}
}

// ─────────────────────────────────────────────────────────────
// Cost of the accounting primitives (the read-loop hot path)
// ─────────────────────────────────────────────────────────────

func BenchmarkFrameAccountingEnqueueDequeue(b *testing.B) {
	var fa frameAccounting
	fa.queueCap = eventQueueSize
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fa.enqueued()
		fa.dequeued()
	}
}

func BenchmarkFrameAccountingDropNoHook(b *testing.B) {
	var fa frameAccounting
	fa.queueCap = eventQueueSize
	f := frame{id: 1, connID: 1, rx: time.Now()}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fa.noteDrop(f, DropQueueFull)
	}
}

func BenchmarkStatsSnapshot(b *testing.B) {
	var fa frameAccounting
	fa.queueCap = eventQueueSize
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fa.snapshot(1)
	}
}
