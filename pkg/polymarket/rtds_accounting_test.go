package polymarket

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Helpers: loopback RTDS venue
// ─────────────────────────────────────────────────────────────

type rtdsVenue struct {
	url string

	mu    sync.Mutex
	conns []*coderws.Conn
}

func newRTDSVenue(t *testing.T) *rtdsVenue {
	t.Helper()
	v := &rtdsVenue{}
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

func (v *rtdsVenue) numConns() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

func (v *rtdsVenue) conn(gen int) *coderws.Conn {
	v.mu.Lock()
	defer v.mu.Unlock()
	if gen >= len(v.conns) {
		return nil
	}
	return v.conns[gen]
}

func (v *rtdsVenue) write(t *testing.T, gen int, frame string) {
	t.Helper()
	c := v.conn(gen)
	if c == nil {
		t.Fatalf("write on generation %d: no such connection", gen)
	}
	if err := c.Write(context.Background(), coderws.MessageText, []byte(frame)); err != nil {
		t.Fatalf("write on generation %d: %v", gen, err)
	}
}

func (v *rtdsVenue) closeGen(gen int) {
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

func waitForRTDS(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// startRTDSLoopback starts an RTDS manager against the loopback venue with the
// given queue depth and worker count. The crypto-price subscription is queued
// before Start, so the connect frame carries it.
func startRTDSLoopback(t *testing.T, v *rtdsVenue, queue, workers int, d func(any), configure func(r *WSPolymarketRTDS)) *WSPolymarketRTDS {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	r := NewWSPolymarketRTDS(base)
	r.SetEventQueueSize(queue)
	r.SetDispatcherWorkers(workers)
	if configure != nil {
		configure(r)
	}
	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt"})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := r.Start(ctx, v.url, 20); err != nil {
		t.Fatalf("rtds Start: %v", err)
	}
	t.Cleanup(r.Stop)
	return r
}

// cryptoPriceFrame is one crypto_prices update whose price identifies it.
func cryptoPriceFrame(n int64) string {
	return fmt.Sprintf(`{"topic":"crypto_prices","type":"update","timestamp":%d,"payload":{"symbol":"btcusdt","timestamp":%d,"value":%d,"full_accuracy_value":"%d"}}`,
		1787312179781+n, 1787312179781+n, n, n)
}

// ─────────────────────────────────────────────────────────────
// Options / Stats / provenance
// ─────────────────────────────────────────────────────────────

// TestRTDSEventQueueSizeAndStats pins the queue-size option (default 8192
// unchanged) and that a fresh manager reports zeroed counters.
func TestRTDSEventQueueSizeAndStats(t *testing.T) {
	base := connector.New(false, nil)
	def := NewWSPolymarketRTDS(base)
	if got := def.Stats().QueueCap; got != defaultRTDSEventQueueSize {
		t.Errorf("default QueueCap = %d, want %d", got, defaultRTDSEventQueueSize)
	}
	def.SetEventQueueSize(16)
	if got := def.Stats().QueueCap; got != 16 {
		t.Errorf("QueueCap after SetEventQueueSize(16) = %d, want 16", got)
	}
	def.SetEventQueueSize(-1) // negative restores the default, never unbuffered
	if got := def.Stats().QueueCap; got != defaultRTDSEventQueueSize {
		t.Errorf("QueueCap after SetEventQueueSize(-1) = %d, want %d", got, defaultRTDSEventQueueSize)
	}
	if st := def.Stats(); st.FramesRead != 0 || st.FramesDropped != 0 || st.ConnID != 0 {
		t.Errorf("fresh rtds stats = %+v, want zeroed", st)
	}
	if got := def.dispatcherWorkers; got != 4 {
		t.Errorf("default dispatcher workers = %d, want 4 (unchanged)", got)
	}
}

// TestRTDSFrameAndConnIDOnEveryEvent proves every typed event the RTDS channel
// dispatches carries the frame's provenance.
func TestRTDSFrameAndConnIDOnEveryEvent(t *testing.T) {
	base := connector.New(false, nil)
	var got []any
	base.SetDispatcher(func(ev any) { got = append(got, ev) })

	r := NewWSPolymarketRTDS(base)
	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt"})
	r.SubscribeChainlinkPrices(context.Background(), []string{"eth/usd"})
	r.SubscribeEquityPrices(context.Background(), []string{"aapl"})
	r.SubscribeChainlinkTWAP(context.Background(), 60, []string{"btc/usd"})

	cases := []struct {
		name string
		raw  string
	}{
		{"crypto", cryptoPriceFrame(1)},
		{"chainlink", `{"topic":"crypto_prices_chainlink","type":"update","timestamp":1787312179781,"payload":{"symbol":"eth/usd","timestamp":1787312179781,"value":3000.5}}`},
		{"snapshot", `{"topic":"crypto_prices","type":"subscribe","timestamp":1787312179781,"payload":{"symbol":"eth/usd","data":[{"timestamp":1787312179000,"value":3000}]}}`},
		{"equity", `{"topic":"equity_prices","type":"update","timestamp":1787312179781,"payload":{"symbol":"aapl","timestamp":1787312179781,"value":190.25,"full_accuracy_value":"190.25"}}`},
		{"twap", `{"topic":"crypto_prices_twap_sixty","type":"update","timestamp":1787312179781,"payload":{"symbol":"btc/usd","timestamp":1787312179781,"value":60000,"window_s":60}}`},
	}
	for i, tc := range cases {
		before := len(got)
		r.processMessage(rtdsFrame{data: []byte(tc.raw), id: uint64(70 + i), connID: 5})
		if len(got) != before+1 {
			t.Fatalf("%s: dispatched %d events, want 1", tc.name, len(got)-before)
		}
		ev := got[len(got)-1]
		var fid, cid uint64
		switch e := ev.(type) {
		case *connector.CryptoPriceEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.EquityPriceEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.PriceSnapshotEvent:
			fid, cid = e.FrameID, e.ConnID
		default:
			t.Fatalf("%s: unexpected event %T", tc.name, ev)
		}
		if fid != uint64(70+i) || cid != 5 {
			t.Errorf("%s: FrameID=%d ConnID=%d, want %d/5", tc.name, fid, cid, 70+i)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Exact accounting under a slow consumer
// ─────────────────────────────────────────────────────────────

// TestRTDSSlowConsumerExactDropAccounting floods the RTDS connection with one
// worker stalled behind a tiny queue and proves the queue-full loss is exactly
// accounted: FrameID gaps == FramesDropped == hook calls, and nothing is
// reported as drained (this stream never discards queued frames on disconnect).
func TestRTDSSlowConsumerExactDropAccounting(t *testing.T) {
	const total = 200
	const queue = 8

	v := newRTDSVenue(t)
	var mu sync.Mutex
	delivered := make(map[uint64]int)
	droppedIDs := make(map[uint64]int)
	drainedHook := 0
	gate := make(chan struct{})

	d := func(ev any) {
		e, ok := ev.(*connector.CryptoPriceEvent)
		if !ok {
			return
		}
		mu.Lock()
		delivered[e.FrameID]++
		n := len(delivered)
		mu.Unlock()
		if n == 1 {
			<-gate
		}
	}

	r := startRTDSLoopback(t, v, queue, 1, d, func(r *WSPolymarketRTDS) {
		r.SetOnFrameDropped(func(df connector.DroppedFrame) {
			mu.Lock()
			if df.Cause == connector.DropDrainedOnDisconnect {
				drainedHook++
			} else {
				droppedIDs[df.FrameID]++
			}
			mu.Unlock()
		})
	})
	waitForRTDS(t, "the connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, cryptoPriceFrame(i))
		time.Sleep(200 * time.Microsecond)
	}
	waitForRTDS(t, "all frames read", 5*time.Second, func() bool { return r.Stats().FramesRead == total })

	st := r.Stats()
	if st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped; the drop path was not exercised (stats %+v)", st)
	}
	if st.FramesDrained != 0 {
		t.Errorf("FramesDrained = %d on a live connection, want 0", st.FramesDrained)
	}
	if st.QueueHighWater != queue {
		t.Errorf("QueueHighWater = %d, want %d", st.QueueHighWater, queue)
	}

	close(gate)
	wantDelivered := int(int64(total) - int64(st.FramesDropped))
	waitForRTDS(t, "the queue to drain", 5*time.Second, func() bool {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		return r.Stats().QueueLen == 0 && n == wantDelivered
	})

	st = r.Stats()
	mu.Lock()
	defer mu.Unlock()
	if int(st.FramesRead) != len(delivered)+int(st.FramesDropped) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d", st.FramesRead, len(delivered), st.FramesDropped)
	}
	if drainedHook != 0 {
		t.Errorf("DropDrainedOnDisconnect hook calls = %d, want 0", drainedHook)
	}
	var missing []uint64
	for i := uint64(1); i <= total; i++ {
		if _, ok := delivered[i]; !ok {
			missing = append(missing, i)
		}
	}
	if uint64(len(missing)) != st.FramesDropped {
		t.Fatalf("FrameID gaps = %d, want dropped %d", len(missing), st.FramesDropped)
	}
	for _, id := range missing {
		if droppedIDs[id] != 1 {
			t.Errorf("frame %d missing from delivery but hook calls = %d, want 1", id, droppedIDs[id])
		}
	}
	for id := range delivered {
		if droppedIDs[id] != 0 {
			t.Errorf("frame %d delivered AND reported dropped", id)
		}
	}
}

// TestRTDSQueuedFramesSurviveDisconnect proves the pre-H3 behaviour that the
// parent review required be kept: frames read and queued when the connection
// dies are STILL dispatched afterwards, counted as delivered (FramesDrained ==
// 0, FramesRead == delivered + dropped). They keep the ConnID of the generation
// that read them while the manager is already on the next generation.
func TestRTDSQueuedFramesSurviveDisconnect(t *testing.T) {
	const total = 40
	const queue = 8

	v := newRTDSVenue(t)
	var mu sync.Mutex
	delivered := make(map[uint64]uint64) // FrameID -> ConnID
	droppedIDs := make(map[uint64]int)
	drainedHook := 0
	gate := make(chan struct{})

	d := func(ev any) {
		e, ok := ev.(*connector.CryptoPriceEvent)
		if !ok {
			return
		}
		mu.Lock()
		delivered[e.FrameID] = e.ConnID
		first := len(delivered) == 1
		mu.Unlock()
		if first {
			<-gate // stall the only worker so the queue stays backed up
		}
	}

	r := startRTDSLoopback(t, v, queue, 1, d, func(r *WSPolymarketRTDS) {
		r.SetOnFrameDropped(func(df connector.DroppedFrame) {
			mu.Lock()
			if df.Cause == connector.DropDrainedOnDisconnect {
				drainedHook++
			} else {
				droppedIDs[df.FrameID]++
			}
			mu.Unlock()
		})
	})
	waitForRTDS(t, "the connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, cryptoPriceFrame(i))
		time.Sleep(200 * time.Microsecond)
	}
	waitForRTDS(t, "all frames read", 5*time.Second, func() bool { return r.Stats().FramesRead == total })

	// The venue drops the socket with the queue still backed up.
	v.closeGen(0)
	waitForRTDS(t, "the reconnect after the disconnect", 10*time.Second, func() bool {
		s := r.Stats()
		return s.Disconnects >= 1 && s.ConnID >= 2 && v.numConns() >= 2
	})

	pre := r.Stats()
	if pre.FramesDrained != 0 {
		t.Fatalf("FramesDrained = %d after the disconnect, want 0 (Polymarket must not drain queued frames)", pre.FramesDrained)
	}
	if pre.QueueLen != queue {
		t.Errorf("QueueLen after the disconnect = %d, want %d (the queue must survive it)", pre.QueueLen, queue)
	}

	close(gate)
	wantDelivered := int(int64(total) - int64(pre.FramesDropped))
	waitForRTDS(t, "the surviving frames to be delivered", 10*time.Second, func() bool {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		return r.Stats().QueueLen == 0 && n == wantDelivered
	})

	st := r.Stats()
	mu.Lock()
	defer mu.Unlock()
	if st.FramesDrained != 0 || drainedHook != 0 {
		t.Errorf("FramesDrained = %d, DropDrainedOnDisconnect hook calls = %d, want 0/0", st.FramesDrained, drainedHook)
	}
	if int(st.FramesRead) != len(delivered)+int(st.FramesDropped) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d (drained %d)",
			st.FramesRead, len(delivered), st.FramesDropped, st.FramesDrained)
	}
	for id, cid := range delivered {
		if cid != 1 {
			t.Errorf("frame %d delivered with ConnID %d, want 1 (the generation that read it)", id, cid)
		}
	}
	if st.ConnID < 2 {
		t.Errorf("ConnID = %d after the reconnect, want >= 2", st.ConnID)
	}
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
			t.Errorf("frame %d missing from delivery but queue-full hook calls = %d, want 1", id, droppedIDs[id])
		}
	}
}

// ─────────────────────────────────────────────────────────────
// The ordering fact
// ─────────────────────────────────────────────────────────────

// TestRTDSWorkersReorderDelivery proves the documented ordering fact: with
// dispatcherWorkers > 1 the workers race for frames, so delivery order is NOT
// FrameID order. The test makes it deterministic by holding all workers inside
// the handler (each frame has its own release gate) and then releasing them in
// reverse FrameID order: delivery order comes out exactly reversed while the
// FrameIDs are the expected 1..8 and nothing was dropped.
//
// The default worker count is untouched by this card (4); this test sets 8 only
// for the duration of the test.
func TestRTDSWorkersReorderDelivery(t *testing.T) {
	const workers = 8
	const n = 8

	v := newRTDSVenue(t)
	started := make(chan int, n)
	released := make(chan int, n)
	release := make([]chan struct{}, n+1)
	for i := range release {
		release[i] = make(chan struct{})
	}
	var mu sync.Mutex
	gotFrames := make(map[uint64]bool)

	d := func(ev any) {
		e, ok := ev.(*connector.CryptoPriceEvent)
		if !ok {
			return
		}
		i, err := strconv.Atoi(e.Price)
		if err != nil || i < 1 || i > n {
			return
		}
		mu.Lock()
		gotFrames[e.FrameID] = true
		mu.Unlock()
		started <- i
		<-release[i]
		released <- i
	}

	r := startRTDSLoopback(t, v, 64, workers, d, nil)
	waitForRTDS(t, "the connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	for i := int64(1); i <= n; i++ {
		v.write(t, 0, cryptoPriceFrame(i))
	}

	// All n frames must be in flight at once: that is what makes the race
	// certain rather than probabilistic.
	seen := map[int]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < n {
		select {
		case i := <-started:
			seen[i] = true
		case <-deadline:
			t.Fatalf("only %d of %d frames were picked up concurrently (workers=%d): %v — reordering could not be forced", len(seen), n, workers, seen)
		}
	}

	// Release in reverse FrameID order and wait for each completion, so the
	// delivery order is deterministic.
	var order []int
	for i := n; i >= 1; i-- {
		close(release[i])
		select {
		case got := <-released:
			order = append(order, got)
		case <-time.After(5 * time.Second):
			t.Fatalf("frame %d did not complete after release", i)
		}
	}
	if len(order) != n {
		t.Fatalf("observed %d completions, want %d", len(order), n)
	}
	ascending := true
	for i, price := range order {
		if price != i+1 {
			ascending = false
		}
	}
	if ascending {
		t.Fatalf("delivery order %v was the FrameID order; the reordering fact was not demonstrated", order)
	}
	// The strongest form: reversed, i.e. the last FrameID was delivered first.
	if order[0] != n {
		t.Logf("delivery order %v (reordering proven; not exactly reversed)", order)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gotFrames) != n {
		t.Errorf("delivered %d distinct FrameIDs, want %d (no frame may be lost)", len(gotFrames), n)
	}
	for i := uint64(1); i <= n; i++ {
		if !gotFrames[i] {
			t.Errorf("FrameID %d never delivered", i)
		}
	}
	if st := r.Stats(); st.FramesDropped != 0 || st.FramesRead != n {
		t.Errorf("stats %+v: want FramesRead=%d, no drops", st, n)
	}
}
