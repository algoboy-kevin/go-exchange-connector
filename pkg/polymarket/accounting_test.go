package polymarket

import (
	"context"
	"encoding/json"
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
// Helpers: loopback market venue
// ─────────────────────────────────────────────────────────────

// marketVenue is a loopback CLOB market-channel endpoint. It records the frames
// the client writes (handshake / incremental ops) per generation and lets the
// test write frames whenever it wants, so drops can be provoked deterministically.
type marketVenue struct {
	url string

	mu     sync.Mutex
	conns  []*coderws.Conn
	frames [][]byte
}

func newMarketVenue(t *testing.T) *marketVenue {
	t.Helper()
	v := &marketVenue{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		v.conns = append(v.conns, c)
		v.mu.Unlock()
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			v.mu.Lock()
			v.frames = append(v.frames, append([]byte(nil), data...))
			v.mu.Unlock()
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

func (v *marketVenue) numConns() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

func (v *marketVenue) conn(gen int) *coderws.Conn {
	v.mu.Lock()
	defer v.mu.Unlock()
	if gen >= len(v.conns) {
		return nil
	}
	return v.conns[gen]
}

func (v *marketVenue) write(t *testing.T, gen int, frame string) {
	t.Helper()
	c := v.conn(gen)
	if c == nil {
		t.Fatalf("write on generation %d: no such connection", gen)
	}
	if err := c.Write(context.Background(), coderws.MessageText, []byte(frame)); err != nil {
		t.Fatalf("write on generation %d: %v", gen, err)
	}
}

func (v *marketVenue) closeGen(gen int) {
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

// handshakes returns the market handshakes (type=market) received so far.
func (v *marketVenue) handshakes() []map[string]any {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []map[string]any
	for _, f := range v.frames {
		var m map[string]any
		if json.Unmarshal(f, &m) != nil {
			continue
		}
		if m["type"] == "market" {
			out = append(out, m)
		}
	}
	return out
}

func waitForMarket(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// startMarketLoopback starts a market WS against the loopback venue with a
// single dispatcher worker (so "the dispatcher is stalled" is well defined) and
// the given queue depth.
func startMarketLoopback(t *testing.T, v *marketVenue, queue int, d func(any), configure func(pm *WSPolymarketMarket), assets []string) *WSPolymarketMarket {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	pm := NewWSPolymarketMarket(base, Config{DispatcherWorkers: 1, PendingFlushMs: 20})
	pm.SetEventQueueSize(queue)
	if configure != nil {
		configure(pm)
	}
	// Subscriptions are queued before Start so the first connection's
	// handshake carries them (the venue's `book` snapshot answers it).
	pm.Subscribe(context.Background(), assets)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := pm.Start(ctx, v.url, 20); err != nil {
		t.Fatalf("market Start: %v", err)
	}
	t.Cleanup(pm.Stop)
	return pm
}

// priceChangeFrame is one price_change frame for asset-a with a distinct price.
func priceChangeFrame(n int64) string {
	return fmt.Sprintf(`[{"event_type":"price_change","market":"m1","timestamp":"1700000000","price_changes":[{"asset_id":"asset-a","price":"%d","size":"10","side":"BUY","hash":"h%d","best_bid":"0.49","best_ask":"0.51"}]}]`,
		n, n)
}

// ─────────────────────────────────────────────────────────────
// Options / Stats / provenance
// ─────────────────────────────────────────────────────────────

// TestMarketEventQueueSizeAndStats pins the queue-size option and the resolved
// default, and proves the existing dropped counter and the shared accounting
// expose the same number.
func TestMarketEventQueueSizeAndStats(t *testing.T) {
	base := connector.New(false, nil)

	def := NewWSPolymarketMarket(base, Config{})
	if got := def.Stats().QueueCap; got != defaultMarketEventQueueSize {
		t.Errorf("default QueueCap = %d, want %d", got, defaultMarketEventQueueSize)
	}
	def.SetEventQueueSize(32)
	if got := def.Stats().QueueCap; got != 32 {
		t.Errorf("QueueCap after SetEventQueueSize(32) = %d, want 32", got)
	}
	def.SetEventQueueSize(0) // 0 restores the default rather than an unbuffered queue
	if got := def.Stats().QueueCap; got != defaultMarketEventQueueSize {
		t.Errorf("QueueCap after SetEventQueueSize(0) = %d, want %d", got, defaultMarketEventQueueSize)
	}
	if got := def.Stats(); got.FramesRead != 0 || got.FramesDropped != 0 || got.ConnID != 0 || got.Disconnects != 0 {
		t.Errorf("fresh market stats = %+v, want zeroed", got)
	}
}

// TestMarketFrameAndConnIDOnEveryEvent proves every typed event the market
// channel dispatches carries the frame's provenance, at the decode seam.
func TestMarketFrameAndConnIDOnEveryEvent(t *testing.T) {
	base := connector.New(false, nil)
	var got []any
	base.SetDispatcher(func(ev any) { got = append(got, ev) })

	pm := NewWSPolymarketMarket(base, Config{DispatcherWorkers: 1})
	pm.subMu.Lock()
	pm.subscribedAssetIDs["asset-a"] = struct{}{}
	pm.subMu.Unlock()

	cases := []struct {
		name string
		raw  string
	}{
		{"price_change", priceChangeFrame(1)},
		{"book", `[{"event_type":"book","market":"m1","asset_id":"asset-a","timestamp":"1700000000","hash":"x","bids":[{"price":"0.49","size":"1"}],"asks":[{"price":"0.51","size":"1"}]}]`},
		{"last_trade_price", `[{"event_type":"last_trade_price","asset_id":"asset-a","market":"m1","price":"0.5","side":"BUY","size":"1","timestamp":"1700000000"}]`},
		{"tick_size_change", `[{"event_type":"tick_size_change","asset_id":"asset-a","market":"m1","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1700000000"}]`},
		{"market_resolved", `[{"event_type":"market_resolved","id":"m1","market":"0xcond","winning_asset_id":"asset-a","winning_outcome":"YES","timestamp":"1700000000"}]`},
	}
	for i, tc := range cases {
		before := len(got)
		pm.processMessage(marketFrame{data: []byte(tc.raw), id: uint64(50 + i), connID: 3})
		if len(got) != before+1 {
			t.Fatalf("%s: dispatched %d events, want 1", tc.name, len(got)-before)
		}
		ev := got[len(got)-1]
		var fid, cid uint64
		switch e := ev.(type) {
		case *connector.PriceChangeEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.BookSnapshotEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.TradeEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.TickChangeEvent:
			fid, cid = e.FrameID, e.ConnID
		case *connector.MarketResolvedEvent:
			fid, cid = e.FrameID, e.ConnID
		default:
			t.Fatalf("%s: unexpected event %T", tc.name, ev)
		}
		if fid != uint64(50+i) || cid != 3 {
			t.Errorf("%s: FrameID=%d ConnID=%d, want %d/3", tc.name, fid, cid, 50+i)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Exact accounting under a slow consumer
// ─────────────────────────────────────────────────────────────

// TestMarketSlowConsumerExactDropAccounting floods the channel while the single
// dispatcher is stalled behind a full queue and proves the loss is exactly
// accounted. With ReconnectOnDrop off (the default) the connection is NOT
// dropped: the historical behaviour is unchanged, only now it is counted.
func TestMarketSlowConsumerExactDropAccounting(t *testing.T) {
	const total = 200
	const queue = 8

	v := newMarketVenue(t)
	var mu sync.Mutex
	delivered := make(map[uint64]int)
	droppedIDs := make(map[uint64]int)
	gate := make(chan struct{})

	d := func(ev any) {
		e, ok := ev.(*connector.PriceChangeEvent)
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

	pm := startMarketLoopback(t, v, queue, d, func(pm *WSPolymarketMarket) {
		pm.SetOnFrameDropped(func(df connector.DroppedFrame) {
			mu.Lock()
			droppedIDs[df.FrameID]++
			mu.Unlock()
		})
	}, []string{"asset-a"})
	waitForMarket(t, "the handshake", 5*time.Second, func() bool { return v.numConns() == 1 && len(v.handshakes()) == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, priceChangeFrame(i))
		time.Sleep(200 * time.Microsecond)
	}
	waitForMarket(t, "all frames read", 5*time.Second, func() bool { return pm.Stats().FramesRead == total })

	st := pm.Stats()
	if st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped; the drop path was not exercised (stats %+v)", st)
	}
	if st.QueueHighWater != queue {
		t.Errorf("QueueHighWater = %d, want %d", st.QueueHighWater, queue)
	}

	close(gate)
	waitForMarket(t, "the queue to drain", 5*time.Second, func() bool {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		s := pm.Stats()
		return s.QueueLen == 0 && n == int(total-s.FramesDropped)
	})

	st = pm.Stats()
	mu.Lock()
	defer mu.Unlock()

	if st.FramesDrained != 0 {
		t.Errorf("FramesDrained = %d on a live connection, want 0", st.FramesDrained)
	}
	if int(st.FramesRead) != len(delivered)+int(st.FramesDropped) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d", st.FramesRead, len(delivered), st.FramesDropped)
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
			t.Errorf("frame %d missing from delivery but hook calls = %d, want 1", id, droppedIDs[id])
		}
	}
	for id := range delivered {
		if droppedIDs[id] != 0 {
			t.Errorf("frame %d delivered AND reported dropped", id)
		}
	}
	// The legacy private counter and the shared accounting are the same number.
	if pm.dropped.Load() != int64(st.FramesDropped) {
		t.Errorf("legacy dropped = %d, Stats().FramesDropped = %d", pm.dropped.Load(), st.FramesDropped)
	}
	// Default behaviour: counted loss, no reconnect storm, one generation.
	if st.ConnID != 1 || st.Disconnects != 0 || v.numConns() != 1 {
		t.Errorf("ReconnectOnDrop off must not drop the connection: ConnID=%d Disconnects=%d conns=%d",
			st.ConnID, st.Disconnects, v.numConns())
	}
}

// TestMarketQueuedFramesSurviveDisconnect proves the pre-H3 behaviour that the
// parent review required be kept: frames already read from the socket and
// queued when the connection dies are STILL dispatched afterwards. Nothing is
// discarded on disconnect, so FramesDrained is 0 and the identity is
// FramesRead == delivered + FramesDropped. The surviving frames still carry the
// ConnID of the generation that read them (1) even though the live generation is
// already 2, so a consumer can tell them apart from the new connection's frames.
func TestMarketQueuedFramesSurviveDisconnect(t *testing.T) {
	const total = 40
	const queue = 8

	v := newMarketVenue(t)
	var mu sync.Mutex
	delivered := make(map[uint64]uint64) // FrameID -> ConnID
	droppedIDs := make(map[uint64]int)
	drainedHook := 0
	gate := make(chan struct{})

	d := func(ev any) {
		e, ok := ev.(*connector.PriceChangeEvent)
		if !ok {
			return
		}
		mu.Lock()
		delivered[e.FrameID] = e.ConnID
		first := len(delivered) == 1
		mu.Unlock()
		if first {
			<-gate // stall the only dispatcher so the queue stays backed up
		}
	}

	pm := startMarketLoopback(t, v, queue, d, func(pm *WSPolymarketMarket) {
		pm.SetOnFrameDropped(func(df connector.DroppedFrame) {
			mu.Lock()
			if df.Cause == connector.DropDrainedOnDisconnect {
				drainedHook++
			} else {
				droppedIDs[df.FrameID]++
			}
			mu.Unlock()
		})
	}, []string{"asset-a"})
	waitForMarket(t, "the handshake", 5*time.Second, func() bool { return v.numConns() == 1 && len(v.handshakes()) == 1 })

	for i := int64(1); i <= total; i++ {
		v.write(t, 0, priceChangeFrame(i))
		time.Sleep(200 * time.Microsecond)
	}
	waitForMarket(t, "all frames read", 5*time.Second, func() bool { return pm.Stats().FramesRead == total })

	// The venue drops the socket with the queue still backed up.
	v.closeGen(0)
	waitForMarket(t, "the reconnect after the disconnect", 10*time.Second, func() bool {
		s := pm.Stats()
		return s.Disconnects >= 1 && s.ConnID >= 2 && v.numConns() >= 2 && len(v.handshakes()) >= 2
	})

	// Nothing was thrown away: the whole queue is still there, awaiting the
	// stalled dispatcher.
	pre := pm.Stats()
	if pre.FramesDrained != 0 {
		t.Fatalf("FramesDrained = %d after the disconnect, want 0 (Polymarket must not drain queued frames)", pre.FramesDrained)
	}
	if pre.QueueLen != queue {
		t.Errorf("QueueLen after the disconnect = %d, want %d (the queue must survive it)", pre.QueueLen, queue)
	}

	close(gate)
	wantDelivered := int(int64(total) - int64(pre.FramesDropped))
	waitForMarket(t, "the surviving frames to be delivered", 10*time.Second, func() bool {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		s := pm.Stats()
		return s.QueueLen == 0 && n == wantDelivered
	})

	st := pm.Stats()
	mu.Lock()
	defer mu.Unlock()

	if st.FramesDrained != 0 || drainedHook != 0 {
		t.Errorf("FramesDrained = %d, DropDrainedOnDisconnect hook calls = %d, want 0/0", st.FramesDrained, drainedHook)
	}
	if int(st.FramesRead) != len(delivered)+int(st.FramesDropped) {
		t.Errorf("FramesRead %d != delivered %d + dropped %d (drained %d)",
			st.FramesRead, len(delivered), st.FramesDropped, st.FramesDrained)
	}
	// The frames read on the dead generation were delivered after it, still
	// stamped with generation 1 while the manager is already reading generation 2.
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
	if got := pm.dropped.Load(); got != int64(st.FramesDropped) {
		t.Errorf("legacy dropped = %d, Stats().FramesDropped = %d", got, st.FramesDropped)
	}
}

// ─────────────────────────────────────────────────────────────
// ReconnectOnDrop (opt-in)
// ─────────────────────────────────────────────────────────────

// TestMarketReconnectOnDropRebuildsBook proves the opt-in recovery: one
// overload episode causes exactly one reconnect (not a loop), and the new
// connection re-sends the full handshake so the venue re-delivers a book
// snapshot per asset. It also proves the rate limiter: every further drop inside
// the same 10s window is counted but does not reconnect again.
func TestMarketReconnectOnDropRebuildsBook(t *testing.T) {
	const queue = 4
	const total = 80

	v := newMarketVenue(t)
	var mu sync.Mutex
	delivered := 0
	gate := make(chan struct{})

	d := func(ev any) {
		if _, ok := ev.(*connector.PriceChangeEvent); !ok {
			return
		}
		mu.Lock()
		delivered++
		first := delivered == 1
		mu.Unlock()
		if first {
			<-gate // stall the only dispatcher: the queue fills immediately
		}
	}

	pm := startMarketLoopback(t, v, queue, d, func(pm *WSPolymarketMarket) {
		pm.SetReconnectOnDrop(true)
	}, []string{"asset-a", "asset-b"})
	waitForMarket(t, "the first handshake", 5*time.Second, func() bool {
		return v.numConns() == 1 && len(v.handshakes()) == 1
	})

	for i := int64(1); i <= total; i++ {
		if v.conn(0) == nil {
			break
		}
		// A write error after the reconnect is expected: the socket is gone.
		if err := v.conn(0).Write(context.Background(), coderws.MessageText, []byte(priceChangeFrame(i))); err != nil {
			break
		}
	}

	waitForMarket(t, "the reconnect", 10*time.Second, func() bool { return v.numConns() >= 2 && len(v.handshakes()) >= 2 })
	waitForMarket(t, "the drop to be counted", 5*time.Second, func() bool { return pm.Stats().FramesDropped > 0 })

	// Exactly one reconnect per episode: no third connection appears while the
	// 10s window is open, even though drops keep happening.
	droppedAfterReconnect := pm.Stats().FramesDropped
	time.Sleep(300 * time.Millisecond)
	if n := v.numConns(); n != 2 {
		t.Errorf("connections after one overload episode = %d, want exactly 2 (one reconnect, not a loop)", n)
	}
	if s := pm.Stats(); s.FramesDropped != droppedAfterReconnect && v.numConns() != 2 {
		t.Errorf("drop accounting and reconnect count disagree: %+v conns=%d", s, v.numConns())
	}

	// The reconnect carried the full subscription set, which is what makes the
	// consumer's book rebuildable.
	h := v.handshakes()[1]
	ids, _ := h["assets_ids"].([]any)
	if len(ids) != 2 {
		t.Fatalf("handshake after reconnect carries %v asset ids, want the 2 subscribed: %v", ids, h)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if s, ok := id.(string); ok {
			seen[s] = true
		}
	}
	if !seen["asset-a"] || !seen["asset-b"] {
		t.Errorf("handshake after reconnect = %v, want [asset-a asset-b]", ids)
	}

	st := pm.Stats()
	if st.Disconnects < 1 {
		t.Errorf("Disconnects = %d, want >= 1", st.Disconnects)
	}
	if st.ConnID < 2 {
		t.Errorf("ConnID = %d after the reconnect, want >= 2", st.ConnID)
	}
	close(gate)
}
