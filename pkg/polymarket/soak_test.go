package polymarket

// soak_test.go is H4a's chaos soak for the two Polymarket streams. Skipped
// unless CONNECTOR_SOAK=1:
//
//	CONNECTOR_SOAK=1 CONNECTOR_SOAK_SECONDS=60 CONNECTOR_SOAK_SEED=2 \
//	    go test -race -run Soak ./pkg/polymarket
//
// TestSoakPolymarketMarketReconnect runs the market channel twice, with
// ReconnectOnDrop ON and OFF, against a loopback venue that answers every
// handshake with a full book snapshot and then streams price_change deltas.
// The consumer is randomly stalled so the queue overflows:
//
//	ON  — drop-triggered reconnects are rate-limited to one per 10 s
//	      (asserted from the venue's accept timestamps), the consumer's book is
//	      rebuilt by the next handshake snapshot and converges.
//	OFF — the connection is never rebuilt by a drop (ConnID stays 1,
//	      Disconnects 0) and the book still converges after a later full
//	      snapshot; the loss stays counted.
//
// TestSoakPolymarketRTDS overloads the 4-worker dispatcher and drops sockets.
// Frames already read survive a disconnect (this stream never drains), only
// queue-full drops are lost, FrameIDs are unique, and venue-timestamp order
// recovers FrameID order even though delivery is reordered.
//
// Loopback only; no venue is contacted.

import (
	"context"
	"encoding/json"
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

func waitSoak(t *testing.T, what string, timeout time.Duration, cond func() bool) {
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

// ─────────────────────────────────────────────────────────────
// Loopback venues
// ─────────────────────────────────────────────────────────────

type soakVenue struct {
	url string
	srv *httptest.Server

	mu       sync.Mutex
	conns    []*coderws.Conn
	accepted []time.Time
	frames   [][]byte
	stop     chan struct{}
	stopOnce sync.Once
}

func newSoakVenue(t *testing.T) *soakVenue {
	t.Helper()
	v := &soakVenue{stop: make(chan struct{})}
	v.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		v.conns = append(v.conns, c)
		v.accepted = append(v.accepted, time.Now())
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
	v.url = "ws" + strings.TrimPrefix(v.srv.URL, "http")
	t.Cleanup(v.shutdown)
	return v
}

func (v *soakVenue) shutdown() {
	v.stopOnce.Do(func() { close(v.stop) })
	v.mu.Lock()
	conns := append([]*coderws.Conn(nil), v.conns...)
	v.mu.Unlock()
	for _, c := range conns {
		_ = c.CloseNow()
	}
	v.srv.Close()
}

func (v *soakVenue) numConns() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

func (v *soakVenue) acceptTimes() []time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]time.Time(nil), v.accepted...)
}

// waitHandshake blocks until a client frame carrying key=val arrives (the
// market channel sends {"type":"market"}, RTDS {"action":"subscribe"}), so the
// venue only answers once the subscription is known.
func (v *soakVenue) waitHandshake(gen int, key, val string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		v.mu.Lock()
		for _, f := range v.frames {
			var m map[string]any
			if json.Unmarshal(f, &m) == nil && m[key] == val {
				v.mu.Unlock()
				return true
			}
		}
		v.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false
}

func (v *soakVenue) write(gen int, payload string) bool {
	v.mu.Lock()
	var c *coderws.Conn
	if gen < len(v.conns) {
		c = v.conns[gen]
	}
	v.mu.Unlock()
	if c == nil {
		return false
	}
	return c.Write(context.Background(), coderws.MessageText, []byte(payload)) == nil
}

func (v *soakVenue) drop(gen int) {
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

// ─────────────────────────────────────────────────────────────
// Market channel: book state shared by venue and consumer
// ─────────────────────────────────────────────────────────────

const (
	soakAsset  = "asset-a"
	soakMarket = "m1"
)

// soakMarketBook is both the venue's authoritative book and the shape the
// consumer rebuilds; only the bid at 0.49 changes, so one number identifies the
// whole state.
type soakMarketBook struct {
	mu   sync.Mutex
	bid  string
	hash string
}

func (b *soakMarketBook) set(bid, hash string) {
	b.mu.Lock()
	b.bid, b.hash = bid, hash
	b.mu.Unlock()
}

func (b *soakMarketBook) get() (string, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bid, b.hash
}

func (b *soakMarketBook) snapshotFrame() string {
	bid, hash := b.get()
	return fmt.Sprintf(`{"event_type":"book","asset_id":%q,"market":%q,"bids":[{"price":"0.49","size":%q}],"asks":[{"price":"0.51","size":"10"}],"timestamp":"1700000000000","hash":%q}`,
		soakAsset, soakMarket, bid, hash)
}

func soakPriceChangeFrame(n int64) string {
	return fmt.Sprintf(`[{"event_type":"price_change","market":%q,"timestamp":"1700000000000","price_changes":[{"asset_id":%q,"price":"0.49","size":"%d","side":"BUY","hash":"h%d","best_bid":"0.49","best_ask":"0.51"}]}]`,
		soakMarket, soakAsset, n, n)
}

// ─────────────────────────────────────────────────────────────
// Scenario 1: market channel, ReconnectOnDrop on and off
// ─────────────────────────────────────────────────────────────

func TestSoakPolymarketMarketReconnect(t *testing.T) {
	seed, dur := soakParams(t)
	onFor := dur * 6 / 10
	if onFor < 12*time.Second {
		onFor = dur / 2
	}
	runMarketSoak(t, newSoakRNG(seed), "reconnect-on", true, onFor)
	runMarketSoak(t, newSoakRNG(seed+1), "reconnect-off", false, dur-onFor)
}

func runMarketSoak(t *testing.T, rng *soakRNG, label string, reconnectOn bool, dur time.Duration) {
	t.Helper()
	if dur < 3*time.Second {
		return
	}
	baseline := runtime.NumGoroutine()
	v := newSoakVenue(t)
	venue := &soakMarketBook{bid: "0", hash: "seed"}

	var mu sync.Mutex
	consumer := &soakMarketBook{bid: "0", hash: "seed"}
	lastSnapshot := ""
	var frames int
	d := func(ev any) {
		mu.Lock()
		frames++
		switch e := ev.(type) {
		case *connector.BookSnapshotEvent:
			for _, l := range e.Bids {
				if l.Price == "0.49" {
					consumer.set(l.Size, e.Hash)
					lastSnapshot = e.Hash
				}
			}
		case *connector.PriceChangeEvent:
			for _, c := range e.Changes {
				if c.AssetID == soakAsset && c.Price == "0.49" {
					bid, hash := consumer.get()
					_ = bid
					consumer.set(c.Size, hash)
				}
			}
		}
		n := frames
		mu.Unlock()
		// Slow consumer with stalls: the 8-slot queue overflows, which is the
		// only trigger for a drop-driven reconnect.
		if rng.intn(30) == 0 {
			time.Sleep(rng.duration(10*time.Millisecond, 50*time.Millisecond))
		} else if n%4 == 0 {
			time.Sleep(time.Duration(rng.intn(2)) * time.Millisecond)
		}
	}

	base := connector.New(false, nil)
	base.SetDispatcher(d)
	pm := NewWSPolymarketMarket(base, Config{DispatcherWorkers: 1, PendingFlushMs: 20})
	pm.SetEventQueueSize(8)
	if reconnectOn {
		pm.SetReconnectOnDrop(true)
	}
	pm.Subscribe(context.Background(), []string{soakAsset})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := pm.Start(ctx, v.url, 50); err != nil {
		t.Fatalf("%s: market Start: %v", label, err)
	}
	t.Cleanup(pm.Stop)
	waitSoak(t, "the market handshake", 5*time.Second, func() bool {
		return v.waitHandshake(0, "type", "market", 2*time.Second)
	})

	var droppedMu sync.Mutex
	var droppedFrames []connector.DroppedFrame
	pm.SetOnFrameDropped(func(f connector.DroppedFrame) {
		droppedMu.Lock()
		droppedFrames = append(droppedFrames, f)
		droppedMu.Unlock()
	})

	// Every accepted connection is answered with the venue's current book.
	snapshotDone := make(chan struct{})
	go func() {
		defer close(snapshotDone)
		answered := map[int]bool{}
		for {
			select {
			case <-v.stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			n := v.numConns()
			for gen := 0; gen < n; gen++ {
				if answered[gen] {
					continue
				}
				if !v.waitHandshake(gen, "type", "market", 50*time.Millisecond) {
					continue
				}
				answered[gen] = true
				venue.set(fmt.Sprintf("%s-seed", label), "snap-"+label)
				v.write(gen, venue.snapshotFrame())
			}
		}
	}()

	stopAt := time.Now().Add(dur)
	var seq int64
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
			if rng.intn(100) < 6 {
				burst = 5 + rng.intn(25)
			}
			for i := 0; i < burst && time.Now().Before(stopAt); i++ {
				mu.Lock()
				seq++
				n := seq
				mu.Unlock()
				venue.set(strconv.FormatInt(n, 10), fmt.Sprintf("h%d", n))
				if !v.write(gen, soakPriceChangeFrame(n)) {
					break
				}
				if burst == 1 {
					time.Sleep(rng.duration(1*time.Millisecond, 3*time.Millisecond))
				}
			}
			// Periodic full snapshot, so a consumer WITHOUT reconnect-on-drop
			// can still converge (the venue sends one every few hundred ms in
			// production, and a book frame resets the local book).
			if rng.intn(100) < 12 {
				v.write(gen, venue.snapshotFrame())
				time.Sleep(rng.duration(20*time.Millisecond, 60*time.Millisecond))
			}
		}
		// Final authoritative snapshot.
		gen := v.numConns() - 1
		if gen >= 0 {
			venue.set(strconv.FormatInt(seq, 10), "final-"+label)
			v.write(gen, venue.snapshotFrame())
		}
	}()
	<-writerDone
	waitSoak(t, "the final snapshot to be delivered", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return lastSnapshot == "final-"+label
	})

	st := pm.Stats()
	queuedAtStop := st.QueueLen
	stopStart := time.Now()
	pm.Stop()
	stopTook := time.Since(stopStart)
	cancel()
	v.shutdown()

	mu.Lock()
	framesSeen := frames
	gotBid, gotHash := consumer.get()
	mu.Unlock()
	droppedMu.Lock()
	droppedFs := append([]connector.DroppedFrame(nil), droppedFrames...)
	droppedMu.Unlock()
	queueFull, drainedHook := 0, 0
	for _, f := range droppedFs {
		switch f.Cause {
		case connector.DropQueueFull:
			queueFull++
		case connector.DropDrainedOnDisconnect:
			drainedHook++
		default:
			t.Fatalf("%s: undocumented drop cause %q", label, f.Cause)
		}
	}
	wantBid, wantHash := venue.get()
	accepts := v.acceptTimes()
	// Reconnect intervals: accepts[0] is the initial connect, so the rate limit
	// applies between consecutive drop-triggered reconnects (accepts[1:]);
	// the initial connect is not rate-limited.
	var minGap time.Duration
	for i := 2; i < len(accepts); i++ {
		gap := accepts[i].Sub(accepts[i-1])
		if minGap == 0 || gap < minGap {
			minGap = gap
		}
	}

	t.Logf("%s: dur=%v frames=%d reads=%d dropped=%d drained=%d hook[queue_full=%d drained=%d] conns=%d connID=%d disconnects=%d min_reconnect_gap=%v book=%s/%s want=%s/%s close=%v",
		label, dur, framesSeen, st.FramesRead, st.FramesDropped, st.FramesDrained, queueFull, drainedHook,
		len(accepts), st.ConnID, st.Disconnects, minGap, gotBid, gotHash, wantBid, wantHash, stopTook)

	// ── accounting: this stream never drains its queue ──
	if st.FramesDrained != 0 {
		t.Fatalf("%s: FramesDrained = %d, want 0 (the market queue survives a disconnect)", label, st.FramesDrained)
	}
	if drainedHook != 0 {
		t.Fatalf("%s: drop hook reported %d drained frames, want 0", label, drainedHook)
	}
	// Two terms are neither delivered nor counted as dropped, and both are
	// reported rather than hidden:
	//   queuedAtStop — frames still queued when Stop() cancels the dispatchers
	//                  (this stream has no drain path);
	//   noEvent      — frames dispatched while the subscription set was empty,
	//                  i.e. queued frames of the dead generation processed
	//                  between onDisconnect (which clears the set) and onConnect
	//                  (which restores it). handlePriceChange/handleBook treat
	//                  them as "for an asset I do not hold" and return without an
	//                  event, without a counter.
	noEvent := int(st.FramesRead) - framesSeen - int(st.FramesDropped) - queuedAtStop
	if noEvent < 0 {
		t.Fatalf("%s: negative unexplained term (%d): frames were counted twice", label, noEvent)
	}
	if got, want := int(st.FramesRead), framesSeen+int(st.FramesDropped)+queuedAtStop+noEvent; got != want {
		t.Fatalf("%s: FramesRead %d != delivered %d + dropped %d + queued_at_stop %d + no_event %d", label, got, framesSeen, st.FramesDropped, queuedAtStop, noEvent)
	}
	reconnects := len(accepts) - 1
	if reconnects == 0 && noEvent != 0 {
		t.Fatalf("%s: %d frames produced no event without a reconnect — a subscription was lost on a live connection", label, noEvent)
	}
	// A race can push one frame past the window boundary on each side, hence
	// the queue depth plus a small margin per reconnect.
	if noEvent > reconnects*10 {
		t.Fatalf("%s: %d no-event frames exceed one queue's worth plus margin (%d) per reconnect (%d reconnects)", label, reconnects*10, 10, reconnects)
	}
	t.Logf("%s: no_event_frames=%d (dispatched while the subscription set was empty) queued_at_stop=%d", label, noEvent, queuedAtStop)
	if queueFull != int(st.FramesDropped) {
		t.Fatalf("%s: drop hook queue_full %d != Stats.FramesDropped %d", label, queueFull, st.FramesDropped)
	}
	if st.FramesDropped == 0 {
		t.Fatalf("%s: no frame was dropped in %v — the overload episodes never overflowed the 8-slot queue", label, dur)
	}
	// ── convergence after the final full snapshot ──
	if gotBid != wantBid || gotHash != wantHash {
		t.Fatalf("%s: consumer book %s/%s != venue book %s/%s after the final snapshot", label, gotBid, gotHash, wantBid, wantHash)
	}
	// ── the option's contract ──
	if reconnectOn {
		if len(accepts) < 2 {
			t.Fatalf("%s: ReconnectOnDrop did not rebuild the connection (accepts=%d)", label, len(accepts))
		}
		if len(accepts) > 2 && minGap < reconnectOnDropMinInterval-200*time.Millisecond {
			t.Fatalf("%s: two drop-triggered reconnects %v apart (< the %v rate limit)", label, minGap, reconnectOnDropMinInterval)
		}
		maxReconnects := int(dur/reconnectOnDropMinInterval) + 1
		if len(accepts)-1 > maxReconnects {
			t.Fatalf("%s: %d reconnects in %v, rate limit allows at most %d", label, len(accepts)-1, dur, maxReconnects)
		}
	} else {
		if len(accepts) != 1 || st.ConnID != 1 {
			t.Fatalf("%s: ReconnectOnDrop off but the connection was rebuilt (accepts=%d connID=%d)", label, len(accepts), st.ConnID)
		}
		if st.Disconnects != 0 {
			t.Fatalf("%s: ReconnectOnDrop off but Disconnects=%d", label, st.Disconnects)
		}
	}
	if stopTook > 2*time.Second {
		t.Fatalf("%s: Stop() took %v (> 2s)", label, stopTook)
	}
	soakAssertNoLeak(t, label, baseline)
}

// ─────────────────────────────────────────────────────────────
// Scenario 2: RTDS, 4 workers
// ─────────────────────────────────────────────────────────────

func TestSoakPolymarketRTDS(t *testing.T) {
	seed, dur := soakParams(t)
	rng := newSoakRNG(seed + 3)
	baseline := runtime.NumGoroutine()
	v := newSoakVenue(t)

	type sample struct {
		n       int64
		frameID uint64
		connID  uint64
	}
	var mu sync.Mutex
	var samples []sample
	d := func(ev any) {
		e, ok := ev.(*connector.CryptoPriceEvent)
		if !ok {
			return
		}
		mu.Lock()
		v, err := strconv.ParseInt(e.Price, 10, 64)
		if err != nil {
			t.Errorf("crypto price %q is not the synthetic integer", e.Price)
			return
		}
		samples = append(samples, sample{n: v, frameID: e.FrameID, connID: e.ConnID})
		mu.Unlock()
		if rng.intn(10) == 0 {
			time.Sleep(rng.duration(0, 3*time.Millisecond))
		}
	}

	base := connector.New(false, nil)
	base.SetDispatcher(d)
	r := NewWSPolymarketRTDS(base)
	r.SetEventQueueSize(8)
	r.SetDispatcherWorkers(4)
	r.SubscribeCryptoPrices(context.Background(), []string{"btcusdt"})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := r.Start(ctx, v.url, 50); err != nil {
		t.Fatalf("rtds Start: %v", err)
	}
	t.Cleanup(r.Stop)
	waitSoak(t, "the RTDS handshake", 5*time.Second, func() bool { return v.waitHandshake(0, "action", "subscribe", 2*time.Second) })

	var droppedMu sync.Mutex
	var droppedFrames []connector.DroppedFrame
	r.SetOnFrameDropped(func(f connector.DroppedFrame) {
		droppedMu.Lock()
		droppedFrames = append(droppedFrames, f)
		droppedMu.Unlock()
	})

	stopAt := time.Now().Add(dur)
	var n int64
	var injectedDrops int
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
			if rng.intn(100) < 8 {
				burst = 8 + rng.intn(30)
			}
			for i := 0; i < burst && time.Now().Before(stopAt); i++ {
				mu.Lock()
				n++
				cur := n
				mu.Unlock()
				if !v.write(gen, cryptoPriceFrame(cur)) {
					break
				}
				if burst == 1 {
					time.Sleep(rng.duration(1*time.Millisecond, 3*time.Millisecond))
				}
			}
		}
	}()
	dropDone := make(chan struct{})
	// A generation is dropped at most once: dropping an already dead socket
	// produces no Disconnects increment.
	var dropMu sync.Mutex
	droppedGen := map[int]bool{}
	go func() {
		defer close(dropDone)
		for time.Now().Before(stopAt) {
			time.Sleep(rng.duration(1*time.Second, 4*time.Second))
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
			v.drop(gen)
			injectedDrops++
		}
	}()
	<-writerDone
	<-dropDone
	// Settle: 4 workers plus the queue must finish.
	deadline := time.Now().Add(10 * time.Second)
	prev := r.Stats()
	for {
		st := r.Stats()
		if st.QueueLen == 0 && st.FramesRead == prev.FramesRead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("RTDS did not settle: %+v", st)
		}
		prev = st
		time.Sleep(100 * time.Millisecond)
	}
	st := r.Stats()
	stopStart := time.Now()
	r.Stop()
	stopTook := time.Since(stopStart)
	cancel()
	v.shutdown()

	mu.Lock()
	ss := append([]sample(nil), samples...)
	mu.Unlock()
	droppedMu.Lock()
	droppedFs := append([]connector.DroppedFrame(nil), droppedFrames...)
	droppedMu.Unlock()
	queueFull, drainedHook := 0, 0
	for _, f := range droppedFs {
		switch f.Cause {
		case connector.DropQueueFull:
			queueFull++
		case connector.DropDrainedOnDisconnect:
			drainedHook++
		default:
			t.Fatalf("undocumented drop cause %q", f.Cause)
		}
	}

	// FrameIDs unique; gaps are exactly the drops.
	seen := map[uint64]bool{}
	reordered := 0
	for i, s := range ss {
		if seen[s.frameID] {
			t.Fatalf("FrameID %d delivered twice", s.frameID)
		}
		seen[s.frameID] = true
		if i > 0 && s.frameID < ss[i-1].frameID {
			reordered++
		}
	}
	gaps := 0
	for id := uint64(1); id <= st.FramesRead; id++ {
		if !seen[id] {
			gaps++
		}
	}
	// Delivery order recovery: sorting by the venue timestamp (the payload
	// value here) must give strictly increasing FrameIDs.
	byTime := append([]sample(nil), ss...)
	sort.Slice(byTime, func(i, j int) bool { return byTime[i].n < byTime[j].n })
	recovered := true
	for i := 1; i < len(byTime); i++ {
		if byTime[i].frameID <= byTime[i-1].frameID {
			recovered = false
		}
	}
	connIDs := map[uint64]bool{}
	for _, s := range ss {
		connIDs[s.connID] = true
	}

	t.Logf("rtds: dur=%v reads=%d delivered=%d dropped=%d drained=%d gaps=%d reorder_points=%d conns=%d connIDs=%d disconnects=%d drops=%d close=%v",
		dur, st.FramesRead, len(ss), st.FramesDropped, st.FramesDrained, gaps, reordered, len(v.acceptTimes()), len(connIDs), st.Disconnects, injectedDrops, stopTook)

	if got, want := int(st.FramesRead), len(ss)+int(st.FramesDropped); got != want {
		t.Fatalf("FramesRead %d != delivered %d + dropped %d (this stream never drains)", got, len(ss), st.FramesDropped)
	}
	if st.FramesDrained != 0 || drainedHook != 0 {
		t.Fatalf("FramesDrained=%d (hook %d), want 0: queued RTDS frames must survive a disconnect", st.FramesDrained, drainedHook)
	}
	if queueFull != int(st.FramesDropped) {
		t.Fatalf("drop hook queue_full %d != Stats.FramesDropped %d", queueFull, st.FramesDropped)
	}
	if gaps != int(st.FramesDropped) {
		t.Fatalf("FrameID gaps %d != FramesDropped %d (no loss other than counted drops)", gaps, st.FramesDropped)
	}
	if !recovered {
		t.Fatalf("sorting the delivered samples by venue timestamp did not recover FrameID order")
	}
	if st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped in %v — the 8-slot queue never overflowed", dur)
	}
	if st.Disconnects < uint64(injectedDrops) {
		t.Fatalf("Disconnects %d < injected drops %d", st.Disconnects, injectedDrops)
	}
	if stopTook > 2*time.Second {
		t.Fatalf("Stop() took %v (> 2s)", stopTook)
	}
	soakAssertNoLeak(t, "rtds", baseline)
}
