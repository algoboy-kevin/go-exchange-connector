package hyperliquid

// soak_test.go is H4a's chaos soak for the Hyperliquid stream manager. Skipped
// unless CONNECTOR_SOAK=1:
//
//	CONNECTOR_SOAK=1 CONNECTOR_SOAK_SECONDS=60 CONNECTOR_SOAK_SEED=2 \
//	    go test -race -run Soak ./pkg/hyperliquid
//
// Two scenarios, one seeded PRNG:
//
//	TestSoakHyperliquidMarket  — numbered l2Book frames at a paced rate, random
//	  socket drops (1-5 s), garbage and oversized frames, bursts, and a
//	  randomly slow consumer. Exact identity: read == delivered + dropped +
//	  drained + garbage, and the FrameID gaps the consumer sees are exactly the
//	  frames that never made it. The raw-frame hook records every frame the
//	  socket delivered, in order, which is what makes "exactly" checkable: the
//	  hook call order IS the FrameID order (both are assigned on the read loop).
//
//	TestSoakHyperliquidAccount — userFills with strictly increasing tids, a
//	  small queue and forced consumer stalls (queue overflow fails the
//	  connection), plus random socket drops. The no-silent-loss rule is checked
//	  over the whole run, generation by generation.
//
// Every scenario ends with Stop() inside 2 s and the goroutine count back to
// the pre-scenario baseline. Loopback only: no venue is contacted.

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

// ─────────────────────────────────────────────────────────────
// Loopback venue
// ─────────────────────────────────────────────────────────────

type soakVenue struct {
	url string
	srv *httptest.Server

	mu       sync.Mutex
	conns    []*coderws.Conn
	accepted int
	writes   map[int]int // successful data-frame writes per generation
	writeErr map[int]error
	stop     chan struct{}
	stopOnce sync.Once
}

func newSoakVenue(t *testing.T) *soakVenue {
	t.Helper()
	v := &soakVenue{writes: map[int]int{}, writeErr: map[int]error{}, stop: make(chan struct{})}
	v.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		gen := len(v.conns)
		v.conns = append(v.conns, c)
		v.accepted = len(v.conns)
		v.mu.Unlock()
		// Drain client frames (subscribe replay / ping) so its writes never
		// block; the venue never sends pongs, so a keepalive is not expected.
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				v.mu.Lock()
				v.writeErr[gen] = err
				v.mu.Unlock()
				return
			}
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

// write sends one frame on generation gen. It reports false when the socket is
// gone; the caller then waits for the next generation.
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
	if err := c.Write(context.Background(), coderws.MessageText, []byte(payload)); err != nil {
		return false
	}
	v.mu.Lock()
	v.writes[gen]++
	v.mu.Unlock()
	return true
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
// Shared frame builders
// ─────────────────────────────────────────────────────────────

// soakBookFrame is a l2Book frame carrying its own sequence number twice: in
// the payload's `time` (so a consumer can correlate) and in a top-level
// `x_seq` key the decoder ignores (so the raw hook can classify the frame even
// if the data payload is later mangled).
func soakBookFrame(seq int64) string {
	return fmt.Sprintf(`{"channel":"l2Book","x_seq":%d,"data":{"coin":"BTC","time":%d,"levels":[[{"px":"100.0","sz":"1","n":1}],[{"px":"101.0","sz":"1","n":1}]]}}`,
		seq, 1_700_000_000_000+seq)
}

// soakPaddedBookFrame is a valid frame with ~48 KiB of padding: oversized for
// the decoder's comfort, still a legal l2Book message.
func soakPaddedBookFrame(seq int64) string {
	pad := strings.Repeat("x", 48<<10)
	return fmt.Sprintf(`{"channel":"l2Book","x_seq":%d,"data":{"coin":"BTC","time":%d,"pad":%q,"levels":[[{"px":"100.0","sz":"1","n":1}],[{"px":"101.0","sz":"1","n":1}]]}}`,
		seq, 1_700_000_000_000+seq, pad)
}

// soakGarbageFrame is not valid JSON. It consumes a FrameID and produces no
// event; the raw hook is the only way to see that it was read at all.
func soakGarbageFrame(seq int64) string {
	return fmt.Sprintf(`GARBAGE:%d {"channel":"l2Book","data":{"coin":"BTC"`, seq)
}

type rawRead struct {
	data []byte
	rx   time.Time
}

type rawLog struct {
	mu    sync.Mutex
	reads []rawRead
}

func (l *rawLog) add(b []byte, rx time.Time) {
	cp := append([]byte(nil), b...)
	l.mu.Lock()
	l.reads = append(l.reads, rawRead{data: cp, rx: rx})
	l.mu.Unlock()
}

func (l *rawLog) snapshot() []rawRead {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]rawRead(nil), l.reads...)
}

// frameSeq extracts the top-level x_seq marker; ok is false for garbage.
func frameSeq(raw []byte) (int64, bool) {
	var env struct {
		Seq *int64 `json:"x_seq"`
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Seq == nil {
		return 0, false
	}
	return *env.Seq, true
}

// waitSoak polls cond until it holds or the timeout expires.
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

// nextConn returns the smallest connID greater than c.
func nextConn(ids []uint64, c uint64) (uint64, bool) {
	for _, id := range ids {
		if id > c {
			return id, true
		}
	}
	return 0, false
}

func containsTid(tids []int64, tid int64) bool {
	for _, t := range tids {
		if t == tid {
			return true
		}
	}
	return false
}

// settleSoak waits until the venue is quiet AND the counters stop moving:
// queue empty, and FramesRead unchanged across two 100 ms probes.
func settleSoak(t *testing.T, h *WSHyperliquid, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	prev := h.Stats().FramesRead
	for {
		st := h.Stats()
		if st.QueueLen == 0 && st.FramesRead == prev {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for the stream to settle: %+v", timeout, st)
		}
		prev = st.FramesRead
		time.Sleep(100 * time.Millisecond)
	}
}

// startSoakManager starts a manager against v with the soak's transport
// settings (keepalives off, short reconnect) and the given queue depth.
func startSoakManager(t *testing.T, v *soakVenue, queue int, d func(any), configure func(*WSHyperliquid)) *WSHyperliquid {
	t.Helper()
	base := connector.New(false, nil)
	if d != nil {
		base.SetDispatcher(d)
	}
	opts := DefaultOptions()
	opts.URL = v.url
	opts.EventQueueSize = queue
	opts.ReconnectIntervalMs = 20
	opts.AppPingIntervalMs = -1
	opts.WSPingIntervalMs = -1
	opts.DataStaleTimeoutMs = 0
	h := NewWithOptions(base, opts)
	if configure != nil {
		configure(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := h.Start(ctx, 20); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(h.Stop)
	return h
}

// ─────────────────────────────────────────────────────────────
// Scenario 1: market connection
// ─────────────────────────────────────────────────────────────

func TestSoakHyperliquidMarket(t *testing.T) {
	seed, dur := soakParams(t)
	rng := newSoakRNG(seed)
	scenarioStart := time.Now()
	baseline := runtime.NumGoroutine()

	const queue = 32
	v := newSoakVenue(t)

	raw := &rawLog{}
	var bookEvents []*connector.HyperliquidBookEvent
	var eventsMu sync.Mutex
	dispatched := 0
	d := func(ev any) {
		eventsMu.Lock()
		if b, ok := ev.(*connector.HyperliquidBookEvent); ok {
			bookEvents = append(bookEvents, b)
		}
		dispatched++
		n := dispatched
		eventsMu.Unlock()
		// A randomly slow consumer, with an occasional long stall: this is what
		// fills the queue and forces the drop path.
		if rng.intn(400) == 0 {
			time.Sleep(rng.duration(5*time.Millisecond, 25*time.Millisecond))
			return
		}
		if n%7 == 0 {
			time.Sleep(time.Duration(rng.intn(3)) * time.Millisecond)
		}
	}

	h := startSoakManager(t, v, queue, d, func(h *WSHyperliquid) {
		h.SetRawFrameHandler(raw.add)
	})
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitSoak(t, "the first connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	var droppedMu sync.Mutex
	var droppedFrames []connector.DroppedFrame
	h.SetOnFrameDropped(func(f connector.DroppedFrame) {
		droppedMu.Lock()
		droppedFrames = append(droppedFrames, f)
		droppedMu.Unlock()
	})

	// Writer: paced frames, occasional bursts, garbage and oversized frames.
	// A separate ticker drops the socket every 1-5 s.
	stopAt := time.Now().Add(dur)
	var injectedDrops int
	// A generation can be dropped only once: dropping an already dead socket
	// does not produce a Disconnects increment, so counting it would make
	// "Disconnects >= injected drops" fail for a reason the connector is not
	// responsible for.
	var dropMu sync.Mutex
	droppedGen := map[int]bool{}
	var garbageWritten int
	var seqWritten int64
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for time.Now().Before(stopAt) {
			// Always write on the newest generation: after a venue-side drop the
			// old socket is dead and the next frame must go to the reconnected
			// one, exactly as a real venue's broadcast would.
			gen := v.numConns() - 1
			if gen < 0 {
				select {
				case <-v.stop:
					return
				case <-time.After(time.Millisecond):
				}
				continue
			}
			burst := 1
			if rng.intn(100) < 4 {
				burst = 10 + rng.intn(40)
			}
			for i := 0; i < burst && time.Now().Before(stopAt); i++ {
				seqWritten++
				var payload string
				switch {
				case rng.intn(100) < 3:
					payload = soakGarbageFrame(seqWritten)
					garbageWritten++
				case rng.intn(500) == 0:
					payload = soakPaddedBookFrame(seqWritten)
				default:
					payload = soakBookFrame(seqWritten)
				}
				if !v.write(gen, payload) {
					time.Sleep(time.Millisecond)
					break
				}
				if burst == 1 {
					time.Sleep(rng.duration(2*time.Millisecond, 6*time.Millisecond))
				}
			}
			if rng.intn(100) < 2 {
				time.Sleep(rng.duration(10*time.Millisecond, 40*time.Millisecond))
			}
		}
	}()
	dropDone := make(chan struct{})
	go func() {
		defer close(dropDone)
		for time.Now().Before(stopAt) {
			time.Sleep(rng.duration(1*time.Second, 5*time.Second))
			// Do not drop in the last 800 ms: the connector must have time to
			// notice the drop and reconnect before the venue is shut down,
			// otherwise "Disconnects >= injected drops" would measure teardown
			// timing instead of the reconnect path.
			if time.Until(stopAt) < 800*time.Millisecond {
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

	// The venue is quiet now; let the consumer drain.
	settleSoak(t, h, 10*time.Second)

	st := h.Stats()
	stopStart := time.Now()
	h.Stop()
	stopTook := time.Since(stopStart)
	v.shutdown()

	eventsMu.Lock()
	events := append([]*connector.HyperliquidBookEvent(nil), bookEvents...)
	eventsMu.Unlock()
	droppedMu.Lock()
	droppedFs := append([]connector.DroppedFrame(nil), droppedFrames...)
	droppedMu.Unlock()
	reads := raw.snapshot()

	// ── observed numbers ──
	read := int(st.FramesRead)
	delivered := len(events)
	garbageRead := 0
	readIDs := make(map[uint64]bool, read)
	for i, r := range reads {
		id := uint64(i + 1) // the hook runs once per read frame, in FrameID order
		readIDs[id] = true
		if _, ok := frameSeq(r.data); !ok {
			garbageRead++
		}
	}
	deliveredIDs := make(map[uint64]int, delivered)
	missing := map[uint64]bool{}
	for _, e := range events {
		deliveredIDs[e.FrameID]++
	}
	for id := uint64(1); id <= uint64(read); id++ {
		if deliveredIDs[id] == 0 {
			missing[id] = true
		}
	}
	dropSet := map[uint64]int{}
	queueFull, drainedHook := 0, 0
	for _, f := range droppedFs {
		dropSet[f.FrameID]++
		switch f.Cause {
		case connector.DropQueueFull:
			queueFull++
		case connector.DropDrainedOnDisconnect:
			drainedHook++
		default:
			t.Fatalf("drop hook reported undocumented cause %q", f.Cause)
		}
	}
	// A missing ID is one of: reported dropped, reported drained, or a frame
	// the decoder read and produced no event for (garbage). Every other
	// category (paced frame, padded frame) produces exactly one book event.
	unexplained := 0
	for id := range missing {
		if dropSet[id] > 0 {
			continue
		}
		if _, ok := frameSeq(reads[id-1].data); !ok {
			continue // garbage, counted below
		}
		unexplained++
	}
	connIDs := map[uint64]bool{}
	var maxConnID uint64
	monotonic := true
	for _, e := range events {
		connIDs[e.ConnID] = true
		if e.ConnID < maxConnID {
			monotonic = false
		}
		maxConnID = e.ConnID
	}

	t.Logf("market: dur=%v reads=%d delivered=%d dropped=%d drained=%d garbage_read=%d missing=%d drops=%d dials(accepts)=%d disconnects=%d connID=%d seq_written=%d close=%v",
		dur, read, delivered, st.FramesDropped, st.FramesDrained, garbageRead, len(missing), injectedDrops, v.numConns(), st.Disconnects, st.ConnID, seqWritten, stopTook)

	// ── assertions ──
	// A garbage frame is enqueued like any other: it may be dropped/drained
	// (already counted in those counters) or dispatched with no event. Counting
	// the ones that were dropped lets the identity below stay exact.
	garbageLost := 0
	for _, f := range droppedFs {
		if _, ok := frameSeq(reads[f.FrameID-1].data); !ok {
			garbageLost++
		}
	}
	deliveredGarbage := 0
	for id := range deliveredIDs {
		if _, ok := frameSeq(reads[id-1].data); !ok {
			deliveredGarbage++
		}
	}
	if len(reads) != read {
		t.Fatalf("raw hook recorded %d frames, Stats.FramesRead = %d (a read frame bypassed the hook or the hooks disagree)", len(reads), read)
	}
	if got, want := int(st.FramesRead), delivered+int(st.FramesDropped)+int(st.FramesDrained)+garbageRead-garbageLost; got != want {
		var dups, outOfRange, both []string
		for id, n := range deliveredIDs {
			if n > 1 {
				dups = append(dups, fmt.Sprintf("%d(x%d)", id, n))
			}
			if id == 0 || id > uint64(read) {
				outOfRange = append(outOfRange, fmt.Sprint(id))
			}
			if dropSet[id] > 0 {
				both = append(both, fmt.Sprint(id))
			}
		}
		t.Fatalf("FramesRead %d != delivered %d + dropped %d + drained %d + garbage_read %d - garbage_dropped %d = %d; duplicate_ids=%v out_of_range=%v delivered_and_dropped=%v",
			got, delivered, st.FramesDropped, st.FramesDrained, garbageRead, garbageLost, want, dups, outOfRange, both)
	}
	if queueFull != int(st.FramesDropped) {
		t.Fatalf("drop hook reported %d queue_full drops, Stats.FramesDropped = %d", queueFull, st.FramesDropped)
	}
	if drainedHook != int(st.FramesDrained) {
		t.Fatalf("drop hook reported %d drained frames, Stats.FramesDrained = %d", drainedHook, st.FramesDrained)
	}
	if deliveredGarbage != 0 {
		t.Fatalf("%d frames the raw hook could not parse were delivered as events", deliveredGarbage)
	}
	if got, want := len(missing), int(st.FramesDropped)+int(st.FramesDrained)+garbageRead-garbageLost; got != want {
		t.Fatalf("FrameID gaps seen by the consumer = %d, want dropped %d + drained %d + garbage %d - garbage_dropped %d = %d",
			got, st.FramesDropped, st.FramesDrained, garbageRead, garbageLost, want)
	}
	if unexplained != 0 {
		t.Fatalf("%d delivered-FrameID gaps are neither a counted drop nor a garbage read", unexplained)
	}
	for id, n := range deliveredIDs {
		if n > 1 {
			t.Fatalf("FrameID %d delivered %d times", id, n)
		}
		if dropSet[id] > 0 {
			t.Fatalf("FrameID %d both delivered and reported dropped", id)
		}
	}
	if st.FramesDropped == 0 {
		t.Fatalf("no frame was dropped in %v — the slow consumer never filled the %d-slot queue, the drop path was not exercised", dur, queue)
	}
	if st.Disconnects < uint64(injectedDrops) {
		t.Fatalf("Disconnects %d < injected drops %d", st.Disconnects, injectedDrops)
	}
	// Every drop except possibly the last one must be followed by a new
	// generation. The final drop can legitimately still be waiting for the next
	// scheduled dial: the reconnect backoff is exponential up to 30 s and, with
	// ten drops in a minute, the next attempt falls outside the run.
	if st.ConnID < uint64(injectedDrops) {
		t.Fatalf("ConnID %d after %d injected drops: the connection was never re-established", st.ConnID, injectedDrops)
	}
	if st.ConnID < uint64(injectedDrops)+1 {
		t.Logf("market: ConnID=%d with %d drops and %d accepts: the last drop's reconnect is still in backoff at teardown",
			st.ConnID, injectedDrops, v.numConns())
	}
	if !monotonic {
		t.Fatalf("ConnID decreased across delivered events")
	}
	if len(connIDs) < 2 && injectedDrops > 1 {
		t.Fatalf("events came from %d generation(s) after %d drops", len(connIDs), injectedDrops)
	}
	// Dial budget: one reconnect per injected drop, plus the paced retries of a
	// venue that is briefly gone. A storm would be orders of magnitude larger.
	dialBound := injectedDrops + int(dur/(20*time.Millisecond)) + 16
	if accepts := v.numConns(); accepts > dialBound {
		t.Fatalf("%d dials for %d injected drops in %v (bound %d)", accepts, injectedDrops, dur, dialBound)
	}
	if stopTook > 2*time.Second {
		t.Fatalf("Stop() took %v (> 2s)", stopTook)
	}
	if st.QueueLen != 0 {
		t.Fatalf("QueueLen = %d after the consumer drained", st.QueueLen)
	}
	soakAssertNoLeak(t, "market", baseline)
	t.Logf("market: wall=%v generations=%d", time.Since(scenarioStart), len(connIDs))
}

// ─────────────────────────────────────────────────────────────
// Scenario 2: account connection (no silent loss)
// ─────────────────────────────────────────────────────────────

func TestSoakHyperliquidAccount(t *testing.T) {
	seed, dur := soakParams(t)
	rng := newSoakRNG(seed + 7)
	scenarioStart := time.Now()
	baseline := runtime.NumGoroutine()

	const queue = 16
	v := newSoakVenue(t)

	type fill struct {
		tid     int64
		frameID uint64
		connID  uint64
	}
	var mu sync.Mutex
	var fills []fill
	var gaps []*connector.HyperliquidGapEvent
	var order []string // dispatch order: "fill:<conn>:<tid>" / "gap:<prev>><conn>"
	d := func(ev any) {
		mu.Lock()
		switch e := ev.(type) {
		case *connector.HyperliquidUserFillsEvent:
			for _, f := range e.Fills {
				fills = append(fills, fill{tid: f.Tid, frameID: e.FrameID, connID: e.ConnID})
				order = append(order, fmt.Sprintf("fill:%d:%d", e.ConnID, f.Tid))
			}
		case *connector.HyperliquidGapEvent:
			gaps = append(gaps, e)
			order = append(order, fmt.Sprintf("gap:%d>%d", e.PrevConnID, e.ConnID))
		}
		mu.Unlock()
		// Forced consumer stalls: with a 16-slot queue this overflows it, which
		// on an account-bearing connection must fail the connection.
		if rng.intn(60) == 0 {
			time.Sleep(rng.duration(20*time.Millisecond, 90*time.Millisecond))
		} else if rng.intn(4) == 0 {
			time.Sleep(time.Duration(rng.intn(2)) * time.Millisecond)
		}
	}

	raw := &rawLog{}
	h := startSoakManager(t, v, queue, d, func(h *WSHyperliquid) {
		h.SetRawFrameHandler(raw.add)
	})
	if err := h.SubscribeUserFills(context.Background(), testUser); err != nil {
		t.Fatalf("subscribe userFills: %v", err)
	}
	waitSoak(t, "the first connection", 5*time.Second, func() bool { return v.numConns() == 1 })

	var droppedMu sync.Mutex
	var droppedFrames []connector.DroppedFrame
	h.SetOnFrameDropped(func(f connector.DroppedFrame) {
		droppedMu.Lock()
		droppedFrames = append(droppedFrames, f)
		droppedMu.Unlock()
	})
	var discMu sync.Mutex
	var discLog []string
	h.SetOnDisconnect(func(error) {
		st := h.Stats()
		discMu.Lock()
		discLog = append(discLog, fmt.Sprintf("read=%d dropped=%d drained=%d conn=%d",
			st.FramesRead, st.FramesDropped, st.FramesDrained, st.ConnID))
		discMu.Unlock()
	})

	stopAt := time.Now().Add(dur)
	var tid int64
	written := map[int][]int64{} // venue generation -> tids whose write succeeded
	var writtenMu sync.Mutex
	var injectedDrops int
	var dropMu sync.Mutex
	droppedGen := map[int]bool{}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for time.Now().Before(stopAt) {
			gen := v.numConns() - 1
			if gen < 0 {
				select {
				case <-v.stop:
					return
				case <-time.After(time.Millisecond):
				}
				continue
			}
			burst := 1
			if rng.intn(100) < 6 {
				burst = 5 + rng.intn(30)
			}
			for i := 0; i < burst && time.Now().Before(stopAt); i++ {
				tid++
				if !v.write(gen, userFillFrame(tid, "BTC")) {
					time.Sleep(time.Millisecond)
					break
				}
				writtenMu.Lock()
				written[gen] = append(written[gen], tid)
				writtenMu.Unlock()
				if burst == 1 {
					time.Sleep(rng.duration(1*time.Millisecond, 4*time.Millisecond))
				}
			}
		}
	}()
	dropDone := make(chan struct{})
	go func() {
		defer close(dropDone)
		for time.Now().Before(stopAt) {
			time.Sleep(rng.duration(1*time.Second, 3*time.Second))
			if time.Until(stopAt) < 800*time.Millisecond {
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
	settleSoak(t, h, 10*time.Second)

	st := h.Stats()
	stopStart := time.Now()
	h.Stop()
	stopTook := time.Since(stopStart)
	v.shutdown()

	mu.Lock()
	fs := append([]fill(nil), fills...)
	gs := append([]*connector.HyperliquidGapEvent(nil), gaps...)
	evOrder := append([]string(nil), order...)
	mu.Unlock()
	discMu.Lock()
	discTrace := append([]string(nil), discLog...)
	discMu.Unlock()
	droppedMu.Lock()
	droppedFs := append([]connector.DroppedFrame(nil), droppedFrames...)
	droppedMu.Unlock()
	writtenMu.Lock()
	w := map[int][]int64{}
	for g, tids := range written {
		w[g] = append([]int64(nil), tids...)
	}
	writtenMu.Unlock()

	reads := raw.snapshot()
	if len(reads) != int(st.FramesRead) {
		t.Fatalf("raw hook recorded %d frames, Stats.FramesRead = %d", len(reads), st.FramesRead)
	}
	// frameID -> tid, straight from the bytes the socket delivered: this is what
	// lets the check below prove a missing tid was a counted loss.
	tidFrame := map[int64]uint64{}
	for i, r := range reads {
		var env struct {
			Data struct {
				Fills []struct {
					Tid int64 `json:"tid"`
				} `json:"fills"`
			} `json:"data"`
		}
		if err := json.Unmarshal(r.data, &env); err != nil || len(env.Data.Fills) != 1 {
			t.Fatalf("frame %d is not the expected single-fill userFills frame: %v", i+1, err)
		}
		tidFrame[env.Data.Fills[0].Tid] = uint64(i + 1)
	}

	// Delivered tids per connector generation + dispatch positions (fills and
	// gap notices share one ordered log).
	delivered := map[uint64][]int64{}
	firstFillPos := map[uint64]int{}
	lastFillPos := map[uint64]int{}
	gapPos := map[uint64]int{}
	for i, ev := range evOrder {
		switch {
		case strings.HasPrefix(ev, "fill:"):
			var connID uint64
			var tid int64
			if _, err := fmt.Sscanf(ev, "fill:%d:%d", &connID, &tid); err != nil {
				t.Fatalf("internal: bad order entry %q", ev)
			}
			if _, ok := firstFillPos[connID]; !ok {
				firstFillPos[connID] = i
			}
			lastFillPos[connID] = i
			delivered[connID] = append(delivered[connID], tid)
		case strings.HasPrefix(ev, "gap:"):
			var prev, conn uint64
			if _, err := fmt.Sscanf(ev, "gap:%d>%d", &prev, &conn); err != nil {
				t.Fatalf("internal: bad order entry %q", ev)
			}
			gapPos[prev] = i
		}
	}
	gapFor := map[uint64]*connector.HyperliquidGapEvent{}
	for _, g := range gs {
		gapFor[g.PrevConnID] = g
	}

	queueFull, drainedHook := 0, 0
	droppedIDs := map[uint64]bool{}
	for _, f := range droppedFs {
		droppedIDs[f.FrameID] = true
		switch f.Cause {
		case connector.DropQueueFull:
			queueFull++
		case connector.DropDrainedOnDisconnect:
			drainedHook++
		default:
			t.Fatalf("drop hook reported undocumented cause %q", f.Cause)
		}
	}

	t.Logf("account: dur=%v fills=%d gaps=%d dropped=%d drained=%d hook[queue_full=%d drained=%d] reads=%d disconnects=%d connID=%d venue_generations=%d drops=%d close=%v",
		dur, len(fs), len(gs), st.FramesDropped, st.FramesDrained, queueFull, drainedHook,
		st.FramesRead, st.Disconnects, st.ConnID, len(w), injectedDrops, stopTook)
	if len(discTrace) <= 10 {
		t.Logf("account: disconnect trace: %v", discTrace)
	}

	// ── identity: every read frame produced exactly one fill event, or was
	// counted as dropped/drained ──
	if got, want := int(st.FramesRead), len(fs)+int(st.FramesDropped)+int(st.FramesDrained); got != want {
		t.Fatalf("FramesRead %d != delivered fills %d + dropped %d + drained %d = %d",
			got, len(fs), st.FramesDropped, st.FramesDrained, want)
	}
	if queueFull != int(st.FramesDropped) {
		t.Fatalf("drop hook reported %d queue_full drops, Stats.FramesDropped = %d", queueFull, st.FramesDropped)
	}
	if drainedHook != int(st.FramesDrained) {
		t.Fatalf("drop hook reported %d drained frames, Stats.FramesDrained = %d", drainedHook, st.FramesDrained)
	}

	// ── no tid twice, no generation goes backwards ──
	seen := map[int64]uint64{}
	var prevConn uint64
	for _, f := range fs {
		if got, ok := seen[f.tid]; ok {
			t.Fatalf("tid %d delivered twice (generations %d and %d)", f.tid, got, f.connID)
		}
		seen[f.tid] = f.connID
		if f.connID < prevConn {
			t.Fatalf("fill tid %d delivered on generation %d after %d", f.tid, f.connID, prevConn)
		}
		prevConn = f.connID
	}

	// ── venue generation ↔ connector generation ──
	// The venue's accept count and the connector's ConnID are not the same
	// counter (an abandoned handshake still consumes a venue accept), so the
	// mapping is derived from the data and asserted to be consistent.
	tidGen := map[int64]int{}
	for gen, tids := range w {
		for _, tid := range tids {
			tidGen[tid] = gen
		}
	}
	connIDs := make([]uint64, 0, len(delivered))
	for c := range delivered {
		connIDs = append(connIDs, c)
	}
	sort.Slice(connIDs, func(i, j int) bool { return connIDs[i] < connIDs[j] })
	connGen := map[uint64]int{}
	prevGen := -1
	for _, c := range connIDs {
		gens := map[int]bool{}
		for _, tid := range delivered[c] {
			gen, ok := tidGen[tid]
			if !ok {
				t.Fatalf("generation %d delivered tid %d that the venue never wrote", c, tid)
			}
			gens[gen] = true
		}
		if len(gens) != 1 {
			t.Fatalf("generation %d delivered fills written in %d different venue generations", c, len(gens))
		}
		for gen := range gens {
			if gen <= prevGen {
				t.Fatalf("connector generation %d maps to venue generation %d after %d (generations interleaved)", c, gen, prevGen)
			}
			prevGen = gen
			connGen[c] = gen
		}
	}

	// ── per connector generation: no silent hole ──
	// For every tid written during a generation that the consumer did not
	// receive, one of these must hold:
	//   - its frame was reported through the drop hook (queue_full or drained):
	//     a counted loss;
	//   - it was never read (socket died / final generation's tail): invisible
	//     to the connector by construction;
	//   - it sits after the last delivered tid of the generation (unread tail).
	// A tid in the middle that was read and has no counter behind it would be a
	// silent loss and fails the test.
	lostGens := 0
	lastGenLost := 0
	postHoleDeliveries := 0
	maxConn := connIDs[len(connIDs)-1]
	for _, c := range connIDs {
		gen := connGen[c]
		writtenGen := w[gen]
		got := delivered[c]
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Fatalf("generation %d delivered tids out of order: %d after %d", c, got[i], got[i-1])
			}
		}
		if len(got) > len(writtenGen) {
			t.Fatalf("generation %d delivered %d tids but only %d were written", c, len(got), len(writtenGen))
		}
		lastDelivered := int64(0)
		if n := len(got); n > 0 {
			lastDelivered = got[n-1]
		}
		var holes []int64
		for _, tid := range writtenGen {
			if containsTid(got, tid) {
				continue
			}
			fid, read := tidFrame[tid]
			switch {
			case !read:
				// written, never read: wire-level, invisible
			case droppedIDs[fid]:
				// counted loss
			case tid > lastDelivered:
				// unread tail of the generation
			default:
				t.Fatalf("generation %d: tid %d (frame %d) was written before the last delivered tid %d, was read, and was neither delivered nor counted as dropped/drained — silent loss",
					c, tid, fid, lastDelivered)
			}
			if read && tid < lastDelivered {
				holes = append(holes, tid)
			}
		}
		if len(got) == len(writtenGen) {
			continue
		}
		if c == maxConn {
			lastGenLost = len(writtenGen) - len(got)
			continue
		}
		lostGens++
		g, ok := gapFor[c]
		if !ok {
			t.Fatalf("generation %d lost %d tids with no HyperliquidGapEvent for it", c, len(writtenGen)-len(got))
		}
		if g.ConnID <= c {
			t.Fatalf("gap for generation %d announces ConnID %d", c, g.ConnID)
		}
		if g.Cause != connector.HyperliquidGapQueueOverflow && g.Cause != connector.HyperliquidGapDisconnect {
			t.Fatalf("gap cause %q is not a documented cause", g.Cause)
		}
		if !g.AccountChannels {
			t.Fatalf("gap for an account-bearing connection has AccountChannels=false")
		}
		if g.FrameID != 0 {
			t.Fatalf("gap event carries FrameID %d, want 0 (synthetic)", g.FrameID)
		}
		if g.LostFrames > uint64(len(writtenGen)) {
			t.Fatalf("gap for generation %d claims %d lost frames but only %d were written", c, g.LostFrames, len(writtenGen))
		}
		// Ordering: last fill of the old generation, then the gap, then the
		// first fill of the next generation.
		if next, hasNext := nextConn(connIDs, c); hasNext && len(delivered[next]) > 0 {
			gp, ok := gapPos[c]
			if !ok {
				t.Fatalf("generation %d has fills and lost frames but no gap position recorded", c)
			}
			if !(lastFillPos[c] < gp && gp < firstFillPos[next]) {
				t.Fatalf("gap for generation %d at dispatch position %d is not between its last fill (%d) and the next generation's first fill (%d)",
					c, gp, lastFillPos[c], firstFillPos[next])
			}
		}
		if len(holes) > 0 {
			// The hole is counted (drop hook) and covered by this generation's
			// gap notice, but fills with a higher tid were delivered first: the
			// drain raced the one in-flight dispatcher frame. At most two can
			// slip through — the in-flight frame plus one enqueued between
			// Disconnect() and the socket actually closing.
			late := 0
			for _, tid := range got {
				if tid > holes[0] {
					late++
				}
			}
			postHoleDeliveries += late
			t.Logf("account: generation %d delivered %d fill(s) after a counted hole of %d tid(s) %v (drain raced the in-flight dispatcher frame; the loss is counted and the generation's gap notice still precedes the next generation)",
				c, late, len(holes), holes)
			if late > 2 {
				t.Fatalf("generation %d delivered %d fills after a counted hole %v — more than the in-flight frame (plus one racing enqueue) the design allows", c, late, holes)
			}
		}
	}
	if lostGens == 0 {
		t.Fatalf("no generation lost a frame — the overflow path was never exercised (queue=%d)", queue)
	}
	if len(gs) == 0 {
		t.Fatalf("no HyperliquidGapEvent in %v", dur)
	}
	if st.Disconnects < uint64(injectedDrops) {
		t.Fatalf("Disconnects %d < injected drops %d", st.Disconnects, injectedDrops)
	}
	if st.ConnID < uint64(injectedDrops) {
		t.Fatalf("ConnID %d after %d injected drops", st.ConnID, injectedDrops)
	}
	dialBound := injectedDrops + len(gs) + int(dur/(20*time.Millisecond)) + 16
	if accepts := v.numConns(); accepts > dialBound {
		t.Fatalf("%d dials for %d drops + %d gaps in %v (bound %d)", accepts, injectedDrops, len(gs), dur, dialBound)
	}
	if stopTook > 2*time.Second {
		t.Fatalf("Stop() took %v (> 2s)", stopTook)
	}
	soakAssertNoLeak(t, "account", baseline)
	t.Logf("account: wall=%v lost_generations=%d last_generation_unread_tail=%d fills_delivered_after_a_counted_hole=%d",
		time.Since(scenarioStart), lostGens, lastGenLost, postHoleDeliveries)
}
