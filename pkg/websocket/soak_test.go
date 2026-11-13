package websocket

// soak_test.go is H4a's chaos soak for the connection lifecycle. It is skipped
// unless CONNECTOR_SOAK=1 (so the default `go test ./...` stays fast):
//
//	CONNECTOR_SOAK=1 CONNECTOR_SOAK_SECONDS=60 CONNECTOR_SOAK_SEED=2 \
//	    go test -race -run Soak ./pkg/websocket
//
// Three scenarios share one seeded PRNG and one deadline:
//
//  1. reconnect storm — one manager against a chaotic loopback venue that
//     refuses, accepts-then-closes, accepts-then-drops after a random hold and
//     accepts-then-never-answers-the-close-handshake. Asserts the dial rate
//     stays under a bound computed from the backoff policy (a tight loop would
//     be orders of magnitude above it), and that Close() returns in < 2 s.
//  2. NoReconnect — a dropped socket is never redialled.
//  3. Close() in every lifecycle state — dialing, connected, reconnect-wait,
//     after a failed first dial — each < 2 s.
//
// Every scenario ends with Close()/shutdown inside 2 s and the goroutine count
// back to the pre-scenario baseline (settle loop + pprof dump on mismatch).
// Nothing here talks to a venue; every endpoint is 127.0.0.1.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// stormRNG is a small xorshift so a seed replays identically on any Go
// version (math/rand's algorithm is stable, but this keeps the report exact).
type stormRNG struct {
	mu sync.Mutex
	s  uint64
}

func newStormRNG(seed int64) *stormRNG {
	s := uint64(seed)*0x9E3779B97F4A7C15 + 0x2545F4914F6CDD1D
	if s == 0 {
		s = 0x9E3779B97F4A7C15
	}
	return &stormRNG{s: s}
}

func (r *stormRNG) next() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s ^= r.s << 13
	r.s ^= r.s >> 7
	r.s ^= r.s << 17
	return r.s
}

func (r *stormRNG) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

func (r *stormRNG) duration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(r.next()%uint64(max-min))
}

// assertNoLeak fails unless the goroutine count settles back to baseline.
func assertNoLeak(t *testing.T, what string, baseline int) {
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
// Chaotic loopback venue
// ─────────────────────────────────────────────────────────────

type chaosMode int

const (
	modeRefuse     chaosMode = iota // HTTP 503 before the handshake
	modeCloseNow                    // accept, then drop the TCP socket at once
	modeHoldDrop                    // accept, hold for a random time, then drop
	modeStallClose                  // accept, then never read: a half-open peer
)

type chaosVenue struct {
	url string
	srv *httptest.Server
	rng *stormRNG

	holdMin, holdMax time.Duration

	mu      sync.Mutex
	accepts int // every request that reached the handler
	dialed  int // successful handshakes
	dropped int // sockets the venue closed itself
	conns   []*coderws.Conn

	stop     chan struct{}
	stopOnce sync.Once
}

func newChaosVenue(t *testing.T, rng *stormRNG, holdMin, holdMax time.Duration) *chaosVenue {
	t.Helper()
	v := &chaosVenue{rng: rng, holdMin: holdMin, holdMax: holdMax, stop: make(chan struct{})}
	v.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v.mu.Lock()
		idx := v.accepts
		v.accepts++
		// One in four requests is refused before the handshake; the rest are
		// split evenly between the three accepted behaviours, with the first
		// request always accepted so the scenario can start from a live
		// connection.
		mode := modeRefuse
		if idx == 0 {
			mode = modeHoldDrop
		} else {
			switch idx % 4 {
			case 0:
				mode = modeRefuse
			case 1:
				mode = modeCloseNow
			case 2:
				mode = modeStallClose
			default:
				mode = modeHoldDrop
			}
		}
		v.mu.Unlock()

		if mode == modeRefuse {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		v.dialed++
		v.conns = append(v.conns, c)
		v.mu.Unlock()

		// One data frame per connection: exercises OnMessage and refreshes the
		// data-staleness clock, so a held connection looks alive.
		_ = c.Write(context.Background(), coderws.MessageText, []byte("hello"))

		switch mode {
		case modeCloseNow:
			v.countDrop()
			_ = c.CloseNow()
		case modeStallClose:
			// Deliberately never read: the client's close frame is never
			// answered and its keepalive pings are never answered either, which
			// is the half-open peer Close() must bound.
			<-v.stop
			v.countDrop()
			_ = c.CloseNow()
		case modeHoldDrop:
			// Drain (and thereby answer pings) in the background while the
			// hold timer runs, so the connection can reach "stable" before the
			// venue drops it.
			go func(c *coderws.Conn) {
				for {
					if _, _, err := c.Read(context.Background()); err != nil {
						return
					}
				}
			}(c)
			select {
			case <-time.After(v.rng.duration(v.holdMin, v.holdMax)):
			case <-v.stop:
			}
			v.countDrop()
			_ = c.CloseNow()
		}

		// Drain whatever the client writes (nothing, keepalives are off) so the
		// handler returns only when the socket dies.
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	v.url = "ws" + strings.TrimPrefix(v.srv.URL, "http")
	t.Cleanup(v.shutdown)
	return v
}

func (v *chaosVenue) countDrop() {
	v.mu.Lock()
	v.dropped++
	v.mu.Unlock()
}

func (v *chaosVenue) counts() (accepts, dialed, dropped int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.accepts, v.dialed, v.dropped
}

func (v *chaosVenue) shutdown() {
	v.stopOnce.Do(func() { close(v.stop) })
	v.mu.Lock()
	conns := append([]*coderws.Conn(nil), v.conns...)
	v.mu.Unlock()
	for _, c := range conns {
		_ = c.CloseNow()
	}
	v.srv.Close()
}

// ─────────────────────────────────────────────────────────────
// Scenario 1: reconnect storm
// ─────────────────────────────────────────────────────────────

func TestSoakWebsocketChaos(t *testing.T) {
	seed, total := soakParams(t)
	rng := newStormRNG(seed)

	deadline := time.Now().Add(total)
	stormFor := total * 6 / 10
	if stormFor < 3*time.Second {
		stormFor = total / 2
	}
	runReconnectStorm(t, rng, stormFor)
	runNoReconnect(t, rng)
	if left := time.Until(deadline); left > 2*time.Second {
		runCloseInEveryState(t, rng, left)
	}
}

// runReconnectStorm holds one manager against the chaotic venue and asserts the
// dial rate stays within the backoff policy's bound.
func runReconnectStorm(t *testing.T, rng *stormRNG, dur time.Duration) {
	t.Helper()
	baseline := runtime.NumGoroutine()

	const (
		baseInterval = 40 * time.Millisecond
		maxInterval  = 160 * time.Millisecond
		holdMin      = 300 * time.Millisecond
		holdMax      = 900 * time.Millisecond
	)
	v := newChaosVenue(t, rng, holdMin, holdMax)

	var dials atomic.Int64
	var dialTimesMu sync.Mutex
	var dialTimes []time.Time
	gate := func(ctx context.Context) error {
		dials.Add(1)
		dialTimesMu.Lock()
		dialTimes = append(dialTimes, time.Now())
		dialTimesMu.Unlock()
		return nil
	}

	var messages, connects, disconnects atomic.Int64
	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error {
		messages.Add(1)
		return nil
	}
	ws.OnConnect = func(context.Context, *coderws.Conn) error {
		connects.Add(1)
		return nil
	}
	ws.OnDisconnect = func(error) { disconnects.Add(1) }

	opts := DefaultWSOptions()
	opts.ReconnectInterval = baseInterval.Milliseconds()
	opts.ReconnectMaxInterval = maxInterval.Milliseconds()
	opts.MinStableConnectionMs = 150
	opts.ConnectionTimeout = 2000
	// Keepalives are part of the chaos: a peer that stops answering pings is
	// exactly the half-open case the pong watchdog exists for, and without it a
	// stalled connection would sit there until the end of the scenario.
	opts.PingInterval = 50
	opts.PongTimeout = 100
	opts.DisableDataStaleWatchdog = true
	opts.DialGate = gate

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ws.Connect(ctx, v.url, opts); err != nil {
		t.Fatalf("storm: initial dial must succeed (the venue accepts request 0): %v", err)
	}

	time.Sleep(dur)

	closeStart := time.Now()
	ws.Close()
	closeTook := time.Since(closeStart)
	cancel()
	v.shutdown()

	accepts, dialed, dropped := v.counts()
	nDials := dials.Load()

	// Bound from the policy: one dial per stable drop (a drop can wake the
	// reconnect loop immediately), plus one per max-interval wait, plus the
	// initial dial and the backoff ramp. A tight loop is 3-4 orders of
	// magnitude above this.
	dropBound := int64(dur/holdMin) + 1
	waitBound := int64(dur/maxInterval) + 1
	bound := dropBound + waitBound + 32
	t.Logf("storm: dur=%v dials=%d (%.2f/s) bound=%d venue{accepts=%d dialed=%d dropped=%d} connects=%d disconnects=%d messages=%d close=%v",
		dur, nDials, float64(nDials)/dur.Seconds(), bound, accepts, dialed, dropped, connects.Load(), disconnects.Load(), messages.Load(), closeTook)

	if closeTook > 2*time.Second {
		t.Fatalf("storm: Close() took %v (> 2s)", closeTook)
	}
	if nDials < 2 {
		t.Fatalf("storm: only %d dial(s) in %v — the chaos never forced a reconnect", nDials, dur)
	}
	if nDials > bound {
		t.Fatalf("storm: %d dial attempts in %v exceed the backoff bound %d (tight loop?)", nDials, dur, bound)
	}
	// Worst 1-second window: a tight loop shows up here even when the average
	// looks plausible.
	dialTimesMu.Lock()
	times := append([]time.Time(nil), dialTimes...)
	dialTimesMu.Unlock()
	maxPerSec := 0
	for i := range times {
		n := 0
		for j := i; j < len(times) && times[j].Sub(times[i]) <= time.Second; j++ {
			n++
		}
		if n > maxPerSec {
			maxPerSec = n
		}
	}
	const oneSecBound = 25 // 1/holdMin + 1/maxInterval + slack ≈ 4+7+14
	t.Logf("storm: worst 1s dial window=%d (bound %d)", maxPerSec, oneSecBound)
	if maxPerSec > oneSecBound {
		t.Fatalf("storm: %d dials inside one second (> %d): the reconnect loop is not paced", maxPerSec, oneSecBound)
	}
	if int64(disconnects.Load()) < int64(dropped)-2 {
		t.Fatalf("storm: OnDisconnect fired %d times for %d venue-side drops", disconnects.Load(), dropped)
	}
	if connects.Load() != int64(dialed) && connects.Load() != int64(dialed)-1 {
		// A handshake the client abandoned mid-dial (Close while dialling) never
		// reaches OnConnect; one such miss is allowed, many mean lost callbacks.
		t.Fatalf("storm: OnConnect fired %d times for %d accepted handshakes", connects.Load(), dialed)
	}
	assertNoLeak(t, "storm", baseline)
}

// ─────────────────────────────────────────────────────────────
// Scenario 2: NoReconnect never redials
// ─────────────────────────────────────────────────────────────

func runNoReconnect(t *testing.T, rng *stormRNG) {
	t.Helper()
	baseline := runtime.NumGoroutine()

	// A dedicated venue that accepts once and drops after a short hold: with
	// NoReconnect the accept count must stay at 1 forever.
	v := newChaosVenue(t, rng, 40*time.Millisecond, 60*time.Millisecond)

	var disconnects atomic.Int64
	disconnectErr := make(chan error, 4)
	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error { return nil }
	ws.OnDisconnect = func(err error) {
		disconnects.Add(1)
		select {
		case disconnectErr <- err:
		default:
		}
	}
	opts := DefaultWSOptions()
	opts.ReconnectInterval = 20
	opts.ReconnectMaxInterval = 40
	opts.MinStableConnectionMs = 10
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true
	opts.NoReconnect = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ws.Connect(ctx, v.url, opts); err != nil {
		t.Fatalf("no-reconnect: initial dial: %v", err)
	}
	time.Sleep(600 * time.Millisecond)

	accepts, dialed, _ := v.counts()
	if accepts != 1 || dialed != 1 {
		t.Fatalf("NoReconnect: venue saw %d requests / %d handshakes, want exactly 1/1 (a redial happened)", accepts, dialed)
	}
	if got := disconnects.Load(); got != 1 {
		t.Fatalf("NoReconnect: OnDisconnect fired %d times, want exactly 1", got)
	}
	if st := ws.Status(); st != StatusDisconnected {
		t.Fatalf("NoReconnect: status = %q after the drop, want %q", st, StatusDisconnected)
	}
	start := time.Now()
	ws.Close()
	took := time.Since(start)
	cancel()
	v.shutdown()
	t.Logf("no-reconnect: accepts=1 dials=1 disconnects=1 status=disconnected close=%v", took)
	if took > 2*time.Second {
		t.Fatalf("NoReconnect: Close() took %v (> 2s)", took)
	}
	assertNoLeak(t, "no-reconnect", baseline)
}

// ─────────────────────────────────────────────────────────────
// Scenario 3: Close() in every lifecycle state
// ─────────────────────────────────────────────────────────────

// runCloseInEveryState cycles the four states the card names until the budget
// runs out: dialling, connected, reconnect-wait and after a failed first dial.
func runCloseInEveryState(t *testing.T, rng *stormRNG, budget time.Duration) {
	t.Helper()
	type result struct {
		state string
		took  time.Duration
		iters int
	}
	results := map[string]*result{
		"dialing":     {state: "dialing"},
		"connected":   {state: "connected"},
		"reconn-wait": {state: "reconn-wait"},
		"failed-dial": {state: "failed-dial"},
	}
	order := []string{"dialing", "connected", "reconn-wait", "failed-dial"}
	deadline := time.Now().Add(budget)
	for i := 0; time.Now().Before(deadline); i++ {
		st := order[i%len(order)]
		var took time.Duration
		switch st {
		case "dialing":
			took = closeWhileDialing(t, rng)
		case "connected":
			took = closeWhileConnected(t, rng)
		case "reconn-wait":
			took = closeDuringReconnectWait(t, rng)
		case "failed-dial":
			took = closeAfterFailedDial(t, rng)
		}
		r := results[st]
		r.iters++
		if took > r.took {
			r.took = took
		}
		if took > 2*time.Second {
			t.Fatalf("Close() during %s took %v (> 2s)", st, took)
		}
	}
	for _, st := range order {
		r := results[st]
		t.Logf("close-state %-12s iterations=%d worst=%v", r.state, r.iters, r.took)
	}
}

// dialingVenue accepts only after a delay, so a Close lands mid-handshake.
func closeWhileDialing(t *testing.T, rng *stormRNG) time.Duration {
	t.Helper()
	var conns []*coderws.Conn
	var mu sync.Mutex
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(rng.duration(200*time.Millisecond, 500*time.Millisecond)):
		case <-release:
			return
		}
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	shutdown := func() {
		unblock()
		mu.Lock()
		for _, c := range conns {
			_ = c.CloseNow()
		}
		mu.Unlock()
		srv.Close()
	}
	defer shutdown()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	baseline := runtime.NumGoroutine()
	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error { return nil }
	opts := DefaultWSOptions()
	opts.ConnectionTimeout = 3000
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true

	connectDone := make(chan error, 1)
	go func() { connectDone <- ws.Connect(context.Background(), url, opts) }()
	time.Sleep(rng.duration(20*time.Millisecond, 80*time.Millisecond))
	start := time.Now()
	ws.Close()
	took := time.Since(start)
	select {
	case <-connectDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("Close() while dialling: Connect never returned")
	}
	if took > 2*time.Second {
		t.Fatalf("Close() while dialling took %v", took)
	}
	// The venue handler may still be sleeping out its accept delay: shut the
	// server down before counting goroutines.
	shutdown()
	assertNoLeak(t, "close-dialing", baseline)
	return took
}

func closeWhileConnected(t *testing.T, rng *stormRNG) time.Duration {
	t.Helper()
	v := newChaosVenue(t, rng, 5*time.Second, 6*time.Second) // held past the scenario
	baseline := runtime.NumGoroutine()
	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error { return nil }
	opts := DefaultWSOptions()
	opts.ReconnectInterval = 20
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true
	ctx, cancel := context.WithCancel(context.Background())
	if err := ws.Connect(ctx, v.url, opts); err != nil {
		cancel()
		t.Fatalf("close-connected: dial: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for ws.Status() != StatusConnected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	ws.Close()
	took := time.Since(start)
	cancel()
	v.shutdown()
	assertNoLeak(t, "close-connected", baseline)
	return took
}

// closeDuringReconnectWait parks the manager in a long backoff wait (the venue
// refuses everything after the first connection) and closes there.
func closeDuringReconnectWait(t *testing.T, rng *stormRNG) time.Duration {
	t.Helper()
	v := newChaosVenue(t, rng, 20*time.Millisecond, 30*time.Millisecond)
	baseline := runtime.NumGoroutine()
	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error { return nil }
	opts := DefaultWSOptions()
	opts.ReconnectInterval = 1000
	opts.ReconnectMaxInterval = 2000
	opts.MinStableConnectionMs = 5
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true
	ctx, cancel := context.WithCancel(context.Background())
	if err := ws.Connect(ctx, v.url, opts); err != nil {
		cancel()
		t.Fatalf("close-reconn-wait: dial: %v", err)
	}
	// The venue drops the first connection after 20-30ms; the client then
	// waits ~1s before redialling. Close inside that window.
	deadline := time.Now().Add(time.Second)
	for ws.Status() != StatusDisconnected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(rng.duration(50*time.Millisecond, 150*time.Millisecond))
	before, _, _ := v.counts()
	start := time.Now()
	ws.Close()
	took := time.Since(start)
	time.Sleep(300 * time.Millisecond) // a redial here would be a bug
	after, _, _ := v.counts()
	cancel()
	v.shutdown()
	if after != before {
		t.Fatalf("redial after Close(): requests %d -> %d", before, after)
	}
	assertNoLeak(t, "close-reconn-wait", baseline)
	return took
}

func closeAfterFailedDial(t *testing.T, rng *stormRNG) time.Duration {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	baseline := runtime.NumGoroutine()

	ws := &BaseWebSocket{}
	ws.OnMessage = func(_ context.Context, _ []byte) error { return nil }
	opts := DefaultWSOptions()
	opts.ConnectionTimeout = 2000
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true
	if err := ws.Connect(context.Background(), url, opts); err == nil {
		t.Fatalf("close-failed-dial: Connect to a refusing server returned nil")
	}
	start := time.Now()
	ws.Close()
	took := time.Since(start)
	srv.Close()
	assertNoLeak(t, "close-failed-dial", baseline)
	return took
}

// keep the linter honest about the helper above being exercised from every
// scenario path; no production symbol is referenced here.
var _ = fmt.Sprintf
