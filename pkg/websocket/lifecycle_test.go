package websocket

// lifecycle_test.go proves the BaseWebSocket lifecycle defects and that every
// documented lifecycle path returns its goroutines after Close():
//
//   1. Close() after a FAILED first dial must return promptly (the historical
//      bug: Connect() created b.done but the dial-failure path never closed it,
//      so Close() blocked on <-b.done forever).
//   2. Close() must not hold b.mu across the close handshake (coder/websocket's
//      Close waits up to ~10s for the peer's close frame, which stalled every
//      Status()/Conn() reader).
//   3. cancel/done access must be safe under Connect/Close races (-race).
//
// plus a goroutine-leak assertion per lifecycle path: failed dial, clean
// connect, peer drop + auto reconnect, caller ctx cancellation, NoReconnect,
// DialGate refusal, double Close, Close racing Connect.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────

// refusedAddr returns a ws:// URL on a loopback port that nothing listens on,
// so the first dial is refused immediately (no timeout involved).
func refusedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return "ws://" + addr
}

// quietOpts returns options that keep the watchdog and keepalive out of the
// picture unless a test opts in: only the lifecycle machinery runs.
func quietOpts() WSOptions {
	opts := DefaultWSOptions()
	opts.PingInterval = 0
	opts.DisableDataStaleWatchdog = true
	opts.ReconnectInterval = 50
	opts.ReconnectMaxInterval = 200
	return opts
}

// newWSReadServer accepts WebSocket connections, counts them, and reads (which
// auto-answers the client's close frame, so Close() completes its handshake
// quickly). stop() shuts the server down early, for tests that assert on the
// goroutine count and must not count the server's own connection handlers.
func newWSReadServer(t *testing.T) (url string, accepts *int32, stop func()) {
	t.Helper()
	var n int32
	accepts = &n
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		atomic.AddInt32(accepts, 1)
		defer c.CloseNow()
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), accepts, srv.Close
}

// wsReadServer is newWSReadServer for tests that do not need to stop the
// server before asserting.
func wsReadServer(t *testing.T) (string, *int32) {
	t.Helper()
	url, accepts, _ := newWSReadServer(t)
	return url, accepts
}

// wsDropServer accepts WebSocket connections, counts them and force-closes the
// socket `after` the handshake — a peer drop on an established connection.
func wsDropServer(t *testing.T, after time.Duration) (string, *int32) {
	t.Helper()
	var accepts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		atomic.AddInt32(&accepts, 1)
		time.Sleep(after)
		c.CloseNow()
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http"), &accepts
}

// goroutineBaseline returns the current goroutine count once it has been
// stable for two consecutive samples, so leftovers from earlier tests are not
// mistaken for a leak of this one.
func goroutineBaseline() int {
	prev := runtime.NumGoroutine()
	for i := 0; i < 60; i++ {
		time.Sleep(10 * time.Millisecond)
		if n := runtime.NumGoroutine(); n == prev {
			return n
		} else {
			prev = n
		}
	}
	return prev
}

// assertNoGoroutineLeak fails with the full goroutine profile if the count does
// not fall back to baseline within 5s of Close().
func assertNoGoroutineLeak(t *testing.T, baseline int, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int
	for {
		got = runtime.NumGoroutine()
		if got <= baseline {
			t.Logf("%s: goroutines=%d (baseline %d) after Close ", label, got, baseline)
			return
		}
		if time.Now().After(deadline) {
			var b strings.Builder
			if err := pprof.Lookup("goroutine").WriteTo(&b, 1); err != nil {
				b.WriteString("(goroutine profile unavailable: " + err.Error() + ")")
			}
			t.Fatalf("%s: %d goroutines alive after Close(), baseline %d\n%s",
				label, got, baseline, b.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// closeWithin runs b.Close() in its own goroutine and fails if it does not
// return within limit. The goroutine guard matters because the defect under
// test is a Close() that never returns.
func closeWithin(t *testing.T, b *BaseWebSocket, limit time.Duration) time.Duration {
	t.Helper()
	type res struct{ took time.Duration }
	ch := make(chan res, 1)
	go func() {
		start := time.Now()
		b.Close()
		ch <- res{time.Since(start)}
	}()
	select {
	case r := <-ch:
		if r.took > limit {
			t.Fatalf("Close() took %v, want <= %v", r.took, limit)
		}
		return r.took
	case <-time.After(limit + 2*time.Second):
		t.Fatalf("Close() did not return within %v (goroutine stuck in Close)", limit+2*time.Second)
		return 0
	}
}

// waitForAccepts waits until the test server has accepted n connections.
// coderws.Dial can return success before the server-side handler increments its
// counter, so a single check would flake.
func waitForAccepts(t *testing.T, n *int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for atomic.LoadInt32(n) < want {
		if time.Now().After(deadline) {
			t.Fatalf("server accepts = %d, want %d", atomic.LoadInt32(n), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ─────────────────────────────────────────────────────────────
// defect 1: Close() after a failed first dial
// ─────────────────────────────────────────────────────────────

func TestCloseAfterRefusedFirstDialReturnsPromptly(t *testing.T) {
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Connect(ctx, refusedAddr(t), quietOpts()); err == nil {
		t.Fatal("Connect() to a refused port returned nil error")
	}

	// The defect: done was created before the dial and never closed on the
	// failure path, so this blocked forever.
	took := closeWithin(t, b, 500*time.Millisecond)
	t.Logf("Close() after refused dial returned in %v", took)

	// Still safe to call repeatedly.
	closeWithin(t, b, 500*time.Millisecond)

	assertNoGoroutineLeak(t, baseline, "failed first dial then Close")
}

func TestConnectRetryAfterFailedDial(t *testing.T) {
	url, accepts := wsReadServer(t)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Connect(ctx, refusedAddr(t), quietOpts()); err == nil {
		t.Fatal("first Connect() to a refused port returned nil error")
	}
	closeWithin(t, b, 500*time.Millisecond)

	// A retry on the same object must dial and connect — the failed attempt
	// must not leave cancel/done in a state that breaks the next generation.
	if err := b.Connect(ctx, url, quietOpts()); err != nil {
		t.Fatalf("Connect() retry after failure: %v", err)
	}
	if got := b.Status(); got != StatusConnected {
		t.Fatalf("Status() after retry = %q, want %q", got, StatusConnected)
	}
	if got := atomic.LoadInt32(accepts); got != 1 {
		t.Fatalf("server accepts = %d, want 1", got)
	}
	closeWithin(t, b, 2*time.Second)
	assertNoGoroutineLeak(t, baseline, "failed dial, retry, Close")
}

// ─────────────────────────────────────────────────────────────
// defect 2: Close() must not hold b.mu across the close handshake
// ─────────────────────────────────────────────────────────────

func TestCloseDoesNotBlockReaders(t *testing.T) {
	// The peer completes the handshake and then never answers anything — no
	// close frame, no data, no pong. coder/websocket's Close() waits ~5s for
	// that close frame; holding b.mu across it stalled every reader.
	url, _ := fakeHalfOpenWSServer(t, false)

	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got := b.Status(); got != StatusConnected {
		t.Fatalf("Status() = %q, want connected", got)
	}

	closed := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		b.Close()
		closed <- time.Since(start)
	}()

	// While the close handshake is in flight, readers must not block.
	blocked := 0
	probeDeadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(probeDeadline) {
		done := make(chan struct{})
		go func() {
			_ = b.Status()
			_ = b.Conn()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			blocked++
			t.Fatalf("Status()/Conn() blocked > 300ms while Close() was closing the connection (b.mu held across the close handshake)")
		}
		select {
		case took := <-closed:
			if took > 2*time.Second {
				t.Fatalf("Close() took %v against a silent peer, want <= 2s", took)
			}
			t.Logf("Close() returned in %v with %d reader probes", took, blocked)
			if got := b.Status(); got != StatusDisconnected {
				t.Fatalf("Status() after Close = %q, want %q", got, StatusDisconnected)
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
	}

	select {
	case took := <-closed:
		if took > 2*time.Second {
			t.Fatalf("Close() took %v against a silent peer, want <= 2s", took)
		}
		t.Logf("Close() returned in %v", took)
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3s against a silent peer")
	}
	assertNoGoroutineLeak(t, baseline, "silent peer Close")
}

// ─────────────────────────────────────────────────────────────
// defect 3: Connect/Close concurrently (-race clean, no post-Close reconnect)
// ─────────────────────────────────────────────────────────────

func TestConnectCloseConcurrent(t *testing.T) {
	url, _ := wsReadServer(t)

	for i := 0; i < 20; i++ {
		b := &BaseWebSocket{}
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = b.Connect(ctx, url, quietOpts())
		}()
		go func() {
			defer wg.Done()
			b.Close()
		}()

		done := make(chan struct{})
		go func() { defer close(done); wg.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Connect/Close deadlocked", i)
		}

		// Whatever won, the object must end up closed and quiet.
		closeWithin(t, b, 2*time.Second)
		cancel()
	}
}

// ─────────────────────────────────────────────────────────────
// leak proofs, one per lifecycle path
// ─────────────────────────────────────────────────────────────

func TestLeakConnectThenClose(t *testing.T) {
	url, accepts := wsReadServer(t)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.Connect(ctx, url, quietOpts()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	waitForAccepts(t, accepts, 1)

	closeWithin(t, b, 2*time.Second)
	if got := b.Status(); got != StatusDisconnected {
		t.Fatalf("Status() after Close = %q, want disconnected", got)
	}
	assertNoGoroutineLeak(t, baseline, "connect then Close")
}

func TestLeakPeerDropReconnectThenClose(t *testing.T) {
	url, accepts := wsDropServer(t, 20*time.Millisecond)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.MinStableConnectionMs = 5 // the 20ms connections count as stable
	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(accepts) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(accepts); got < 2 {
		t.Fatalf("accepts = %d, want >= 2 (auto reconnect did not happen)", got)
	}

	closeWithin(t, b, 3*time.Second)
	assertNoGoroutineLeak(t, baseline, "peer drop, auto reconnect, Close")
}

func TestLeakContextCancelled(t *testing.T) {
	url, _ := wsReadServer(t)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.Connect(ctx, url, quietOpts()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cancel() // the caller's ctx, as a supervisor would on shutdown

	deadline := time.Now().Add(3 * time.Second)
	for b.Status() == StatusConnected && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := b.Status(); got == StatusConnected {
		t.Fatalf("Status() = %q after the Connect ctx was cancelled, want disconnected", got)
	}

	closeWithin(t, b, 2*time.Second)
	assertNoGoroutineLeak(t, baseline, "Connect ctx cancelled then Close")
}

func TestLeakNoReconnectDropThenClose(t *testing.T) {
	url, accepts := wsDropServer(t, 10*time.Millisecond)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.NoReconnect = true
	disc := make(chan error, 4)
	b.OnDisconnect = func(err error) { disc <- err }

	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	var readErr error
	select {
	case readErr = <-disc:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect did not fire after the peer dropped the connection")
	}

	// No redial: the accept count must stay at 1 and the status stay
	// disconnected for well over the (50ms) reconnect interval.
	time.Sleep(400 * time.Millisecond)
	if got := atomic.LoadInt32(accepts); got != 1 {
		t.Fatalf("accepts = %d after drop with NoReconnect, want 1 (the loop must not redial)", got)
	}
	if got := b.Status(); got != StatusDisconnected {
		t.Fatalf("Status() = %q with NoReconnect after a drop, want disconnected", got)
	}
	// OnDisconnect fired exactly once.
	select {
	case extra := <-disc:
		t.Fatalf("OnDisconnect fired again after NoReconnect drop (err=%v, first=%v)", extra, readErr)
	case <-time.After(150 * time.Millisecond):
	}

	closeWithin(t, b, 2*time.Second)
	assertNoGoroutineLeak(t, baseline, "NoReconnect drop then Close")
}

func TestLeakDialGateRefusing(t *testing.T) {
	baseline := goroutineBaseline()

	var calls int32
	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.ReconnectInterval = 10 // fast; the gate denial must still back off
	opts.ReconnectMaxInterval = 40
	opts.DialGate = func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return errTestGateDenied
	}

	if err := b.Connect(ctx, refusedAddr(t), opts); err == nil {
		t.Fatal("Connect() with a denying DialGate returned nil error")
	}
	if got := b.Status(); got != StatusDisconnected {
		t.Fatalf("Status() = %q after a denied dial, want disconnected", got)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("DialGate calls = %d for a failed initial dial, want 1", got)
	}

	closeWithin(t, b, 2*time.Second)
	assertNoGoroutineLeak(t, baseline, "DialGate refusing then Close")
}

func TestLeakCloseTwice(t *testing.T) {
	url, _ := wsReadServer(t)
	baseline := goroutineBaseline()

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.Connect(ctx, url, quietOpts()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	closeWithin(t, b, 2*time.Second)
	closeWithin(t, b, 500*time.Millisecond) // second Close must be a no-op
	assertNoGoroutineLeak(t, baseline, "Close twice")

	// And Close on a never-connected object must be a no-op too.
	(&BaseWebSocket{}).Close()
}

func TestLeakCloseRacingConnect(t *testing.T) {
	url, _, stopServer := newWSReadServer(t)
	baseline := goroutineBaseline()

	for i := 0; i < 10; i++ {
		b := &BaseWebSocket{}
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = b.Connect(ctx, url, quietOpts())
		}()
		go func() {
			defer wg.Done()
			b.Close()
		}()
		done := make(chan struct{})
		go func() { defer close(done); wg.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Connect/Close deadlocked", i)
		}
		closeWithin(t, b, 3*time.Second)
		cancel()
	}
	// A Close that lands mid-dial can leave the server holding a TCP
	// connection it never got a request on. Those goroutines belong to the
	// test server, not to BaseWebSocket: stop it before counting.
	stopServer()
	assertNoGoroutineLeak(t, baseline, "Close racing Connect")
}
