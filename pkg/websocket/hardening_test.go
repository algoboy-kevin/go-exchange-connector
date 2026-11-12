package websocket

// hardening_test.go covers the optional control hooks added in H1:
// DialGate (before every dial), SendGate (before every frame this package
// writes) and NoReconnect. All three are opt-in: nil/false keeps the
// historical behaviour, which the pre-existing tests already pin.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// errTestGateDenied is the sentinel a test gate returns; errors.Is must see it
// through the wrapping the package adds.
var errTestGateDenied = errors.New("test gate: budget exhausted")

// wsRecordingServer accepts one connection at a time, records every text
// message the client sends and answers the close handshake. The returned
// closure is the only safe way to read the recording (the handler goroutine
// writes it concurrently).
func wsRecordingServer(t *testing.T) (string, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var msgs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			msgs = append(msgs, string(data))
			mu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	recorded := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), msgs...)
	}
	return "ws" + strings.TrimPrefix(srv.URL, "http"), recorded
}

// ─────────────────────────────────────────────────────────────
// DialGate
// ─────────────────────────────────────────────────────────────

func TestDialGateDeniesInitialDial(t *testing.T) {
	url, accepts := wsReadServer(t)

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.DialGate = func(context.Context) error { return errTestGateDenied }

	err := b.Connect(ctx, url, opts)
	if err == nil {
		t.Fatal("Connect() succeeded despite a denying DialGate")
	}
	if !errors.Is(err, errTestGateDenied) {
		t.Fatalf("Connect() error = %v, want errors.Is(err, errTestGateDenied)", err)
	}
	if got := atomic.LoadInt32(accepts); got != 0 {
		t.Fatalf("server accepts = %d, want 0 (the gate must run before the dial)", got)
	}
	if got := b.Status(); got != StatusDisconnected {
		t.Fatalf("Status() = %q, want disconnected", got)
	}
	closeWithin(t, b, 500*time.Millisecond)
}

func TestDialGateDeniesReconnectWithoutTightLoop(t *testing.T) {
	url, _ := wsDropServer(t, 10*time.Millisecond)

	var calls int32
	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.ReconnectInterval = 10
	opts.ReconnectMaxInterval = 40
	opts.DialGate = func(ctx context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Allow the initial dial, deny every redial.
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil
		}
		return errTestGateDenied
	}

	disc := make(chan struct{}, 1)
	b.OnDisconnect = func(error) {
		select {
		case disc <- struct{}{}:
		default:
		}
	}
	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-disc:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect did not fire after the peer dropped the connection")
	}

	time.Sleep(500 * time.Millisecond)
	got := atomic.LoadInt32(&calls)
	// 1 initial + denials paced 10,20,40,40,40… ≈ 13 in 500ms. A denial that
	// was treated as "retry now" would call the gate thousands of times.
	if got > 16 {
		t.Fatalf("DialGate called %d times in 500ms after a denial: the denial did not escalate the backoff (tight loop)", got)
	}
	if got < 3 {
		t.Fatalf("DialGate called %d times in 500ms: the reconnect loop stopped retrying after a denial", got)
	}
	if st := b.Status(); st != StatusDisconnected {
		t.Fatalf("Status() = %q after a denied redial, want disconnected", st)
	}
	closeWithin(t, b, 2*time.Second)
}

func TestDialGateBlockingRespectsClose(t *testing.T) {
	url, accepts := wsReadServer(t)

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gateEntered := make(chan struct{})
	opts := quietOpts()
	opts.DialGate = func(ctx context.Context) error {
		close(gateEntered)
		<-ctx.Done() // a rate limiter waiting for a token, never granted
		return ctx.Err()
	}

	connectDone := make(chan error, 1)
	go func() { connectDone <- b.Connect(ctx, url, opts) }()

	select {
	case <-gateEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("DialGate was never consulted")
	}

	closed := make(chan struct{})
	go func() { b.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked while a DialGate call was in progress")
	}
	select {
	case err := <-connectDone:
		if err == nil {
			t.Fatal("Connect() returned nil after Close() cancelled the dial gate")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect() did not return after Close() cancelled the dial gate")
	}
	if got := atomic.LoadInt32(accepts); got != 0 {
		t.Fatalf("server accepts = %d, want 0", got)
	}
}

// ─────────────────────────────────────────────────────────────
// SendGate
// ─────────────────────────────────────────────────────────────

func TestSendGateDeniesPingWithoutKillingConnection(t *testing.T) {
	url, _ := wsReadServer(t) // the peer reads, so it auto-pongs: connection healthy

	var gateCalls int32
	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.PingInterval = 40
	opts.PongTimeout = 2000
	opts.SendGate = func(context.Context, int) error {
		atomic.AddInt32(&gateCalls, 1)
		return errTestGateDenied
	}
	gateErrs := make(chan error, 16)
	b.OnError = func(err error) {
		select {
		case gateErrs <- err:
		default:
		}
	}

	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case err := <-gateErrs:
		if !errors.Is(err, errTestGateDenied) {
			t.Fatalf("OnError = %v, want the gate error (errors.Is)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnError did not report the denied ping")
	}
	if got := atomic.LoadInt32(&gateCalls); got < 1 {
		t.Fatalf("SendGate calls = %d, want >= 1 (pings must consult the gate)", got)
	}

	// A denied ping must not drop the connection.
	time.Sleep(250 * time.Millisecond)
	if got := b.Status(); got != StatusConnected {
		t.Fatalf("Status() = %q after denied pings, want connected", got)
	}
	closeWithin(t, b, 2*time.Second)
}

func TestSendGateDeniesSendHelpers(t *testing.T) {
	url, msgs := wsRecordingServer(t)

	var deny atomic.Bool
	deny.Store(true)

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.SendGate = func(_ context.Context, n int) error {
		if deny.Load() {
			_ = n
			return errTestGateDenied
		}
		return nil
	}
	reported := make(chan error, 8)
	b.OnError = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}

	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	err := b.SendText(ctx, "denied-frame")
	if err == nil {
		t.Fatal("SendText() returned nil despite a denying SendGate")
	}
	if !errors.Is(err, errTestGateDenied) {
		t.Fatalf("SendText() error = %v, want errors.Is(err, errTestGateDenied)", err)
	}
	select {
	case e := <-reported:
		if !errors.Is(e, errTestGateDenied) {
			t.Fatalf("OnError = %v, want the gate error", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnError did not report the denied SendText")
	}
	if got := b.Status(); got != StatusConnected {
		t.Fatalf("Status() = %q after a denied send, want connected", got)
	}

	// Allowed: the frame goes out.
	deny.Store(false)
	if err := b.SendJSON(ctx, map[string]any{"ok": true}); err != nil {
		t.Fatalf("SendJSON with an allowing gate: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(msgs()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := msgs()
	if len(got) != 1 || !strings.Contains(got[0], `"ok":true`) {
		t.Fatalf("server received %v, want exactly the allowed JSON frame (the denied frame must never be written)", got)
	}
	closeWithin(t, b, 2*time.Second)
}

// ─────────────────────────────────────────────────────────────
// NoReconnect
// ─────────────────────────────────────────────────────────────

func TestNoReconnectLeavesStatusDisconnected(t *testing.T) {
	url, accepts := wsDropServer(t, 10*time.Millisecond)

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.NoReconnect = true
	errs := make(chan error, 2)
	b.OnDisconnect = func(err error) { errs <- err }

	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	var got error
	select {
	case got = <-errs:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect did not fire")
	}
	if got == nil {
		t.Fatal("OnDisconnect err = nil for a peer drop, want the read error")
	}
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(accepts); n != 1 {
		t.Fatalf("accepts = %d with NoReconnect, want 1", n)
	}
	if st := b.Status(); st != StatusDisconnected {
		t.Fatalf("Status() = %q with NoReconnect, want disconnected", st)
	}
	closeWithin(t, b, 2*time.Second)
}
