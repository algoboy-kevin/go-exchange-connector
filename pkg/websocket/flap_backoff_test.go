package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

func TestNextDelayAfterDrop(t *testing.T) {
	base := 500 * time.Millisecond
	max := 30 * time.Second
	stable := 5 * time.Second

	cases := []struct {
		name        string
		current     time.Duration
		lived       time.Duration
		want        time.Duration
		wantFlapped bool
	}{
		{"stable connection resets to base", 4 * time.Second, 30 * time.Second, base, false},
		{"exactly at the window counts as stable", 4 * time.Second, stable, base, false},
		{"flap doubles the current delay", base, 10 * time.Millisecond, time.Second, true},
		{"flap keeps compounding", 8 * time.Second, 10 * time.Millisecond, 16 * time.Second, true},
		{"flap caps at max", 20 * time.Second, 0, max, true},
	}
	for _, c := range cases {
		got, flapped := nextDelayAfterDrop(c.current, base, max, stable, c.lived)
		if got != c.want || flapped != c.wantFlapped {
			t.Errorf("%s: nextDelayAfterDrop(%v, lived=%v) = (%v, %v), want (%v, %v)",
				c.name, c.current, c.lived, got, flapped, c.want, c.wantFlapped)
		}
	}

	// A non-positive window disables the check: every successful dial resets
	// the backoff, i.e. the pre-0.7.2 behaviour.
	if got, flapped := nextDelayAfterDrop(8*time.Second, base, max, -1, 0); got != base || flapped {
		t.Errorf("minStable disabled: got (%v, %v), want (%v, false)", got, flapped, base)
	}
}

// fakeRejectingWSServer accepts the WebSocket handshake and then immediately
// closes the connection with a reason — the shape of a venue refusing us (per-IP
// connection cap, refused subscription) rather than dropping a healthy socket.
// Every accepted connection is counted.
func fakeRejectingWSServer(t *testing.T) (string, func() int) {
	t.Helper()
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		count++
		mu.Unlock()
		_ = c.Close(coderws.StatusPolicyViolation, "Cannot open more than 15 connections.")
	}))
	t.Cleanup(srv.Close)

	return "ws" + strings.TrimPrefix(srv.URL, "http"), func() int {
		mu.Lock()
		defer mu.Unlock()
		return count
	}
}

func TestFlappingConnectionBacksOff(t *testing.T) {
	url, connections := fakeRejectingWSServer(t)

	ws := &BaseWebSocket{}
	opts := DefaultWSOptions()
	opts.ReconnectInterval = 50
	opts.ReconnectMaxInterval = 400
	opts.MinStableConnectionMs = 200 // every connection here dies far sooner
	opts.PingInterval = 0            // no keepalive: the server closes at once
	opts.DisableDataStaleWatchdog = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ws.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer ws.Close()

	time.Sleep(1600 * time.Millisecond)

	// The old behaviour reset the delay to 50ms after every accepted-but-closed
	// connection, yielding ~32 dials in 1.6s. Flap-aware backoff paces them
	// 50,100,200,400,400… so the count stays in single digits.
	got := connections()
	if got > 12 {
		t.Fatalf("connections = %d in 1.6s (want <= 12): the backoff is being reset by a connection that is accepted and then closed", got)
	}
	if got < 2 {
		t.Fatalf("connections = %d in 1.6s (want >= 2): the reconnect loop did not redial", got)
	}
}
