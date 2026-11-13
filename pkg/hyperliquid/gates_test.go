package hyperliquid

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

var errTestGate = errors.New("test gate refused")

// newGatedHL builds a manager against url with keepalives off, so the venue sees only what a test
// provokes. Stop runs at cleanup.
func newGatedHL(t *testing.T, url string, o Options) *WSHyperliquid {
	t.Helper()
	o.URL = url
	o.ReconnectIntervalMs = 20
	o.AppPingIntervalMs = -1
	o.WSPingIntervalMs = -1
	h := NewWithOptions(connector.New(false, nil), o)
	t.Cleanup(h.Stop)
	return h
}

// DialGate reaches the websocket layer: it is consulted before the first dial, and a refusal comes
// back from Start wrapping the gate's own error with no connection made.
func TestOptionsDialGateIsConsultedAndRefusalSurfaces(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	var calls atomic.Int32
	h := newGatedHL(t, v.url, Options{DialGate: func(context.Context) error { calls.Add(1); return nil }})
	if err := h.Start(context.Background(), 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("DialGate calls after the first dial = %d, want 1", n)
	}

	v2 := newFakeVenue(t, nil, nil)
	h2 := newGatedHL(t, v2.url, Options{DialGate: func(context.Context) error { return errTestGate }})
	err := h2.Start(context.Background(), 0)
	if !errors.Is(err, errTestGate) {
		t.Fatalf("Start err = %v, want the gate's own error", err)
	}
	if n := v2.connCount(); n != 0 {
		t.Fatalf("venue saw %d connections although the gate refused the dial", n)
	}
}

// NoReconnect leaves the redial to the supervisor: after the venue drops the socket the manager
// stays disconnected and dials nothing more; without it the connector redials on its own.
func TestOptionsNoReconnectLeavesTheRedialToTheSupervisor(t *testing.T) {
	run := func(noReconnect bool) (int32, ws.ConnectionStatus) {
		var calls atomic.Int32
		v := newFakeVenue(t, nil, func(idx int, c *coderws.Conn) {
			if idx == 0 {
				go func() { time.Sleep(60 * time.Millisecond); _ = c.Close(coderws.StatusNormalClosure, "bye") }()
			}
		})
		h := newGatedHL(t, v.url, Options{
			NoReconnect: noReconnect,
			DialGate:    func(context.Context) error { calls.Add(1); return nil },
		})
		if err := h.Start(context.Background(), 0); err != nil {
			t.Fatalf("Start: %v", err)
		}
		// The connector never redials a manager with nothing subscribed (ShouldConnect is
		// len(subs) > 0), so subscribe in BOTH legs: without NoReconnect the drop below must
		// be followed by a redial, with it must not. That makes the NoReconnect leg discriminating.
		if err := h.Subscribe(context.Background(), Subscription{Type: ChannelTrades, Coin: "BTC"}); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		time.Sleep(600 * time.Millisecond)
		return calls.Load(), h.Status()
	}
	dials, status := run(true)
	if dials != 1 || status != ws.StatusDisconnected {
		t.Fatalf("NoReconnect: dials=%d status=%s, want exactly 1 dial and disconnected", dials, status)
	}
	if dials, _ = run(false); dials < 2 {
		t.Fatalf("default: dials=%d, want the connector to redial (>= 2)", dials)
	}
}
