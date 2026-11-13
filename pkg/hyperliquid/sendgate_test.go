package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

// gateVenue is a loopback venue that records every client frame by method (pings included), the
// bytes of every frame, and which subscriptions each accepted connection carried.
type gateVenue struct {
	url string

	mu     sync.Mutex
	conns  []*coderws.Conn
	counts map[string]int
	bytes  int
	subsOn map[int]map[string]bool
}

func newGateVenue(t *testing.T) *gateVenue {
	t.Helper()
	v := &gateVenue{counts: map[string]int{}, subsOn: map[int]map[string]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		idx := len(v.conns)
		v.conns = append(v.conns, c)
		v.subsOn[idx] = map[string]bool{}
		v.mu.Unlock()
		ctx := context.Background()
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var req struct {
				Method       string       `json:"method"`
				Subscription Subscription `json:"subscription"`
			}
			if json.Unmarshal(data, &req) != nil {
				continue
			}
			v.mu.Lock()
			v.counts[req.Method]++
			v.bytes += len(data)
			if req.Method == "subscribe" {
				v.subsOn[idx][req.Subscription.Key()] = true
			}
			v.mu.Unlock()
			if req.Method == "subscribe" {
				_ = c.Write(ctx, coderws.MessageText, []byte(venueAck(req.Subscription)))
			}
		}
	}))
	t.Cleanup(srv.Close)
	v.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return v
}

func (v *gateVenue) count(method string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.counts[method]
}

func (v *gateVenue) frameBytes() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.bytes
}

func (v *gateVenue) connCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

func (v *gateVenue) carries(idx int, keys ...string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	m := v.subsOn[idx]
	if m == nil {
		return false
	}
	for _, k := range keys {
		if !m[k] {
			return false
		}
	}
	return true
}

// laterConnCarries reports whether some connection after the first carries every key.
func (v *gateVenue) laterConnCarries(keys ...string) bool {
	for i := 1; i < v.connCount(); i++ {
		if v.carries(i, keys...) {
			return true
		}
	}
	return false
}

func (v *gateVenue) drop(idx int) {
	v.mu.Lock()
	c := v.conns[idx]
	v.mu.Unlock()
	_ = c.Close(coderws.StatusNormalClosure, "drop")
}

// gateRec is a SendGate that records every call and can refuse on demand.
type gateRec struct {
	calls     atomic.Int32
	bytes     atomic.Int64
	deny      atomic.Int32 // number of upcoming non-ping calls to refuse
	denyPings atomic.Bool
}

func (g *gateRec) gate(_ context.Context, n int) error {
	g.calls.Add(1)
	g.bytes.Add(int64(n))
	if n == len(PingFrame()) {
		if g.denyPings.Load() {
			return errTestGate
		}
		return nil
	}
	for {
		d := g.deny.Load()
		if d <= 0 {
			return nil
		}
		if g.deny.CompareAndSwap(d, d-1) {
			return errTestGate
		}
	}
}

func trades(coin string) Subscription { return Subscription{Type: ChannelTrades, Coin: coin} }

// Exact accounting: every subscribe, unsubscribe and replay frame passes the gate exactly once,
// with the right byte count, on the unpaced and the paced path. 3 subscribes + 1 unsubscribe +
// a 2-frame reconnect replay = 6 frames.
func TestSendGateCoversEverySubscribeAndUnsubscribeFrame(t *testing.T) {
	for _, pace := range []int64{0, 10} {
		t.Run(fmt.Sprintf("pace%d", pace), func(t *testing.T) {
			v := newGateVenue(t)
			g := &gateRec{}
			h := newGatedHL(t, v.url, Options{SendGate: g.gate, SubscribePaceMs: pace})
			ctx := context.Background()
			if err := h.Start(ctx, 0); err != nil {
				t.Fatalf("Start: %v", err)
			}
			btc, eth, sol := trades("BTC"), trades("ETH"), trades("SOL")
			if err := h.Subscribe(ctx, btc, eth, sol); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			waitFor(t, 5*time.Second, "3 subscribe frames", func() bool { return v.count("subscribe") == 3 })
			if err := h.Unsubscribe(ctx, sol); err != nil {
				t.Fatalf("Unsubscribe: %v", err)
			}
			waitFor(t, 5*time.Second, "1 unsubscribe frame", func() bool { return v.count("unsubscribe") == 1 })
			v.drop(0)
			waitFor(t, 5*time.Second, "the replay of the 2 remaining subscriptions", func() bool {
				return v.carries(1, btc.Key(), eth.Key())
			})
			time.Sleep(100 * time.Millisecond) // quiescent: nothing is left to write

			const want = 3 + 1 + 2
			if got := int(g.calls.Load()); got != want {
				t.Fatalf("gate calls = %d, want %d (every frame exactly once)", got, want)
			}
			if got := v.count("subscribe") + v.count("unsubscribe"); got != want {
				t.Fatalf("venue received %d frames, want %d", got, want)
			}
			if g.bytes.Load() != int64(v.frameBytes()) {
				t.Fatalf("gate saw %d bytes, venue received %d", g.bytes.Load(), v.frameBytes())
			}
		})
	}
}

// A refused write during the reconnect replay must fail the connection so the next connect
// replays everything: no subscription is silently lost because of a gate.
func TestSendGateErrorOnReplayFailsTheConnectionAndLosesNoSubscription(t *testing.T) {
	for _, pace := range []int64{0, 10} {
		t.Run(fmt.Sprintf("pace%d", pace), func(t *testing.T) {
			v := newGateVenue(t)
			g := &gateRec{}
			h := newGatedHL(t, v.url, Options{SendGate: g.gate, SubscribePaceMs: pace})
			ctx := context.Background()
			if err := h.Start(ctx, 0); err != nil {
				t.Fatalf("Start: %v", err)
			}
			subs := []Subscription{trades("BTC"), trades("ETH"), trades("SOL")}
			keys := []string{subs[0].Key(), subs[1].Key(), subs[2].Key()}
			if err := h.Subscribe(ctx, subs...); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			waitFor(t, 5*time.Second, "3 subscribe frames", func() bool { return v.count("subscribe") == 3 })

			g.deny.Store(1) // the first write of the reconnect replay is refused
			v.drop(0)
			waitFor(t, 10*time.Second, "a later connection carrying all 3 subscriptions", func() bool {
				return v.laterConnCarries(keys...)
			})
			if n := v.connCount(); n < 3 {
				t.Fatalf("venue saw %d connections, want >= 3 (the drop, the gate-failed replay, the good replay)", n)
			}
		})
	}
}

// A refused write of a live Subscribe fails the connection too, and the reconnect replay carries
// the subscription that never left.
func TestSendGateErrorOnLiveSubscribeFailsTheConnectionAndTheReplayCarriesIt(t *testing.T) {
	v := newGateVenue(t)
	g := &gateRec{}
	h := newGatedHL(t, v.url, Options{SendGate: g.gate})
	ctx := context.Background()
	if err := h.Start(ctx, 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	btc, eth := trades("BTC"), trades("ETH")
	if err := h.Subscribe(ctx, btc); err != nil {
		t.Fatalf("Subscribe BTC: %v", err)
	}
	waitFor(t, 5*time.Second, "the BTC subscribe frame", func() bool { return v.count("subscribe") == 1 })

	g.deny.Store(1)
	if err := h.Subscribe(ctx, eth); err != nil {
		t.Fatalf("Subscribe ETH: %v (registration succeeds; the gated write fails the connection)", err)
	}
	waitFor(t, 10*time.Second, "a later connection carrying BTC and ETH", func() bool {
		return v.laterConnCarries(btc.Key(), eth.Key())
	})
}

// A refused application ping skips that tick and never closes the connection; once the gate
// allows pings again they reach the venue.
func TestSendGateErrorOnApplicationPingSkipsTheTick(t *testing.T) {
	v := newGateVenue(t)
	g := &gateRec{}
	g.denyPings.Store(true)
	h := newGatedHL(t, v.url, Options{SendGate: g.gate, AppPingIntervalMs: 30})
	ctx := context.Background()
	if err := h.Start(ctx, 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := h.Subscribe(ctx, trades("BTC")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitFor(t, 5*time.Second, "the subscribe frame", func() bool { return v.count("subscribe") == 1 })
	time.Sleep(400 * time.Millisecond)
	if n := v.count("ping"); n != 0 {
		t.Fatalf("venue received %d pings although the gate refused them all", n)
	}
	if n := g.calls.Load(); n < 4 {
		t.Fatalf("gate calls = %d, want >= 4 (1 subscribe + several refused pings)", n)
	}
	if v.connCount() != 1 || h.Status() != ws.StatusConnected {
		t.Fatalf("connections=%d status=%s: a refused ping must not close the connection", v.connCount(), h.Status())
	}
	g.denyPings.Store(false)
	waitFor(t, 2*time.Second, "pings to flow once the gate allows them", func() bool { return v.count("ping") >= 1 })
}
