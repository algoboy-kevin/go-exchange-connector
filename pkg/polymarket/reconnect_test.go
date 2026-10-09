package polymarket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
	coderws "github.com/coder/websocket"
)

// TestReconnectMarketNilSafe proves the primitive is a no-op before Start and
// when no market WS exists, rather than a nil dereference. A watchdog may call
// it during startup, when the market manager is not built yet.
func TestReconnectMarketNilSafe(t *testing.T) {
	// No market WS at all.
	New(false, Config{}, nil).ReconnectMarket()

	// Market WS constructed but never started: Disconnect must not panic.
	pc := New(false, Config{}, nil)
	pc.market = NewWSPolymarketMarket(pc.Connector, pc.cfg)
	pc.ReconnectMarket()

	if got := pc.market.Status(); got != ws.StatusDisconnected {
		t.Errorf("status after ReconnectMarket on an unstarted market = %q, want %q", got, ws.StatusDisconnected)
	}
}

// TestReconnectMarketRedialsAndResendsHandshake proves the primitive recovers
// the market subscription: it drops the live socket, the WS re-dials, and the
// new connection's handshake carries every subscribed asset id (the market
// channel honours the handshake form on connect as a full replacement set).
//
// The handshake is what makes the recovery useful — Disconnect() alone would
// leave the reconnected socket with no subscriptions.
func TestReconnectMarketRedialsAndResendsHandshake(t *testing.T) {
	var mu sync.Mutex
	var conns int
	var frames [][]byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns++
		mu.Unlock()
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			frames = append(frames, append([]byte(nil), data...))
			mu.Unlock()
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc := New(false, Config{MarketWSURL: url, ReconnectIntervalMs: 30}, nil)
	pc.market = NewWSPolymarketMarket(pc.Connector, pc.cfg)
	if err := pc.market.Start(ctx, url, 30); err != nil {
		t.Fatalf("market Start: %v", err)
	}
	t.Cleanup(pc.market.Stop)

	want := []string{"asset-a", "asset-b", "asset-c"}
	pc.Subscribe(want)

	// The initial subscribe lands on connection #1 as an incremental op frame,
	// proving the socket is live before we force it down.
	waitFor(t, 5*time.Second, func() bool {
		n, fs := snapshot(&mu, &conns, &frames)
		return n >= 1 && len(fs) >= 1
	})

	connsBefore, _ := snapshot(&mu, &conns, &frames)
	pc.ReconnectMarket()

	waitFor(t, 5*time.Second, func() bool {
		n, fs := snapshot(&mu, &conns, &frames)
		return n > connsBefore && handshakeCarriesAll(fs, want)
	})

	// Sanity: the handshake really did carry ALL three asset ids, not a subset.
	_, fs := snapshot(&mu, &conns, &frames)
	if !handshakeCarriesAll(fs, want) {
		t.Fatalf("no reconnected handshake carried %v; frames=%s", want, describeFrames(fs))
	}
}

// ── helpers ─────────────────────────────────────────────────

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func snapshot(mu *sync.Mutex, conns *int, frames *[][]byte) (int, [][]byte) {
	mu.Lock()
	defer mu.Unlock()
	fs := make([][]byte, len(*frames))
	copy(fs, *frames)
	return *conns, fs
}

// handshakeCarriesAll reports whether any frame is a market handshake
// (type=market) whose assets_ids is a superset of want.
func handshakeCarriesAll(frames [][]byte, want []string) bool {
	for _, f := range frames {
		var m struct {
			Type     string   `json:"type"`
			AssetIDs []string `json:"assets_ids"`
		}
		if json.Unmarshal(f, &m) != nil {
			continue
		}
		if m.Type != "market" {
			continue
		}
		if containsAll(m.AssetIDs, want) {
			return true
		}
	}
	return false
}

func containsAll(have, want []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, h := range have {
		set[h] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			return false
		}
	}
	return true
}

func describeFrames(frames [][]byte) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(string(f))
		b.WriteByte('\n')
	}
	return b.String()
}
