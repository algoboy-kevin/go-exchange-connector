package websocket

// reconnect_latency_test.go MEASURES how long the reconnect loop takes to
// redial after an established connection drops, then pins the fix: the loop
// used to poll at ReconnectInterval, so a drop was redialled after a uniformly
// random 0..interval wait (up to a full second with the common 1s setting).
// The read loop now wakes the loop immediately when a connection that
// outlived MinStableConnectionMs drops; short-lived (flapping) connections
// keep the poll so their backoff is untouched.

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"
)

// latencyDropServer handshakes every connection, keeps it open for `live` and
// then drops the TCP socket WITHOUT a close frame — the shape of a venue
// killing a healthy socket (LB recycle, policy close). It records when each
// connection was accepted and when it was dropped.
func latencyDropServer(t *testing.T, live time.Duration) (string, func() (accepts, drops []time.Time)) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var mu sync.Mutex
	var accepts, drops []time.Time

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			accepts = append(accepts, time.Now())
			mu.Unlock()

			go func(c net.Conn) {
				defer c.Close()
				req, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				key := req.Header.Get("Sec-WebSocket-Key")
				if key == "" {
					return
				}
				resp := "HTTP/1.1 101 Switching Protocols\r\n" +
					"Upgrade: websocket\r\n" +
					"Connection: Upgrade\r\n" +
					"Sec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n\r\n"
				if _, err := c.Write([]byte(resp)); err != nil {
					return
				}
				time.Sleep(live)
				mu.Lock()
				drops = append(drops, time.Now())
				mu.Unlock()
			}(c)
		}
	}()

	snapshot := func() ([]time.Time, []time.Time) {
		mu.Lock()
		defer mu.Unlock()
		return append([]time.Time(nil), accepts...), append([]time.Time(nil), drops...)
	}
	return "ws://" + ln.Addr().String(), snapshot
}

func TestReconnectLatencyAfterStableDrop(t *testing.T) {
	const samples = 20
	const interval = time.Second

	url, snapshot := latencyDropServer(t, 30*time.Millisecond)

	b := &BaseWebSocket{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := quietOpts()
	opts.ReconnectInterval = int64(interval / time.Millisecond)
	opts.ReconnectMaxInterval = int64(interval / time.Millisecond)
	opts.MinStableConnectionMs = 5 // the 30ms connections are stable

	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer closeWithin(t, b, 3*time.Second)

	deadline := time.Now().Add(40 * time.Second)
	var accepts, drops []time.Time
	for time.Now().Before(deadline) {
		accepts, drops = snapshot()
		if len(accepts) > samples && len(drops) >= samples {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(accepts) <= samples || len(drops) < samples {
		t.Fatalf("only %d accepts / %d drops in 40s, want > %d (reconnect did not keep up)",
			len(accepts), len(drops), samples)
	}

	lat := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		lat = append(lat, accepts[i+1].Sub(drops[i]))
	}
	sorted := append([]time.Duration(nil), lat...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range lat {
		sum += d
	}
	mean := sum / time.Duration(len(lat))
	median := sorted[len(sorted)/2]
	t.Logf("reconnect latency after a stable drop (%d samples, ReconnectInterval=%v): min=%v median=%v mean=%v max=%v",
		len(lat), interval, sorted[0], median, mean, sorted[len(sorted)-1])

	// Measured before the fix: uniformly 0..1s, mean ~500ms, median ~500ms.
	if mean > 250*time.Millisecond {
		t.Fatalf("mean reconnect latency = %v with ReconnectInterval=%v: the loop is still polling instead of waking on the drop (want <= 250ms)", mean, interval)
	}
	if sorted[len(sorted)-1] > 600*time.Millisecond {
		t.Fatalf("max reconnect latency = %v with ReconnectInterval=%v, want <= 600ms", sorted[len(sorted)-1], interval)
	}
}
