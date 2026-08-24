package websocket

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

func TestNextBackoff(t *testing.T) {
	max := 30 * time.Second

	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"base doubles", 5 * time.Second, 10 * time.Second},
		{"second double", 10 * time.Second, 20 * time.Second},
		{"capped at max", 20 * time.Second, 30 * time.Second},
		{"stays at max", 30 * time.Second, 30 * time.Second},
	}
	for _, c := range cases {
		if got := nextBackoff(c.in, max); got != c.want {
			t.Errorf("%s: nextBackoff(%v) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Raw-TCP half-open fake server
//
// coder/websocket CANNOT simulate a peer that stops ponging: its Read call
// processes control frames internally even while blocked waiting for a data
// frame, so a server that keeps reading keeps auto-ponging the client's pings
// forever. We therefore implement a minimal raw WebSocket server that completes
// the handshake, optionally answers the first ping, then goes silent while
// keeping the TCP connection open — exactly the half-open death the pong
// watchdog guards against.
// ─────────────────────────────────────────────────────────────

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsAcceptKey(key string) string {
	h := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// readWSFrame reads one WebSocket frame from c, unmasking client frames.
// Returns the opcode and the unmasked payload.
func readWSFrame(c net.Conn) (opcode byte, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(c, hdr[:]); err != nil {
		return 0, nil, err
	}
	opcode = hdr[0] & 0x0f
	masked := hdr[1]&0x80 != 0
	length := uint64(hdr[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(ext[0])<<8 | uint64(ext[1])
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c, ext[:]); err != nil {
			return 0, nil, err
		}
		length = 0
		for _, b := range ext {
			length = length<<8 | uint64(b)
		}
	}
	var key [4]byte
	if masked {
		if _, err = io.ReadFull(c, key[:]); err != nil {
			return 0, nil, err
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(c, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return opcode, payload, nil
}

// writeWSFrame writes an unmasked (server→client) WebSocket frame.
func writeWSFrame(c net.Conn, opcode byte, payload []byte) error {
	header := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 0xffff:
		header = append(header, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		l := uint64(len(payload))
		for i := 0; i < 8; i++ {
			header[2+i] = byte(l >> (8 * (7 - i)))
		}
	}
	if _, err := c.Write(append(header, payload...)); err != nil {
		return err
	}
	return nil
}

// handleHalfOpenConn serves one raw WebSocket connection: handshake, optional
// single pong, then silence (reads to detect client close but never responds).
func handleHalfOpenConn(c net.Conn, answerFirstPing bool) {
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

	if answerFirstPing {
		for {
			opcode, payload, err := readWSFrame(c)
			if err != nil {
				return
			}
			if opcode == 0x9 { // ping
				_ = writeWSFrame(c, 0xA, payload) // pong
				break
			}
		}
	}

	// Go silent: keep the TCP connection open (so the client sees no RST/EOF)
	// but never respond to further pings or send any data.
	buf := make([]byte, 4096)
	for {
		if _, err := c.Read(buf); err != nil {
			return
		}
	}
}

// fakeHalfOpenWSServer listens for WebSocket connections, counting each one.
// Returns the ws:// URL and a closure returning the connection count.
func fakeHalfOpenWSServer(t *testing.T, answerFirstPing bool) (string, *func() int) {
	t.Helper()
	var mu sync.Mutex
	count := 0
	getCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return count
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			count++
			mu.Unlock()
			go func() {
				defer c.Close()
				handleHalfOpenConn(c, answerFirstPing)
			}()
		}
	}()

	return "ws://" + ln.Addr().String(), &getCount
}

func TestPongTimeoutForcesReconnect(t *testing.T) {
	// The peer answers the first ping, then goes silent (half-open): the
	// per-ping deadline must fire, Disconnect must be called, and the
	// reconnLoop must redial. DataStaleTimeout is set large and the staleness
	// clock starts at connect, so the staleness watchdog cannot fire first —
	// this isolates the pong-timeout path.
	url, getCount := fakeHalfOpenWSServer(t, true)

	ws := &BaseWebSocket{}
	disconnected := make(chan struct{}, 1)
	ws.OnDisconnect = func(err error) {
		select {
		case disconnected <- struct{}{}:
		default:
		}
	}

	opts := DefaultWSOptions()
	opts.PingInterval = 100
	opts.PongTimeout = 300
	opts.DataStaleTimeout = 60000 // keep the staleness watchdog out of the picture
	opts.ReconnectInterval = 100
	opts.ReconnectMaxInterval = 200

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ws.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect did not fire within 5s — pong watchdog did not engage")
	}

	// The reconnLoop must redial after the forced disconnect.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if (*getCount)() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no reconnection observed; connectionCount=%d, want >= 2", (*getCount)())
}

func TestDataStaleTimeoutForcesReconnect(t *testing.T) {
	// The peer sends one data frame, then keeps the socket alive (auto-pongs
	// every ping) but never sends data again — the data-staleness watchdog
	// must force the reconnect while the transport still looks healthy.
	var mu sync.Mutex
	var count int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		count++
		mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = c.Write(ctx, coderws.MessageText, []byte("hello"))
		// Keep reading (auto-pong) but never write another data frame.
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	getCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return count
	}

	ws := &BaseWebSocket{}
	disconnected := make(chan struct{}, 1)
	ws.OnDisconnect = func(err error) {
		select {
		case disconnected <- struct{}{}:
		default:
		}
	}

	opts := DefaultWSOptions()
	opts.PingInterval = 100
	opts.PongTimeout = 500
	opts.DataStaleTimeout = 250
	opts.ReconnectInterval = 100
	opts.ReconnectMaxInterval = 200

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ws.Connect(ctx, url, opts); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnDisconnect did not fire within 5s — data-staleness watchdog did not engage")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if getCount() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no reconnection observed; connectionCount=%d, want >= 2", getCount())
}
