package polymarket

// logger_test.go covers H5's optional instance logger for the three Polymarket
// streams (market, RTDS, user). Each stream exposes SetLogger — the same shape
// as the existing Set* setters (SetEventQueueSize, SetReadLimitBytes,
// SetDispatcherWorkers) — and the embedded BaseWebSocket inherits it through the
// wsOptions each Start builds.
//
// Every injected-logger test also requires the line to be ABSENT from the
// swapped default, which is what fails without the change.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// pmLogBuffer is a mutex-guarded sink: stream goroutines log while the test
// reads.
type pmLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *pmLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *pmLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func pmInstanceLogger(level slog.Level) (*slog.Logger, *pmLogBuffer) {
	buf := &pmLogBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// pmSwapDefault replaces the process default logger for the duration of the
// test and returns its buffer; cleanup restores the previous default.
func pmSwapDefault(t *testing.T, level slog.Level) (*slog.Logger, *pmLogBuffer) {
	t.Helper()
	l, buf := pmInstanceLogger(level)
	old := slog.Default()
	slog.SetDefault(l)
	t.Cleanup(func() { slog.SetDefault(old) })
	return l, buf
}

// pmWaitForMsg polls buf until it holds the given JSON "msg" value or the
// deadline passes; stream lines are written by connection goroutines.
func pmWaitForMsg(t *testing.T, buf *pmLogBuffer, msg string) bool {
	t.Helper()
	want := `"msg":"` + msg + `"`
	deadline := time.Now().Add(3 * time.Second)
	for {
		if strings.Contains(buf.String(), want) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMarketWSInjectedLogger is case (a) for the market channel plus the
// websocket wiring: connect (BaseWebSocket line) and disconnect (stream line)
// both go to the injected logger, neither to the swapped default.
func TestMarketWSInjectedLogger(t *testing.T) {
	v := newRTDSVenue(t) // plain loopback WS server: accepts and reads
	_, defBuf := pmSwapDefault(t, slog.LevelDebug)
	inj, injBuf := pmInstanceLogger(slog.LevelDebug)

	pm := NewWSPolymarketMarket(connector.New(false, nil), Config{})
	pm.SetLogger(inj)
	t.Cleanup(pm.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := pm.Start(ctx, v.url, 20); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !pmWaitForMsg(t, injBuf, "websocket: connected") {
		t.Fatalf("websocket layer did not use the stream logger; got %q", injBuf.String())
	}

	// Drop the server side: onDisconnect logs the stream-level line.
	if c := v.conn(0); c != nil {
		_ = c.CloseNow()
	}
	if !pmWaitForMsg(t, injBuf, "market WS: disconnected") {
		t.Errorf("injected logger did not receive the disconnect line; got %q", injBuf.String())
	}
	if def := defBuf.String(); strings.Contains(def, "market WS:") || strings.Contains(def, "websocket:") {
		t.Errorf("injected-logger stream also logged to the default logger: %q", def)
	}
}

// TestRTDSInjectedLogger is case (a) for RTDS.
func TestRTDSInjectedLogger(t *testing.T) {
	v := newRTDSVenue(t)
	_, defBuf := pmSwapDefault(t, slog.LevelDebug)
	inj, injBuf := pmInstanceLogger(slog.LevelDebug)

	r := NewWSPolymarketRTDS(connector.New(false, nil))
	r.SetLogger(inj)
	t.Cleanup(r.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := r.Start(ctx, v.url, 20); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !pmWaitForMsg(t, injBuf, "rtds: connected") {
		t.Errorf("injected logger did not receive the RTDS connect line; got %q", injBuf.String())
	}
	if got := injBuf.String(); !strings.Contains(got, `"msg":"websocket: connected"`) {
		t.Errorf("websocket layer did not use the stream logger; got %q", got)
	}
	if def := defBuf.String(); strings.Contains(def, "rtds:") || strings.Contains(def, "websocket:") {
		t.Errorf("injected-logger stream also logged to the default logger: %q", def)
	}
}

// TestUserWSInjectedLogger is case (a) for the user channel (its disconnect line
// is Debug, so the handler is at Debug level).
func TestUserWSInjectedLogger(t *testing.T) {
	v := newRTDSVenue(t)
	_, defBuf := pmSwapDefault(t, slog.LevelDebug)
	inj, injBuf := pmInstanceLogger(slog.LevelDebug)

	u := NewWSPolymarketUserWS(connector.New(false, nil), UserAuth{APIKey: "k", Secret: "s", Passphrase: "p"}, UserHandlers{})
	u.SetLogger(inj)
	t.Cleanup(u.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := u.Start(ctx, v.url); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !pmWaitForMsg(t, injBuf, "websocket: connected") {
		t.Fatalf("websocket layer did not use the stream logger; got %q", injBuf.String())
	}
	if c := v.conn(0); c != nil {
		_ = c.CloseNow()
	}
	if !pmWaitForMsg(t, injBuf, "user WS: disconnected") {
		t.Errorf("injected logger did not receive the user disconnect line; got %q", injBuf.String())
	}
	if def := defBuf.String(); strings.Contains(def, "user WS:") || strings.Contains(def, "websocket:") {
		t.Errorf("injected-logger stream also logged to the default logger: %q", def)
	}
}

// TestNilLoggerUsesDefaultResolvedAtCallTime is case (b) for the market
// channel: the stream is built (and SetLogger never called) BEFORE the default
// is swapped, so a logger captured at construction would miss the buffers.
func TestNilLoggerUsesDefaultResolvedAtCallTime(t *testing.T) {
	v := newRTDSVenue(t)
	pm := NewWSPolymarketMarket(connector.New(false, nil), Config{}) // Logger nil
	t.Cleanup(pm.Stop)

	_, buf := pmSwapDefault(t, slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := pm.Start(ctx, v.url, 20); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !pmWaitForMsg(t, buf, "websocket: connected") {
		t.Errorf("nil Logger did not log to the swapped default; got %q", buf.String())
	}
	if c := v.conn(0); c != nil {
		_ = c.CloseNow()
	}
	if !pmWaitForMsg(t, buf, "market WS: disconnected") {
		t.Errorf("nil Logger did not log the disconnect line to the default; got %q", buf.String())
	}
}

// TestSetLoggerNilRestoresDefault documents that SetLogger(nil) is the default
// state, not a way to silence a stream.
func TestSetLoggerNilRestoresDefault(t *testing.T) {
	pm := NewWSPolymarketMarket(connector.New(false, nil), Config{})
	inj, _ := pmInstanceLogger(slog.LevelInfo)
	pm.SetLogger(inj)
	if pm.log() != inj {
		t.Fatal("SetLogger did not take effect")
	}
	pm.SetLogger(nil)
	if pm.log() != slog.Default() {
		t.Fatal("SetLogger(nil) did not fall back to slog.Default()")
	}
	if pm.wsOptions(0).Logger != nil {
		t.Fatal("wsOptions kept a logger after SetLogger(nil)")
	}
}
