package hyperliquid

// logger_test.go covers H5's optional instance logger for this package: an
// Options.Logger receives this manager's lines (including the ones the embedded
// websocket.BaseWebSocket writes, which Start wires to the same logger), and a
// nil Logger keeps logging to slog.Default() resolved at call time.
//
// Both injected-logger tests assert the line is ABSENT from the swapped default,
// so they fail without the change (before H5 the global received everything).

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// hlLogBuffer is a mutex-guarded sink: the connection logs from its own
// goroutines while the test reads.
type hlLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *hlLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *hlLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func hlInstanceLogger(level slog.Level) (*slog.Logger, *hlLogBuffer) {
	buf := &hlLogBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// hlSwapDefault replaces the process default logger for the duration of the
// test and returns its buffer; cleanup restores the previous default.
func hlSwapDefault(t *testing.T, level slog.Level) (*slog.Logger, *hlLogBuffer) {
	t.Helper()
	l, buf := hlInstanceLogger(level)
	old := slog.Default()
	slog.SetDefault(l)
	t.Cleanup(func() { slog.SetDefault(old) })
	return l, buf
}

// TestInjectedLoggerReceivesConnectLines is case (a) plus the wiring from item
// 3: the manager's own connect line AND the BaseWebSocket's line land in the
// injected logger, and neither reaches the swapped default.
func TestInjectedLoggerReceivesConnectLines(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	_, defBuf := hlSwapDefault(t, slog.LevelDebug)
	inj, injBuf := hlInstanceLogger(slog.LevelDebug)

	opts := DefaultOptions()
	opts.URL = v.url
	opts.Logger = inj
	// No subscriptions: the manager dials on Start and stays quiet otherwise.
	h := NewWithOptions(connector.New(false, nil), opts)
	t.Cleanup(h.Stop)

	if err := h.Start(context.Background(), 50); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := injBuf.String(); !strings.Contains(got, `"msg":"hyperliquid: connected"`) {
		t.Errorf("injected logger did not receive the hyperliquid connect line; got %q", got)
	}
	if got := injBuf.String(); !strings.Contains(got, `"msg":"websocket: connected"`) {
		t.Errorf("websocket layer did not use the venue logger; got %q", got)
	}
	if got := defBuf.String(); strings.Contains(got, "hyperliquid:") || strings.Contains(got, "websocket:") {
		t.Errorf("injected-logger manager also logged to the default logger: %q", got)
	}
}

// TestInjectedLoggerReceivesQueueDropLine is the drop path this card exists
// for: a full event queue warns through the instance logger, not the global.
// The dispatcher is deliberately not started, so the second frame cannot be
// drained and must be dropped.
func TestInjectedLoggerReceivesQueueDropLine(t *testing.T) {
	_, defBuf := hlSwapDefault(t, slog.LevelDebug)
	inj, injBuf := hlInstanceLogger(slog.LevelDebug)

	opts := DefaultOptions()
	opts.EventQueueSize = 1
	opts.Logger = inj
	h := NewWithOptions(connector.New(false, nil), opts)

	// Market channel frame (not account-bearing): a drop is counted and logged.
	if err := h.conn.onMessage(context.Background(), []byte(`{"channel":"trades"}`)); err != nil {
		t.Fatalf("onMessage 1: %v", err)
	}
	if err := h.conn.onMessage(context.Background(), []byte(`{"channel":"trades"}`)); err != nil {
		t.Fatalf("onMessage 2: %v", err)
	}

	if got := injBuf.String(); !strings.Contains(got, `"msg":"hyperliquid: dropping frame, event queue full"`) {
		t.Errorf("injected logger did not receive the queue-full warning; got %q", got)
	}
	if got := defBuf.String(); strings.Contains(got, "hyperliquid:") {
		t.Errorf("drop warning also reached the default logger: %q", got)
	}
	if d := h.Stats().FramesDropped; d != 1 {
		t.Errorf("FramesDropped = %d, want 1", d)
	}
}

// TestNilLoggerUsesDefaultResolvedAtCallTime is case (b): the manager is built
// with Logger nil BEFORE the default is swapped, so a logger captured at
// construction would send these lines to the old default and the buffers would
// stay empty.
func TestNilLoggerUsesDefaultResolvedAtCallTime(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	opts := DefaultOptions()
	opts.URL = v.url // Logger nil
	h := NewWithOptions(connector.New(false, nil), opts)
	t.Cleanup(h.Stop)

	_, buf := hlSwapDefault(t, slog.LevelDebug)

	if err := h.Start(context.Background(), 50); err != nil {
		t.Fatalf("start: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, `"msg":"hyperliquid: connected"`) {
		t.Errorf("nil Logger did not log to the swapped default; got %q", got)
	}
	if !strings.Contains(got, `"msg":"websocket: connected"`) {
		t.Errorf("websocket layer did not fall back to the swapped default; got %q", got)
	}
}
