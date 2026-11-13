package binance

// logger_test.go covers H5's optional instance logger: Options.Logger receives
// this manager's lines, including the BaseWebSocket's connect line that
// wsOptions (used by Start) wires to it, and a nil Logger keeps logging to
// slog.Default() resolved at call time.
//
// The injected-logger assertions also require the line to be ABSENT from the
// swapped default, which is what fails without the change.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// bnLogBuffer is a mutex-guarded sink: connection goroutines log while the test
// reads.
type bnLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *bnLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *bnLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func bnInstanceLogger(level slog.Level) (*slog.Logger, *bnLogBuffer) {
	buf := &bnLogBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// bnSwapDefault replaces the process default logger for the duration of the
// test and returns its buffer; cleanup restores the previous default.
func bnSwapDefault(t *testing.T, level slog.Level) (*slog.Logger, *bnLogBuffer) {
	t.Helper()
	l, buf := bnInstanceLogger(level)
	old := slog.Default()
	slog.SetDefault(l)
	t.Cleanup(func() { slog.SetDefault(old) })
	return l, buf
}

// TestInjectedLoggerReceivesConnectionLines is case (a) plus the wiring from
// item 3: the subscription write line and the websocket layer's connect line
// both land in the injected logger and neither reaches the swapped default.
// Spot is never dialled — only the perp "public" connection is, against
// loopback (the spot endpoint is a constant, so a test must not use Start).
func TestInjectedLoggerReceivesConnectionLines(t *testing.T) {
	v := newLoVenue(t)
	_, defBuf := bnSwapDefault(t, slog.LevelDebug)
	inj, injBuf := bnInstanceLogger(slog.LevelDebug)

	b := NewWithOptions(connector.New(false, nil), Options{
		PerpMarketWS: v.url,
		PerpPublicWS: v.url,
		PerpREST:     v.url,
		Logger:       inj,
	})
	c := b.connFor(MarketPerp, classPublic)
	t.Cleanup(b.Stop)

	opts := b.wsOptions(20)
	if opts.Logger != inj {
		t.Fatalf("wsOptions did not wire the manager's logger into WSOptions.Logger")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.Connect(ctx, c.wsURL(), opts); err != nil {
		t.Fatalf("loopback connect: %v", err)
	}
	b.SubscribeBookTicker(ctx, MarketPerp, []string{"btcusdt"})

	got := injBuf.String()
	if !strings.Contains(got, `"msg":"binance: writeSubscribe"`) {
		t.Errorf("injected logger did not receive the subscribe line; got %q", got)
	}
	if !strings.Contains(got, `"msg":"websocket: connected"`) {
		t.Errorf("websocket layer did not use the venue logger; got %q", got)
	}
	if def := defBuf.String(); strings.Contains(def, "binance:") || strings.Contains(def, "websocket:") {
		t.Errorf("injected-logger manager also logged to the default logger: %q", def)
	}
}

// TestNilLoggerUsesDefaultResolvedAtCallTime is case (b). The manager is built
// with Logger nil BEFORE the default is swapped, and the line chosen (a dropped
// send with no connection) is emitted synchronously by a method call, so a
// logger captured at construction cannot satisfy the assertion.
func TestNilLoggerUsesDefaultResolvedAtCallTime(t *testing.T) {
	b := NewWithOptions(connector.New(false, nil), Options{}) // Logger nil
	t.Cleanup(b.Stop)

	_, buf := bnSwapDefault(t, slog.LevelDebug)

	// No Start: the connection is nil, so the send is dropped and logged.
	b.SubscribeBookTicker(context.Background(), MarketPerp, []string{"btcusdt"})

	got := buf.String()
	if !strings.Contains(got, `"msg":"binance: send DROPPED (conn nil)"`) {
		t.Errorf("nil Logger did not log to the swapped default; got %q", got)
	}
	if !strings.Contains(got, `"market":"perp"`) {
		t.Errorf("drop line lost its attributes: %q", got)
	}
}

// TestNilLoggerWsOptionsCarriesNoLogger pins the default: with Logger nil the
// connection options get a nil Logger, which the websocket layer resolves to
// slog.Default() at each call — not a captured logger.
func TestNilLoggerWsOptionsCarriesNoLogger(t *testing.T) {
	b := New(connector.New(false, nil))
	if got := b.wsOptions(0).Logger; got != nil {
		t.Fatalf("wsOptions().Logger = %v, want nil for the zero Options", got)
	}
}
