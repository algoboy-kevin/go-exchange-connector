package websocket

// logger_test.go covers the optional instance logger added in H5: a
// WSOptions.Logger receives exactly this connection's lines, and a nil Logger
// keeps logging to slog.Default() — resolved at CALL time, not captured at
// Connect, so a process that swaps the default still gets the lines.
//
// The discriminating property is the second half of each assertion: the line
// must appear in the injected logger and NOT in the swapped default (and the
// reverse for the nil case). Before H5 every line went to the global, so both
// injected-logger tests fail without it.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// instanceLogger returns a logger writing JSON into its own buffer, standing in
// for a consumer's per-stream With("venue", "role") logger.
func instanceLogger(level slog.Level) (*slog.Logger, *lockedLogBuffer) {
	buf := &lockedLogBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// swapDefaultLogger replaces the process default logger for the duration of the
// test and returns the buffer it writes to. Cleanup restores the previous
// default, so a failure cannot leak into another test's expectations.
func swapDefaultLogger(t *testing.T, level slog.Level) (*slog.Logger, *lockedLogBuffer) {
	t.Helper()
	l, buf := instanceLogger(level)
	old := slog.Default()
	slog.SetDefault(l)
	t.Cleanup(func() { slog.SetDefault(old) })
	return l, buf
}

// waitForMsg polls buf until it holds the given JSON "msg" value or the
// deadline passes. Log lines are written by connection goroutines, so they
// arrive shortly after the call that caused them rather than during it.
func waitForMsg(t *testing.T, buf *lockedLogBuffer, msg string) bool {
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

// TestInjectedLoggerReceivesConnectionLines is case (a): the connect line of an
// instance with WSOptions.Logger set goes to that logger and nowhere else.
func TestInjectedLoggerReceivesConnectionLines(t *testing.T) {
	url, _ := wsRecordingServer(t)
	// Swap the default first: the injected logger must win over it.
	_, defBuf := swapDefaultLogger(t, slog.LevelDebug)
	inj, injBuf := instanceLogger(slog.LevelDebug)

	opts := DefaultWSOptions()
	opts.DisableDataStaleWatchdog = true
	opts.Logger = inj

	b := &BaseWebSocket{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("connect: %v", err)
	}
	b.Close()

	if !strings.Contains(injBuf.String(), `"msg":"websocket: connected"`) {
		t.Errorf("injected logger did not receive the connect line; got %q", injBuf.String())
	}
	if strings.Contains(defBuf.String(), "websocket:") {
		t.Errorf("injected-logger instance also logged to the default logger: %q", defBuf.String())
	}
}

// TestNilLoggerUsesDefaultResolvedAtCallTime is case (b), and pins the
// resolution rule: the BaseWebSocket and its options exist BEFORE the default
// logger is swapped, and a second swap after Connect must also be honoured. A
// logger captured at construction (or at Connect) would send both lines to the
// wrong buffer.
func TestNilLoggerUsesDefaultResolvedAtCallTime(t *testing.T) {
	url, _ := wsRecordingServer(t)

	// Built with Logger nil, before any default-logger swap.
	b := &BaseWebSocket{}
	opts := DefaultWSOptions()
	opts.DisableDataStaleWatchdog = true
	opts.ReconnectInterval = 20 // redial quickly after the forced disconnect

	// Swap #1: the object already exists; a call-time lookup must use this.
	_, first := swapDefaultLogger(t, slog.LevelDebug)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Connect(ctx, url, opts); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !strings.Contains(first.String(), `"msg":"websocket: connected"`) {
		t.Fatalf("nil Logger did not log to the swapped default; got %q", first.String())
	}

	// Swap #2, after Connect: the reconnect line must follow the NEW default,
	// proving the lookup happens per call rather than once at Connect.
	_, second := swapDefaultLogger(t, slog.LevelDebug)
	b.Disconnect() // reconnLoop logs "websocket: reconnecting" and redials
	if !waitForMsg(t, second, "websocket: reconnecting") {
		t.Errorf("post-Connect swap not honoured (call-time lookup); got %q", second.String())
	}
	b.Close()
}

// TestWSOptionsLoggerIsNotSerialised guards the additive contract: the field is
// a pointer to a logger, so it must stay out of JSON (WSOptions is part of
// consumers' configuration surface).
func TestWSOptionsLoggerIsNotSerialised(t *testing.T) {
	inj, _ := instanceLogger(slog.LevelInfo)
	opts := DefaultWSOptions()
	opts.Logger = inj
	raw, err := json.Marshal(opts)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(strings.ToLower(string(raw)), "logger") {
		t.Errorf("WSOptions.Logger leaked into JSON: %s", raw)
	}
}
