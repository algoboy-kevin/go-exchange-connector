package hyperliquid

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// ─────────────────────────────────────────────────────────────
// Fixtures: scripted clock, observation recorder, frame bodies
// ─────────────────────────────────────────────────────────────

// seqClock hands out a scripted sequence of times, one per read, and panics if
// the code under test reads it more often than scripted. That makes both the
// exact values and the NUMBER of clock reads assertions, not guesses, and it
// keeps every duration in this file deterministic — no sleeps.
type seqClock struct {
	mu    sync.Mutex
	times []time.Time
	calls int
}

func (c *seqClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls >= len(c.times) {
		panic("hyperliquid: decode clock read more often than the test scripted")
	}
	t := c.times[c.calls]
	c.calls++
	return t
}

// tickClock advances a fixed step on every read; used where the exact values do
// not matter (parity runs) but the clock must stay deterministic and monotonic.
type tickClock struct {
	mu    sync.Mutex
	at    time.Time
	step  time.Duration
	calls int
}

func (c *tickClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.at
	c.at = c.at.Add(c.step)
	c.calls++
	return t
}

func (c *seqClock) reads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// decodeRecorder captures the hook observations and the order in which the hook
// and the typed dispatcher ran.
type decodeRecorder struct {
	mu  sync.Mutex
	obs []FrameDecodeTiming
	seq []string
}

func (r *decodeRecorder) addHook(f FrameDecodeTiming) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.obs = append(r.obs, f)
	r.seq = append(r.seq, "hook")
}

func (r *decodeRecorder) addEvent() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq = append(r.seq, "event")
}

func (r *decodeRecorder) observations() []FrameDecodeTiming {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]FrameDecodeTiming(nil), r.obs...)
}

func (r *decodeRecorder) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seq...)
}

// newDecodeTestHL builds a manager with the decode hook installed and a
// dispatcher that records every typed event plus its position relative to the
// hook. Nothing connects; frames go through processFrame exactly as the
// dispatcher receives them.
func newDecodeTestHL(t *testing.T) (*WSHyperliquid, *decodeRecorder, *[]any) {
	t.Helper()
	base := connector.New(false, nil)
	rec := &decodeRecorder{}
	var events []any
	base.SetDispatcher(func(ev any) {
		rec.addEvent()
		events = append(events, ev)
	})
	opts := DefaultOptions()
	opts.OnMarketFrameDecoded = rec.addHook
	return NewWithOptions(base, opts), rec, &events
}

// subscribePublic registers the four public market channels this hook covers
// for BTC (and ETH where a test needs a mixed batch).
func subscribePublic(t *testing.T, h *WSHyperliquid, coins ...string) {
	t.Helper()
	ctx := context.Background()
	for _, err := range []error{
		h.SubscribeL2Book(ctx, coins, SubParams{}),
		h.SubscribeTrades(ctx, coins),
		h.SubscribeBBO(ctx, coins),
		h.SubscribeActiveAssetCtx(ctx, coins),
	} {
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}
}

const (
	decodeBookFrame = `{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,` +
		`"levels":[[{"px":"100","sz":"1","n":1}],[{"px":"101","sz":"2","n":1}]]}}`
	decodeTradesFrame = `{"channel":"trades","data":[` +
		`{"coin":"BTC","side":"B","px":"100","sz":"1","time":1700000000000,"tid":1},` +
		`{"coin":"BTC","side":"A","px":"101","sz":"2","time":1700000000001,"tid":2}]}`
	decodeBBOFrame = `{"channel":"bbo","data":{"coin":"BTC","time":1700000000000,` +
		`"bbo":[{"px":"100","sz":"1","n":1},{"px":"101","sz":"2","n":1}]}}`
	decodeCtxFrame = `{"channel":"activeAssetCtx","data":{"coin":"BTC",` +
		`"ctx":{"funding":"0.00001","oraclePx":"100","openInterest":"5"}}}`
)

func wantObservation(t *testing.T, got, want FrameDecodeTiming) {
	t.Helper()
	if got.FrameID != want.FrameID || got.ConnID != want.ConnID {
		t.Errorf("FrameID/ConnID = %d/%d, want %d/%d", got.FrameID, got.ConnID, want.FrameID, want.ConnID)
	}
	if !got.ReceivedAt.Equal(want.ReceivedAt) {
		t.Errorf("ReceivedAt = %v, want %v", got.ReceivedAt, want.ReceivedAt)
	}
	if !got.StartedAt.Equal(want.StartedAt) {
		t.Errorf("StartedAt = %v, want %v", got.StartedAt, want.StartedAt)
	}
	if !got.CompletedAt.Equal(want.CompletedAt) {
		t.Errorf("CompletedAt = %v, want %v", got.CompletedAt, want.CompletedAt)
	}
	if got.Channel != want.Channel {
		t.Errorf("Channel = %q, want %q", got.Channel, want.Channel)
	}
	if got.Outcome != want.Outcome {
		t.Errorf("Outcome = %q, want %q", got.Outcome, want.Outcome)
	}
}

// ─────────────────────────────────────────────────────────────
// Vertical path: l2Book through the real processFrame
// ─────────────────────────────────────────────────────────────

// TestDecodeHookObservesL2BookOnce is the vertical RED test: the API exists but
// no decode path reports into it yet, so it fails on "0 observations" until the
// book path is wired.
func TestDecodeHookObservesL2BookOnce(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	start := rx.Add(2 * time.Millisecond)
	done := start.Add(700 * time.Microsecond)
	clk := &seqClock{times: []time.Time{start, done}}
	h.decodeNow = clk.now

	h.processFrame(frame{data: []byte(decodeBookFrame), rx: rx, id: 9, connID: 3})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want exactly 1 (hook not wired into the decode path?)", len(obs))
	}
	wantObservation(t, obs[0], FrameDecodeTiming{
		FrameID:     9,
		ConnID:      3,
		ReceivedAt:  rx,
		StartedAt:   start,
		CompletedAt: done,
		Channel:     ChannelL2Book,
		Outcome:     DecodeSucceeded,
	})
	if d := obs[0].CompletedAt.Sub(obs[0].StartedAt); d != 700*time.Microsecond {
		t.Errorf("CompletedAt-StartedAt = %v, want 700µs", d)
	}
	if clk.reads() != 2 {
		t.Errorf("clock reads = %d, want exactly 2 (entry + completion)", clk.reads())
	}
	if len(*events) != 1 {
		t.Fatalf("typed events = %d, want 1 (hook must not replace delivery)", len(*events))
	}
	if order := rec.order(); len(order) != 2 || order[0] != "hook" || order[1] != "event" {
		t.Errorf("order = %v, want [hook event] (hook must fire before DispatchEvent)", order)
	}
}

// ─────────────────────────────────────────────────────────────
// Every covered channel reports exactly once
// ─────────────────────────────────────────────────────────────

func TestDecodeHookObservesBBO(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 1, 0, time.UTC)
	start, done := rx.Add(time.Millisecond), rx.Add(9*time.Millisecond)
	h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

	h.processFrame(frame{data: []byte(decodeBBOFrame), rx: rx, id: 11, connID: 2})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want 1", len(obs))
	}
	wantObservation(t, obs[0], FrameDecodeTiming{
		FrameID: 11, ConnID: 2, ReceivedAt: rx, StartedAt: start, CompletedAt: done,
		Channel: ChannelBBO, Outcome: DecodeSucceeded,
	})
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1", len(*events))
	}
}

// One trades frame carrying N prints must produce ONE observation, not N: the
// hook measures a wire-frame decode, not a typed event.
func TestDecodeHookTradesBatchIsOneObservation(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 2, 0, time.UTC)
	start, done := rx.Add(time.Millisecond), rx.Add(3*time.Millisecond)
	clk := &seqClock{times: []time.Time{start, done}}
	h.decodeNow = clk.now

	h.processFrame(frame{data: []byte(decodeTradesFrame), rx: rx, id: 12, connID: 2})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want exactly 1 for a 2-print batch", len(obs))
	}
	wantObservation(t, obs[0], FrameDecodeTiming{
		FrameID: 12, ConnID: 2, ReceivedAt: rx, StartedAt: start, CompletedAt: done,
		Channel: ChannelTrades, Outcome: DecodeSucceeded,
	})
	if len(*events) != 2 {
		t.Fatalf("typed trade events = %d, want 2 (both prints still delivered)", len(*events))
	}
	if clk.reads() != 2 {
		t.Errorf("clock reads = %d, want 2 (one observation, not one per print)", clk.reads())
	}
}

// An empty batch still decoded successfully: one observation, zero events.
func TestDecodeHookEmptyTradesIsOneObservation(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 3, 0, time.UTC)
	h.decodeNow = (&seqClock{times: []time.Time{rx.Add(time.Millisecond), rx.Add(2 * time.Millisecond)}}).now

	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[]}`), rx: rx, id: 13, connID: 2})

	if obs := rec.observations(); len(obs) != 1 || obs[0].Outcome != DecodeSucceeded {
		t.Fatalf("observations = %+v, want exactly one DecodeSucceeded", obs)
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0 for an empty batch", len(*events))
	}
}

// A batch mixing a subscribed and an unsubscribed coin is still one decode: one
// observation, and the filter still drops only the unsubscribed prints.
func TestDecodeHookTradesMixedBatchIsOneObservation(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 4, 0, time.UTC)
	h.decodeNow = (&seqClock{times: []time.Time{rx.Add(time.Millisecond), rx.Add(2 * time.Millisecond)}}).now

	h.processFrame(frame{data: []byte(`{"channel":"trades","data":[` +
		`{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1},` +
		`{"coin":"SOL","side":"A","px":"2","sz":"1","time":2,"tid":2}]}`), rx: rx, id: 14, connID: 2})

	if obs := rec.observations(); len(obs) != 1 || obs[0].Channel != ChannelTrades {
		t.Fatalf("observations = %+v, want one trades observation", obs)
	}
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1 (the unsubscribed print is still filtered)", len(*events))
	}
}

// ─────────────────────────────────────────────────────────────
// activeAssetCtx: success only after the INNER decode; skipped vs failed
// ─────────────────────────────────────────────────────────────

func TestDecodeHookObservesActiveAssetCtxAfterInnerDecode(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 5, 0, time.UTC)
	start, done := rx.Add(time.Millisecond), rx.Add(5*time.Millisecond)
	h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

	h.processFrame(frame{data: []byte(decodeCtxFrame), rx: rx, id: 15, connID: 2})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want 1", len(obs))
	}
	wantObservation(t, obs[0], FrameDecodeTiming{
		FrameID: 15, ConnID: 2, ReceivedAt: rx, StartedAt: start, CompletedAt: done,
		Channel: ChannelActiveAssetCtx, Outcome: DecodeSucceeded,
	})
	if len(*events) != 1 {
		t.Fatalf("events = %d, want 1", len(*events))
	}
}

// The existing early subscription check stops before the inner ctx decode, so
// an unsubscribed ctx frame is a PARTIAL decode: DecodeSkipped exactly once,
// never DecodeSucceeded, and no event.
func TestDecodeHookActiveAssetCtxUnsubscribedIsSkipped(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	ctx := context.Background()
	if err := h.SubscribeTrades(ctx, []string{"BTC"}); err != nil { // ctx not subscribed
		t.Fatalf("subscribe: %v", err)
	}

	rx := time.Date(2026, 11, 13, 10, 0, 6, 0, time.UTC)
	start := rx.Add(time.Millisecond)
	done := start.Add(200 * time.Microsecond)
	h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

	h.processFrame(frame{data: []byte(decodeCtxFrame), rx: rx, id: 16, connID: 2})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want exactly 1", len(obs))
	}
	if obs[0].Outcome != DecodeSkipped {
		t.Fatalf("Outcome = %q, want %q (partial decode, not success)", obs[0].Outcome, DecodeSkipped)
	}
	if obs[0].Channel != ChannelActiveAssetCtx || !obs[0].StartedAt.Equal(start) || !obs[0].CompletedAt.Equal(done) {
		t.Errorf("observation = %+v, want channel ctx and the scripted span", obs[0])
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0", len(*events))
	}
}

// An inner ctx decode failure is ONE DecodeFailed, with the channel known.
func TestDecodeHookActiveAssetCtxInnerErrorIsFailed(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 0, 7, 0, time.UTC)
	start := rx.Add(time.Millisecond)
	done := start.Add(time.Millisecond)
	h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

	h.processFrame(frame{data: []byte(`{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":{"a":1}}}}`),
		rx: rx, id: 17, connID: 2})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want exactly 1", len(obs))
	}
	if obs[0].Outcome != DecodeFailed || obs[0].Channel != ChannelActiveAssetCtx {
		t.Fatalf("observation = %+v, want one error on activeAssetCtx", obs[0])
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0", len(*events))
	}
}

// ─────────────────────────────────────────────────────────────
// Failures and non-observations
// ─────────────────────────────────────────────────────────────

func TestDecodeHookMalformedPayloadsAreOneFailureEach(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		channel Channel
	}{
		{"l2Book", `{"channel":"l2Book","data":{"coin":"BTC","levels":{}}}`, ChannelL2Book},
		{"trades", `{"channel":"trades","data":{"coin":"BTC"}}`, ChannelTrades},
		{"bbo", `{"channel":"bbo","data":{"coin":123}}`, ChannelBBO},
		{"activeAssetCtx outer", `{"channel":"activeAssetCtx","data":{"coin":123}}`, ChannelActiveAssetCtx},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, rec, events := newDecodeTestHL(t)
			subscribePublic(t, h, "BTC")
			rx := time.Date(2026, 11, 13, 10, 1, 0, 0, time.UTC)
			start, done := rx.Add(time.Millisecond), rx.Add(4*time.Millisecond)
			h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

			h.processFrame(frame{data: []byte(tc.raw), rx: rx, id: 21, connID: 4})

			obs := rec.observations()
			if len(obs) != 1 {
				t.Fatalf("observations = %d, want exactly 1", len(obs))
			}
			if obs[0].Outcome != DecodeFailed || obs[0].Channel != tc.channel {
				t.Fatalf("observation = %+v, want error on %q", obs[0], tc.channel)
			}
			if obs[0].FrameID != 21 || obs[0].ConnID != 4 || !obs[0].ReceivedAt.Equal(rx) {
				t.Errorf("identity = %+v, want the original frame id/conn/rx", obs[0])
			}
			if len(*events) != 0 {
				t.Fatalf("events = %d, want 0", len(*events))
			}
		})
	}
}

// A malformed ENVELOPE cannot name a channel: one DecodeFailed with an empty
// Channel, which the application maps to its bounded "unknown" label.
func TestDecodeHookMalformedEnvelopeIsUnknownFailure(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 2, 0, 0, time.UTC)
	start, done := rx.Add(time.Millisecond), rx.Add(2*time.Millisecond)
	clk := &seqClock{times: []time.Time{start, done}}
	h.decodeNow = clk.now

	h.processFrame(frame{data: []byte(`{"channel":`), rx: rx, id: 22, connID: 4})

	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want exactly 1", len(obs))
	}
	if obs[0].Channel != "" || obs[0].Outcome != DecodeFailed {
		t.Fatalf("observation = %+v, want empty channel + error", obs[0])
	}
	if obs[0].FrameID != 22 || obs[0].ConnID != 4 {
		t.Errorf("identity = %+v, want id 22 / conn 4", obs[0])
	}
	if len(*events) != 0 {
		t.Fatalf("events = %d, want 0", len(*events))
	}
	if clk.reads() != 2 {
		t.Errorf("clock reads = %d, want 2", clk.reads())
	}
}

// Valid frames on channels outside the hook's scope — the other public streams,
// the account streams and every control channel — produce no observation at all.
func TestDecodeHookNonPublicAndControlChannelsProduceNothing(t *testing.T) {
	h, rec, events := newDecodeTestHL(t)
	subscribePublic(t, h, "BTC")
	ctx := context.Background()
	for _, err := range []error{
		h.SubscribeAllMids(ctx),
		h.SubscribeCandles(ctx, []string{"BTC"}, "1m"),
		h.SubscribeOrderUpdates(ctx, testUser),
		h.SubscribeUserFills(ctx, testUser),
	} {
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
	}
	// The entry sample is taken once per real frame when the hook is installed
	// (it must precede the envelope unmarshal), so script exactly one time per
	// frame and fail on any extra read: that proves no completion sample.
	frames := []string{
		`{"channel":"allMids","data":{"mids":{"BTC":"1"}}}`,
		`{"channel":"candle","data":[{"t":1,"T":2,"s":"BTC","i":"1m","o":"1","c":"1","h":"1","l":"1","v":"1","n":1}]}`,
		`{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"1","sz":"1","oid":1,"timestamp":1,"origSz":"1"},"status":"open","statusTimestamp":1}]}`,
		`{"channel":"userFills","data":{"user":"` + testUser + `","fills":[{"coin":"BTC","px":"1","sz":"1","side":"B","dir":"Open Long","closedPnl":"0","fee":"0","startPosition":"0","time":1,"oid":1,"tid":1}]}}`,
		`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}`,
		`{"channel":"pong"}`,
		`{"channel":"error","data":"Invalid subscription"}`,
		`{"channel":"bogus","data":null}`,
	}
	times := make([]time.Time, len(frames))
	for i := range times {
		times[i] = time.Date(2026, 11, 13, 10, 3, i, 0, time.UTC)
	}
	clk := &seqClock{times: times}
	h.decodeNow = clk.now

	for i, raw := range frames {
		h.processFrame(frame{data: []byte(raw), rx: times[i], id: uint64(30 + i), connID: 5})
	}

	if obs := rec.observations(); len(obs) != 0 {
		t.Fatalf("observations = %+v, want none outside the four public market channels", obs)
	}
	if clk.reads() != len(frames) {
		t.Errorf("clock reads = %d, want %d (one entry sample each, never a completion)", clk.reads(), len(frames))
	}
	// Delivery is untouched: allMids, candle, orderUpdates, userFills and the
	// venue error frame dispatch as before; the ack/pong/unknown channels never
	// did.
	if len(*events) != 5 {
		t.Fatalf("events = %d, want 5 (hook must not add or remove delivery)", len(*events))
	}
}

// The synthetic gap notice is not a wire frame: no decode happened, so it must
// not even sample the clock.
func TestDecodeHookSyntheticGapProducesNothing(t *testing.T) {
	h, rec, _ := newDecodeTestHL(t)
	h.decodeNow = func() time.Time { panic("gap frame must not read the decode clock") }

	h.processFrame(frame{gap: &gapFrame{prevConnID: 1, lostFrames: 3, cause: "test"}})

	if obs := rec.observations(); len(obs) != 0 {
		t.Fatalf("observations = %+v, want none for a synthetic gap", obs)
	}
}

// ─────────────────────────────────────────────────────────────
// The boundary is real: hook fires before the typed subscriber work
// ─────────────────────────────────────────────────────────────

// TestDecodeHookExcludesTypedSubscriberWork proves the completion sample happens
// BEFORE DispatchEvent, not after it: the typed dispatcher is held inside a
// handshake while the test reads the already-completed observation. The
// scripted clock panics on any third read, so a completion sample taken after
// the subscriber would crash the test rather than silently inflate a duration.
func TestDecodeHookExcludesTypedSubscriberWork(t *testing.T) {
	base := connector.New(false, nil)
	rec := &decodeRecorder{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	base.SetDispatcher(func(any) {
		close(entered)
		<-release
		rec.addEvent()
	})

	opts := DefaultOptions()
	opts.OnMarketFrameDecoded = rec.addHook
	h := NewWithOptions(base, opts)
	subscribePublic(t, h, "BTC")

	rx := time.Date(2026, 11, 13, 10, 4, 0, 0, time.UTC)
	start, done := rx.Add(time.Millisecond), rx.Add(2*time.Millisecond)
	h.decodeNow = (&seqClock{times: []time.Time{start, done}}).now

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		h.processFrame(frame{data: []byte(decodeBookFrame), rx: rx, id: 40, connID: 6})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("typed dispatcher was never reached")
	}
	obs := rec.observations()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want the hook to have completed before the subscriber ran", len(obs))
	}
	if !obs[0].CompletedAt.Equal(done) {
		t.Fatalf("CompletedAt = %v, want %v: subscriber work must not move the completion sample", obs[0].CompletedAt, done)
	}
	if d := obs[0].CompletedAt.Sub(obs[0].StartedAt); d != time.Millisecond {
		t.Fatalf("Duration = %v, want exactly 1ms regardless of how long the subscriber holds", d)
	}

	unblock()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("processFrame did not return after the subscriber was released")
	}
	if order := rec.order(); len(order) != 2 || order[0] != "hook" || order[1] != "event" {
		t.Fatalf("order = %v, want [hook event]", order)
	}
}

// ─────────────────────────────────────────────────────────────
// Nil hook: identical delivery, zero clock reads
// ─────────────────────────────────────────────────────────────

func newHookOffTestHL(t *testing.T) (*WSHyperliquid, *[]any) {
	t.Helper()
	base := connector.New(false, nil)
	var events []any
	base.SetDispatcher(func(ev any) { events = append(events, ev) })
	h := NewWithOptions(base, DefaultOptions())
	h.decodeNow = func() time.Time {
		panic("hyperliquid: decode clock read with OnMarketFrameDecoded nil")
	}
	return h, &events
}

// canonicalEvents preserves every dispatched field, including SeqID and exact
// integer provenance, while sorting the top-level JSON object keys. RawMessage
// avoids float64 rounding of nanosecond timestamps during normalization.
func canonicalEvents(t *testing.T, events []any) []string {
	t.Helper()
	out := make([]string, 0, len(events))
	for _, ev := range events {
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal %T: %v", ev, err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal %T: %v", ev, err)
		}
		norm, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("re-marshal %T: %v", ev, err)
		}
		out = append(out, string(norm))
	}
	return out
}

// TestDecodeHookNilIsSemanticallyIdentical is the compatibility gate: the same
// frames through a hook-less manager (whose clock panics on any read) and a
// hook-installed one deliver byte-identical typed events, in the same order.
func TestDecodeHookNilIsSemanticallyIdentical(t *testing.T) {
	on, rec, onEvents := newDecodeTestHL(t)
	off, offEvents := newHookOffTestHL(t)
	for _, h := range []*WSHyperliquid{on, off} {
		subscribePublic(t, h, "BTC")
	}
	// A real clock for the hook-on side; it only has to be monotonic.
	base := time.Date(2026, 11, 13, 10, 5, 0, 0, time.UTC)
	on.decodeNow = (&tickClock{at: base, step: time.Millisecond}).now

	frames := []string{
		decodeBookFrame,
		decodeTradesFrame,
		`{"channel":"trades","data":[]}`,
		decodeBBOFrame,
		decodeCtxFrame,
		// unsubscribed coin: decoded, then filtered — still one observation, no event
		`{"channel":"l2Book","data":{"coin":"SOL","time":1,"levels":[[],[]]}}`,
		// failures
		`{"channel":"l2Book","data":{"coin":"BTC","levels":{}}}`,
		`{"channel":`,
		// out-of-scope channels still deliver
		`{"channel":"allMids","data":{"mids":{"BTC":"1"}}}`,
		`{"channel":"error","data":"Invalid subscription"}`,
	}
	rx := base.Add(time.Second)
	for i, raw := range frames {
		f := frame{data: []byte(raw), rx: rx.Add(time.Duration(i) * time.Millisecond), id: uint64(50 + i), connID: 7}
		on.processFrame(f)
		off.processFrame(f)
	}

	if len(rec.observations()) == 0 {
		t.Fatal("hook-on run produced no observations; the parity check would be vacuous")
	}
	onCanon := canonicalEvents(t, *onEvents)
	offCanon := canonicalEvents(t, *offEvents)
	if len(onCanon) != len(offCanon) {
		t.Fatalf("event counts differ: hook-on %d, hook-off %d", len(onCanon), len(offCanon))
	}
	for i := range onCanon {
		if onCanon[i] != offCanon[i] {
			t.Fatalf("event %d differs\n hook-on:  %s\n hook-off: %s", i, onCanon[i], offCanon[i])
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Bounded local benchmark (disclosure only, no SLO)
// ─────────────────────────────────────────────────────────────

func benchHyperliquid(b *testing.B) (*WSHyperliquid, frame) {
	base := connector.New(false, nil)
	opts := DefaultOptions()
	opts.OnMarketFrameDecoded = func(FrameDecodeTiming) {}
	h := NewWithOptions(base, opts)
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	return h, frame{data: []byte(decodeBookFrame), rx: time.Unix(1, 0), id: 1, connID: 1}
}

func BenchmarkDecodeHookOff(b *testing.B) {
	base := connector.New(false, nil)
	h := NewWithOptions(base, DefaultOptions())
	if err := h.SubscribeL2Book(context.Background(), []string{"BTC"}, SubParams{}); err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	f := frame{data: []byte(decodeBookFrame), rx: time.Unix(1, 0), id: 1, connID: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.processFrame(f)
	}
}

func BenchmarkDecodeHookOn(b *testing.B) {
	h, f := benchHyperliquid(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.processFrame(f)
	}
}
