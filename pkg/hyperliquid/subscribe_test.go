package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Fake venue
// ─────────────────────────────────────────────────────────────

// venueFrame is one client frame as the fake venue received it.
type venueFrame struct {
	conn   int // index of the accepted connection, 0-based
	at     time.Time
	method string
	sub    Subscription
}

// fakeVenue is an httptest WebSocket server speaking the Hyperliquid
// subscribe protocol. By default it acks every subscribe with the real frame
// shape; reply overrides the response per subscription ("" = no response).
type fakeVenue struct {
	url      string
	reply    func(sub Subscription) string
	onAccept func(idx int, c *coderws.Conn)

	mu     sync.Mutex
	frames []venueFrame
	conns  []*coderws.Conn
}

func newFakeVenue(t *testing.T, reply func(Subscription) string, onAccept func(int, *coderws.Conn)) *fakeVenue {
	t.Helper()
	v := &fakeVenue{reply: reply, onAccept: onAccept}
	if v.reply == nil {
		v.reply = venueAck
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		v.mu.Lock()
		idx := len(v.conns)
		v.conns = append(v.conns, c)
		v.mu.Unlock()
		if v.onAccept != nil {
			v.onAccept(idx, c)
		}
		ctx := context.Background()
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			at := time.Now()
			var req struct {
				Method       string       `json:"method"`
				Subscription Subscription `json:"subscription"`
			}
			if json.Unmarshal(data, &req) != nil || req.Method == "ping" {
				continue
			}
			v.mu.Lock()
			v.frames = append(v.frames, venueFrame{conn: idx, at: at, method: req.Method, sub: req.Subscription})
			v.mu.Unlock()
			if req.Method == "subscribe" {
				if out := v.reply(req.Subscription); out != "" {
					_ = c.Write(ctx, coderws.MessageText, []byte(out))
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	v.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return v
}

// subscribes returns the subscribe frames received, optionally on one
// connection (conn < 0 = all).
func (v *fakeVenue) subscribes(conn int) []venueFrame {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []venueFrame
	for _, f := range v.frames {
		if f.method == "subscribe" && (conn < 0 || f.conn == conn) {
			out = append(out, f)
		}
	}
	return out
}

func (v *fakeVenue) connCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.conns)
}

// closeConn closes accepted connection idx from the venue side.
func (v *fakeVenue) closeConn(idx int) {
	v.mu.Lock()
	c := v.conns[idx]
	v.mu.Unlock()
	go func() { _ = c.Close(coderws.StatusTryAgainLater, "venue restart") }()
}

// venueAck is the real ack frame shape. l2Book echoes the venue defaults
// (nSigFigs/mantissa null) like the live venue does.
func venueAck(sub Subscription) string {
	b, _ := json.Marshal(sub)
	if sub.Type == ChannelL2Book {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if _, ok := m["nSigFigs"]; !ok {
			m["nSigFigs"] = nil
		}
		if _, ok := m["mantissa"]; !ok {
			m["mantissa"] = nil
		}
		b, _ = json.Marshal(m)
	}
	return `{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":` + string(b) + `}}`
}

// venueError is the venue's refusal frame, embedding the subscription JSON.
func venueError(sub Subscription) string {
	b, _ := json.Marshal(sub)
	msg, _ := json.Marshal("Invalid subscription " + string(b))
	return `{"channel":"error","data":` + string(msg) + `}`
}

// ─────────────────────────────────────────────────────────────
// Client helpers
// ─────────────────────────────────────────────────────────────

type eventLog struct {
	mu   sync.Mutex
	evs  []any
	ats  []time.Time
	acks []SubscriptionAck
}

func (l *eventLog) add(ev any) {
	l.mu.Lock()
	l.evs = append(l.evs, ev)
	l.ats = append(l.ats, time.Now())
	l.mu.Unlock()
}

func (l *eventLog) addAck(a SubscriptionAck) {
	l.mu.Lock()
	l.acks = append(l.acks, a)
	l.mu.Unlock()
}

func (l *eventLog) ackList() []SubscriptionAck {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]SubscriptionAck(nil), l.acks...)
}

// firstOf returns the receive time of the first event matching match.
func (l *eventLog) firstOf(match func(any) bool) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, ev := range l.evs {
		if match(ev) {
			return l.ats[i], true
		}
	}
	return time.Time{}, false
}

// newLiveHL builds a manager against url (not started). Keepalives are off so
// the venue sees only subscription frames; Stop runs at cleanup.
func newLiveHL(t *testing.T, url string, paceMs int64) (*WSHyperliquid, *eventLog) {
	t.Helper()
	log := &eventLog{}
	base := connector.New(false, nil)
	base.SetDispatcher(log.add)
	h := NewWithOptions(base, Options{
		URL:                 url,
		ReconnectIntervalMs: 20,
		AppPingIntervalMs:   -1,
		WSPingIntervalMs:    -1,
		SubscribePaceMs:     paceMs,
	})
	h.SetOnSubscriptionAck(log.addAck)
	t.Cleanup(h.Stop)
	return h, log
}

func startHL(t *testing.T, h *WSHyperliquid) {
	t.Helper()
	if err := h.Start(context.Background(), 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func tradeSubs(n int) []Subscription {
	out := make([]Subscription, n)
	for i := range out {
		out[i] = Subscription{Type: ChannelTrades, Coin: fmt.Sprintf("C%02d", i)}
	}
	return out
}

func keysOf(subs []Subscription) []string {
	out := make([]string, len(subs))
	for i, s := range subs {
		out[i] = s.Key()
	}
	return out
}

// assertGaps checks consecutive receive times are at least min apart.
func assertGaps(t *testing.T, frames []venueFrame, min time.Duration) {
	t.Helper()
	for i := 1; i < len(frames); i++ {
		if gap := frames[i].at.Sub(frames[i-1].at); gap < min {
			t.Errorf("gap %d->%d = %v, want >= %v", i-1, i, gap, min)
		}
	}
}

// ─────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────

func TestSubscribeManyPacing(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 25)
	startHL(t, h)

	rep, err := h.SubscribeMany(context.Background(), tradeSubs(10), ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}
	if len(rep.Acked) != 10 {
		t.Fatalf("acked = %d, want 10: %+v", len(rep.Acked), rep)
	}
	frames := v.subscribes(0)
	if len(frames) != 10 {
		t.Fatalf("venue got %d subscribe frames, want 10", len(frames))
	}
	assertGaps(t, frames, 20*time.Millisecond)
}

func TestSubscribeManyAcks(t *testing.T) {
	user := "0x8BA1F109551BD432803012645AC136DDD64DBA72"
	// The venue echoes the user address as it likes; the ack must still match.
	reply := func(sub Subscription) string {
		if sub.User != "" {
			sub.User = strings.ToUpper(sub.User[:2]) + strings.ToUpper(sub.User[2:])
		}
		return venueAck(sub)
	}
	v := newFakeVenue(t, reply, nil)
	h, log := newLiveHL(t, v.url, 0) // unpaced path

	// Registered before Start: sent inline by the unpaced connect hook.
	if err := h.SubscribeAllMids(context.Background()); err != nil {
		t.Fatalf("SubscribeAllMids: %v", err)
	}
	if got := h.ConnID(); got != 0 {
		t.Fatalf("ConnID before Start = %d, want 0", got)
	}
	startHL(t, h)
	if got := h.ConnID(); got != 1 {
		t.Fatalf("ConnID after Start = %d, want 1", got)
	}

	subs := []Subscription{
		{Type: ChannelL2Book, Coin: "SOL"},
		{Type: ChannelTrades, Coin: "BTC"},
		{Type: ChannelBBO, Coin: "ETH"},
		{Type: ChannelCandle, Coin: "BTC", Interval: "1m"},
		{Type: ChannelOrderUpdates, User: user},
	}
	rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}
	if len(rep.Acked) != len(subs) || len(rep.Unconfirmed)+len(rep.Failed)+len(rep.Deferred)+len(rep.Skipped) != 0 {
		t.Fatalf("report = %+v, want all %d acked", rep, len(subs))
	}

	// allMids (inline replay) + the 5 above.
	waitFor(t, 2*time.Second, "6 ack callbacks", func() bool { return len(log.ackList()) == 6 })
	seen := map[string]bool{}
	for _, a := range log.ackList() {
		if a.ConnID != 1 {
			t.Errorf("ack %s ConnID = %d, want 1", a.Sub.Key(), a.ConnID)
		}
		if a.At.IsZero() {
			t.Errorf("ack %s has zero At", a.Sub.Key())
		}
		seen[a.Sub.Key()] = true
	}
	for _, k := range append(keysOf(subs[:4]), "orderUpdates:"+strings.ToLower(user), "allMids") {
		if !seen[k] {
			t.Errorf("no ack callback for %s (got %v)", k, seen)
		}
	}
}

func TestSubscribeManyFailure(t *testing.T) {
	bad := Subscription{Type: ChannelTrades, Coin: "BAD"}
	reply := func(sub Subscription) string {
		if sub.Key() == bad.Key() {
			return venueError(sub)
		}
		return venueAck(sub)
	}
	v := newFakeVenue(t, reply, nil)
	h, log := newLiveHL(t, v.url, 10)
	startHL(t, h)

	subs := []Subscription{{Type: ChannelTrades, Coin: "BTC"}, bad, {Type: ChannelTrades, Coin: "ETH"}}
	rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}
	if len(rep.Failed) != 1 || rep.Failed[0].Sub.Key() != bad.Key() {
		t.Fatalf("failed = %+v, want [%s]", rep.Failed, bad.Key())
	}
	if !strings.Contains(rep.Failed[0].Message, "Invalid subscription") {
		t.Errorf("failure message = %q", rep.Failed[0].Message)
	}
	if len(rep.Acked) != 2 || len(rep.Unconfirmed) != 0 {
		t.Errorf("acked=%v unconfirmed=%v, want 2 acked", keysOf(rep.Acked), keysOf(rep.Unconfirmed))
	}
	found := false
	for _, s := range h.Subscriptions() {
		if s.Key() == bad.Key() {
			found = true
		}
	}
	if !found {
		t.Error("failed subscription was dropped from the registry; it must stay until Unsubscribe")
	}
	// The existing error event still fires.
	if _, ok := log.firstOf(func(ev any) bool { _, ok := ev.(*connector.HyperliquidErrorEvent); return ok }); !ok {
		t.Error("no HyperliquidErrorEvent dispatched for the error frame")
	}
}

func TestSubscribeManyTimeout(t *testing.T) {
	silent := Subscription{Type: ChannelTrades, Coin: "SILENT"}
	reply := func(sub Subscription) string {
		if sub.Key() == silent.Key() {
			return ""
		}
		return venueAck(sub)
	}
	v := newFakeVenue(t, reply, nil)
	h, _ := newLiveHL(t, v.url, 0)
	startHL(t, h)

	start := time.Now()
	rep, err := h.SubscribeMany(context.Background(),
		[]Subscription{{Type: ChannelTrades, Coin: "BTC"}, silent, {Type: ChannelTrades, Coin: "ETH"}},
		ManyOptions{AckTimeout: 150 * time.Millisecond})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}
	if len(rep.Unconfirmed) != 1 || rep.Unconfirmed[0].Key() != silent.Key() {
		t.Fatalf("unconfirmed = %v, want [%s]", keysOf(rep.Unconfirmed), silent.Key())
	}
	if len(rep.Acked) != 2 {
		t.Errorf("acked = %v, want 2", keysOf(rep.Acked))
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned after %v, before the 150ms AckTimeout", elapsed)
	}
	if elapsed > time.Second {
		t.Errorf("returned after %v, want promptly after the 150ms AckTimeout", elapsed)
	}
}

func TestSubscribeManyDeferredBeforeStart(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 10)

	subs := tradeSubs(3)
	rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{})
	if err != nil {
		t.Fatalf("SubscribeMany: %v", err)
	}
	if len(rep.Deferred) != 3 || len(rep.Acked)+len(rep.Unconfirmed)+len(rep.Failed)+len(rep.Skipped) != 0 {
		t.Fatalf("report = %+v, want all 3 deferred", rep)
	}
	if v.connCount() != 0 || len(v.subscribes(-1)) != 0 {
		t.Fatal("something was sent before Start")
	}

	startHL(t, h)
	waitFor(t, 2*time.Second, "3 deferred frames after connect", func() bool { return len(v.subscribes(0)) == 3 })
	got := map[string]bool{}
	for _, f := range v.subscribes(0) {
		got[f.sub.Key()] = true
	}
	for _, k := range keysOf(subs) {
		if !got[k] {
			t.Errorf("deferred %s not sent after connect", k)
		}
	}
}

func TestSubscribeManySkipped(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 0)
	startHL(t, h)

	subs := tradeSubs(3)
	if rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{AckTimeout: 2 * time.Second}); err != nil || len(rep.Acked) != 3 {
		t.Fatalf("first SubscribeMany = %+v, %v", rep, err)
	}
	before := len(v.subscribes(-1))

	rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("repeat SubscribeMany: %v", err)
	}
	if len(rep.Skipped) != 3 || len(rep.Acked)+len(rep.Unconfirmed)+len(rep.Failed)+len(rep.Deferred) != 0 {
		t.Fatalf("repeat report = %+v, want all 3 skipped", rep)
	}

	// A duplicate inside one call: the first is sent, the second skipped.
	dup := Subscription{Type: ChannelBBO, Coin: "DUP"}
	rep, err = h.SubscribeMany(context.Background(), []Subscription{dup, dup}, ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil || len(rep.Acked) != 1 || len(rep.Skipped) != 1 {
		t.Fatalf("duplicate-in-call report = %+v, %v", rep, err)
	}

	time.Sleep(100 * time.Millisecond)
	if got := len(v.subscribes(-1)); got != before+1 {
		t.Errorf("venue frames = %d, want %d (no frame for skipped subs)", got, before+1)
	}
}

func TestSubscribeManyValidationRegistersNothing(t *testing.T) {
	h, _ := newTestHyperliquid(t)
	_, err := h.SubscribeMany(context.Background(),
		[]Subscription{{Type: ChannelTrades, Coin: "BTC"}, {Type: "nope", Coin: "ETH"}}, ManyOptions{})
	if err == nil {
		t.Fatal("SubscribeMany with an invalid sub = nil error")
	}
	if n := len(h.Subscriptions()); n != 0 {
		t.Fatalf("registry has %d subs after a failed validation, want 0", n)
	}
}

func TestReconnectReplayPacedAndConnID(t *testing.T) {
	stale := Subscription{Type: ChannelTrades, Coin: "STALE"}
	reply := func(sub Subscription) string {
		if sub.Key() == stale.Key() {
			return ""
		}
		return venueAck(sub)
	}
	v := newFakeVenue(t, reply, nil)
	h, log := newLiveHL(t, v.url, 20)
	startHL(t, h)

	subs := tradeSubs(3)
	rep, err := h.SubscribeMany(context.Background(), subs, ManyOptions{AckTimeout: 2 * time.Second})
	if err != nil || len(rep.Acked) != 3 {
		t.Fatalf("SubscribeMany on conn 1 = %+v, %v", rep, err)
	}
	if h.ConnID() != 1 {
		t.Fatalf("ConnID = %d, want 1", h.ConnID())
	}

	v.closeConn(0)
	waitFor(t, 3*time.Second, "reconnect (ConnID 2)", func() bool { return h.ConnID() == 2 })
	waitFor(t, 2*time.Second, "3 replay frames on conn 2", func() bool { return len(v.subscribes(1)) == 3 })
	assertGaps(t, v.subscribes(1), 15*time.Millisecond)

	waitFor(t, 2*time.Second, "3 acks on conn 2", func() bool {
		n := 0
		for _, a := range log.ackList() {
			if a.ConnID == 2 {
				n++
			}
		}
		return n == 3
	})

	// A stale ack stamped with connection 1 must not confirm a sub sent on 2.
	type result struct {
		rep *SubscribeReport
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := h.SubscribeMany(context.Background(), []Subscription{stale}, ManyOptions{AckTimeout: 300 * time.Millisecond})
		done <- result{rep, err}
	}()
	waitFor(t, 2*time.Second, "STALE frame at the venue", func() bool {
		for _, f := range v.subscribes(1) {
			if f.sub.Key() == stale.Key() {
				return true
			}
		}
		return false
	})
	h.conn.eventCh <- frame{data: []byte(venueAck(stale)), rx: time.Now(), connID: 1}

	res := <-done
	if res.err != nil {
		t.Fatalf("SubscribeMany(STALE): %v", res.err)
	}
	if len(res.rep.Acked) != 0 || len(res.rep.Unconfirmed) != 1 {
		t.Fatalf("stale ack confirmed a sub on the new connection: %+v", res.rep)
	}
	// The callback still reports the stale ack, with its own ConnID.
	waitFor(t, time.Second, "stale ack callback", func() bool {
		for _, a := range log.ackList() {
			if a.Sub.Key() == stale.Key() && a.ConnID == 1 {
				return true
			}
		}
		return false
	})
}

// Same rule exercised directly on processFrame: only the matching ConnID
// resolves a pending subscription.
func TestProcessFrameAckConnIDMatch(t *testing.T) {
	h, _ := newTestHyperliquid(t)
	c := h.conn
	w := c.newAckWaiter()
	defer c.removeAckWaiter(w)
	sub := Subscription{Type: ChannelL2Book, Coin: "SOL"}
	c.expectAck(w, sub.Key(), 2)

	h.processFrame(frame{data: []byte(venueAck(sub)), rx: time.Now(), connID: 1})
	if c.ackSettled(w) {
		t.Fatal("ack from connection 1 resolved a sub sent on connection 2")
	}
	h.processFrame(frame{data: []byte(`{"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{"type":"l2Book","coin":"SOL"}}}`), rx: time.Now(), connID: 2})
	if c.ackSettled(w) {
		t.Fatal("unsubscribe ack resolved a pending subscribe")
	}
	h.processFrame(frame{data: []byte(venueAck(sub)), rx: time.Now(), connID: 2})
	if !c.ackSettled(w) || !w.acked[sub.Key()] {
		t.Fatal("ack from connection 2 did not resolve the sub")
	}
}

func TestReplayDoesNotBlockReading(t *testing.T) {
	bbo := `{"channel":"bbo","data":{"coin":"C00","time":1,"bbo":[{"px":"1","sz":"1","n":1},null]}}`
	v := newFakeVenue(t, nil, func(_ int, c *coderws.Conn) {
		_ = c.Write(context.Background(), coderws.MessageText, []byte(bbo))
	})
	h, log := newLiveHL(t, v.url, 50)

	subs := make([]Subscription, 20)
	for i := range subs {
		subs[i] = Subscription{Type: ChannelBBO, Coin: fmt.Sprintf("C%02d", i)}
	}
	if err := h.Subscribe(context.Background(), subs...); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	t0 := time.Now()
	startHL(t, h)
	isBBO := func(ev any) bool { _, ok := ev.(*connector.HyperliquidBBOEvent); return ok }
	waitFor(t, 2*time.Second, "bbo event", func() bool { _, ok := log.firstOf(isBBO); return ok })
	bboAt, _ := log.firstOf(isBBO)
	if d := bboAt.Sub(t0); d >= 300*time.Millisecond {
		t.Errorf("bbo data frame dispatched %v after Start, want < 300ms (replay must not block reading)", d)
	}

	waitFor(t, 3*time.Second, "20 replay frames", func() bool { return len(v.subscribes(0)) == 20 })
	frames := v.subscribes(0)
	if d := frames[19].at.Sub(frames[0].at); d < 900*time.Millisecond {
		t.Errorf("replay took %v, want >= 900ms at a 50ms pace", d)
	}
	if !bboAt.Before(frames[19].at) {
		t.Error("bbo dispatched only after the replay finished")
	}
}

func TestStopCancelsReplay(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 50)
	if err := h.Subscribe(context.Background(), tradeSubs(20)...); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	baseline := runtime.NumGoroutine()

	startHL(t, h)
	waitFor(t, 2*time.Second, "3 replay frames", func() bool { return len(v.subscribes(0)) >= 3 })
	st := h.conn.currentConnState()
	if st == nil {
		t.Fatal("no connection state while connected")
	}
	h.Stop()
	select {
	case <-st.done:
	case <-time.After(time.Second):
		t.Fatal("replay goroutine still running after Stop")
	}
	n := len(v.subscribes(-1))
	if n >= 20 {
		t.Fatalf("replay finished (%d frames) before Stop; test is not exercising cancellation", n)
	}
	time.Sleep(200 * time.Millisecond)
	if got := len(v.subscribes(-1)); got != n {
		t.Errorf("venue got %d frames after Stop (had %d)", got, n)
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline {
		t.Errorf("goroutines = %d after Stop, baseline %d (leak)", got, baseline)
	}
}

func TestSubscribeManyCtxCancelDefers(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 50)
	startHL(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		rep *SubscribeReport
		err error
	}
	done := make(chan result, 1)
	subs := tradeSubs(10)
	go func() {
		rep, err := h.SubscribeMany(ctx, subs, ManyOptions{AckTimeout: 2 * time.Second})
		done <- result{rep, err}
	}()
	waitFor(t, 2*time.Second, "2 frames", func() bool { return len(v.subscribes(0)) >= 2 })
	cancel()
	res := <-done

	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", res.err)
	}
	r := res.rep
	if len(r.Deferred) == 0 {
		t.Fatalf("no Deferred after cancel: %+v", r)
	}
	if total := len(r.Acked) + len(r.Unconfirmed) + len(r.Deferred) + len(r.Failed); total != 10 {
		t.Errorf("report classifies %d subs, want 10: %+v", total, r)
	}
	if n := len(h.Subscriptions()); n != 10 {
		t.Errorf("registry = %d subs, want all 10 still registered", n)
	}
	time.Sleep(150 * time.Millisecond)
	if got := len(v.subscribes(0)); got >= 10 {
		t.Errorf("venue got %d frames on the connection; cancel did not stop sending", got)
	}
}

func TestNoDoubleSendDuringReplay(t *testing.T) {
	v := newFakeVenue(t, nil, nil)
	h, _ := newLiveHL(t, v.url, 50)
	if err := h.Subscribe(context.Background(), tradeSubs(10)...); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	startHL(t, h)
	waitFor(t, 2*time.Second, "2 replay frames", func() bool { return len(v.subscribes(0)) >= 2 })

	newSub := Subscription{Type: ChannelTrades, Coin: "NEW"}
	if err := h.Subscribe(context.Background(), newSub); err != nil {
		t.Fatalf("Subscribe(NEW): %v", err)
	}
	waitFor(t, 3*time.Second, "11 frames", func() bool { return len(v.subscribes(0)) >= 11 })
	time.Sleep(150 * time.Millisecond)

	count := map[string]int{}
	for _, f := range v.subscribes(-1) {
		count[f.sub.Key()]++
	}
	if count[newSub.Key()] != 1 {
		t.Errorf("NEW sent %d times, want exactly 1", count[newSub.Key()])
	}
	for k, n := range count {
		if n != 1 {
			t.Errorf("%s sent %d times, want 1", k, n)
		}
	}
	if len(count) != 11 {
		t.Errorf("distinct subscriptions sent = %d, want 11", len(count))
	}
	assertGaps(t, v.subscribes(0), 45*time.Millisecond)
}

func TestSubscriptionInMessage(t *testing.T) {
	tests := []struct {
		msg  string
		want string
		ok   bool
	}{
		{`Invalid subscription {"type":"l2Book","coin":"FOO"}`, "l2Book:FOO", true},
		{`Already subscribed: {"type":"candle","coin":"BTC","interval":"1m"} (dup)`, "candle:BTC:1m", true},
		{`bad {"type":"trades","coin":"a}b"} trailing {`, "trades:a}b", true},
		{`{"type":"userFills","user":"0xABCDEF0000000000000000000000000000000000"}`, "userFills:0xabcdef0000000000000000000000000000000000", true},
		{`no json here`, "", false},
		{`unterminated {"type":"trades"`, "", false},
		{`{"foo":1}`, "", false},
	}
	for _, tc := range tests {
		sub, ok := subscriptionInMessage(tc.msg)
		if ok != tc.ok || (ok && sub.Key() != tc.want) {
			t.Errorf("subscriptionInMessage(%q) = %q, %v; want %q, %v", tc.msg, sub.Key(), ok, tc.want, tc.ok)
		}
	}
}
