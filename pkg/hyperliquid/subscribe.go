package hyperliquid

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

// defaultManyAckTimeout is ManyOptions.AckTimeout when left zero.
const defaultManyAckTimeout = 10 * time.Second

// SubscriptionAck is one venue acknowledgement of a subscribe frame:
//
//	{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{...}}}
type SubscriptionAck struct {
	// Sub is the subscription object the venue echoed, with its defaults
	// filled in (a null nSigFigs/mantissa decodes to zero, i.e. unset). User
	// is lowercased so Sub.Key() matches the registered key.
	Sub Subscription
	// ConnID is the connection generation the ack arrived on (see ConnID).
	ConnID uint64
	// At is the local receive time of the frame (sampled on the read loop).
	At time.Time
}

// SubscribeFailure is a subscription the venue refused with an error frame.
type SubscribeFailure struct {
	Sub     Subscription
	Message string
}

// SubscribeReport classifies every subscription passed to SubscribeMany.
type SubscribeReport struct {
	// Acked were acknowledged on the connection they were sent on, before
	// the deadline.
	Acked []Subscription
	// Failed were refused by a venue error frame attributed to them.
	Failed []SubscribeFailure
	// Unconfirmed were sent but neither acked nor failed before AckTimeout
	// (or before ctx was done, or the connection changed).
	Unconfirmed []Subscription
	// Deferred are registered but were not sent because the socket is down
	// (or ctx was cancelled first); they go out on the next connect.
	Deferred []Subscription
	// Skipped were already registered (same Key); nothing was sent.
	Skipped []Subscription
}

// ManyOptions tunes SubscribeMany.
type ManyOptions struct {
	// AckTimeout bounds the wait for acks, counted from the last frame sent.
	// Zero selects 10s.
	AckTimeout time.Duration
}

// SetOnSubscriptionAck registers a callback for subscribe acks
// (method=="subscribe"; unsubscribe acks are not reported). It runs on the
// dispatcher goroutine, in frame order, so keep it short. A recorder can use
// it as the per-channel "resubscribed" signal after a reconnect: ConnID tells
// which connection the ack belongs to. nil clears it.
func (h *WSHyperliquid) SetOnSubscriptionAck(fn func(SubscriptionAck)) {
	h.conn.sm.ackMu.Lock()
	h.conn.sm.onAck = fn
	h.conn.sm.ackMu.Unlock()
}

// ConnID returns the connection generation: 0 before the first successful
// connect, +1 on every successful (re)connect.
func (h *WSHyperliquid) ConnID() uint64 {
	return h.conn.sm.connID.Load()
}

// SubscribeMany validates, registers and sends subs, then waits for the
// venue's verdict on each and reports it.
//
//   - Validation and normalization are those of Subscribe; any invalid
//     subscription fails the call and registers nothing.
//   - Subscriptions already registered are Skipped.
//   - With the socket down everything newly registered is Deferred and the
//     call returns at once: the registry is the source of truth and the next
//     connect sends them.
//   - Otherwise one frame per subscription is sent, Options.SubscribePaceMs
//     apart, and the call waits until each is acked or failed, AckTimeout
//     after the last frame, or ctx is done. An ack only counts on the
//     connection the frame was sent on.
//
// A cancelled ctx stops sending: the unsent subscriptions stay registered and
// are reported Deferred, and the error is ctx.Err(). Transport problems are not
// errors (as with Subscribe).
//
// Failure attribution is best effort: the venue's error frame is a plain
// string, so a subscription is Failed only when the message embeds its JSON
// object. A Failed subscription stays registered — a reconnect would retry it
// — so call Unsubscribe to drop it.
//
// Unconfirmed on a live socket needs the same treatment. Observed against the
// live venue (2026-10-30): subscribing a coin that does not exist produces
// neither an ack nor an error frame, and the venue closes the socket a
// fraction of a second later. Left registered, the reconnect replay re-sends
// the bad subscription and the connection flaps forever (exponential
// backoff, but it still spends the 30 connections/minute budget). Validate
// the universe against the venue's meta first (InfoClient.Meta/MetaDex), and
// Unsubscribe whatever comes back Unconfirmed or Failed.
//
// Safe for concurrent use.
func (h *WSHyperliquid) SubscribeMany(ctx context.Context, subs []Subscription, opt ManyOptions) (*SubscribeReport, error) {
	norm, err := normalizeSubscriptions(subs)
	if err != nil {
		return nil, err
	}
	c := h.conn
	rep := &SubscribeReport{}

	added := c.addSubs(norm)
	addedKeys := make(map[string]int, len(added))
	for _, sub := range added {
		addedKeys[sub.Key()]++
	}
	for _, sub := range norm {
		if k := sub.Key(); addedKeys[k] > 0 {
			addedKeys[k]--
			continue
		}
		rep.Skipped = append(rep.Skipped, sub)
	}
	if len(added) == 0 {
		return rep, nil
	}
	if c.currentConnState() == nil {
		rep.Deferred = append(rep.Deferred, added...)
		return rep, nil
	}

	timeout := opt.AckTimeout
	if timeout <= 0 {
		timeout = defaultManyAckTimeout
	}

	w := c.newAckWaiter()
	defer c.removeAckWaiter(w)

	var (
		sent     []Subscription
		lastSent time.Time
		retErr   error
	)
sendLoop:
	for i, sub := range added {
		key := sub.Key()
		out, err := c.sendSubscribe(ctx, nil, sub, func(connID uint64) { c.expectAck(w, key, connID) })
		switch out {
		case sendSent, sendAlreadySent:
			sent = append(sent, sub)
			lastSent = time.Now()
		case sendGone:
			// Unsubscribed concurrently: no longer ours to report.
			c.forgetAck(w, key)
		default:
			c.forgetAck(w, key)
			rep.Deferred = append(rep.Deferred, sub)
		}
		if err != nil {
			rep.Deferred = append(rep.Deferred, added[i+1:]...)
			if ctxErr := ctx.Err(); ctxErr != nil {
				retErr = ctxErr
			} else {
				// The connection is dying; the reconnect replay sends the rest.
				h.log().Warn("hyperliquid: SubscribeMany write failed", "subscription", key, "err", err)
			}
			break sendLoop
		}
	}

	if len(sent) > 0 && retErr == nil {
		deadline := time.NewTimer(time.Until(lastSent.Add(timeout)))
		defer deadline.Stop()
	waitLoop:
		for !c.ackSettled(w) {
			select {
			case <-w.notify:
			case <-deadline.C:
				break waitLoop
			case <-ctx.Done():
				retErr = ctx.Err()
				break waitLoop
			}
		}
	}

	c.sm.trackMu.Lock()
	for _, sub := range sent {
		key := sub.Key()
		if msg, ok := w.failed[key]; ok {
			rep.Failed = append(rep.Failed, SubscribeFailure{Sub: sub, Message: msg})
		} else if w.acked[key] {
			rep.Acked = append(rep.Acked, sub)
		} else {
			rep.Unconfirmed = append(rep.Unconfirmed, sub)
		}
	}
	c.sm.trackMu.Unlock()
	return rep, retErr
}

// ─────────────────────────────────────────────────────────────
// Per-connection state, pacing and replay
// ─────────────────────────────────────────────────────────────

// subState groups the subscription machinery of one hlConn. The zero value is
// ready to use.
type subState struct {
	// connID is the connection generation, bumped by every onConnect.
	connID atomic.Uint64

	// cur is the state of the live connection, nil while down. Guarded by
	// hlConn.mu (the registry mutex) so registration, the sent set and the
	// replay snapshot change atomically with respect to each other.
	cur *connState

	// paceMu is the pacing gate shared by every subscribe frame (Subscribe,
	// SubscribeMany, replay, across connections): it is held across the wait
	// and the write, so frames are serialized and lastSend is exact.
	paceMu   sync.Mutex
	lastSend time.Time

	ackMu sync.RWMutex
	onAck func(SubscriptionAck)

	// trackMu guards waiters and every ackWaiter's maps. Lock order:
	// hlConn.mu before trackMu, never the reverse.
	trackMu sync.Mutex
	waiters map[*ackWaiter]struct{}
}

// connState is one connection generation: its socket, the context of its
// paced replay (cancelled by onDisconnect and Stop) and the set of
// subscription keys already sent on it.
type connState struct {
	id     uint64
	conn   *coderws.Conn
	ctx    context.Context
	cancel context.CancelFunc
	sent   map[string]struct{} // guarded by hlConn.mu
	done   chan struct{}       // closed when the replay goroutine exits
}

// sendOutcome is the result of sendSubscribe.
type sendOutcome int

const (
	sendDeferred    sendOutcome = iota // not sent: socket down / state replaced / ctx done / write failed
	sendSent                           // written to the current connection
	sendAlreadySent                    // already sent on the current connection by someone else
	sendGone                           // no longer registered, nothing sent
)

func (c *hlConn) paceGap() time.Duration {
	if c.owner.opts.SubscribePaceMs <= 0 {
		return 0
	}
	return time.Duration(c.owner.opts.SubscribePaceMs) * time.Millisecond
}

func (c *hlConn) currentConnState() *connState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sm.cur
}

// installConnState starts a new connection generation and returns its state
// plus the replay snapshot (sorted by key). Unpaced, every snapshot key is
// marked sent here, because onConnect writes them inline right away.
func (c *hlConn) installConnState(ctx context.Context, conn *coderws.Conn) (*connState, []Subscription) {
	id := c.sm.connID.Add(1)
	stCtx, cancel := context.WithCancel(ctx)
	st := &connState{
		id:     id,
		conn:   conn,
		ctx:    stCtx,
		cancel: cancel,
		sent:   make(map[string]struct{}),
		done:   make(chan struct{}),
	}
	paced := c.paceGap() > 0

	c.mu.Lock()
	prev := c.sm.cur
	c.sm.cur = st
	subs := make([]Subscription, 0, len(c.subs))
	for key, sub := range c.subs {
		subs = append(subs, sub)
		if !paced {
			st.sent[key] = struct{}{}
		}
	}
	c.mu.Unlock()
	sort.Slice(subs, func(i, j int) bool { return subs[i].Key() < subs[j].Key() })

	if prev != nil {
		prev.cancel()
	}
	return st, subs
}

// dropConnState clears the current connection state if it is st (any state
// when st is nil) and cancels its replay. It returns the dropped state, or nil.
func (c *hlConn) dropConnState(st *connState) *connState {
	c.mu.Lock()
	cur := c.sm.cur
	if cur == nil || (st != nil && cur != st) {
		c.mu.Unlock()
		if st != nil {
			st.cancel()
		}
		return nil
	}
	c.sm.cur = nil
	c.mu.Unlock()
	cur.cancel()
	return cur
}

// waitConnState waits for st's replay goroutine to exit. Nil is a no-op.
func waitConnState(st *connState) {
	if st != nil {
		<-st.done
	}
}

// sendSubscribe writes one subscribe frame through the pacing gate.
//
// want pins the connection (the replay passes its own state); nil means the
// current one. The key is claimed in the connection's sent set right before
// the write, so a subscription is written at most once per connection;
// onClaim (optional) runs under the registry lock at that moment with the
// connection id, also when someone else already sent it.
//
// A non-nil error is ctx's error or the write error; the outcome is then
// sendDeferred.
func (c *hlConn) sendSubscribe(ctx context.Context, want *connState, sub Subscription, onClaim func(connID uint64)) (sendOutcome, error) {
	st := want
	if st == nil {
		st = c.currentConnState()
	}
	if st == nil {
		return sendDeferred, nil
	}
	data, err := SubscribeFrame(sub)
	if err != nil {
		return sendDeferred, err
	}

	gap := c.paceGap()
	if gap > 0 {
		c.sm.paceMu.Lock()
		defer c.sm.paceMu.Unlock()
		if wait := time.Until(c.sm.lastSend.Add(gap)); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return sendDeferred, ctx.Err()
			case <-st.ctx.Done():
				// The connection went away while we waited: the next
				// connect's replay sends it.
				t.Stop()
				return sendDeferred, nil
			}
		}
	}
	// Checked before the write: coder/websocket closes the whole connection
	// when a write's ctx is cancelled mid-frame.
	if err := ctx.Err(); err != nil {
		return sendDeferred, err
	}

	key := sub.Key()
	c.mu.Lock()
	if c.sm.cur != st {
		c.mu.Unlock()
		return sendDeferred, nil
	}
	if _, ok := c.subs[key]; !ok {
		c.mu.Unlock()
		return sendGone, nil
	}
	if _, ok := st.sent[key]; ok {
		if onClaim != nil {
			onClaim(st.id)
		}
		c.mu.Unlock()
		return sendAlreadySent, nil
	}
	st.sent[key] = struct{}{}
	if onClaim != nil {
		onClaim(st.id)
	}
	c.mu.Unlock()

	err = c.writeFrame(ctx, st.conn, data)
	if gap > 0 {
		c.sm.lastSend = time.Now()
	}
	if err != nil {
		c.mu.Lock()
		if c.sm.cur == st {
			delete(st.sent, key)
		}
		c.mu.Unlock()
		return sendDeferred, err
	}
	return sendSent, nil
}

// replay re-sends the snapshot on st, paced, from its own goroutine. It stops
// when st is cancelled (disconnect, Stop) or on the first write error — the
// connection is dying and the next connect replays again.
func (c *hlConn) replay(st *connState, subs []Subscription) {
	defer close(st.done)
	start := time.Now()
	n := 0
	for _, sub := range subs {
		if st.ctx.Err() != nil {
			c.log().Debug("hyperliquid: replay cancelled", "conn_id", st.id, "sent", n, "of", len(subs))
			return
		}
		out, err := c.sendSubscribe(st.ctx, st, sub, nil)
		if err != nil {
			if st.ctx.Err() == nil {
				c.log().Warn("hyperliquid: replay write failed, stopping replay",
					"conn_id", st.id, "subscription", sub.Key(), "err", err)
				if failedByGate(err) {
					// not a dying connection: fail it so the next connect replays the whole registry
					c.failConnection(st)
				}
			}
			return
		}
		switch out {
		case sendSent:
			n++
		case sendDeferred:
			return // the connection state was replaced
		}
	}
	c.log().Info("hyperliquid: subscriptions replayed", "conn_id", st.id, "sent", n,
		"subscriptions", len(subs), "took", time.Since(start).Round(time.Millisecond).String())
}

// ─────────────────────────────────────────────────────────────
// Acks and failures
// ─────────────────────────────────────────────────────────────

// ackWaiter tracks the pending subscriptions of one SubscribeMany call. All
// fields are guarded by subState.trackMu.
type ackWaiter struct {
	pending map[string]uint64 // key -> connection id the frame was sent on
	acked   map[string]bool
	failed  map[string]string
	notify  chan struct{} // cap 1: "something resolved"
}

func (c *hlConn) newAckWaiter() *ackWaiter {
	w := &ackWaiter{
		pending: make(map[string]uint64),
		acked:   make(map[string]bool),
		failed:  make(map[string]string),
		notify:  make(chan struct{}, 1),
	}
	c.sm.trackMu.Lock()
	if c.sm.waiters == nil {
		c.sm.waiters = make(map[*ackWaiter]struct{})
	}
	c.sm.waiters[w] = struct{}{}
	c.sm.trackMu.Unlock()
	return w
}

func (c *hlConn) removeAckWaiter(w *ackWaiter) {
	c.sm.trackMu.Lock()
	delete(c.sm.waiters, w)
	c.sm.trackMu.Unlock()
}

func (c *hlConn) expectAck(w *ackWaiter, key string, connID uint64) {
	c.sm.trackMu.Lock()
	w.pending[key] = connID
	c.sm.trackMu.Unlock()
}

func (c *hlConn) forgetAck(w *ackWaiter, key string) {
	c.sm.trackMu.Lock()
	delete(w.pending, key)
	c.sm.trackMu.Unlock()
}

func (c *hlConn) ackSettled(w *ackWaiter) bool {
	c.sm.trackMu.Lock()
	defer c.sm.trackMu.Unlock()
	return len(w.pending) == 0
}

// resolve marks key acked (fail == false) or failed with msg on every waiter
// that sent it on connection connID.
func (c *hlConn) resolve(key string, connID uint64, fail bool, msg string) {
	c.sm.trackMu.Lock()
	defer c.sm.trackMu.Unlock()
	for w := range c.sm.waiters {
		id, ok := w.pending[key]
		if !ok || id != connID {
			continue
		}
		delete(w.pending, key)
		if fail {
			w.failed[key] = msg
		} else {
			w.acked[key] = true
		}
		select {
		case w.notify <- struct{}{}:
		default:
		}
	}
}

// handleSubscriptionResponse resolves a subscribe ack against in-flight
// SubscribeMany calls and reports it to the ack callback.
func (h *WSHyperliquid) handleSubscriptionResponse(data json.RawMessage, f frame) {
	var resp struct {
		Method       string        `json:"method"`
		Subscription *Subscription `json:"subscription"`
	}
	if err := json.Unmarshal(data, &resp); err != nil || resp.Subscription == nil {
		h.log().Debug("hyperliquid: unrecognised subscription response", "data", truncate(string(data), 256))
		return
	}
	if resp.Method != "subscribe" {
		return
	}
	sub := *resp.Subscription
	sub.User = strings.ToLower(sub.User)
	ack := SubscriptionAck{Sub: sub, ConnID: f.connID, At: f.rx}

	h.conn.resolve(sub.Key(), f.connID, false, "")

	h.conn.sm.ackMu.RLock()
	fn := h.conn.sm.onAck
	h.conn.sm.ackMu.RUnlock()
	if fn != nil {
		fn(ack)
	}
}

// attributeErrorFrame marks the subscription embedded in a venue error message
// as failed, if an in-flight SubscribeMany sent it on this connection.
func (h *WSHyperliquid) attributeErrorFrame(msg string, f frame) {
	sub, ok := subscriptionInMessage(msg)
	if !ok {
		return
	}
	h.conn.resolve(sub.Key(), f.connID, true, msg)
}

// subscriptionInMessage extracts the first {...} object of msg (brace matched,
// string aware) and decodes it as a Subscription.
func subscriptionInMessage(msg string) (Subscription, bool) {
	start := strings.IndexByte(msg, '{')
	if start < 0 {
		return Subscription{}, false
	}
	depth := 0
	inString, escaped := false, false
	end := -1
scan:
	for i := start; i < len(msg); i++ {
		ch := msg[i]
		switch {
		case escaped:
			escaped = false
		case inString:
			switch ch {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
		case ch == '"':
			inString = true
		case ch == '{':
			depth++
		case ch == '}':
			depth--
			if depth == 0 {
				end = i
				break scan
			}
		}
	}
	if end < 0 {
		return Subscription{}, false
	}
	var sub Subscription
	if err := json.Unmarshal([]byte(msg[start:end+1]), &sub); err != nil || sub.Type == "" {
		return Subscription{}, false
	}
	sub.User = strings.ToLower(sub.User)
	return sub, true
}
