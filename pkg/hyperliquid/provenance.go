package hyperliquid

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats is a point-in-time snapshot of one WSHyperliquid's frame accounting.
// It is read from lock-free atomics, so Stats is safe to call from any
// goroutine, including a dispatcher handler and a signal handler.
//
// The identity that holds at any instant is
//
//	FramesRead == FramesDelivered + FramesDropped + FramesDrained + inFlight
//
// where inFlight is 0 or 1 (the frame the dispatcher is currently decoding) and
// FramesDelivered is not tracked here (it is whatever the consumer counted).
// See the H2 report for the loopback test that asserts it.
type Stats struct {
	// FramesRead counts every frame handed to the read loop, before the decode
	// queue — including pongs, acks, error frames and frames for unsubscribed
	// coins. Every frame read consumes exactly one FrameID.
	FramesRead uint64
	// FramesDropped counts frames discarded because the decode queue was full.
	FramesDropped uint64
	// FramesDrained counts frames discarded on disconnect: queued but never
	// decoded, so never delivered to the dispatcher.
	FramesDrained uint64
	// QueueLen is the number of frames currently waiting in the decode queue.
	QueueLen int
	// QueueCap is the configured depth of the decode queue
	// (Options.EventQueueSize, default 8192).
	QueueCap int
	// QueueHighWater is the deepest the decode queue has ever been. It equals
	// QueueCap once a drop has happened.
	QueueHighWater int
	// ConnID is the current connection generation (0 before the first connect).
	ConnID uint64
	// Disconnects counts connection drops since Start (not deliberate Stop).
	Disconnects uint64
}

// DropCause says why a frame never reached the dispatcher.
type DropCause string

const (
	// DropQueueFull: the read loop could not enqueue it because the decode
	// queue was full. On a market-only connection the read loop keeps going;
	// on an account-bearing connection it fails the connection instead (see
	// HyperliquidGapEvent).
	DropQueueFull DropCause = "queue_full"
	// DropDrainedOnDisconnect: it was queued when the connection dropped and
	// the queue was discarded (account frames included: this is the loss the
	// gap event exists to announce).
	DropDrainedOnDisconnect DropCause = "drained_on_disconnect"
)

// DroppedFrame describes one frame that never reached the dispatcher. It is
// delivered to the SetOnFrameDropped hook on the goroutine that discarded the
// frame (read loop for DropQueueFull, the disconnect path for
// DropDrainedOnDisconnect), so the hook must be cheap: no blocking, no I/O.
type DroppedFrame struct {
	FrameID uint64    // the frame's ID; consecutive delivered FrameIDs skip it
	ConnID  uint64    // connection generation the frame arrived on
	At      time.Time // the frame's read time
	Cause   DropCause
}

// frameAccounting is the atomic counter block of an hlConn. The zero value is
// ready to use; queueCap must be set once, before the read loop starts.
type frameAccounting struct {
	frameSeq  atomic.Uint64 // last FrameID handed out
	read      atomic.Uint64
	dropped   atomic.Uint64
	drained   atomic.Uint64
	queueLen  atomic.Int64
	highWater atomic.Int64
	disc      atomic.Uint64
	// lostSinceGap accumulates drops and drains not yet announced to a consumer
	// by a gap event. Swapped to zero when a gap event is enqueued.
	lostSinceGap atomic.Uint64
	// overflowed records that the last disconnect was caused by a queue
	// overflow, so the next gap event can report the right cause.
	overflowed atomic.Bool
	queueCap   int

	dropMu sync.RWMutex
	onDrop func(DroppedFrame)
}

func (a *frameAccounting) cap() int {
	if a.queueCap <= 0 {
		return eventQueueSize
	}
	return a.queueCap
}

// enqueued records a frame accepted into the decode queue and raises the
// high-water mark if it is a new deepest point.
func (a *frameAccounting) enqueued() {
	n := a.queueLen.Add(1)
	for {
		cur := a.highWater.Load()
		if n <= cur || a.highWater.CompareAndSwap(cur, n) {
			return
		}
	}
}

// dequeued records a frame leaving the queue (decoded or drained).
func (a *frameAccounting) dequeued() { a.queueLen.Add(-1) }

// noteDrop counts a discarded frame and, if a hook is registered, reports it.
func (a *frameAccounting) noteDrop(f frame, cause DropCause) {
	a.lostSinceGap.Add(1)
	switch cause {
	case DropQueueFull:
		a.dropped.Add(1)
	case DropDrainedOnDisconnect:
		a.drained.Add(1)
	}
	a.dropMu.RLock()
	fn := a.onDrop
	a.dropMu.RUnlock()
	if fn == nil {
		return
	}
	fn(DroppedFrame{FrameID: f.id, ConnID: f.connID, At: f.rx, Cause: cause})
}

func (a *frameAccounting) setOnDrop(fn func(DroppedFrame)) {
	a.dropMu.Lock()
	a.onDrop = fn
	a.dropMu.Unlock()
}

func (a *frameAccounting) snapshot(connID uint64) Stats {
	return Stats{
		FramesRead:     a.read.Load(),
		FramesDropped:  a.dropped.Load(),
		FramesDrained:  a.drained.Load(),
		QueueLen:       int(a.queueLen.Load()),
		QueueCap:       a.cap(),
		QueueHighWater: int(a.highWater.Load()),
		ConnID:         connID,
		Disconnects:    a.disc.Load(),
	}
}

// gapFrame is the synthetic payload of a frame that is not a wire frame but a
// provable-loss notice, so it travels the same queue and is ordered by the same
// FIFO as decoded events.
type gapFrame struct {
	prevConnID      uint64
	lostFrames      uint64
	cause           string
	accountChannels bool
}
