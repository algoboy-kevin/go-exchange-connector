package hyperliquid

import (
	"sync"
	"sync/atomic"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

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
	onDrop func(connector.DroppedFrame)
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
func (a *frameAccounting) noteDrop(f frame, cause connector.DropCause) {
	a.lostSinceGap.Add(1)
	switch cause {
	case connector.DropQueueFull:
		a.dropped.Add(1)
	case connector.DropDrainedOnDisconnect:
		a.drained.Add(1)
	}
	a.dropMu.RLock()
	fn := a.onDrop
	a.dropMu.RUnlock()
	if fn == nil {
		return
	}
	fn(connector.DroppedFrame{FrameID: f.id, ConnID: f.connID, At: f.rx, Cause: cause})
}

func (a *frameAccounting) setOnDrop(fn func(connector.DroppedFrame)) {
	a.dropMu.Lock()
	a.onDrop = fn
	a.dropMu.Unlock()
}

func (a *frameAccounting) snapshot(connID uint64) connector.FrameStats {
	return connector.FrameStats{
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
