// Package framestat holds the frame-level accounting shared by the venue
// adapters: a monotonic frame-ID source, exact read/dropped/drained counters,
// queue depth and high-water mark, the connection generation, and the
// drop-hook slot.
//
// The hot path is atomics only, so a read loop can stamp a frame with two
// atomic adds, enqueue it, and (on the drop path) report it without ever
// blocking. Stats are read as separate atomics, so a snapshot is eventually
// consistent at the nanosecond scale and exact at rest (queue empty).
//
// The zero value is ready to use. SetQueueCap must be called once, before the
// read loop starts, with the resolved depth of the queue the accounting
// describes.
package framestat

import (
	"sync"
	"sync/atomic"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// Accounting is the atomic counter block of one stream connection. It spans
// reconnects: FrameIDs and the read/drop/drain totals are never reset, and the
// connection generation is advanced with SetConnID on every successful dial.
type Accounting struct {
	frameSeq  atomic.Uint64 // last FrameID handed out
	read      atomic.Uint64
	dropped   atomic.Uint64
	drained   atomic.Uint64
	queueLen  atomic.Int64
	highWater atomic.Int64
	connID    atomic.Uint64
	disc      atomic.Uint64

	// queueCap is written once before the read loop starts (SetQueueCap) and
	// read by snapshot without synchronisation; it is not mutated afterwards,
	// which is the same contract the adapters' constructors follow.
	queueCap int

	dropMu sync.RWMutex
	onDrop func(connector.DroppedFrame)
}

// SetQueueCap records the resolved depth of the queue this accounting
// describes. It must be called once, before any frame is read.
func (a *Accounting) SetQueueCap(n int) { a.queueCap = n }

// QueueCap returns the recorded queue depth (0 when it was never set, which
// only happens for an adapter that has not been constructed yet).
func (a *Accounting) QueueCap() int { return a.queueCap }

// Stamp assigns the next FrameID and counts the frame as read. It is called on
// the read loop, before the queue, so a frame that is later dropped still
// consumes an ID: the gaps a consumer sees in delivered FrameIDs are exactly
// the frames that never made it.
func (a *Accounting) Stamp() (id uint64) {
	id = a.frameSeq.Add(1)
	a.read.Add(1)
	return id
}

// Enqueued records a frame accepted into the queue and raises the high-water
// mark if it is a new deepest point.
func (a *Accounting) Enqueued() {
	n := a.queueLen.Add(1)
	for {
		cur := a.highWater.Load()
		if n <= cur || a.highWater.CompareAndSwap(cur, n) {
			return
		}
	}
}

// Dequeued records a frame leaving the queue (decoded or drained).
func (a *Accounting) Dequeued() { a.queueLen.Add(-1) }

// NoteDrop counts one discarded frame, in the bucket named by cause, and calls
// the drop hook when one is registered. It runs on the goroutine that discarded
// the frame (the read loop for a full queue, the disconnect path for a drain),
// so the hook must be cheap: no blocking, no I/O.
func (a *Accounting) NoteDrop(id, connID uint64, at time.Time, cause connector.DropCause) {
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
	fn(connector.DroppedFrame{FrameID: id, ConnID: connID, At: at, Cause: cause})
}

// SetOnDrop installs (or clears) the drop hook. A nil hook disables reporting;
// the counters keep counting either way.
func (a *Accounting) SetOnDrop(fn func(connector.DroppedFrame)) {
	a.dropMu.Lock()
	a.onDrop = fn
	a.dropMu.Unlock()
}

// SetConnID records the generation of the connection now being read.
func (a *Accounting) SetConnID(id uint64) { a.connID.Store(id) }

// ConnID returns the current connection generation (0 before the first dial).
func (a *Accounting) ConnID() uint64 { return a.connID.Load() }

// NoteDisconnect counts one connection drop. An adapter that drains its queue
// on disconnect (Binance) calls it AFTER the drain, so an observer that sees
// Disconnects grow also sees the drain counters settled. Adapters that never
// drain (the Polymarket streams) simply call it on the drop.
func (a *Accounting) NoteDisconnect() { a.disc.Add(1) }

// Snapshot returns a point-in-time view of the counters.
func (a *Accounting) Snapshot() connector.FrameStats {
	return connector.FrameStats{
		FramesRead:     a.read.Load(),
		FramesDropped:  a.dropped.Load(),
		FramesDrained:  a.drained.Load(),
		QueueLen:       int(a.queueLen.Load()),
		QueueCap:       a.queueCap,
		QueueHighWater: int(a.highWater.Load()),
		ConnID:         a.connID.Load(),
		Disconnects:    a.disc.Load(),
	}
}
