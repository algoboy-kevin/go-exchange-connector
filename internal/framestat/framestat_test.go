package framestat

import (
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// TestAccountingIdentityAndHighWater pins the primitive the venue adapters rely
// on: Stamp assigns consecutive IDs and counts a read, Enqueued/Dequeued keep
// QueueLen consistent, and the high-water mark only follows new maxima.
func TestAccountingIdentityAndHighWater(t *testing.T) {
	var a Accounting
	a.SetQueueCap(4)

	for i := uint64(1); i <= 3; i++ {
		if id := a.Stamp(); id != i {
			t.Fatalf("Stamp() = %d, want %d", id, i)
		}
		a.Enqueued()
	}
	st := a.Snapshot()
	if st.FramesRead != 3 || st.QueueLen != 3 || st.QueueHighWater != 3 || st.QueueCap != 4 {
		t.Fatalf("stats after 3 enqueues = %+v", st)
	}
	a.Dequeued()
	a.Dequeued()
	a.Dequeued()
	a.Enqueued()
	st = a.Snapshot()
	if st.QueueLen != 1 {
		t.Errorf("QueueLen = %d, want 1", st.QueueLen)
	}
	if st.QueueHighWater != 3 {
		t.Errorf("QueueHighWater = %d, want 3 (it must not shrink)", st.QueueHighWater)
	}
	// FrameIDs are never reset by a queue operation: the next stamp continues.
	if id := a.Stamp(); id != 4 {
		t.Errorf("Stamp() after drain = %d, want 4", id)
	}
}

// TestAccountingDropHookAndCounters proves both causes land in the right
// counter, both reach the hook with the frame's provenance, and a nil hook
// leaves the counters intact.
func TestAccountingDropHookAndCounters(t *testing.T) {
	var a Accounting
	a.SetQueueCap(2)

	var mu sync.Mutex
	var got []connector.DroppedFrame
	a.SetOnDrop(func(df connector.DroppedFrame) {
		mu.Lock()
		got = append(got, df)
		mu.Unlock()
	})

	at := time.Unix(1700000000, 0)
	a.NoteDrop(7, 3, at, connector.DropQueueFull)
	a.NoteDrop(8, 3, at, connector.DropDrainedOnDisconnect)

	st := a.Snapshot()
	if st.FramesDropped != 1 || st.FramesDrained != 1 {
		t.Errorf("FramesDropped=%d FramesDrained=%d, want 1/1", st.FramesDropped, st.FramesDrained)
	}
	if len(got) != 2 {
		t.Fatalf("hook calls = %d, want 2", len(got))
	}
	if got[0].FrameID != 7 || got[0].Cause != connector.DropQueueFull || !got[0].At.Equal(at) || got[0].ConnID != 3 {
		t.Errorf("first hook call = %+v", got[0])
	}
	if got[1].Cause != connector.DropDrainedOnDisconnect {
		t.Errorf("second hook cause = %q", got[1].Cause)
	}

	// Clearing the hook stops the calls but not the counting.
	a.SetOnDrop(nil)
	a.NoteDrop(9, 3, at, connector.DropQueueFull)
	if len(got) != 2 {
		t.Errorf("hook called after being cleared: %d calls", len(got))
	}
	if st := a.Snapshot(); st.FramesDropped != 2 {
		t.Errorf("FramesDropped after a drop with no hook = %d, want 2", st.FramesDropped)
	}
}

// TestAccountingConnectionGeneration covers the generation and disconnect
// counters, which adapters advance on connect/disconnect.
func TestAccountingConnectionGeneration(t *testing.T) {
	var a Accounting
	if a.ConnID() != 0 {
		t.Errorf("ConnID before any connect = %d, want 0", a.ConnID())
	}
	a.SetConnID(1)
	a.NoteDisconnect()
	a.SetConnID(2)
	st := a.Snapshot()
	if st.ConnID != 2 || st.Disconnects != 1 {
		t.Errorf("ConnID=%d Disconnects=%d, want 2/1", st.ConnID, st.Disconnects)
	}
	if st.QueueCap != 0 {
		t.Errorf("QueueCap without SetQueueCap = %d, want 0", st.QueueCap)
	}
}

// TestAccountingConcurrentStamp proves the ID source and the read counter stay
// in lockstep under concurrent readers (one goroutine per connection, but the
// primitive must not assume it).
func TestAccountingConcurrentStamp(t *testing.T) {
	var a Accounting
	const goroutines = 4
	const perG = 250
	var wg sync.WaitGroup
	ids := make(chan uint64, goroutines*perG)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ids <- a.Stamp()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, goroutines*perG)
	for id := range ids {
		if seen[id] {
			t.Fatalf("FrameID %d handed out twice", id)
		}
		seen[id] = true
	}
	if len(seen) != goroutines*perG {
		t.Errorf("distinct IDs = %d, want %d", len(seen), goroutines*perG)
	}
	if st := a.Snapshot(); st.FramesRead != goroutines*perG {
		t.Errorf("FramesRead = %d, want %d", st.FramesRead, goroutines*perG)
	}
}
