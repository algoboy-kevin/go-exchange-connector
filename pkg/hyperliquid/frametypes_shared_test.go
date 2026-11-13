package hyperliquid

import (
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// The Hyperliquid adapter reports frame accounting in the connector's shared
// types, so one drop hook and one Stats reader serve every venue. These are
// compile-time guards: with an hl-local Stats/DroppedFrame they do not build.
var (
	_ func(*WSHyperliquid) connector.FrameStats          = (*WSHyperliquid).Stats
	_ func(*WSHyperliquid, func(connector.DroppedFrame)) = (*WSHyperliquid).SetOnFrameDropped
)

func TestFrameAccountingSpeaksSharedConnectorTypes(t *testing.T) {
	var fa frameAccounting
	fa.queueCap = 4

	var got connector.DroppedFrame
	fa.setOnDrop(func(d connector.DroppedFrame) { got = d })

	rx := time.Unix(1700000000, 0)
	fa.noteDrop(frame{id: 5, connID: 7, rx: rx}, connector.DropQueueFull)
	fa.noteDrop(frame{id: 6, connID: 7, rx: rx}, connector.DropDrainedOnDisconnect)

	var st connector.FrameStats = fa.snapshot(7)
	if st.ConnID != 7 || st.QueueCap != 4 || st.FramesDropped != 1 || st.FramesDrained != 1 {
		t.Fatalf("snapshot = %+v, want ConnID 7 / QueueCap 4 / dropped 1 / drained 1", st)
	}
	if got.FrameID != 6 || got.ConnID != 7 || !got.At.Equal(rx) || got.Cause != connector.DropDrainedOnDisconnect {
		t.Fatalf("last DroppedFrame = %+v, want frame 6 / conn 7 / At rx / cause %q", got, connector.DropDrainedOnDisconnect)
	}
	if string(connector.DropQueueFull) != "queue_full" || string(connector.DropDrainedOnDisconnect) != "drained_on_disconnect" {
		t.Fatalf("shared DropCause values changed: %q %q", connector.DropQueueFull, connector.DropDrainedOnDisconnect)
	}
}
