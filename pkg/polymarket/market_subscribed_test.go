package polymarket

// market_subscribed_test.go is the H4c D2 guard: frames already read from the
// socket must not be silently discarded at the asset gate while a reconnect is
// in flight. onDisconnect moves every subscribed asset into pendingSubscribeIDs
// and empties subscribedAssetIDs; a queued frame processed inside that window
// used to be treated as "an asset I do not hold" and dropped with no event and
// no counter. isSubscribed now also accepts an asset that is awaiting
// re-subscribe (pendingSubscribe and not pendingUnsubscribe).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func marketPriceChangeFrame(asset string, n int64) string {
	return fmt.Sprintf(`[{"event_type":"price_change","market":"m1","timestamp":"1700000000000","price_changes":[{"asset_id":%q,"price":"0.49","size":"%d","side":"BUY","hash":"h%d","best_bid":"0.49","best_ask":"0.51"}]}]`, asset, n, n)
}

func marketBookFrame(asset string, n int64) string {
	return fmt.Sprintf(`[{"event_type":"book","market":"m1","asset_id":%q,"timestamp":"1700000000000","hash":"b%d","bids":[{"price":"0.49","size":"%d"}],"asks":[{"price":"0.51","size":"5"}]}]`, asset, n, n)
}

func marketTradeFrame(asset string, n int64) string {
	return fmt.Sprintf(`[{"event_type":"last_trade_price","market":"m1","asset_id":%q,"timestamp":"1700000000000","price":"0.49","size":"%d","side":"BUY"}]`, asset, n)
}

// TestMarketDeliversQueuedFramesAcrossDisconnect drives a real drop with a
// blocked dispatcher and a reconnect interval far longer than the assertions, so
// every queued frame is handled strictly inside the disconnect window.
func TestMarketDeliversQueuedFramesAcrossDisconnect(t *testing.T) {
	const (
		subscribed    = "asset-sub"
		unsubscribed  = "asset-unsub"
		neverSeen     = "asset-never"
		queueSize     = 64
		framesPerKind = 6
	)
	v := newMarketVenue(t)

	var mu sync.Mutex
	byAsset := map[string]int{}
	books := map[string]int{}
	trades := map[string]int{}
	var delivered int
	gate := make(chan struct{})
	var holdOnce sync.Once
	d := func(ev any) {
		holdOnce.Do(func() { <-gate }) // hold the first frame until the test releases the dispatcher
		mu.Lock()
		delivered++
		switch e := ev.(type) {
		case *connector.PriceChangeEvent:
			for _, c := range e.Changes {
				byAsset[c.AssetID]++
			}
		case *connector.BookSnapshotEvent:
			books[e.AssetID]++
		case *connector.TradeEvent:
			trades[e.AssetID]++
		}
		mu.Unlock()
	}

	base := connector.New(false, nil)
	base.SetDispatcher(d)
	pm := NewWSPolymarketMarket(base, Config{DispatcherWorkers: 1, PendingFlushMs: 20})
	pm.SetEventQueueSize(queueSize)

	disconnected := make(chan error, 1)
	pm.SetOnDisconnect(func(err error) {
		select {
		case disconnected <- err:
		default:
		}
	})

	// asset-unsub is subscribed and then explicitly removed before the drop, so
	// its frames must stay filtered; asset-never was never subscribed at all.
	pm.Subscribe(context.Background(), []string{subscribed, unsubscribed})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// A reconnect interval far longer than the assertions below: the window in
	// which subscribedAssetIDs is empty stays open for the whole test body.
	if err := pm.Start(ctx, v.url, 5000); err != nil {
		t.Fatalf("market Start: %v", err)
	}
	t.Cleanup(pm.Stop)

	waitSoak(t, "the first handshake", 5*time.Second, func() bool {
		return len(v.handshakes()) >= 1
	})
	pm.Unsubscribe(context.Background(), []string{unsubscribed})
	waitMarketNoAsset(t, 5*time.Second, pm, unsubscribed)

	var seq int64
	write := func(frame string) {
		seq++
		v.write(t, 0, frame)
	}
	for i := 0; i < framesPerKind; i++ {
		write(marketPriceChangeFrame(subscribed, seq+1))
		write(marketBookFrame(subscribed, seq+1))
		write(marketTradeFrame(subscribed, seq+1))
		write(marketPriceChangeFrame(unsubscribed, seq+1))
		write(marketPriceChangeFrame(neverSeen, seq+1))
	}
	// Five frames per round, three of them for the subscribed asset.
	wantReads := uint64(framesPerKind * 5)

	// Every frame must be read off the socket and queued before the drop; the
	// dispatcher is blocked on the first, so the rest wait in the queue.
	waitSoak(t, "all frames to be queued", 5*time.Second, func() bool {
		return pm.Stats().FramesRead >= wantReads
	})

	// Drop the socket. onDisconnect moves the subscribed asset to the pending
	// re-subscribe set and empties the subscribed set — the window under test.
	v.closeGen(0)
	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatalf("no disconnect was reported")
	}
	waitSoak(t, "the disconnect window to be open", 5*time.Second, func() bool {
		return !pm.hasSubscribedAsset(subscribed) && pm.isPendingResubscribe(subscribed) && v.numConns() == 1
	})

	close(gate) // release the dispatcher INSIDE the window
	waitSoak(t, "the queue to drain", 10*time.Second, func() bool {
		return pm.Stats().QueueLen == 0
	})
	// Give a late (wrong) dispatch a chance to show up before asserting.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	gotSub, gotBooks, gotTrades := byAsset[subscribed], books[subscribed], trades[subscribed]
	gotUnsub, gotNever := byAsset[unsubscribed], byAsset[neverSeen]
	n := delivered
	mu.Unlock()

	t.Logf("queued-frames: price_change[%s]=%d books=%d trades=%d foreign[%s]=%d foreign[%s]=%d events=%d reads=%d",
		subscribed, gotSub, gotBooks, gotTrades, unsubscribed, gotUnsub, neverSeen, gotNever, n, pm.Stats().FramesRead)

	if gotSub != framesPerKind {
		t.Fatalf("delivered %d/%d price_change events of the subscribed asset from the queue: frames of a subscribed asset were dropped inside the disconnect window",
			gotSub, framesPerKind)
	}
	if gotBooks != framesPerKind {
		t.Fatalf("delivered %d/%d book snapshots of the subscribed asset", gotBooks, framesPerKind)
	}
	if gotTrades != framesPerKind {
		t.Fatalf("delivered %d/%d last_trade_price events of the subscribed asset", gotTrades, framesPerKind)
	}
	if gotUnsub != 0 {
		t.Fatalf("explicitly unsubscribed asset %q produced %d price_change events, want 0", unsubscribed, gotUnsub)
	}
	if gotNever != 0 {
		t.Fatalf("never-subscribed asset %q produced %d price_change events, want 0", neverSeen, gotNever)
	}
	if n != framesPerKind*3 {
		t.Fatalf("delivered %d events, want exactly the %d subscribed-asset frames", n, framesPerKind*3)
	}
	// Everything was delivered before any reconnect: this really is the window.
	if conns := v.numConns(); conns != 1 {
		t.Fatalf("the connection was rebuilt (%d accepts) before the queued frames were dispatched: the window under test was not the one measured", conns)
	}
}

// hasSubscribedAsset reports whether assetID is in the live subscribed set.
func (pm *WSPolymarketMarket) hasSubscribedAsset(assetID string) bool {
	pm.subMu.RLock()
	defer pm.subMu.RUnlock()
	_, ok := pm.subscribedAssetIDs[assetID]
	return ok
}

// isPendingResubscribe reports whether assetID is queued for re-subscribe.
func (pm *WSPolymarketMarket) isPendingResubscribe(assetID string) bool {
	pm.pendingMu.Lock()
	defer pm.pendingMu.Unlock()
	_, ok := pm.pendingSubscribeIDs[assetID]
	return ok
}

func waitMarketNoAsset(t *testing.T, d time.Duration, pm *WSPolymarketMarket, assetID string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !pm.hasSubscribedAsset(assetID) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("asset %q was still subscribed after Unsubscribe", assetID)
}
