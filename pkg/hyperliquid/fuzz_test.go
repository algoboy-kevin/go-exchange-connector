package hyperliquid

// fuzz_test.go feeds arbitrary bytes into the real frame decoder
// (WSHyperliquid.processFrame) against a fake dispatcher. Properties asserted
// for every input:
//
//	no panic                     — the fuzz engine's contract, implicit
//	bounded time (2 s)           — the call runs under a per-input watchdog
//	no goroutine leak            — count settles back to the pre-input baseline
//	bounded allocations          — TotalAlloc delta ≤ 64×len(input) + 64 KiB
//
// The 64×+64 KiB cap is ~2 orders of magnitude above what encoding/json needs
// for these frame shapes (a 1 KiB frame decodes in ~5 KiB of allocations); the
// constant absorbs the runtime's own per-call noise. A change that, say,
// buffered the whole frame per event or pre-allocated a map per level would
// blow through it.
//
// Seeds live in testdata/fuzz/<FuzzName>/ and are the frames the existing
// tests record for every channel, including the error and
// subscriptionResponse envelopes.

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// fuzzHarness builds a manager that has every channel subscribed (so the
// handlers' "is this coin mine" gates let frames through) and a dispatcher that
// throws events away. It never dials.
func fuzzHarness() *WSHyperliquid {
	base := connector.New(false, nil)
	base.SetDispatcher(func(any) {})
	opts := DefaultOptions()
	opts.AppPingIntervalMs = -1
	opts.WSPingIntervalMs = -1
	h := NewWithOptions(base, opts)
	ctx := context.Background()
	for _, err := range []error{
		h.SubscribeL2Book(ctx, []string{"BTC"}, SubParams{}),
		h.SubscribeTrades(ctx, []string{"BTC"}),
		h.SubscribeBBO(ctx, []string{"BTC"}),
		h.SubscribeActiveAssetCtx(ctx, []string{"BTC"}),
		h.SubscribeAllMids(ctx),
		h.SubscribeCandles(ctx, []string{"BTC"}, "1m"),
		h.SubscribeOrderUpdates(ctx, testUser),
		h.SubscribeUserFills(ctx, testUser),
	} {
		if err != nil {
			panic(fmt.Sprintf("fuzz harness subscription: %v", err))
		}
	}
	return h
}

func FuzzProcessFrame(f *testing.F) {
	h := fuzzHarness()
	for _, seed := range []string{
		`{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"1","sz":"1","n":1}],[]]}}`,
		`{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1}]}`,
		`{"channel":"bbo","data":{"coin":"BTC","time":1,"bbo":[{"px":"1","sz":"1","n":1},null]}}`,
		`{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.00001","oraclePx":"1","openInterest":"1"}}}`,
		`{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"circulatingSupply":"1","dayNtlVlm":"1","markPx":"1","midPx":"1","prevDayPx":"1"}}}`,
		`{"channel":"allMids","data":{"mids":{"BTC":"1"}}}`,
		`{"channel":"candle","data":[{"t":1,"T":2,"s":"BTC","i":"1m","o":"1","c":"1","h":"1","l":"1","v":"1","n":1}]}`,
		`{"channel":"orderUpdates","data":[{"order":{"coin":"BTC","side":"B","limitPx":"1","sz":"1","oid":1,"timestamp":1,"origSz":"1"},"status":"open","statusTimestamp":1}]}`,
		`{"channel":"userFills","data":{"user":"` + testUser + `","fills":[{"coin":"BTC","px":"1","sz":"1","side":"B","dir":"Open Long","closedPnl":"0","fee":"0","startPosition":"0","time":1,"oid":1,"tid":1}]}}`,
		`{"channel":"error","data":"Invalid subscription"}`,
		`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}}`,
		`{"channel":"pong"}`,
		`{"channel":"unknown","data":{"x":1}}`,
		`not json at all`,
		`{"channel":"l2Book","data":{"coin":"BTC","levels":"not-an-array"}}`,
		`{}`,
		``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		baseline := runtime.NumGoroutine()
		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.processFrame(frame{data: data, id: 1, connID: 1, rx: time.Now()})
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("decode did not return within 2s for %d bytes", len(data))
		}

		runtime.ReadMemStats(&ms1)
		if alloc := ms1.TotalAlloc - ms0.TotalAlloc; alloc > uint64(64*len(data))+64<<10 {
			t.Fatalf("decode allocated %d bytes for a %d-byte input (cap 64x+64KiB)", alloc, len(data))
		}
		// Give a short-lived goroutine, if the decoder started one, time to
		// exit before declaring a leak.
		deadline := time.Now().Add(time.Second)
		for runtime.NumGoroutine() > baseline {
			if time.Now().After(deadline) {
				t.Fatalf("goroutines grew from %d to %d for one input", baseline, runtime.NumGoroutine())
			}
			time.Sleep(time.Millisecond)
		}
	})
}
