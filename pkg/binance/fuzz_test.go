package binance

// fuzz_test.go feeds arbitrary bytes into the real market-frame decoder
// (WSBinance.processMessage) with a fake dispatcher and a loopback REST depth
// endpoint. Properties asserted per input:
//
//	no panic, returns within 2 s, goroutines settle back to the pre-input
//	count, and TotalAlloc delta ≤ 64×len(input) + 64 KiB.
//
// The depth handler can start one REST fetch goroutine per input (production
// behaviour: an unsnapshotted or gapped book re-requests a snapshot); it is
// served by a loopback httptest server, and the goroutine check allows it to
// finish (net growth must be zero) rather than forbidding it.
//
// Seeds in testdata/fuzz/FuzzProcessMessage/ are the frame shapes the existing
// tests record for every stream class.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func fuzzBinance() (*WSBinance, *binanceConn) {
	base := connector.New(false, nil)
	base.SetDispatcher(func(any) {})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"lastUpdateId":1,"bids":[["1.0","1"]],"asks":[["2.0","1"]]}`)
	}))
	// No t.Cleanup available outside a test: the server lives for the process,
	// which is what a fuzz target's harness needs anyway.
	b := NewWithOptions(base, Options{
		PerpMarketWS:   "ws://127.0.0.1:1/never-dialled",
		PerpPublicWS:   "ws://127.0.0.1:1/never-dialled",
		PerpREST:       srv.URL,
		EventQueueSize: 64,
	})
	c := b.connFor(MarketPerp, classPublic)
	c.addStreams([]string{
		"btcusdt@depth@100ms",
		"btcusdt@depth20@100ms",
		"btcusdt@bookTicker",
		"btcusdt@aggTrade",
		"btcusdt@kline_1m",
		"btcusdt@markPrice@1s",
	})
	b.rootCx = context.Background()
	return b, c
}

func FuzzProcessMessage(f *testing.F) {
	b, c := fuzzBinance()
	for _, seed := range []string{
		`{"e":"bookTicker","u":1,"E":1700000000000,"T":1700000000001,"s":"BTCUSDT","b":"1.0","B":"1","a":"2.0","A":"1"}`,
		`{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","p":"1.0","q":"1","T":1700000000001,"m":true,"a":1,"f":1,"l":1}`,
		`{"e":"kline","E":1700000000000,"s":"BTCUSDT","k":{"t":1,"T":2,"s":"BTCUSDT","i":"1m","o":"1","c":"2","h":"3","l":"0.5","v":"10","n":5,"x":false}}`,
		`{"e":"markPriceUpdate","E":1700000000000,"s":"BTCUSDT","p":"1.0","i":"2.0","P":"1.0","r":"0.0001","T":1700000000001}`,
		`{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":2,"u":3,"b":[["1.0","5"]],"a":[]}`,
		`{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[["2.0","0"]]}`,
		`{"e":"depthUpdate","s":"BTCUSDT","U":9,"u":9,"b":[["1.0","9"]],"a":[]}`,
		`{"id":1,"result":null}`,
		`{"id":2,"error":{"code":-1121,"msg":"Invalid symbol."}}`,
		`{"b":"1.0","B":"1","a":"2.0","A":"1"}`, // spot bookTicker: no event type
		`not json`,
		`[]`,
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
			b.processMessage(c, binanceFrame{data: data, id: 1, connID: 1, rx: time.Now()})
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
		deadline := time.Now().Add(2 * time.Second)
		for runtime.NumGoroutine() > baseline {
			if time.Now().After(deadline) {
				t.Fatalf("goroutines grew from %d to %d for one input", baseline, runtime.NumGoroutine())
			}
			time.Sleep(time.Millisecond)
		}
	})
}
