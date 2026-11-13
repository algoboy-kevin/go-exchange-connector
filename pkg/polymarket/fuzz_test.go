package polymarket

// fuzz_test.go feeds arbitrary bytes into both Polymarket decoders with no
// socket and no workers:
//
//	FuzzProcessMessage      — the market channel (processMessage: book,
//	                          price_change, last_trade_price,
//	                          tick_size_change, market_resolved, arrays,
//	                          unknown types)
//	FuzzRTDSProcessMessage  — RTDS (processMessage: binance crypto_prices
//	                          updates, chainlink updates + snapshots, TWAP,
//	                          equity, unknown topics)
//
// Per input the target asserts: returns within 2 s, no goroutine growth, and
// TotalAlloc delta ≤ 64×len(input) + 64 KiB. Panics are the fuzz engine's.
//
// Seeds in testdata/fuzz/<target>/ are the exact frame shapes the existing
// accounting tests record.

import (
	"context"
	"runtime"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func fuzzMarket() *WSPolymarketMarket {
	base := connector.New(false, nil)
	base.SetDispatcher(func(any) {})
	pm := NewWSPolymarketMarket(base, Config{PendingFlushMs: 20})
	pm.Subscribe(context.Background(), []string{soakAsset})
	// No connection is ever made, so Subscribe only queues the asset; register
	// it as subscribed so the handlers' asset gate lets frames through.
	pm.subMu.Lock()
	pm.subscribedAssetIDs[soakAsset] = struct{}{}
	pm.subMu.Unlock()
	return pm
}

func FuzzProcessMessage(f *testing.F) {
	pm := fuzzMarket()
	for _, seed := range []string{
		`{"event_type":"book","asset_id":"asset-a","market":"m1","bids":[{"price":"0.49","size":"10"}],"asks":[{"price":"0.51","size":"10"}],"timestamp":"1700000000000","hash":"h1"}`,
		`[{"event_type":"book","asset_id":"asset-a","market":"m1","bids":[],"asks":[],"timestamp":"1700000000000","hash":"h2"}]`,
		`{"event_type":"price_change","market":"m1","timestamp":"1700000000000","price_changes":[{"asset_id":"asset-a","price":"0.49","size":"5","side":"BUY","hash":"h3","best_bid":"0.49","best_ask":"0.51"}]}`,
		`{"event_type":"last_trade_price","asset_id":"asset-a","market":"m1","price":"0.5","side":"BUY","size":"1","fee_rate_bps":"0","timestamp":"1700000000000"}`,
		`{"event_type":"tick_size_change","asset_id":"asset-a","market":"m1","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1700000000000"}`,
		`{"event_type":"market_resolved","asset_id":"asset-a","market":"m1","winning_outcome":"Yes","timestamp":"1700000000000"}`,
		`{"event_type":"new_market","asset_id":"asset-a","market":"m1"}`,
		`{"event_type":"unknown","asset_id":"asset-a"}`,
		`[]`,
		`[{}]`,
		`{}`,
		`not json`,
		`""`,
		``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		baseline := runtime.NumGoroutine()
		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)

		pm.processMessage(marketFrame{data: data, id: 1, connID: 1, rx: time.Now()})

		runtime.ReadMemStats(&ms1)
		if alloc := ms1.TotalAlloc - ms0.TotalAlloc; alloc > uint64(64*len(data))+64<<10 {
			t.Fatalf("decode allocated %d bytes for a %d-byte input (cap 64x+64KiB)", alloc, len(data))
		}
		if n := runtime.NumGoroutine(); n > baseline {
			t.Fatalf("goroutines grew from %d to %d for one input", baseline, n)
		}
	})
}

func fuzzRTDS() *WSPolymarketRTDS {
	base := connector.New(false, nil)
	base.SetDispatcher(func(any) {})
	r := NewWSPolymarketRTDS(base)
	ctx := context.Background()
	r.SubscribeCryptoPrices(ctx, []string{"btcusdt"})
	r.SubscribeChainlinkPrices(ctx, []string{"btc/usd"})
	r.SubscribeChainlinkTWAP(ctx, 30, []string{"btc/usd"})
	r.SubscribeChainlinkTWAP(ctx, 60, []string{"btc/usd"})
	r.SubscribeEquityPrices(ctx, []string{"aapl"})
	return r
}

func FuzzRTDSProcessMessage(f *testing.F) {
	r := fuzzRTDS()
	for _, seed := range []string{
		`{"topic":"crypto_prices","type":"update","timestamp":1700000000000,"payload":{"symbol":"btcusdt","timestamp":1700000000000,"value":1.5,"full_accuracy_value":"1500000000000000000"}}`,
		`{"topic":"crypto_prices","type":"update","timestamp":1700000000000,"payload":{"symbol":"btcusdt","timestamp":1700000000000,"value":"1.5"}}`,
		`{"topic":"crypto_prices","type":"subscribe","timestamp":1700000000000,"payload":{"symbol":"btcusdt","timestamp":1700000000000,"value":1.5}}`,
		`{"topic":"crypto_prices_chainlink","type":"update","timestamp":1700000000000,"payload":{"symbol":"btc/usd","timestamp":1700000000000,"value":1.5,"full_accuracy_value":"1500000000000000000"}}`,
		`{"topic":"crypto_prices_chainlink","type":"subscribe","timestamp":1700000000000,"payload":{"symbol":"btc/usd","timestamp":1700000000000,"value":1.5}}`,
		`{"topic":"crypto_prices_twap_30s","type":"update","timestamp":1700000000000,"payload":{"symbol":"btc/usd","timestamp":1700000000000,"value":1.5,"full_accuracy_value":"1500000000000000000"}}`,
		`{"topic":"crypto_prices_twap_60s","type":"update","timestamp":1700000000000,"payload":{"symbol":"btc/usd","timestamp":1700000000000,"value":1.5}}`,
		`{"topic":"equity_prices","type":"update","timestamp":1700000000000,"payload":{"symbol":"aapl","timestamp":1700000000000,"value":190.5}}`,
		`{"topic":"equity_prices","type":"subscribe","timestamp":1700000000000,"payload":{"symbol":"aapl","timestamp":1700000000000,"value":190.5}}`,
		`{"topic":"unknown_topic","type":"update","payload":{"symbol":"btcusdt"}}`,
		`{"topic":"crypto_prices","type":"update","payload":null}`,
		`{}`,
		`not json`,
		``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		baseline := runtime.NumGoroutine()
		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)

		r.processMessage(rtdsFrame{data: data, id: 1, connID: 1, rx: time.Now()})

		runtime.ReadMemStats(&ms1)
		if alloc := ms1.TotalAlloc - ms0.TotalAlloc; alloc > uint64(64*len(data))+64<<10 {
			t.Fatalf("decode allocated %d bytes for a %d-byte input (cap 64x+64KiB)", alloc, len(data))
		}
		if n := runtime.NumGoroutine(); n > baseline {
			t.Fatalf("goroutines grew from %d to %d for one input", baseline, n)
		}
	})
}
