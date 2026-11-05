package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
)

type failActionTransport struct{ fail atomic.Bool }

func (rt *failActionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/exchange" && rt.fail.Swap(false) {
		return nil, io.ErrUnexpectedEOF
	}
	return http.DefaultTransport.RoundTrip(r)
}
func TestHIP3TransportFailureConsumesActualSignedNonce(t *testing.T) {
	f, local := newDexFixture(t)
	rt := &failActionTransport{}
	c, err := New(context.Background(), Config{BaseURL: local, AgentKey: testAgentKey, Account: testAccount, HTTP: &http.Client{Transport: rt}, PerpDexs: []string{"xyz"}})
	if err != nil {
		t.Fatal(err)
	}
	const seed = int64(4102444800000)
	c.signer.last = seed
	rt.fail.Store(true)
	_, err = c.Place(context.Background(), []Order{{Coin: "xyz:BTC", Px: "100", Sz: "1", Tif: TifAlo}})
	if !IsUncertain(err) || c.signer.last != seed+1 {
		t.Fatalf("failed action err=%v last=%d", err, c.signer.last)
	}
	if _, err := c.Place(context.Background(), []Order{{Coin: "BTC", Px: "100", Sz: "1", Tif: TifAlo}}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Fatalf("transport requests=%d", len(f.requests))
	}
	var e struct {
		Nonce int64 `json:"nonce"`
	}
	if err := json.Unmarshal(f.requests[0], &e); err != nil {
		t.Fatal(err)
	}
	if e.Nonce != seed+2 {
		t.Fatalf("failed HIP-3 request nonce reused by core: %d", e.Nonce)
	}
}

type localRedirectTransport struct{ target *url.URL }

func (rt localRedirectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	cp := r.Clone(r.Context())
	u := *r.URL
	u.Scheme = rt.target.Scheme
	u.Host = rt.target.Host
	cp.URL = &u
	cp.Host = rt.target.Host
	return http.DefaultTransport.RoundTrip(cp)
}

// Exact SDK URL comparison is retained even when tests redirect the transport
// to localhost. No request in this test can reach an external host.
func TestHIP3SigningDomainsAndBothHTTPClients(t *testing.T) {
	for _, mainnet := range []bool{false, true} {
		t.Run(fmt.Sprintf("mainnet=%v", mainnet), func(t *testing.T) {
			f, local := newDexFixture(t)
			target, err := url.Parse(local)
			if err != nil {
				t.Fatal(err)
			}
			base := hl.TestnetAPIURL
			if mainnet {
				base = hl.MainnetAPIURL
			}
			hc := &http.Client{Transport: localRedirectTransport{target: target}}
			c, err := New(context.Background(), Config{BaseURL: base, AgentKey: testAgentKey, Account: testAccount, HTTP: hc, PerpDexs: []string{"xyz"}})
			if err != nil {
				t.Fatal(err)
			}
			if c.http.Timeout <= 0 || hc.Timeout != 0 {
				t.Fatal("timeout absent or mutated caller's client")
			}
			ex, err := c.scope("xyz")
			if err != nil {
				t.Fatal(err)
			}
			// Also exercise SDK Info's separate transport, not just /exchange.
			if _, err := ex.Info().PerpDexs(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.signer.last = 4102444800000
			if _, err := c.Place(context.Background(), []Order{{Coin: "xyz:TSLA", Px: "100", Sz: "1", Tif: TifAlo}}); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			raw := append([]byte(nil), f.requests[0]...)
			f.mu.Unlock()
			var e struct {
				Action    hl.OrderAction     `json:"action"`
				Nonce     int64              `json:"nonce"`
				Signature hl.SignatureResult `json:"signature"`
			}
			if err := json.Unmarshal(raw, &e); err != nil {
				t.Fatal(err)
			}
			// Known deterministic HIP-3 fixture signatures, generated with the
			// pinned v0.46.1 SDK for asset130001, price100, size1, Alo,
			// nonce4102444800001 and the public test key. Pin both domains.
			golden := hl.SignatureResult{R: "0xab3e58fd20b3fce7f00d27dcb99761c1ec492881eaee2c919d02c4febfc2361", S: "0x6b9a641a3dbad0dfe2d94a862a4d5320af7ee32ac341a42a5ac0778b2cb9707c", V: 27}
			if mainnet {
				golden = hl.SignatureResult{R: "0x8d1045a7996c35e2e2b8866655e44ec5ba90cd6d932e2c1d2e62f0e4ece5e4f5", S: "0x11dac1732f69e478a8d15c49531c20efe2f80141cb712761aef2aaecb506a16d", V: 28}
			}
			if e.Nonce != 4102444800001 || e.Signature != golden {
				t.Fatalf("HIP-3 signature differs from known fixture: %+v", e)
			}
			want, err := hl.SignL1Action(c.key, e.Action, "", e.Nonce, nil, mainnet)
			if err != nil || want != e.Signature {
				t.Fatalf("domain signature got %+v want %+v err=%v", e.Signature, want, err)
			}
		})
	}
}

func TestHIP3RegistrationAndSDKInfoHonorTimeoutCancellation(t *testing.T) {
	var block atomic.Bool
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q map[string]string
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			t.Error(err)
			return
		}
		if block.Load() {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		if q["type"] == "perpDexs" {
			fmt.Fprint(w, `[null,{"name":"xyz"}]`)
			return
		}
		if q["dex"] == "xyz" {
			fmt.Fprint(w, `{"universe":[{"name":"xyz:BTC","szDecimals":3}]}`)
			return
		}
		fmt.Fprint(w, metaFixture)
	}))
	defer srv.Close()
	defer close(release)
	c, err := New(context.Background(), Config{BaseURL: srv.URL, AgentKey: testAgentKey, Account: testAccount, HTTP: &http.Client{Timeout: 30 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	block.Store(true)
	if err := c.RegisterDex(context.Background(), "xyz"); err == nil || !IsUncertain(err) {
		t.Fatalf("bounded registration=%v", err)
	}
	if _, err := c.route("xyz:BTC"); err == nil {
		t.Fatal("timeout published route")
	}
	block.Store(false)
	registerTestDex(t, c, "xyz")
	ex, _ := c.scope("xyz")
	block.Store(true)
	if _, err := ex.Info().PerpDexs(context.Background()); err == nil {
		t.Fatal("SDK info ignored bounded client")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.RegisterDex(ctx, "io"); !errors.Is(err, context.Canceled) {
		t.Fatalf("registration cancellation=%v", err)
	}
	if _, err := c.Positions(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("query cancellation=%v", err)
	}
	// Waiting on either serialized gate is context aware, with no sleeping probe.
	c.registerGate <- struct{}{}
	if err := c.RegisterDex(ctx, "io"); !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
	<-c.registerGate
	c.actionGate <- struct{}{}
	if err := c.ScheduleCancel(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Error(err)
	}
	<-c.actionGate
}

func TestHIP3MalformedRegistryAndMetadataNeverPublish(t *testing.T) {
	for _, tc := range []struct{ name, registry, meta string }{
		{name: "unknown", registry: `[null,{"name":"io"}]`},
		{name: "duplicate registry", registry: `[null,{"name":"xyz"},{"name":"xyz"}]`},
		{name: "malformed registry", registry: `[null,5,{"name":"xyz"}]`},
		{name: "null registry", registry: `null`},
		{name: "core registry index", registry: `[{"name":"xyz"}]`},
		{name: "unqualified", meta: `{"universe":[{"name":"BTC","szDecimals":3}]}`},
		{name: "wrong scope", meta: `{"universe":[{"name":"io:BTC","szDecimals":3}]}`},
		{name: "duplicate coin", meta: `{"universe":[{"name":"xyz:BTC","szDecimals":3},{"name":"xyz:BTC","szDecimals":4}]}`},
		{name: "bad decimals", meta: `{"universe":[{"name":"xyz:BTC","szDecimals":7}]}`},
		{name: "null decimals", meta: `{"universe":[{"name":"xyz:BTC","szDecimals":null}]}`},
		{name: "empty", meta: `{"universe":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, local := newDexFixture(t)
			c := newTestClient(t, local)
			f.mu.Lock()
			if tc.registry != "" {
				f.queryRaw = map[string]string{"perpDexs": tc.registry}
			}
			if tc.meta != "" {
				f.meta["xyz"] = tc.meta
			}
			f.mu.Unlock()
			err := c.RegisterDex(context.Background(), "xyz")
			var se *ScopeError
			if !errors.As(err, &se) {
				t.Fatalf("expected typed ScopeError, got %T %v", err, err)
			}
			if _, err := c.route("xyz:BTC"); err == nil {
				t.Fatal("malformed scope published")
			}
		})
	}
}
