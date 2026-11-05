package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
)

// S32 mixed-DEX bulk action fixtures. Registry [null,xyz,io,para] gives the
// golden global ids: core = plain index, xyz = 11000x, io = 12000x, para =
// 13000x (100000 + registryIndex*10000 + indexInMeta).
const mixedRegistry = `[null,{"name":"xyz"},{"name":"io"},{"name":"para"}]`

var mixedMeta = map[string]string{
	"":     `{"universe":[{"name":"BTC","szDecimals":5},{"name":"ETH","szDecimals":4}],"marginTables":[]}`,
	"xyz":  `{"universe":[{"name":"xyz:A","szDecimals":1},{"name":"xyz:B","szDecimals":2}],"marginTables":[]}`,
	"io":   `{"universe":[{"name":"io:A","szDecimals":3},{"name":"io:B","szDecimals":4}],"marginTables":[]}`,
	"para": `{"universe":[{"name":"para:A","szDecimals":5},{"name":"para:B","szDecimals":6}],"marginTables":[]}`,
}

// 8 symbols over all four scopes, deliberately NOT grouped or sorted.
var (
	mixedCoins  = []string{"para:B", "BTC", "xyz:A", "io:B", "ETH", "xyz:B", "para:A", "io:A"}
	mixedAssets = []int{130001, 0, 110000, 120001, 1, 110001, 130000, 120000}
)

type mixedVenue struct {
	mu       sync.Mutex
	bodies   [][]byte
	handler  func(w http.ResponseWriter, r *http.Request, n int, body []byte)
	infoSeen []string
}

func newMixedVenue(t *testing.T) (*mixedVenue, string) {
	t.Helper()
	v := &mixedVenue{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/info":
			var q map[string]string
			_ = json.Unmarshal(body, &q)
			v.mu.Lock()
			v.infoSeen = append(v.infoSeen, q["type"]+":"+q["dex"])
			v.mu.Unlock()
			switch q["type"] {
			case "meta":
				fmt.Fprint(w, mixedMeta[q["dex"]])
			case "perpDexs":
				fmt.Fprint(w, mixedRegistry)
			default:
				http.Error(w, `{"error":"unexpected"}`, 400)
			}
		case "/exchange":
			v.mu.Lock()
			v.bodies = append(v.bodies, body)
			n := len(v.bodies)
			h := v.handler
			v.mu.Unlock()
			if h == nil {
				okExchange(w, body)
				return
			}
			h(w, r, n, body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return v, srv.URL
}

// okExchange answers any order/cancel action with per-item success. Orders
// rest with oid = asset+1000000 so each status is traceable to its asset.
func okExchange(w http.ResponseWriter, body []byte) {
	var e struct {
		Action struct {
			Type    string `json:"type"`
			Orders  []struct{ A int }
			Cancels []json.RawMessage
		} `json:"action"`
	}
	_ = json.Unmarshal(body, &e)
	switch e.Action.Type {
	case "order":
		sts := make([]string, len(e.Action.Orders))
		for i, o := range e.Action.Orders {
			sts[i] = fmt.Sprintf(`{"resting":{"oid":%d}}`, o.A+1000000)
		}
		fmt.Fprintf(w, `{"status":"ok","response":{"type":"order","data":{"statuses":[%s]}}}`, strings.Join(sts, ","))
	case "cancel", "cancelByCloid":
		sts := make([]string, len(e.Action.Cancels))
		for i := range sts {
			sts[i] = `"success"`
		}
		fmt.Fprintf(w, `{"status":"ok","response":{"type":"cancel","data":{"statuses":[%s]}}}`, strings.Join(sts, ","))
	default:
		fmt.Fprint(w, `{"status":"ok"}`)
	}
}

func (v *mixedVenue) requests() [][]byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([][]byte(nil), v.bodies...)
}

func newMixedClient(t *testing.T, url string, hc *http.Client, dexes ...string) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{BaseURL: url, AgentKey: testAgentKey, Account: testAccount, HTTP: hc, PerpDexs: dexes})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type wireAction struct {
	Nonce     int64              `json:"nonce"`
	Signature hl.SignatureResult `json:"signature"`
	Action    json.RawMessage    `json:"action"`
}

// decodeWire returns the action type and its per-item (asset, key) pairs, in
// wire order. key is the cloid (cancelByCloid), oid (cancel) or limit price.
func decodeWire(t *testing.T, raw []byte) (wireAction, string, []int, []string) {
	t.Helper()
	var w wireAction
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	var a struct {
		Type   string `json:"type"`
		Orders []struct {
			A int    `json:"a"`
			P string `json:"p"`
		} `json:"orders"`
		Cancels []struct {
			A     int    `json:"a"`
			Asset int    `json:"asset"`
			O     int64  `json:"o"`
			Cloid string `json:"cloid"`
		} `json:"cancels"`
	}
	if err := json.Unmarshal(w.Action, &a); err != nil {
		t.Fatal(err)
	}
	var assets []int
	var keys []string
	switch a.Type {
	case "order":
		for _, o := range a.Orders {
			assets = append(assets, o.A)
			keys = append(keys, o.P)
		}
	case "cancel":
		for _, c := range a.Cancels {
			assets = append(assets, c.A)
			keys = append(keys, fmt.Sprint(c.O))
		}
	case "cancelByCloid":
		for _, c := range a.Cancels {
			assets = append(assets, c.Asset)
			keys = append(keys, c.Cloid)
		}
	default:
		t.Fatalf("unexpected action type %q", a.Type)
	}
	return w, a.Type, assets, keys
}

func mixedOrders() []Order {
	out := make([]Order, len(mixedCoins))
	for i, c := range mixedCoins {
		out[i] = Order{Coin: c, IsBuy: i%2 == 0, Px: fmt.Sprintf("%d", 100+i), Sz: "1", Tif: TifAlo, Cloid: fmt.Sprintf("0x%032x", i+1)}
	}
	return out
}
func mixedCloidCancels() []CancelReq {
	out := make([]CancelReq, len(mixedCoins))
	for i, c := range mixedCoins {
		out[i] = CancelReq{Coin: c, Cloid: fmt.Sprintf("0x%032x", i+1)}
	}
	return out
}
func mixedOidCancels() []OidCancel {
	out := make([]OidCancel, len(mixedCoins))
	for i, c := range mixedCoins {
		out[i] = OidCancel{Coin: c, Oid: int64(500 + i)}
	}
	return out
}

func eqInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The core S32 result: ONE signed /exchange request per homogeneous call over
// core+io+para+xyz, every global asset id correct, wire and results in the
// caller's original order. (Before S32 this was 4 requests per call.)
func TestMixedDexBulkActionsAreOneRequestEach(t *testing.T) {
	v, url := newMixedVenue(t)
	c := newMixedClient(t, url, nil, "para", "xyz", "io")
	const seed = int64(4102444800000)
	c.signer.last = seed
	ctx := context.Background()

	placed, err := c.Place(ctx, mixedOrders())
	if err != nil {
		t.Fatal(err)
	}
	if got := len(v.requests()); got != 1 {
		t.Fatalf("Place used %d /exchange requests, want 1", got)
	}
	for i, st := range placed {
		if !st.Resting || st.Oid != int64(mixedAssets[i]+1000000) {
			t.Fatalf("result[%d]=%+v not aligned with input %s (asset %d)", i, st, mixedCoins[i], mixedAssets[i])
		}
	}
	cc, err := c.CancelByCloid(ctx, mixedCloidCancels())
	if err != nil || len(cc) != 8 {
		t.Fatalf("CancelByCloid=%+v err=%v", cc, err)
	}
	if got := len(v.requests()); got != 2 {
		t.Fatalf("after CancelByCloid %d requests, want 2", got)
	}
	co, err := c.CancelByOid(ctx, mixedOidCancels())
	if err != nil || len(co) != 8 {
		t.Fatalf("CancelByOid=%+v err=%v", co, err)
	}
	reqs := v.requests()
	if len(reqs) != 3 {
		t.Fatalf("total /exchange requests=%d want 3 (one per call)", len(reqs))
	}

	for i, raw := range reqs {
		w, typ, assets, keys := decodeWire(t, raw)
		if !eqInts(assets, mixedAssets) {
			t.Fatalf("%s assets=%v want original order %v", typ, assets, mixedAssets)
		}
		if w.Nonce != seed+int64(i)+1 {
			t.Fatalf("%s nonce=%d want %d", typ, w.Nonce, seed+int64(i)+1)
		}
		for j := range keys {
			switch typ {
			case "order":
				if keys[j] != fmt.Sprint(100+j) {
					t.Fatalf("order[%d] px=%s", j, keys[j])
				}
			case "cancelByCloid":
				if keys[j] != fmt.Sprintf("0x%032x", j+1) {
					t.Fatalf("cancelByCloid[%d] cloid=%s", j, keys[j])
				}
			case "cancel":
				if keys[j] != fmt.Sprint(500+j) {
					t.Fatalf("cancel[%d] oid=%s", j, keys[j])
				}
			}
		}
		// The SDK's own canonical signing path must verify the wire signature
		// (testnet domain: the overridden BaseURL is not the exact mainnet URL).
		var action any
		switch typ {
		case "order":
			action = &hl.OrderAction{}
		case "cancel":
			action = &hl.CancelAction{}
		case "cancelByCloid":
			action = &hl.CancelByCloidAction{}
		}
		if err := json.Unmarshal(w.Action, action); err != nil {
			t.Fatal(err)
		}
		want, err := hl.SignL1Action(c.key, action, "", w.Nonce, nil, false)
		if err != nil || want != w.Signature {
			t.Fatalf("%s signature mismatch: got %+v want %+v err=%v", typ, w.Signature, want, err)
		}
	}
}

func TestMixedDexPlaceItemAlignedStatuses(t *testing.T) {
	v, url := newMixedVenue(t)
	v.handler = func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
		fmt.Fprint(w, `{"status":"ok","response":{"type":"order","data":{"statuses":[`+
			`{"resting":{"oid":11}},{"error":"Insufficient margin"},{"filled":{"totalSz":"1.0","avgPx":"9.5","oid":12}},`+
			`{"error":"Post only order would have immediately matched"},{"resting":{"oid":13}},{"error":"Order has invalid size"},`+
			`{"resting":{"oid":14}},{"filled":{"totalSz":"2","avgPx":"7","oid":15}}]}}}`)
	}
	c := newMixedClient(t, url, nil, "xyz", "io", "para")
	got, err := c.Place(context.Background(), mixedOrders())
	if err != nil {
		t.Fatalf("per-item rejections must not be a call failure: %v", err)
	}
	if len(v.requests()) != 1 {
		t.Fatalf("requests=%d", len(v.requests()))
	}
	want := []OrderStatus{
		{Resting: true, Oid: 11}, {Error: "Insufficient margin"}, {Filled: true, Oid: 12, TotalSz: "1.0", AvgPx: "9.5"},
		{Error: "Post only order would have immediately matched"}, {Resting: true, Oid: 13}, {Error: "Order has invalid size"},
		{Resting: true, Oid: 14}, {Filled: true, Oid: 15, TotalSz: "2", AvgPx: "7"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("status[%d]=%+v want %+v", i, got[i], want[i])
		}
	}
}

func TestMixedDexCancelItemAlignedAndAlreadyGone(t *testing.T) {
	const gone = "Order was never placed, already canceled, or filled."
	for _, op := range []string{"cancelByCloid", "cancel"} {
		t.Run(op, func(t *testing.T) {
			v, url := newMixedVenue(t)
			v.handler = func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
				fmt.Fprintf(w, `{"status":"ok","response":{"type":"cancel","data":{"statuses":["success",{"error":%q},"success",{"error":"other"},"success","success",{"error":%q},"success"]}}}`, gone, gone)
			}
			c := newMixedClient(t, url, nil, "xyz", "io", "para")
			var got []CancelStatus
			var err error
			if op == "cancel" {
				got, err = c.CancelByOid(context.Background(), mixedOidCancels())
			} else {
				got, err = c.CancelByCloid(context.Background(), mixedCloidCancels())
			}
			if err != nil {
				t.Fatalf("per-item errors must not be a call failure: %v", err)
			}
			if len(v.requests()) != 1 {
				t.Fatalf("requests=%d", len(v.requests()))
			}
			wantErr := map[int]string{1: gone, 3: "other", 6: gone}
			for i, st := range got {
				if st.OK != (wantErr[i] == "") || st.Error != wantErr[i] {
					t.Fatalf("status[%d]=%+v want error %q", i, st, wantErr[i])
				}
			}
		})
	}
}

// Every failure shape of the single request: exactly one attempt, never a
// resend, with the existing classification.
func TestMixedDexSingleAttemptFailureClassification(t *testing.T) {
	type tc struct {
		name      string
		handler   func(release chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte)
		timeout   time.Duration
		uncertain bool
		limited   bool
		action    bool
	}
	cases := []tc{
		{name: "top-level err envelope", action: true, handler: func(chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
				fmt.Fprint(w, `{"status":"err","response":"Cannot place order: rejected"}`)
			}
		}},
		{name: "malformed 200 body", uncertain: true, handler: func(chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) { fmt.Fprint(w, `{"status":"ok","resp`) }
		}},
		{name: "truncated body", uncertain: true, handler: func(chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
				conn, buf, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 400\r\n\r\n{\"status\":\"ok\",\"response\":{\"type\":\"order\"")
				_ = buf.Flush()
				_ = conn.Close()
			}
		}},
		{name: "http 429", limited: true, handler: func(chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
				w.WriteHeader(429)
				fmt.Fprint(w, "Too Many Requests")
			}
		}},
		{name: "http 503", uncertain: true, handler: func(chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(w http.ResponseWriter, _ *http.Request, _ int, _ []byte) {
				w.WriteHeader(503)
				fmt.Fprint(w, "unavailable")
			}
		}},
		{name: "client timeout", uncertain: true, timeout: 60 * time.Millisecond, handler: func(release chan struct{}) func(http.ResponseWriter, *http.Request, int, []byte) {
			return func(_ http.ResponseWriter, r *http.Request, _ int, _ []byte) {
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}
		}},
	}
	ops := map[string]func(c *Client) error{
		"Place": func(c *Client) error { _, err := c.Place(context.Background(), mixedOrders()); return err },
		"CancelByCloid": func(c *Client) error {
			_, err := c.CancelByCloid(context.Background(), mixedCloidCancels())
			return err
		},
		"CancelByOid": func(c *Client) error { _, err := c.CancelByOid(context.Background(), mixedOidCancels()); return err },
	}
	for _, k := range cases {
		for name, op := range ops {
			t.Run(k.name+"/"+name, func(t *testing.T) {
				v, url := newMixedVenue(t)
				release := make(chan struct{})
				t.Cleanup(func() { close(release) })
				v.handler = k.handler(release)
				hc := &http.Client{}
				if k.timeout > 0 {
					hc.Timeout = k.timeout
				}
				c := newMixedClient(t, url, hc, "xyz", "io", "para")
				const seed = int64(4102444800000)
				c.signer.last = seed
				err := op(c)
				if err == nil {
					t.Fatal("expected a call failure")
				}
				if n := len(v.requests()); n != 1 {
					t.Fatalf("%d /exchange attempts, want exactly 1 (no resend)", n)
				}
				if IsUncertain(err) != k.uncertain {
					t.Fatalf("IsUncertain=%v want %v: %v", IsUncertain(err), k.uncertain, err)
				}
				if IsRateLimited(err) != k.limited {
					t.Fatalf("IsRateLimited=%v want %v: %v", IsRateLimited(err), k.limited, err)
				}
				var ae *ActionError
				if k.action && !errors.As(err, &ae) {
					t.Fatalf("want *ActionError, got %T %v", err, err)
				}
				var pe *PartialBatchError
				if errors.As(err, &pe) {
					t.Fatalf("single request must not report partial-prefix semantics: %v", err)
				}
				// Even a failed send consumed its signed nonce.
				if c.signer.last != seed+1 {
					t.Fatalf("nonce after failed send=%d want %d", c.signer.last, seed+1)
				}
			})
		}
	}
}

// Anything invalid anywhere in the batch (later item, other scope) means ZERO
// wire actions, for every bulk call.
func TestMixedDexInvalidLaterItemSendsNothing(t *testing.T) {
	v, url := newMixedVenue(t)
	c := newMixedClient(t, url, nil, "xyz", "io") // para deliberately NOT registered
	ctx := context.Background()
	good := func() []Order {
		return []Order{
			{Coin: "BTC", Px: "100", Sz: "1", Tif: TifAlo},
			{Coin: "xyz:A", Px: "100", Sz: "1", Tif: TifAlo},
			{Coin: "io:A", Px: "100", Sz: "1", Tif: TifAlo},
		}
	}
	mut := map[string]func(o []Order){
		"unregistered later scope": func(o []Order) { o[2] = Order{Coin: "para:A", Px: "1", Sz: "1", Tif: TifAlo} },
		"unknown later coin":       func(o []Order) { o[2].Coin = "io:NOPE" },
		"later bad decimal":        func(o []Order) { o[2].Px = "1.123456789" },
		"later zero size":          func(o []Order) { o[2].Sz = "0" },
		"later bad tif":            func(o []Order) { o[2].Tif = "Fok" },
		"later bad cloid":          func(o []Order) { o[2].Cloid = "0xnothex" },
		"later short cloid":        func(o []Order) { o[2].Cloid = "0x01" },
	}
	for name, m := range mut {
		o := good()
		m(o)
		if _, err := c.Place(ctx, o); err == nil {
			t.Fatalf("Place accepted %s", name)
		}
	}
	cloids := func() []CancelReq {
		return []CancelReq{{Coin: "BTC", Cloid: testCloid1}, {Coin: "xyz:A", Cloid: testCloid2}, {Coin: "io:A", Cloid: testCloid1}}
	}
	for name, m := range map[string]func(r []CancelReq){
		"scope": func(r []CancelReq) { r[2].Coin = "para:A" },
		"cloid": func(r []CancelReq) { r[2].Cloid = "0xzz" },
		"empty": func(r []CancelReq) { r[2].Cloid = "" },
		"coin":  func(r []CancelReq) { r[2].Coin = "xyz:ZZZ" },
	} {
		r := cloids()
		m(r)
		if _, err := c.CancelByCloid(ctx, r); err == nil {
			t.Fatalf("CancelByCloid accepted %s", name)
		}
	}
	oids := []OidCancel{{Coin: "BTC", Oid: 1}, {Coin: "xyz:A", Oid: 2}, {Coin: "para:A", Oid: 3}}
	if _, err := c.CancelByOid(ctx, oids); err == nil {
		t.Fatal("CancelByOid accepted unregistered later scope")
	}
	if n := len(v.requests()); n != 0 {
		t.Fatalf("invalid batches emitted %d /exchange requests, want 0", n)
	}
}

func TestMixedDexEmptyAndCoreOnly(t *testing.T) {
	v, url := newMixedVenue(t)
	c := newMixedClient(t, url, nil) // no HIP-3 scope at all
	ctx := context.Background()
	if o, err := c.Place(ctx, nil); err != nil || o != nil {
		t.Fatalf("empty Place=%v,%v", o, err)
	}
	if o, err := c.CancelByCloid(ctx, []CancelReq{}); err != nil || o != nil {
		t.Fatalf("empty CancelByCloid=%v,%v", o, err)
	}
	if o, err := c.CancelByOid(ctx, nil); err != nil || o != nil {
		t.Fatalf("empty CancelByOid=%v,%v", o, err)
	}
	if n := len(v.requests()); n != 0 {
		t.Fatalf("empty inputs sent %d requests", n)
	}
	got, err := c.Place(ctx, []Order{{Coin: "ETH", Px: "10", Sz: "1", Tif: TifAlo}, {Coin: "BTC", Px: "11", Sz: "1", Tif: TifGtc}})
	if err != nil || len(got) != 2 || got[0].Oid != 1000001 || got[1].Oid != 1000000 {
		t.Fatalf("core-only Place=%+v err=%v", got, err)
	}
	reqs := v.requests()
	if len(reqs) != 1 {
		t.Fatalf("core-only requests=%d", len(reqs))
	}
	if _, _, assets, _ := decodeWire(t, reqs[0]); !eqInts(assets, []int{1, 0}) {
		t.Fatalf("core assets=%v", assets)
	}
}

// 36 concurrent mixed bulk actions plus dead-man arming: unique, gap-free,
// account-wide nonces, one request per call. Channels, no sleeps.
func TestMixedDexConcurrentCallsKeepUniqueNonces(t *testing.T) {
	v, url := newMixedVenue(t)
	c := newMixedClient(t, url, nil, "xyz", "io", "para")
	const seed = int64(4102444800000)
	c.signer.last = seed
	const n = 36
	start := make(chan struct{})
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			<-start
			ctx := context.Background()
			var err error
			switch i % 4 {
			case 0:
				_, err = c.Place(ctx, mixedOrders())
			case 1:
				_, err = c.CancelByCloid(ctx, mixedCloidCancels())
			case 2:
				_, err = c.CancelByOid(ctx, mixedOidCancels())
			case 3:
				err = c.ScheduleCancel(ctx, nil)
			}
			done <- err
		}(i)
	}
	close(start)
	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("deadlock")
		}
	}
	reqs := v.requests()
	if len(reqs) != n {
		t.Fatalf("requests=%d want exactly %d (one per call)", len(reqs), n)
	}
	for i, raw := range reqs {
		var w wireAction
		if err := json.Unmarshal(raw, &w); err != nil {
			t.Fatal(err)
		}
		if w.Nonce != seed+int64(i)+1 {
			t.Fatalf("request %d nonce=%d want %d", i, w.Nonce, seed+int64(i)+1)
		}
		var typ struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(w.Action, &typ)
		if typ.Type != "scheduleCancel" {
			if _, _, assets, _ := decodeWire(t, raw); !eqInts(assets, mixedAssets) {
				t.Fatalf("request %d assets=%v", i, assets)
			}
		}
	}
}

// RegisterDex rebuilds the merged routing while mutations are in flight. Run
// with -race. Every Place must succeed with correct ids before and after.
func TestMixedDexRegisterDexRebuildsLookupRaceFree(t *testing.T) {
	v, url := newMixedVenue(t)
	c := newMixedClient(t, url, nil, "xyz")
	if _, err := c.Place(context.Background(), []Order{{Coin: "io:A", Px: "1", Sz: "1", Tif: TifAlo}}); err == nil {
		t.Fatal("io must not route before registration")
	}
	stop := make(chan struct{})
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := c.Place(context.Background(), []Order{{Coin: "BTC", Px: "1", Sz: "1", Tif: TifAlo}, {Coin: "xyz:B", Px: "1", Sz: "1", Tif: TifAlo}}); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}
	for _, d := range []string{"io", "para"} {
		if err := c.RegisterDex(context.Background(), d); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatalf("in-flight Place failed during registration: %v", err)
	default:
	}
	before := len(v.requests())
	if _, err := c.Place(context.Background(), mixedOrders()); err != nil {
		t.Fatal(err)
	}
	reqs := v.requests()
	if len(reqs) != before+1 {
		t.Fatalf("post-registration Place used %d requests", len(reqs)-before)
	}
	if _, _, assets, _ := decodeWire(t, reqs[len(reqs)-1]); !eqInts(assets, mixedAssets) {
		t.Fatalf("assets after RegisterDex=%v want %v", assets, mixedAssets)
	}
}
