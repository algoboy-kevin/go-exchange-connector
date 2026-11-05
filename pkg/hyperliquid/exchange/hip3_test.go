package exchange

import (
	"bytes"
	"context"
	"errors"
	"time"

	"encoding/json"
	"fmt"
	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Holes and nonalphabetical names are deliberate: registry position, not rank,
// determines the SDK's asset IDs.
type dexFixture struct {
	mu          sync.Mutex
	requests    []json.RawMessage
	queries     []map[string]string
	meta        map[string]string
	fail        string
	exFailAsset int
	queryRaw    map[string]string
}

func newDexFixture(t *testing.T) (*dexFixture, string) {
	t.Helper()
	f := &dexFixture{meta: map[string]string{"": metaFixture, "xyz": `{"universe":[{"name":"xyz:BTC","szDecimals":3},{"name":"xyz:TSLA","szDecimals":2}],"marginTables":[[1,{"marginTiers":[]}]]}`, "io": `{"universe":[{"name":"io:BTC","szDecimals":4}]}`}}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.URL.Path == "/info" {
			var q map[string]string
			_ = json.Unmarshal(raw, &q)
			f.queries = append(f.queries, q)
			if q["dex"] == f.fail && f.fail != "" && q["type"] != "meta" {
				http.Error(w, `{"error":"scope unavailable"}`, 503)
				return
			}
			if replacement, ok := f.queryRaw[q["type"]]; ok {
				fmt.Fprint(w, replacement)
				return
			}
			switch q["type"] {
			case "meta":
				fmt.Fprint(w, f.meta[q["dex"]])
			case "perpDexs":
				fmt.Fprint(w, `[null,{"name":"zzz"},null,{"name":"xyz"},{"name":"io"}]`)
			case "clearinghouseState":
				coin := "BTC"
				if q["dex"] != "" {
					coin = q["dex"] + ":BTC"
				}
				fmt.Fprintf(w, `{"assetPositions":[{"position":{"coin":%q,"szi":"1","entryPx":"100"}}],"marginSummary":{"accountValue":"100","totalMarginUsed":"5"},"withdrawable":"95"}`, coin)
			case "frontendOpenOrders":
				coin := "BTC"
				if q["dex"] != "" {
					coin = q["dex"] + ":BTC"
				}
				fmt.Fprintf(w, `[{"coin":%q,"oid":1,"side":"B","limitPx":"100","sz":"1","origSz":"1","cloid":%q}]`, coin, testCloid1)
			case "activeAssetData":
				fmt.Fprint(w, `{"leverage":{"type":"cross","value":5}}`)
			default:
				t.Errorf("unexpected query %v", q)
			}
			return
		}
		if r.URL.Path != "/exchange" {
			t.Errorf("unexpected path %s", r.URL.Path)
			return
		}
		f.requests = append(f.requests, append(json.RawMessage(nil), raw...))
		var e struct {
			Action struct {
				Type   string `json:"type"`
				Orders []struct {
					Asset int `json:"a"`
				} `json:"orders"`
				Cancels []json.RawMessage `json:"cancels"`
			} `json:"action"`
		}
		_ = json.Unmarshal(raw, &e)
		switch e.Action.Type {
		case "order":
			var sts []string
			for _, o := range e.Action.Orders {
				if f.exFailAsset > 0 && o.Asset == f.exFailAsset {
					fmt.Fprint(w, `{"status":"err","response":"scope rejected"}`)
					return
				}
				sts = append(sts, fmt.Sprintf(`{"resting":{"oid":%d}}`, o.Asset+1))
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
	}))
	t.Cleanup(s.Close)
	return f, s.URL
}
func registerTestDex(t *testing.T, c *Client, dex string) {
	t.Helper()
	r, ok := any(c).(interface {
		RegisterDex(context.Context, string) error
	})
	if !ok {
		t.Fatal("execution client lacks RegisterDex")
	}
	if err := r.RegisterDex(context.Background(), dex); err != nil {
		t.Fatal(err)
	}
}
func TestHIP3QueriesAggregateFailClosedAndKeepCoreAccount(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	registerTestDex(t, c, "io")
	ps, err := c.Positions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 3 || ps[0].Coin != "BTC" || ps[1].Coin != "io:BTC" || ps[2].Coin != "xyz:BTC" {
		t.Fatalf("positions=%+v want core+io+xyz", ps)
	}
	os, err := c.OpenOrders(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(os) != 3 || os[2].Coin != "xyz:BTC" || os[2].Cloid != testCloid1 {
		t.Fatalf("orders=%+v", os)
	}
	a, err := c.AccountState(context.Background())
	if err != nil || a.AccountValue != "100" {
		t.Fatalf("core account=%+v err=%v", a, err)
	}
	f.mu.Lock()
	last := f.queries[len(f.queries)-1]
	f.fail = "xyz"
	f.mu.Unlock()
	if _, ok := last["dex"]; ok {
		t.Fatalf("core AccountState query=%v", last)
	}
	if got, err := c.Positions(context.Background()); err == nil || got != nil {
		t.Fatalf("partial positions=%+v err=%v", got, err)
	}
	if got, err := c.OpenOrders(context.Background()); err == nil || got != nil {
		t.Fatalf("partial orders=%+v err=%v", got, err)
	}
}

func TestHIP3MutationsShareActualSignedNonces(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	registerTestDex(t, c, "io")
	// A future fixture nonce makes this discriminate SDK per-exchange counters
	// even on slow machines where wall-clock milliseconds do not collide.
	const seed = int64(4102444800000)
	c.signer.last = seed
	const n = 36
	done := make(chan error, n)
	start := make(chan struct{})
	coins := []string{"BTC", "xyz:BTC", "io:BTC"}
	for i := 0; i < n; i++ {
		go func(i int) {
			<-start
			coin := coins[(i/5)%3]
			ctx := context.Background()
			var err error
			switch i % 5 {
			case 0:
				_, err = c.Place(ctx, []Order{{Coin: coin, Px: "100.25", Sz: "1.25", Tif: TifAlo, ReduceOnly: true, Cloid: testCloid1}})
			case 1:
				_, err = c.CancelByOid(ctx, []OidCancel{{Coin: coin, Oid: 17}})
			case 2:
				_, err = c.CancelByCloid(ctx, []CancelReq{{Coin: coin, Cloid: testCloid1}})
			case 3:
				err = c.UpdateLeverage(ctx, coin, 5, true)
			case 4:
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
		case <-time.After(5 * time.Second):
			t.Fatal("actions deadlocked")
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != n {
		t.Fatalf("requests=%d", len(f.requests))
	}
	seen := make(map[int64]bool)
	types := make(map[string]bool)
	assets := make(map[int]bool)
	byType := make(map[string]map[int]bool)
	for i, raw := range f.requests {
		var e struct {
			Nonce     int64              `json:"nonce"`
			Signature hl.SignatureResult `json:"signature"`
			Action    json.RawMessage    `json:"action"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		if seen[e.Nonce] || e.Nonce != seed+int64(i)+1 {
			t.Fatalf("nonce[%d]=%d reused or not account-wide", i, e.Nonce)
		}
		seen[e.Nonce] = true
		if bytes.Contains(raw, []byte(testAgentKey)) || bytes.Contains(raw, []byte(testAccount)) {
			t.Fatal("credential/master serialized in action")
		}
		var a struct {
			Type   string `json:"type"`
			Asset  int    `json:"asset"`
			Orders []struct {
				Asset  int    `json:"a"`
				Px     string `json:"p"`
				Sz     string `json:"s"`
				Reduce bool   `json:"r"`
				Cloid  string `json:"c"`
			} `json:"orders"`
			Cancels []map[string]json.RawMessage `json:"cancels"`
		}
		_ = json.Unmarshal(e.Action, &a)
		types[a.Type] = true
		if byType[a.Type] == nil {
			byType[a.Type] = make(map[int]bool)
		}
		switch a.Type {
		case "order":
			o := a.Orders[0]
			assets[o.Asset] = true
			byType[a.Type][o.Asset] = true
			if o.Px != "100.25" || o.Sz != "1.25" || !o.Reduce || o.Cloid != testCloid1 {
				t.Fatalf("wire order=%+v", o)
			}
		case "cancel", "cancelByCloid":
			key := "a"
			if a.Type == "cancelByCloid" {
				key = "asset"
			}
			var id int
			_ = json.Unmarshal(a.Cancels[0][key], &id)
			assets[id] = true
			byType[a.Type][id] = true
		case "updateLeverage":
			assets[a.Asset] = true
			byType[a.Type][a.Asset] = true
		}
		// Decode into the SDK action types so its existing canonical signing path
		// verifies the wire signature without inventing a production signer.
		var action any
		switch a.Type {
		case "order":
			action = &hl.OrderAction{}
		case "cancel":
			action = &hl.CancelAction{}
		case "cancelByCloid":
			action = &hl.CancelByCloidAction{}
		case "updateLeverage":
			action = &hl.UpdateLeverageAction{}
		case "scheduleCancel":
			action = &hl.ScheduleCancelAction{}
		default:
			t.Fatalf("unknown action %q", a.Type)
		}
		if err := json.Unmarshal(e.Action, action); err != nil {
			t.Fatal(err)
		}
		sig, err := hl.SignL1Action(c.key, action, "", e.Nonce, nil, false)
		if err != nil || sig != e.Signature {
			t.Fatalf("signature %s mismatch: got %+v want %+v err=%v", a.Type, e.Signature, sig, err)
		}
	}
	for _, typ := range []string{"order", "cancel", "cancelByCloid", "updateLeverage"} {
		m := byType[typ]
		if len(m) != 3 || !m[0] || !m[130000] || !m[140000] {
			t.Fatalf("%s asset IDs=%v", typ, m)
		}
	}
	if len(types) != 5 || !assets[0] || !assets[130000] || !assets[140000] || len(assets) != 3 {
		t.Fatalf("types=%v assets=%v", types, assets)
	}
}

func TestHIP3BatchValidatesLaterScopeDecimalBeforeExchange(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	_, err := c.Place(context.Background(), []Order{{Coin: "BTC", Px: "100", Sz: "1", Tif: TifAlo}, {Coin: "xyz:BTC", Px: "1.123456789", Sz: "1", Tif: TifAlo}})
	if err == nil {
		t.Fatal("accepted SDK-unrepresentable decimal")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 0 {
		t.Fatalf("local decimal error submitted %d earlier actions", len(f.requests))
	}
}

func TestHIP3BatchRefusesEveryInvalidRouteBeforeExchange(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	for _, coin := range []string{"missing:BTC", "xyz:NOPE", "NOPE", "xyz:", ":BTC", "xyz:BTC:extra", " BTC"} {
		if _, err := c.Place(context.Background(), []Order{{Coin: "BTC", Px: "1", Sz: "1", Tif: TifAlo}, {Coin: coin, Px: "1", Sz: "1", Tif: TifAlo}}); err == nil {
			t.Fatalf("Place accepted %q", coin)
		}
		if _, err := c.CancelByOid(context.Background(), []OidCancel{{Coin: "BTC", Oid: 1}, {Coin: coin, Oid: 2}}); err == nil {
			t.Fatalf("CancelByOid accepted %q", coin)
		}
		if _, err := c.CancelByCloid(context.Background(), []CancelReq{{Coin: "BTC", Cloid: testCloid1}, {Coin: coin, Cloid: testCloid2}}); err == nil {
			t.Fatalf("CancelByCloid accepted %q", coin)
		}
		if err := c.UpdateLeverage(context.Background(), coin, 5, true); err == nil {
			t.Fatalf("leverage accepted %q", coin)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 0 {
		t.Fatalf("invalid routes emitted %d actions", len(f.requests))
	}
}

// S32: a mixed-scope batch is ONE request, so a top-level business rejection is
// an ordinary *ActionError for the whole action (not the retired multi-request
// PartialBatchError, and not uncertain). It was the old split's
// TestHIP3PartialBatchIsUncertainEvenBusinessRejection.
func TestHIP3MixedBatchTopLevelRejectionIsSingleRequestBusinessError(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	f.mu.Lock()
	f.exFailAsset = 130000
	f.mu.Unlock()
	out, err := c.Place(context.Background(), []Order{{Coin: "xyz:BTC", Px: "1", Sz: "1", Tif: TifAlo}, {Coin: "BTC", Px: "1", Sz: "1", Tif: TifAlo}})
	var ae *ActionError
	var partial *PartialBatchError
	if !errors.As(err, &ae) || errors.As(err, &partial) || IsUncertain(err) {
		t.Fatalf("error=%T %v want plain business *ActionError", err, err)
	}
	if out != nil {
		t.Fatalf("whole-action rejection returned statuses: %+v", out)
	}
	f.mu.Lock()
	if len(f.requests) != 1 {
		f.mu.Unlock()
		t.Fatalf("mixed batch used %d requests, want exactly 1", len(f.requests))
	}
	f.mu.Unlock()
	// A rejected request still consumed its signed nonce.
	last := c.signer.last
	f.mu.Lock()
	f.exFailAsset = 0
	f.mu.Unlock()
	if _, err := c.Place(context.Background(), []Order{{Coin: "BTC", Px: "1", Sz: "1", Tif: TifAlo}}); err != nil {
		t.Fatal(err)
	}
	if c.signer.last <= last {
		t.Fatal("failed request's signed nonce was reused")
	}
}

func TestHIP3ConcurrentRegistrationPublishesOnce(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	done := make(chan error, 24)
	for i := 0; i < 24; i++ {
		go func() { done <- c.RegisterDex(context.Background(), "xyz") }()
	}
	for i := 0; i < 24; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("registration deadlocked")
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, q := range f.queries {
		if q["type"] == "meta" && q["dex"] == "xyz" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("scope meta fetches=%d want 1", n)
	}
}

func TestHIP3RejectsMalformedRequiredQueryFields(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	for _, raw := range []string{`{}`, `null`, `{"assetPositions":null}`, `{"assetPositions":[{}]}`, `{"assetPositions":[{"position":{"coin":"BTC"}}]}`, `{"assetPositions":[{"position":{"coin":"BTC","szi":"oops"}}]}`} {
		f.mu.Lock()
		f.queryRaw = map[string]string{"clearinghouseState": raw}
		f.mu.Unlock()
		if _, err := c.Positions(context.Background()); err == nil {
			t.Fatalf("Positions accepted %s", raw)
		}
	}
	for _, raw := range []string{`null`, `{}`, `[{}]`, `[{"coin":"BTC","oid":1}]`, `[{"coin":"BTC","oid":1,"side":"B","limitPx":"oops","sz":"1","origSz":"1"}]`} {
		f.mu.Lock()
		f.queryRaw = map[string]string{"frontendOpenOrders": raw}
		f.mu.Unlock()
		if _, err := c.OpenOrders(context.Background()); err == nil {
			t.Fatalf("OpenOrders accepted %s", raw)
		}
	}
}

func TestHIP3MetadataRejectsMissingDecimals(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	f.mu.Lock()
	f.meta["xyz"] = `{"universe":[{"name":"xyz:BTC"}]}`
	f.mu.Unlock()
	err := c.RegisterDex(context.Background(), "xyz")
	if err == nil {
		t.Fatal("accepted metadata with missing szDecimals")
	}
	if _, err := c.route("xyz:BTC"); err == nil {
		t.Fatal("failed metadata published route")
	}
}

func TestHIP3RegistrationRoutesOriginalRegistryIndex(t *testing.T) {
	f, url := newDexFixture(t)
	c := newTestClient(t, url)
	registerTestDex(t, c, "xyz")
	registerTestDex(t, c, "io")
	out, err := c.Place(context.Background(), []Order{{Coin: "xyz:TSLA", Px: "100", Sz: "1", Tif: TifAlo}, {Coin: "io:BTC", Px: "101", Sz: "2", Tif: TifGtc}, {Coin: "BTC", Px: "102", Sz: "3", Tif: TifIoc}, {Coin: "xyz:BTC", Px: "103", Sz: "4", Tif: TifAlo}})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{130002, 140001, 1, 130001}
	for i, s := range out {
		if s.Oid != want[i] {
			t.Fatalf("status[%d]=%+v want oid %d", i, s, want[i])
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 1 {
		t.Fatalf("actions=%d want 1 (S32: one signed request across all scopes)", len(f.requests))
	}
}
