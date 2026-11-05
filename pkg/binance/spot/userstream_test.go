package spot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
)

// ─────────────────────────────────────────────────────────────
// Fake WS API server
// ─────────────────────────────────────────────────────────────

// subReq is one subscribe request seen by the server.
type subReq struct {
	ID     string
	Method string
	Params map[string]any
	Raw    string
}

// wsServer accepts connections; for each it reads the subscribe request, hands
// it to onSub (which returns the frames to send and whether to close after),
// then keeps the connection open until the test closes it.
type wsServer struct {
	srv   *httptest.Server
	mu    sync.Mutex
	subs  []subReq
	conns []*coderws.Conn
	// onSub decides the reply for the n-th (1-based) subscribe.
	onSub func(n int, r subReq) (frames []string, closeAfter bool)
}

func newWSServer(t *testing.T, onSub func(n int, r subReq) ([]string, bool)) *wsServer {
	t.Helper()
	w := &wsServer{onSub: onSub}
	w.srv = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(rw, r, nil)
		if err != nil {
			return
		}
		w.mu.Lock()
		w.conns = append(w.conns, c)
		w.mu.Unlock()
		ctx := r.Context()
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var req subReq
		req.Raw = string(data)
		var env struct {
			ID     string         `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(data, &env)
		req.ID, req.Method, req.Params = env.ID, env.Method, env.Params
		w.mu.Lock()
		w.subs = append(w.subs, req)
		n := len(w.subs)
		w.mu.Unlock()
		frames, closeAfter := w.onSub(n, req)
		for _, f := range frames {
			if err := c.Write(ctx, coderws.MessageText, []byte(f)); err != nil {
				return
			}
		}
		if closeAfter {
			_ = c.Close(coderws.StatusGoingAway, "bye")
			return
		}
		// Drain until the client goes away (also answers pings inside Read).
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}))
	t.Cleanup(func() {
		w.mu.Lock()
		for _, c := range w.conns {
			_ = c.CloseNow()
		}
		w.mu.Unlock()
		w.srv.Close()
	})
	return w
}

func (w *wsServer) url() string { return "ws" + strings.TrimPrefix(w.srv.URL, "http") }

func (w *wsServer) subCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.subs)
}

func (w *wsServer) sub(i int) subReq {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.subs[i]
}

func ackFrame(id string, subID int) string {
	return `{"id":"` + id + `","status":200,"result":{"subscriptionId":` + strconv.Itoa(subID) + `}}`
}

func compact(t *testing.T, name string) string {
	t.Helper()
	var buf strings.Builder
	var v any
	if err := json.Unmarshal(testdata(t, name), &v); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(v)
	buf.Write(b)
	return buf.String()
}

// verifySignature recomputes the subscribe signature the way the WS API doc
// describes: every param except signature, sorted by name, k=v joined by &.
func verifySignature(t *testing.T, r subReq, secret string) {
	t.Helper()
	if r.Method != "userDataStream.subscribe.signature" {
		t.Fatalf("method %q", r.Method)
	}
	sig, _ := r.Params["signature"].(string)
	var parts []string
	for _, k := range sortedKeys(r.Params) {
		if k == "signature" {
			continue
		}
		switch v := r.Params[k].(type) {
		case string:
			parts = append(parts, k+"="+v)
		case float64:
			parts = append(parts, k+"="+strconv.FormatInt(int64(v), 10))
		case map[string]any:
			// filter: compact JSON with sorted keys (Go marshals map keys sorted).
			b, _ := json.Marshal(v)
			parts = append(parts, k+"="+string(b))
		default:
			t.Fatalf("param %s has type %T", k, v)
		}
	}
	if want := signHex(secret, strings.Join(parts, "&")); sig != want {
		t.Fatalf("signature %s, want %s over %q", sig, want, strings.Join(parts, "&"))
	}
}

func newStream(t *testing.T, w *wsServer, mod func(*UserStreamConfig)) *UserStream {
	t.Helper()
	c, err := New(Config{BaseURL: "http://unused.invalid", APIKey: docKey, APISecret: docSecret})
	if err != nil {
		t.Fatal(err)
	}
	cfg := UserStreamConfig{Client: c, WSURL: w.url(), ReconnectIntervalMs: 20, AckTimeout: 3 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	u, err := NewUserStream(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(u.Stop)
	return u
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ─────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────

func TestUserStreamSubscribeAndEvents(t *testing.T) {
	exec := compact(t, "ws_execution_report.json")
	pos := compact(t, "ws_outbound_account_position.json")
	bal := compact(t, "ws_balance_update.json")
	w := newWSServer(t, func(_ int, r subReq) ([]string, bool) {
		return []string{ackFrame(r.ID, 7), exec, pos, bal, compact(t, "ws_server_shutdown.json")}, false
	})
	u := newStream(t, w, nil)

	var mu sync.Mutex
	var execs []ExecutionReport
	var poss []OutboundAccountPosition
	var bals []BalanceUpdate
	var statuses []bool
	u.SetOnExecutionReport(func(e ExecutionReport) { mu.Lock(); execs = append(execs, e); mu.Unlock() })
	u.SetOnAccountPosition(func(e OutboundAccountPosition) { mu.Lock(); poss = append(poss, e); mu.Unlock() })
	u.SetOnBalanceUpdate(func(e BalanceUpdate) { mu.Lock(); bals = append(bals, e); mu.Unlock() })
	u.SetOnStatus(func(up bool, _ error) { mu.Lock(); statuses = append(statuses, up); mu.Unlock() })

	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := u.Start(context.Background()); err == nil {
		t.Fatal("second Start should fail")
	}
	waitFor(t, "events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(execs) == 1 && len(poss) == 1 && len(bals) == 1 && u.Stats().Shutdowns == 1
	})
	if id, ok := u.SubscriptionID(); !ok || id != 7 {
		t.Fatalf("subscription id %d %v", id, ok)
	}

	// Request shape and signature.
	r := w.sub(0)
	verifySignature(t, r, docSecret)
	if r.Params["apiKey"] != docKey {
		t.Fatalf("apiKey %v", r.Params["apiKey"])
	}
	if _, has := r.Params["filter"]; has {
		t.Fatal("no filter configured but one was sent")
	}
	if got := sortedKeys(r.Params); strings.Join(got, ",") != "apiKey,recvWindow,signature,timestamp" {
		t.Fatalf("params %v", got)
	}

	mu.Lock()
	defer mu.Unlock()
	e := execs[0]
	if e.Event != "executionReport" || e.Symbol != "ETHBTC" || e.ClientOrderID != "mUvoqJxFIILMdfAW5iGSOW" || e.OrigClientOrderID != "" ||
		e.Side != "BUY" || e.OrderType != "LIMIT" || e.TimeInForce != "GTC" || e.Quantity != "1.00000000" || e.Price != "0.10264410" ||
		e.ExecType != "NEW" || e.Status != "NEW" || e.RejectReason != "NONE" || e.OrderID != 4293153 || e.OrderIDString() != "4293153" ||
		e.ExecutionID != 8641984 || e.TradeID != -1 || e.OrderListID != -1 || !e.OnBook || e.Maker || e.CommissionAsset != "" ||
		e.PreventedMatchID != 3 || e.STPMode != "NONE" || e.WorkingTime != 1499405658657 || e.CreatedTime != 1499405658657 || e.TransactTime != 1499405658657 ||
		e.EventTime != 1499405658658 || e.CumQuoteQty != "0.00000000" || e.AvgPrice() != "" {
		t.Fatalf("%+v", e)
	}
	if p := poss[0]; p.Event != "outboundAccountPosition" || p.EventTime != 1564034571105 || p.LastUpdateTime != 1564034571073 || len(p.Balances) != 1 || p.Balances[0].Asset != "ETH" || p.Balances[0].Free != "10000.000000" || p.Balances[0].Locked != "0.000000" {
		t.Fatalf("%+v", p)
	}
	if b := bals[0]; b.Asset != "BTC" || b.Delta != "100.00000000" || b.ClearTime != 1573200697068 || b.EventTime != 1573200697110 {
		t.Fatalf("%+v", b)
	}
	if len(statuses) == 0 || !statuses[0] {
		t.Fatalf("statuses %v", statuses)
	}
	if u.Stats().Msgs < 5 {
		t.Fatalf("stats %+v", u.Stats())
	}
}

// The case-insensitive decode trap: executionReport has keys that differ only
// by case. Every key must land in its own field.
func TestExecutionReportCaseSensitiveKeys(t *testing.T) {
	raw := `{"e":"executionReport","E":1,"s":"BTCUSDT","c":"cid","S":"SELL","o":"LIMIT","f":"IOC","q":"2","p":"3","P":"4","F":"5","g":-1,
	"C":"orig","x":"TRADE","X":"PARTIALLY_FILLED","r":"NONE","i":9,"l":"0.5","z":"1.5","L":"3","n":"0.1","N":"BNB","T":10,"t":77,"I":88,
	"w":false,"m":true,"M":true,"O":11,"Z":"4.5","Y":"1.5","Q":"6","W":12,"V":"EXPIRE_MAKER",
	"d":4,"D":13,"j":14,"J":15,"v":16,"A":"17.5","B":"18.5","u":19,"U":20,"Cs":"ETHUSDT","pl":"21","pL":"22","pY":"23","Qq":"24","eV":"DECREMENT",
	"b":"ONE_PARTY_TRADE_REPORT","a":1234,"k":"SOR","uS":true,"gP":"PRIMARY_PEG","gOT":"PRICE_LEVEL","gOV":5,"gp":"1.00000000"}`
	var e ExecutionReport
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"E vs e", e.EventTime, int64(1)}, {"e", e.Event, "executionReport"}, {"c", e.ClientOrderID, "cid"}, {"C", e.OrigClientOrderID, "orig"},
		{"s", e.Symbol, "BTCUSDT"}, {"S", e.Side, "SELL"}, {"x", e.ExecType, "TRADE"}, {"X", e.Status, "PARTIALLY_FILLED"},
		{"p", e.Price, "3"}, {"P", e.StopPrice, "4"}, {"q", e.Quantity, "2"}, {"Q", e.QuoteOrderQty, "6"}, {"f", e.TimeInForce, "IOC"}, {"F", e.IcebergQty, "5"},
		{"l", e.LastQty, "0.5"}, {"L", e.LastPrice, "3"}, {"z", e.CumQty, "1.5"}, {"Z", e.CumQuoteQty, "4.5"}, {"Y", e.LastQuoteQty, "1.5"},
		{"n", e.Commission, "0.1"}, {"N", e.CommissionAsset, "BNB"}, {"t", e.TradeID, int64(77)}, {"T", e.TransactTime, int64(10)},
		{"i", e.OrderID, int64(9)}, {"I", e.ExecutionID, int64(88)}, {"w", e.OnBook, false}, {"m", e.Maker, true}, {"M", e.Ignore, true},
		{"o", e.OrderType, "LIMIT"}, {"O", e.CreatedTime, int64(11)}, {"w W", e.WorkingTime, int64(12)}, {"V", e.STPMode, "EXPIRE_MAKER"},
		{"d", e.TrailingDelta, int64(4)}, {"D", e.TrailingTime, int64(13)}, {"j", e.StrategyID, int64(14)}, {"J", e.StrategyType, int64(15)},
		{"v", e.PreventedMatchID, int64(16)}, {"A", e.PreventedQty, "17.5"}, {"B", e.LastPreventedQty, "18.5"},
		{"u", e.TradeGroupID, int64(19)}, {"U", e.CounterOrderID, int64(20)}, {"Cs", e.CounterSymbol, "ETHUSDT"},
		{"pl", e.PreventedExecQty, "21"}, {"pL", e.PreventedExecPrice, "22"}, {"pY", e.PreventedExecQuote, "23"}, {"Qq", e.QuoteNotionalLimit, "24"},
		{"eV", e.EffectiveSTPMode, "DECREMENT"}, {"b", e.AllocationMatchType, "ONE_PARTY_TRADE_REPORT"}, {"a", e.AllocationID, int64(1234)},
		{"k", e.WorkingFloor, "SOR"}, {"uS", e.UsedSOR, true}, {"gP", e.PegPriceType, "PRIMARY_PEG"}, {"gOT", e.PegOffsetType, "PRICE_LEVEL"},
		{"gOV", e.PegOffsetValue, int64(5)}, {"gp", e.PeggedPrice, "1.00000000"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v want %v", c.name, c.got, c.want)
		}
	}
	if got := e.AvgPrice(); got != "3.00000000" {
		t.Errorf("avg price %q (Z/z = 4.5/1.5)", got)
	}
	// A report that carries 'a' (int) but no 'A' must not smear one into the other.
	var f ExecutionReport
	if err := json.Unmarshal([]byte(`{"e":"executionReport","a":5}`), &f); err != nil {
		t.Fatal(err)
	}
	if f.AllocationID != 5 || f.PreventedQty != "" {
		t.Fatalf("%+v", f)
	}
	// Every source key must be carried back out under the same spelling.
	var src map[string]any
	_ = json.Unmarshal([]byte(raw), &src)
	out, _ := json.Marshal(e)
	var dst map[string]any
	_ = json.Unmarshal(out, &dst)
	for k := range src {
		if _, ok := dst[k]; !ok {
			t.Errorf("key %q lost on round trip", k)
		}
	}
	// Doc payload round trip.
	var env struct {
		Event json.RawMessage `json:"event"`
	}
	_ = json.Unmarshal(testdata(t, "ws_execution_report.json"), &env)
	var de ExecutionReport
	if err := json.Unmarshal(env.Event, &de); err != nil {
		t.Fatal(err)
	}
	keysCovered(t, "ws_execution_report", env.Event, de)
	_ = env
	var pos struct {
		Event json.RawMessage `json:"event"`
	}
	_ = json.Unmarshal(testdata(t, "ws_outbound_account_position.json"), &pos)
	var dp OutboundAccountPosition
	_ = json.Unmarshal(pos.Event, &dp)
	keysCovered(t, "ws_outbound_account_position", pos.Event, dp)
	var bu struct {
		Event json.RawMessage `json:"event"`
	}
	_ = json.Unmarshal(testdata(t, "ws_balance_update.json"), &bu)
	var db BalanceUpdate
	_ = json.Unmarshal(bu.Event, &db)
	keysCovered(t, "ws_balance_update", bu.Event, db)
}

func TestUserStreamRejectedSubscribeFailsStart(t *testing.T) {
	w := newWSServer(t, func(_ int, r subReq) ([]string, bool) {
		return []string{`{"id":"` + r.ID + `","status":401,"error":{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}}`}, false
	})
	u := newStream(t, w, nil)
	var downs int
	var mu sync.Mutex
	u.SetOnStatus(func(up bool, _ error) {
		if !up {
			mu.Lock()
			downs++
			mu.Unlock()
		}
	})
	err := u.Start(context.Background())
	ae, ok := err.(*APIError)
	if !ok || ae.Code != -2015 || ae.HTTPStatus != 401 {
		t.Fatalf("err %T %v", err, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if downs == 0 {
		t.Fatal("status callback not told")
	}
	// Start failed: Stop was called; Start again is allowed.
	if _, ok := u.SubscriptionID(); ok {
		t.Fatal("subscription reported active")
	}
}

func TestUserStreamAckTimeout(t *testing.T) {
	w := newWSServer(t, func(_ int, _ subReq) ([]string, bool) { return nil, false })
	u := newStream(t, w, func(c *UserStreamConfig) { c.AckTimeout = 150 * time.Millisecond })
	err := u.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not acknowledged") {
		t.Fatalf("%v", err)
	}
}

func TestUserStreamReconnectResubscribes(t *testing.T) {
	exec := compact(t, "ws_execution_report.json")
	w := newWSServer(t, func(n int, r subReq) ([]string, bool) {
		if n == 1 {
			return []string{ackFrame(r.ID, 0)}, true // first connection: ack, then the server drops us (24 h cut)
		}
		return []string{ackFrame(r.ID, 3), exec}, false
	})
	u := newStream(t, w, nil)
	var mu sync.Mutex
	var execs int
	var ups, downs int
	u.SetOnExecutionReport(func(ExecutionReport) { mu.Lock(); execs++; mu.Unlock() })
	u.SetOnStatus(func(up bool, _ error) {
		mu.Lock()
		defer mu.Unlock()
		if up {
			ups++
		} else {
			downs++
		}
	})
	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "resubscribe and event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return execs == 1 && ups >= 2 && downs >= 1
	})
	if w.subCount() != 2 {
		t.Fatalf("%d subscribes, want 2", w.subCount())
	}
	a, b := w.sub(0), w.sub(1)
	verifySignature(t, a, docSecret)
	verifySignature(t, b, docSecret)
	if a.ID == b.ID {
		t.Fatal("subscribe request ids should differ per connection")
	}
	if a.Params["signature"] == b.Params["signature"] && a.Params["timestamp"] == b.Params["timestamp"] {
		t.Log("same ms timestamp on both subscribes (possible, harmless)")
	}
	if id, ok := u.SubscriptionID(); !ok || id != 3 {
		t.Fatalf("subscription id %d %v", id, ok)
	}
	if st := u.Stats(); st.Reconnects < 1 || st.Disconnects < 1 {
		t.Fatalf("%+v", st)
	}
}

func TestUserStreamFilterIsSignedAndSorted(t *testing.T) {
	w := newWSServer(t, func(_ int, r subReq) ([]string, bool) { return []string{ackFrame(r.ID, 0)}, false })
	u := newStream(t, w, func(c *UserStreamConfig) {
		c.EventTypes = []string{"outboundAccountPosition", "executionReport"}
	})
	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := w.sub(0)
	verifySignature(t, r, docSecret)
	f, ok := r.Params["filter"].(map[string]any)
	if !ok {
		t.Fatalf("filter missing: %v", r.Params)
	}
	es, _ := f["e"].([]any)
	if len(es) != 2 || es[0] != "executionReport" || es[1] != "outboundAccountPosition" {
		t.Fatalf("filter %v", f)
	}
	// The signed string must contain the filter between apiKey and recvWindow, no whitespace.
	want := "apiKey=" + docKey + `&filter={"e":["executionReport","outboundAccountPosition"]}&recvWindow=5000&timestamp=` + strconv.FormatInt(int64(r.Params["timestamp"].(float64)), 10)
	if got := signHex(docSecret, want); got != r.Params["signature"] {
		t.Fatalf("signature does not cover %q", want)
	}
}

func TestSubscribePayloadOrder(t *testing.T) {
	got := subscribePayload("K", "", 5000, 99)
	if got != "apiKey=K&recvWindow=5000&timestamp=99" {
		t.Fatal(got)
	}
	got = subscribePayload("K", `{"e":["a"]}`, 5000, 99)
	if got != `apiKey=K&filter={"e":["a"]}&recvWindow=5000&timestamp=99` {
		t.Fatal(got)
	}
	parts := strings.Split(got, "&")
	if !sort.StringsAreSorted(parts) { // names sort apiKey < filter < recvWindow < timestamp
		t.Fatal("not alphabetical")
	}
}

func TestUserStreamEventStreamTerminatedResubscribes(t *testing.T) {
	w := newWSServer(t, func(n int, r subReq) ([]string, bool) {
		if n == 1 {
			return []string{ackFrame(r.ID, 0), compact(t, "ws_event_stream_terminated.json")}, false
		}
		return []string{ackFrame(r.ID, 0)}, false
	})
	u := newStream(t, w, nil)
	if err := u.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "resubscribe after eventStreamTerminated", func() bool { return w.subCount() >= 2 && u.Stats().Terminated == 1 })
}

func TestUserStreamBadFramesAreRecordedNotFatal(t *testing.T) {
	c, _ := New(Config{APIKey: docKey, APISecret: docSecret})
	u, _ := NewUserStream(UserStreamConfig{Client: c})
	u.handleMessage([]byte(`not json`))
	if !strings.Contains(u.Stats().LastError, "decode") {
		t.Fatalf("%+v", u.Stats())
	}
	u.handleMessage([]byte(`{"event":{"e":"executionReport","i":"notanumber"}}`))
	if !strings.Contains(u.Stats().LastError, "executionReport") {
		t.Fatalf("%+v", u.Stats())
	}
	// Unknown event types and stray responses are ignored.
	u.handleMessage([]byte(`{"subscriptionId":0,"event":{"e":"listStatus","E":1}}`))
	u.handleMessage([]byte(`{"id":"other","status":200,"result":{}}`))
}

func TestNewUserStreamValidation(t *testing.T) {
	if _, err := NewUserStream(UserStreamConfig{}); err == nil {
		t.Fatal("nil client accepted")
	}
	noCreds, _ := New(Config{})
	if _, err := NewUserStream(UserStreamConfig{Client: noCreds}); err == nil {
		t.Fatal("client without credentials accepted")
	}
	c, _ := New(Config{APIKey: docKey, APISecret: docSecret})
	if _, err := NewUserStream(UserStreamConfig{Client: c, Network: "x"}); err == nil {
		t.Fatal("unknown network accepted")
	}
	if _, err := NewUserStream(UserStreamConfig{Client: c, EventTypes: []string{`a"b`}}); err == nil {
		t.Fatal("bad event type accepted")
	}
	u, err := NewUserStream(UserStreamConfig{Client: c, Network: Testnet})
	if err != nil || u.wsURL != "wss://demo-ws-api.binance.com/ws-api/v3" {
		t.Fatalf("%v %v", u, err)
	}
	u, _ = NewUserStream(UserStreamConfig{Client: c})
	if u.wsURL != "wss://ws-api.binance.com:443/ws-api/v3" {
		t.Fatal(u.wsURL)
	}
}
