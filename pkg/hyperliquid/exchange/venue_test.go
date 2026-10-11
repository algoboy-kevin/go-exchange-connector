package exchange

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// testAgentKey is a fixed agent-wallet key (the hex used by sonirico's own
// signing tests). testAccount is the master account every query must target.
const (
	testAgentKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testAccount  = "0x1111111111111111111111111111111111111111"

	testCloid1 = "0x00000000000000000000000000000001"
	testCloid2 = "0x00000000000000000000000000000002"
)

// metaFixture is a venue-shaped meta body: marginTables is the real nested
// [[id, {…}]] form, which hl.Meta CANNOT decode (see parseMeta) — a fixture
// without it would not exercise the trap.
const metaFixture = `{
  "universe": [
    {"name":"BTC","szDecimals":5,"maxLeverage":40,"onlyIsolated":false},
    {"name":"ETH","szDecimals":4,"maxLeverage":25},
    {"name":"OLD","szDecimals":2,"maxLeverage":10,"onlyIsolated":true,"isDelisted":true}
  ],
  "marginTables": [[50,{"description":"Tiered","marginTiers":[{"lowerBound":"0","maxLeverage":40}]}]],
  "collateralToken": 0
}`

// venue is a fake Hyperliquid REST endpoint serving /info and /exchange.
type venue struct {
	mu sync.Mutex

	infoTypes  []string
	infoBodies [][]byte
	infoRaw    map[string]string // request "type" -> response body
	infoStatus map[string]int    // request "type" -> HTTP status (0 = 200)

	exBodies  [][]byte
	exStatus  int
	exBody    string
	exHandler func(w http.ResponseWriter, body []byte)
}

func newVenue(t *testing.T) (*venue, string) {
	t.Helper()
	v := &venue{
		infoRaw:    map[string]string{"meta": metaFixture},
		infoStatus: map[string]int{},
	}
	srv := httptest.NewServer(http.HandlerFunc(v.handle))
	t.Cleanup(srv.Close)
	return v, srv.URL
}

func (v *venue) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	switch r.URL.Path {
	case "/info":
		var req struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(body, &req)
		v.mu.Lock()
		v.infoTypes = append(v.infoTypes, req.Type)
		v.infoBodies = append(v.infoBodies, body)
		raw, ok := v.infoRaw[req.Type]
		status := v.infoStatus[req.Type]
		v.mu.Unlock()
		if !ok {
			http.Error(w, `{"error":"no fixture for info type"}`, http.StatusBadRequest)
			return
		}
		if status != 0 {
			w.WriteHeader(status)
		}
		_, _ = w.Write([]byte(raw))
	case "/exchange":
		v.mu.Lock()
		v.exBodies = append(v.exBodies, body)
		h, status, resp := v.exHandler, v.exStatus, v.exBody
		v.mu.Unlock()
		if h != nil {
			h(w, body)
			return
		}
		if status != 0 {
			w.WriteHeader(status)
		}
		if resp == "" {
			resp = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":1,"status":"resting"}}]}}}`
		}
		_, _ = w.Write([]byte(resp))
	default:
		http.NotFound(w, r)
	}
}

// setInfo registers a fixture for one /info query type.
func (v *venue) setInfo(typ, raw string, status int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.infoRaw[typ] = raw
	if status != 0 {
		v.infoStatus[typ] = status
	}
}

// setExchange registers the /exchange response.
func (v *venue) setExchange(raw string, status int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.exBody, v.exStatus = raw, status
}

// setExchangeHandler installs a custom /exchange handler (streaming walls,
// sleeps for timeout tests, ...).
func (v *venue) setExchangeHandler(h func(w http.ResponseWriter, body []byte)) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.exHandler = h
}

func (v *venue) infoCalls() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.infoTypes...)
}

// queryRequests returns the /info bodies excluding the constructor's meta fetch,
// which every client makes.
func (v *venue) queryRequests() [][]byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out [][]byte
	for i, typ := range v.infoTypes {
		if typ == "meta" {
			continue
		}
		out = append(out, v.infoBodies[i])
	}
	return out
}

func (v *venue) exchangeRequests() [][]byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([][]byte, len(v.exBodies))
	copy(out, v.exBodies)
	return out
}

// newTestClient builds a Client against the fake venue.
func newTestClient(t *testing.T, url string) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{
		BaseURL:  url,
		AgentKey: testAgentKey,
		Account:  testAccount,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// newTestClientHTTP is newTestClient with a caller-supplied http.Client.
func newTestClientHTTP(t *testing.T, url string, hc *http.Client) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{
		BaseURL:  url,
		AgentKey: testAgentKey,
		Account:  testAccount,
		HTTP:     hc,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// lastExchangeBody returns the most recent /exchange request body.
func lastExchangeBody(t *testing.T, v *venue) []byte {
	t.Helper()
	reqs := v.exchangeRequests()
	if len(reqs) == 0 {
		t.Fatal("no /exchange request was made")
	}
	return reqs[len(reqs)-1]
}
