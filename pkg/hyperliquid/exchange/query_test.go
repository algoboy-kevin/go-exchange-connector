package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const clearinghouseFixture = `{
  "assetPositions": [
    {"type":"oneWay","position":{"coin":"BTC","szi":"0.012","entryPx":"97123.5","leverage":{"type":"cross","value":10},"marginUsed":"58.2","positionValue":"1165.5","unrealizedPnl":"1.2","returnOnEquity":"0.02","liquidationPx":"50000","cumFunding":{"allTime":"0","sinceChange":"0","sinceOpen":"0"}}},
    {"type":"oneWay","position":{"coin":"ETH","szi":"-1.5","entryPx":null}}
  ],
  "crossMarginSummary": {"accountValue":"1000"},
  "withdrawable": "100"
}`

const frontendOpenOrdersFixture = `[
  {"coin":"BTC","limitPx":"97123.5","oid":7001,"side":"B","sz":"0.002","origSz":"0.005","timestamp":1787380860000,"cloid":"0x00000000000000000000000000000001","orderType":"Limit","reduceOnly":false,"isTrigger":false},
  {"coin":"ETH","limitPx":"3000.25","oid":7002,"side":"A","sz":"0.5","origSz":"0.5","timestamp":1787380861000,"orderType":"Limit","reduceOnly":true,"isTrigger":false}
]`

func TestPositions(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("clearinghouseState", clearinghouseFixture, 0)

	pos, err := c.Positions(context.Background())
	if err != nil {
		t.Fatalf("Positions: %v", err)
	}
	if len(pos) != 2 {
		t.Fatalf("positions = %d, want 2", len(pos))
	}
	if pos[0].Coin != "BTC" || pos[0].Szi != "0.012" || pos[0].EntryPx != "97123.5" {
		t.Errorf("BTC position = %+v", pos[0])
	}
	if pos[1].Coin != "ETH" || pos[1].Szi != "-1.5" || pos[1].EntryPx != "" {
		t.Errorf("ETH position (null entryPx) = %+v", pos[1])
	}

	// The query must carry the MASTER account, never the agent wallet.
	reqs := v.queryRequests()
	if len(reqs) != 1 {
		t.Fatalf("/info queries = %d, want 1", len(reqs))
	}
	assertUsesMasterAccount(t, reqs[0])
}

func TestOpenOrders(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("frontendOpenOrders", frontendOpenOrdersFixture, 0)

	orders, err := c.OpenOrders(context.Background())
	if err != nil {
		t.Fatalf("OpenOrders: %v", err)
	}
	if len(orders) != 2 {
		t.Fatalf("orders = %d, want 2", len(orders))
	}
	o := orders[0]
	if o.Coin != "BTC" || o.Side != "B" || o.LimitPx != "97123.5" || o.Sz != "0.002" || o.OrigSz != "0.005" ||
		o.Oid != 7001 || o.Cloid != testCloid1 || o.Timestamp != 1787380860000 {
		t.Errorf("order[0] = %+v", o)
	}
	if orders[1].Cloid != "" {
		t.Errorf("order[1] cloid = %q, want empty when the venue omits it", orders[1].Cloid)
	}

	reqs := v.queryRequests()
	if len(reqs) != 1 {
		t.Fatalf("/info queries = %d, want 1", len(reqs))
	}
	assertUsesMasterAccount(t, reqs[0])

	var body struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(reqs[0], &body); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if body.Type != "frontendOpenOrders" {
		t.Errorf("query type = %q, want frontendOpenOrders", body.Type)
	}
}

// TestQueriesUseMasterAccountNotAgent is the explicit guard for rule 7: the
// agent wallet address must never be queried.
func TestQueriesUseMasterAccountNotAgent(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("clearinghouseState", `{"assetPositions":[]}`, 0)
	v.setInfo("frontendOpenOrders", `[]`, 0)

	if _, err := c.Positions(context.Background()); err != nil {
		t.Fatalf("Positions: %v", err)
	}
	if _, err := c.OpenOrders(context.Background()); err != nil {
		t.Fatalf("OpenOrders: %v", err)
	}
	agent := agentAddress(t)
	if strings.Contains(strings.ToLower(testAccount), strings.ToLower(agent)) {
		t.Fatal("test setup is wrong: agent and master addresses match")
	}
	for i, req := range v.queryRequests() {
		if bytes.Contains(bytes.ToLower(req), []byte(strings.ToLower(agent))) {
			t.Errorf("request %d uses the agent address: %s", i, req)
		}
		assertUsesMasterAccount(t, req)
	}
}

// TestQueryInfoErrorsClassify checks the /info error path is typed well enough
// for the classifiers.
func TestQueryInfoErrorsClassify(t *testing.T) {
	t.Run("http 500 is uncertain", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setInfo("clearinghouseState", `{"error":"internal"}`, 500)
		if _, err := c.Positions(context.Background()); err == nil {
			t.Fatal("Positions = nil error")
		} else if !IsUncertain(err) {
			t.Errorf("IsUncertain(%v) = false, want true", err)
		}
	})
	t.Run("http 429 is rate limited", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setInfo("frontendOpenOrders", `{"error":"slow down"}`, 429)
		if _, err := c.OpenOrders(context.Background()); err == nil {
			t.Fatal("OpenOrders = nil error")
		} else if !IsRateLimited(err) {
			t.Errorf("IsRateLimited(%v) = false, want true", err)
		} else if IsUncertain(err) {
			t.Errorf("a 429 classified as uncertain: %v", err)
		}
	})
	t.Run("200 with a non-json body is uncertain", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setInfo("clearinghouseState", `<html>nope</html>`, 0)
		if _, err := c.Positions(context.Background()); err == nil {
			t.Fatal("Positions = nil error")
		} else if !IsUncertain(err) {
			t.Errorf("IsUncertain(%v) = false, want true", err)
		}
	})
	t.Run("garbage json body is a business error", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setInfo("clearinghouseState", `{"assetPositions":"not an array"}`, 0)
		if _, err := c.Positions(context.Background()); err == nil {
			t.Fatal("Positions = nil error")
		} else if IsUncertain(err) {
			t.Errorf("a shape mismatch classified as uncertain: %v", err)
		}
	})
}

// assertUsesMasterAccount checks a request body targets the master account.
func assertUsesMasterAccount(t *testing.T, body []byte) {
	t.Helper()
	if !bytes.Contains(body, []byte(testAccount)) {
		t.Errorf("request does not carry the master account %s: %s", testAccount, body)
	}
}
