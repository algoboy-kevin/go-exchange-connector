package exchange

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// S25 slice A (Go owner): typed account-mode and unified-collateral queries
// over the same unsigned /info transport the other account reads use. The
// wire shapes are the anonymous fixtures below — no live access, no secrets.
//
// userAbstraction answers with a bare JSON STRING mode ("disabled",
// "default", "unifiedAccount"). Unknown values are returned typed but must be
// rejected by the caller's accounting path (fail closed), and a malformed or
// failed query must never fall back to legacy balances.

func TestAccountModeValuesAndExactPayload(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		want AccountMode
	}{
		"disabled": {body: `"disabled"`, want: AccountModeDisabled},
		"default":  {body: `"default"`, want: AccountModeDefault},
		"unified":  {body: `"unifiedAccount"`, want: AccountModeUnified},
		// Well-formed but unknown modes round-trip typed so the caller
		// can report them before refusing the account interpretation.
		"unknown": {body: `"portfolioMargin"`, want: AccountMode("portfolioMargin")},
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("userAbstraction", tc.body, 0)

			mode, err := c.AccountMode(context.Background())
			if err != nil || mode != tc.want {
				t.Fatalf("AccountMode = %q, %v; want %q", mode, err, tc.want)
			}
			reqs := v.queryRequests()
			if len(reqs) != 1 {
				t.Fatalf("/info queries = %d, want 1", len(reqs))
			}
			if string(reqs[0]) != `{"type":"userAbstraction","user":"`+testAccount+`"}` {
				t.Errorf("query is not the exact userAbstraction payload: %s", reqs[0])
			}
			assertUsesMasterAccount(t, reqs[0])
			if n := len(v.exchangeRequests()); n != 0 {
				t.Errorf("AccountMode made %d /exchange requests; it must be read-only", n)
			}
		})
	}
}

func TestAccountModeRejectsMalformedOrFailedQueries(t *testing.T) {
	for name, tc := range map[string]struct {
		body        string
		status      int
		uncertain   bool
		rateLimited bool
	}{
		"object":            {body: `{"mode":"unifiedAccount"}`},
		"null":              {body: `null`},
		"number":            {body: `123`},
		"array":             {body: `["unifiedAccount"]`},
		"empty string":      {body: `""`},
		"bool":              {body: `true`},
		"not json (2xx)":    {body: `<html>`, uncertain: true},
		"server error":      {body: `oops`, status: http.StatusBadGateway, uncertain: true},
		"rate limited":      {body: `slow down`, status: http.StatusTooManyRequests, rateLimited: true},
		"no fixture (400)":  {body: ``, status: http.StatusBadRequest},
		"gateway timeout":   {body: `oops`, status: http.StatusGatewayTimeout, uncertain: true},
		"unavailable (503)": {body: `oops`, status: http.StatusServiceUnavailable, uncertain: true},
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			if tc.body != "" || tc.status != 0 {
				v.setInfo("userAbstraction", tc.body, tc.status)
			}
			mode, err := c.AccountMode(context.Background())
			if err == nil {
				t.Fatalf("AccountMode accepted %q from %q", mode, tc.body)
			}
			if IsUncertain(err) != tc.uncertain {
				t.Errorf("IsUncertain(%v) = %v, want %v", err, IsUncertain(err), tc.uncertain)
			}
			if IsRateLimited(err) != tc.rateLimited {
				t.Errorf("IsRateLimited(%v) = %v, want %v", err, IsRateLimited(err), tc.rateLimited)
			}
		})
	}
}

// spotUSDCFixture mirrors the parent-verified live shape anonymously:
// balances rows carry token/coin/total/hold and the maintenance availability
// lives in separate [token, amount] tuples (NOT on the rows).
const spotUSDCFixture = `{
  "balances": [
    {"coin":"USDC","token":0,"total":"101.757823","hold":"0","entryNtl":"0"},
    {"coin":"FUR","token":3,"total":"2","hold":"1","entryNtl":"0"}
  ],
  "tokenToAvailableAfterMaintenance": [[0,"101.757823"],[3,"1"]]
}`

func TestSpotUSDCBalanceParsesValidatedStrings(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("spotClearinghouseState", spotUSDCFixture, 0)

	b, err := c.SpotUSDCBalance(context.Background())
	if err != nil {
		t.Fatalf("SpotUSDCBalance: %v", err)
	}
	want := SpotUSDCBalance{
		Account:                   testAccount,
		Total:                     "101.757823",
		Hold:                      "0",
		AvailableAfterMaintenance: "101.757823",
	}
	if *b != want {
		t.Fatalf("SpotUSDCBalance = %+v\nwant %+v", *b, want)
	}
	reqs := v.queryRequests()
	if len(reqs) != 1 {
		t.Fatalf("/info queries = %d, want 1", len(reqs))
	}
	if string(reqs[0]) != `{"type":"spotClearinghouseState","user":"`+testAccount+`"}` {
		t.Errorf("query is not the exact spotClearinghouseState payload: %s", reqs[0])
	}
	assertUsesMasterAccount(t, reqs[0])
	if n := len(v.exchangeRequests()); n != 0 {
		t.Errorf("SpotUSDCBalance made %d /exchange requests; it must be read-only", n)
	}
}

func TestSpotUSDCBalanceRejectsBadWireData(t *testing.T) {
	for name, body := range map[string]string{
		"missing balances":      `{"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"null balances":         `{"balances":null,"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"balances not array":    `{"balances":{},"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"row not object":        `{"balances":[[]],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"null row":              `{"balances":[null],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token missing":         `{"balances":[{"coin":"USDC","total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token negative":        `{"balances":[{"coin":"X","token":-1,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token float":           `{"balances":[{"coin":"X","token":0.5,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token string":          `{"balances":[{"coin":"X","token":"1","total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token bool":            `{"balances":[{"coin":"X","token":true,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"coin missing":          `{"balances":[{"token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"coin empty":            `{"balances":[{"coin":"","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"duplicate token":       `{"balances":[{"coin":"A","token":1,"total":"1","hold":"0"},{"coin":"B","token":1,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"duplicate coin":        `{"balances":[{"coin":"A","token":1,"total":"1","hold":"0"},{"coin":"A","token":2,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"token0 wrong coin":     `{"balances":[{"coin":"USDT","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"usdc wrong token":      `{"balances":[{"coin":"USDC","token":2,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"missing USDC":          `{"balances":[{"coin":"FUR","token":3,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"total missing":         `{"balances":[{"coin":"USDC","token":0,"hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"total number":          `{"balances":[{"coin":"USDC","token":0,"total":1,"hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"total NaN":             `{"balances":[{"coin":"USDC","token":0,"total":"NaN","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"total negative":        `{"balances":[{"coin":"USDC","token":0,"total":"-1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"total garbage":         `{"balances":[{"coin":"USDC","token":0,"total":"abc","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"hold missing":          `{"balances":[{"coin":"USDC","token":0,"total":"1"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"hold negative":         `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"-0.5"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"hold garbage":          `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"x"}],"tokenToAvailableAfterMaintenance":[[0,"1"]]}`,
		"tuples missing":        `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}]}`,
		"tuples null":           `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":null}`,
		"tuples not array":      `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":{}}`,
		"tuple not pair":        `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0]]}`,
		"tuple too long":        `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1","x"]]}`,
		"tuple not array row":   `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[{}]}`,
		"tuple token negative":  `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[-1,"1"]]}`,
		"tuple token float":     `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0.5,"1"]]}`,
		"tuple duplicate token": `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"1"],[0,"2"]]}`,
		"tuple amount number":   `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,1]]}`,
		"tuple amount negative": `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0,"-1"]]}`,
		"tuple amount missing":  `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[0]]}`,
		"tuple USDC missing":    `{"balances":[{"coin":"USDC","token":0,"total":"1","hold":"0"}],"tokenToAvailableAfterMaintenance":[[3,"1"]]}`,
		"response is array":     `[{"coin":"USDC","token":0,"total":"1","hold":"0"}]`,
		"not json (2xx)":        `<html>`,
		"server error":          `oops`,
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("spotClearinghouseState", body, 0)
			if strings.HasPrefix(name, "server error") {
				v.setInfo("spotClearinghouseState", body, 502)
			}
			b, err := c.SpotUSDCBalance(context.Background())
			if err == nil {
				t.Fatalf("SpotUSDCBalance accepted %s: %+v", body, b)
			}
		})
	}
}

func TestSpotUSDCBalanceAcceptsNegativeZeroAndUnusedTokens(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	// "-0" is a nonnegative finite decimal (mirrors Decimal semantics);
	// tuples for tokens without a balance row are validated, not required.
	v.setInfo("spotClearinghouseState", `{
	  "balances": [{"coin":"USDC","token":0,"total":"0","hold":"-0"}],
	  "tokenToAvailableAfterMaintenance": [[0,"0"],[9,"1"]]
	}`, 0)
	b, err := c.SpotUSDCBalance(context.Background())
	if err != nil {
		t.Fatalf("SpotUSDCBalance: %v", err)
	}
	if b.Total != "0" || b.Hold != "-0" || b.AvailableAfterMaintenance != "0" {
		t.Fatalf("SpotUSDCBalance = %+v", *b)
	}
}

// metaBodiesSince returns the /info bodies requested after index n (the
// constructor's meta fetch is request 0 and is excluded).
func metaBodiesSince(t *testing.T, v *venue, n int) [][]byte {
	t.Helper()
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.infoBodies) < n {
		t.Fatalf("info bodies = %d, want >= %d", len(v.infoBodies), n)
	}
	out := make([][]byte, 0, len(v.infoBodies)-n)
	for i := n; i < len(v.infoBodies); i++ {
		out = append(out, v.infoBodies[i])
	}
	return out
}

func TestCollateralTokenRequiresExplicitMetadataConfirmation(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		dex  string
		want int
	}{
		"core": {body: `{"universe":[{"name":"BTC","szDecimals":5}],"collateralToken":0}`, want: 0},
		"hip3": {body: `{"universe":[{"name":"xyz:JP225","szDecimals":5}],"collateralToken":0}`, dex: "xyz", want: 0},
		"non-usdc is returned typed for the caller to refuse": {
			body: `{"universe":[{"name":"para:FOO","szDecimals":5}],"collateralToken":3}`, dex: "para", want: 3,
		},
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("meta", tc.body, 0)
			v.mu.Lock()
			before := len(v.infoBodies)
			v.mu.Unlock()

			got, err := c.CollateralToken(context.Background(), tc.dex)
			if err != nil || got != tc.want {
				t.Fatalf("CollateralToken(%q) = %d, %v; want %d", tc.dex, got, err, tc.want)
			}
			reqs := metaBodiesSince(t, v, before)
			if len(reqs) != 1 {
				t.Fatalf("meta queries = %d, want exactly 1 per call", len(reqs))
			}
			want := `{"type":"meta"}`
			if tc.dex != "" {
				want = `{"dex":"` + tc.dex + `","type":"meta"}`
			}
			if string(reqs[0]) != want {
				t.Errorf("meta query = %s, want %s", reqs[0], want)
			}
		})
	}
}

func TestCollateralTokenRejectsMissingOrTypedWrong(t *testing.T) {
	for name, body := range map[string]string{
		"missing field":     `{"universe":[{"name":"BTC","szDecimals":5}]}`,
		"null":              `{"collateralToken":null}`,
		"float":             `{"collateralToken":0.5}`,
		"string":            `{"collateralToken":"0"}`,
		"bool":              `{"collateralToken":true}`,
		"not an object":     `[]`,
		"not json (2xx)":    `<html>`,
		"unavailable (503)": `oops`,
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("meta", body, 0)
			if name == "unavailable (503)" {
				v.setInfo("meta", body, 503)
			}
			if got, err := c.CollateralToken(context.Background(), "xyz"); err == nil {
				t.Fatalf("CollateralToken accepted %s -> %d", body, got)
			}
		})
	}
}
