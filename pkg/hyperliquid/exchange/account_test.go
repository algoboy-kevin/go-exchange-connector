package exchange

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// S16 slice 4: a typed, read-only account snapshot from clearinghouseState for
// the MASTER account. Decimals stay strings; the driver converts/validates.
const accountFixture = `{
  "marginSummary": {"accountValue":"1013.482328","totalNtlPos":"25.56","totalRawUsd":"987.92","totalMarginUsed":"5.112"},
  "crossMarginSummary": {"accountValue":"1013.482328","totalNtlPos":"25.56","totalRawUsd":"987.92","totalMarginUsed":"5.112"},
  "crossMaintenanceMarginUsed": "1.278",
  "withdrawable": "1008.370328",
  "assetPositions": [],
  "time": 1787380860123
}`

func TestAccountState(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("clearinghouseState", accountFixture, 0)

	st, err := c.AccountState(context.Background())
	if err != nil {
		t.Fatalf("AccountState: %v", err)
	}
	want := AccountState{
		Account:                    testAccount,
		AccountValue:               "1013.482328",
		TotalMarginUsed:            "5.112",
		TotalNtlPos:                "25.56",
		TotalRawUsd:                "987.92",
		CrossMaintenanceMarginUsed: "1.278",
		Withdrawable:               "1008.370328",
		Time:                       1787380860123,
	}
	if *st != want {
		t.Fatalf("AccountState = %+v\nwant %+v", *st, want)
	}
	reqs := v.queryRequests()
	if len(reqs) != 1 {
		t.Fatalf("/info queries = %d, want 1", len(reqs))
	}
	if !strings.Contains(string(reqs[0]), `"type":"clearinghouseState"`) {
		t.Errorf("query is not clearinghouseState: %s", reqs[0])
	}
	assertUsesMasterAccount(t, reqs[0])
	if n := len(v.exchangeRequests()); n != 0 {
		t.Errorf("AccountState made %d /exchange requests; it must be read-only", n)
	}
}

func TestAccountStateRejectsIncompleteOrBadResponses(t *testing.T) {
	for name, tc := range map[string]struct {
		body      string
		status    int
		uncertain bool
	}{
		"missing marginSummary": {body: `{"withdrawable":"1","time":1}`},
		"missing accountValue":  {body: `{"marginSummary":{"totalMarginUsed":"0"},"withdrawable":"1","time":1}`},
		"missing withdrawable":  {body: `{"marginSummary":{"accountValue":"1","totalMarginUsed":"0"},"time":1}`},
		"missing margin used":   {body: `{"marginSummary":{"accountValue":"1"},"withdrawable":"1","time":1}`},
		"numeric not string":    {body: `{"marginSummary":{"accountValue":1,"totalMarginUsed":"0"},"withdrawable":"1"}`},
		"not json (2xx)":        {body: `<html>`, uncertain: true},
		"server error":          {body: `oops`, status: http.StatusBadGateway, uncertain: true},
		"null body":             {body: `null`},
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("clearinghouseState", tc.body, tc.status)
			st, err := c.AccountState(context.Background())
			if err == nil {
				t.Fatalf("AccountState accepted %s: %+v", tc.body, st)
			}
			if IsUncertain(err) != tc.uncertain {
				t.Errorf("IsUncertain(%v) = %v, want %v", err, IsUncertain(err), tc.uncertain)
			}
		})
	}
}
