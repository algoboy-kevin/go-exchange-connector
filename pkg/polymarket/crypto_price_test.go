package polymarket

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return tm
}

func TestWindowFromMarket(t *testing.T) {
	tests := []struct {
		name      string
		variant   string
		endDate   string
		wantStart string
		wantErr   bool
	}{
		{"fiveminute", "fiveminute", "2026-08-21T11:40:00Z", "2026-08-21T11:35:00Z", false},
		{"fifteen", "fifteen", "2026-08-21T11:45:00Z", "2026-08-21T11:30:00Z", false},
		{"hourly", "hourly", "2026-08-21T11:00:00Z", "2026-08-21T10:00:00Z", false},
		{"daily", "daily", "2026-08-21T00:00:00Z", "2026-08-20T00:00:00Z", false},
		{"unknown variant", "weekly", "2026-08-21T11:40:00Z", "", true},
		{"nil market", "fiveminute", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gm *GammaMarket
			if tc.endDate != "" {
				gm = &GammaMarket{EndDate: mustTime(t, tc.endDate)}
			}
			start, end, err := WindowFromMarket(gm, tc.variant)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none (start=%v end=%v)", start, end)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if want := mustTime(t, tc.wantStart); !start.Equal(want) {
				t.Errorf("start = %v, want %v", start, want)
			}
			if want := mustTime(t, tc.endDate); !end.Equal(want) {
				t.Errorf("end = %v, want %v", end, want)
			}
		})
	}
}

func TestCryptoPriceClientFetch(t *testing.T) {
	const responseBody = `{"openPrice":76578.37804472372,"closePrice":null,"timestamp":1787312179781,"completed":false,"incomplete":true,"cached":true}`

	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(responseBody))
	}))
	defer srv.Close()

	client := NewCryptoPriceClient(srv.URL)

	req := connector.CryptoPriceRequest{
		Symbol:              "BTC",
		Variant:             "fiveminute",
		EventStartTime:      mustTime(t, "2026-08-21T11:35:00Z"),
		EndDate:             mustTime(t, "2026-08-21T11:40:00Z"),
		TWAPEnabled:         true,
		TWAPLookbackSeconds: 60,
	}

	price, err := client.Fetch(req)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Query params sent to the API.
	if got := gotQuery.Get("symbol"); got != "BTC" {
		t.Errorf("symbol = %q, want BTC", got)
	}
	if got := gotQuery.Get("variant"); got != "fiveminute" {
		t.Errorf("variant = %q, want fiveminute", got)
	}
	if got := gotQuery.Get("eventStartTime"); got != "2026-08-21T11:35:00Z" {
		t.Errorf("eventStartTime = %q, want 2026-08-21T11:35:00Z", got)
	}
	if got := gotQuery.Get("endDate"); got != "2026-08-21T11:40:00Z" {
		t.Errorf("endDate = %q, want 2026-08-21T11:40:00Z", got)
	}
	if got := gotQuery.Get("twapEnabled"); got != "true" {
		t.Errorf("twapEnabled = %q, want true", got)
	}
	if got := gotQuery.Get("twapLookbackSeconds"); got != "60" {
		t.Errorf("twapLookbackSeconds = %q, want 60", got)
	}

	// Parsed response.
	if price.OpenPrice != 76578.37804472372 {
		t.Errorf("OpenPrice = %v, want 76578.37804472372", price.OpenPrice)
	}
	if price.ClosePrice != nil {
		t.Errorf("ClosePrice = %v, want nil", *price.ClosePrice)
	}
	if price.Timestamp != 1787312179781 {
		t.Errorf("Timestamp = %d, want 1787312179781", price.Timestamp)
	}
	if price.Completed || !price.Incomplete || !price.Cached {
		t.Errorf("flags = completed:%v incomplete:%v cached:%v, want false/true/true",
			price.Completed, price.Incomplete, price.Cached)
	}
}

func TestCryptoPriceClientFetchRejectsIncompleteRequest(t *testing.T) {
	client := NewCryptoPriceClient("http://example.invalid")

	cases := []struct {
		name string
		req  connector.CryptoPriceRequest
	}{
		{"missing symbol", connector.CryptoPriceRequest{Variant: "fiveminute", EventStartTime: time.Now(), EndDate: time.Now()}},
		{"missing variant", connector.CryptoPriceRequest{Symbol: "BTC", EventStartTime: time.Now(), EndDate: time.Now()}},
		{"missing start", connector.CryptoPriceRequest{Symbol: "BTC", Variant: "fiveminute", EndDate: time.Now()}},
		{"missing end", connector.CryptoPriceRequest{Symbol: "BTC", Variant: "fiveminute", EventStartTime: time.Now()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.Fetch(tc.req); err == nil {
				t.Fatal("expected error for incomplete request")
			}
		})
	}
}

func TestNormalizeGammaMarketParsesDates(t *testing.T) {
	raw := &RawGammaMarket{
		ID:        "1",
		Outcomes:  "[\"Up\", \"Down\"]",
		StartDate: "2026-08-21T11:35:14.576Z",
		EndDate:   "2026-08-21T11:40:00Z",
	}
	gm := normalizeGammaMarket(raw)
	if !gm.EndDate.Equal(mustTime(t, "2026-08-21T11:40:00Z")) {
		t.Errorf("EndDate = %v, want 2026-08-21T11:40:00Z", gm.EndDate)
	}
	if !gm.StartDate.Equal(mustTime(t, "2026-08-21T11:35:14.576Z")) {
		t.Errorf("StartDate = %v, want 2026-08-21T11:35:14.576Z", gm.StartDate)
	}

	// Unparseable/missing dates fall back to zero.
	gm2 := normalizeGammaMarket(&RawGammaMarket{Outcomes: "[\"Yes\", \"No\"]"})
	if !gm2.EndDate.IsZero() || !gm2.StartDate.IsZero() {
		t.Errorf("expected zero dates, got start=%v end=%v", gm2.StartDate, gm2.EndDate)
	}
}
