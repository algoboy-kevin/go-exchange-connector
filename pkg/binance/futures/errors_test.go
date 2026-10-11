package futures

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// errServer starts a server that answers every request with status/body.
func errServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestErrorClassification drives every documented error class through a real
// HTTP round trip and asserts the helper predicates.
func TestErrorClassification(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		body        string
		wantUncert  bool
		wantRate    bool
		wantUnknown bool
	}{
		{"http500", 500, `{"code":-1000,"msg":"Internal error"}`, true, false, false},
		{"http503_html", 503, `<html>service unavailable</html>`, true, false, false},
		{"code-1007", 400, `{"code":-1007,"msg":"Timeout waiting for response from backend server. Send status unknown; execution status unknown."}`, true, false, false},
		{"http429", 429, `{"code":-1003,"msg":"Too many requests."}`, false, true, false},
		{"http418", 418, `{"code":-1003,"msg":"I'm a teapot."}`, false, true, false},
		{"code-1003", 400, `{"code":-1003,"msg":"Too many requests."}`, false, true, false},
		{"code-1015", 400, `{"code":-1015,"msg":"Too many new orders."}`, false, true, false},
		{"code-2011", 400, `{"code":-2011,"msg":"Unknown order sent."}`, false, false, true},
		{"code-2013", 400, `{"code":-2013,"msg":"Order does not exist."}`, false, false, true},
		{"code-5022", 400, `{"code":-5022,"msg":"Post Only order would cross the book."}`, false, false, false},
		{"code-2019", 400, `{"code":-2019,"msg":"Margin is insufficient."}`, false, false, false},
		{"code-4164", 400, `{"code":-4164,"msg":"Order's notional must be no smaller than 100."}`, false, false, false},
		{"code-1111", 400, `{"code":-1111,"msg":"Precision is over the maximum defined for this asset."}`, false, false, false},
		{"code-4014", 400, `{"code":-4014,"msg":"Price not increased by tick size."}`, false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := errServer(t, tc.status, tc.body)
			c := testClient(t, srv.URL)
			_, err := c.OpenOrders(context.Background(), "BTCUSDT")
			if err == nil {
				t.Fatal("expected error")
			}
			if got := IsUncertain(err); got != tc.wantUncert {
				t.Errorf("IsUncertain = %v, want %v (err: %v)", got, tc.wantUncert, err)
			}
			if got := IsRateLimited(err); got != tc.wantRate {
				t.Errorf("IsRateLimited = %v, want %v", got, tc.wantRate)
			}
			if got := IsUnknownOrder(err); got != tc.wantUnknown {
				t.Errorf("IsUnknownOrder = %v, want %v", got, tc.wantUnknown)
			}
			var ae *APIError
			if !errors.As(err, &ae) {
				t.Errorf("error is not *APIError: %T", err)
			}
		})
	}
}

// TestTransportErrorUncertain checks that a connection failure is uncertain.
func TestTransportErrorUncertain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	c := testClient(t, url)
	_, err := c.OpenOrders(context.Background(), "BTCUSDT")
	if err == nil {
		t.Fatal("expected a transport error against a closed server")
	}
	if !IsUncertain(err) {
		t.Fatalf("IsUncertain(transport) = false: %v", err)
	}
	if IsRateLimited(err) || IsUnknownOrder(err) {
		t.Fatalf("transport error misclassified: %v", err)
	}
}

// TestSetMarginTypeNoNeedToChange checks −4046 is swallowed.
func TestSetMarginTypeNoNeedToChange(t *testing.T) {
	srv := errServer(t, 400, `{"code":-4046,"msg":"No need to change margin type."}`)
	c := testClient(t, srv.URL)
	if err := c.SetMarginType(context.Background(), "BTCUSDT", "ISOLATED"); err != nil {
		t.Fatalf("SetMarginType(-4046) = %v, want nil", err)
	}
}

// TestSetMarginTypeRealError checks a genuine error still propagates.
func TestSetMarginTypeRealError(t *testing.T) {
	srv := errServer(t, 400, `{"code":-2019,"msg":"Margin is insufficient."}`)
	c := testClient(t, srv.URL)
	err := c.SetMarginType(context.Background(), "BTCUSDT", "ISOLATED")
	if err == nil || codeOf(err) != -2019 {
		t.Fatalf("SetMarginType = %v, want -2019", err)
	}
}

// TestSetPositionModeNoNeedToChange checks −4059 is swallowed.
func TestSetPositionModeNoNeedToChange(t *testing.T) {
	srv := errServer(t, 400, `{"code":-4059,"msg":"No need to change position side."}`)
	c := testClient(t, srv.URL)
	if err := c.SetPositionMode(context.Background(), true); err != nil {
		t.Fatalf("SetPositionMode(-4059) = %v, want nil", err)
	}
}

// TestPositionMode checks the dual-side flag decodes.
func TestPositionMode(t *testing.T) {
	srv := errServer(t, 200, `{"dualSidePosition":true}`)
	c := testClient(t, srv.URL)
	dual, err := c.PositionMode(context.Background())
	if err != nil {
		t.Fatalf("PositionMode: %v", err)
	}
	if !dual {
		t.Fatalf("dual = false, want true")
	}
}
