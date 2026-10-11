package exchange

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

// TestLeverageQuery pins the activeAssetData decode: the venue reports
// {"leverage":{"type":"cross"|"isolated","value":N}}.
func TestLeverageQuery(t *testing.T) {
	cases := []struct {
		name        string
		fixture     string
		wantLev     int
		wantIsCross bool
	}{
		{"cross", `{"user":"0x1111111111111111111111111111111111111111","coin":"BTC","leverage":{"type":"cross","value":20,"rawUsd":"-1.5"},"maxTradeSzs":["0","0"]}`, 20, true},
		{"isolated", `{"coin":"ETH","leverage":{"type":"isolated","value":5}}`, 5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setInfo("activeAssetData", tc.fixture, 0)

			lev, isCross, err := c.Leverage(context.Background(), "BTC")
			if err != nil {
				t.Fatalf("Leverage: %v", err)
			}
			if lev != tc.wantLev || isCross != tc.wantIsCross {
				t.Errorf("Leverage = (%d, %v), want (%d, %v)", lev, isCross, tc.wantLev, tc.wantIsCross)
			}

			reqs := v.queryRequests()
			if len(reqs) != 1 {
				t.Fatalf("/info queries = %d, want 1", len(reqs))
			}
			assertUsesMasterAccount(t, reqs[0])
			if !bytes.Contains(reqs[0], []byte(`"type":"activeAssetData"`)) ||
				!bytes.Contains(reqs[0], []byte(`"coin":"BTC"`)) {
				t.Errorf("query = %s, want activeAssetData for BTC", reqs[0])
			}
		})
	}
}

// TestLeverageUnknownType is a decode guard: an unrecognised margin type must be
// an error, not a silently-wrong "isolated".
func TestLeverageUnknownType(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setInfo("activeAssetData", `{"leverage":{"type":"portfolio","value":10}}`, 0)

	if _, _, err := c.Leverage(context.Background(), "BTC"); err == nil {
		t.Fatal("Leverage = nil error for an unknown margin type")
	} else if !strings.Contains(err.Error(), "unknown leverage type") {
		t.Errorf("error = %v, want an unknown-leverage-type error", err)
	}
}

// TestUpdateLeverageApplied is the happy path: the venue reports exactly the
// requested setting, so UpdateLeverage returns nil.
func TestUpdateLeverageApplied(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{}`, http.StatusOK)
	v.setInfo("activeAssetData", `{"leverage":{"type":"isolated","value":5}}`, 0)

	if err := c.UpdateLeverage(context.Background(), "ETH", 5, false); err != nil {
		t.Fatalf("UpdateLeverage: %v", err)
	}
	if n := len(v.exchangeRequests()); n != 1 {
		t.Errorf("/exchange requests = %d, want 1", n)
	}
	if n := len(v.queryRequests()); n != 1 {
		t.Errorf("/info verification queries = %d, want 1", n)
	}
}

// TestUpdateLeverageNotApplied is the case sonirico cannot see: the action posts
// fine (a {"status":"err"} envelope decodes into a zero UserState with no Go
// error) but the venue never applied it. The verification must turn that into an
// error naming both the wanted and the observed setting.
func TestUpdateLeverageNotApplied(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	// A top-level rejection the wrapper cannot observe on the action path.
	v.setExchange(`{"status":"err","response":"could not update leverage"}`, http.StatusOK)
	// The venue still reports the OLD setting.
	v.setInfo("activeAssetData", `{"leverage":{"type":"cross","value":20}}`, 0)

	err := c.UpdateLeverage(context.Background(), "ETH", 5, false)
	if err == nil {
		t.Fatal("UpdateLeverage = nil error, want a not-applied error")
	}
	for _, want := range []string{
		"leverage for ETH not applied",
		"want 5 isolated",
		"venue has 20 cross",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	// It is a business outcome, not an uncertain one: the venue answered.
	if IsUncertain(err) || IsRateLimited(err) {
		t.Errorf("not-applied misclassified: uncertain=%v rateLimited=%v", IsUncertain(err), IsRateLimited(err))
	}
	var ae *ActionError
	if errors.As(err, &ae) {
		t.Errorf("not-applied surfaced as *ActionError: %v", err)
	}
}

// TestUpdateLeverageVerifyQueryFailure covers the verification read failing: the
// error must propagate (so the caller knows the outcome is unknown), and a
// transport/5xx failure classifies as uncertain.
func TestUpdateLeverageVerifyQueryFailure(t *testing.T) {
	t.Run("500 is uncertain", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(`{}`, http.StatusOK)
		v.setInfo("activeAssetData", `{"error":"internal"}`, http.StatusInternalServerError)

		err := c.UpdateLeverage(context.Background(), "ETH", 5, false)
		if err == nil {
			t.Fatal("UpdateLeverage = nil error when the verification query failed")
		}
		if !strings.Contains(err.Error(), "verify leverage ETH") {
			t.Errorf("error = %v, want a verify-leverage error", err)
		}
		if !IsUncertain(err) {
			t.Errorf("a 5xx verification failure should be uncertain: %v", err)
		}
	})
}
