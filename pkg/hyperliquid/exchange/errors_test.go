package exchange

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestPlaceCallFailureClassification drives the sonirico error surfaces through
// a real HTTP round trip: the two shapes it produces for a failed request are a
// JSON error body (hl.APIError, which carries NO status) and a plain
// "status %d: %s" error for a non-JSON body.
func TestPlaceCallFailureClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		uncertain bool
		rateLim   bool
		actionErr bool
		why       string
	}{
		{
			name: "http 500 html", status: http.StatusInternalServerError,
			body: `<html>bad gateway</html>`, uncertain: true,
			why: "non-JSON 5xx: sonirico surfaces 'status 500: …'",
		},
		{
			name: "http 503 json", status: http.StatusServiceUnavailable,
			body: `{"code":-1,"msg":"service unavailable"}`, uncertain: false,
			why: "a 5xx WITH a JSON body arrives as hl.APIError, which drops the status — classified as business",
		},
		{
			name: "http 429 json", status: http.StatusTooManyRequests,
			body: `{"code":-1,"msg":"Too many requests"}`, rateLim: true,
			why: "the rate-limit text is the only signal on the JSON path",
		},
		{
			name: "http 429 text", status: http.StatusTooManyRequests,
			body: `Too many requests, please slow down`, rateLim: true,
			why: "non-JSON 429 → 'status 429: …'",
		},
		{
			name: "200 not json", status: http.StatusOK,
			body: `this is not json at all`, uncertain: true,
			why: "the action may have executed; the response cannot be read",
		},
		{
			name: "400 business", status: http.StatusBadRequest,
			body: `{"code":-1,"msg":"Invalid asset"}`, actionErr: false,
			why: "a plain 4xx business error",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setExchange(tc.body, tc.status)

			_, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}})
			if err == nil {
				t.Fatal("Place = nil error, want a call failure")
			}
			if got := IsUncertain(err); got != tc.uncertain {
				t.Errorf("IsUncertain = %v, want %v (%s): %v", got, tc.uncertain, tc.why, err)
			}
			if got := IsRateLimited(err); got != tc.rateLim {
				t.Errorf("IsRateLimited = %v, want %v (%s): %v", got, tc.rateLim, tc.why, err)
			}
			if !tc.uncertain && !tc.rateLim {
				var ae *ActionError
				if errors.As(err, &ae) {
					t.Errorf("unexpected *ActionError for a call failure: %v", err)
				}
			}
		})
	}
}

// TestPlaceUncertainOnTimeout covers a context/client timeout: the request may
// have reached the venue.
func TestPlaceUncertainOnTimeout(t *testing.T) {
	v, url := newVenue(t)
	v.setExchangeHandler(func(w http.ResponseWriter, body []byte) {
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte(`{"status":"ok","response":{"type":"order","data":{"statuses":[]}}}`))
	})
	c := newTestClientHTTP(t, url, &http.Client{Timeout: 20 * time.Millisecond})

	_, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}})
	if err == nil {
		t.Fatal("Place = nil error, want a timeout")
	}
	if !IsUncertain(err) {
		t.Errorf("IsUncertain(%v) = false, want true", err)
	}
}

// TestPlaceUncertainOnContextDeadline covers the ctx deadline/cancel path.
func TestPlaceUncertainOnContextDeadline(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Place(ctx, []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}}); err == nil {
		t.Fatal("Place = nil error, want a context error")
	} else if !IsUncertain(err) {
		t.Errorf("IsUncertain(%v) = false, want true", err)
	}

	v.setExchangeHandler(func(w http.ResponseWriter, body []byte) { time.Sleep(150 * time.Millisecond) })
	c = newTestClient(t, url)
	tctx, tcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer tcancel()
	if _, err := c.Place(tctx, []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}}); err == nil {
		t.Fatal("Place = nil error, want a deadline")
	} else if !IsUncertain(err) {
		t.Errorf("IsUncertain(%v) = false, want true", err)
	}
}

// TestClassifierUnitCases pins the non-network branches directly.
func TestClassifierUnitCases(t *testing.T) {
	plain := errors.New("some local validation failure")
	if IsUncertain(plain) || IsRateLimited(plain) {
		t.Errorf("a plain error was classified: uncertain=%v rateLimited=%v",
			IsUncertain(plain), IsRateLimited(plain))
	}
	if IsUncertain(nil) || IsRateLimited(nil) {
		t.Error("nil classified as an error")
	}

	ae := &ActionError{Msg: "Too many open orders. rate limit exceeded"}
	if IsUncertain(ae) {
		t.Error("ActionError classified as uncertain")
	}
	if !IsRateLimited(ae) {
		t.Error("an ActionError mentioning a rate limit should classify as rate limited")
	}

	if !IsUncertain(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should be uncertain")
	}
	if !IsUncertain(fmt.Errorf("place: %w", context.Canceled)) {
		t.Error("a wrapped context.Canceled should be uncertain")
	}
	if !IsRateLimited(errors.New(`status 429: slow down`)) {
		t.Error("a bare status-429 error should be rate limited")
	}
	if IsUncertain(errors.New(`status 400: bad request`)) {
		t.Error("a bare status-400 error is a business error")
	}
}
