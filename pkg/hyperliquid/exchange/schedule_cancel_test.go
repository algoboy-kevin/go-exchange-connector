package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// S16 slice 1: the schedule-cancel response must be classified from the RAW
// venue body (captured per request), not from sonirico's ScheduleCancelResponse,
// which drops the top-level "response" rejection string.

// venueVolumeRefusal is shaped like the venue's top-level rejection envelope:
// {"status":"err","response":"<message>"}.
const venueVolumeRefusal = `{"status":"err","response":"Cannot set scheduled cancel time until enough volume traded. Required: $1000000. Traded: $12.34."}`

func scheduleAt() *time.Time {
	at := time.Now().Add(time.Minute)
	return &at
}

func TestScheduleCancelResponseStringVolumeRefusal(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(venueVolumeRefusal, http.StatusOK)

	err := c.ScheduleCancel(context.Background(), scheduleAt())
	if err == nil {
		t.Fatal("ScheduleCancel = nil, want the volume refusal")
	}
	if !errors.Is(err, ErrScheduleCancelUnavailable) {
		t.Fatalf("error %q is not ErrScheduleCancelUnavailable", err)
	}
	if !strings.Contains(err.Error(), "enough volume traded") {
		t.Errorf("error %q does not preserve the venue's message", err)
	}
	if IsUncertain(err) || IsRateLimited(err) {
		t.Errorf("volume refusal misclassified as uncertain/rate-limited: %v", err)
	}
}

func TestScheduleCancelArbitraryResponseRejection(t *testing.T) {
	for _, body := range []string{
		`{"status":"err","response":"User or API Wallet 0xabc does not exist."}`,
		// Mentions volume but is not a traded-volume refusal.
		`{"status":"err","response":"Invalid scheduleCancel volume field"}`,
		`{"status":"err","response":"Invalid trading volume parameter"}`,
		// Textless and non-string responses.
		`{"status":"err"}`,
		`{"status":"err","response":{"code":7}}`,
	} {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(body, http.StatusOK)

		err := c.ScheduleCancel(context.Background(), scheduleAt())
		if err == nil {
			t.Fatalf("%s: ScheduleCancel = nil, want a rejection", body)
		}
		if errors.Is(err, ErrScheduleCancelUnavailable) {
			t.Errorf("%s: generic rejection mapped to ErrScheduleCancelUnavailable: %v", body, err)
		}
		var ae *ActionError
		if !errors.As(err, &ae) {
			t.Errorf("%s: error %v is not an *ActionError", body, err)
		}
		if IsUncertain(err) {
			t.Errorf("%s: clean venue rejection classified uncertain: %v", body, err)
		}
		if strings.Contains(body, "does not exist") && !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("%s: venue message lost: %v", body, err)
		}
	}
}

func TestScheduleCancelMalformedResponseIsUncertain(t *testing.T) {
	huge := `{"status":"err","response":"enough volume traded ` + strings.Repeat("x", 1<<20) + `"}`
	for name, body := range map[string]string{
		"not json":      `<html>gateway</html>`,
		"truncated":     `{"status":"er`,
		"no status":     `{}`,
		"unknown state": `{"status":"pending"}`,
		"oversize":      huge,
	} {
		t.Run(name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setExchange(body, http.StatusOK)

			err := c.ScheduleCancel(context.Background(), scheduleAt())
			if err == nil {
				t.Fatal("ScheduleCancel = nil for an unreadable response")
			}
			if errors.Is(err, ErrScheduleCancelUnavailable) {
				t.Errorf("unreadable response mapped to ErrScheduleCancelUnavailable: %v", err)
			}
			if !IsUncertain(err) {
				t.Errorf("unreadable response not uncertain: %v", err)
			}
			if len(err.Error()) > 2048 {
				t.Errorf("error message is unbounded (%d bytes)", len(err.Error()))
			}
		})
	}
}

func TestScheduleCancelServerAndTransportErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		rate   bool
	}{
		{"502 text mentioning volume", http.StatusBadGateway, "bad gateway: enough volume traded", false},
		{"503 json envelope mentioning volume", http.StatusServiceUnavailable, venueVolumeRefusal, false},
		{"500 json", http.StatusInternalServerError, `{"code":1,"msg":"internal"}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			v.setExchange(tc.body, tc.status)
			err := c.ScheduleCancel(context.Background(), scheduleAt())
			if err == nil {
				t.Fatal("ScheduleCancel = nil for a server error")
			}
			if errors.Is(err, ErrScheduleCancelUnavailable) {
				t.Errorf("server error mapped to ErrScheduleCancelUnavailable: %v", err)
			}
			if !IsUncertain(err) {
				t.Errorf("server error not uncertain: %v", err)
			}
		})
	}

	t.Run("429", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(`{"status":"err","response":"enough volume traded"}`, http.StatusTooManyRequests)
		err := c.ScheduleCancel(context.Background(), scheduleAt())
		if err == nil || errors.Is(err, ErrScheduleCancelUnavailable) || !IsRateLimited(err) {
			t.Fatalf("429: err=%v, want rate-limited and not unavailable", err)
		}
	})

	t.Run("connection reset", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchangeHandler(func(w http.ResponseWriter, _ []byte) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("no hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		})
		err := c.ScheduleCancel(context.Background(), scheduleAt())
		if err == nil || errors.Is(err, ErrScheduleCancelUnavailable) || !IsUncertain(err) {
			t.Fatalf("reset: err=%v, want uncertain and not unavailable", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClientHTTP(t, url, &http.Client{Timeout: 100 * time.Millisecond})
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		v.setExchangeHandler(func(w http.ResponseWriter, _ []byte) {
			select {
			case <-release:
			case <-time.After(5 * time.Second):
			}
		})
		err := c.ScheduleCancel(context.Background(), scheduleAt())
		if err == nil || errors.Is(err, ErrScheduleCancelUnavailable) || !IsUncertain(err) {
			t.Fatalf("timeout: err=%v, want uncertain and not unavailable", err)
		}
	})
}

func TestScheduleCancelSuccessArmAndDisarm(t *testing.T) {
	for _, body := range []string{
		`{"status":"ok","response":{"type":"default"}}`,
		`{"status":"ok"}`,
	} {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(body, http.StatusOK)
		if err := c.ScheduleCancel(context.Background(), scheduleAt()); err != nil {
			t.Fatalf("%s: arm: %v", body, err)
		}
		if err := c.ScheduleCancel(context.Background(), nil); err != nil {
			t.Fatalf("%s: disarm: %v", body, err)
		}
	}
}

// TestScheduleCancelUsesCallerHTTPClient: the capture hook must wrap, not
// replace, a caller-supplied client's transport.
func TestScheduleCancelUsesCallerHTTPClient(t *testing.T) {
	v, url := newVenue(t)
	var n atomic.Int64
	hc := &http.Client{Transport: countingRT{n: &n, base: http.DefaultTransport}}
	c := newTestClientHTTP(t, url, hc)
	v.setExchange(venueVolumeRefusal, http.StatusOK)
	before := n.Load()
	err := c.ScheduleCancel(context.Background(), scheduleAt())
	if !errors.Is(err, ErrScheduleCancelUnavailable) {
		t.Fatalf("err = %v, want ErrScheduleCancelUnavailable", err)
	}
	if n.Load() == before {
		t.Error("ScheduleCancel bypassed the caller's transport")
	}
}

type countingRT struct {
	n    *atomic.Int64
	base http.RoundTripper
}

func (c countingRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.base.RoundTrip(r)
}

// TestScheduleCancelConcurrentCallsDoNotCrossContaminate: the response each
// call classifies is its OWN response. The fake venue answers by the requested
// time's parity (even = volume refusal, odd = arbitrary rejection) with jitter,
// so responses complete out of order; order placements run alongside.
func TestScheduleCancelConcurrentCallsDoNotCrossContaminate(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchangeHandler(func(w http.ResponseWriter, body []byte) {
		var req struct {
			Action struct {
				Type string `json:"type"`
				Time *int64 `json:"time"`
			} `json:"action"`
		}
		_ = json.Unmarshal(body, &req)
		if req.Action.Type != "scheduleCancel" {
			_, _ = w.Write([]byte(`{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":1}}]}}}`))
			return
		}
		ms := *req.Action.Time
		time.Sleep(time.Duration(ms%7) * time.Millisecond)
		if ms%2 == 0 {
			_, _ = w.Write([]byte(venueVolumeRefusal))
			return
		}
		_, _ = fmt.Fprintf(w, `{"status":"err","response":"rejected %d"}`, ms)
	})

	base := time.Now().Add(time.Minute).Truncate(time.Second)
	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			at := base.Add(time.Duration(i) * time.Millisecond)
			err := c.ScheduleCancel(context.Background(), &at)
			ms := at.UnixMilli()
			switch {
			case err == nil:
				errs <- fmt.Errorf("call %d: nil error", i)
			case ms%2 == 0 && !errors.Is(err, ErrScheduleCancelUnavailable):
				errs <- fmt.Errorf("call %d (even): %v, want unavailable", i, err)
			case ms%2 == 1 && errors.Is(err, ErrScheduleCancelUnavailable):
				errs <- fmt.Errorf("call %d (odd): got another call's volume refusal: %v", i, err)
			case ms%2 == 1 && !strings.Contains(err.Error(), fmt.Sprintf("rejected %d", ms)):
				errs <- fmt.Errorf("call %d (odd): %v does not carry its own message", i, err)
			}
		}(i)
		go func() {
			defer wg.Done()
			if _, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "100", Sz: "0.001", Tif: TifAlo}}); err != nil {
				errs <- fmt.Errorf("place: %v", err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
