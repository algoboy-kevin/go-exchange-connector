package exchange

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Read-only /info rate-limit retry (S31). Every test uses a localhost
// httptest server; nothing here touches the network. The wait is injected, so
// no test sleeps.

func init() {
	// Package default: no test ever sleeps through a real backoff. Tests that
	// assert the waits install their own recorder via recordWaits.
	infoWait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
}

// recordWaits installs a wait recorder (optionally cancelling ctx on the nth
// wait) and restores the package default afterwards. These tests are not
// parallel: infoWait is package state.
func recordWaits(t *testing.T, cancelOn int, cancel context.CancelFunc) *[]time.Duration {
	t.Helper()
	prev := infoWait
	var waits []time.Duration
	infoWait = func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		if cancelOn > 0 && len(waits) == cancelOn {
			cancel()
		}
		return ctx.Err()
	}
	t.Cleanup(func() { infoWait = prev })
	return &waits
}

// scriptedOrders serves meta (constructor) and frontendOpenOrders with a
// scripted status sequence (the last entry repeats), and counts /exchange.
type scriptedOrders struct {
	mu       sync.Mutex
	script   []int
	body     string
	queries  int
	exchange int
}

func newScriptedOrders(t *testing.T, body string, script ...int) (*scriptedOrders, string) {
	t.Helper()
	s := &scriptedOrders{script: script, body: body}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf strings.Builder
		b := make([]byte, 4096)
		for {
			n, err := r.Body.Read(b)
			buf.Write(b[:n])
			if err != nil {
				break
			}
		}
		req := buf.String()
		switch {
		case r.URL.Path == "/exchange":
			s.mu.Lock()
			s.exchange++
			s.mu.Unlock()
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"status":"err","response":"too many"}`))
		case strings.Contains(req, `"meta"`):
			_, _ = w.Write([]byte(metaFixture))
		case strings.Contains(req, `"frontendOpenOrders"`):
			s.mu.Lock()
			i := s.queries
			s.queries++
			st := s.script[min(i, len(s.script)-1)]
			s.mu.Unlock()
			if st != http.StatusOK {
				w.WriteHeader(st)
				_, _ = w.Write([]byte(`{"error":"slow down"}`))
				return
			}
			_, _ = w.Write([]byte(s.body))
		default:
			http.Error(w, `{"error":"no fixture"}`, http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return s, srv.URL
}

func (s *scriptedOrders) count() (queries, exchange int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queries, s.exchange
}

const oneOrderBody = `[{"coin":"BTC","limitPx":"1","oid":7,"side":"B","sz":"1","origSz":"1"}]`

func TestInfo429ThenSuccessRetriesWithinBudget(t *testing.T) {
	s, url := newScriptedOrders(t, oneOrderBody, 429, 429, 200)
	c := newTestClient(t, url)
	waits := recordWaits(t, 0, nil)
	got, err := c.OpenOrdersDex(context.Background(), "")
	if err != nil || len(got) != 1 {
		t.Fatalf("OpenOrdersDex = %+v, %v; want success after two 429s", got, err)
	}
	if q, _ := s.count(); q != 3 {
		t.Fatalf("queries = %d, want 3 (two 429 + success)", q)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second}; len(*waits) != 2 || (*waits)[0] != want[0] || (*waits)[1] != want[1] {
		t.Fatalf("waits = %v, want %v", *waits, want)
	}
}

func TestInfo429ExhaustedIsBoundedAndStillTyped(t *testing.T) {
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newTestClient(t, url)
	waits := recordWaits(t, 0, nil)
	_, err := c.OpenOrdersDex(context.Background(), "")
	if err == nil || !IsRateLimited(err) {
		t.Fatalf("err = %v, want a rate-limited error after the budget", err)
	}
	if IsUncertain(err) {
		t.Fatalf("an exhausted read 429 must not be uncertain: %v", err)
	}
	if q, _ := s.count(); q != infoRetryMax+1 {
		t.Fatalf("queries = %d, want %d (1 + %d retries)", q, infoRetryMax+1, infoRetryMax)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	if len(*waits) != len(want) {
		t.Fatalf("waits = %v, want %v", *waits, want)
	}
	for i := range want {
		if (*waits)[i] != want[i] {
			t.Fatalf("waits = %v, want %v", *waits, want)
		}
	}
	// The label and status survive for a safe diagnostic.
	if !strings.Contains(err.Error(), "frontendOpenOrders") || !strings.Contains(err.Error(), "429") {
		t.Fatalf("diagnostic lost the query label/status: %v", err)
	}
}

func TestInfoNon429FailsImmediately(t *testing.T) {
	for name, st := range map[string]int{"500": 500, "400": 400, "503": 503} {
		t.Run(name, func(t *testing.T) {
			s, url := newScriptedOrders(t, oneOrderBody, st)
			c := newTestClient(t, url)
			waits := recordWaits(t, 0, nil)
			if _, err := c.OpenOrdersDex(context.Background(), ""); err == nil || IsRateLimited(err) {
				t.Fatalf("err = %v, want a non-rate-limit failure", err)
			}
			if q, _ := s.count(); q != 1 || len(*waits) != 0 {
				t.Fatalf("queries=%d waits=%v, want exactly one attempt and no wait", q, *waits)
			}
		})
	}
	t.Run("malformed 200", func(t *testing.T) {
		s, url := newScriptedOrders(t, `{"not":"an array"}`, 200)
		c := newTestClient(t, url)
		waits := recordWaits(t, 0, nil)
		if _, err := c.OpenOrdersDex(context.Background(), ""); err == nil {
			t.Fatal("malformed body accepted")
		}
		if q, _ := s.count(); q != 1 || len(*waits) != 0 {
			t.Fatalf("queries=%d waits=%v", q, *waits)
		}
	})
}

func TestInfoRetryAbortsOnCancelDuringBackoff(t *testing.T) {
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newTestClient(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := recordWaits(t, 2, cancel) // cancelled while waiting for retry #2
	_, err := c.OpenOrdersDex(ctx, "")
	if !errors.Is(err, context.Canceled) || !IsRateLimited(err) {
		t.Fatalf("err = %v, want context.Canceled wrapping the last rate-limit error", err)
	}
	if q, _ := s.count(); q != 2 || len(*waits) != 2 {
		t.Fatalf("queries=%d waits=%v, want 2 attempts and the cancelled wait", q, *waits)
	}
}

func TestInfoRetrySkippedWhenWaitExceedsDeadline(t *testing.T) {
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newTestClient(t, url)
	waits := recordWaits(t, 0, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.OpenOrdersDex(ctx, ""); err == nil || !IsRateLimited(err) {
		t.Fatalf("err = %v, want the 429 returned without burning the deadline", err)
	}
	if q, _ := s.count(); q != 1 || len(*waits) != 0 {
		t.Fatalf("queries=%d waits=%v, want 1 attempt and no wait", q, *waits)
	}
}

func TestExchangeMutationIsNeverRetriedOn429(t *testing.T) {
	s, url := newScriptedOrders(t, oneOrderBody, 200)
	c := newTestClient(t, url)
	waits := recordWaits(t, 0, nil)
	_, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}})
	if err == nil {
		t.Fatal("Place accepted a 429")
	}
	if _, ex := s.count(); ex != 1 || len(*waits) != 0 {
		t.Fatalf("/exchange requests=%d waits=%v, want exactly one and no backoff", ex, *waits)
	}
	if err := c.ScheduleCancel(context.Background(), nil); err == nil {
		t.Fatal("ScheduleCancel accepted a 429")
	}
	if _, ex := s.count(); ex != 2 || len(*waits) != 0 {
		t.Fatalf("/exchange requests=%d waits=%v after ScheduleCancel, want 2 and no backoff", ex, *waits)
	}
}
