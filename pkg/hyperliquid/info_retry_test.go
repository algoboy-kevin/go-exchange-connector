package hyperliquid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock replaces the client's time source and sleeper so retry and
// pacing tests run instantly and deterministically. sleep advances the clock.
type fakeClock struct {
	t      time.Time
	sleeps []time.Duration
}

func (f *fakeClock) install(c *InfoClient) {
	f.t = time.Unix(1_800_000_000, 0)
	c.now = func() time.Time { return f.t }
	c.sleep = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		f.sleeps = append(f.sleeps, d)
		f.t = f.t.Add(d)
		return nil
	}
	c.jitter = func(d time.Duration) time.Duration { return d } // upper bound, deterministic
}

// scriptedServer answers request i with script[i] (the last entry repeats).
type reply struct {
	status     int
	body       string
	retryAfter string
}

func scriptedServer(t *testing.T, script []reply) (*httptest.Server, *int32) {
	t.Helper()
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(atomic.AddInt32(&n, 1)) - 1
		if i >= len(script) {
			i = len(script) - 1
		}
		rp := script[i]
		if rp.retryAfter != "" {
			w.Header().Set("Retry-After", rp.retryAfter)
		}
		w.WriteHeader(rp.status)
		_, _ = w.Write([]byte(rp.body))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func TestDefaultClientDoesNotRetry(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `rate limited`, ""}, {200, `{}`, ""}})
	_, err := NewInfoClient(srv.URL).Post(context.Background(), map[string]any{"type": "meta"})
	if !IsRateLimited(err) || atomic.LoadInt32(n) != 1 {
		t.Errorf("err = %v after %d requests; want one request and a 429 error (no options = no retry)", err, atomic.LoadInt32(n))
	}
}

func TestRetryRecoversFromRateLimit(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `slow down`, ""}, {429, `slow down`, ""}, {200, `{"ok":true}`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(3, 100*time.Millisecond, 10*time.Second))
	var fc fakeClock
	fc.install(c)

	raw, err := c.Post(context.Background(), map[string]any{"type": "meta"})
	if err != nil || string(raw) != `{"ok":true}` {
		t.Fatalf("raw = %q, err = %v", raw, err)
	}
	if atomic.LoadInt32(n) != 3 {
		t.Errorf("%d requests, want 3", atomic.LoadInt32(n))
	}
	// Exponential: 100 ms then 200 ms (jitter pinned to the upper bound).
	if len(fc.sleeps) != 2 || fc.sleeps[0] != 100*time.Millisecond || fc.sleeps[1] != 200*time.Millisecond {
		t.Errorf("backoff sleeps = %v, want [100ms 200ms]", fc.sleeps)
	}
}

func TestRetryExhaustedKeepsTheVenueError(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `slow down`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(2, time.Millisecond, time.Second))
	var fc fakeClock
	fc.install(c)

	_, err := c.Post(context.Background(), map[string]any{"type": "meta"})
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != 429 || !IsRateLimited(err) {
		t.Fatalf("err = %v, want the *HTTPError 429 preserved", err)
	}
	if atomic.LoadInt32(n) != 3 {
		t.Errorf("%d requests, want 1 + 2 retries", atomic.LoadInt32(n))
	}
	if !IsRateLimited(fmt.Errorf("wrapped: %w", err)) {
		t.Error("IsRateLimited must see through wrapping")
	}
}

func TestRetryAfterRaisesTheWait(t *testing.T) {
	srv, _ := scriptedServer(t, []reply{{429, `x`, "2"}, {200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(2, 100*time.Millisecond, 10*time.Second))
	var fc fakeClock
	fc.install(c)
	if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
		t.Fatal(err)
	}
	if len(fc.sleeps) != 1 || fc.sleeps[0] != 2*time.Second {
		t.Errorf("sleeps = %v, want [2s] (Retry-After beats the 100 ms backoff)", fc.sleeps)
	}
}

func TestRetryAfterBeyondCapGivesUp(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `x`, "600"}, {200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(5, time.Millisecond, time.Second))
	var fc fakeClock
	fc.install(c)
	_, err := c.Post(context.Background(), map[string]any{"type": "meta"})
	if !IsRateLimited(err) || atomic.LoadInt32(n) != 1 || len(fc.sleeps) != 0 {
		t.Errorf("err = %v, %d requests, sleeps %v; want an immediate 429 error for a 10-minute hint", err, atomic.LoadInt32(n), fc.sleeps)
	}
}

func TestRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   int32 // requests
	}{
		{"500 retried", 500, 3},
		{"502 retried", 502, 3},
		{"503 retried", 503, 3},
		{"504 retried", 504, 3},
		{"400 not retried", 400, 1},
		{"404 not retried", 404, 1},
		{"422 not retried", 422, 1},
	} {
		srv, n := scriptedServer(t, []reply{{tc.status, `err`, ""}})
		c := NewInfoClient(srv.URL, WithRetry(2, time.Millisecond, time.Second))
		var fc fakeClock
		fc.install(c)
		_, _ = c.Post(context.Background(), map[string]any{"type": "meta"})
		if got := atomic.LoadInt32(n); got != tc.want {
			t.Errorf("%s: %d requests, want %d", tc.name, got, tc.want)
		}
	}
}

func TestNonJSON2xxIsNotRetried(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{200, `<html>captive portal</html>`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(3, time.Millisecond, time.Second))
	var fc fakeClock
	fc.install(c)
	if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err == nil || atomic.LoadInt32(n) != 1 {
		t.Errorf("err = %v after %d requests; a 2xx non-JSON body is a proxy problem, not a transient failure", err, atomic.LoadInt32(n))
	}
}

func TestTransportErrorIsRetried(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			hj, _ := w.(http.Hijacker)
			conn, _, _ := hj.Hijack()
			_ = conn.Close() // drop the connection mid-request
			return
		}
		_, _ = w.Write([]byte(`{"ok":1}`))
	}))
	defer srv.Close()
	c := NewInfoClient(srv.URL, WithRetry(2, time.Millisecond, time.Second))
	var fc fakeClock
	fc.install(c)
	raw, err := c.Post(context.Background(), map[string]any{"type": "meta"})
	if err != nil || string(raw) != `{"ok":1}` || atomic.LoadInt32(&n) != 2 {
		t.Errorf("raw = %q, err = %v, requests = %d", raw, err, atomic.LoadInt32(&n))
	}
}

func TestCancelledContextStopsRetrying(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `x`, ""}})
	c := NewInfoClient(srv.URL, WithRetry(10, time.Millisecond, time.Second))
	var fc fakeClock
	fc.install(c)
	ctx, cancel := context.WithCancel(context.Background())
	c.sleep = func(ctx context.Context, d time.Duration) error { cancel(); return ctx.Err() }
	_, err := c.Post(ctx, map[string]any{"type": "meta"})
	if !IsRateLimited(err) || atomic.LoadInt32(n) != 1 {
		t.Errorf("err = %v after %d requests; want the venue error and no further attempt once the caller gave up", err, atomic.LoadInt32(n))
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	future := now.Add(90 * time.Second).UTC().Format(http.TimeFormat)
	past := now.Add(-time.Hour).UTC().Format(http.TimeFormat)
	for in, want := range map[string]time.Duration{
		"":        0,
		"3":       3 * time.Second,
		"0":       0,
		"-5":      0,
		"1.5":     1500 * time.Millisecond,
		"garbage": 0,
		future:    90 * time.Second,
		past:      0,
		"  7  ":   7 * time.Second,
	} {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestHTTPErrorCarriesRetryAfter(t *testing.T) {
	srv, _ := scriptedServer(t, []reply{{429, `x`, "4"}})
	_, err := NewInfoClient(srv.URL).Post(context.Background(), map[string]any{"type": "meta"})
	var he *HTTPError
	if !errors.As(err, &he) || he.RetryAfter != 4*time.Second {
		t.Errorf("err = %v, want RetryAfter 4s", err)
	}
}

func TestRateLimitPacesAfterTheBurst(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRateLimit(2, 2)) // 2 req/s, burst 2
	var fc fakeClock
	fc.install(c)
	for i := 0; i < 5; i++ {
		if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
			t.Fatal(err)
		}
	}
	// The fake sleep advances the clock, so each later call waits one more
	// interval (500 ms at 2/s) after its predecessor: the burst of 2 is free.
	want := []time.Duration{500 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond}
	if len(fc.sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", fc.sleeps, want)
	}
	for i := range want {
		if fc.sleeps[i] != want[i] {
			t.Errorf("sleep %d = %v, want %v (all %v)", i, fc.sleeps[i], want[i], fc.sleeps)
		}
	}
	if atomic.LoadInt32(n) != 5 {
		t.Errorf("%d requests", atomic.LoadInt32(n))
	}
}

func TestRateLimitRefillsWhenIdleButNeverExceedsBurst(t *testing.T) {
	srv, _ := scriptedServer(t, []reply{{200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRateLimit(1, 2))
	var fc fakeClock
	fc.install(c)
	post := func() {
		if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
			t.Fatal(err)
		}
	}
	post()
	post() // burst used up, no waiting so far
	fc.t = fc.t.Add(time.Hour)
	post()
	post() // refill is capped at the burst of 2, so these two are free too
	if len(fc.sleeps) != 0 {
		t.Fatalf("sleeps = %v, want none", fc.sleeps)
	}
	post() // third in a row at the same instant must wait one interval
	if len(fc.sleeps) != 1 || fc.sleeps[0] != time.Second {
		t.Errorf("sleeps = %v, want [1s]", fc.sleeps)
	}
}

func TestRetriesConsumeRateLimitTokens(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{429, `x`, ""}, {200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRateLimit(1, 1), WithRetry(2, time.Millisecond, time.Millisecond))
	var fc fakeClock
	fc.install(c)
	if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
		t.Fatal(err)
	}
	// attempt 1: free (burst). backoff 1 ms. attempt 2: needs a token again
	// (1 s at 1/s, minus the 1 ms already elapsed) -> a retry cannot bypass the budget.
	var limiterWait time.Duration
	for _, s := range fc.sleeps {
		if s > 100*time.Millisecond {
			limiterWait = s
		}
	}
	if atomic.LoadInt32(n) != 2 || limiterWait < 900*time.Millisecond {
		t.Errorf("%d requests, sleeps %v; the retry must wait for its own token", atomic.LoadInt32(n), fc.sleeps)
	}
}

func TestRateLimitWaitHonoursContextAndReturnsTheToken(t *testing.T) {
	srv, n := scriptedServer(t, []reply{{200, `{}`, ""}})
	c := NewInfoClient(srv.URL, WithRateLimit(1, 1))
	var fc fakeClock
	fc.install(c)
	if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Post(ctx, map[string]any{"type": "meta"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled while waiting for a token", err)
	}
	if atomic.LoadInt32(n) != 1 {
		t.Errorf("%d requests, want 1 (the cancelled call must not reach the server)", atomic.LoadInt32(n))
	}
	// The cancelled call returned its reservation: the next live call waits
	// one interval, not two.
	fc.sleeps = nil
	if _, err := c.Post(context.Background(), map[string]any{"type": "meta"}); err != nil {
		t.Fatal(err)
	}
	if len(fc.sleeps) != 1 || fc.sleeps[0] != time.Second {
		t.Errorf("sleeps = %v, want [1s]", fc.sleeps)
	}
}

func TestWithRetryNormalizesOptions(t *testing.T) {
	c := NewInfoClient("http://x", WithRetry(-3, 0, 0))
	if c.maxRetries != 0 || c.backoffMin != 500*time.Millisecond || c.backoffMax != 10*time.Second {
		t.Errorf("retries %d base %v max %v", c.maxRetries, c.backoffMin, c.backoffMax)
	}
	c = NewInfoClient("http://x", WithRetry(2, 2*time.Second, time.Second))
	if c.backoffMax != 2*time.Second {
		t.Errorf("max below base must be raised to base, got %v", c.backoffMax)
	}
	if NewInfoClient("http://x", WithRateLimit(0, 5)).limiter != nil {
		t.Error("rps <= 0 must disable pacing")
	}
}

func TestBackoffCapsAndJitterBounds(t *testing.T) {
	c := NewInfoClient("http://x", WithRetry(30, 100*time.Millisecond, 5*time.Second))
	c.jitter = func(d time.Duration) time.Duration { return d }
	err := &HTTPError{StatusCode: 503}
	for attempt, want := range map[int]time.Duration{0: 100 * time.Millisecond, 1: 200 * time.Millisecond, 5: 3200 * time.Millisecond, 6: 5 * time.Second, 29: 5 * time.Second} {
		if d, ok := c.backoff(attempt, err); !ok || d != want {
			t.Errorf("backoff(%d) = %v, %v; want %v", attempt, d, ok, want)
		}
	}
	// The real jitter stays within [d/2, d].
	real := NewInfoClient("http://x")
	for i := 0; i < 1000; i++ {
		d := 800 * time.Millisecond
		if j := real.jitter(d); j < d/2 || j > d {
			t.Fatalf("jitter(%v) = %v outside [d/2, d]", d, j)
		}
	}
}
