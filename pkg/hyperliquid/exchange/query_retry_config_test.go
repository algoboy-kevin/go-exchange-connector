package exchange

import (
	"context"
	"testing"
	"time"
)

// Config.InfoRetry: nil keeps the default budget; a pointer to 0 must return a
// 429 on the first attempt with no wait (the quoting path sets this).

func newClientWithInfoRetry(t *testing.T, url string, retries *int) *Client {
	t.Helper()
	c, err := New(context.Background(), Config{
		BaseURL:   url,
		AgentKey:  testAgentKey,
		Account:   testAccount,
		InfoRetry: retries,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestInfoRetryZero429IsNotRetried(t *testing.T) {
	zero := 0
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newClientWithInfoRetry(t, url, &zero)
	waits := recordWaits(t, 0, nil)

	_, err := c.OpenOrdersDex(context.Background(), "")
	if err == nil {
		t.Fatal("OpenOrdersDex succeeded on a 429; want the rate-limit error")
	}
	if !IsRateLimited(err) {
		t.Fatalf("err = %v; want a rate-limited error", err)
	}
	if q, _ := s.count(); q != 1 {
		t.Fatalf("queries = %d, want exactly 1 (no retry with InfoRetry=0)", q)
	}
	if len(*waits) != 0 {
		t.Fatalf("waits = %v, want none", *waits)
	}
}

func TestInfoRetryNegativeIsTreatedAsZero(t *testing.T) {
	neg := -5
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newClientWithInfoRetry(t, url, &neg)
	recordWaits(t, 0, nil)

	_, _ = c.OpenOrdersDex(context.Background(), "")
	if q, _ := s.count(); q != 1 {
		t.Fatalf("queries = %d, want 1 for a negative budget", q)
	}
}

func TestInfoRetryOneAllowsExactlyOneRetry(t *testing.T) {
	one := 1
	s, url := newScriptedOrders(t, oneOrderBody, 429)
	c := newClientWithInfoRetry(t, url, &one)
	waits := recordWaits(t, 0, nil)

	_, _ = c.OpenOrdersDex(context.Background(), "")
	if q, _ := s.count(); q != 2 {
		t.Fatalf("queries = %d, want 2 (1 + 1 retry)", q)
	}
	if len(*waits) != 1 || (*waits)[0] != 500*time.Millisecond {
		t.Fatalf("waits = %v, want [500ms]", *waits)
	}
}
