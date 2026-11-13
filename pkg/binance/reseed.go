package binance

// reseed.go is the gate in front of GAP-TRIGGERED depth re-seeds (H4c, D1).
//
// Before it, every diff that revealed a gap (or arrived for a book that was not
// seeded yet) spawned its own goroutine and its own REST GET, so with a consumer
// that cannot keep up the stream is permanently gapped and the fetch rate
// tracked the frame rate: the H4a soak measured 2,201-2,766 snapshot calls in
// 60 s for ONE symbol (4,400-5,500 weight/min against a 2,400/min IP budget).
//
// The gate bounds that with four rules that all apply to gap-triggered re-seeds
// only — the initial per-connect seeding (snapshotDepthBooks) does not pass
// through here and is unchanged:
//
//   - single-flight per book: at most one fetch in flight per (market, symbol);
//     further triggers while one is pending are coalesced, counted, not spawned;
//   - per-symbol minimum interval (Options.DepthReseedMinInterval, 1 s default);
//   - a sliding one-minute budget shared by every symbol and both market types
//     of one WSBinance (Options.DepthReseedPerMin, 300 default);
//   - after an HTTP 429/418, every gap-triggered re-seed is paused for the
//     response's Retry-After (60 s if absent or unparseable).
//
// A trigger the gate refuses is DEFERRED: the book stays unseeded, diffs for it
// are dropped exactly as before (they are already counted as no-event frames),
// and the next diff after the interval/budget allows one triggers the fetch. A
// failed fetch does not retry on its own — the same gate delays it until a later
// diff — so an outage cannot turn into a retry loop.

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// defaultDepthReseedMinInterval is the floor between two gap-triggered
	// re-seeds of the same symbol when Options.DepthReseedMinInterval is unset.
	defaultDepthReseedMinInterval = time.Second

	// defaultDepthReseedPerMin is the default shared budget for gap-triggered
	// re-seeds when Options.DepthReseedPerMin is unset. Callers should set it
	// to floor(IP weight budget / weight of one depth request): a spot or USDⓈ-M
	// depth?limit=50 request costs weight 2, so 300/min spends 600 of the
	// 2,400/min IP budget and leaves room for everything else on that IP.
	defaultDepthReseedPerMin = 300

	// defaultDepthReseedPause is how long a 429/418 pauses gap-triggered
	// re-seeds when the response carries no usable Retry-After.
	defaultDepthReseedPause = 60 * time.Second
)

// DepthReseedStats is the decision ledger of the gap-triggered depth re-seed
// gate. Every trigger is counted exactly once in Triggers and exactly once in
// Fetched, Coalesced or Deferred (Fetched includes the fetches that later
// failed). Failed and Paused are separate counters, not trigger outcomes.
type DepthReseedStats struct {
	// Triggers is every gap-triggered re-seed request (one per diff that found
	// the book unseeded or gapped).
	Triggers uint64
	// Fetched is every re-seed the gate admitted (and so spawned).
	Fetched uint64
	// Coalesced is every trigger dropped because a fetch for the same book was
	// already in flight.
	Coalesced uint64
	// Deferred is every trigger the gate refused because of the per-symbol
	// minimum interval, the shared per-minute budget, or a 429/418 pause.
	Deferred uint64
	// Failed is every admitted fetch that returned an error (transport, non-200,
	// decode).
	Failed uint64
	// Paused is every time an HTTP 429/418 opened or extended a pause.
	Paused uint64
}

// depthReseedKey names one book.
type depthReseedKey struct {
	mkt    MarketType
	symbol string
}

// depthReseedGate enforces the four rules above. Its zero value is not usable;
// build it with newDepthReseedGate.
type depthReseedGate struct {
	minInterval time.Duration
	perMin      int
	now         func() time.Time

	mu          sync.Mutex
	inflight    map[depthReseedKey]struct{}
	lastStart   map[depthReseedKey]time.Time
	window      []time.Time // start times inside the last minute
	pausedUntil time.Time

	triggers  atomic.Uint64
	fetched   atomic.Uint64
	coalesced atomic.Uint64
	deferred  atomic.Uint64
	failed    atomic.Uint64
	paused    atomic.Uint64
}

func newDepthReseedGate(minInterval time.Duration, perMin int) *depthReseedGate {
	if minInterval <= 0 {
		minInterval = defaultDepthReseedMinInterval
	}
	if perMin <= 0 {
		perMin = defaultDepthReseedPerMin
	}
	return &depthReseedGate{
		minInterval: minInterval,
		perMin:      perMin,
		now:         time.Now,
		inflight:    make(map[depthReseedKey]struct{}),
		lastStart:   make(map[depthReseedKey]time.Time),
	}
}

// admit records one trigger and reports whether it may spawn a fetch now.
// Coalesced/Deferred are incremented here so the ledger always balances.
func (g *depthReseedGate) admit(key depthReseedKey) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.triggers.Add(1)

	if _, busy := g.inflight[key]; busy {
		g.coalesced.Add(1)
		return false
	}

	now := g.now()
	if now.Before(g.pausedUntil) {
		g.deferred.Add(1)
		return false
	}

	// Sliding window: a fetch counts against the budget for a full minute.
	cut := now.Add(-time.Minute)
	drop := 0
	for drop < len(g.window) && !g.window[drop].After(cut) {
		drop++
	}
	if drop > 0 {
		g.window = g.window[drop:]
	}
	if len(g.window) >= g.perMin {
		g.deferred.Add(1)
		return false
	}

	if last, ok := g.lastStart[key]; ok && now.Sub(last) < g.minInterval {
		g.deferred.Add(1)
		return false
	}

	g.inflight[key] = struct{}{}
	g.lastStart[key] = now
	g.window = append(g.window, now)
	g.fetched.Add(1)
	return true
}

// finish releases the single-flight slot of an admitted fetch. A positive pause
// (the Retry-After of a 429/418) additionally stops every gap-triggered re-seed
// until it elapses; it returns true when this response opened or extended the
// pause, which is when the caller logs it once.
func (g *depthReseedGate) finish(key depthReseedKey, failed bool, pause time.Duration) (paused bool) {
	g.mu.Lock()
	delete(g.inflight, key)
	if pause > 0 {
		until := g.now().Add(pause)
		if until.After(g.pausedUntil) {
			g.pausedUntil = until
			paused = true
		}
	}
	g.mu.Unlock()
	if failed {
		g.failed.Add(1)
	}
	if paused {
		g.paused.Add(1)
	}
	return paused
}

// stats returns a consistent-enough snapshot of the ledger: the four trigger
// counters are read under the lock so Triggers == Fetched+Coalesced+Deferred
// holds for the returned values.
func (g *depthReseedGate) stats() DepthReseedStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return DepthReseedStats{
		Triggers:  g.triggers.Load(),
		Fetched:   g.fetched.Load(),
		Coalesced: g.coalesced.Load(),
		Deferred:  g.deferred.Load(),
		Failed:    g.failed.Load(),
		Paused:    g.paused.Load(),
	}
}

// depthSnapshotHTTPError is a non-200 depth-snapshot response. It carries the
// status and, for 429/418, the pause the gate must apply.
type depthSnapshotHTTPError struct {
	Market     MarketType
	Symbol     string
	Status     int
	RetryAfter time.Duration
}

func (e *depthSnapshotHTTPError) Error() string {
	return fmt.Sprintf("binance %s depth snapshot %s: status %d", e.Market, e.Symbol, e.Status)
}

// retryAfterPause parses a Retry-After header (delta-seconds form) for a
// rate-limit response, falling back to the documented default. It returns 0 for
// any status other than 429/418, so a server error never pauses the gate.
func retryAfterPause(status int, header http.Header) time.Duration {
	if status != http.StatusTooManyRequests && status != 418 {
		return 0
	}
	if v := header.Get("Retry-After"); v != "" {
		if secs, err := strconv.ParseInt(v, 10, 64); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultDepthReseedPause
}
