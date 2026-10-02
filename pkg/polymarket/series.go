package polymarket

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

const (
	// seriesCacheTTL is how long a GetSeries result is cached. A series'
	// identity is stable (slug → ID), so the lookup is cached; the TTL keeps
	// the (optionally embedded) event list from going stale for too long.
	// ListSeriesEvents is live state and is never cached.
	seriesCacheTTL = time.Hour

	// defaultSeriesEventsLimit / maxSeriesEventsLimit bound
	// connector.EventQuery.Limit. A limit of 0 means the default; anything
	// above the maximum is clamped (the Gamma API caps pages at 500).
	defaultSeriesEventsLimit = 100
	maxSeriesEventsLimit     = 500

	// seriesEventsPageSize is how many events are requested per page. A limit
	// larger than this is filled over several requests; the loop stops early
	// on a short page (no more matching events on the server).
	seriesEventsPageSize = 100
)

// ─────────────────────────────────────────────────────────────
// Gamma series discovery
// ─────────────────────────────────────────────────────────────

// GetSeries fetches series by slug. Returns an empty slice (nil error) when
// the slug matches no series.
//
// The result is cached for seriesCacheTTL (see InvalidateSeries) because
// a series' ID — the key the collector schedules on — does not change. Use
// ListSeriesEvents for live event state.
func (p *PolymarketConnector) GetSeries(ctx context.Context, slug string) ([]connector.GammaSeries, error) {
	if slug == "" {
		return nil, fmt.Errorf("gamma: GetSeries: slug required")
	}
	if cached, ok := p.series.lookup(slug); ok {
		return cached, nil
	}

	series, err := p.gamma.fetchSeries(ctx, slug)
	if err != nil {
		return nil, err
	}

	p.series.store(slug, series)
	return series, nil
}

// InvalidateSeries drops the cached series lookup for slug, so the next
// GetSeries call goes back to the API.
func (p *PolymarketConnector) InvalidateSeries(slug string) {
	p.series.invalidate(slug)
}

// ListSeriesEvents lists the events of a series, in the order Gamma returns
// them for the given EventQuery.Order (newest window last by default).
//
// It paginates internally — starting at EventQuery.Offset — until Limit is
// reached, the server returns a short page, or (when Closed is explicitly
// false) a page contains only closed events. Duplicates across pages are
// dropped. The result is never cached: it is live state.
//
// A series with no matching events yields an empty slice and a nil error.
func (p *PolymarketConnector) ListSeriesEvents(ctx context.Context, q connector.EventQuery) ([]connector.GammaEvent, error) {
	if q.SeriesID == "" {
		return nil, fmt.Errorf("gamma: ListSeriesEvents: SeriesID required")
	}

	limit := q.Limit
	if limit <= 0 {
		limit = defaultSeriesEventsLimit
	}
	if limit > maxSeriesEventsLimit {
		limit = maxSeriesEventsLimit
	}

	events := make([]connector.GammaEvent, 0, limit)
	seen := make(map[string]struct{}, limit)
	openOnly := q.Closed != nil && !*q.Closed

	for offset := q.Offset; len(events) < limit; {
		pageSize := min(limit-len(events), seriesEventsPageSize)

		page, err := p.gamma.fetchSeriesEvents(ctx, q, pageSize, offset)
		if err != nil {
			return nil, err
		}

		allClosed := len(page) > 0
		for i := range page {
			ev := page[i]
			if !ev.Closed {
				allClosed = false
			}

			key := ev.ID
			if key == "" {
				key = ev.Slug
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, ev)

			if len(events) >= limit {
				break
			}
		}

		// A short page means there is nothing after it.
		if len(page) < pageSize {
			break
		}
		// With Closed=false, a page of only closed events means there is no
		// open/future window left to discover.
		if openOnly && allClosed {
			break
		}
		offset += len(page)
	}

	return events, nil
}

// GetEvent fetches a single event (with its nested markets) by slug or
// ticker. Series events use the same string for both, so the slug lookup
// covers either. Returns (nil, nil) when the slug matches no event.
func (p *PolymarketConnector) GetEvent(ctx context.Context, slug string) (*connector.GammaEvent, error) {
	if slug == "" {
		return nil, fmt.Errorf("gamma: GetEvent: slug required")
	}

	events, err := p.gamma.fetchEventsBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &events[0], nil
}

// ─────────────────────────────────────────────────────────────
// Gamma HTTP (series/event endpoints)
// ─────────────────────────────────────────────────────────────

// fetchSeries queries GET /series?slug=<slug>.
func (g *GammaClient) fetchSeries(ctx context.Context, slug string) ([]connector.GammaSeries, error) {
	v := url.Values{}
	v.Set("slug", slug)

	var series []connector.GammaSeries
	if err := g.doJSON(ctx, g.baseURL+"/series?"+v.Encode(), &series); err != nil {
		return nil, err
	}
	return series, nil
}

// fetchEventsBySlug queries GET /events?slug=<slug>.
func (g *GammaClient) fetchEventsBySlug(ctx context.Context, slug string) ([]connector.GammaEvent, error) {
	v := url.Values{}
	v.Set("slug", slug)
	return g.fetchEvents(ctx, v)
}

// fetchSeriesEvents queries GET /events?series_id=<id> for one page.
func (g *GammaClient) fetchSeriesEvents(ctx context.Context, q connector.EventQuery, limit, offset int) ([]connector.GammaEvent, error) {
	v := url.Values{}
	v.Set("series_id", q.SeriesID)
	if q.Closed != nil {
		v.Set("closed", strconv.FormatBool(*q.Closed))
	}
	v.Set("limit", strconv.Itoa(limit))
	v.Set("offset", strconv.Itoa(offset))
	if q.Order != "" {
		v.Set("order", q.Order)
		v.Set("ascending", strconv.FormatBool(q.Ascending))
	}
	return g.fetchEvents(ctx, v)
}

func (g *GammaClient) fetchEvents(ctx context.Context, v url.Values) ([]connector.GammaEvent, error) {
	var events []connector.GammaEvent
	if err := g.doJSON(ctx, g.baseURL+"/events?"+v.Encode(), &events); err != nil {
		return nil, err
	}
	return events, nil
}

// ─────────────────────────────────────────────────────────────
// Series cache (internal)
// ─────────────────────────────────────────────────────────────

type seriesCacheEntry struct {
	series    []connector.GammaSeries
	expiresAt time.Time
}

// seriesCache caches Gamma series lookups by slug. Its zero value is usable.
//
// Access is guarded by mu: the collector runs discovery from a scheduler
// goroutine while the WebSocket clients are live.
type seriesCache struct {
	mu      sync.Mutex
	ttl     time.Duration // 0 = seriesCacheTTL (tests may shorten it)
	entries map[string]seriesCacheEntry
}

func (c *seriesCache) lookup(slug string) ([]connector.GammaSeries, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[slug]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(c.entries, slug)
		return nil, false
	}
	return cloneSeries(entry.series), true
}

func (c *seriesCache) store(slug string, series []connector.GammaSeries) {
	ttl := c.ttl
	if ttl <= 0 {
		ttl = seriesCacheTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]seriesCacheEntry)
	}
	c.entries[slug] = seriesCacheEntry{
		series:    cloneSeries(series),
		expiresAt: time.Now().Add(ttl),
	}
}

func (c *seriesCache) invalidate(slug string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, slug)
}

// cloneSeries copies the slice (and each series' embedded events) so callers
// cannot mutate cached state.
func cloneSeries(in []connector.GammaSeries) []connector.GammaSeries {
	if in == nil {
		return nil
	}
	out := make([]connector.GammaSeries, len(in))
	copy(out, in)
	for i := range out {
		if in[i].Events != nil {
			out[i].Events = append([]connector.GammaEvent(nil), in[i].Events...)
		}
	}
	return out
}
