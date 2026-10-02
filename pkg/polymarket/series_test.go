package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// ─────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────

// gammaRecorder captures the requests a fake Gamma server received.
type gammaRecorder struct {
	mu      sync.Mutex
	paths   []string
	queries []url.Values
	uas     []string
}

func (r *gammaRecorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = append(r.paths, req.URL.Path)
	r.queries = append(r.queries, req.URL.Query())
	r.uas = append(r.uas, req.Header.Get("User-Agent"))
}

func (r *gammaRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.paths)
}

// query returns the query values of request i (0-based).
func (r *gammaRecorder) query(t *testing.T, i int) url.Values {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.queries) {
		t.Fatalf("request %d not recorded (%d total)", i, len(r.queries))
	}
	return r.queries[i]
}

// newGammaServer serves body for every request and records what it received.
func newGammaServer(body string) (*httptest.Server, *gammaRecorder) {
	rec := &gammaRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	return srv, rec
}

// newEventsServer serves a fixed dataset with limit/offset slicing, mirroring
// how GET /events?series_id=… paginates.
func newEventsServer(dataset []string) (*httptest.Server, *gammaRecorder) {
	rec := &gammaRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = len(dataset)
		}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		start := min(offset, len(dataset))
		end := min(start+limit, len(dataset))

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "["+strings.Join(dataset[start:end], ",")+"]")
	}))
	return srv, rec
}

// gammaEventJSON renders one event in the Gamma wire shape.
func gammaEventJSON(id, slug string, closed bool) string {
	return fmt.Sprintf(`{"id":%q,"ticker":%q,"slug":%q,"title":"t","seriesSlug":"btc-up-or-down-daily",`+
		`"startDate":"2026-09-26T16:00:15Z","endDate":"2026-10-03T16:00:00Z","closed":%t,"active":true,"negRisk":false,`+
		`"markets":[{"id":%q,"slug":%q,"groupItemTitle":"74,000","outcomes":"[\"Yes\", \"No\"]",`+
		`"clobTokenIds":"[\"yes-%s\", \"no-%s\"]","closed":%t,"active":true,"negRisk":false,"negRiskMarketID":null}]}`,
		id, slug, slug, closed, "m-"+id, "m-"+slug, id, id, closed)
}

// eventDataset builds n events with increasing ids.
func eventDataset(n int, closed bool) []string {
	out := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		id := strconv.Itoa(i)
		out = append(out, gammaEventJSON(id, "event-"+id, closed))
	}
	return out
}

// newSeriesTestConnector builds a connector whose Gamma client points at url.
func newSeriesTestConnector(url string) *PolymarketConnector {
	return New(false, Config{GammaAPIURL: url}, nil)
}

const seriesBody = `[{"id":"41","ticker":"btc-up-or-down-daily","slug":"btc-up-or-down-daily",` +
	`"title":"BTC Up or Down Daily","seriesType":"single","recurrence":"daily",` +
	`"active":true,"closed":false,"archived":false}]`

// ─────────────────────────────────────────────────────────────
// GetSeries
// ─────────────────────────────────────────────────────────────

func TestGetSeriesQueriesAndCaches(t *testing.T) {
	srv, rec := newGammaServer(seriesBody)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	first, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily")
	if err != nil {
		t.Fatalf("GetSeries: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("series = %d, want 1", len(first))
	}
	if first[0].ID != "41" || first[0].Recurrence != connector.RecurrenceDaily {
		t.Errorf("bad series: %+v", first[0])
	}

	// Request shape: GET /series?slug=… with a User-Agent (Gamma 403s without one).
	if got := rec.paths[0]; got != "/series" {
		t.Errorf("path = %q, want /series", got)
	}
	if got := rec.query(t, 0).Get("slug"); got != "btc-up-or-down-daily" {
		t.Errorf("slug = %q, want btc-up-or-down-daily", got)
	}
	if ua := rec.uas[0]; !strings.HasPrefix(ua, "go-exchange-connector/") {
		t.Errorf("User-Agent = %q, want go-exchange-connector/<version>", ua)
	}

	// Second call is served from the cache.
	second, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily")
	if err != nil {
		t.Fatalf("GetSeries (cached): %v", err)
	}
	if rec.count() != 1 {
		t.Errorf("requests = %d, want 1 (cached)", rec.count())
	}

	// Cached results are copies — mutating one must not corrupt the cache.
	second[0].Slug = "mutated"
	third, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily")
	if err != nil {
		t.Fatalf("GetSeries (cached): %v", err)
	}
	if third[0].Slug != "btc-up-or-down-daily" {
		t.Errorf("cached series was mutated: %q", third[0].Slug)
	}

	// InvalidateSeries forces a refetch.
	conn.InvalidateSeries("btc-up-or-down-daily")
	if _, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily"); err != nil {
		t.Fatalf("GetSeries (after invalidate): %v", err)
	}
	if rec.count() != 2 {
		t.Errorf("requests = %d, want 2 after InvalidateSeries", rec.count())
	}
}

func TestGetSeriesCacheExpires(t *testing.T) {
	srv, rec := newGammaServer(seriesBody)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)
	conn.series.ttl = time.Millisecond

	if _, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily"); err != nil {
		t.Fatalf("GetSeries: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily"); err != nil {
		t.Fatalf("GetSeries (expired): %v", err)
	}

	if rec.count() != 2 {
		t.Errorf("requests = %d, want 2 (entry expired)", rec.count())
	}
}

func TestGetSeriesNotFound(t *testing.T) {
	srv, _ := newGammaServer(`[]`)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)
	series, err := conn.GetSeries(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("GetSeries: %v, want nil error for a miss", err)
	}
	if len(series) != 0 {
		t.Errorf("series = %v, want empty", series)
	}
}

func TestGetSeriesRequiresSlug(t *testing.T) {
	conn := newSeriesTestConnector("http://example.invalid")
	if _, err := conn.GetSeries(context.Background(), ""); err == nil {
		t.Error("GetSeries(\"\") = nil error, want error")
	}
}

// ─────────────────────────────────────────────────────────────
// ListSeriesEvents
// ─────────────────────────────────────────────────────────────

func TestListSeriesEventsPaginates(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(120, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)
	closed := false

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{
		SeriesID: "45",
		Closed:   &closed,
		Limit:    150,
	})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}

	// 100 from the first page, 20 (short page) from the second.
	if len(events) != 120 {
		t.Errorf("events = %d, want 120", len(events))
	}
	if rec.count() != 2 {
		t.Fatalf("requests = %d, want 2 (100 + 20)", rec.count())
	}

	q0 := rec.query(t, 0)
	if q0.Get("series_id") != "45" || q0.Get("closed") != "false" ||
		q0.Get("limit") != "100" || q0.Get("offset") != "0" {
		t.Errorf("first request query = %v", q0)
	}
	q1 := rec.query(t, 1)
	if q1.Get("offset") != "100" {
		t.Errorf("second request offset = %q, want 100", q1.Get("offset"))
	}

	// Events are returned in order, with no duplicates.
	seen := make(map[string]bool, len(events))
	for i, ev := range events {
		if seen[ev.ID] {
			t.Fatalf("duplicate event id %q", ev.ID)
		}
		seen[ev.ID] = true
		if want := strconv.Itoa(i + 1); ev.ID != want {
			t.Fatalf("event %d id = %q, want %q", i, ev.ID, want)
		}
	}
}

func TestListSeriesEventsRespectsLimit(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(500, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{SeriesID: "45", Limit: 7})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}
	if len(events) != 7 {
		t.Errorf("events = %d, want 7", len(events))
	}
	if rec.count() != 1 {
		t.Errorf("requests = %d, want 1", rec.count())
	}
	if got := rec.query(t, 0).Get("limit"); got != "7" {
		t.Errorf("limit = %q, want 7", got)
	}
	// Closed was nil → the parameter must be omitted (server default).
	if _, ok := rec.query(t, 0)["closed"]; ok {
		t.Errorf("closed sent for nil pointer: %v", rec.query(t, 0))
	}
}

func TestListSeriesEventsClampsLimit(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(1000, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{SeriesID: "45", Limit: 9999})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}
	if len(events) != maxSeriesEventsLimit {
		t.Errorf("events = %d, want %d (limit clamped)", len(events), maxSeriesEventsLimit)
	}
	if rec.count() != 5 {
		t.Errorf("requests = %d, want 5 (500 / 100 per page)", rec.count())
	}
}

func TestListSeriesEventsDropsDuplicates(t *testing.T) {
	// Page 2 repeats the last id of page 1 (as an unstable server might).
	dataset := eventDataset(120, false)
	dataset[100] = dataset[99]

	srv, _ := newEventsServer(dataset)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{SeriesID: "45", Limit: 150})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}
	seen := make(map[string]bool, len(events))
	for _, ev := range events {
		if seen[ev.ID] {
			t.Fatalf("duplicate event id %q returned", ev.ID)
		}
		seen[ev.ID] = true
	}
	if len(events) != 119 {
		t.Errorf("events = %d, want 119 (duplicate dropped)", len(events))
	}
}

func TestListSeriesEventsStopsOnAllClosedPage(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(300, true))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)
	closed := false

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{
		SeriesID: "45",
		Closed:   &closed,
		Limit:    300,
	})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}
	if len(events) != 100 {
		t.Errorf("events = %d, want 100 (stopped after an all-closed page)", len(events))
	}
	if rec.count() != 1 {
		t.Errorf("requests = %d, want 1 (no second page requested)", rec.count())
	}
}

func TestListSeriesEventsSendsOrderParams(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(3, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	if _, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{
		SeriesID:  "45",
		Limit:     3,
		Order:     "endDate",
		Ascending: true,
	}); err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}

	q := rec.query(t, 0)
	if q.Get("order") != "endDate" || q.Get("ascending") != "true" {
		t.Errorf("order params = %v", q)
	}
}

func TestListSeriesEventsOffset(t *testing.T) {
	srv, rec := newEventsServer(eventDataset(10, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	events, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{
		SeriesID: "45",
		Limit:    2,
		Offset:   4,
	})
	if err != nil {
		t.Fatalf("ListSeriesEvents: %v", err)
	}
	if len(events) != 2 || events[0].ID != "5" || events[1].ID != "6" {
		t.Errorf("events = %v, want ids 5,6", events)
	}
	if got := rec.query(t, 0).Get("offset"); got != "4" {
		t.Errorf("offset = %q, want 4", got)
	}
}

func TestListSeriesEventsRequiresSeriesID(t *testing.T) {
	conn := newSeriesTestConnector("http://example.invalid")
	if _, err := conn.ListSeriesEvents(context.Background(), connector.EventQuery{}); err == nil {
		t.Error("ListSeriesEvents without SeriesID = nil error, want error")
	}
}

func TestListSeriesEventsContextCancellation(t *testing.T) {
	srv, _ := newEventsServer(eventDataset(10, false))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := conn.ListSeriesEvents(ctx, connector.EventQuery{SeriesID: "45"})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// ─────────────────────────────────────────────────────────────
// GetEvent
// ─────────────────────────────────────────────────────────────

func TestGetEventReturnsNestedMarkets(t *testing.T) {
	srv, rec := newGammaServer("[" + gammaEventJSON("1086127", "bitcoin-above-on-october-3-2026", false) + "]")
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	ev, err := conn.GetEvent(context.Background(), "bitcoin-above-on-october-3-2026")
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if ev == nil {
		t.Fatal("GetEvent = nil, want event")
	}
	if ev.ID != "1086127" || ev.SeriesSlug != "btc-up-or-down-daily" {
		t.Errorf("bad event: %+v", ev)
	}
	if len(ev.Markets) != 1 {
		t.Fatalf("markets = %d, want 1", len(ev.Markets))
	}

	strike := ev.Markets[0]
	if strike.GroupItemTitle != "74,000" {
		t.Errorf("GroupItemTitle = %q, want 74,000", strike.GroupItemTitle)
	}
	tokens, err := strike.TokenIDList()
	if err != nil {
		t.Fatalf("TokenIDList: %v", err)
	}
	if len(tokens) != 2 || tokens[0] != "yes-1086127" || tokens[1] != "no-1086127" {
		t.Errorf("token ids = %v", tokens)
	}
	if got := rec.query(t, 0).Get("slug"); got != "bitcoin-above-on-october-3-2026" {
		t.Errorf("slug = %q", got)
	}
}

func TestGetEventNotFound(t *testing.T) {
	srv, _ := newGammaServer(`[]`)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)
	ev, err := conn.GetEvent(context.Background(), "does-not-exist")
	if err != nil {
		t.Fatalf("GetEvent: %v, want nil error for a miss", err)
	}
	if ev != nil {
		t.Errorf("GetEvent = %+v, want nil", ev)
	}
}

func TestGetEventRequiresSlug(t *testing.T) {
	conn := newSeriesTestConnector("http://example.invalid")
	if _, err := conn.GetEvent(context.Background(), ""); err == nil {
		t.Error("GetEvent(\"\") = nil error, want error")
	}
}

// ─────────────────────────────────────────────────────────────
// Transport behaviour
// ─────────────────────────────────────────────────────────────

func TestGammaHTTPErrorIsTyped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
	}))
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	_, err := conn.GetSeries(context.Background(), "btc-up-or-down-daily")
	if err == nil {
		t.Fatal("expected error for 403")
	}

	var httpErr *GammaHTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("err = %v (%T), want *GammaHTTPError", err, err)
	}
	if httpErr.StatusCode != http.StatusForbidden {
		t.Errorf("StatusCode = %d, want 403", httpErr.StatusCode)
	}
	if !strings.Contains(httpErr.URL, "/series?slug=btc-up-or-down-daily") {
		t.Errorf("URL = %q, want the /series request URL", httpErr.URL)
	}
	if httpErr.IsNotFound() {
		t.Error("IsNotFound() = true for a 403")
	}
}

func TestGetMarketSendsUserAgent(t *testing.T) {
	body := `{"id":"1","conditionId":"0xc","slug":"s","question":"q","outcomes":"[\"Yes\",\"No\"]",` +
		`"clobTokenIds":"[\"yes1\",\"no1\"]","closed":false,"orderPriceMinTickSize":0.01,"negRisk":false,` +
		`"groupItemTitle":"80,000","description":"rule","negRiskMarketID":"0xnr",` +
		`"events":[{"id":"9","slug":"ev-slug","ticker":"ev-ticker"}],` +
		`"startDate":"2026-09-26T16:00:15Z","endDate":"2026-10-03T16:00:00Z"}`

	srv, rec := newGammaServer(body)
	defer srv.Close()

	conn := newSeriesTestConnector(srv.URL)

	m, err := conn.GetMarket("1", "")
	if err != nil {
		t.Fatalf("GetMarket: %v", err)
	}
	if ua := rec.uas[0]; !strings.HasPrefix(ua, "go-exchange-connector/") {
		t.Errorf("User-Agent = %q, want go-exchange-connector/<version>", ua)
	}
	// Ladder metadata is mapped onto connector.Market.
	if m.GroupItemTitle != "80,000" || m.NegRiskMarketID != "0xnr" ||
		m.EventSlug != "ev-slug" || m.EventTicker != "ev-ticker" ||
		m.Description != "rule" {
		t.Errorf("market metadata not mapped: %+v", m)
	}
	if m.YesAssetID != "yes1" || m.NoAssetID != "no1" {
		t.Errorf("token ids = %q/%q", m.YesAssetID, m.NoAssetID)
	}
	if want := mustTime(t, "2026-10-03T16:00:00Z"); !m.EndDate.Equal(want) {
		t.Errorf("EndDate = %v, want %v", m.EndDate, want)
	}
}

func TestGammaResponseIsJSON(t *testing.T) {
	// Guards the wire tags of the new root types against the recorded live shape.
	var series []connector.GammaSeries
	if err := json.Unmarshal([]byte(seriesBody), &series); err != nil {
		t.Fatalf("unmarshal series: %v", err)
	}
	if len(series) != 1 || series[0].Slug != "btc-up-or-down-daily" {
		t.Errorf("bad series: %+v", series)
	}
}
