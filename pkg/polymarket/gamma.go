package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

const defaultGammaAPIURL = "https://gamma-api.polymarket.com"

// gammaUserAgent identifies this client to Gamma. A User-Agent header is
// mandatory on the series/event endpoints — Gamma answers 403 Forbidden
// without one — so every request sent through doJSON sets it.
var gammaUserAgent = "go-exchange-connector/" + connector.Version

// GammaHTTPError is a non-2xx response from the Gamma API. It carries the
// status code and the request URL so callers can distinguish "not found" from
// rate limiting or server errors without parsing the message.
type GammaHTTPError struct {
	StatusCode int
	URL        string
	Body       string // truncated response body, for diagnostics
}

func (e *GammaHTTPError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("gamma: %s returned %d: %s", e.URL, e.StatusCode, e.Body)
	}
	return fmt.Sprintf("gamma: %s returned %d", e.URL, e.StatusCode)
}

// IsNotFound reports whether the error is a 404 from the Gamma API.
func (e *GammaHTTPError) IsNotFound() bool { return e.StatusCode == http.StatusNotFound }

// GammaClient handles Polymarket Gamma API requests for market metadata.
type GammaClient struct {
	baseURL string
	http    *http.Client
}

// NewGammaClient creates a new Gamma API client.
func NewGammaClient(baseURL string) *GammaClient {
	if baseURL == "" {
		baseURL = defaultGammaAPIURL
	}
	return &GammaClient{
		baseURL: baseURL,
		http:    &http.Client{},
	}
}

// FetchMarketByID fetches a market by its Gamma ID.
func (g *GammaClient) FetchMarketByID(id string) (*GammaMarket, error) {
	return g.fetch(fetchParams{ID: id})
}

// FetchMarketBySlug fetches a market by its slug.
func (g *GammaClient) FetchMarketBySlug(slug string) (*GammaMarket, error) {
	return g.fetch(fetchParams{Slug: slug})
}

// ToConnectorMarket converts a GammaMarket to the SDK's connector.Market.
func (g *GammaClient) ToConnectorMarket(gm *GammaMarket) *connector.Market {
	if gm == nil {
		return nil
	}
	m := &connector.Market{
		ID:              gm.ID,
		Slug:            gm.Slug,
		Question:        gm.Question,
		ConditionID:     gm.ConditionID,
		YesAssetID:      gm.YesTokenID,
		NoAssetID:       gm.NoTokenID,
		Outcomes:        gm.Outcomes,
		TickSize:        gm.TickSize,
		GroupItemTitle:  gm.GroupItemTitle,
		NegRisk:         gm.NegRisk,
		NegRiskMarketID: gm.NegRiskMarketID,
		EventSlug:       gm.EventSlug,
		EventTicker:     gm.EventTicker,
		Description:     gm.Description,
		StartDate:       gm.StartDate,
		EndDate:         gm.EndDate,
	}
	if gm.Resolution != nil {
		m.IsResolved = true
		res := connector.Resolution(*gm.Resolution)
		m.Resolution = &res
	}
	return m
}

// ─────────────────────────────────────────────────────────────
// Internal
// ─────────────────────────────────────────────────────────────

// doJSON performs a GET against the Gamma API and decodes the JSON response
// into out. Every request carries a User-Agent (required — Gamma returns 403
// without one) and honours ctx cancellation. Non-2xx responses are returned
// as *GammaHTTPError.
func (g *GammaClient) doJSON(ctx context.Context, rawURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("gamma: build request: %w", err)
	}
	req.Header.Set("User-Agent", gammaUserAgent)
	req.Header.Set("Accept", "application/json")

	slog.Debug("gamma: querying", "url", rawURL)

	resp, err := g.http.Do(req)
	if err != nil {
		return fmt.Errorf("gamma: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &GammaHTTPError{
			StatusCode: resp.StatusCode,
			URL:        rawURL,
			Body:       strings.TrimSpace(string(body)),
		}
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("gamma: decode %s: %w", rawURL, err)
	}
	return nil
}

type fetchParams struct {
	ID   string
	Slug string
}

func (g *GammaClient) fetch(params fetchParams) (*GammaMarket, error) {
	var query string
	switch {
	case params.ID != "":
		query = fmt.Sprintf("%s/markets/%s", g.baseURL, params.ID)
	case params.Slug != "":
		query = fmt.Sprintf("%s/markets/slug/%s", g.baseURL, params.Slug)
	default:
		return nil, fmt.Errorf("gamma: must provide id or slug")
	}

	var raw RawGammaMarket
	if err := g.doJSON(context.Background(), query, &raw); err != nil {
		return nil, err
	}

	return normalizeGammaMarket(&raw), nil
}

func normalizeGammaMarket(raw *RawGammaMarket) *GammaMarket {
	var outcomes []string
	if raw.Outcomes != "" {
		_ = json.Unmarshal([]byte(raw.Outcomes), &outcomes)
	}
	if outcomes == nil {
		outcomes = []string{"Yes", "No"}
	}

	startDate := parseGammaTime(raw.StartDate)
	endDate := parseGammaTime(raw.EndDate)

	var tokenIDs []string
	if raw.ClobTokenIDs != "" {
		_ = json.Unmarshal([]byte(raw.ClobTokenIDs), &tokenIDs)
	}

	yesID := ""
	noID := ""
	if len(tokenIDs) >= 2 {
		yesID = tokenIDs[0]
		noID = tokenIDs[1]
	}

	var resolution *string
	if raw.Closed {
		res := resolveFromOutcomePrices(raw.OutcomePrices)
		resolution = &res
	}

	// NegRisk defaults to false if the API omits it.
	negRisk := false
	if raw.NegRisk != nil {
		negRisk = *raw.NegRisk
	}

	// GroupItemTitle is the strike label for ladder markets (e.g. "80,000").
	// The enclosing event is referenced by a nested events array; a ladder
	// event's markets all share it, which is how rungs are grouped.
	eventSlug, eventTicker := "", ""
	if len(raw.Events) > 0 {
		eventSlug = raw.Events[0].Slug
		eventTicker = raw.Events[0].Ticker
	}

	return &GammaMarket{
		ID:              raw.ID,
		ConditionID:     raw.ConditionID,
		Slug:            raw.Slug,
		Question:        raw.Question,
		GroupItemTitle:  raw.GroupItemTitle,
		Description:     raw.Description,
		Outcomes:        outcomes,
		YesTokenID:      yesID,
		NoTokenID:       noID,
		TickSize:        raw.TickSize,
		NegRisk:         negRisk,
		NegRiskMarketID: raw.NegRiskMarketID,
		EventSlug:       eventSlug,
		EventTicker:     eventTicker,
		Resolution:      resolution,
		StartDate:       startDate,
		EndDate:         endDate,
	}
}

// parseGammaTime parses a Gamma API timestamp (RFC3339, e.g.
// "2026-08-21T11:40:00Z" or "2026-04-27T21:55:14.576Z"). Returns the zero
// time on empty or unparseable input.
func parseGammaTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		slog.Debug("gamma: unparseable timestamp", "value", s, "err", err)
		return time.Time{}
	}
	return t
}

func resolveFromOutcomePrices(outcomePrices string) string {
	var prices []string
	if err := json.Unmarshal([]byte(outcomePrices), &prices); err != nil {
		return "NO"
	}
	if len(prices) < 2 {
		return "NO"
	}
	// prices[0] = YES probability, prices[1] = NO probability
	if len(prices) > 0 {
		var yesPrice float64
		if err := json.Unmarshal([]byte(prices[0]), &yesPrice); err == nil && yesPrice >= 0.5 {
			return "YES"
		}
	}
	return "NO"
}
