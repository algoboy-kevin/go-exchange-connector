package polymarket

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/algoboy-kevin/go-exchange-connector"
)

const defaultGammaAPIURL = "https://gamma-api.polymarket.com"

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
		ID:          gm.ID,
		Slug:        gm.Slug,
		Question:    gm.Question,
		ConditionID: gm.ConditionID,
		YesAssetID:  gm.YesTokenID,
		NoAssetID:   gm.NoTokenID,
		Outcomes:    gm.Outcomes,
		TickSize:    gm.TickSize,
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

	slog.Debug("gamma: querying", "url", query)

	resp, err := g.http.Get(query)
	if err != nil {
		return nil, fmt.Errorf("gamma: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gamma: %s returned %d", query, resp.StatusCode)
	}

	var raw RawGammaMarket
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("gamma: decode: %w", err)
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

	return &GammaMarket{
		ID:          raw.ID,
		ConditionID: raw.ConditionID,
		Slug:        raw.Slug,
		Question:    raw.Question,
		Outcomes:    outcomes,
		YesTokenID:  yesID,
		NoTokenID:   noID,
		TickSize:    raw.TickSize,
		Resolution:  resolution,
	}
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
