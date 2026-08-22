package polymarket

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

const defaultCryptoPriceAPIURL = "https://polymarket.com"

// priceHistoryPoint is a single point in Polymarket's price-history API
// response: a millisecond epoch timestamp and the price at that time.
type priceHistoryPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// CryptoPriceClient handles Polymarket's public price-history API
// (https://polymarket.com/api/crypto/price-history), which returns the
// open/close price of a crypto asset over a market's window as a time series.
type CryptoPriceClient struct {
	baseURL string
	http    *http.Client
}

// NewCryptoPriceClient creates a new price-history API client.
func NewCryptoPriceClient(baseURL string) *CryptoPriceClient {
	if baseURL == "" {
		baseURL = defaultCryptoPriceAPIURL
	}
	return &CryptoPriceClient{
		baseURL: baseURL,
		http:    &http.Client{},
	}
}

// Fetch queries the price-history API for the given window and returns the
// open/close price. The request must have Symbol, Variant, EventStartTime and
// EndDate fully resolved (see WindowFromMarket for deriving them from a
// market).
//
// The API returns a series of {timestamp, value} points (ms epoch, price): the
// open price is the first point's value and the close price is the last
// point's value once the window has settled (the last point reaches EndDate).
func (c *CryptoPriceClient) Fetch(req connector.CryptoPriceRequest) (*connector.CryptoPrice, error) {
	if req.Symbol == "" {
		return nil, fmt.Errorf("price-history: symbol required")
	}
	if req.Variant == "" {
		return nil, fmt.Errorf("price-history: variant required")
	}
	if req.EventStartTime.IsZero() {
		return nil, fmt.Errorf("price-history: eventStartTime required")
	}
	if req.EndDate.IsZero() {
		return nil, fmt.Errorf("price-history: endDate required")
	}

	q := url.Values{}
	q.Set("symbol", req.Symbol)
	q.Set("variant", req.Variant)
	q.Set("eventStartTime", req.EventStartTime.UTC().Format(time.RFC3339))
	q.Set("endDate", req.EndDate.UTC().Format(time.RFC3339))
	if req.TWAPEnabled {
		q.Set("twapEnabled", "true")
		if req.TWAPLookbackSeconds > 0 {
			q.Set("twapLookbackSeconds", strconv.Itoa(req.TWAPLookbackSeconds))
		}
	}

	query := fmt.Sprintf("%s/api/crypto/price-history?%s", c.baseURL, q.Encode())
	slog.Debug("price-history: querying", "url", query)

	resp, err := c.http.Get(query)
	if err != nil {
		return nil, fmt.Errorf("price-history: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("price-history: %s returned %d", query, resp.StatusCode)
	}

	// The response is a top-level JSON array of {timestamp, value} points,
	// e.g. [{"timestamp":1787312100000,"value":76578.38}, ...]. The first
	// point is the window open; the last point is the window close once the
	// window has settled.
	var pts []priceHistoryPoint
	if err := json.NewDecoder(resp.Body).Decode(&pts); err != nil {
		return nil, fmt.Errorf("price-history: decode: %w", err)
	}

	// The API can return an empty array while a window is still forming (e.g.
	// TWAP with not enough lookback history yet). The collector retries on
	// error, so surface a descriptive one here.
	if len(pts) == 0 {
		return nil, fmt.Errorf("price-history: empty response for %s %s..%s",
			req.Symbol,
			req.EventStartTime.UTC().Format(time.RFC3339),
			req.EndDate.UTC().Format(time.RFC3339))
	}

	last := pts[len(pts)-1]
	completed := last.Timestamp >= req.EndDate.UTC().UnixMilli()

	price := &connector.CryptoPrice{
		// The array has no top-level timestamp; use the last point's — the
		// latest data in the window.
		OpenPrice:  pts[0].Value,
		Timestamp:  last.Timestamp,
		Completed:  completed,
		Incomplete: !completed,
		Cached:     false, // price-history does not report a cache flag
	}

	if completed {
		v := last.Value
		price.ClosePrice = &v
	}

	return price, nil
}

// ─────────────────────────────────────────────────────────────
// Window derivation
// ─────────────────────────────────────────────────────────────

// variantDuration maps a price-history variant to its window duration.
func variantDuration(variant string) (time.Duration, error) {
	switch variant {
	case "fiveminute":
		return 5 * time.Minute, nil
	case "fifteen":
		return 15 * time.Minute, nil
	case "hourly":
		return time.Hour, nil
	case "daily":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("price-history: unknown variant %q", variant)
	}
}

// WindowFromMarket derives the price-history window (eventStartTime, endDate)
// from a market's Gamma end date and the series variant:
//
//	eventStartTime = market.EndDate − variantDuration(variant)
//	endDate        = market.EndDate
//
// For a 5-minute market ending at 11:40Z this yields 11:35Z → 11:40Z, matching
// the price-history API's expected window for that variant.
func WindowFromMarket(gm *GammaMarket, variant string) (start, end time.Time, err error) {
	if gm == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("price-history: nil market")
	}
	if gm.EndDate.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("price-history: market has no endDate")
	}
	dur, err := variantDuration(variant)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return gm.EndDate.Add(-dur), gm.EndDate, nil
}
