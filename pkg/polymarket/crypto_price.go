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

// CryptoPriceClient handles Polymarket's public crypto-price API
// (https://polymarket.com/api/crypto/crypto-price), which returns the
// open/close price of a crypto asset over a market's window.
type CryptoPriceClient struct {
	baseURL string
	http    *http.Client
}

// NewCryptoPriceClient creates a new crypto-price API client.
func NewCryptoPriceClient(baseURL string) *CryptoPriceClient {
	if baseURL == "" {
		baseURL = defaultCryptoPriceAPIURL
	}
	return &CryptoPriceClient{
		baseURL: baseURL,
		http:    &http.Client{},
	}
}

// Fetch queries the crypto-price API for the given window and returns the
// open/close price. The request must have Symbol, Variant, EventStartTime and
// EndDate fully resolved (see WindowFromMarket for deriving them from a
// market).
func (c *CryptoPriceClient) Fetch(req connector.CryptoPriceRequest) (*connector.CryptoPrice, error) {
	if req.Symbol == "" {
		return nil, fmt.Errorf("crypto-price: symbol required")
	}
	if req.Variant == "" {
		return nil, fmt.Errorf("crypto-price: variant required")
	}
	if req.EventStartTime.IsZero() {
		return nil, fmt.Errorf("crypto-price: eventStartTime required")
	}
	if req.EndDate.IsZero() {
		return nil, fmt.Errorf("crypto-price: endDate required")
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

	query := fmt.Sprintf("%s/api/crypto/crypto-price?%s", c.baseURL, q.Encode())
	slog.Debug("crypto-price: querying", "url", query)

	resp, err := c.http.Get(query)
	if err != nil {
		return nil, fmt.Errorf("crypto-price: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("crypto-price: %s returned %d", query, resp.StatusCode)
	}

	var price connector.CryptoPrice
	if err := json.NewDecoder(resp.Body).Decode(&price); err != nil {
		return nil, fmt.Errorf("crypto-price: decode: %w", err)
	}

	return &price, nil
}

// ─────────────────────────────────────────────────────────────
// Window derivation
// ─────────────────────────────────────────────────────────────

// variantDuration maps a crypto-price variant to its window duration.
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
		return 0, fmt.Errorf("crypto-price: unknown variant %q", variant)
	}
}

// WindowFromMarket derives the crypto-price window (eventStartTime, endDate)
// from a market's Gamma end date and the series variant:
//
//	eventStartTime = market.EndDate − variantDuration(variant)
//	endDate        = market.EndDate
//
// For a 5-minute market ending at 11:40Z this yields 11:35Z → 11:40Z, matching
// the crypto-price API's expected window for that variant.
func WindowFromMarket(gm *GammaMarket, variant string) (start, end time.Time, err error) {
	if gm == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("crypto-price: nil market")
	}
	if gm.EndDate.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("crypto-price: market has no endDate")
	}
	dur, err := variantDuration(variant)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return gm.EndDate.Add(-dur), gm.EndDate, nil
}
