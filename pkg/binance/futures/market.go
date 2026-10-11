package futures

import (
	"context"
	"encoding/json"
	"net/http"
)

// ExchangeInfo fetches GET /fapi/v1/exchangeInfo (public, unsigned) and
// returns the typed symbol/filter/rate-limit set. The caller uses the filters
// to round prices/quantities to the symbol's tick and step.
func (c *Client) ExchangeInfo(ctx context.Context) (*ExchangeInfo, error) {
	body, err := c.do(ctx, http.MethodGet, "/fapi/v1/exchangeInfo", nil, false)
	if err != nil {
		return nil, err
	}
	var out ExchangeInfo
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}
