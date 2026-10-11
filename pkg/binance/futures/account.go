package futures

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

// PositionRisk lists current position risk (GET /fapi/v3/positionRisk, signed,
// no symbol filter).
func (c *Client) PositionRisk(ctx context.Context) ([]PositionRisk, error) {
	body, err := c.do(ctx, http.MethodGet, "/fapi/v3/positionRisk", nil, true)
	if err != nil {
		return nil, err
	}
	var out []PositionRisk
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return out, nil
}

// SetLeverage sets the leverage for a symbol (POST /fapi/v1/leverage, signed).
func (c *Client) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	p := newParams()
	p.add("symbol", symbol)
	p.add("leverage", strconv.Itoa(leverage))
	_, err := c.do(ctx, http.MethodPost, "/fapi/v1/leverage", p, true)
	return err
}

// SetMarginType sets the margin type for a symbol (POST /fapi/v1/marginType,
// signed). marginType is "ISOLATED" or "CROSSED". Binance code −4046
// ("No need to change margin type") is treated as success.
func (c *Client) SetMarginType(ctx context.Context, symbol, marginType string) error {
	p := newParams()
	p.add("symbol", symbol)
	p.add("marginType", marginType)
	_, err := c.do(ctx, http.MethodPost, "/fapi/v1/marginType", p, true)
	if isBinanceCode(err, -4046) {
		return nil
	}
	return err
}

// PositionMode reports whether hedge mode (dual-side positions) is enabled
// (GET /fapi/v1/positionSide/dual, signed).
func (c *Client) PositionMode(ctx context.Context) (bool, error) {
	body, err := c.do(ctx, http.MethodGet, "/fapi/v1/positionSide/dual", nil, true)
	if err != nil {
		return false, err
	}
	var out struct {
		DualSidePosition bool `json:"dualSidePosition"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return false, &transportError{err: err}
	}
	return out.DualSidePosition, nil
}

// SetPositionMode switches between one-way and hedge mode
// (POST /fapi/v1/positionSide/dual, signed). Binance code −4059
// ("No need to change position side") is treated as success.
func (c *Client) SetPositionMode(ctx context.Context, dual bool) error {
	p := newParams()
	p.add("dualSidePosition", strconv.FormatBool(dual))
	_, err := c.do(ctx, http.MethodPost, "/fapi/v1/positionSide/dual", p, true)
	if isBinanceCode(err, -4059) {
		return nil
	}
	return err
}

// isBinanceCode reports whether err is an *APIError carrying code.
func isBinanceCode(err error, code int) bool {
	if err == nil {
		return false
	}
	return codeOf(err) == code
}
