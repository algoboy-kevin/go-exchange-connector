package exchange

import (
	"context"
	"encoding/json"
	"fmt"
)

// Meta returns the perpetual universe fetched by New.
//
// It does not hit the network: the constructor must fetch meta anyway (before
// handing it to sonirico), so Meta exposes that startup snapshot. A listing that
// appears after startup is not picked up until the client is rebuilt.
func (c *Client) Meta(_ context.Context) (*Meta, error) {
	m := c.meta
	return &m, nil
}

// Positions returns the master account's open perpetual positions from
// clearinghouseState. Both queries use the MASTER account address — never the
// agent wallet, which holds no positions.
func (c *Client) Positions(ctx context.Context) ([]Position, error) {
	var resp struct {
		AssetPositions []struct {
			Position struct {
				Coin    string  `json:"coin"`
				Szi     string  `json:"szi"`
				EntryPx *string `json:"entryPx"`
			} `json:"position"`
		} `json:"assetPositions"`
	}
	if err := c.postInfo(ctx, "clearinghouseState", map[string]any{
		"type": "clearinghouseState",
		"user": c.account,
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]Position, 0, len(resp.AssetPositions))
	for _, ap := range resp.AssetPositions {
		p := Position{Coin: ap.Position.Coin, Szi: ap.Position.Szi}
		if ap.Position.EntryPx != nil {
			p.EntryPx = *ap.Position.EntryPx
		}
		out = append(out, p)
	}
	return out, nil
}

// OpenOrders returns the master account's open orders from frontendOpenOrders
// (the richer variant: it carries the client order id, which plain openOrders
// does not). Decimals stay strings.
func (c *Client) OpenOrders(ctx context.Context) ([]OpenOrder, error) {
	var resp []struct {
		Coin      string `json:"coin"`
		Side      string `json:"side"`
		LimitPx   string `json:"limitPx"`
		Sz        string `json:"sz"`
		OrigSz    string `json:"origSz"`
		Oid       int64  `json:"oid"`
		Cloid     string `json:"cloid"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := c.postInfo(ctx, "frontendOpenOrders", map[string]any{
		"type": "frontendOpenOrders",
		"user": c.account,
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]OpenOrder, 0, len(resp))
	for _, o := range resp {
		out = append(out, OpenOrder{
			Coin:      o.Coin,
			Side:      o.Side,
			LimitPx:   o.LimitPx,
			Sz:        o.Sz,
			OrigSz:    o.OrigSz,
			Oid:       o.Oid,
			Cloid:     o.Cloid,
			Timestamp: o.Timestamp,
		})
	}
	return out, nil
}

// postInfo POSTs one /info query and decodes the response into out. Errors keep
// the /info client's typing (*hyperliquid.HTTPError) so IsUncertain /
// IsRateLimited can classify them.
func (c *Client) postInfo(ctx context.Context, label string, payload, out any) error {
	raw, err := c.info.Post(ctx, payload)
	if err != nil {
		return fmt.Errorf("hyperliquid: %s: %w", label, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("hyperliquid: %s: decode response: %w", label, err)
	}
	return nil
}
