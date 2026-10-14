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

// AccountState returns a read-only snapshot of the MASTER account's perp
// clearinghouse (POST /info {"type":"clearinghouseState"}), with every decimal
// kept as the venue's string. The required fields (marginSummary.accountValue,
// marginSummary.totalMarginUsed, withdrawable) must be present and be JSON
// strings, else a decode error is returned (never a zero-valued snapshot).
//
// Scope: this is the PERP clearinghouse only. Spot balances, vault equity and
// funding-payment history are not included.
func (c *Client) AccountState(ctx context.Context) (*AccountState, error) {
	var resp struct {
		MarginSummary *struct {
			AccountValue    *string `json:"accountValue"`
			TotalNtlPos     string  `json:"totalNtlPos"`
			TotalRawUsd     string  `json:"totalRawUsd"`
			TotalMarginUsed *string `json:"totalMarginUsed"`
		} `json:"marginSummary"`
		CrossMaintenanceMarginUsed string  `json:"crossMaintenanceMarginUsed"`
		Withdrawable               *string `json:"withdrawable"`
		Time                       int64   `json:"time"`
	}
	if err := c.postInfo(ctx, "clearinghouseState", map[string]any{
		"type": "clearinghouseState",
		"user": c.account,
	}, &resp); err != nil {
		return nil, err
	}
	ms := resp.MarginSummary
	switch {
	case ms == nil:
		return nil, fmt.Errorf("hyperliquid: clearinghouseState: response has no marginSummary")
	case ms.AccountValue == nil || *ms.AccountValue == "":
		return nil, fmt.Errorf("hyperliquid: clearinghouseState: response has no marginSummary.accountValue")
	case ms.TotalMarginUsed == nil || *ms.TotalMarginUsed == "":
		return nil, fmt.Errorf("hyperliquid: clearinghouseState: response has no marginSummary.totalMarginUsed")
	case resp.Withdrawable == nil || *resp.Withdrawable == "":
		return nil, fmt.Errorf("hyperliquid: clearinghouseState: response has no withdrawable")
	}
	return &AccountState{
		Account:                    c.account,
		AccountValue:               *ms.AccountValue,
		TotalMarginUsed:            *ms.TotalMarginUsed,
		TotalNtlPos:                ms.TotalNtlPos,
		TotalRawUsd:                ms.TotalRawUsd,
		CrossMaintenanceMarginUsed: resp.CrossMaintenanceMarginUsed,
		Withdrawable:               *resp.Withdrawable,
		Time:                       resp.Time,
	}, nil
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

// Leverage returns the master account's configured leverage for coin and
// whether that coin is currently in cross margin, from the venue's
// activeAssetData query.
//
// It is the observable counterpart of UpdateLeverage: sonirico's UpdateLeverage
// cannot see a venue rejection (it decodes the response into a UserState), so
// UpdateLeverage re-queries through this method to confirm the change.
func (c *Client) Leverage(ctx context.Context, coin string) (leverage int, isCross bool, err error) {
	var resp struct {
		Leverage struct {
			Type  string `json:"type"` // "cross" | "isolated"
			Value int    `json:"value"`
		} `json:"leverage"`
	}
	if err := c.postInfo(ctx, "activeAssetData", map[string]any{
		"type": "activeAssetData",
		"user": c.account,
		"coin": coin,
	}, &resp); err != nil {
		return 0, false, err
	}
	switch resp.Leverage.Type {
	case "cross":
		isCross = true
	case "isolated":
		isCross = false
	default:
		return 0, false, fmt.Errorf("hyperliquid: activeAssetData for %s: unknown leverage type %q",
			coin, resp.Leverage.Type)
	}
	return resp.Leverage.Value, isCross, nil
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
