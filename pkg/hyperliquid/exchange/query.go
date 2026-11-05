package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
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
	var out []Position
	for _, dex := range c.scopes() {
		part, err := c.PositionsDex(ctx, dex)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

// PositionsDex queries one registered scope without aggregating balances.
func (c *Client) PositionsDex(ctx context.Context, dex string) ([]Position, error) {
	if _, err := c.scope(dex); err != nil {
		return nil, err
	}
	var resp struct {
		AssetPositions []struct {
			Position struct {
				Coin    string  `json:"coin"`
				Szi     string  `json:"szi"`
				EntryPx *string `json:"entryPx"`
			} `json:"position"`
		} `json:"assetPositions"`
	}
	if err := c.postInfo(ctx, "clearinghouseState", c.scopePayload("clearinghouseState", dex), &resp); err != nil {
		return nil, err
	}
	if resp.AssetPositions == nil {
		return nil, fmt.Errorf("hyperliquid: clearinghouseState missing assetPositions array")
	}
	out := make([]Position, 0, len(resp.AssetPositions))
	for _, ap := range resp.AssetPositions {
		if err := responseCoin(ap.Position.Coin, dex); err != nil {
			return nil, err
		}
		if !finiteDecimal(ap.Position.Szi) {
			return nil, fmt.Errorf("hyperliquid: position missing/invalid szi")
		}
		if ap.Position.EntryPx != nil && !finiteDecimal(*ap.Position.EntryPx) {
			return nil, fmt.Errorf("hyperliquid: position invalid entryPx")
		}
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
	return c.AccountStateDex(ctx, "")
}

// AccountStateDex queries only the selected registered clearinghouse.
func (c *Client) AccountStateDex(ctx context.Context, dex string) (*AccountState, error) {
	if _, err := c.scope(dex); err != nil {
		return nil, err
	}
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
	if err := c.postInfo(ctx, "clearinghouseState", c.scopePayload("clearinghouseState", dex), &resp); err != nil {
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
	var out []OpenOrder
	for _, dex := range c.scopes() {
		part, err := c.OpenOrdersDex(ctx, dex)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

// OpenOrdersDex retains the cloid-rich frontendOpenOrders query for one scope.
func (c *Client) OpenOrdersDex(ctx context.Context, dex string) ([]OpenOrder, error) {
	if _, err := c.scope(dex); err != nil {
		return nil, err
	}
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
	if err := c.postInfo(ctx, "frontendOpenOrders", c.scopePayload("frontendOpenOrders", dex), &resp); err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("hyperliquid: frontendOpenOrders missing array")
	}
	out := make([]OpenOrder, 0, len(resp))
	for _, o := range resp {
		if err := responseCoin(o.Coin, dex); err != nil {
			return nil, err
		}
		if (o.Side != "A" && o.Side != "B") || !finiteDecimal(o.LimitPx) || !finiteDecimal(o.Sz) || !finiteDecimal(o.OrigSz) || o.Oid <= 0 {
			return nil, fmt.Errorf("hyperliquid: frontendOpenOrders missing required fields")
		}
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
	if _, err = c.route(coin); err != nil {
		return 0, false, err
	}
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

func (c *Client) scopePayload(typ, dex string) map[string]any {
	p := map[string]any{"type": typ, "user": c.account}
	if dex != "" {
		p["dex"] = dex
	}
	return p
}
func finiteDecimal(s string) bool {
	v, err := strconv.ParseFloat(s, 64)
	return err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
}
func responseCoin(coin, dex string) error {
	got, err := coinDex(coin)
	if err != nil {
		return err
	}
	if got != dex {
		return scopeError(dex, "response coin belongs to another scope: "+coin)
	}
	return nil
}

// Read-only /info rate-limit retry policy. postInfo is the ONLY retry owner for
// /info reads: callers (the driver adapter, the batch runner) must not wrap it in
// a second retry for HTTP 429. /exchange mutations never pass through here and
// are never retried.
//
// Budget: at most infoRetryMax retries (infoRetryMax+1 attempts) per query, with
// exponential waits infoRetryBase, 2x, 4x... capped at infoRetryCap. With the
// defaults (3 retries, 500ms base, 2s cap) the waits are 500ms, 1s, 2s: at most
// 3.5s of added latency and 4 requests per query. A retry whose wait would not
// fit before the context deadline is not attempted.
const (
	infoRetryMax  = 3
	infoRetryBase = 500 * time.Millisecond
	infoRetryCap  = 2 * time.Second
)

// infoWait sleeps for d or until ctx ends (returning ctx.Err()). It is a
// variable so tests can replace it with a fast fake; production never does.
var infoWait = func(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// infoBackoff is the wait before retry number n (0-based).
func infoBackoff(n int) time.Duration {
	d := infoRetryBase << uint(n)
	if d <= 0 || d > infoRetryCap {
		d = infoRetryCap
	}
	return d
}

// postInfo POSTs one /info query and decodes the response into out. Errors keep
// the /info client's typing (*hyperliquid.HTTPError) so IsUncertain /
// IsRateLimited can classify them.
//
// A rate-limited (IsRateLimited, i.e. HTTP 429) response is retried within the
// bounded budget above; every other error fails immediately. A cancelled or
// expired context during a wait aborts at once, and the returned error wraps
// both the context error and the last rate-limit error.
func (c *Client) postInfo(ctx context.Context, label string, payload, out any) error {
	var (
		raw []byte
		err error
	)
	maxRetries := infoRetryMax
	if c.infoRetry != nil {
		maxRetries = max(*c.infoRetry, 0)
	}
	for attempt := 0; ; attempt++ {
		raw, err = c.info.Post(ctx, payload)
		if err == nil || !IsRateLimited(err) || attempt >= maxRetries {
			break
		}
		d := infoBackoff(attempt)
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= d {
			break // the wait would consume the rest of the caller's budget
		}
		if werr := infoWait(ctx, d); werr != nil {
			return fmt.Errorf("hyperliquid: %s: retry aborted: %w (last error: %w)", label, werr, err)
		}
	}
	if err != nil {
		return fmt.Errorf("hyperliquid: %s: %w", label, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("hyperliquid: %s: decode response: %w", label, err)
	}
	return nil
}
