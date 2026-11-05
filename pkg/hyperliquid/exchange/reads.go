package exchange

import (
	"context"
)

// Fill is one userFills entry for the master account. Numeric fields are the
// venue's decimal strings, unchanged. Oid links the fill to the order that
// produced it.
type Fill struct {
	Coin      string `json:"coin"`
	Px        string `json:"px"`
	Sz        string `json:"sz"`
	Side      string `json:"side"`
	Dir       string `json:"dir"`
	Fee       string `json:"fee"`
	FeeToken  string `json:"feeToken"`
	ClosedPnl string `json:"closedPnl"`
	Crossed   bool   `json:"crossed"`
	Oid       int64  `json:"oid"`
	Hash      string `json:"hash"`
	Time      int64  `json:"time"`
}

// UserFills returns the master account's recent fills (POST /info
// {"type":"userFills","user":<master>}). It is a read-only query and does not
// use the order request quota.
func (c *Client) UserFills(ctx context.Context) ([]Fill, error) {
	var out []Fill
	if err := c.postInfo(ctx, "userFills", map[string]any{
		"type": "userFills",
		"user": c.account,
	}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Mids returns the venue's mid price per coin as decimal strings
// (POST /info {"type":"allMids"}). Read-only, no request quota.
func (c *Client) Mids(ctx context.Context) (map[string]string, error) {
	var out map[string]string
	if err := c.postInfo(ctx, "allMids", map[string]any{"type": "allMids"}, &out); err != nil {
		return nil, err
	}
	return out, nil
}
