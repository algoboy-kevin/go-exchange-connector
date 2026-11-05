package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// AccountMode is the venue's account-abstraction mode for the MASTER account,
// as reported by the userAbstraction /info query (a bare JSON string). It
// decides how balances must be interpreted:
//
//   - AccountModeDisabled / AccountModeDefault ("disabled", "default"): the
//     standard account shapes — independent per-DEX clearinghouse balances.
//   - AccountModeUnified ("unifiedAccount"): all perp DEXes share one USDC
//     collateral pool held in spot state (spotClearinghouseState); individual
//     perp DEX balances are not meaningful.
//
// Any other value is returned typed so the caller can report it, but callers
// MUST fail closed on it: unknown/malformed/failed mode reads never fall back
// to legacy balances. The mode is small and cheap — query it per poll instead
// of caching it forever.
type AccountMode string

// Known account-abstraction modes. Anything outside this set is unsupported.
const (
	AccountModeDisabled AccountMode = "disabled"       // unified accounting off
	AccountModeDefault  AccountMode = "default"        // standard default shape
	AccountModeUnified  AccountMode = "unifiedAccount" // shared USDC collateral
)

// AccountMode returns the master account's account-abstraction mode from
// POST /info {"type":"userAbstraction","user":<master>}. The response must be
// a non-empty JSON string; anything else is a malformed query error (fail
// closed — never guess a mode).
func (c *Client) AccountMode(ctx context.Context) (AccountMode, error) {
	var mode *string
	if err := c.postInfo(ctx, "userAbstraction", map[string]any{
		"type": "userAbstraction",
		"user": c.account,
	}, &mode); err != nil {
		return "", err
	}
	if mode == nil || *mode == "" {
		return "", fmt.Errorf("hyperliquid: userAbstraction: malformed mode response")
	}
	return AccountMode(*mode), nil
}

// SpotUSDCBalance is one validated spotClearinghouseState read reduced to the
// shared USDC collateral pool (spot token 0 / coin "USDC"). Every decimal is
// the venue's string; the caller converts and derives its availability bound.
// Token identity and uniqueness are validated across the whole response:
// USDC must appear exactly once as token 0, every row's token/coin pair must
// be consistent, and the tokenToAvailableAfterMaintenance tuple for token 0
// must be present. Other spot balances are returned by the venue but are
// deliberately NOT dollar equity here.
type SpotUSDCBalance struct {
	Account                   string // master address the read was made for (lowercase)
	Total                     string // USDC total balance
	Hold                      string // USDC held against open orders
	AvailableAfterMaintenance string // tokenToAvailableAfterMaintenance[0]
}

// SpotUSDCBalance queries the master account's spot clearinghouse state once
// (POST /info {"type":"spotClearinghouseState","user":<master>}) and returns
// the strictly validated USDC collateral row. Any missing, duplicated,
// malformed or nonfinite value fails the read — never a zero/funded default.
// Spot state carries no exchange timestamp: callers must stamp their own
// observation time and never present it as venue time.
func (c *Client) SpotUSDCBalance(ctx context.Context) (*SpotUSDCBalance, error) {
	var resp struct {
		Balances json.RawMessage `json:"balances"`
		Tuples   json.RawMessage `json:"tokenToAvailableAfterMaintenance"`
	}
	if err := c.postInfo(ctx, "spotClearinghouseState", map[string]any{
		"type": "spotClearinghouseState",
		"user": c.account,
	}, &resp); err != nil {
		return nil, err
	}
	rows, err := rawArray(resp.Balances)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: spotClearinghouseState requires a balances array")
	}
	type balanceRow struct {
		total, hold string
	}
	var usdc *balanceRow
	tokens, coins := make(map[int]bool), make(map[string]bool)
	for _, raw := range rows {
		fields, err := rawObject(raw)
		if err != nil {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState has malformed balance row")
		}
		token, ok := strictInt(fields["token"])
		coin, okCoin := strictString(fields["coin"])
		if !ok || token < 0 || !okCoin || coin == "" {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState has invalid balance token/coin identity")
		}
		if tokens[token] || coins[coin] {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState has duplicate balance token/coin")
		}
		tokens[token], coins[coin] = true, true
		if (token == 0) != (coin == "USDC") {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState requires USDC token=0 identity")
		}
		if token == 0 {
			total, err := strictDecimal(fields["total"], "USDC total")
			if err != nil {
				return nil, err
			}
			hold, err := strictDecimal(fields["hold"], "USDC hold")
			if err != nil {
				return nil, err
			}
			usdc = &balanceRow{total: total, hold: hold}
		}
	}
	if usdc == nil {
		return nil, fmt.Errorf("hyperliquid: spotClearinghouseState is missing USDC token=0 balance")
	}
	tuples, err := rawArray(resp.Tuples)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: spotClearinghouseState requires tokenToAvailableAfterMaintenance tuple array")
	}
	seen := make(map[int]bool, len(tuples))
	var maintenance string
	for _, raw := range tuples {
		var pair []json.RawMessage
		if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState has malformed maintenance tuple")
		}
		token, ok := strictInt(pair[0])
		if !ok || token < 0 || seen[token] {
			return nil, fmt.Errorf("hyperliquid: spotClearinghouseState has invalid/duplicate maintenance token")
		}
		seen[token] = true
		amount, err := strictDecimal(pair[1], "maintenance availability")
		if err != nil {
			return nil, err
		}
		if token == 0 {
			maintenance = amount
		}
	}
	if !seen[0] {
		return nil, fmt.Errorf("hyperliquid: spotClearinghouseState is missing USDC token=0 maintenance availability")
	}
	return &SpotUSDCBalance{
		Account:                   c.account,
		Total:                     usdc.total,
		Hold:                      usdc.hold,
		AvailableAfterMaintenance: maintenance,
	}, nil
}

// CollateralToken returns the explicitly configured collateral token of one
// perp DEX scope ("" = core) from its metadata: POST /info
// {"type":"meta"} / {"type":"meta","dex":<dex>}. The collateralToken field
// must be present and a JSON integer — a missing or mistyped field is NOT an
// implicit 0 and fails the confirmation. Callers decide whether a token is
// acceptable (unified USDC accounting requires exactly 0 on every scope);
// unknown values are returned typed so the refusal can name them.
func (c *Client) CollateralToken(ctx context.Context, dex string) (int, error) {
	if dex != "" {
		if err := validateDex(dex); err != nil {
			return 0, err
		}
	}
	payload := map[string]any{"type": "meta"}
	if dex != "" {
		payload["dex"] = dex
	}
	var resp struct {
		CollateralToken json.RawMessage `json:"collateralToken"`
	}
	if err := c.postInfo(ctx, "meta", payload, &resp); err != nil {
		return 0, err
	}
	token, ok := strictInt(resp.CollateralToken)
	if !ok {
		return 0, fmt.Errorf("hyperliquid: meta %q: metadata does not explicitly confirm an integer collateralToken", dex)
	}
	return token, nil
}

// rawArray requires a JSON array (null/missing/object are not arrays).
func rawArray(raw json.RawMessage) ([]json.RawMessage, error) {
	if raw == nil {
		return nil, fmt.Errorf("missing")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, fmt.Errorf("not an array")
	}
	var out []json.RawMessage
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// rawObject requires a JSON object with raw members (null/array are not).
func rawObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, fmt.Errorf("not an object")
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// strictInt requires a JSON integer (bools, floats and strings are not).
func strictInt(raw json.RawMessage) (int, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var v int
	if err := json.Unmarshal(raw, &v); err != nil {
		return 0, false
	}
	return v, true
}

// strictString requires a JSON string (null is not).
func strictString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", false
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", false
	}
	return v, true
}

// decimalString matches a finite decimal numeral (no NaN/Inf spellings).
var decimalString = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// strictDecimal requires a nonnegative finite decimal STRING (JSON numbers
// are rejected; "-0"-style negative zero is zero). Returns the venue's string
// unchanged. Values outside float64 range stay strings here and fail later at
// the caller's numeric conversion.
func strictDecimal(raw json.RawMessage, field string) (string, error) {
	s, ok := strictString(raw)
	if !ok || !decimalString.MatchString(s) {
		return "", fmt.Errorf("hyperliquid: spotClearinghouseState requires a finite decimal string for %s", field)
	}
	digits := strings.TrimLeft(s, "+-")
	if s != "" && s[0] == '-' {
		if i := strings.IndexAny(digits, "eE"); i >= 0 {
			digits = digits[:i]
		}
		for i := 0; i < len(digits); i++ {
			if digits[i] != '0' && digits[i] != '.' {
				return "", fmt.Errorf("hyperliquid: spotClearinghouseState requires nonnegative finite %s", field)
			}
		}
	}
	return s, nil
}
