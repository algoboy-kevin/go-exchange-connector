package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	hl "github.com/sonirico/go-hyperliquid"
)

// Place submits orders as one "order" action (grouping "na") and returns one
// status per order, in the order they were submitted.
//
// sonirico's BulkOrders returns a non-nil result AND an error whenever any item
// failed, so that error is NOT a call failure: the statuses are mapped per item
// (resting / filled / error) and the call returns no error. Only a nil result is
// a call failure (classified by IsUncertain / IsRateLimited), and a top-level
// {"status":"err"} envelope becomes an *ActionError.
func (c *Client) Place(ctx context.Context, orders []Order) ([]OrderStatus, error) {
	if len(orders) == 0 {
		return nil, nil
	}
	reqs := make([]hl.CreateOrderRequest, len(orders))
	for i, o := range orders {
		px, err := parseDecimal("price", o.Coin, o.Px)
		if err != nil {
			return nil, err
		}
		sz, err := parseDecimal("size", o.Coin, o.Sz)
		if err != nil {
			return nil, err
		}
		tif, err := parseTif(o.Tif)
		if err != nil {
			return nil, fmt.Errorf("hyperliquid: order %d (%s): %w", i, o.Coin, err)
		}
		var cloid *string
		if o.Cloid != "" {
			cl := o.Cloid
			cloid = &cl
		}
		reqs[i] = hl.CreateOrderRequest{
			Coin:          o.Coin,
			IsBuy:         o.IsBuy,
			Price:         px,
			Size:          sz,
			ReduceOnly:    o.ReduceOnly,
			OrderType:     hl.OrderType{Limit: &hl.LimitOrderType{Tif: tif}},
			ClientOrderID: cloid,
		}
	}

	res, err := c.ex.BulkOrders(ctx, reqs, nil)
	if res == nil {
		return nil, callFailure("place", err)
	}
	if !res.Ok {
		return nil, &ActionError{Msg: res.Err}
	}
	return mapOrderStatuses(res.Data.Statuses, len(orders)), nil
}

// CancelByCloid cancels orders by client order id, one status per request.
//
// Like Place, a per-item failure comes back as (non-nil result, error) — here
// sonirico's FirstError() — and is mapped into the statuses rather than
// returned as a call failure.
func (c *Client) CancelByCloid(ctx context.Context, reqs []CancelReq) ([]CancelStatus, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	wire := make([]hl.CancelOrderRequestByCloid, len(reqs))
	for i, r := range reqs {
		wire[i] = hl.CancelOrderRequestByCloid{Coin: r.Coin, Cloid: r.Cloid}
	}
	res, err := c.ex.BulkCancelByCloids(ctx, wire)
	return mapCancelResult(res, err, len(reqs), "cancelByCloid")
}

// CancelByOid cancels orders by venue order id, one status per request.
func (c *Client) CancelByOid(ctx context.Context, reqs []OidCancel) ([]CancelStatus, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	wire := make([]hl.CancelOrderRequest, len(reqs))
	for i, r := range reqs {
		wire[i] = hl.CancelOrderRequest{Coin: r.Coin, OrderID: r.Oid}
	}
	res, err := c.ex.BulkCancel(ctx, wire)
	return mapCancelResult(res, err, len(reqs), "cancel")
}

// ScheduleCancel arms (at non-nil) or disarms (at nil) the venue's dead-man
// switch: every open order is cancelled at the given time.
//
// The time must be at least 5s in the future — the venue rejects anything
// closer, so it is validated locally.
//
// The outcome is classified from THIS request's raw HTTP status and (bounded)
// body, captured through the /exchange client's transport: sonirico decodes the
// response into a struct that keeps "status"/"error" and drops the venue's
// top-level "response" rejection string. Signing and the request itself remain
// sonirico's. The classification:
//
//   - 2xx {"status":"ok"}                        → nil
//   - 2xx {"status":"err"} naming traded volume  → ErrScheduleCancelUnavailable
//     (wrapping an *ActionError with the venue's message)
//   - 2xx {"status":"err"} with any other text   → *ActionError (business)
//   - 2xx with no/unknown status, oversize body  → uncertain (IsUncertain)
//   - HTTP ≥ 400                                 → *StatusError (5xx uncertain,
//     429 rate-limited), never ErrScheduleCancelUnavailable
//   - transport error / timeout                  → uncertain
func (c *Client) ScheduleCancel(ctx context.Context, at *time.Time) error {
	var ms *int64
	if at != nil {
		t := at.UnixMilli()
		if lead := minScheduleCancelLead; t < time.Now().Add(lead).UnixMilli() {
			return fmt.Errorf("hyperliquid: schedule cancel time %s is less than %s ahead (venue minimum)",
				at.UTC().Format(time.RFC3339Nano), lead)
		}
		ms = &t
	}

	cctx, rc := withCapture(ctx)
	_, err := c.ex.ScheduleCancel(cctx, ms)
	return classifyScheduleCancel(err, rc.snapshot())
}

// classifyScheduleCancel maps one schedule-cancel exchange (sonirico's error
// plus the captured raw response) onto this package's error classes.
func classifyScheduleCancel(err error, got captured) error {
	if err != nil {
		if got.seen && got.status >= 400 {
			return fmt.Errorf("hyperliquid: schedule cancel: %w", &StatusError{Code: got.status, Err: err})
		}
		// Transport error, timeout, unreadable/oversize body: keep sonirico's
		// error so IsUncertain classifies it. Never a volume refusal.
		return fmt.Errorf("hyperliquid: schedule cancel: %w", err)
	}
	if !got.seen || got.over {
		return &uncertainError{msg: "schedule cancel: response was not captured; outcome unknown"}
	}
	var env struct {
		Status   string          `json:"status"`
		Response json.RawMessage `json:"response"`
		Error    string          `json:"error"`
	}
	if jerr := json.Unmarshal(got.body, &env); jerr != nil {
		return fmt.Errorf("hyperliquid: schedule cancel: decode response: %w", jerr)
	}
	switch env.Status {
	case "ok":
		return nil
	case "err":
		msg := venueMessage(env.Response, env.Error)
		ae := &ActionError{Msg: "schedule cancel rejected: " + msg}
		if msg == "" {
			ae.Msg = "schedule cancel rejected (status err, no message)"
		}
		if isVolumeRefusal(msg) {
			return fmt.Errorf("%w: %w", ErrScheduleCancelUnavailable, ae)
		}
		return ae
	default:
		return &uncertainError{msg: fmt.Sprintf("schedule cancel: response status %q is neither ok nor err; outcome unknown", sanitizeVenueText(env.Status))}
	}
}

// venueMessage extracts the rejection text: the top-level "response" when it is
// a JSON string, else the "error" field. Non-string responses yield "".
func venueMessage(resp json.RawMessage, errField string) string {
	var s string
	if len(resp) > 0 && json.Unmarshal(resp, &s) == nil && strings.TrimSpace(s) != "" {
		return sanitizeVenueText(s)
	}
	return sanitizeVenueText(errField)
}

// maxVenueText bounds venue-provided text carried in errors and logs.
const maxVenueText = 512

// sanitizeVenueText trims, replaces control characters and bounds venue text.
func sanitizeVenueText(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxVenueText {
		s = s[:maxVenueText] + "…"
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)
}

// isVolumeRefusal reports whether a venue rejection message explicitly refuses
// schedule-cancel for insufficient traded volume: it must mention "volume" AND
// "traded"/"trading". A rejection that merely mentions volume (a parameter
// error, say) is not one.
func isVolumeRefusal(msg string) bool {
	m := strings.ToLower(msg)
	if !strings.Contains(m, "volume") {
		return false
	}
	// Require an explicit unmet trading threshold, not merely the words
	// "trading" and "volume" in an unrelated parameter rejection.
	return strings.Contains(m, "enough volume traded") ||
		strings.Contains(m, "not traded enough") ||
		strings.Contains(m, "has not traded") ||
		strings.Contains(m, "insufficient traded volume") ||
		strings.Contains(m, "insufficient trading volume")
}

// UpdateLeverage sets the leverage for one coin (isCross selects cross margin)
// and then VERIFIES that the venue applied it.
//
// sonirico's UpdateLeverage decodes the response straight into a UserState, so a
// top-level venue rejection ({"status":"err","response":"…"}) is not observable
// there — a failed leverage change looks like success. To close that hole this
// method re-queries the account's leverage for the coin (activeAssetData) and
// returns an error when the value or the margin mode did not take, e.g.
//
//	hyperliquid: leverage for ETH not applied: want 5 isolated, venue has 20 cross
//
// So a nil return means the venue really reports the requested setting. The
// verification is one extra read-only /info request; it is deliberate, not
// something to optimise away.
func (c *Client) UpdateLeverage(ctx context.Context, coin string, leverage int, isCross bool) error {
	if leverage <= 0 {
		return fmt.Errorf("hyperliquid: leverage for %s must be positive, got %d", coin, leverage)
	}
	if _, err := c.ex.UpdateLeverage(ctx, leverage, coin, isCross); err != nil {
		return fmt.Errorf("hyperliquid: update leverage %s: %w", coin, err)
	}

	got, gotCross, err := c.Leverage(ctx, coin)
	if err != nil {
		return fmt.Errorf("hyperliquid: verify leverage %s: %w", coin, err)
	}
	if got != leverage || gotCross != isCross {
		return fmt.Errorf("hyperliquid: leverage for %s not applied: want %d %s, venue has %d %s",
			coin, leverage, marginMode(isCross), got, marginMode(gotCross))
	}
	return nil
}

// marginMode renders a cross/isolated flag the way the venue spells it.
func marginMode(isCross bool) string {
	if isCross {
		return "cross"
	}
	return "isolated"
}

// ─────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────

// callFailure returns the error to report for a call that produced no result.
func callFailure(op string, err error) error {
	if err == nil {
		err = errors.New("empty response")
	}
	return fmt.Errorf("hyperliquid: %s: %w", op, err)
}

func parseTif(tif string) (hl.Tif, error) {
	switch hl.Tif(tif) {
	case hl.TifAlo, hl.TifIoc, hl.TifGtc:
		return hl.Tif(tif), nil
	}
	return "", fmt.Errorf("unsupported time-in-force %q (want %s, %s or %s)", tif, TifAlo, TifIoc, TifGtc)
}

// parseDecimal converts a decimal string to the float64 sonirico's wire encoder
// needs. Hyperliquid perps carry ≤ 6 price / ≤ 8 size decimals, so the
// conversion is exact (floatToWire re-renders the identical digits — the tests
// pin that). NaN/Inf and non-positive values are rejected: they would otherwise
// reach the venue as "NaN" or a nonsense order.
func parseDecimal(what, coin, s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("hyperliquid: order %s: missing %s", coin, what)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("hyperliquid: order %s: invalid %s %q", coin, what, s)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("hyperliquid: order %s: %s %q is not finite", coin, what, s)
	}
	if f <= 0 {
		return 0, fmt.Errorf("hyperliquid: order %s: %s must be positive, got %q", coin, what, s)
	}
	return f, nil
}

// mapOrderStatuses maps sonirico's per-order statuses onto this package's. If
// the venue returns fewer statuses than orders, the gap is filled with an
// explicit per-item error so the caller's index mapping never shifts.
func mapOrderStatuses(statuses []hl.OrderStatus, want int) []OrderStatus {
	out := make([]OrderStatus, 0, want)
	for _, s := range statuses {
		var st OrderStatus
		switch {
		case s.Error != nil:
			st.Error = *s.Error
		case s.Resting != nil:
			st.Resting = true
			st.Oid = s.Resting.Oid
		case s.Filled != nil:
			st.Filled = true
			st.Oid = int64(s.Filled.Oid)
			st.TotalSz = s.Filled.TotalSz
			st.AvgPx = s.Filled.AvgPx
		}
		out = append(out, st)
	}
	for len(out) < want {
		out = append(out, OrderStatus{Error: "no status returned by the venue"})
	}
	return out
}

// mapCancelResult interprets a bulk cancel response. A nil result is a call
// failure; a non-OK envelope is an ActionError; otherwise the per-item statuses
// are mapped (the accompanying error is sonirico's FirstError()).
func mapCancelResult(res *hl.APIResponse[hl.CancelOrderResponse], err error, want int, op string) ([]CancelStatus, error) {
	if res == nil {
		return nil, callFailure(op, err)
	}
	if !res.Ok || res.Status == "err" {
		msg := res.Err
		if msg == "" {
			msg = op + " rejected"
		}
		return nil, &ActionError{Msg: msg}
	}
	return mapCancelStatuses(res.Data.Statuses, want), nil
}

// mapCancelStatuses maps the venue's MixedArray statuses: each element is the
// string "success" or an object {"error": "…"} (rendered verbatim when the error
// value is not a string).
func mapCancelStatuses(statuses hl.MixedArray, want int) []CancelStatus {
	out := make([]CancelStatus, 0, want)
	for i := range statuses {
		mv := &statuses[i]
		var st CancelStatus
		switch {
		case mv == nil:
			st.Error = "unknown cancel status"
		default:
			if s, ok := mv.String(); ok {
				if s == "success" {
					st.OK = true
				} else {
					st.Error = s
				}
				break
			}
			if obj, ok := mv.Object(); ok {
				if raw, ok := obj["error"]; ok {
					if msg, ok := raw.(string); ok && msg != "" {
						st.Error = msg
					} else if b, merr := json.Marshal(raw); merr == nil {
						st.Error = string(b)
					}
				} else {
					st.Error = "unknown cancel status"
				}
				break
			}
			st.Error = "unknown cancel status"
		}
		out = append(out, st)
	}
	for len(out) < want {
		out = append(out, CancelStatus{Error: "no status returned by the venue"})
	}
	return out
}
