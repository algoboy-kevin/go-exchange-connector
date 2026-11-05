// Package hladapter adapts the Hyperliquid perpetual execution client to the
// shared contract. The native client keeps the venue payloads (asset ids, cloid
// format, signed actions); this file only maps names, enums and errors.
package hladapter

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid/exchange"
)

// maxBatch is a LOCAL cap, UNVERIFIED. The Hyperliquid docs (exchange endpoint
// and rate-limits pages, checked 2026-11-05 via exchange-docs-hub) give no
// per-action maximum for order or cancel batches. They give only the rate-limit
// weight formula 1 + floor(batch_length / 40), and a batch of 79 orders is their
// example. Do not treat 50 as a venue limit. Revisit once a number is documented
// or confirmed on testnet.
const maxBatch = 50

// Adapter implements contract.Trader, BatchLimiter, CancelAller, DeadManSwitch,
// PositionReader, AccountReader and LeverageSetter for Hyperliquid perps.
type Adapter struct {
	c *hl.Client
}

// New wraps a configured execution client.
func New(c *hl.Client) *Adapter { return &Adapter{c: c} }

var (
	_ contract.Trader         = (*Adapter)(nil)
	_ contract.BatchLimiter   = (*Adapter)(nil)
	_ contract.DeadManSwitch  = (*Adapter)(nil)
	_ contract.PositionReader = (*Adapter)(nil)
	_ contract.AccountReader  = (*Adapter)(nil)
	_ contract.LeverageSetter = (*Adapter)(nil)
)

// MaxPlaceBatch is the local cap (see maxBatch).
func (*Adapter) MaxPlaceBatch() int { return maxBatch }

// MaxCancelBatch is the local cap (see maxBatch).
func (*Adapter) MaxCancelBatch() int { return maxBatch }

var cloidRE = regexp.MustCompile(`^0x[0-9a-fA-F]{32}$`)

// tifValue maps the contract TIF to Hyperliquid. Post-only is Alo.
func tifValue(t contract.TIF) (string, error) {
	switch t {
	case contract.GTC:
		return hl.TifGtc, nil
	case contract.IOC:
		return hl.TifIoc, nil
	case contract.PostOnly:
		return hl.TifAlo, nil
	}
	return "", fmt.Errorf("%w: tif %q", contract.ErrInvalid, t)
}

// toOrder builds the native order. A ClientID must be "0x" plus 32 hex characters.
// The adapter rejects any other form before sending.
func toOrder(r contract.OrderRequest) (hl.Order, error) {
	if err := r.Validate(); err != nil {
		return hl.Order{}, err
	}
	tif, err := tifValue(r.TIF)
	if err != nil {
		return hl.Order{}, err
	}
	if r.ClientID != "" && !cloidRE.MatchString(r.ClientID) {
		return hl.Order{}, fmt.Errorf("%w: client id %q must be 0x plus 32 hex characters", contract.ErrInvalid, r.ClientID)
	}
	return hl.Order{
		Coin:       r.Symbol,
		IsBuy:      r.Side == contract.Buy,
		Px:         r.Price,
		Sz:         r.Qty,
		Tif:        tif,
		ReduceOnly: r.ReduceOnly,
		Cloid:      r.ClientID,
	}, nil
}

// resultFromStatus maps one per-item venue status. A rejection with an error
// message is Rejected. A resting or filled order is Resting or Filled. Anything
// else is Unknown.
func resultFromStatus(s hl.OrderStatus, req contract.OrderRequest, sent, ack time.Time) contract.OrderResult {
	res := contract.OrderResult{ClientID: req.ClientID, SentAt: sent, AckAt: ack}
	if s.Oid != 0 {
		res.OrderID = strconv.FormatInt(s.Oid, 10)
	}
	switch {
	case s.Error != "":
		res.Status = contract.Rejected
		res.Reason = s.Error
	case s.Filled:
		res.Status = contract.Filled
		res.FilledQty = s.TotalSz
		res.AvgPrice = s.AvgPx
	case s.Resting:
		res.Status = contract.Resting
	default:
		res.Status = contract.Unknown
		res.Reason = "venue returned no resting, filled or error status"
	}
	return res
}

// OpenOrders lists open orders for the account. Hyperliquid returns all coins;
// a non-empty symbol filters by coin. The venue reports Side as "A" (sell) or "B" (buy).
func (a *Adapter) OpenOrders(ctx context.Context, symbol string) ([]contract.Order, error) {
	rows, err := a.c.OpenOrders(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]contract.Order, 0, len(rows))
	for _, o := range rows {
		if symbol != "" && o.Coin != symbol {
			continue
		}
		side := contract.Sell
		if o.Side == "B" {
			side = contract.Buy
		}
		cloid := o.Cloid
		out = append(out, contract.Order{
			Symbol:    o.Coin,
			OrderID:   strconv.FormatInt(o.Oid, 10),
			ClientID:  cloid,
			Side:      side,
			Price:     o.LimitPx,
			Qty:       o.OrigSz,
			FilledQty: subDecimal(o.OrigSz, o.Sz),
			Timestamp: time.UnixMilli(o.Timestamp),
		})
	}
	return out, nil
}

// subDecimal returns a-b for decimal strings, as text, when both parse. Otherwise "".
// Using big.Rat keeps the venue's exact decimals.
func subDecimal(a, b string) string {
	ra, ok1 := new(big.Rat).SetString(a)
	rb, ok2 := new(big.Rat).SetString(b)
	if !ok1 || !ok2 {
		return ""
	}
	return ra.Sub(ra, rb).FloatString(8)
}

// PlaceOrder places one order through the bulk action with one item.
func (a *Adapter) PlaceOrder(ctx context.Context, req contract.OrderRequest) (contract.OrderResult, error) {
	out, err := a.PlaceOrders(ctx, []contract.OrderRequest{req})
	if err != nil {
		return contract.OrderResult{}, err
	}
	return out[0], nil
}

// PlaceOrders places up to maxBatch orders in one signed action. Results are in
// request order. A per-item venue error is a Rejected result, not a call error.
func (a *Adapter) PlaceOrders(ctx context.Context, reqs []contract.OrderRequest) ([]contract.OrderResult, error) {
	if err := contract.CheckBatch(len(reqs), maxBatch); err != nil {
		return nil, err
	}
	orders := make([]hl.Order, len(reqs))
	for i, r := range reqs {
		o, err := toOrder(r)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", i, err)
		}
		orders[i] = o
	}
	sent := time.Now()
	statuses, err := a.c.Place(ctx, orders)
	ack := time.Now()
	if err != nil {
		if hl.IsUncertain(err) {
			out := make([]contract.OrderResult, len(reqs))
			for i, r := range reqs {
				out[i] = contract.OrderResult{ClientID: r.ClientID, Status: contract.Unknown,
					Reason: err.Error(), SentAt: sent, AckAt: ack}
			}
			return out, fmt.Errorf("%w: %v", contract.ErrUncertain, err)
		}
		var actErr *hl.ActionError
		if errors.As(err, &actErr) {
			// Envelope-level rejection: the whole action was refused.
			out := make([]contract.OrderResult, len(reqs))
			for i, r := range reqs {
				out[i] = contract.OrderResult{ClientID: r.ClientID, Status: contract.Rejected,
					Reason: actErr.Msg, SentAt: sent, AckAt: ack}
			}
			return out, nil
		}
		return nil, err
	}
	if len(statuses) != len(reqs) {
		return nil, fmt.Errorf("hyperliquid: place returned %d statuses for %d orders", len(statuses), len(reqs))
	}
	out := make([]contract.OrderResult, len(reqs))
	for i, s := range statuses {
		out[i] = resultFromStatus(s, reqs[i], sent, ack)
	}
	return out, nil
}

// CancelOrder cancels by exchange order id, or by client id when OrderID is empty.
func (a *Adapter) CancelOrder(ctx context.Context, req contract.CancelRequest) (contract.CancelResult, error) {
	out, err := a.CancelOrders(ctx, []contract.CancelRequest{req})
	if err != nil {
		return contract.CancelResult{}, err
	}
	return out[0], nil
}

// CancelOrders cancels up to maxBatch orders. The venue cancel action is by oid
// (preferred) or cloid. A batch mixes them only if every item has the same kind.
func (a *Adapter) CancelOrders(ctx context.Context, reqs []contract.CancelRequest) ([]contract.CancelResult, error) {
	if err := contract.CheckBatch(len(reqs), maxBatch); err != nil {
		return nil, err
	}
	byOid := reqs[0].OrderID != ""
	oids := make([]hl.OidCancel, 0, len(reqs))
	cloids := make([]hl.CancelReq, 0, len(reqs))
	for i, r := range reqs {
		if err := r.Validate(); err != nil {
			return nil, fmt.Errorf("cancel %d: %w", i, err)
		}
		if (r.OrderID != "") != byOid {
			return nil, fmt.Errorf("%w: cancel batch mixes order id and client id (item %d)", contract.ErrInvalid, i)
		}
		if byOid {
			oid, err := strconv.ParseInt(r.OrderID, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: cancel %d order id %q is not numeric", contract.ErrInvalid, i, r.OrderID)
			}
			oids = append(oids, hl.OidCancel{Coin: r.Symbol, Oid: oid})
		} else {
			cloids = append(cloids, hl.CancelReq{Coin: r.Symbol, Cloid: r.ClientID})
		}
	}
	var statuses []hl.CancelStatus
	var err error
	if byOid {
		statuses, err = a.c.CancelByOid(ctx, oids)
	} else {
		statuses, err = a.c.CancelByCloid(ctx, cloids)
	}
	if err != nil {
		if hl.IsUncertain(err) {
			out := make([]contract.CancelResult, len(reqs))
			for i, r := range reqs {
				out[i] = contract.CancelResult{OrderID: r.OrderID, ClientID: r.ClientID, Reason: err.Error()}
			}
			return out, fmt.Errorf("%w: %v", contract.ErrUncertain, err)
		}
		return nil, err
	}
	if len(statuses) != len(reqs) {
		return nil, fmt.Errorf("hyperliquid: cancel returned %d statuses for %d orders", len(statuses), len(reqs))
	}
	out := make([]contract.CancelResult, len(reqs))
	for i, s := range statuses {
		out[i] = contract.CancelResult{OrderID: reqs[i].OrderID, ClientID: reqs[i].ClientID, Canceled: s.OK}
		if !s.OK {
			out[i].Reason = s.Error
		}
	}
	return out, nil
}

// ArmDeadManSwitch schedules a cancel of all orders after the timeout. Hyperliquid
// scopes this to the whole account, so symbol is ignored. The venue requires at
// least 5 s of lead time (enforced by the native client). after <= 0 disarms.
func (a *Adapter) ArmDeadManSwitch(ctx context.Context, _ string, after time.Duration) error {
	if after <= 0 {
		return a.c.ScheduleCancel(ctx, nil)
	}
	at := time.Now().Add(after)
	return a.c.ScheduleCancel(ctx, &at)
}

// Positions lists perp positions. Hyperliquid returns one list for the account;
// a non-empty symbol filters it by coin.
func (a *Adapter) Positions(ctx context.Context, symbol string) ([]contract.Position, error) {
	rows, err := a.c.Positions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]contract.Position, 0, len(rows))
	for _, p := range rows {
		if symbol != "" && p.Coin != symbol {
			continue
		}
		out = append(out, contract.Position{Symbol: p.Coin, Qty: p.Szi, EntryPrice: p.EntryPx})
	}
	return out, nil
}

// Account reads the perp clearinghouse snapshot. Available is the withdrawable amount.
func (a *Adapter) Account(ctx context.Context) (contract.Account, error) {
	st, err := a.c.AccountState(ctx)
	if err != nil {
		return contract.Account{}, err
	}
	at := time.Time{}
	if st.Time > 0 {
		at = time.UnixMilli(st.Time)
	}
	return contract.Account{Currency: "USDC", Equity: st.AccountValue, Available: st.Withdrawable, At: at}, nil
}

// SetLeverage updates leverage for one coin. The native call takes isCross, so an
// empty mode keeps the current margin mode.
func (a *Adapter) SetLeverage(ctx context.Context, symbol string, leverage int, mode contract.MarginMode) error {
	if symbol == "" || leverage < 1 {
		return fmt.Errorf("%w: symbol and leverage >= 1 are required", contract.ErrInvalid)
	}
	isCross := true
	switch mode {
	case "":
		_, cur, err := a.c.Leverage(ctx, symbol)
		if err != nil {
			return err
		}
		isCross = cur
	case contract.Cross:
		isCross = true
	case contract.Isolated:
		isCross = false
	default:
		return fmt.Errorf("%w: margin mode %q", contract.ErrInvalid, mode)
	}
	return a.c.UpdateLeverage(ctx, symbol, leverage, isCross)
}
