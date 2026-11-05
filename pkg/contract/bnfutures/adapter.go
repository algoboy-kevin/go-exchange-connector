// Package bnfutures adapts the Binance USDS-M futures client to the shared
// contract. Native payloads stay in pkg/binance/futures; this file only maps
// names, enums and errors.
package bnfutures

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/futures"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

// Venue batch limits (see futures.maxBatchOrders / maxCancelOrders).
const (
	maxPlaceBatch  = 5
	maxCancelBatch = 10
)

// Adapter implements contract.Trader, BatchLimiter, CancelAller, DeadManSwitch,
// PositionReader and LeverageSetter for Binance USDS-M futures.
type Adapter struct {
	c *futures.Client
}

// New wraps a configured futures client.
func New(c *futures.Client) *Adapter { return &Adapter{c: c} }

var (
	_ contract.Trader         = (*Adapter)(nil)
	_ contract.BatchLimiter   = (*Adapter)(nil)
	_ contract.CancelAller    = (*Adapter)(nil)
	_ contract.DeadManSwitch  = (*Adapter)(nil)
	_ contract.PositionReader = (*Adapter)(nil)
	_ contract.LeverageSetter = (*Adapter)(nil)
)

// MaxPlaceBatch is 5 on Binance USDS-M (batchOrders).
func (*Adapter) MaxPlaceBatch() int { return maxPlaceBatch }

// MaxCancelBatch is 10 on Binance USDS-M (cancel batch).
func (*Adapter) MaxCancelBatch() int { return maxCancelBatch }

// tifValue maps the contract TIF to Binance. Post-only is GTX on futures.
func tifValue(t contract.TIF) (string, error) {
	switch t {
	case contract.GTC:
		return "GTC", nil
	case contract.IOC:
		return "IOC", nil
	case contract.PostOnly:
		return "GTX", nil
	}
	return "", fmt.Errorf("%w: tif %q", contract.ErrInvalid, t)
}

// toOrderReq builds the native request. Type is always LIMIT here.
func toOrderReq(r contract.OrderRequest) (futures.OrderReq, error) {
	if err := r.Validate(); err != nil {
		return futures.OrderReq{}, err
	}
	tif, err := tifValue(r.TIF)
	if err != nil {
		return futures.OrderReq{}, err
	}
	return futures.OrderReq{
		Symbol:           r.Symbol,
		Side:             string(r.Side),
		Type:             "LIMIT",
		TimeInForce:      tif,
		Quantity:         r.Qty,
		Price:            r.Price,
		ReduceOnly:       r.ReduceOnly,
		NewClientOrderID: r.ClientID,
		NewOrderRespType: "RESULT",
	}, nil
}

// mapStatus maps a Binance order status to the contract. A non-final status
// (NEW, PARTIALLY_FILLED) is resting.
func mapStatus(s string) contract.Status {
	switch s {
	case "NEW", "PARTIALLY_FILLED":
		return contract.Resting
	case "FILLED":
		return contract.Filled
	case "REJECTED", "EXPIRED", "CANCELED":
		return contract.Rejected
	}
	return contract.Unknown
}

// resultFromOrder converts a venue order into a placement result.
func resultFromOrder(o *futures.Order, clientID string, sent, ack time.Time) contract.OrderResult {
	res := contract.OrderResult{
		ClientID:  clientID,
		Status:    mapStatus(o.Status),
		FilledQty: o.ExecutedQty,
		AvgPrice:  o.AvgPrice,
		SentAt:    sent,
		AckAt:     ack,
	}
	if o.OrderID != 0 {
		res.OrderID = strconv.FormatInt(o.OrderID, 10)
	}
	if res.Status == contract.Rejected {
		res.Reason = o.Status
	}
	return res
}

// resultFromError maps a per-item or call error. An uncertain outcome is Unknown
// and wraps ErrUncertain. A definite venue rejection is Rejected. Anything else
// is returned as a call error (the order was not placed).
func resultFromError(err error, clientID string, sent, ack time.Time) (contract.OrderResult, error) {
	if futures.IsUncertain(err) {
		return contract.OrderResult{
			ClientID: clientID, Status: contract.Unknown, Reason: err.Error(),
			SentAt: sent, AckAt: ack,
		}, fmt.Errorf("%w: %v", contract.ErrUncertain, err)
	}
	var apiErr *futures.APIError
	if errors.As(err, &apiErr) {
		return contract.OrderResult{
			ClientID: clientID, Status: contract.Rejected, Reason: apiErr.Msg,
			SentAt: sent, AckAt: ack,
		}, nil
	}
	return contract.OrderResult{}, err
}

// PlaceOrder places one limit order (POST /fapi/v1/order).
func (a *Adapter) PlaceOrder(ctx context.Context, req contract.OrderRequest) (contract.OrderResult, error) {
	nreq, err := toOrderReq(req)
	if err != nil {
		return contract.OrderResult{}, err
	}
	sent := time.Now()
	o, err := a.c.NewOrder(ctx, nreq)
	ack := time.Now()
	if err != nil {
		return resultFromError(err, req.ClientID, sent, ack)
	}
	return resultFromOrder(o, req.ClientID, sent, ack), nil
}

// PlaceOrders places up to 5 limit orders in one call (batchOrders). Results are
// in request order. A per-item rejection is a Rejected result, not an error.
func (a *Adapter) PlaceOrders(ctx context.Context, reqs []contract.OrderRequest) ([]contract.OrderResult, error) {
	if err := contract.CheckBatch(len(reqs), maxPlaceBatch); err != nil {
		return nil, err
	}
	nreqs := make([]futures.OrderReq, len(reqs))
	for i, r := range reqs {
		n, err := toOrderReq(r)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", i, err)
		}
		nreqs[i] = n
	}
	sent := time.Now()
	items, err := a.c.BatchOrders(ctx, nreqs)
	ack := time.Now()
	if err != nil {
		if futures.IsUncertain(err) {
			out := make([]contract.OrderResult, len(reqs))
			for i, r := range reqs {
				out[i] = contract.OrderResult{ClientID: r.ClientID, Status: contract.Unknown,
					Reason: err.Error(), SentAt: sent, AckAt: ack}
			}
			return out, fmt.Errorf("%w: %v", contract.ErrUncertain, err)
		}
		return nil, err
	}
	if len(items) != len(reqs) {
		return nil, fmt.Errorf("binance futures: batch returned %d items for %d orders", len(items), len(reqs))
	}
	out := make([]contract.OrderResult, len(reqs))
	for i, it := range items {
		if it.Err != nil {
			res, err := resultFromError(it.Err, reqs[i].ClientID, sent, ack)
			if err != nil {
				return nil, err
			}
			out[i] = res
			continue
		}
		out[i] = resultFromOrder(it.Order, reqs[i].ClientID, sent, ack)
	}
	return out, nil
}

// CancelOrder cancels by exchange order id when OrderID is set (it wins when
// both are set), otherwise by client order id.
func (a *Adapter) CancelOrder(ctx context.Context, req contract.CancelRequest) (contract.CancelResult, error) {
	if err := req.Validate(); err != nil {
		return contract.CancelResult{}, err
	}
	var (
		o   *futures.Order
		err error
	)
	if req.OrderID != "" {
		id, perr := parseOrderID(req.OrderID)
		if perr != nil {
			return contract.CancelResult{}, perr
		}
		o, err = a.c.CancelOrderByID(ctx, req.Symbol, id)
	} else {
		o, err = a.c.CancelOrder(ctx, req.Symbol, req.ClientID)
	}
	if err != nil {
		if futures.IsUncertain(err) {
			return contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Reason: err.Error()},
				fmt.Errorf("%w: %v", contract.ErrUncertain, err)
		}
		var apiErr *futures.APIError
		if errors.As(err, &apiErr) {
			return contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Reason: apiErr.Msg}, nil
		}
		return contract.CancelResult{}, err
	}
	res := contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Canceled: true}
	if o != nil && o.OrderID != 0 {
		res.OrderID = strconv.FormatInt(o.OrderID, 10)
	}
	return res, nil
}

// parseOrderID converts a contract order id to Binance's positive integer.
func parseOrderID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: order id %q is not a positive integer", contract.ErrInvalid, s)
	}
	return id, nil
}

// CancelOrders cancels up to 10 orders in one call. All requests must be on one
// symbol and must all use order ids (OrderID wins when both are set) or all use
// client ids; a mixed batch is rejected locally because Binance takes one list.
func (a *Adapter) CancelOrders(ctx context.Context, reqs []contract.CancelRequest) ([]contract.CancelResult, error) {
	if err := contract.CheckBatch(len(reqs), maxCancelBatch); err != nil {
		return nil, err
	}
	symbol := reqs[0].Symbol
	byID := reqs[0].OrderID != ""
	oids := make([]int64, 0, len(reqs))
	cids := make([]string, 0, len(reqs))
	for i, r := range reqs {
		if err := r.Validate(); err != nil {
			return nil, fmt.Errorf("cancel %d: %w", i, err)
		}
		if r.Symbol != symbol {
			return nil, fmt.Errorf("%w: one symbol per cancel batch (got %s and %s)", contract.ErrInvalid, symbol, r.Symbol)
		}
		if (r.OrderID != "") != byID {
			return nil, fmt.Errorf("cancel %d: %w: a cancel batch cannot mix order ids and client ids", i, contract.ErrInvalid)
		}
		if byID {
			id, err := parseOrderID(r.OrderID)
			if err != nil {
				return nil, fmt.Errorf("cancel %d: %w", i, err)
			}
			oids = append(oids, id)
		} else {
			cids = append(cids, r.ClientID)
		}
	}
	var (
		items []futures.BatchItem
		err   error
	)
	if byID {
		items, err = a.c.CancelBatchByID(ctx, symbol, oids)
	} else {
		items, err = a.c.CancelBatch(ctx, symbol, cids)
	}
	if err != nil {
		if futures.IsUncertain(err) {
			out := make([]contract.CancelResult, len(reqs))
			for i, r := range reqs {
				out[i] = contract.CancelResult{OrderID: r.OrderID, ClientID: r.ClientID, Reason: err.Error()}
			}
			return out, fmt.Errorf("%w: %v", contract.ErrUncertain, err)
		}
		return nil, err
	}
	if len(items) != len(reqs) {
		return nil, fmt.Errorf("binance futures: cancel batch returned %d items for %d orders", len(items), len(reqs))
	}
	out := make([]contract.CancelResult, len(reqs))
	for i, it := range items {
		out[i] = contract.CancelResult{OrderID: reqs[i].OrderID, ClientID: reqs[i].ClientID, Canceled: it.Err == nil}
		if it.Err != nil {
			var apiErr *futures.APIError
			if errors.As(it.Err, &apiErr) {
				out[i].Reason = apiErr.Msg
			} else {
				out[i].Reason = it.Err.Error()
			}
		} else if it.Order != nil && it.Order.OrderID != 0 {
			out[i].OrderID = strconv.FormatInt(it.Order.OrderID, 10)
		}
	}
	return out, nil
}

// OpenOrders lists open orders for one symbol.
func (a *Adapter) OpenOrders(ctx context.Context, symbol string) ([]contract.Order, error) {
	if symbol == "" {
		return nil, fmt.Errorf("%w: symbol is empty", contract.ErrInvalid)
	}
	orders, err := a.c.OpenOrders(ctx, symbol)
	if err != nil {
		return nil, err
	}
	out := make([]contract.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, contract.Order{
			Symbol:     o.Symbol,
			OrderID:    strconv.FormatInt(o.OrderID, 10),
			ClientID:   o.ClientOrderID,
			Side:       contract.Side(o.Side),
			Price:      o.Price,
			Qty:        o.OrigQty,
			FilledQty:  o.ExecutedQty,
			ReduceOnly: o.ReduceOnly,
			Timestamp:  time.UnixMilli(o.UpdateTime),
		})
	}
	return out, nil
}

// CancelAll cancels every open order for a symbol (DELETE allOpenOrders).
func (a *Adapter) CancelAll(ctx context.Context, symbol string) error {
	if symbol == "" {
		return fmt.Errorf("%w: symbol is empty", contract.ErrInvalid)
	}
	return a.c.CancelAll(ctx, symbol)
}

// ArmDeadManSwitch arms countdownCancelAll for a symbol. Calling it again renews
// the countdown. after <= 0 disarms (countdownTime 0). Binance scopes this per symbol.
func (a *Adapter) ArmDeadManSwitch(ctx context.Context, symbol string, after time.Duration) error {
	if symbol == "" {
		return fmt.Errorf("%w: symbol is empty", contract.ErrInvalid)
	}
	var ms int64
	if after > 0 {
		ms = after.Milliseconds()
		if ms < 1 {
			return fmt.Errorf("%w: dead-man timeout %s rounds to 0 ms", contract.ErrInvalid, after)
		}
	}
	return a.c.CountdownCancelAll(ctx, symbol, ms)
}

// Positions lists positions. An empty symbol returns every position, including
// zero-size ones, as the venue reports them.
func (a *Adapter) Positions(ctx context.Context, symbol string) ([]contract.Position, error) {
	rows, err := a.c.PositionRisk(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]contract.Position, 0, len(rows))
	for _, p := range rows {
		if symbol != "" && p.Symbol != symbol {
			continue
		}
		out = append(out, contract.Position{Symbol: p.Symbol, Qty: p.PositionAmt, EntryPrice: p.EntryPrice})
	}
	return out, nil
}

// SetLeverage sets leverage, then margin mode if mode is set. Binance names
// cross margin "CROSSED".
func (a *Adapter) SetLeverage(ctx context.Context, symbol string, leverage int, mode contract.MarginMode) error {
	if symbol == "" || leverage < 1 {
		return fmt.Errorf("%w: symbol and leverage >= 1 are required", contract.ErrInvalid)
	}
	if err := a.c.SetLeverage(ctx, symbol, leverage); err != nil {
		return err
	}
	switch mode {
	case "":
		return nil
	case contract.Cross:
		return a.c.SetMarginType(ctx, symbol, "CROSSED")
	case contract.Isolated:
		return a.c.SetMarginType(ctx, symbol, "ISOLATED")
	}
	return fmt.Errorf("%w: margin mode %q", contract.ErrInvalid, mode)
}
