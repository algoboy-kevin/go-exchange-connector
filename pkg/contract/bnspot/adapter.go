// Package bnspot adapts the Binance SPOT client to the shared contract. Native
// payloads stay in pkg/binance/spot; this file maps names, enums and errors.
//
// The contract is written for perpetual venues. What changes on spot, decided
// here and visible in the tests:
//
//   - No batch endpoint exists (the spot order endpoint list has none), so
//     PlaceOrders and CancelOrders send ONE REQUEST PER ITEM, sequentially, in
//     order. MaxPlaceBatch and MaxCancelBatch are a LOCAL cap of 5 (the same as
//     the futures place cap), chosen only to bound the latency and the order
//     rate a single call can spend; the venue has no such number. Callers chunk
//     to it as for any BatchLimiter.
//   - Reduce-only does not exist on spot: a request with ReduceOnly set is
//     rejected locally, before anything is sent, with an error that wraps both
//     contract.ErrUnsupported (the venue does not offer it) and
//     contract.ErrInvalid (nothing sent). It is never dropped silently.
//   - Post-only is the order type LIMIT_MAKER, not a time in force: the request
//     carries type=LIMIT_MAKER and no timeInForce. GTC and IOC are LIMIT with
//     that timeInForce. The contract has no FOK, so FOK is not exposed.
//   - AccountReader reports ONE asset, the adapter's quote asset (default USDT,
//     option WithAccountAsset): Equity is free+locked, Available is free. Spot
//     has no account-wide collateral value; other assets are not converted.
//   - CancelAll uses DELETE /api/v3/openOrders. When nothing is open the venue
//     is believed to answer -2011 "Unknown order sent."; that is treated as
//     success (nothing left to cancel). The docs do not state this answer, see
//     spot.Client.CancelAll.
package bnspot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/spot"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

// Local batch caps. The venue has no batch endpoint; see the package comment.
const (
	maxPlaceBatch  = 5
	maxCancelBatch = 5
)

// defaultAccountAsset is the quote asset AccountReader reports.
const defaultAccountAsset = "USDT"

// Adapter implements contract.Trader, BatchLimiter, CancelAller and
// AccountReader for Binance spot.
type Adapter struct {
	c     *spot.Client
	asset string
}

// Compile-time interface checks come before any mapping code on purpose: a
// signature drift in the contract fails the build here, not in a caller.
var (
	_ contract.Trader        = (*Adapter)(nil)
	_ contract.BatchLimiter  = (*Adapter)(nil)
	_ contract.CancelAller   = (*Adapter)(nil)
	_ contract.AccountReader = (*Adapter)(nil)
)

// Option configures an Adapter.
type Option func(*Adapter)

// WithAccountAsset sets the asset AccountReader reports (default USDT).
func WithAccountAsset(asset string) Option {
	return func(a *Adapter) {
		if asset != "" {
			a.asset = asset
		}
	}
}

// New wraps a configured spot client.
func New(c *spot.Client, opts ...Option) *Adapter {
	a := &Adapter{c: c, asset: defaultAccountAsset}
	for _, o := range opts {
		o(a)
	}
	return a
}

// MaxPlaceBatch is a local cap (5); spot has no batch endpoint.
func (*Adapter) MaxPlaceBatch() int { return maxPlaceBatch }

// MaxCancelBatch is a local cap (5); spot has no batch endpoint.
func (*Adapter) MaxCancelBatch() int { return maxCancelBatch }

// ─────────────────────────────────────────────────────────────
// Mapping
// ─────────────────────────────────────────────────────────────

// orderTypeAndTIF maps the contract TIF. PostOnly is the LIMIT_MAKER type with
// no timeInForce.
func orderTypeAndTIF(t contract.TIF) (typ, tif string, err error) {
	switch t {
	case contract.GTC:
		return spot.TypeLimit, spot.TIFGTC, nil
	case contract.IOC:
		return spot.TypeLimit, spot.TIFIOC, nil
	case contract.PostOnly:
		return spot.TypeLimitMaker, "", nil
	}
	return "", "", fmt.Errorf("%w: tif %q", contract.ErrInvalid, t)
}

// toOrderReq validates and builds the native request. RESULT is requested so
// the response carries status, executedQty and cummulativeQuoteQty.
func toOrderReq(r contract.OrderRequest) (spot.OrderReq, error) {
	if err := r.Validate(); err != nil {
		return spot.OrderReq{}, err
	}
	if r.ReduceOnly {
		return spot.OrderReq{}, fmt.Errorf("%w: reduce-only does not exist on spot; %w", contract.ErrUnsupported, contract.ErrInvalid)
	}
	if r.ClientID != "" && !spot.ValidClientOrderID(r.ClientID) {
		return spot.OrderReq{}, fmt.Errorf("%w: client id %q must match ^[a-zA-Z0-9-_]{1,36}$", contract.ErrInvalid, r.ClientID)
	}
	typ, tif, err := orderTypeAndTIF(r.TIF)
	if err != nil {
		return spot.OrderReq{}, err
	}
	return spot.OrderReq{
		Symbol:           r.Symbol,
		Side:             string(r.Side),
		Type:             typ,
		TimeInForce:      tif,
		Quantity:         r.Qty,
		Price:            r.Price,
		NewClientOrderID: r.ClientID,
		NewOrderRespType: spot.RespRESULT,
	}, nil
}

// mapStatus maps a Binance spot order status. NEW and PARTIALLY_FILLED are
// resting; FILLED is filled; REJECTED, EXPIRED, CANCELED and EXPIRED_IN_MATCH
// (self-trade prevention) are rejected, with the venue status as the Reason.
// An IOC that fills only partly ends EXPIRED: it maps to Rejected and its
// FilledQty still reports what filled. Anything else is Unknown.
func mapStatus(s string) contract.Status {
	switch s {
	case spot.StatusNew, spot.StatusPartiallyFilled:
		return contract.Resting
	case spot.StatusFilled:
		return contract.Filled
	case spot.StatusRejected, spot.StatusExpired, spot.StatusCanceled, spot.StatusExpiredInMatch:
		return contract.Rejected
	}
	return contract.Unknown
}

func resultFromOrder(o *spot.Order, clientID string, sent, ack time.Time) contract.OrderResult {
	res := contract.OrderResult{
		ClientID:  clientID,
		Status:    mapStatus(o.Status),
		FilledQty: o.ExecutedQty,
		AvgPrice:  spot.AvgPrice(o.CummulativeQuoteQty, o.ExecutedQty),
		SentAt:    sent,
		AckAt:     ack,
	}
	if o.OrderID != 0 {
		res.OrderID = strconv.FormatInt(o.OrderID, 10)
	}
	switch res.Status {
	case contract.Rejected:
		res.Reason = o.Status
	case contract.Unknown:
		res.Reason = "unrecognised order status " + strconv.Quote(o.Status)
	}
	return res
}

// callLevel reports an error that is not a verdict on the order: the request
// was refused before it was evaluated (rate limit, ban, WAF, clock, signature,
// bad key) or never left the client. These stop a loop and are returned as Go
// errors, because a Rejected result would invite an immediate resend.
func callLevel(err error) bool {
	if spot.IsRateLimited(err) || spot.IsBanned(err) || spot.IsWAFBlocked(err) || spot.IsTimestampError(err) || spot.IsInvalidSignature(err) {
		return true
	}
	var ae *spot.APIError
	if errors.As(err, &ae) {
		return ae.HTTPStatus == 401
	}
	return true // not a venue answer at all (e.g. missing credentials)
}

// placeError maps a failed placement. uncertain => Unknown result + ErrUncertain.
// A definite venue refusal => Rejected result, no error. Call-level => Go error.
func placeError(err error, clientID string, sent, ack time.Time) (res contract.OrderResult, callErr error, stop bool) {
	if spot.IsUncertain(err) {
		return contract.OrderResult{ClientID: clientID, Status: contract.Unknown, Reason: err.Error(), SentAt: sent, AckAt: ack},
			fmt.Errorf("%w: %w", contract.ErrUncertain, err), true
	}
	if callLevel(err) {
		return contract.OrderResult{}, fmt.Errorf("binance spot: place order: %w", err), true
	}
	var ae *spot.APIError
	errors.As(err, &ae)
	return contract.OrderResult{ClientID: clientID, Status: contract.Rejected, Reason: ae.Msg, SentAt: sent, AckAt: ack}, nil, false
}

// ─────────────────────────────────────────────────────────────
// Trader
// ─────────────────────────────────────────────────────────────

// PlaceOrder places one limit order (POST /api/v3/order).
func (a *Adapter) PlaceOrder(ctx context.Context, req contract.OrderRequest) (contract.OrderResult, error) {
	nreq, err := toOrderReq(req)
	if err != nil {
		return contract.OrderResult{}, err
	}
	sent := time.Now()
	o, err := a.c.NewOrder(ctx, nreq)
	ack := time.Now()
	if err != nil {
		res, callErr, _ := placeError(err, req.ClientID, sent, ack)
		return res, callErr
	}
	return resultFromOrder(o, req.ClientID, sent, ack), nil
}

// PlaceOrders places up to 5 orders, one request each, sequentially. All
// requests are validated first and nothing is sent if any is invalid. It
// returns one result per request, in order. A definite refusal of one order is
// a Rejected result and the loop continues. It stops at the first request whose
// outcome is unknown (Unknown result, error wraps ErrUncertain) or that fails
// at call level (rate limit, ban, ...): the remaining requests were NOT sent,
// and are reported Unknown (after an uncertain outcome) or Rejected (after a
// call-level failure) with a Reason that starts with "not sent".
func (a *Adapter) PlaceOrders(ctx context.Context, reqs []contract.OrderRequest) ([]contract.OrderResult, error) {
	if err := contract.CheckBatch(len(reqs), maxPlaceBatch); err != nil {
		return nil, err
	}
	nreqs := make([]spot.OrderReq, len(reqs))
	for i, r := range reqs {
		n, err := toOrderReq(r)
		if err != nil {
			return nil, fmt.Errorf("order %d: %w", i, err)
		}
		nreqs[i] = n
	}
	out := make([]contract.OrderResult, len(reqs))
	for i, n := range nreqs {
		sent := time.Now()
		o, err := a.c.NewOrder(ctx, n)
		ack := time.Now()
		if err == nil {
			out[i] = resultFromOrder(o, reqs[i].ClientID, sent, ack)
			continue
		}
		res, callErr, stop := placeError(err, reqs[i].ClientID, sent, ack)
		if !stop {
			out[i] = res
			continue
		}
		unsent := contract.Rejected
		why := "not sent: an earlier order in this call failed at call level: " + err.Error()
		if callErr != nil && errors.Is(callErr, contract.ErrUncertain) {
			unsent = contract.Unknown
			why = "not sent: an earlier order in this call has an unknown outcome"
			out[i] = res
		} else {
			// The failing item itself was not evaluated by the venue either.
			out[i] = contract.OrderResult{ClientID: reqs[i].ClientID, Status: contract.Rejected, Reason: "not sent: " + err.Error(), SentAt: sent, AckAt: ack}
		}
		for j := i + 1; j < len(reqs); j++ {
			out[j] = contract.OrderResult{ClientID: reqs[j].ClientID, Status: unsent, Reason: why}
		}
		return out, callErr
	}
	return out, nil
}

func parseOrderID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: order id %q is not a positive integer", contract.ErrInvalid, s)
	}
	return id, nil
}

// toCancelReq validates and builds the native cancel. OrderID wins when both
// are set, and only the order id is sent (the venue is faster that way).
func toCancelReq(r contract.CancelRequest) (spot.CancelReq, error) {
	if err := r.Validate(); err != nil {
		return spot.CancelReq{}, err
	}
	if r.OrderID != "" {
		id, err := parseOrderID(r.OrderID)
		if err != nil {
			return spot.CancelReq{}, err
		}
		return spot.CancelReq{Symbol: r.Symbol, OrderID: id}, nil
	}
	if !spot.ValidClientOrderID(r.ClientID) {
		return spot.CancelReq{}, fmt.Errorf("%w: client id %q must match ^[a-zA-Z0-9-_]{1,36}$", contract.ErrInvalid, r.ClientID)
	}
	return spot.CancelReq{Symbol: r.Symbol, OrigClientOrderID: r.ClientID}, nil
}

// cancelOne sends one cancel and maps the answer. stop reports that the caller
// must not send further requests.
func (a *Adapter) cancelOne(ctx context.Context, req contract.CancelRequest, n spot.CancelReq) (res contract.CancelResult, callErr error, stop bool) {
	o, err := a.c.CancelOrder(ctx, n)
	if err == nil {
		res = contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Canceled: true}
		if o != nil && o.OrderID != 0 {
			res.OrderID = strconv.FormatInt(o.OrderID, 10)
		}
		return res, nil, false
	}
	if spot.IsUncertain(err) {
		return contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Reason: err.Error()},
			fmt.Errorf("%w: %w", contract.ErrUncertain, err), true
	}
	if callLevel(err) {
		return contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Reason: "not sent: " + err.Error()},
			fmt.Errorf("binance spot: cancel order: %w", err), true
	}
	var ae *spot.APIError
	errors.As(err, &ae)
	return contract.CancelResult{OrderID: req.OrderID, ClientID: req.ClientID, Reason: ae.Msg}, nil, false
}

// CancelOrder cancels one order by order id (preferred) or client id.
func (a *Adapter) CancelOrder(ctx context.Context, req contract.CancelRequest) (contract.CancelResult, error) {
	n, err := toCancelReq(req)
	if err != nil {
		return contract.CancelResult{}, err
	}
	res, callErr, _ := a.cancelOne(ctx, req, n)
	return res, callErr
}

// CancelOrders cancels up to 5 orders, one request each, sequentially. Requests
// may name different symbols and mix order ids and client ids (each is its own
// request). All are validated first; nothing is sent if any is invalid. A
// definite refusal (e.g. unknown order) is a result with Canceled false. It
// stops at the first uncertain or call-level failure; the remaining requests
// were not sent and carry a Reason starting with "not sent".
func (a *Adapter) CancelOrders(ctx context.Context, reqs []contract.CancelRequest) ([]contract.CancelResult, error) {
	if err := contract.CheckBatch(len(reqs), maxCancelBatch); err != nil {
		return nil, err
	}
	nreqs := make([]spot.CancelReq, len(reqs))
	for i, r := range reqs {
		n, err := toCancelReq(r)
		if err != nil {
			return nil, fmt.Errorf("cancel %d: %w", i, err)
		}
		nreqs[i] = n
	}
	out := make([]contract.CancelResult, len(reqs))
	for i, n := range nreqs {
		res, callErr, stop := a.cancelOne(ctx, reqs[i], n)
		out[i] = res
		if !stop {
			continue
		}
		why := "not sent: an earlier cancel in this call failed"
		if errors.Is(callErr, contract.ErrUncertain) {
			why = "not sent: an earlier cancel in this call has an unknown outcome"
		}
		for j := i + 1; j < len(reqs); j++ {
			out[j] = contract.CancelResult{OrderID: reqs[j].OrderID, ClientID: reqs[j].ClientID, Reason: why}
		}
		return out, callErr
	}
	return out, nil
}

// OpenOrders lists open orders for one symbol. A symbol is required: without
// one the venue charges weight 80 instead of 6.
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
		ts := o.UpdateTime
		if ts == 0 {
			ts = o.Time
		}
		out = append(out, contract.Order{
			Symbol:    o.Symbol,
			OrderID:   strconv.FormatInt(o.OrderID, 10),
			ClientID:  o.ClientOrderID,
			Side:      contract.Side(o.Side),
			Price:     o.Price,
			Qty:       o.OrigQty,
			FilledQty: o.ExecutedQty,
			Timestamp: time.UnixMilli(ts),
		})
	}
	return out, nil
}

// CancelAll cancels every open order on a symbol. "Nothing open" is success.
func (a *Adapter) CancelAll(ctx context.Context, symbol string) error {
	if symbol == "" {
		return fmt.Errorf("%w: symbol is empty", contract.ErrInvalid)
	}
	_, err := a.c.CancelAll(ctx, symbol)
	if err == nil || spot.IsUnknownOrder(err) {
		return nil
	}
	if spot.IsUncertain(err) {
		return fmt.Errorf("%w: %w", contract.ErrUncertain, err)
	}
	return err
}

// Account reports the configured quote asset: Equity free+locked, Available free.
func (a *Adapter) Account(ctx context.Context) (contract.Account, error) {
	acc, err := a.c.Account(ctx, false)
	if err != nil {
		return contract.Account{}, err
	}
	b := acc.Balance(a.asset)
	eq, err := spot.AddDecimal(b.Free, b.Locked)
	if err != nil {
		return contract.Account{}, fmt.Errorf("binance spot: balance of %s: %w", a.asset, err)
	}
	at := time.Now()
	if acc.UpdateTime > 0 {
		at = time.UnixMilli(acc.UpdateTime)
	}
	return contract.Account{Currency: a.asset, Equity: eq, Available: b.Free, At: at}, nil
}
