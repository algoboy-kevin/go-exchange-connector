package futures

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

// ErrTooManyOrders is returned locally by BatchOrders (> 5) and CancelBatch
// (> 10) without contacting the server.
var ErrTooManyOrders = errors.New("binance futures: too many orders in batch")

const (
	maxBatchOrders  = 5
	maxCancelOrders = 10
)

// orderParams appends an OrderReq to p using the single-order parameter names
// in a fixed order. reduceOnly and newOrderRespType are always present (the
// documented batch field set), so single and batch placements share one shape.
func orderParams(p *params, r OrderReq) {
	p.add("symbol", r.Symbol)
	p.add("side", r.Side)
	p.add("type", r.Type)
	if r.TimeInForce != "" {
		p.add("timeInForce", r.TimeInForce)
	}
	if r.Quantity != "" {
		p.add("quantity", r.Quantity)
	}
	if r.Price != "" {
		p.add("price", r.Price)
	}
	p.add("reduceOnly", strconv.FormatBool(r.ReduceOnly))
	if r.NewClientOrderID != "" {
		p.add("newClientOrderId", r.NewClientOrderID)
	}
	resp := r.NewOrderRespType
	if resp == "" {
		resp = "ACK"
	}
	p.add("newOrderRespType", resp)
}

// batchOrder is the JSON shape of one element of the batchOrders parameter.
// Field names match the single-order parameters; reduceOnly is a string.
type batchOrder struct {
	Symbol           string `json:"symbol"`
	Side             string `json:"side"`
	Type             string `json:"type"`
	TimeInForce      string `json:"timeInForce,omitempty"`
	Quantity         string `json:"quantity,omitempty"`
	Price            string `json:"price,omitempty"`
	ReduceOnly       string `json:"reduceOnly"`
	NewClientOrderID string `json:"newClientOrderId,omitempty"`
	NewOrderRespType string `json:"newOrderRespType,omitempty"`
}

func toBatchOrder(r OrderReq) batchOrder {
	resp := r.NewOrderRespType
	if resp == "" {
		resp = "ACK"
	}
	return batchOrder{
		Symbol:           r.Symbol,
		Side:             r.Side,
		Type:             r.Type,
		TimeInForce:      r.TimeInForce,
		Quantity:         r.Quantity,
		Price:            r.Price,
		ReduceOnly:       strconv.FormatBool(r.ReduceOnly),
		NewClientOrderID: r.NewClientOrderID,
		NewOrderRespType: resp,
	}
}

// NewOrder places a single order (POST /fapi/v1/order, signed). On success the
// ACK/RESULT order is returned.
func (c *Client) NewOrder(ctx context.Context, req OrderReq) (*Order, error) {
	p := newParams()
	orderParams(p, req)
	body, err := c.do(ctx, http.MethodPost, "/fapi/v1/order", p, true)
	if err != nil {
		return nil, err
	}
	var out Order
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}

// BatchOrders places up to 5 orders (POST /fapi/v1/batchOrders). The batch is
// sent as the batchOrders query parameter: a JSON array of order objects. The
// 200 response is a JSON array whose elements are each either an order or an
// error object; per-item errors are decoded into BatchItem.Err and are NOT
// returned as the call's error. More than 5 orders is rejected locally
// (ErrTooManyOrders) without contacting the server.
func (c *Client) BatchOrders(ctx context.Context, reqs []OrderReq) ([]BatchItem, error) {
	if len(reqs) > maxBatchOrders {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrTooManyOrders, len(reqs), maxBatchOrders)
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	items := make([]batchOrder, len(reqs))
	for i, r := range reqs {
		items[i] = toBatchOrder(r)
	}
	raw, err := json.Marshal(items)
	if err != nil {
		return nil, &transportError{err: err}
	}
	p := newParams()
	p.add("batchOrders", string(raw))
	body, err := c.do(ctx, http.MethodPost, "/fapi/v1/batchOrders", p, true)
	if err != nil {
		return nil, err
	}
	return decodeBatchItems(body)
}

// CancelOrder cancels a single order by client order id
// (DELETE /fapi/v1/order, signed, origClientOrderId).
func (c *Client) CancelOrder(ctx context.Context, symbol, clientOrderID string) (*Order, error) {
	p := newParams()
	p.add("symbol", symbol)
	p.add("origClientOrderId", clientOrderID)
	body, err := c.do(ctx, http.MethodDelete, "/fapi/v1/order", p, true)
	if err != nil {
		return nil, err
	}
	var out Order
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}

// CancelBatch cancels up to 10 orders by client order id
// (DELETE /fapi/v1/batchOrders, signed, origClientOrderIdList = JSON array of
// ids). Decoding is per-item, exactly like BatchOrders.
func (c *Client) CancelBatch(ctx context.Context, symbol string, clientOrderIDs []string) ([]BatchItem, error) {
	if len(clientOrderIDs) > maxCancelOrders {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrTooManyOrders, len(clientOrderIDs), maxCancelOrders)
	}
	if len(clientOrderIDs) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(clientOrderIDs)
	if err != nil {
		return nil, &transportError{err: err}
	}
	p := newParams()
	p.add("symbol", symbol)
	p.add("origClientOrderIdList", string(raw))
	body, err := c.do(ctx, http.MethodDelete, "/fapi/v1/batchOrders", p, true)
	if err != nil {
		return nil, err
	}
	return decodeBatchItems(body)
}

// CancelAll cancels all open orders for a symbol
// (DELETE /fapi/v1/allOpenOrders, signed).
func (c *Client) CancelAll(ctx context.Context, symbol string) error {
	p := newParams()
	p.add("symbol", symbol)
	_, err := c.do(ctx, http.MethodDelete, "/fapi/v1/allOpenOrders", p, true)
	return err
}

// CountdownCancelAll arms the countdown cancel-all timer for a symbol
// (POST /fapi/v1/countdownCancelAll, signed). countdownMillis 0 disables it.
func (c *Client) CountdownCancelAll(ctx context.Context, symbol string, countdownMillis int64) error {
	p := newParams()
	p.add("symbol", symbol)
	p.add("countdownTime", strconv.FormatInt(countdownMillis, 10))
	_, err := c.do(ctx, http.MethodPost, "/fapi/v1/countdownCancelAll", p, true)
	return err
}

// OpenOrders lists open orders (GET /fapi/v1/openOrders, signed). An empty
// symbol lists all symbols.
func (c *Client) OpenOrders(ctx context.Context, symbol string) ([]Order, error) {
	var p *params
	if symbol != "" {
		p = newParams()
		p.add("symbol", symbol)
	}
	body, err := c.do(ctx, http.MethodGet, "/fapi/v1/openOrders", p, true)
	if err != nil {
		return nil, err
	}
	var out []Order
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return out, nil
}

// decodeBatchItems decodes a batch response: a JSON array whose elements are
// each either an order object or {"code":N,"msg":"..."}.
func decodeBatchItems(body []byte) ([]BatchItem, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, &transportError{err: err}
	}
	out := make([]BatchItem, len(raws))
	for i, raw := range raws {
		out[i] = decodeBatchItem(raw)
	}
	return out, nil
}

func decodeBatchItem(raw json.RawMessage) BatchItem {
	var probe struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &probe); err == nil && probe.Code != 0 {
		return BatchItem{Err: &APIError{Code: probe.Code, Msg: probe.Msg}}
	}
	var o Order
	if err := json.Unmarshal(raw, &o); err != nil {
		return BatchItem{Err: &APIError{Msg: err.Error()}}
	}
	return BatchItem{Order: &o}
}
