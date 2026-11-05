package futures

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// CancelOrderByID cancels a single order by exchange order id
// (DELETE /fapi/v1/order, signed, orderId). orderID must be positive.
func (c *Client) CancelOrderByID(ctx context.Context, symbol string, orderID int64) (*Order, error) {
	if symbol == "" || orderID <= 0 {
		return nil, &ConfigError{Reason: "cancel by order id needs a symbol and a positive order id"}
	}
	p := newParams()
	p.add("symbol", symbol)
	p.add("orderId", strconv.FormatInt(orderID, 10))
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

// CancelBatchByID cancels up to 10 orders by exchange order id
// (DELETE /fapi/v1/batchOrders, signed, orderIdList = JSON array of integers).
// Decoding is per item, like CancelBatch.
func (c *Client) CancelBatchByID(ctx context.Context, symbol string, orderIDs []int64) ([]BatchItem, error) {
	if len(orderIDs) > maxCancelOrders {
		return nil, fmt.Errorf("%w: got %d, max %d", ErrTooManyOrders, len(orderIDs), maxCancelOrders)
	}
	if len(orderIDs) == 0 {
		return nil, nil
	}
	for _, id := range orderIDs {
		if id <= 0 {
			return nil, &ConfigError{Reason: "cancel batch by order id needs positive order ids"}
		}
	}
	raw, err := json.Marshal(orderIDs)
	if err != nil {
		return nil, &transportError{err: err}
	}
	p := newParams()
	p.add("symbol", symbol)
	p.add("orderIdList", strings.ReplaceAll(string(raw), " ", ""))
	body, err := c.do(ctx, http.MethodDelete, "/fapi/v1/batchOrders", p, true)
	if err != nil {
		return nil, err
	}
	return decodeBatchItems(body)
}

// GetOrder queries one order (GET /fapi/v1/order, signed). Send orderID > 0 or
// a client order id; the order id wins when both are given. An unknown order
// is an APIError with code -2013 (see IsUnknownOrder).
func (c *Client) GetOrder(ctx context.Context, symbol string, orderID int64, clientOrderID string) (*Order, error) {
	if symbol == "" || (orderID <= 0 && clientOrderID == "") {
		return nil, &ConfigError{Reason: "get order needs a symbol and an order id or client order id"}
	}
	p := newParams()
	p.add("symbol", symbol)
	if orderID > 0 {
		p.add("orderId", strconv.FormatInt(orderID, 10))
	} else {
		p.add("origClientOrderId", clientOrderID)
	}
	body, err := c.do(ctx, http.MethodGet, "/fapi/v1/order", p, true)
	if err != nil {
		return nil, err
	}
	var out Order
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}
