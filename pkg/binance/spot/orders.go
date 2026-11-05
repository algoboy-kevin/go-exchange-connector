package spot

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// clientOrderIDRE is the documented client order id form for the FIX API
// (llms-full line 22600). The REST page states no rule (see package doc); the
// client applies the same one locally so a bad id never reaches the venue
// (-2039 CLIENT_ORDER_ID_INVALID, line 24809).
var clientOrderIDRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,36}$`)

// ValidClientOrderID reports whether id matches ^[a-zA-Z0-9-_]{1,36}$.
func ValidClientOrderID(id string) bool { return clientOrderIDRE.MatchString(id) }

func validSymbol(s string) bool {
	return s != "" && !strings.ContainsAny(s, " \t\r\n&=?#%\"'")
}

// Validate checks an order request locally. Nothing is sent when it fails.
// Rules (lines 119920-119930 type table, 24495-24503 TIF errors):
// LIMIT needs timeInForce in GTC/IOC/FOK, quantity and price; LIMIT_MAKER
// needs quantity and price and no timeInForce.
func (r OrderReq) Validate() error {
	if !validSymbol(r.Symbol) {
		return invalidf("symbol %q", r.Symbol)
	}
	if r.Side != SideBuy && r.Side != SideSell {
		return invalidf("side %q (want BUY or SELL)", r.Side)
	}
	switch r.Type {
	case TypeLimit:
		switch r.TimeInForce {
		case TIFGTC, TIFIOC, TIFFOK:
		case "":
			return invalidf("type LIMIT needs timeInForce (GTC, IOC or FOK)")
		default:
			return invalidf("timeInForce %q (want GTC, IOC or FOK)", r.TimeInForce)
		}
	case TypeLimitMaker:
		if r.TimeInForce != "" {
			return invalidf("type LIMIT_MAKER takes no timeInForce (got %q)", r.TimeInForce)
		}
	default:
		return invalidf("type %q (this client places LIMIT and LIMIT_MAKER)", r.Type)
	}
	if !ValidPositiveDecimal(r.Quantity) {
		return invalidf("quantity %q must be a positive plain decimal", r.Quantity)
	}
	if !ValidPositiveDecimal(r.Price) {
		return invalidf("price %q must be a positive plain decimal", r.Price)
	}
	if r.NewClientOrderID != "" && !ValidClientOrderID(r.NewClientOrderID) {
		return invalidf("newClientOrderId %q must match ^[a-zA-Z0-9-_]{1,36}$", r.NewClientOrderID)
	}
	switch r.NewOrderRespType {
	case "", RespACK, RespRESULT, RespFULL:
	default:
		return invalidf("newOrderRespType %q (want ACK, RESULT or FULL)", r.NewOrderRespType)
	}
	return nil
}

// Validate checks a cancel/query identifier locally. Symbol is required and so
// is at least one of OrderID / OrigClientOrderID (lines 120236, 120173).
func (r CancelReq) Validate() error {
	if !validSymbol(r.Symbol) {
		return invalidf("symbol %q", r.Symbol)
	}
	if r.OrderID < 0 {
		return invalidf("orderId %d must be positive", r.OrderID)
	}
	if r.OrderID == 0 && r.OrigClientOrderID == "" {
		return invalidf("need orderId or origClientOrderId")
	}
	if r.OrigClientOrderID != "" && !ValidClientOrderID(r.OrigClientOrderID) {
		return invalidf("origClientOrderId %q must match ^[a-zA-Z0-9-_]{1,36}$", r.OrigClientOrderID)
	}
	if r.NewClientOrderID != "" && !ValidClientOrderID(r.NewClientOrderID) {
		return invalidf("newClientOrderId %q must match ^[a-zA-Z0-9-_]{1,36}$", r.NewClientOrderID)
	}
	return nil
}

const maxTradesLimit = 1000

const day24h = int64(24 * 3600 * 1000)

// Validate checks the myTrades selector (combinations: line 122307).
func (r MyTradesReq) Validate() error {
	if !validSymbol(r.Symbol) {
		return invalidf("symbol %q", r.Symbol)
	}
	if r.OrderID < 0 || r.StartTime < 0 || r.EndTime < 0 || r.FromID < 0 {
		return invalidf("orderId, startTime, endTime and fromId must not be negative")
	}
	if r.Limit < 0 || r.Limit > maxTradesLimit {
		return invalidf("limit %d out of range 0..%d", r.Limit, maxTradesLimit)
	}
	hasTime := r.StartTime > 0 || r.EndTime > 0
	if r.OrderID > 0 && hasTime {
		return invalidf("orderId cannot be combined with startTime/endTime")
	}
	if r.FromID > 0 && hasTime {
		return invalidf("fromId cannot be combined with startTime/endTime")
	}
	if r.StartTime > 0 && r.EndTime > 0 {
		if r.EndTime < r.StartTime {
			return invalidf("endTime before startTime")
		}
		if r.EndTime-r.StartTime > day24h {
			return invalidf("startTime..endTime cannot span more than 24 hours")
		}
	}
	return nil
}

func orderParams(p *params, r OrderReq) {
	p.add("symbol", r.Symbol)
	p.add("side", r.Side)
	p.add("type", r.Type)
	if r.TimeInForce != "" {
		p.add("timeInForce", r.TimeInForce)
	}
	p.add("quantity", r.Quantity)
	p.add("price", r.Price)
	if r.NewClientOrderID != "" {
		p.add("newClientOrderId", r.NewClientOrderID)
	}
	resp := r.NewOrderRespType
	if resp == "" {
		resp = RespRESULT
	}
	p.add("newOrderRespType", resp)
}

func cancelParams(p *params, r CancelReq, withNewID bool) {
	p.add("symbol", r.Symbol)
	if r.OrderID > 0 {
		p.add("orderId", strconv.FormatInt(r.OrderID, 10))
	}
	if r.OrigClientOrderID != "" {
		p.add("origClientOrderId", r.OrigClientOrderID)
	}
	if withNewID && r.NewClientOrderID != "" {
		p.add("newClientOrderId", r.NewClientOrderID)
	}
}

func decodeOrder(body []byte) (*Order, error) {
	var o Order
	if err := json.Unmarshal(body, &o); err != nil {
		return nil, &transportError{err: err}
	}
	return &o, nil
}

// NewOrder places one order (POST /api/v3/order, TRADE, weight 1, unfilled
// order count 1, line 119876). The request is validated first; a validation
// failure is a *ValidationError and nothing is sent. On a transport error, 5xx
// or -1007 the outcome is unknown (IsUncertain): reconcile by newClientOrderId.
func (c *Client) NewOrder(ctx context.Context, req OrderReq) (*Order, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p := newParams()
	orderParams(p, req)
	body, err := c.do(ctx, http.MethodPost, "/api/v3/order", p, true)
	if err != nil {
		return nil, err
	}
	return decodeOrder(body)
}

// TestOrder validates an order and the signature/recvWindow against the venue
// without sending it to the matching engine (POST /api/v3/order/test, TRADE,
// weight 1, line 120090). The success response is {}. It checks filters such as
// tickSize and minNotional, so it is the safe way to try a plan.
func (c *Client) TestOrder(ctx context.Context, req OrderReq) error {
	if err := req.Validate(); err != nil {
		return err
	}
	p := newParams()
	orderParams(p, req)
	_, err := c.do(ctx, http.MethodPost, "/api/v3/order/test", p, true)
	return err
}

// CancelOrder cancels one active order (DELETE /api/v3/order, TRADE, weight 1,
// line 120212) by OrderID or OrigClientOrderID. The returned Order has status
// CANCELED.
func (c *Client) CancelOrder(ctx context.Context, req CancelReq) (*Order, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p := newParams()
	cancelParams(p, req, true)
	body, err := c.do(ctx, http.MethodDelete, "/api/v3/order", p, true)
	if err != nil {
		return nil, err
	}
	return decodeOrder(body)
}

// CancelAll cancels every open order on a symbol (DELETE /api/v3/openOrders,
// TRADE, weight 1, line 120296) and returns what was cancelled.
//
// UNVERIFIED IN THE DOCS: the endpoint page does not say what the venue answers
// when no order is open. Real venues are reported to answer -2011 "Unknown
// order sent." (a message the errors page lists at line 24830); if so it is
// returned as an *APIError for which IsUnknownOrder is true, and callers that
// mean "make sure nothing is open" should treat that as success. The unit
// tests only pin that this client passes such an error through unchanged.
func (c *Client) CancelAll(ctx context.Context, symbol string) (CancelAllResult, error) {
	if !validSymbol(symbol) {
		return CancelAllResult{}, invalidf("symbol %q", symbol)
	}
	p := newParams()
	p.add("symbol", symbol)
	body, err := c.do(ctx, http.MethodDelete, "/api/v3/openOrders", p, true)
	if err != nil {
		return CancelAllResult{}, err
	}
	out, derr := decodeCancelAll(body)
	if derr != nil {
		return CancelAllResult{}, &transportError{err: derr}
	}
	return out, nil
}

// GetOrder queries one order (GET /api/v3/order, USER_DATA, weight 4, line
// 120151) by OrderID or OrigClientOrderID.
func (c *Client) GetOrder(ctx context.Context, req CancelReq) (*Order, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p := newParams()
	cancelParams(p, req, false)
	body, err := c.do(ctx, http.MethodGet, "/api/v3/order", p, true)
	if err != nil {
		return nil, err
	}
	return decodeOrder(body)
}

// OpenOrders lists open orders (GET /api/v3/openOrders, USER_DATA, line 121985).
// Weight is 6 for one symbol and 80 without a symbol: "Careful when accessing
// this with no symbol". An empty symbol lists all symbols.
func (c *Client) OpenOrders(ctx context.Context, symbol string) ([]Order, error) {
	var p *params
	if symbol != "" {
		if !validSymbol(symbol) {
			return nil, invalidf("symbol %q", symbol)
		}
		p = newParams()
		p.add("symbol", symbol)
	}
	body, err := c.do(ctx, http.MethodGet, "/api/v3/openOrders", p, true)
	if err != nil {
		return nil, err
	}
	var out []Order
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return out, nil
}

// MyTrades lists the account's trades for a symbol (GET /api/v3/myTrades,
// USER_DATA, weight 20, or 5 with orderId, line 122274).
func (c *Client) MyTrades(ctx context.Context, req MyTradesReq) ([]Trade, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	p := newParams()
	p.add("symbol", req.Symbol)
	if req.OrderID > 0 {
		p.add("orderId", strconv.FormatInt(req.OrderID, 10))
	}
	if req.StartTime > 0 {
		p.add("startTime", strconv.FormatInt(req.StartTime, 10))
	}
	if req.EndTime > 0 {
		p.add("endTime", strconv.FormatInt(req.EndTime, 10))
	}
	if req.FromID > 0 {
		p.add("fromId", strconv.FormatInt(req.FromID, 10))
	}
	if req.Limit > 0 {
		p.add("limit", strconv.Itoa(req.Limit))
	}
	body, err := c.do(ctx, http.MethodGet, "/api/v3/myTrades", p, true)
	if err != nil {
		return nil, err
	}
	var out []Trade
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return out, nil
}

// Account returns the account information with balances (GET /api/v3/account,
// USER_DATA, weight 20, line 121926). omitZeroBalances hides zero balances.
func (c *Client) Account(ctx context.Context, omitZeroBalances bool) (*Account, error) {
	var p *params
	if omitZeroBalances {
		p = newParams()
		p.add("omitZeroBalances", "true")
	}
	body, err := c.do(ctx, http.MethodGet, "/api/v3/account", p, true)
	if err != nil {
		return nil, err
	}
	var out Account
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}

// ─────────────────────────────────────────────────────────────
// Public endpoints
// ─────────────────────────────────────────────────────────────

// ExchangeInfo returns trading rules and symbol filters (GET
// /api/v3/exchangeInfo, public, weight 20, line 118488). With no symbols the
// venue returns every symbol; one symbol is sent as symbol=, several as
// symbols=["A","B"].
func (c *Client) ExchangeInfo(ctx context.Context, symbols ...string) (*ExchangeInfo, error) {
	var p *params
	switch len(symbols) {
	case 0:
	case 1:
		if !validSymbol(symbols[0]) {
			return nil, invalidf("symbol %q", symbols[0])
		}
		p = newParams()
		p.add("symbol", symbols[0])
	default:
		for _, s := range symbols {
			if !validSymbol(s) {
				return nil, invalidf("symbol %q", s)
			}
		}
		raw, err := json.Marshal(symbols)
		if err != nil {
			return nil, invalidf("symbols: %v", err)
		}
		p = newParams()
		p.add("symbols", string(raw))
	}
	body, err := c.do(ctx, http.MethodGet, "/api/v3/exchangeInfo", p, false)
	if err != nil {
		return nil, err
	}
	var out ExchangeInfo
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}

// BookTicker returns the best bid/ask for one symbol (GET
// /api/v3/ticker/bookTicker, public, weight 2, line 119481).
func (c *Client) BookTicker(ctx context.Context, symbol string) (*BookTicker, error) {
	if !validSymbol(symbol) {
		return nil, invalidf("symbol %q", symbol)
	}
	p := newParams()
	p.add("symbol", symbol)
	body, err := c.do(ctx, http.MethodGet, "/api/v3/ticker/bookTicker", p, false)
	if err != nil {
		return nil, err
	}
	var out BookTicker
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}

// ExecutionRules returns the execution rules of a symbol (GET
// /api/v3/executionRules, public, weight 2, line 118606).
func (c *Client) ExecutionRules(ctx context.Context, symbol string) (*ExecutionRules, error) {
	if !validSymbol(symbol) {
		return nil, invalidf("symbol %q", symbol)
	}
	p := newParams()
	p.add("symbol", symbol)
	body, err := c.do(ctx, http.MethodGet, "/api/v3/executionRules", p, false)
	if err != nil {
		return nil, err
	}
	var out ExecutionRules
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &transportError{err: err}
	}
	return &out, nil
}
