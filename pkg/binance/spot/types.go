package spot

import (
	"encoding/json"
	"fmt"
)

// Order sides, types and enums used by this client (enums doc, llms-full line
// 24913). Only the order types this client places are listed as constants.
const (
	SideBuy  = "BUY"
	SideSell = "SELL"

	TypeLimit      = "LIMIT"
	TypeLimitMaker = "LIMIT_MAKER" // post-only

	TIFGTC = "GTC"
	TIFIOC = "IOC"
	TIFFOK = "FOK"

	RespACK    = "ACK"
	RespRESULT = "RESULT"
	RespFULL   = "FULL"

	StatusNew             = "NEW"
	StatusPartiallyFilled = "PARTIALLY_FILLED"
	StatusFilled          = "FILLED"
	StatusCanceled        = "CANCELED"
	StatusRejected        = "REJECTED"
	StatusExpired         = "EXPIRED"
	StatusExpiredInMatch  = "EXPIRED_IN_MATCH"
)

// OrderReq describes a LIMIT or LIMIT_MAKER order. Decimals are strings.
// Side is BUY/SELL. For Type LIMIT, TimeInForce is GTC/IOC/FOK and required;
// for Type LIMIT_MAKER (post-only) TimeInForce must be empty (the order type
// carries the semantics; the venue rejects TIF on it as -1114 TIF_NOT_REQUIRED).
// NewOrderRespType defaults to RESULT when empty.
type OrderReq struct {
	Symbol           string
	Side             string
	Type             string
	TimeInForce      string
	Quantity         string
	Price            string
	NewClientOrderID string
	NewOrderRespType string
}

// CancelReq identifies the order to cancel or query: OrderID or
// OrigClientOrderID (at least one). If both are set the venue searches by
// orderId first and checks the client id against it (line 120237).
// NewClientOrderID optionally names the cancel request itself (cancel only).
type CancelReq struct {
	Symbol            string
	OrderID           int64
	OrigClientOrderID string
	NewClientOrderID  string
}

// Fill is one element of the FULL order response "fills" array (line 120020).
type Fill struct {
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	TradeID         int64  `json:"tradeId"`
}

// Order is a Binance spot order as returned by the REST API (new order ACK /
// RESULT / FULL, query order, open orders, cancel). Decimals are strings.
// Fields not present in a given response stay zero. For a cancel response the
// ClientOrderID is the id of the cancel request and OrigClientOrderID is the
// id of the order that was cancelled.
type Order struct {
	Symbol                  string `json:"symbol"`
	OrderID                 int64  `json:"orderId"`
	OrderListID             int64  `json:"orderListId"`
	ClientOrderID           string `json:"clientOrderId"`
	OrigClientOrderID       string `json:"origClientOrderId"`
	TransactTime            int64  `json:"transactTime"`
	Price                   string `json:"price"`
	OrigQty                 string `json:"origQty"`
	ExecutedQty             string `json:"executedQty"`
	OrigQuoteOrderQty       string `json:"origQuoteOrderQty"`
	CummulativeQuoteQty     string `json:"cummulativeQuoteQty"` // sic: the venue's spelling
	Status                  string `json:"status"`
	TimeInForce             string `json:"timeInForce"`
	Type                    string `json:"type"`
	Side                    string `json:"side"`
	StopPrice               string `json:"stopPrice"`
	IcebergQty              string `json:"icebergQty"`
	Time                    int64  `json:"time"`
	UpdateTime              int64  `json:"updateTime"`
	IsWorking               bool   `json:"isWorking"`
	WorkingTime             int64  `json:"workingTime"`
	SelfTradePreventionMode string `json:"selfTradePreventionMode"`
	Fills                   []Fill `json:"fills"`
}

// OrderListRef is an {symbol, orderId, clientOrderId} reference inside an order
// list.
type OrderListRef struct {
	Symbol        string `json:"symbol"`
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
}

// CancelledOrderList is an order-list (OCO/OTO) element of the DELETE
// /api/v3/openOrders response: "This includes orders that are part of an order
// list" (line 120300). This client places no order lists, but an account may
// hold them.
type CancelledOrderList struct {
	OrderListID       int64          `json:"orderListId"`
	ContingencyType   string         `json:"contingencyType"`
	ListStatusType    string         `json:"listStatusType"`
	ListOrderStatus   string         `json:"listOrderStatus"`
	ListClientOrderID string         `json:"listClientOrderId"`
	TransactionTime   int64          `json:"transactionTime"`
	Symbol            string         `json:"symbol"`
	Orders            []OrderListRef `json:"orders"`
	OrderReports      []Order        `json:"orderReports"`
}

// CancelAllResult is the DELETE /api/v3/openOrders response: the cancelled
// orders (Orders) and the cancelled order lists (OrderLists).
type CancelAllResult struct {
	Orders     []Order
	OrderLists []CancelledOrderList
}

// decodeCancelAll splits the mixed array: an element with contingencyType or
// orderReports is an order list, anything else is an order.
func decodeCancelAll(body []byte) (CancelAllResult, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		return CancelAllResult{}, err
	}
	var out CancelAllResult
	for i, raw := range raws {
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(raw, &probe); err != nil {
			return CancelAllResult{}, fmt.Errorf("element %d: %w", i, err)
		}
		_, isList := probe["contingencyType"]
		if _, ok := probe["orderReports"]; ok {
			isList = true
		}
		if isList {
			var l CancelledOrderList
			if err := json.Unmarshal(raw, &l); err != nil {
				return CancelAllResult{}, fmt.Errorf("element %d: %w", i, err)
			}
			out.OrderLists = append(out.OrderLists, l)
			continue
		}
		var o Order
		if err := json.Unmarshal(raw, &o); err != nil {
			return CancelAllResult{}, fmt.Errorf("element %d: %w", i, err)
		}
		out.Orders = append(out.Orders, o)
	}
	return out, nil
}

// Trade is one element of GET /api/v3/myTrades (line 122274).
type Trade struct {
	Symbol          string `json:"symbol"`
	ID              int64  `json:"id"`
	OrderID         int64  `json:"orderId"`
	OrderListID     int64  `json:"orderListId"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	IsBuyer         bool   `json:"isBuyer"`
	IsMaker         bool   `json:"isMaker"`
	IsBestMatch     bool   `json:"isBestMatch"`
}

// MyTradesReq selects trades. Symbol is required. The venue supports only
// these combinations (line 122307): symbol; +orderId; +startTime; +endTime;
// +fromId; +startTime+endTime; +orderId+fromId. The window between startTime
// and endTime cannot exceed 24 hours. Limit 0 = venue default (500), max 1000.
// Zero-valued integer fields are omitted.
type MyTradesReq struct {
	Symbol    string
	OrderID   int64
	StartTime int64
	EndTime   int64
	FromID    int64
	Limit     int
}

// Balance is one asset balance of GET /api/v3/account.
type Balance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

// CommissionRates are the account's commission rates.
type CommissionRates struct {
	Maker  string `json:"maker"`
	Taker  string `json:"taker"`
	Buyer  string `json:"buyer"`
	Seller string `json:"seller"`
}

// Account is GET /api/v3/account (line 121926). Balances are decimal strings.
type Account struct {
	MakerCommission            int64           `json:"makerCommission"`
	TakerCommission            int64           `json:"takerCommission"`
	BuyerCommission            int64           `json:"buyerCommission"`
	SellerCommission           int64           `json:"sellerCommission"`
	CommissionRates            CommissionRates `json:"commissionRates"`
	CanTrade                   bool            `json:"canTrade"`
	CanWithdraw                bool            `json:"canWithdraw"`
	CanDeposit                 bool            `json:"canDeposit"`
	Brokered                   bool            `json:"brokered"`
	RequireSelfTradePrevention bool            `json:"requireSelfTradePrevention"`
	PreventSor                 bool            `json:"preventSor"`
	UpdateTime                 int64           `json:"updateTime"`
	AccountType                string          `json:"accountType"`
	Balances                   []Balance       `json:"balances"`
	Permissions                []string        `json:"permissions"`
	UID                        int64           `json:"uid"`
}

// Balance returns the balance of asset (zero strings if the account holds none).
func (a Account) Balance(asset string) Balance {
	for _, b := range a.Balances {
		if b.Asset == asset {
			return b
		}
	}
	return Balance{Asset: asset, Free: "0", Locked: "0"}
}

// BookTicker is GET /api/v3/ticker/bookTicker for one symbol (public, line 119481).
type BookTicker struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidQty   string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskQty   string `json:"askQty"`
}

// ─────────────────────────────────────────────────────────────
// exchangeInfo
// ─────────────────────────────────────────────────────────────

// ExchangeInfo is the typed subset of GET /api/v3/exchangeInfo (line 118488).
type ExchangeInfo struct {
	Timezone   string      `json:"timezone"`
	ServerTime int64       `json:"serverTime"`
	RateLimits []RateLimit `json:"rateLimits"`
	Symbols    []Symbol    `json:"symbols"`
}

// RateLimit is one entry of exchangeInfo.rateLimits (enums, line 25049).
type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}

// Symbol is one entry of exchangeInfo.symbols.
type Symbol struct {
	Symbol                         string   `json:"symbol"`
	Status                         string   `json:"status"`
	BaseAsset                      string   `json:"baseAsset"`
	BaseAssetPrecision             int      `json:"baseAssetPrecision"`
	QuoteAsset                     string   `json:"quoteAsset"`
	QuoteAssetPrecision            int      `json:"quoteAssetPrecision"`
	OrderTypes                     []string `json:"orderTypes"`
	IsSpotTradingAllowed           bool     `json:"isSpotTradingAllowed"`
	Filters                        []Filter `json:"filters"`
	Permissions                    []string `json:"permissions"`
	DefaultSelfTradePreventionMode string   `json:"defaultSelfTradePreventionMode"`
}

// Filter is a symbol filter (filters doc, line 23897). Only the fields needed
// for price/quantity/notional rounding and the percent-price band are typed;
// the others stay empty. Note the spot notional field is minNotional (in both
// MIN_NOTIONAL and NOTIONAL), not "notional" as on USD-M futures.
type Filter struct {
	FilterType string `json:"filterType"`
	// PRICE_FILTER
	MinPrice string `json:"minPrice"`
	MaxPrice string `json:"maxPrice"`
	TickSize string `json:"tickSize"`
	// LOT_SIZE
	MinQty   string `json:"minQty"`
	MaxQty   string `json:"maxQty"`
	StepSize string `json:"stepSize"`
	// MIN_NOTIONAL / NOTIONAL
	MinNotional      string `json:"minNotional"`
	MaxNotional      string `json:"maxNotional"`
	ApplyToMarket    bool   `json:"applyToMarket"`
	ApplyMinToMarket bool   `json:"applyMinToMarket"`
	ApplyMaxToMarket bool   `json:"applyMaxToMarket"`
	AvgPriceMins     int    `json:"avgPriceMins"`
	// PERCENT_PRICE
	MultiplierUp   string `json:"multiplierUp"`
	MultiplierDown string `json:"multiplierDown"`
	// PERCENT_PRICE_BY_SIDE
	BidMultiplierUp   string `json:"bidMultiplierUp"`
	BidMultiplierDown string `json:"bidMultiplierDown"`
	AskMultiplierUp   string `json:"askMultiplierUp"`
	AskMultiplierDown string `json:"askMultiplierDown"`
}

// Rules are the order-entry rules distilled from a symbol's filters. A rule the
// symbol does not carry is the empty string; "0" in the venue's data means the
// rule is disabled (filters doc, line 23913).
type Rules struct {
	TickSize    string
	MinPrice    string
	MaxPrice    string
	StepSize    string
	MinQty      string
	MaxQty      string
	MinNotional string // from NOTIONAL if present, else MIN_NOTIONAL
	MaxNotional string // from NOTIONAL
	// BidMultiplierDown is the lowest BUY price as a fraction of the reference
	// average: PERCENT_PRICE_BY_SIDE.bidMultiplierDown, else
	// PERCENT_PRICE.multiplierDown.
	BidMultiplierDown string
}

// Rules extracts the order-entry rules. PRICE_FILTER.tickSize and
// LOT_SIZE.stepSize must exist and be positive.
func (s Symbol) Rules() (Rules, error) {
	var r Rules
	var pct string
	for _, f := range s.Filters {
		switch f.FilterType {
		case "PRICE_FILTER":
			r.TickSize, r.MinPrice, r.MaxPrice = f.TickSize, f.MinPrice, f.MaxPrice
		case "LOT_SIZE":
			r.StepSize, r.MinQty, r.MaxQty = f.StepSize, f.MinQty, f.MaxQty
		case "MIN_NOTIONAL":
			if r.MinNotional == "" {
				r.MinNotional = f.MinNotional
			}
		case "NOTIONAL":
			r.MinNotional, r.MaxNotional = f.MinNotional, f.MaxNotional
		case "PERCENT_PRICE_BY_SIDE":
			r.BidMultiplierDown = f.BidMultiplierDown
		case "PERCENT_PRICE":
			pct = f.MultiplierDown
		}
	}
	if r.BidMultiplierDown == "" {
		r.BidMultiplierDown = pct
	}
	if !ValidPositiveDecimal(r.TickSize) {
		return Rules{}, fmt.Errorf("symbol %s: missing or invalid tickSize %q", s.Symbol, r.TickSize)
	}
	if !ValidPositiveDecimal(r.StepSize) {
		return Rules{}, fmt.Errorf("symbol %s: missing or invalid stepSize %q", s.Symbol, r.StepSize)
	}
	return r, nil
}

// SupportsOrderType reports whether the symbol lists the order type
// (exchangeInfo orderTypes, e.g. LIMIT_MAKER).
func (s Symbol) SupportsOrderType(t string) bool {
	for _, o := range s.OrderTypes {
		if o == t {
			return true
		}
	}
	return false
}

// ExecutionRule is one rule of GET /api/v3/executionRules (line 118606). Only
// PRICE_RANGE exists today.
type ExecutionRule struct {
	RuleType         string `json:"ruleType"`
	BidLimitMultUp   string `json:"bidLimitMultUp"`
	BidLimitMultDown string `json:"bidLimitMultDown"`
	AskLimitMultUp   string `json:"askLimitMultUp"`
	AskLimitMultDown string `json:"askLimitMultDown"`
}

// SymbolExecutionRules is the per-symbol element of the executionRules response.
type SymbolExecutionRules struct {
	Symbol string          `json:"symbol"`
	Rules  []ExecutionRule `json:"rules"`
}

// ExecutionRules is GET /api/v3/executionRules.
type ExecutionRules struct {
	SymbolRules []SymbolExecutionRules `json:"symbolRules"`
}
