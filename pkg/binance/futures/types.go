package futures

// OrderReq describes a single order to place. Decimals (Quantity, Price) are
// strings. Side is "BUY"/"SELL"; Type is e.g. "LIMIT"; TimeInForce is
// "GTC"/"IOC"/"GTX" (post-only). NewOrderRespType defaults to "ACK" when empty
// (fills arrive on the user-data stream; ACK keeps responses small).
type OrderReq struct {
	Symbol           string
	Side             string
	Type             string
	TimeInForce      string
	Quantity         string
	Price            string
	ReduceOnly       bool
	NewClientOrderID string
	NewOrderRespType string
}

// Order is a Binance order as returned by the REST API. Decimals are strings;
// OrderID and UpdateTime are integers.
type Order struct {
	OrderID       int64  `json:"orderId"`
	ClientOrderID string `json:"clientOrderId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	Status        string `json:"status"`
	Price         string `json:"price"`
	OrigQty       string `json:"origQty"`
	ExecutedQty   string `json:"executedQty"`
	AvgPrice      string `json:"avgPrice"`
	TimeInForce   string `json:"timeInForce"`
	ReduceOnly    bool   `json:"reduceOnly"`
	UpdateTime    int64  `json:"updateTime"`
}

// BatchItem is one element of a batchOrders / cancel-batch response. Exactly
// one of Order / Err is set. A per-item error is NOT a Go error of the
// enclosing call — the HTTP status is 200.
type BatchItem struct {
	Order *Order
	Err   *APIError
}

// ─────────────────────────────────────────────────────────────
// exchangeInfo
// ─────────────────────────────────────────────────────────────

// ExchangeInfo is the typed subset of GET /fapi/v1/exchangeInfo.
type ExchangeInfo struct {
	Timezone   string      `json:"timezone"`
	ServerTime int64       `json:"serverTime"`
	RateLimits []RateLimit `json:"rateLimits"`
	Symbols    []Symbol    `json:"symbols"`
}

// RateLimit is one entry of exchangeInfo.rateLimits.
type RateLimit struct {
	RateLimitType string `json:"rateLimitType"`
	Interval      string `json:"interval"`
	IntervalNum   int    `json:"intervalNum"`
	Limit         int    `json:"limit"`
}

// Symbol is one symbol entry of exchangeInfo.symbols.
type Symbol struct {
	Symbol            string   `json:"symbol"`
	Status            string   `json:"status"`
	ContractType      string   `json:"contractType"`
	PricePrecision    int      `json:"pricePrecision"`
	QuantityPrecision int      `json:"quantityPrecision"`
	Filters           []Filter `json:"filters"`
}

// Filter is a symbol filter. Only the fields relevant to price/quantity
// rounding and minimums are typed; unavailable fields stay empty.
type Filter struct {
	FilterType string `json:"filterType"`
	TickSize   string `json:"tickSize"`
	MinPrice   string `json:"minPrice"`
	MaxPrice   string `json:"maxPrice"`
	StepSize   string `json:"stepSize"`
	MinQty     string `json:"minQty"`
	MaxQty     string `json:"maxQty"`
	Notional   string `json:"notional"`
}

// ─────────────────────────────────────────────────────────────
// positionRisk
// ─────────────────────────────────────────────────────────────

// PositionRisk is one entry of GET /fapi/v3/positionRisk. Decimals are
// strings. Leverage is present in the v3 payload.
type PositionRisk struct {
	Symbol           string `json:"symbol"`
	PositionSide     string `json:"positionSide"`
	PositionAmt      string `json:"positionAmt"`
	EntryPrice       string `json:"entryPrice"`
	MarkPrice        string `json:"markPrice"`
	UnRealizedProfit string `json:"unRealizedProfit"`
	Leverage         string `json:"leverage"`
}
