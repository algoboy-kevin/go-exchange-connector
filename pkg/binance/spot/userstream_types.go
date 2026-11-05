package spot

import "strconv"

// User-data stream payloads. Decimals are strings exactly as the venue sent
// them; ids and timestamps are int64.
//
// The spot executionReport uses keys that differ only by case (c/C, s/S, x/X,
// f/F, p/P, q/Q, l/L, n/N, t/T, i/I, w/W, m/M, o/O, z/Z, v/V, u/U, a/A, b/B,
// d/D, j/J, ...). encoding/json prefers an exact key match but falls back to a
// case-INSENSITIVE one, so a key we do not declare would be decoded into the
// same-letter field of the other case (the 'a' allocation id into 'A', a
// string). Every key the user-data doc lists for executionReport is therefore
// declared with an explicit tag, including the conditional ones, and the tests
// pin the decode of the doc payloads.
//
// Sources (llms-full.txt, testnet user-data section): outboundAccountPosition
// payload lines 117234-117255; balanceUpdate 117258-117276; executionReport
// payload 117283-117327 and conditional-field table 117329-117480;
// eventStreamTerminated 117556. The mainnet user-data page (line 21147) holds
// the same events but embeds them as schema references, so the payload text is
// only in the testnet section (average price note: line 21203 and 117327).

// Execution types (x) and order statuses (X) seen on executionReport
// (enums, line 24913 onward; user-data doc line 117283).
const (
	ExecNew             = "NEW"
	ExecCanceled        = "CANCELED"
	ExecReplaced        = "REPLACED"
	ExecRejected        = "REJECTED"
	ExecTrade           = "TRADE"
	ExecExpired         = "EXPIRED"
	ExecTradePrevention = "TRADE_PREVENTION"
)

// ExecutionReport is the order-update event ("executionReport"). It is the
// content of the "event" object of a WebSocket API user-data frame.
type ExecutionReport struct {
	Event             string `json:"e"` // "executionReport"
	EventTime         int64  `json:"E"` // event time (ms)
	Symbol            string `json:"s"`
	ClientOrderID     string `json:"c"`
	Side              string `json:"S"`
	OrderType         string `json:"o"`
	TimeInForce       string `json:"f"`
	Quantity          string `json:"q"` // order quantity
	Price             string `json:"p"` // order price
	StopPrice         string `json:"P"`
	IcebergQty        string `json:"F"`
	OrderListID       int64  `json:"g"` // -1 when not part of a list
	OrigClientOrderID string `json:"C"` // the id of the order being cancelled (on CANCELED)
	ExecType          string `json:"x"` // current execution type
	Status            string `json:"X"` // current order status
	RejectReason      string `json:"r"` // "NONE" unless rejected
	OrderID           int64  `json:"i"`
	LastQty           string `json:"l"` // last executed quantity
	CumQty            string `json:"z"` // cumulative filled quantity
	LastPrice         string `json:"L"` // last executed price
	Commission        string `json:"n"` // commission amount
	CommissionAsset   string `json:"N"` // commission asset; null when none
	TransactTime      int64  `json:"T"`
	TradeID           int64  `json:"t"` // -1 when no trade
	ExecutionID       int64  `json:"I"`
	OnBook            bool   `json:"w"` // is the order on the book
	Maker             bool   `json:"m"` // is this trade the maker side
	Ignore            bool   `json:"M"` // documented as "Ignore"
	CreatedTime       int64  `json:"O"` // order creation time
	CumQuoteQty       string `json:"Z"` // cumulative quote asset transacted quantity
	LastQuoteQty      string `json:"Y"` // last quote asset transacted quantity
	QuoteOrderQty     string `json:"Q"`
	WorkingTime       int64  `json:"W"`
	STPMode           string `json:"V"` // SelfTradePreventionMode

	// Conditional fields (doc table, lines 117329-117480); zero when absent.
	TrailingDelta       int64  `json:"d,omitempty"`
	TrailingTime        int64  `json:"D,omitempty"`
	StrategyID          int64  `json:"j,omitempty"`
	StrategyType        int64  `json:"J,omitempty"`
	PreventedMatchID    int64  `json:"v,omitempty"`
	PreventedQty        string `json:"A,omitempty"`
	LastPreventedQty    string `json:"B,omitempty"`
	TradeGroupID        int64  `json:"u,omitempty"`
	CounterOrderID      int64  `json:"U,omitempty"`
	CounterSymbol       string `json:"Cs,omitempty"`
	PreventedExecQty    string `json:"pl,omitempty"`
	PreventedExecPrice  string `json:"pL,omitempty"`
	PreventedExecQuote  string `json:"pY,omitempty"`
	QuoteNotionalLimit  string `json:"Qq,omitempty"`
	EffectiveSTPMode    string `json:"eV,omitempty"`
	AllocationMatchType string `json:"b,omitempty"`
	AllocationID        int64  `json:"a,omitempty"`
	WorkingFloor        string `json:"k,omitempty"`
	UsedSOR             bool   `json:"uS,omitempty"`
	PegPriceType        string `json:"gP,omitempty"`
	PegOffsetType       string `json:"gOT,omitempty"`
	PegOffsetValue      int64  `json:"gOV,omitempty"`
	PeggedPrice         string `json:"gp,omitempty"`
}

// AvgPrice is Z/z ("Average price can be found by doing Z divided by z", line
// 117327), 8 decimals, or "" when nothing is filled.
func (e ExecutionReport) AvgPrice() string { return AvgPrice(e.CumQuoteQty, e.CumQty) }

// OrderIDString is the order id as a decimal string (the contract's form).
func (e ExecutionReport) OrderIDString() string { return strconv.FormatInt(e.OrderID, 10) }

// StreamBalance is one element of outboundAccountPosition.B.
type StreamBalance struct {
	Asset  string `json:"a"`
	Free   string `json:"f"`
	Locked string `json:"l"`
}

// OutboundAccountPosition is sent "any time an account balance has changed and
// contains the assets that were possibly changed" (line 117234).
type OutboundAccountPosition struct {
	Event          string          `json:"e"` // "outboundAccountPosition"
	EventTime      int64           `json:"E"`
	LastUpdateTime int64           `json:"u"` // time of last account update
	Balances       []StreamBalance `json:"B"`
}

// BalanceUpdate is sent on deposits, withdrawals and transfers between accounts
// (line 117258).
type BalanceUpdate struct {
	Event     string `json:"e"` // "balanceUpdate"
	EventTime int64  `json:"E"`
	Asset     string `json:"a"`
	Delta     string `json:"d"`
	ClearTime int64  `json:"T"`
}
