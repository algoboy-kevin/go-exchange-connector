package futures

// User-data stream payloads. Every decimal is a string exactly as the venue
// sent it; ids and timestamps are int64.
//
// The Binance JSON keys are case-sensitive and several differ only by case
// (s/S, x/X, l/L, n/N at the order level; T at the event level vs t in the
// order). encoding/json matches tags case-INSENSITIVELY only when no exact
// match exists, and prefers the exact one — so every field below carries an
// explicit tag and the fixtures in the tests pin the exact-case decode.

// OrderTradeUpdate is an ORDER_TRADE_UPDATE event: the event/transaction
// timestamps plus the venue's "o" order object.
type OrderTradeUpdate struct {
	Event           string      `json:"e"` // "ORDER_TRADE_UPDATE"
	EventTime       int64       `json:"E"` // event time (ms)
	TransactionTime int64       `json:"T"` // transaction time (ms)
	Order           OrderUpdate `json:"o"`
}

// OrderUpdate is the "o" object of an ORDER_TRADE_UPDATE.
//
// ExecType (x) is NEW, TRADE, CANCELED, EXPIRED, CALCULATED or AMENDMENT;
// Status (X) is the order status. LastQty/CumQty/LastPrice are zero/empty on
// non-fill executions; Commission/CommissionAsset are set on TRADE;
// ExpiryReason (er) is present on EXPIRED.
type OrderUpdate struct {
	Symbol          string `json:"s"`  // symbol
	ClientOrderID   string `json:"c"`  // client order id
	Side            string `json:"S"`  // BUY / SELL
	Type            string `json:"o"`  // order type
	TimeInForce     string `json:"f"`  // GTC / IOC / GTX
	OrigQty         string `json:"q"`  // original quantity
	Price           string `json:"p"`  // order price
	AvgPrice        string `json:"ap"` // average fill price
	ExecType        string `json:"x"`  // execution type
	Status          string `json:"X"`  // order status
	OrderID         int64  `json:"i"`  // order id
	LastQty         string `json:"l"`  // last filled quantity
	CumQty          string `json:"z"`  // cumulative filled quantity
	LastPrice       string `json:"L"`  // last fill price
	Commission      string `json:"n"`  // commission for this fill
	CommissionAsset string `json:"N"`  // commission asset
	TradeID         int64  `json:"t"`  // trade id
	Maker           bool   `json:"m"`  // the fill was the maker
	ReduceOnly      bool   `json:"R"`  // reduce-only order
	PositionSide    string `json:"ps"` // BOTH / LONG / SHORT
	ExpiryReason    string `json:"er"` // present on EXPIRED

	// OrderTime is the venue's order-trade time ("T" inside the order object).
	// It is declared purely so encoding/json consumes it with an exact match:
	// without a "T" tag the case-insensitive fallback would match the key "T"
	// to TradeID ("t") and overwrite the trade id with a timestamp.
	OrderTime int64 `json:"T"`
}

// AccountUpdate is an ACCOUNT_UPDATE event (balance and/or position change).
type AccountUpdate struct {
	Event     string            `json:"e"` // "ACCOUNT_UPDATE"
	EventTime int64             `json:"E"` // event time (ms)
	Time      int64             `json:"T"` // transaction time (ms)
	Account   AccountUpdateData `json:"a"`
}

// AccountUpdateData is the "a" object of an ACCOUNT_UPDATE. Reason is the
// update trigger ("DEPOSIT", "ORDER", "FUNDING_FEE", …).
type AccountUpdateData struct {
	Reason    string            `json:"m"`
	Balances  []AccountBalance  `json:"B"`
	Positions []AccountPosition `json:"P"`
}

// AccountBalance is one entry of AccountUpdateData.Balances.
type AccountBalance struct {
	Asset              string `json:"a"`  // asset, e.g. "USDT"
	WalletBalance      string `json:"wb"` // wallet balance
	CrossWalletBalance string `json:"cw"` // cross wallet balance
}

// AccountPosition is one entry of AccountUpdateData.Positions. PositionAmount
// (pa) is negative for a short; EntryPrice (ep) is the position entry price.
type AccountPosition struct {
	Symbol         string `json:"s"`  // symbol
	PositionAmount string `json:"pa"` // position quantity
	EntryPrice     string `json:"ep"` // entry price
	PositionSide   string `json:"ps"` // BOTH / LONG / SHORT
}
