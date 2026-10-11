package exchange

import "time"

// Order is one order to place. Px and Sz are decimal strings exactly as they
// should reach the venue; Tif is "Alo" (post-only), "Ioc" or "Gtc"; Cloid is
// "0x" + 32 hex characters (empty = no client order id).
type Order struct {
	Coin       string
	IsBuy      bool
	Px         string
	Sz         string
	Tif        string
	ReduceOnly bool
	Cloid      string
}

// Time-in-force values accepted by Order.Tif (the venue's exact spellings).
const (
	TifAlo = "Alo" // add liquidity only (post-only)
	TifIoc = "Ioc" // immediate or cancel
	TifGtc = "Gtc" // good till cancel
)

// OrderStatus is the venue's per-order result inside a bulk order response.
// Exactly one of Resting / Filled is normally set; Error carries a per-item
// rejection (e.g. "Post only order would have immediately matched") and is NOT
// an error of the enclosing Place call.
type OrderStatus struct {
	Resting bool   // accepted and resting on the book
	Filled  bool   // fully and immediately filled
	Oid     int64  // venue order id (resting or filled)
	TotalSz string // filled size (Filled only)
	AvgPx   string // average fill price (Filled only)
	Error   string // per-item rejection message
}

// CancelStatus is the venue's per-item result of a cancel request: "success" is
// OK, anything else (including "Order was never placed, already canceled, or
// filled.") is an Error. Per-item errors are not an error of the enclosing call.
type CancelStatus struct {
	OK    bool
	Error string
}

// CancelReq identifies an order to cancel by client order id.
type CancelReq struct {
	Coin  string
	Cloid string
}

// OidCancel identifies an order to cancel by venue order id.
type OidCancel struct {
	Coin string
	Oid  int64
}

// Meta is the perpetual universe. Asset ids are the index into Universe.
type Meta struct {
	Universe []Asset `json:"universe"`
}

// Asset is one tradable perpetual.
type Asset struct {
	Name         string `json:"name"`
	SzDecimals   int    `json:"szDecimals"`
	MaxLeverage  int    `json:"maxLeverage"`
	OnlyIsolated bool   `json:"onlyIsolated,omitempty"`
	IsDelisted   bool   `json:"isDelisted,omitempty"`
}

// Position is one open perpetual position. Szi is signed (negative = short) and
// EntryPx is empty when the venue reports null.
type Position struct {
	Coin    string
	Szi     string
	EntryPx string
}

// OpenOrder is one open order as the venue's frontend reports it. LimitPx, Sz
// and OrigSz are decimal strings; Cloid is empty when the order has none.
type OpenOrder struct {
	Coin      string
	Side      string // "A" (ask/sell) or "B" (bid/buy)
	LimitPx   string
	Sz        string
	OrigSz    string
	Oid       int64
	Cloid     string
	Timestamp int64 // venue ms — order creation time
}

// minScheduleCancelLead is the venue's minimum lead time for ScheduleCancel.
const minScheduleCancelLead = 5 * time.Second
