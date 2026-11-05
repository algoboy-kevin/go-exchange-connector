// Package contract is the venue-neutral trading contract for perpetual venues.
//
// Every venue exposes the same request and result shapes here. Venue packages
// implement the contract through thin adapters (see pkg/contract/hl and
// pkg/contract/bnfutures). Native clients keep their own payloads and remain
// reachable for venue-only features.
//
// Conventions shared by all adapters:
//   - Prices, quantities and balances are decimal strings. Never floats.
//   - Symbol is the venue-native name: Hyperliquid "BTC", Binance/Aster "BTCUSDT".
//   - Post-only is a time-in-force value, because venues encode it that way.
//   - A per-item rejection is a result with Status Rejected and no Go error.
//     A Go error means the whole call failed.
//   - When the outcome of a mutation is unknown, the result has Status Unknown and
//     the error wraps ErrUncertain. Reconcile by ClientID or OrderID before resending.
package contract

import "time"

// Side of an order.
type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

// TIF is the time-in-force of a limit order.
type TIF string

const (
	GTC      TIF = "GTC"       // rests until filled or cancelled
	IOC      TIF = "IOC"       // unfilled remainder is cancelled at once
	PostOnly TIF = "POST_ONLY" // rejected instead of crossing the book
)

// OrderRequest is one limit order. All venues in the contract accept limit
// orders. Market orders are venue-specific until a second venue needs them.
type OrderRequest struct {
	Symbol     string // venue-native symbol (see package doc)
	Side       Side
	Price      string // decimal string on the venue tick
	Qty        string // decimal string in base units
	TIF        TIF
	ReduceOnly bool
	ClientID   string // caller's id; empty = none. Each adapter checks its own format
}

// Status of an order result.
type Status string

const (
	Resting  Status = "RESTING"  // accepted and on the book (may be partly filled)
	Filled   Status = "FILLED"   // fully filled at placement
	Rejected Status = "REJECTED" // venue refused this order; Reason says why
	Unknown  Status = "UNKNOWN"  // outcome not known; reconcile before any resend
)

// OrderResult is the outcome of one order placement.
type OrderResult struct {
	OrderID   string // venue order id
	ClientID  string // echo of the request ClientID
	Status    Status
	FilledQty string
	AvgPrice  string
	Reason    string    // venue message when Rejected or Unknown
	SentAt    time.Time // local clock, before the request was written
	AckAt     time.Time // local clock, when the response was decoded
}

// CancelRequest identifies one order. OrderID is preferred when both are set.
type CancelRequest struct {
	Symbol   string
	OrderID  string
	ClientID string
}

// CancelResult is the outcome of one cancel.
type CancelResult struct {
	OrderID  string
	ClientID string
	Canceled bool
	Reason   string
}

// Order is one open order.
type Order struct {
	Symbol     string
	OrderID    string
	ClientID   string
	Side       Side
	Price      string
	Qty        string // original size
	FilledQty  string
	ReduceOnly bool
	Timestamp  time.Time // venue creation or update time
}

// Position is one open position. Qty is signed: positive long, negative short.
type Position struct {
	Symbol     string
	Qty        string
	EntryPrice string // empty if the venue reports none
}

// Account is a snapshot of the account's collateral.
type Account struct {
	Currency  string
	Equity    string
	Available string // free for new margin / withdrawable
	At        time.Time
}

// MarginMode is cross or isolated margin.
type MarginMode string

const (
	Cross    MarginMode = "CROSS"
	Isolated MarginMode = "ISOLATED"
)

// Channel is a market-data stream kind.
type Channel string

const (
	Book   Channel = "book"
	Trades Channel = "trades"
	BBO    Channel = "bbo"
)

// Subscription is one market-data stream for one symbol.
type Subscription struct {
	Channel Channel
	Symbol  string
}
