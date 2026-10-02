package connector

import (
	"encoding/json"
	"time"
)

// ─────────────────────────────────────────────────────────────
// Gamma series discovery
// ─────────────────────────────────────────────────────────────
//
// These types mirror Polymarket's Gamma REST API shape for recurring market
// series (e.g. "BTC Up or Down Daily", "BTC multi-strikes weekly"). They are
// deliberately separate from Market (the CLOB-shaped type returned by
// GetMarket): GammaMarket below is the wire-format market nested inside an
// event, which may carry a groupItemTitle (the ladder rung / strike) and a
// negRiskMarketID that Market does not model.
//
// Series discovery flow:
//
//	series, _ := conn.GetSeries(ctx, "btc-up-or-down-daily")   // → series[0].ID
//	events, _ := conn.ListSeriesEvents(ctx, EventQuery{
//	    SeriesID: series[0].ID, Closed: &openOnly,
//	})                                                          // → open + future events
//	for _, ev := range events { /* ev.Markets = the event's markets */ }

// GammaSeries is a recurring Gamma market series.
type GammaSeries struct {
	ID         string       `json:"id"`
	Ticker     string       `json:"ticker"`
	Slug       string       `json:"slug"`
	Title      string       `json:"title"`
	SeriesType string       `json:"seriesType"` // "single"
	Recurrence string       `json:"recurrence"` // see Recurrence* constants
	Active     bool         `json:"active"`
	Closed     bool         `json:"closed"`
	Archived   bool         `json:"archived"`
	Events     []GammaEvent `json:"events,omitempty"`
}

// Recurrence values reported by Gamma for GammaSeries.Recurrence.
const (
	RecurrenceFiveMinute    = "5m"
	RecurrenceFifteenMinute = "15m"
	RecurrenceHourly        = "hourly"
	RecurrenceFourHour      = "4h"
	RecurrenceDaily         = "daily"
	RecurrenceWeekly        = "weekly"
	RecurrenceMonthly       = "monthly"
)

// GammaEvent is one dated event; it may contain several strike/range markets
// (a "ladder" event such as BTC above $74,000 … $94,000 holds 11 markets).
type GammaEvent struct {
	ID          string        `json:"id"`
	Ticker      string        `json:"ticker"` // == the market/event slug used by GetMarket
	Slug        string        `json:"slug"`
	Title       string        `json:"title"`
	Description string        `json:"description"`
	SeriesSlug  string        `json:"seriesSlug"`
	StartDate   time.Time     `json:"startDate"` // trading window open (NOT the settle instant)
	EndDate     time.Time     `json:"endDate"`   // trading window close (== settle instant for our families)
	Closed      bool          `json:"closed"`
	Active      bool          `json:"active"`
	NegRisk     bool          `json:"negRisk"`
	Tags        []GammaTag    `json:"tags,omitempty"`
	Markets     []GammaMarket `json:"markets"`
}

// GammaTag is one Gamma tag attached to an event (e.g. "bitcoin", "weekly").
type GammaTag struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
}

// GammaMarket is one market inside an event (one rung of a ladder).
// Wire format only — map into connector.Market where convenient.
type GammaMarket struct {
	ID              string `json:"id"`
	Slug            string `json:"slug"`
	Question        string `json:"question"`
	ConditionID     string `json:"conditionId"`
	GroupItemTitle  string `json:"groupItemTitle"` // the strike, e.g. "80,000"
	Outcomes        string `json:"outcomes"`       // JSON-encoded array, e.g. ["Yes","No"]
	ClobTokenIDs    string `json:"clobTokenIds"`   // JSON-encoded array, index-aligned with Outcomes
	Description     string `json:"description"`    // contains the machine-readable resolution rule
	StartDate       string `json:"startDate"`
	EndDate         string `json:"endDate"`
	Closed          bool   `json:"closed"`
	Active          bool   `json:"active"`
	NegRisk         bool   `json:"negRisk"`
	NegRiskMarketID string `json:"negRiskMarketID"`
}

// OutcomeList decodes the JSON-string encoded outcomes array
// (e.g. `["Yes", "No"]`). An empty field yields a nil slice and no error.
func (m GammaMarket) OutcomeList() ([]string, error) {
	return decodeJSONStringArray(m.Outcomes)
}

// TokenIDList decodes the JSON-string encoded CLOB token ID array. It is
// index-aligned with OutcomeList: for a Yes/No market, index 0 is YES and
// index 1 is NO.
func (m GammaMarket) TokenIDList() ([]string, error) {
	return decodeJSONStringArray(m.ClobTokenIDs)
}

// decodeJSONStringArray decodes a Gamma "JSON-encoded array in a string" field.
// Empty input is treated as "absent" (nil, nil) rather than an error.
func decodeJSONStringArray(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// EventQuery filters ListSeriesEvents.
type EventQuery struct {
	SeriesID string // required
	// Closed filters on event state. nil = server default (all events);
	// use a pointer to false for "open + future" only.
	Closed    *bool
	Limit     int    // default 100, max 500
	Offset    int    // starting offset (for manual pagination)
	Order     string // e.g. "endDate"; empty = server default
	Ascending bool   // order direction, only sent when Order is set
}
