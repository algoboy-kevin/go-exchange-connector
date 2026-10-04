package hyperliquid

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// This file holds the Hyperliquid wire types: the payloads the venue puts in
// the `data` field of a WebSocket frame, and the shapes returned by the REST
// /info endpoint.
//
// Everything here is deliberately tolerant. A frame that fails to decode is a
// frame the caller can never see again, so the parsing rules are:
//
//   - numbers are decoded by Num, which accepts both a JSON decimal string
//     ("97500.5") and a JSON number (97500.5) — the venue documents some price
//     fields as numbers while the wire (and every published SDK) sends strings;
//   - optional/absent sides (e.g. one half of a bbo update) are pointers so a
//     null is distinguishable from a zero level;
//   - unknown fields are ignored, so a new venue field needs no code change.

// ─────────────────────────────────────────────────────────────
// Num
// ─────────────────────────────────────────────────────────────

// Num is a decimal value that Hyperliquid may encode either as a JSON string
// ("97500.5") or as a JSON number (97500.5).
//
// The exact decimal text is preserved — no float64 round-trip, matching the
// rest of this repo (Binance payloads use decimal strings for the same
// reason). A JSON null decodes to the empty string, which IsZero reports.
type Num string

// String returns the exact decimal text as sent by the venue.
func (n Num) String() string { return string(n) }

// IsZero reports whether the value was absent or null.
func (n Num) IsZero() bool { return n == "" }

// Float64 parses the decimal text. An absent value (JSON null) parses as 0.
// Prefer the string form when precision matters (funding rates, sizes).
func (n Num) Float64() (float64, error) {
	if n == "" {
		return 0, nil
	}
	return strconv.ParseFloat(string(n), 64)
}

// UnmarshalJSON accepts a JSON string, number or null.
func (n *Num) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	switch {
	case s == "" || s == "null":
		*n = ""
		return nil
	case s[0] == '"':
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*n = Num(v)
		return nil
	default:
		// JSON number: keep the literal text (it is already decimal-exact).
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return fmt.Errorf("hyperliquid: invalid decimal %q", s)
		}
		*n = Num(s)
		return nil
	}
}

// MarshalJSON emits the value as a JSON string, so a re-encoded payload keeps
// the venue's decimal text.
func (n Num) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(n))
}

// ─────────────────────────────────────────────────────────────
// WebSocket data payloads
// ─────────────────────────────────────────────────────────────

// L2Level is one price level of an l2Book snapshot or a bbo update.
type L2Level struct {
	Price Num `json:"px"`
	Size  Num `json:"sz"`
	Count int `json:"n"` // number of resting orders at the level
}

// L2BookSnapshot is the payload of an l2Book frame, and of the REST
// {"type":"l2Book"} request. Levels[0] is the bid side, Levels[1] the ask
// side, both ordered best-first by the venue.
//
// A snapshot is pushed on every block that is at least 0.5s after the previous
// push — it is paced, not event-driven, and `fast` only changes the depth (5
// levels instead of 20), not the cadence.
type L2BookSnapshot struct {
	Coin   string      `json:"coin"`
	Time   int64       `json:"time"` // ms since epoch
	Levels [][]L2Level `json:"levels"`
}

// Bids returns the bid side (empty if the venue omitted it).
func (b L2BookSnapshot) Bids() []L2Level {
	if len(b.Levels) < 1 {
		return nil
	}
	return b.Levels[0]
}

// Asks returns the ask side (empty if the venue omitted it).
func (b L2BookSnapshot) Asks() []L2Level {
	if len(b.Levels) < 2 {
		return nil
	}
	return b.Levels[1]
}

// Trade is one trade print from the trades channel. Users is the venue's
// [buyer, seller] address pair.
type Trade struct {
	Coin    string   `json:"coin"`
	Side    string   `json:"side"` // "B" (buy/bid) or "A" (sell/ask)
	Price   Num      `json:"px"`
	Size    Num      `json:"sz"`
	Hash    string   `json:"hash"` // L1 transaction hash
	Time    int64    `json:"time"` // ms since epoch
	TradeID int64    `json:"tid"`  // 50-bit hash of (buyer oid, seller oid)
	Users   []string `json:"users"`
}

// BBO is a best-bid/offer update from the bbo channel. The venue sends it only
// when the bbo changes on a block, and either side may be null (a market with
// no resting bid, or none on the ask).
type BBO struct {
	Coin string     `json:"coin"`
	Time int64      `json:"time"` // ms since epoch
	BBO  []*L2Level `json:"bbo"`  // [bid, ask]
}

// Bid returns the best bid, or nil when the venue reported none.
func (b BBO) Bid() *L2Level {
	if len(b.BBO) < 1 {
		return nil
	}
	return b.BBO[0]
}

// Ask returns the best ask, or nil when the venue reported none.
func (b BBO) Ask() *L2Level {
	if len(b.BBO) < 2 {
		return nil
	}
	return b.BBO[1]
}

// AssetCtx is the perpetual asset context carried by activeAssetCtx. It is
// pushed once per block and carries no timestamp of its own — consumers stamp
// it with the local receive time.
type AssetCtx struct {
	Funding      Num   `json:"funding"`
	OpenInterest Num   `json:"openInterest"`
	Premium      Num   `json:"premium"`
	DayNtlVlm    Num   `json:"dayNtlVlm"`
	PrevDayPx    Num   `json:"prevDayPx"`
	OraclePx     Num   `json:"oraclePx"`
	MarkPx       Num   `json:"markPx"`
	MidPx        Num   `json:"midPx"`
	ImpactPxs    []Num `json:"impactPxs"`
	DayBaseVlm   Num   `json:"dayBaseVlm"`
	// CirculatingSupply is sent only by the spot variant of the channel
	// ("activeSpotAssetCtx"), which this package does not subscribe to.
	CirculatingSupply Num `json:"circulatingSupply"`
}

// ActiveAssetCtx is the payload of an activeAssetCtx frame.
type ActiveAssetCtx struct {
	Coin string   `json:"coin"`
	Ctx  AssetCtx `json:"ctx"`
}

// AllMids is the payload of an allMids frame: one mid price per coin. The
// first frame after subscribing is a full snapshot; later frames still contain
// every coin the venue has a mid for.
type AllMids struct {
	Mids map[string]Num `json:"mids"`
}

// Candle is an OHLCV candle, used both by the candle channel and by the REST
// candleSnapshot request. OpenTime/CloseTime are ms since epoch; the venue
// sends a fresh candle update on each trade inside the in-progress candle
// (there is no "is final" flag — a new OpenTime marks the next candle).
type Candle struct {
	OpenTime   int64  `json:"t"`
	CloseTime  int64  `json:"T"`
	Coin       string `json:"s"`
	Interval   string `json:"i"`
	Open       Num    `json:"o"`
	Close      Num    `json:"c"`
	High       Num    `json:"h"`
	Low        Num    `json:"l"`
	Volume     Num    `json:"v"`
	TradeCount int64  `json:"n"`
}

// ─────────────────────────────────────────────────────────────
// REST /info types
// ─────────────────────────────────────────────────────────────

// PerpMeta is the response of POST /info {"type":"meta"}: the perpetual
// universe (one AssetInfo per coin, in asset-id order) plus margin tables.
//
// MarginTables is kept as raw JSON: the venue encodes it as a list of
// [id, {...}] tuples, which cannot be modelled as an array of objects.
type PerpMeta struct {
	Universe        []AssetInfo       `json:"universe"`
	MarginTables    []json.RawMessage `json:"marginTables"`
	CollateralToken int               `json:"collateralToken"`
}

// AssetInfo is one perpetual in PerpMeta.Universe. The index of the entry in
// Universe is the venue's asset id for the default perp dex.
type AssetInfo struct {
	Name          string `json:"name"`
	SzDecimals    int    `json:"szDecimals"`
	MaxLeverage   int    `json:"maxLeverage"`
	MarginTableID int    `json:"marginTableId"`
	OnlyIsolated  bool   `json:"onlyIsolated"`
	IsDelisted    bool   `json:"isDelisted"`
}

// PerpDex is one entry of POST /info {"type":"perpDexs"}. The response is a
// list whose first element is null (the default perp dex), so it decodes into
// []*PerpDex and a nil entry means "the first dex".
type PerpDex struct {
	Name          string  `json:"name"`
	FullName      string  `json:"fullName"`
	Deployer      string  `json:"deployer"`
	OracleUpdater *string `json:"oracleUpdater"`
	FeeRecipient  *string `json:"feeRecipient"`
}

// MetaAndAssetCtxs is the response of {"type":"metaAndAssetCtxs"} — a
// [meta, ctxs] tuple, not an object. Ctxs is index-aligned with
// Meta.Universe, which is what CoinContext relies on.
type MetaAndAssetCtxs struct {
	Meta PerpMeta
	Ctxs []AssetCtx
}

// UnmarshalJSON decodes the venue's two-element array.
func (m *MetaAndAssetCtxs) UnmarshalJSON(b []byte) error {
	var tuple []json.RawMessage
	if err := json.Unmarshal(b, &tuple); err != nil {
		return fmt.Errorf("hyperliquid: metaAndAssetCtxs: %w", err)
	}
	if len(tuple) < 2 {
		return fmt.Errorf("hyperliquid: metaAndAssetCtxs: expected [meta, ctxs], got %d element(s)", len(tuple))
	}
	if err := json.Unmarshal(tuple[0], &m.Meta); err != nil {
		return fmt.Errorf("hyperliquid: metaAndAssetCtxs meta: %w", err)
	}
	if err := json.Unmarshal(tuple[1], &m.Ctxs); err != nil {
		return fmt.Errorf("hyperliquid: metaAndAssetCtxs ctxs: %w", err)
	}
	return nil
}

// CoinContext returns the asset context of coin, using the index alignment
// between Meta.Universe and Ctxs. The second return is false when the coin is
// not in the universe or the tuple is short.
func (m *MetaAndAssetCtxs) CoinContext(coin string) (*AssetCtx, bool) {
	if m == nil {
		return nil, false
	}
	for i, info := range m.Meta.Universe {
		if info.Name == coin {
			if i >= len(m.Ctxs) {
				return nil, false
			}
			return &m.Ctxs[i], true
		}
	}
	return nil, false
}

// FundingRate returns the current hourly funding rate for coin, as the exact
// decimal string the venue sent.
func (m *MetaAndAssetCtxs) FundingRate(coin string) (Num, bool) {
	ctx, ok := m.CoinContext(coin)
	if !ok {
		return "", false
	}
	return ctx.Funding, true
}

// AssetID returns the venue's asset id (the index of coin in the universe) for
// the default perp dex.
func (m *PerpMeta) AssetID(coin string) (int, bool) {
	if m == nil {
		return 0, false
	}
	for i, info := range m.Universe {
		if info.Name == coin {
			return i, true
		}
	}
	return 0, false
}
