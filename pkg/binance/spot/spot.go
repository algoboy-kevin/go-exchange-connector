// Package spot is a Binance SPOT signed REST client plus a user-data stream.
//
// Source of every rule below: the Binance docs as recorded by exchange-docs-hub
// (catalog entry `binance`, https://developers.binance.com/en/docs/llms-full.txt,
// content_sha256 7fe8634d231eacc5564bef1995fe45c1bbc52ea7b7072cc50e3b0d06ad05c68e).
// "line N" below is a line of that file. The mainnet spot REST page in that text
// holds only the general information (line 21453); the per-endpoint pages
// (POST /api/v3/order, DELETE /api/v3/openOrders, ...) are present only in the
// spot TESTNET REST page (line 117730), which documents the same API on
// testnet.binance.vision. Endpoint parameters and response shapes here are cited
// to that page.
//
// # Signing (mainnet lines 21665-21720; testnet 117896-118090)
//
// Every SIGNED endpoint (TRADE, USER_DATA) gets all parameters in the query
// string (GET, POST and DELETE alike; POST/DELETE may use the query string or an
// urlencoded body, line 21541). The client appends recvWindow, then timestamp
// (local ms plus the offset learned by SyncTime), then signature = hex(
// HMAC_SHA256(secret, queryString)) LAST. "The signature payload of your request
// is the query string concatenated without separator to the HTTP body" (line
// 117951) and "any non-ASCII character must be percent-encoded before signing",
// so the string that is signed is byte-for-byte the string that is sent; params
// are kept in insertion order. X-MBX-APIKEY carries the key. recvWindow defaults
// to 5000 ms and may not exceed 60000 ms (line 21692); the client rejects a
// larger value in New. HMAC keys work for REST: the documented example is HMAC
// (line 117953) and the signature test vectors in the tests are the ones printed
// there.
//
// # Limits and errors
//
//   - Request weight is per IP, in X-MBX-USED-WEIGHT-<n><S|M|H|D> response headers
//     (line 21595); the unfilled order count is per account, in
//     X-MBX-ORDER-COUNT-<n><S|M|H|D> headers on successful order responses
//     (line 21611; rejected orders are not guaranteed to carry them). Client.Usage
//     returns every such header seen, keyed by the interval suffix ("1M", "10S",
//     "1D" on mainnet, per the rateLimits example at line 19917).
//   - 429: back off (IsRateLimited). 418: automated IP ban, 2 minutes to 3 days,
//     Retry-After says when it ends (IsBanned). 403: WAF block (IsWAFBlocked).
//   - 5xx, -1006, -1007 and transport errors/timeouts: the outcome is UNKNOWN
//     (IsUncertain). Reconcile by client order id before any resend.
//   - There are NO retries anywhere in this package.
//
// # Orders
//
// Spot has no batch place and no batch cancel in the endpoint list, so there is
// none here. The order surface is: NewOrder (POST /api/v3/order), TestOrder
// (POST /api/v3/order/test, line 120090: validates and checks signature/
// recvWindow, "does not send it into the matching engine"), CancelOrder
// (DELETE /api/v3/order by orderId or origClientOrderId, line 120212), CancelAll
// (DELETE /api/v3/openOrders, returns the cancelled list, line 120296), GetOrder,
// OpenOrders (weight 6 per symbol, 80 without a symbol, line 121985), MyTrades
// and Account (balances).
//
//   - LIMIT needs timeInForce, quantity, price (line 119920 table); GTC, IOC and
//     FOK exist (enums, line 25047). LIMIT_MAKER needs quantity and price, no
//     timeInForce, and "will be rejected if the order immediately matches and
//     trades as a taker ... also known as a POST-ONLY order"; the rejection is
//     -2010 "Order would immediately match and take." (line 24848).
//   - newClientOrderId is "a unique id among open orders"; a reused id is
//     "Duplicate order sent." (line 24831). The REST page states no length rule;
//     the FIX section states client order ids must match ^[a-zA-Z0-9-_]{1,36}$
//     (line 22600), and this client enforces that for newClientOrderId and
//     origClientOrderId (AMBIGUOUS: not stated for REST, see the report).
//   - newOrderRespType defaults here to RESULT (the venue default for LIMIT is
//     FULL, line 119907). ACK has no status; RESULT has status, executedQty and
//     cummulativeQuoteQty. Decimals stay strings.
//   - Cancel by orderId or origClientOrderId; if both are sent "the orderId is
//     searched first" (line 120237). "The performance for canceling an order ...
//     is always better when only orderId is sent" (line 120270).
//
// # Filters (lines 23897-24100)
//
// ExchangeInfo returns the symbol filters. PRICE_FILTER has tickSize,
// LOT_SIZE has stepSize, and the notional minimum is minNotional in both
// MIN_NOTIONAL and NOTIONAL (NOT "notional" as on USD-M futures); NOTIONAL also
// has maxNotional. Symbol.Rules extracts them. RoundDown/RoundUp round to a step
// exactly.
//
// # User-data stream (user_stream.go)
//
// See UserStream: the REST listenKey mechanism is deprecated for spot (line
// 26370) and its documentation removed (line 25918); the current mechanism is
// the WebSocket API, and with an HMAC key only userDataStream.subscribe.signature
// works. A WebSocket API connection is valid for 24 hours (line 19800).
package spot

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Network selects the Binance spot environment used when Config.BaseURL is empty.
type Network string

const (
	// Mainnet is production spot (https://api.binance.com, line 21466).
	Mainnet Network = "mainnet"
	// Testnet is the spot test network (https://testnet.binance.vision, line 117740).
	Testnet Network = "testnet"
)

const (
	mainnetBaseURL = "https://api.binance.com"
	testnetBaseURL = "https://testnet.binance.vision"

	defaultRecvWindow  = 5 * time.Second
	maxRecvWindow      = 60 * time.Second // line 21692
	defaultHTTPTimeout = 10 * time.Second
	maxBodyBytes       = 32 << 20
)

// Config configures a Client. Network picks a default host; BaseURL overrides
// it (tests point it at an httptest server). RecvWindow defaults to 5s and may
// not exceed 60s; HTTP defaults to a client with a 10s timeout (the venue's
// own request timeout is 10 s, line 21493).
type Config struct {
	Network    Network
	BaseURL    string
	APIKey     string
	APISecret  string
	RecvWindow time.Duration
	HTTP       *http.Client
}

// Client is a Binance spot REST client. It is safe for concurrent use. It
// performs no retries: the caller decides how to react to each error (see
// IsUncertain / IsRateLimited / IsBanned / IsUnknownOrder).
type Client struct {
	baseURL    string
	apiKey     string
	apiSecret  string
	recvWindow time.Duration
	http       *http.Client

	// timeOffset is server-local in milliseconds, applied to every signed
	// timestamp. Updated by SyncTime.
	timeOffset atomic.Int64

	usageMu    sync.Mutex
	usedWeight map[string]int64
	orderCount map[string]int64
}

// New builds a Client. Unknown networks are rejected; a missing BaseURL falls
// back to the network default (empty Network means Mainnet). APIKey/APISecret
// may be empty: the public endpoints (time, exchangeInfo, bookTicker) need none,
// and a signed call without them fails locally with a ConfigError.
func New(cfg Config) (*Client, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		switch cfg.Network {
		case "", Mainnet:
			base = mainnetBaseURL
		case Testnet:
			base = testnetBaseURL
		default:
			return nil, &ConfigError{Reason: "unknown network " + string(cfg.Network)}
		}
	}
	recv := cfg.RecvWindow
	if recv <= 0 {
		recv = defaultRecvWindow
	}
	if recv > maxRecvWindow {
		return nil, &ConfigError{Reason: "recvWindow " + recv.String() + " exceeds the 60s maximum"}
	}
	if recv < time.Millisecond {
		return nil, &ConfigError{Reason: "recvWindow " + recv.String() + " is below 1ms"}
	}
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: defaultHTTPTimeout}
	}
	return &Client{
		baseURL:    base,
		apiKey:     cfg.APIKey,
		apiSecret:  cfg.APISecret,
		recvWindow: recv,
		http:       hc,
		usedWeight: map[string]int64{},
		orderCount: map[string]int64{},
	}, nil
}

// BaseURL returns the resolved REST base URL (useful for logging/tests).
func (c *Client) BaseURL() string { return c.baseURL }

// hasCredentials reports whether signed calls are possible.
func (c *Client) hasCredentials() bool { return c.apiKey != "" && c.apiSecret != "" }

// now is the signed-timestamp clock: local ms plus the SyncTime offset.
func (c *Client) now() int64 { return time.Now().UnixMilli() + c.timeOffset.Load() }

// ─────────────────────────────────────────────────────────────
// Signed-query construction
// ─────────────────────────────────────────────────────────────

// params is an insertion-ordered key/value set. The order values are added is
// the order they are encoded, signed and sent; url.Values would sort keys.
type params struct {
	keys []string
	vals map[string]string
}

func newParams() *params { return &params{vals: make(map[string]string)} }

// add appends or replaces a parameter, preserving first-insertion order.
func (p *params) add(key, value string) {
	if _, ok := p.vals[key]; !ok {
		p.keys = append(p.keys, key)
	}
	p.vals[key] = value
}

// encode renders the params as application/x-www-form-urlencoded in insertion
// order. QueryEscape percent-encodes non-ASCII as uppercase UTF-8 octets, the
// form the signing example uses (line 118025).
func (p *params) encode() string {
	var b strings.Builder
	for i, k := range p.keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(k))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(p.vals[k]))
	}
	return b.String()
}

// signHex returns hex(HMAC_SHA256(secret, payload)).
func signHex(secret, payload string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(payload))
	return hex.EncodeToString(m.Sum(nil))
}

// do executes an HTTP request against path. When signed, it appends recvWindow,
// timestamp and signature (in that order) to the encoded params.
func (c *Client) do(ctx context.Context, method, path string, p *params, signed bool) ([]byte, error) {
	if signed && !c.hasCredentials() {
		return nil, &ConfigError{Reason: "signed request needs APIKey and APISecret"}
	}
	q := ""
	if p != nil {
		q = p.encode()
	}
	if signed {
		if q != "" {
			q += "&"
		}
		q += "recvWindow=" + strconv.FormatInt(c.recvWindow.Milliseconds(), 10)
		q += "&timestamp=" + strconv.FormatInt(c.now(), 10)
		q += "&signature=" + signHex(c.apiSecret, q)
	}
	u := c.baseURL + path
	if q != "" {
		u += "?" + q
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, &transportError{err: err}
	}
	if c.apiKey != "" {
		req.Header.Set("X-MBX-APIKEY", c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	c.recordUsage(resp.Header)
	if rerr != nil {
		return nil, &transportError{err: rerr}
	}
	if resp.StatusCode >= 400 {
		return nil, parseAPIError(resp.StatusCode, resp.Header, body)
	}
	return body, nil
}

// parseAPIError turns an error response into an *APIError. Binance error
// bodies are {"code":N,"msg":"..."}; anything else keeps the status and the
// raw body as the message.
func parseAPIError(status int, h http.Header, body []byte) *APIError {
	ae := &APIError{HTTPStatus: status}
	if ra := strings.TrimSpace(h.Get("Retry-After")); ra != "" {
		if secs, err := strconv.ParseInt(ra, 10, 64); err == nil && secs > 0 {
			ae.RetryAfter = time.Duration(secs) * time.Second
		}
	}
	var e struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(body, &e); err == nil && (e.Code != 0 || e.Msg != "") {
		ae.Code, ae.Msg = e.Code, e.Msg
		return ae
	}
	ae.Msg = strings.TrimSpace(string(body))
	return ae
}

// ─────────────────────────────────────────────────────────────
// Usage headers
// ─────────────────────────────────────────────────────────────

const (
	usedWeightPrefix = "X-MBX-USED-WEIGHT-"
	orderCountPrefix = "X-MBX-ORDER-COUNT-"
)

// recordUsage stores every X-MBX-USED-WEIGHT-* and X-MBX-ORDER-COUNT-* header
// of a response, keyed by the interval suffix upper-cased ("1M", "10S", "1D").
func (c *Client) recordUsage(h http.Header) {
	c.usageMu.Lock()
	defer c.usageMu.Unlock()
	for name, vals := range h {
		if len(vals) == 0 {
			continue
		}
		up := strings.ToUpper(name)
		var dst map[string]int64
		var suffix string
		switch {
		case strings.HasPrefix(up, usedWeightPrefix):
			dst, suffix = c.usedWeight, strings.TrimPrefix(up, usedWeightPrefix)
		case strings.HasPrefix(up, orderCountPrefix):
			dst, suffix = c.orderCount, strings.TrimPrefix(up, orderCountPrefix)
		default:
			continue
		}
		if v, err := strconv.ParseInt(strings.TrimSpace(vals[0]), 10, 64); err == nil && suffix != "" {
			dst[suffix] = v
		}
	}
}

// Usage is the most recently observed rate-limit usage. A key keeps its
// previous value when a later response lacks that header.
type Usage struct {
	// UsedWeight maps interval suffix ("1M") to the per-IP request weight used.
	UsedWeight map[string]int64
	// OrderCount maps interval suffix ("10S", "1D") to the per-account
	// unfilled order count.
	OrderCount map[string]int64
}

// Usage returns a copy of the last observed rate-limit usage counters.
func (c *Client) Usage() Usage {
	c.usageMu.Lock()
	defer c.usageMu.Unlock()
	u := Usage{UsedWeight: make(map[string]int64, len(c.usedWeight)), OrderCount: make(map[string]int64, len(c.orderCount))}
	for k, v := range c.usedWeight {
		u.UsedWeight[k] = v
	}
	for k, v := range c.orderCount {
		u.OrderCount[k] = v
	}
	return u
}

// ─────────────────────────────────────────────────────────────
// Time
// ─────────────────────────────────────────────────────────────

// SyncTime calls GET /api/v3/time (weight 1, line 118464) and stores the
// server-local clock offset used for the timestamp on every later signed
// request. The offset is computed from the midpoint of the round trip.
func (c *Client) SyncTime(ctx context.Context) error {
	before := time.Now().UnixMilli()
	body, err := c.do(ctx, http.MethodGet, "/api/v3/time", nil, false)
	if err != nil {
		return err
	}
	after := time.Now().UnixMilli()
	var out struct {
		ServerTime int64 `json:"serverTime"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return &transportError{err: err}
	}
	if out.ServerTime == 0 {
		return &transportError{err: errNoServerTime}
	}
	c.timeOffset.Store(out.ServerTime - (before+after)/2)
	return nil
}
