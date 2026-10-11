// Package futures implements a Binance USDⓈ-M (FUTURES) signed REST client
// for execution (Task 11A of S11).
//
// Decimals are carried as strings exactly as Binance sends and expects them —
// never float64 — so a caller's integer price/quantity formatting
// round-trips through the API without loss. Integer ids and timestamps are
// int64.
//
// Signing: every USER_DATA / TRADE method puts all parameters in the query
// string (GET, POST and DELETE alike), appends recvWindow then timestamp
// (local ms + the server offset learned by SyncTime), and appends
// signature = hex(HMAC_SHA256(APISecret, queryString)) last. The query is
// built in insertion order, so the string that is signed is byte-for-byte the
// string that is sent. X-MBX-APIKEY carries the key. The listenKey endpoints
// are signed with the API key header only (no timestamp, no signature).
package futures

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
	"sync/atomic"
	"time"
)

// Network selects the Binance environment used when Config.BaseURL is empty.
type Network string

const (
	// Mainnet is production USDⓈ-M futures (https://fapi.binance.com).
	Mainnet Network = "mainnet"
	// Testnet is the Binance futures testnet (https://demo-fapi.binance.com).
	Testnet Network = "testnet"
)

const (
	mainnetBaseURL = "https://fapi.binance.com"
	testnetBaseURL = "https://demo-fapi.binance.com"

	defaultRecvWindow  = 5 * time.Second
	defaultHTTPTimeout = 10 * time.Second
)

// Config configures a Client. Network picks a default host; BaseURL overrides
// it (tests point it at an httptest server). RecvWindow defaults to 5s and
// HTTP defaults to a client with a 10s timeout.
type Config struct {
	Network    Network
	BaseURL    string
	APIKey     string
	APISecret  string
	RecvWindow time.Duration
	HTTP       *http.Client
}

// Client is a Binance USDⓈ-M REST client. It is safe for concurrent use.
// It performs no retries: the caller decides how to react to each error
// (see IsUncertain / IsRateLimited / IsUnknownOrder).
type Client struct {
	baseURL    string
	apiKey     string
	apiSecret  string
	recvWindow time.Duration
	http       *http.Client

	// timeOffset is server−local in milliseconds, applied to every signed
	// timestamp. Updated by SyncTime.
	timeOffset atomic.Int64

	// Rate-limit headers captured from the most recent response.
	usedWeight    atomic.Int64
	orderCount10s atomic.Int64
	orderCount1m  atomic.Int64
}

// New builds a Client. Unknown networks are rejected; a missing BaseURL falls
// back to the network default. APIKey / APISecret may be empty (public
// endpoints such as time and exchangeInfo do not need them).
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
	}, nil
}

// BaseURL returns the resolved REST base URL (useful for logging/tests).
func (c *Client) BaseURL() string { return c.baseURL }

// ─────────────────────────────────────────────────────────────
// Signed-query construction
// ─────────────────────────────────────────────────────────────

// params is an insertion-ordered key/value set. The order values are added is
// the order they are encoded, signed and sent — url.Values would sort keys
// alphabetically and is deliberately not used.
type params struct {
	keys []string
	vals map[string]string
}

func newParams() *params {
	return &params{vals: make(map[string]string)}
}

// add appends or replaces a parameter, preserving first-insertion order.
func (p *params) add(key, value string) {
	if _, ok := p.vals[key]; !ok {
		p.keys = append(p.keys, key)
	}
	p.vals[key] = value
}

// encode renders the params as an application/x-www-form-urlencoded string in
// insertion order.
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

// signQuery returns hex(HMAC_SHA256(secret, payload)). The payload is the
// exact query string that will be sent.
func signQuery(secret, payload string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(payload))
	return hex.EncodeToString(m.Sum(nil))
}

// do executes an HTTP request against path. When signed, it appends
// recvWindow, timestamp and signature (in that order) to the encoded params.
// The API key header is always sent (harmless on public endpoints, required on
// the listenKey endpoints that are otherwise unsigned).
func (c *Client) do(ctx context.Context, method, path string, p *params, signed bool) ([]byte, error) {
	q := ""
	if p != nil {
		q = p.encode()
	}
	if signed {
		if q != "" {
			q += "&"
		}
		q += "recvWindow=" + strconv.FormatInt(c.recvWindow.Milliseconds(), 10)
		ts := time.Now().UnixMilli() + c.timeOffset.Load()
		q += "&timestamp=" + strconv.FormatInt(ts, 10)
		q += "&signature=" + signQuery(c.apiSecret, q)
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

	body, rerr := io.ReadAll(resp.Body)
	c.recordUsage(resp.Header)
	if rerr != nil {
		return nil, &transportError{err: rerr}
	}
	if resp.StatusCode >= 400 {
		return nil, parseAPIError(resp.StatusCode, body)
	}
	return body, nil
}

// parseAPIError turns an error response body into an *APIError. Binance error
// bodies are {"code":N,"msg":"..."}; anything else (e.g. an HTML 502) keeps
// the HTTP status and the raw body as the message.
func parseAPIError(status int, body []byte) *APIError {
	var e struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(body, &e); err == nil && (e.Code != 0 || e.Msg != "") {
		return &APIError{HTTPStatus: status, Code: e.Code, Msg: e.Msg}
	}
	return &APIError{HTTPStatus: status, Msg: strings.TrimSpace(string(body))}
}

// recordUsage stores the rate-limit headers carried by every response.
func (c *Client) recordUsage(h http.Header) {
	setIfPresent(&c.usedWeight, h.Get("X-MBX-USED-WEIGHT-1M"))
	setIfPresent(&c.orderCount10s, h.Get("X-MBX-ORDER-COUNT-10S"))
	setIfPresent(&c.orderCount1m, h.Get("X-MBX-ORDER-COUNT-1M"))
}

func setIfPresent(dst *atomic.Int64, raw string) {
	if raw == "" {
		return
	}
	if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		dst.Store(v)
	}
}

// Usage reports the most recently observed rate-limit headers. A field stays
// at its previous value when the header was absent from the last response.
type Usage struct {
	UsedWeight1M  int64
	OrderCount10s int64
	OrderCount1M  int64
}

// Usage returns the last observed rate-limit usage counters.
func (c *Client) Usage() Usage {
	return Usage{
		UsedWeight1M:  c.usedWeight.Load(),
		OrderCount10s: c.orderCount10s.Load(),
		OrderCount1M:  c.orderCount1m.Load(),
	}
}

// ─────────────────────────────────────────────────────────────
// Time
// ─────────────────────────────────────────────────────────────

// SyncTime calls GET /fapi/v1/time and stores the server−local clock offset
// (atomic) used for the timestamp on every subsequent signed request. The
// offset is computed from the midpoint of the round trip to reduce skew.
func (c *Client) SyncTime(ctx context.Context) error {
	before := time.Now().UnixMilli()
	body, err := c.do(ctx, http.MethodGet, "/fapi/v1/time", nil, false)
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
	local := (before + after) / 2
	c.timeOffset.Store(out.ServerTime - local)
	return nil
}
