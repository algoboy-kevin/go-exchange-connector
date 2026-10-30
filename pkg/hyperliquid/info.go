package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

const (
	// DefaultInfoURL is the mainnet REST endpoint shared by /info requests.
	DefaultInfoURL = "https://api.hyperliquid.xyz/info"

	// DefaultWSURL is the mainnet WebSocket endpoint.
	DefaultWSURL = "wss://api.hyperliquid.xyz/ws"

	// infoMaxBody caps how much of a response body is read, so a misbehaving
	// proxy cannot make the client allocate without bound. The largest real
	// response (meta, allMids, a candle history) is a few hundred KB.
	infoMaxBody = 16 << 20 // 16 MiB
)

// infoUserAgent identifies this client to the venue.
var infoUserAgent = "go-exchange-connector/" + connector.Version

// HTTPError is a non-2xx response from the /info endpoint, or a 2xx response
// whose body is not JSON. It carries the status, URL and (truncated) body so a
// caller can tell rate limiting from a rejected request without string
// matching.
type HTTPError struct {
	StatusCode int
	URL        string
	Body       string

	// RetryAfter is the venue's Retry-After hint (seconds or HTTP-date form),
	// zero when the header was absent or unparsable. Use IsRateLimited to
	// classify; the client's own retry loop (WithRetry) already honours it.
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("hyperliquid: %s returned %d: %s", e.URL, e.StatusCode, e.Body)
	}
	return fmt.Sprintf("hyperliquid: %s returned %d", e.URL, e.StatusCode)
}

// InfoClient performs Hyperliquid /info requests: the venue's metadata,
// snapshot and account endpoints. It is the one-shot counterpart of the
// WebSocket stream manager.
//
// /info is one POST endpoint; the request body's "type" selects the query:
//
//	{"type":"meta"}                                   → PerpMeta
//	{"type":"perpDexs"}                               → []*PerpDex
//	{"type":"metaAndAssetCtxs"}                       → MetaAndAssetCtxs
//	{"type":"allMids"}                                → map[string]Num
//	{"type":"l2Book","coin":"BTC"}                    → L2BookSnapshot
//	{"type":"candleSnapshot","req":{...}}             → []Candle
//
// Post is the escape hatch for anything not wrapped here, and is also how a
// raw recorder should fetch meta: it returns the response body **verbatim**,
// so the capture keeps exactly what the venue sent.
//
// The endpoint is weight-rate-limited (a shared per-IP budget; the venue
// documents 1200 weight per minute). A client built with no options does not
// throttle and does not retry: callers that issue a handful of requests at
// startup stay far below the limit. A caller that issues bulk requests
// (per-coin funding history, a validation sweep over many dexes) must build
// the client with WithRateLimit and WithRetry, which keep the pacing and the
// 429 handling in this one place instead of in every consumer.
type InfoClient struct {
	url       string
	userAgent string
	http      *http.Client

	// Throttling / retry (nil / zero = off).
	limiter    *tokenBucket
	maxRetries int
	backoffMin time.Duration
	backoffMax time.Duration

	// Injectable for tests; production values are set by NewInfoClient.
	now    func() time.Time
	sleep  func(ctx context.Context, d time.Duration) error
	jitter func(d time.Duration) time.Duration // returns a duration in [d/2, d]
}

// InfoOption configures an InfoClient.
type InfoOption func(*InfoClient)

// WithHTTPClient replaces the default http.Client (20 s timeout). The
// client's Timeout applies per attempt.
func WithHTTPClient(h *http.Client) InfoOption {
	return func(c *InfoClient) {
		if h != nil {
			c.http = h
		}
	}
}

// WithRateLimit paces requests with a token bucket: at most rps requests per
// second on average, with bursts up to burst. Every attempt (including
// retries) takes a token, so retries cannot push the client over its budget.
// rps <= 0 disables pacing; burst < 1 is treated as 1. Requests wait for a
// token in call order and honour ctx while waiting.
//
// The venue budget is weight based, not request based. Pick rps from the
// heaviest query you issue: with a 1200/min budget and weight-20 queries,
// 1 request/s is the sustainable ceiling.
func WithRateLimit(rps float64, burst int) InfoOption {
	return func(c *InfoClient) {
		if rps <= 0 {
			c.limiter = nil
			return
		}
		if burst < 1 {
			burst = 1
		}
		c.limiter = &tokenBucket{rate: rps, burst: float64(burst), tokens: float64(burst)}
	}
}

// WithRetry retries a failed request up to maxRetries more times with
// jittered exponential backoff (base, doubling, capped at max). Retried
// failures: HTTP 429, 500, 502, 503, 504 and transport errors while the
// caller's context is still live. A Retry-After hint on the response raises
// the wait to at least that long; a hint longer than 2 minutes ends the
// retrying and returns the error. Requests are read-only queries, so
// repeating them is safe. Zero base/max select 500 ms / 10 s.
func WithRetry(maxRetries int, base, max time.Duration) InfoOption {
	return func(c *InfoClient) {
		if maxRetries < 0 {
			maxRetries = 0
		}
		if base <= 0 {
			base = 500 * time.Millisecond
		}
		if max <= 0 {
			max = 10 * time.Second
		}
		if max < base {
			max = base
		}
		c.maxRetries, c.backoffMin, c.backoffMax = maxRetries, base, max
	}
}

// retryAfterCap bounds how long a Retry-After hint is honoured.
const retryAfterCap = 2 * time.Minute

// IsRateLimited reports whether err is a venue rate-limit rejection (HTTP
// 429) from the /info endpoint, however deeply it is wrapped.
func IsRateLimited(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.StatusCode == http.StatusTooManyRequests
}

// tokenBucket is a reservation-style token bucket: each wait takes one token
// immediately (the balance may go negative) and sleeps for the time the
// balance needs to climb back to zero, which keeps concurrent callers in
// arrival order.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
}

func (b *tokenBucket) wait(ctx context.Context, now func() time.Time, sleep func(context.Context, time.Duration) error) error {
	b.mu.Lock()
	t := now()
	if b.last.IsZero() {
		b.last = t
	}
	if el := t.Sub(b.last).Seconds(); el > 0 {
		b.tokens += el * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = t
	}
	b.tokens--
	var d time.Duration
	if b.tokens < 0 {
		d = time.Duration(-b.tokens / b.rate * float64(time.Second))
	}
	b.mu.Unlock()
	if d <= 0 {
		return nil
	}
	if err := sleep(ctx, d); err != nil {
		b.mu.Lock()
		b.tokens++ // give the reservation back
		b.mu.Unlock()
		return err
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// NewInfoClient creates an /info client. An empty baseURL selects
// DefaultInfoURL. With no options the client neither throttles nor retries.
func NewInfoClient(baseURL string, opts ...InfoOption) *InfoClient {
	if baseURL == "" {
		baseURL = DefaultInfoURL
	}
	c := &InfoClient{
		url:       baseURL,
		userAgent: infoUserAgent,
		http:      &http.Client{Timeout: 20 * time.Second},
		now:       time.Now,
		sleep:     sleepCtx,
		jitter: func(d time.Duration) time.Duration {
			if d <= 1 {
				return d
			}
			return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Post sends one /info request and returns the response body exactly as
// received. The body is checked with json.Valid — a proxy or captive portal
// answering with an HTML error page returns cheaply and loudly here rather
// than turning into a decode error much later — but it is not parsed.
//
// This is the raw path: a caller that will act on fields should use the typed
// wrappers below, and a caller that only archives venue output (e.g. a
// recorder dumping meta.json) should use this.
func (c *InfoClient) Post(ctx context.Context, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: marshal /info request: %w", err)
	}
	for attempt := 0; ; attempt++ {
		if c.limiter != nil {
			if err := c.limiter.wait(ctx, c.now, c.sleep); err != nil {
				return nil, err
			}
		}
		raw, err := c.postOnce(ctx, body)
		if err == nil {
			return raw, nil
		}
		if attempt >= c.maxRetries || ctx.Err() != nil || !retryable(err) {
			return nil, err
		}
		d, ok := c.backoff(attempt, err)
		if !ok {
			return nil, err
		}
		if serr := c.sleep(ctx, d); serr != nil {
			return nil, err // the caller gave up while we were waiting: report the venue error
		}
	}
}

// retryable reports whether a failed attempt is worth repeating.
func retryable(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		switch he.StatusCode {
		case http.StatusTooManyRequests, http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
		return false // 4xx rejections and the 2xx-but-not-JSON case
	}
	return true // transport or read error with a live context
}

// backoff returns the wait before retry number attempt+1; false means the
// venue asked for a wait longer than we are willing to honour.
func (c *InfoClient) backoff(attempt int, err error) (time.Duration, bool) {
	d := c.backoffMin << uint(min(attempt, 20))
	if d <= 0 || d > c.backoffMax {
		d = c.backoffMax
	}
	d = c.jitter(d)
	var he *HTTPError
	if errors.As(err, &he) && he.RetryAfter > 0 {
		if he.RetryAfter > retryAfterCap {
			return 0, false
		}
		if he.RetryAfter > d {
			d = he.RetryAfter
		}
	}
	return d, true
}

// parseRetryAfter reads a Retry-After header: delta-seconds or an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		if n < 0 {
			return 0
		}
		return time.Duration(n * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// postOnce performs exactly one HTTP attempt.
func (c *InfoClient) postOnce(ctx context.Context, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: build /info request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: /info request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, infoMaxBody))
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: read /info response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        c.url,
			Body:       truncate(strings.TrimSpace(string(raw)), 256),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
		}
	}
	if !json.Valid(raw) {
		return nil, &HTTPError{
			StatusCode: resp.StatusCode,
			URL:        c.url,
			Body:       "response body is not JSON: " + truncate(strings.TrimSpace(string(raw)), 256),
		}
	}
	return raw, nil
}

// get performs an /info request and decodes the response into out.
func (c *InfoClient) get(ctx context.Context, payload any, out any) error {
	raw, err := c.Post(ctx, payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("hyperliquid: decode /info response: %w", err)
	}
	return nil
}

// Meta fetches the perpetual universe (names, size decimals, max leverage) for
// the default perp dex. Asset ids are the index into Universe.
func (c *InfoClient) Meta(ctx context.Context) (*PerpMeta, error) {
	var meta PerpMeta
	if err := c.get(ctx, map[string]any{"type": "meta"}, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// MetaRaw fetches the meta response body verbatim, for callers that archive
// venue output instead of interpreting it.
func (c *InfoClient) MetaRaw(ctx context.Context) ([]byte, error) {
	return c.Post(ctx, map[string]any{"type": "meta"})
}

// PerpDexs fetches the perp dex list. The first element is always the default
// dex and is null on the wire, hence the nil entry. Only needed for HIP-3
// (builder-deployed) markets: asset ids there are
// 100_000 + dexIndex*10_000 + indexInMeta.
func (c *InfoClient) PerpDexs(ctx context.Context) ([]*PerpDex, error) {
	var dexs []*PerpDex
	if err := c.get(ctx, map[string]any{"type": "perpDexs"}, &dexs); err != nil {
		return nil, err
	}
	return dexs, nil
}

// PerpDexsRaw fetches the perpDexs response body verbatim.
func (c *InfoClient) PerpDexsRaw(ctx context.Context) ([]byte, error) {
	return c.Post(ctx, map[string]any{"type": "perpDexs"})
}

// MetaAndAssetCtxs fetches the universe together with the current asset
// contexts — the convenient source of funding rates, open interest and
// oracle/mark prices for a set of coins.
func (c *InfoClient) MetaAndAssetCtxs(ctx context.Context) (*MetaAndAssetCtxs, error) {
	var out MetaAndAssetCtxs
	if err := c.get(ctx, map[string]any{"type": "metaAndAssetCtxs"}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AllMids fetches the current mid price of every coin.
func (c *InfoClient) AllMids(ctx context.Context) (map[string]Num, error) {
	var out AllMids
	if err := c.get(ctx, map[string]any{"type": "allMids"}, &out); err != nil {
		return nil, err
	}
	return out.Mids, nil
}

// L2Snapshot fetches a one-shot L2 book snapshot for coin. This is the REST
// twin of the l2Book channel; unlike the channel it is not paced, so a caller
// polling it must respect the endpoint's weight budget.
func (c *InfoClient) L2Snapshot(ctx context.Context, coin string) (*L2BookSnapshot, error) {
	coin, err := NormalizeCoin(coin)
	if err != nil {
		return nil, err
	}
	var book L2BookSnapshot
	if err := c.get(ctx, map[string]any{"type": "l2Book", "coin": coin}, &book); err != nil {
		return nil, err
	}
	return &book, nil
}

// CandleSnapshot fetches historical candles for coin/interval in
// [startTime, endTime] (ms since epoch). A non-positive endTime means "now",
// which is what the venue expects for a trailing window.
func (c *InfoClient) CandleSnapshot(ctx context.Context, coin, interval string, startTime, endTime int64) ([]Candle, error) {
	coin, err := NormalizeCoin(coin)
	if err != nil {
		return nil, err
	}
	if !ValidCandleInterval(interval) {
		return nil, fmt.Errorf("hyperliquid: unsupported candle interval %q (supported: %s)",
			interval, strings.Join(candleIntervals, ", "))
	}
	if endTime <= 0 {
		endTime = time.Now().UnixMilli()
	}
	req := map[string]any{
		"coin":      coin,
		"interval":  interval,
		"startTime": startTime,
		"endTime":   endTime,
	}
	var candles []Candle
	if err := c.get(ctx, map[string]any{"type": "candleSnapshot", "req": req}, &candles); err != nil {
		return nil, err
	}
	return candles, nil
}

// ─────────────────────────────────────────────────────────────
// HIP-3 dex metadata (C1)
// ─────────────────────────────────────────────────────────────

// validateDex checks a perp dex name before it is put on the wire. The empty
// string is the default dex and is valid; quotes, backslashes, whitespace and
// control characters are rejected so a name can never break out of the JSON
// body.
func validateDex(dex string) error {
	for _, r := range dex {
		switch {
		case r == '"' || r == '\\':
			return fmt.Errorf("hyperliquid: dex %q contains %q", dex, string(r))
		case unicode.IsSpace(r), unicode.IsControl(r):
			return fmt.Errorf("hyperliquid: dex %q contains whitespace or a control character", dex)
		}
	}
	return nil
}

func dexPayload(typ, dex string) map[string]any {
	p := map[string]any{"type": typ}
	if dex != "" {
		p["dex"] = dex
	}
	return p
}

// MetaDex fetches the perpetual universe of one HIP-3 (builder-deployed) perp
// dex: {"type":"meta","dex":"<name>"}. Coin names in the response carry the
// dex prefix ("xyz:TSLA"). An empty dex is the default dex, identical to
// Meta. Dex names come from PerpDexs.
func (c *InfoClient) MetaDex(ctx context.Context, dex string) (*PerpMeta, error) {
	if err := validateDex(dex); err != nil {
		return nil, err
	}
	var meta PerpMeta
	if err := c.get(ctx, dexPayload("meta", dex), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// MetaDexRaw is MetaDex returning the response body verbatim.
func (c *InfoClient) MetaDexRaw(ctx context.Context, dex string) ([]byte, error) {
	if err := validateDex(dex); err != nil {
		return nil, err
	}
	return c.Post(ctx, dexPayload("meta", dex))
}

// MetaAndAssetCtxsDex is MetaAndAssetCtxs for one HIP-3 dex: the universe
// with the current asset contexts (funding, open interest, oracle/mark).
func (c *InfoClient) MetaAndAssetCtxsDex(ctx context.Context, dex string) (*MetaAndAssetCtxs, error) {
	if err := validateDex(dex); err != nil {
		return nil, err
	}
	var out MetaAndAssetCtxs
	if err := c.get(ctx, dexPayload("metaAndAssetCtxs", dex), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ─────────────────────────────────────────────────────────────
// Funding (C3)
// ─────────────────────────────────────────────────────────────

// FundingHistoryPageSize is the most entries the venue returns for one
// fundingHistory request. A full page means there may be more.
const FundingHistoryPageSize = 500

// maxFundingPages bounds FundingHistoryAll so a misbehaving server cannot
// make it loop forever.
const maxFundingPages = 200

// FundingHistory fetches the settled funding rates of coin in
// [startMs, endMs] (ms since epoch): {"type":"fundingHistory","coin",
// "startTime","endTime"}. A non-positive endMs omits endTime (the venue then
// means "now"). The venue returns at most FundingHistoryPageSize entries,
// oldest first; use FundingHistoryAll to page through a longer range.
func (c *InfoClient) FundingHistory(ctx context.Context, coin string, startMs, endMs int64) ([]FundingPoint, error) {
	coin, err := NormalizeCoin(coin)
	if err != nil {
		return nil, err
	}
	if startMs < 0 {
		return nil, fmt.Errorf("hyperliquid: fundingHistory startTime %d is negative", startMs)
	}
	if endMs > 0 && endMs < startMs {
		return nil, fmt.Errorf("hyperliquid: fundingHistory endTime %d is before startTime %d", endMs, startMs)
	}
	req := map[string]any{"type": "fundingHistory", "coin": coin, "startTime": startMs}
	if endMs > 0 {
		req["endTime"] = endMs
	}
	var pts []FundingPoint
	if err := c.get(ctx, req, &pts); err != nil {
		return nil, err
	}
	return pts, nil
}

// FundingHistoryAll pages FundingHistory forward until the range is covered:
// each next page starts one millisecond after the last entry received.
func (c *InfoClient) FundingHistoryAll(ctx context.Context, coin string, startMs, endMs int64) ([]FundingPoint, error) {
	var all []FundingPoint
	next := startMs
	for page := 0; page < maxFundingPages; page++ {
		pts, err := c.FundingHistory(ctx, coin, next, endMs)
		if err != nil {
			return all, err
		}
		all = append(all, pts...)
		if len(pts) < FundingHistoryPageSize {
			return all, nil
		}
		last := pts[len(pts)-1].Time
		if last < next {
			return all, nil // no forward progress: stop rather than loop
		}
		next = last + 1
	}
	return all, fmt.Errorf("hyperliquid: fundingHistory for %s exceeded %d pages", coin, maxFundingPages)
}

// PredictedFundings fetches {"type":"predictedFundings"}: for every coin the
// predicted next funding rate on Hyperliquid and on the other venues the
// venue tracks, each with its next funding time and (when reported) funding
// interval. The interval differs per venue and per symbol (observed: 1 h on
// HlPerp, 4 h or 8 h on BinPerp/BybitPerp), so consumers must read it from
// here or from configuration, never assume it.
func (c *InfoClient) PredictedFundings(ctx context.Context) ([]PredictedFunding, error) {
	var out []PredictedFunding
	if err := c.get(ctx, map[string]any{"type": "predictedFundings"}, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
