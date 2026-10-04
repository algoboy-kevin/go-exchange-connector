package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

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
// The endpoint is weight-rate-limited (a shared per-IP budget). This client
// does not throttle: callers issue a handful of requests at startup, which is
// far below the limit.
type InfoClient struct {
	url       string
	userAgent string
	http      *http.Client
}

// NewInfoClient creates an /info client. An empty baseURL selects
// DefaultInfoURL.
func NewInfoClient(baseURL string) *InfoClient {
	if baseURL == "" {
		baseURL = DefaultInfoURL
	}
	return &InfoClient{
		url:       baseURL,
		userAgent: infoUserAgent,
		http:      &http.Client{Timeout: 20 * time.Second},
	}
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
