// Package polymarket — CLOB REST API client for Polymarket.
//
// The ClobClient handles L2-authenticated requests to the Polymarket CLOB API
// (https://clob.polymarket.com). It provides methods for posting and cancelling
// orders, as well as HMAC-SHA256 request signing using API credentials.
//
// L2 authentication headers (every request):
//
//	POLY_API_KEY     — API key UUID
//	POLY_ADDRESS     — Signer address
//	POLY_SIGNATURE   — HMAC-SHA256(timestamp + method + path + body, secret)
//	POLY_TIMESTAMP   — Current UNIX timestamp (seconds)
//	POLY_PASSPHRASE  — API passphrase
package polymarket

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const defaultClobURL = "https://clob.polymarket.com"

// ─────────────────────────────────────────────────────────────
// CLOB API types (mirrors the clob-openapi.yaml spec)
// ─────────────────────────────────────────────────────────────

// Order is the EIP-712 signed order payload submitted to the CLOB API.
// In CLOB V2, `expiration` remains in the POST wire body for GTD handling
// but is NOT part of the EIP-712 signed struct.
//
// Note: Salt is a string in Go (for EIP-712 big.Int parsing) but must
// serialize as a JSON number (type: integer per the OpenAPI spec).
// The custom MarshalJSON handles this conversion.
type Order struct {
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Side          string `json:"side"`
	Expiration    string `json:"expiration"`
	Timestamp     string `json:"timestamp"`
	Metadata      string `json:"metadata"`
	Builder       string `json:"builder"`
	Signature     string `json:"signature"`
	Salt          string `json:"-"` // custom marshaled below
	SignatureType int    `json:"signatureType"`
}

// MarshalJSON implements json.Marshaler for Order.
// salt must serialize as a JSON number (integer), not a JSON string.
func (o Order) MarshalJSON() ([]byte, error) {
	// Alias avoids infinite recursion.
	type orderAlias Order
	parsed, _ := strconv.ParseInt(o.Salt, 10, 64)

	return json.Marshal(struct {
		orderAlias
		Salt int64 `json:"salt"`
	}{
		orderAlias: orderAlias(o),
		Salt:       parsed,
	})
}

// SendOrder wraps an Order with API-level metadata for submission.
type SendOrder struct {
	Order     Order  `json:"order"`
	Owner     string `json:"owner"`
	OrderType string `json:"orderType,omitempty"` // GTC | FOK | GTD | FAK
	DeferExec bool   `json:"deferExec,omitempty"`
	PostOnly  bool   `json:"postOnly,omitempty"`
}

// SendOrderResponse describes the outcome of a single order submission.
type SendOrderResponse struct {
	Success            bool     `json:"success"`
	OrderID            string   `json:"orderID"`
	Status             string   `json:"status"`                       // live | matched | delayed
	MakingAmount       string   `json:"makingAmount,omitempty"`       // empty on failure
	TakingAmount       string   `json:"takingAmount,omitempty"`       // empty on failure
	TransactionsHashes []string `json:"transactionsHashes,omitempty"` // present when matched
	TradeIDs           []string `json:"tradeIDs,omitempty"`           // present when matched
	ErrorMsg           string   `json:"errorMsg"`                     // empty on success
}

// CancelOrdersResponse describes the outcome of a cancel request.
type CancelOrdersResponse struct {
	Canceled    []string          `json:"canceled"`
	NotCanceled map[string]string `json:"not_canceled"` // orderID → reason
}

// CancelOrderPayload is the body for DELETE /order (single cancel).
type CancelOrderPayload struct {
	OrderID string `json:"orderID"`
}

// ClobAPIError represents a structured error from the CLOB API.
type ClobAPIError struct {
	StatusCode     int    `json:"-"`
	Message        string `json:"error"`
	Code           string `json:"code,omitempty"`
	RetryAfterSecs int    `json:"retry_after_seconds,omitempty"`
}

func (e *ClobAPIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("clob: %s (code=%s, status=%d)", e.Message, e.Code, e.StatusCode)
	}
	return fmt.Sprintf("clob: %s (status=%d)", e.Message, e.StatusCode)
}

// ─────────────────────────────────────────────────────────────
// ClobClient
// ─────────────────────────────────────────────────────────────

// ClobClient is an authenticated HTTP client for the Polymarket CLOB REST API.
//
// It handles L2 authentication (HMAC-SHA256 headers) for every request.
// Order body signing (EIP-712) is a separate step — see PROD.md Phase 3.
type ClobClient struct {
	baseURL string
	http    *http.Client

	// L2 API credentials.
	apiKey     string
	secret     string
	passphrase string

	// Wallet addresses.
	makerAddress  string
	signerAddress string

	// API key owner UUID (returned when creating API key).
	ownerUUID string

	// Signature type: 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE, 3=POLY_1271.
	signatureType int
}

// ClobClientOption configures the ClobClient.
type ClobClientOption func(*ClobClient)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(hc *http.Client) ClobClientOption {
	return func(c *ClobClient) { c.http = hc }
}

// WithSignatureType sets the EIP-712 signature type.
func WithSignatureType(sigType int) ClobClientOption {
	return func(c *ClobClient) { c.signatureType = sigType }
}

// WithOwnerUUID sets the API key owner UUID.
func WithOwnerUUID(uuid string) ClobClientOption {
	return func(c *ClobClient) { c.ownerUUID = uuid }
}

// WithMakerAddress sets the maker (proxy) address.
func WithMakerAddress(addr string) ClobClientOption {
	return func(c *ClobClient) { c.makerAddress = addr }
}

// WithSignerAddress sets the signer address.
func WithSignerAddress(addr string) ClobClientOption {
	return func(c *ClobClient) { c.signerAddress = addr }
}

// NewClobClient creates a new CLOB API client.
//
//	baseURL   — CLOB API base URL (e.g. "https://clob.polymarket.com")
//	apiKey    — L2 API key UUID
//	secret    — L2 API secret (for HMAC signing)
//	passphrase — L2 API passphrase
//
// Use options to set wallet addresses, owner UUID, and signature type.
func NewClobClient(baseURL, apiKey, secret, passphrase string, opts ...ClobClientOption) *ClobClient {
	if baseURL == "" {
		baseURL = defaultClobURL
	}

	c := &ClobClient{
		baseURL:       strings.TrimRight(baseURL, "/"),
		http:          &http.Client{},
		apiKey:        apiKey,
		secret:        secret,
		passphrase:    passphrase,
		signatureType: 1, // default: POLY_PROXY
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// ─────────────────────────────────────────────────────────────
// Public API methods
// ─────────────────────────────────────────────────────────────

// PostOrders submits multiple orders to the CLOB API.
// Maximum 15 orders per request (enforced by the API).
func (c *ClobClient) PostOrders(orders []SendOrder) ([]SendOrderResponse, error) {
	if len(orders) == 0 {
		return nil, fmt.Errorf("clob: no orders to post")
	}
	if len(orders) > 15 {
		return nil, fmt.Errorf("clob: max 15 orders per batch, got %d", len(orders))
	}

	body, err := json.Marshal(orders)
	if err != nil {
		return nil, fmt.Errorf("clob: marshal orders: %w", err)
	}

	resp, err := c.doRequest(http.MethodPost, "/orders", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results []SendOrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("clob: decode response: %w", err)
	}

	return results, nil
}

// PostOrder submits a single order.
func (c *ClobClient) PostOrder(order SendOrder) (*SendOrderResponse, error) {
	body, err := json.Marshal(order)
	if err != nil {
		return nil, fmt.Errorf("clob: marshal order: %w", err)
	}

	resp, err := c.doRequest(http.MethodPost, "/order", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result SendOrderResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("clob: decode response: %w", err)
	}

	return &result, nil
}

// CancelOrders cancels multiple orders by their order hashes.
// Maximum 1000 order IDs per request. Duplicates are ignored by the API.
func (c *ClobClient) CancelOrders(orderIDs []string) (*CancelOrdersResponse, error) {
	if len(orderIDs) == 0 {
		return &CancelOrdersResponse{}, nil
	}
	if len(orderIDs) > 1000 {
		return nil, fmt.Errorf("clob: max 1000 order IDs per batch, got %d", len(orderIDs))
	}

	body, err := json.Marshal(orderIDs)
	if err != nil {
		return nil, fmt.Errorf("clob: marshal order IDs: %w", err)
	}

	resp, err := c.doRequest(http.MethodDelete, "/orders", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result CancelOrdersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("clob: decode response: %w", err)
	}

	return &result, nil
}

// CancelOrder cancels a single order by its order hash.
func (c *ClobClient) CancelOrder(orderID string) (*CancelOrdersResponse, error) {
	payload := CancelOrderPayload{OrderID: orderID}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("clob: marshal cancel payload: %w", err)
	}

	resp, err := c.doRequest(http.MethodDelete, "/order", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result CancelOrdersResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("clob: decode response: %w", err)
	}

	return &result, nil
}

// ─────────────────────────────────────────────────────────────
// HMAC-SHA256 L2 signing
// ─────────────────────────────────────────────────────────────

// l2Headers computes the 5 L2 authentication headers for a CLOB API request.
//
// The POLY_SIGNATURE is HMAC-SHA256 of the concatenated string:
//
//	timestamp + method + requestPath + body
//
// where:
//   - timestamp = current UNIX epoch seconds as a string
//   - method = HTTP method (e.g. "POST", "DELETE")
//   - requestPath = URL path (e.g. "/orders", "/order")
//   - body = request body string (empty if no body)
//
// The HMAC key is the API secret. The output is base64-encoded.
func (c *ClobClient) l2Headers(method, path string, body []byte) map[string]string {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	bodyStr := string(body)

	// HMAC-SHA256(timestamp + method + path + body, secret)
	// Note: The official SDK decodes the base64 secret first, then uses URLEncoding for the output.
	decodedSecret, err := base64.StdEncoding.DecodeString(c.secret)
	if err != nil {
		// Fallback: try URL-safe decoding
		decodedSecret, err = base64.URLEncoding.DecodeString(c.secret)
		if err != nil {
			decodedSecret = []byte(c.secret)
		}
	}
	mac := hmac.New(sha256.New, decodedSecret)
	mac.Write([]byte(ts))
	mac.Write([]byte(method))
	mac.Write([]byte(path))
	mac.Write([]byte(bodyStr))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))

	// POLY_ADDRESS must be the signer (EOA) address per the official SDK's BuildL2Headers.
	addr := c.signerAddress
	if addr == "" {
		addr = c.makerAddress
	}

	return map[string]string{
		"POLY_API_KEY":    c.apiKey,
		"POLY_ADDRESS":    addr,
		"POLY_SIGNATURE":  sig,
		"POLY_TIMESTAMP":  ts,
		"POLY_PASSPHRASE": c.passphrase,
	}
}

// ─────────────────────────────────────────────────────────────
// Internal HTTP helpers
// ─────────────────────────────────────────────────────────────

// doRequest sends an L2-authenticated request and checks the response status.
func (c *ClobClient) doRequest(method, path string, body []byte) (*http.Response, error) {
	url := c.baseURL + path
	req, err := http.NewRequest(method, url, io.NopCloser(strings.NewReader(string(body))))
	if err != nil {
		return nil, fmt.Errorf("clob: create request: %w", err)
	}

	// Set L2 auth headers.
	headers := c.l2Headers(method, path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")

	slog.Debug("clob: request",
		"method", method,
		"path", path,
		"body", string(body),
		"body_len", len(body),
	)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clob: request failed: %w", err)
	}

	// Check for HTTP-level errors.
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		var apiErr ClobAPIError
		respBody, _ := io.ReadAll(resp.Body)
		slog.Warn("clob: error response",
			"status", resp.StatusCode,
			"body", string(respBody),
		)
		if err := json.Unmarshal(respBody, &apiErr); err != nil {
			apiErr.Message = strings.TrimSpace(string(respBody))
		}
		apiErr.StatusCode = resp.StatusCode
		return nil, &apiErr
	}

	return resp, nil
}
