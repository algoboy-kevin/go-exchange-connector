// Package exchange is the Hyperliquid *execution* wrapper the market-making
// driver uses: agent-wallet signing, order placement and cancellation, the
// schedule-cancel safety net, and the account queries the driver reconciles
// against.
//
// It wraps github.com/sonirico/go-hyperliquid (v0.46.1) rather than speaking the
// protocol itself, and follows this repository's conventions:
//
//   - Every decimal (price, size, entry price, limit price) is a STRING exactly
//     as the venue sends and expects it. Conversion to the float64 that
//     sonirico's wire encoder requires happens at the boundary and is exact for
//     Hyperliquid perps (≤ 6 price decimals, ≤ 8 size decimals).
//   - Everything is a business error unless IsUncertain or IsRateLimited says
//     otherwise: an order action that "failed" may still have executed.
//   - Queries read the master account's state; only signing uses the agent key.
//   - Mutations are never retried: the driver decides what a rate limit or an
//     uncertain outcome means for its own order state. Read-only /info queries
//     have bounded context-aware HTTP 429 backoff (see postInfo).
//
// Scope is core perps plus explicitly registered HIP-3 DEXes; no spot or vaults.
package exchange

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	// mainnetAPIURL/testnetAPIURL mirror the venue's REST bases. Sonirico picks
	// the mainnet signing domain by comparing the exchange's base URL to
	// mainnetAPIURL *exactly*, so these must not be re-derived or normalised
	// beyond the trailing-slash trim below.
	mainnetAPIURL = hl.MainnetAPIURL
	testnetAPIURL = hl.TestnetAPIURL

	// defaultHTTPTimeout is applied to the /exchange client when Config.HTTP is
	// nil: sonirico's own default http.Client has NO timeout, so a hung
	// connection would block an order indefinitely.
	defaultHTTPTimeout = 10 * time.Second
)

// Config configures a Client.
//
// Network selects the REST base, BaseURL overrides it (tests point it at an
// httptest server — note that only the exact mainnet URL signs mainnet, so an
// overridden BaseURL signs as testnet), AgentKey is the API/agent wallet
// private key approved by the master account, and Account is the MASTER address
// every query is made for.
type Config struct {
	Network  string // "mainnet" (default) | "testnet"
	BaseURL  string // overrides Network entirely
	AgentKey string // hex private key, with or without a 0x prefix
	Account  string // master account address, 0x… (40 hex)
	HTTP     *http.Client
	PerpDexs []string // explicitly registered optional HIP-3 scopes

	// InfoRetry overrides the bounded HTTP 429 retry budget of read-only /info
	// queries. nil selects the default (3 retries). A pointer to 0 disables
	// retries, so a 429 is returned at once (use this on the quoting path). A
	// negative value is treated as 0.
	InfoRetry *int
}

// Client is safe for concurrent use. Routing snapshots are immutable after
// publication; all SDK mutations share one account-wide action/nonce gate.
type Client struct {
	ex           *hl.Exchange
	info         *boundedInfo
	base         string
	account      string
	meta         Meta
	key          *ecdsa.PrivateKey
	http         *http.Client
	routesMu     sync.RWMutex
	routes       map[string]*hl.Exchange
	metas        map[string]*hl.Meta // per-scope SDK meta, same lifetime as routes
	write        *hl.Exchange        // merged core+HIP-3 Exchange: the only order/cancel poster
	registerGate chan struct{}
	actionGate   chan struct{}
	signer       *nonceSigner // last is protected by actionGate, never routesMu
	infoRetry    *int         // Config.InfoRetry; nil means the default budget
}

// New builds a Client.
//
// It fetches the perp meta itself (POST /info {"type":"meta"}) before touching
// sonirico: hl.NewExchange → NewInfo *panics* when meta/spotMeta are nil and the
// fetch fails, so the constructor must never leave that fetch to the library. An
// empty &hl.SpotMeta{} is passed for the same reason (spot is out of scope and
// its own fetch would be equally fatal).
func New(ctx context.Context, cfg Config) (*Client, error) {
	base, err := resolveBaseURL(cfg)
	if err != nil {
		return nil, err
	}
	key, err := parseAgentKey(cfg.AgentKey)
	if err != nil {
		return nil, err
	}
	account, err := normalizeAddress(cfg.Account)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Timeout: defaultHTTPTimeout}
	if cfg.HTTP != nil {
		*hc = *cfg.HTTP
		if hc.Timeout <= 0 {
			hc.Timeout = defaultHTTPTimeout
		}
	}
	info := &boundedInfo{url: base + "/info", http: hc}
	raw, err := info.Post(ctx, map[string]any{"type": "meta"})
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: fetch perp meta: %w", err)
	}

	hlMeta, meta, err := parseMeta(raw)
	if err != nil {
		return nil, err
	}

	c := &Client{info: info, base: base, account: account, meta: meta, key: key, http: withCaptureTransport(hc), routes: make(map[string]*hl.Exchange), metas: make(map[string]*hl.Meta), registerGate: make(chan struct{}, 1), actionGate: make(chan struct{}, 1), signer: &nonceSigner{delegate: hl.ECDSAL1Signer(key)}, infoRetry: cfg.InfoRetry}
	if err := validateMeta(meta, ""); err != nil {
		return nil, err
	}
	c.ex, err = c.newExchange(ctx, "", hlMeta, nil)
	if err != nil {
		return nil, err
	}
	c.routes[""] = c.ex
	c.metas[""] = hlMeta
	c.write, err = c.newWriteExchange(ctx, map[string]*hl.Meta{"": hlMeta}, nil)
	if err != nil {
		return nil, err
	}
	for _, dex := range cfg.PerpDexs {
		if err := c.RegisterDex(ctx, dex); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// resolveBaseURL picks the REST base: BaseURL wins, else Network, else mainnet.
func resolveBaseURL(cfg Config) (string, error) {
	if b := strings.TrimSpace(cfg.BaseURL); b != "" {
		return strings.TrimRight(b, "/"), nil
	}
	switch strings.TrimSpace(strings.ToLower(cfg.Network)) {
	case "", "mainnet":
		return mainnetAPIURL, nil
	case "testnet":
		return testnetAPIURL, nil
	default:
		return "", &ConfigError{Reason: fmt.Sprintf("unknown network %q (want mainnet or testnet)", cfg.Network)}
	}
}

// parseAgentKey decodes the agent wallet's hex private key, with or without a
// 0x prefix.
func parseAgentKey(s string) (*ecdsa.PrivateKey, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, &ConfigError{Reason: "agent key is empty"}
	}
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	// Errors describe only the SHAPE of the key: hex.DecodeString's error
	// quotes the offending character, which is key material.
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, &ConfigError{Reason: fmt.Sprintf("agent key is not valid hex (%d hex characters after any 0x prefix; want 64)", len(s))}
	}
	key, err := crypto.ToECDSA(raw)
	if err != nil {
		return nil, &ConfigError{Reason: "agent key is not a valid secp256k1 private key"}
	}
	return key, nil
}

// normalizeAddress validates a 0x-prefixed 20-byte account address, returning it
// lowercase (the venue treats the user field case-insensitively).
func normalizeAddress(s string) (string, error) {
	a := strings.TrimSpace(s)
	if a == "" {
		return "", &ConfigError{Reason: "account address is empty"}
	}
	if !strings.HasPrefix(a, "0x") && !strings.HasPrefix(a, "0X") {
		a = "0x" + a
	}
	if len(a) != 42 {
		return "", &ConfigError{Reason: fmt.Sprintf("account address %q must be 0x + 40 hex characters", s)}
	}
	if _, err := hex.DecodeString(a[2:]); err != nil {
		return "", &ConfigError{Reason: fmt.Sprintf("account address %q is not valid hex", s)}
	}
	return strings.ToLower(a), nil
}

// hlMetaShadow is the meta payload as hl.NewInfo needs it.
//
// The raw body CANNOT be unmarshalled straight into hl.Meta: the venue sends
// "marginTables": [[id, {…}], …] while hl.Meta declares marginTables as
// []MarginTable, so hl.Meta's generated decoder fails with a parse error and the
// constructor would be left without its asset→id map. Only universe and
// collateralToken matter to sonirico (MarginTables is unused by NewInfo), so the
// shadow carries exactly those.
type hlMetaShadow struct {
	Universe        []hl.AssetInfo `json:"universe"`
	CollateralToken int            `json:"collateralToken"`
}

// parseMeta decodes the venue's meta body into both the shape sonirico needs and
// this package's public Meta.
func parseMeta(raw []byte) (*hl.Meta, Meta, error) {
	// Required fields use stdlib pointers: generated SDK decoders otherwise
	// silently turn missing/null decimals into zero (a valid but wrong precision).
	var required struct {
		Universe []struct {
			Name       *string `json:"name"`
			SzDecimals *int    `json:"szDecimals"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(raw, &required); err != nil {
		return nil, Meta{}, &ScopeError{Reason: "invalid metadata shape"}
	}
	for _, a := range required.Universe {
		if a.Name == nil || a.SzDecimals == nil {
			return nil, Meta{}, &ScopeError{Reason: "metadata missing name/szDecimals"}
		}
	}
	var shadow hlMetaShadow
	if err := json.Unmarshal(raw, &shadow); err != nil {
		return nil, Meta{}, fmt.Errorf("hyperliquid: decode perp meta: %w", err)
	}
	if len(shadow.Universe) == 0 {
		return nil, Meta{}, errors.New("hyperliquid: perp meta has an empty universe")
	}
	var meta Meta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, Meta{}, fmt.Errorf("hyperliquid: decode perp meta: %w", err)
	}
	return &hl.Meta{
		Universe:        shadow.Universe,
		CollateralToken: shadow.CollateralToken,
	}, meta, nil
}

// Account returns the master account address queries are made for.
func (c *Client) Account() string { return c.account }

// BaseURL returns the resolved REST base the client was built with.
func (c *Client) BaseURL() string { return c.base }
