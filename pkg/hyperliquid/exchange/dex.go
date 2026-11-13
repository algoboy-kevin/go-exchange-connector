package exchange

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"unicode"

	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
	hlinfo "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
)

// ScopeError is a local routing/metadata failure. No action has been submitted.
type ScopeError struct{ Dex, Reason string }

func (e *ScopeError) Error() string       { return fmt.Sprintf("hyperliquid: scope %q: %s", e.Dex, e.Reason) }
func scopeError(dex, reason string) error { return &ScopeError{Dex: dex, Reason: reason} }

// boundedInfo keeps connector response limits and error typing while honoring
// Config.HTTP for unsigned requests too. SDK info options receive the same client.
type boundedInfo struct {
	url  string
	http *http.Client
}

func (c *boundedInfo) Post(ctx context.Context, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "go-exchange-connector")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	const maxBody = 16 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBody {
		return nil, fmt.Errorf("hyperliquid: /info response exceeds %d bytes", maxBody)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !json.Valid(raw) {
		msg := string(raw)
		if len(msg) > 256 {
			msg = msg[:256]
		}
		return nil, &hlinfo.HTTPError{StatusCode: resp.StatusCode, URL: c.url, Body: msg}
	}
	return raw, nil
}

// nonceSigner observes the exact SDK nonce and delegates signing unchanged.
// actionGate covers generation, signing AND transport, including failed sends.
type nonceSigner struct {
	delegate hl.L1ActionSigner
	last     int64
}

func (s *nonceSigner) SignL1Action(ctx context.Context, action any, vault string, nonce int64, expires *int64, mainnet bool) (hl.SignatureResult, error) {
	s.last = nonce
	return s.delegate.SignL1Action(ctx, action, vault, nonce, expires, mainnet)
}
func (c *Client) lockAction(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.actionGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-c.actionGate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) unlockAction() { <-c.actionGate }
func (c *Client) seed(ex *hl.Exchange) {
	if c.signer.last != 0 {
		ex.SetLastNonce(c.signer.last)
	}
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == ':' {
			return false
		}
	}
	return true
}
func validateDex(dex string) error {
	if !validToken(dex) {
		return scopeError(dex, "malformed DEX name")
	}
	return nil
}
func coinDex(coin string) (string, error) {
	dex, short, qualified := strings.Cut(coin, ":")
	if !qualified {
		if !validToken(coin) {
			return "", scopeError("", "malformed coin")
		}
		return "", nil
	}
	if err := validateDex(dex); err != nil {
		return "", err
	}
	if !validToken(short) {
		return "", scopeError(dex, "malformed qualified coin")
	}
	return dex, nil
}
func validateMeta(meta Meta, dex string) error {
	if len(meta.Universe) == 0 {
		return scopeError(dex, "empty metadata universe")
	}
	seen := make(map[string]bool, len(meta.Universe))
	for _, a := range meta.Universe {
		d, err := coinDex(a.Name)
		if err != nil || d != dex {
			return scopeError(dex, "metadata coin is not qualified for scope: "+a.Name)
		}
		if a.SzDecimals < 0 || a.SzDecimals > 6 {
			return scopeError(dex, "invalid size decimals for "+a.Name)
		}
		if seen[a.Name] {
			return scopeError(dex, "duplicate metadata coin: "+a.Name)
		}
		seen[a.Name] = true
	}
	return nil
}
func (c *Client) newExchange(ctx context.Context, dex string, meta *hl.Meta, registry *hl.MixedArray) (ex *hl.Exchange, err error) {
	defer func() {
		if recover() != nil {
			ex = nil
			err = scopeError(dex, "SDK metadata initialization failed")
		}
	}()
	ex = hl.NewExchange(ctx, c.key, c.base, meta, "", c.account, &hl.SpotMeta{}, registry,
		hl.ExchangeOptPerpDex(dex), hl.ExchangeOptL1Signer(c.signer),
		hl.ExchangeOptClientOptions(hl.ClientOptHTTPClient(c.http)),
		hl.ExchangeOptInfoOptions(hl.InfoOptClientOptions(hl.ClientOptHTTPClient(c.http))))
	return ex, nil
}

// newWriteExchange builds the ONE Exchange that signs and posts every
// order/cancel action. Its Info spans core plus every registered DEX
// (hl.ExchangeOptPerpDexs, Python `perp_dexs` parity), so a mixed batch resolves
// to global asset ids inside a single signed /exchange request. It follows the
// same construction discipline as newExchange: pre-supplied metas and an empty
// SpotMeta (no SDK fetch), the shared signer wrapper and the shared HTTP
// client; SDK construction panics are recovered into a ScopeError.
func (c *Client) newWriteExchange(ctx context.Context, metas map[string]*hl.Meta, registry *hl.MixedArray) (ex *hl.Exchange, err error) {
	defer func() {
		if recover() != nil {
			ex = nil
			err = scopeError("", "SDK merged metadata initialization failed")
		}
	}()
	names := make([]string, 0, len(metas))
	for dex := range metas {
		names = append(names, dex)
	}
	sort.Strings(names) // "" (core) sorts first; order does not affect ids
	ex = hl.NewExchange(ctx, c.key, c.base, metas[""], "", c.account, &hl.SpotMeta{}, registry,
		hl.ExchangeOptPerpDexs(names...), hl.ExchangeOptPerpDexMetas(metas),
		hl.ExchangeOptL1Signer(c.signer),
		hl.ExchangeOptClientOptions(hl.ClientOptHTTPClient(c.http)),
		hl.ExchangeOptInfoOptions(hl.InfoOptClientOptions(hl.ClientOptHTTPClient(c.http))))
	return ex, nil
}

// verifyWriteIDs is the fail-closed guard between the read-side scoped
// Exchanges and the merged write Exchange: every metadata coin must map to the
// identical global asset id in both, or the new routing is not published.
func verifyWriteIDs(write *hl.Exchange, metas map[string]*hl.Meta, scoped map[string]*hl.Exchange) error {
	for dex, meta := range metas {
		ex := scoped[dex]
		if ex == nil {
			return scopeError(dex, "missing scoped exchange for merged routing")
		}
		for _, a := range meta.Universe {
			want, ok1 := ex.Info().CoinToAsset(a.Name)
			got, ok2 := write.Info().CoinToAsset(a.Name)
			if !ok1 || !ok2 || want != got {
				return scopeError(dex, "merged asset id disagrees with scoped id for "+a.Name)
			}
		}
	}
	return nil
}

// writeExchange returns the current merged write Exchange (never nil after New).
func (c *Client) writeExchange() *hl.Exchange {
	c.routesMu.RLock()
	defer c.routesMu.RUnlock()
	return c.write
}

// RegisterDex initializes an explicit HIP-3 routing scope atomically. Empty is
// core/no-op. Initialization never holds the account action or routing lock.
func (c *Client) RegisterDex(ctx context.Context, dex string) error {
	if dex == "" {
		return nil
	}
	if err := validateDex(dex); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.registerGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.registerGate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	c.routesMu.RLock()
	ex := c.routes[dex]
	c.routesMu.RUnlock()
	if ex != nil {
		return nil
	}
	raw, err := c.info.Post(ctx, map[string]any{"type": "perpDexs"})
	if err != nil {
		return err
	}
	var registry hl.MixedArray
	if err := json.Unmarshal(raw, &registry); err != nil {
		return scopeError(dex, "invalid perpDexs registry")
	}
	// Decode names separately but pass the untouched MixedArray to the SDK. Nulls
	// and holes MUST occupy their original array positions.
	found := -1
	names := make(map[string]bool)
	for i, mv := range registry {
		if bytes.Equal(bytes.TrimSpace(mv), []byte("null")) {
			continue
		}
		var entry struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(mv, &entry); err != nil || validateDex(entry.Name) != nil {
			return scopeError(dex, "malformed perpDexs entry")
		}
		if names[entry.Name] {
			return scopeError(dex, "duplicate perpDexs name")
		}
		names[entry.Name] = true
		if entry.Name == dex {
			found = i
		}
	}
	if found <= 0 {
		return scopeError(dex, "DEX not present at a non-core registry index")
	}
	raw, err = c.info.Post(ctx, map[string]any{"type": "meta", "dex": dex})
	if err != nil {
		return err
	}
	hm, meta, err := parseMeta(raw)
	if err != nil {
		return scopeError(dex, err.Error())
	}
	if err := validateMeta(meta, dex); err != nil {
		return err
	}
	ex, err = c.newExchange(ctx, dex, hm, &registry)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Rebuild the merged write lookup from an immutable copy of the published
	// state plus this DEX. registerGate serializes writers of these maps, so the
	// snapshot cannot go stale; readers only ever see the fully built result.
	c.routesMu.RLock()
	metas := make(map[string]*hl.Meta, len(c.metas)+1)
	scoped := make(map[string]*hl.Exchange, len(c.routes)+1)
	for d, m := range c.metas {
		metas[d] = m
	}
	for d, e := range c.routes {
		scoped[d] = e
	}
	c.routesMu.RUnlock()
	metas[dex], scoped[dex] = hm, ex
	write, err := c.newWriteExchange(ctx, metas, &registry)
	if err != nil {
		return err
	}
	if err := verifyWriteIDs(write, metas, scoped); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.routesMu.Lock()
	c.routes[dex] = ex
	c.metas[dex] = hm
	c.write = write
	c.routesMu.Unlock()
	return nil
}
func (c *Client) scopes() []string {
	c.routesMu.RLock()
	defer c.routesMu.RUnlock()
	out := make([]string, 0, len(c.routes))
	for dex := range c.routes {
		out = append(out, dex)
	}
	sort.Strings(out)
	return out
}
func (c *Client) scope(dex string) (*hl.Exchange, error) {
	if dex != "" {
		if err := validateDex(dex); err != nil {
			return nil, err
		}
	}
	c.routesMu.RLock()
	ex := c.routes[dex]
	c.routesMu.RUnlock()
	if ex == nil {
		return nil, scopeError(dex, "DEX is not registered")
	}
	return ex, nil
}
func (c *Client) route(coin string) (*hl.Exchange, error) {
	dex, err := coinDex(coin)
	if err != nil {
		return nil, err
	}
	ex, err := c.scope(dex)
	if err != nil {
		return nil, err
	}
	if _, ok := ex.Info().CoinToAsset(coin); !ok {
		return nil, scopeError(dex, "coin "+coin+" not found in info metadata")
	}
	return ex, nil
}
func validateCloid(cloid string, required bool) error {
	if cloid == "" && !required {
		return nil
	}
	s := strings.TrimPrefix(cloid, "0x")
	if len(s) != 32 {
		return fmt.Errorf("hyperliquid: cloid must be 32 hex characters")
	}
	_, err := hex.DecodeString(s)
	return err
}

// resolveWrite validates EVERY coin of a bulk action (well-formed, registered
// scope, present in that scope's metadata) and returns the single merged write
// Exchange that will sign and post the whole batch. Nothing is sent here, so an
// invalid item anywhere means zero wire actions. The scoped read-side Exchange
// and the merged one must agree on each coin's global asset id.
func (c *Client) resolveWrite(coins []string) (*hl.Exchange, error) {
	c.routesMu.RLock()
	defer c.routesMu.RUnlock()
	write := c.write
	if write == nil {
		return nil, scopeError("", "write routing is not initialized")
	}
	for _, coin := range coins {
		dex, err := coinDex(coin)
		if err != nil {
			return nil, err
		}
		if dex != "" {
			if err := validateDex(dex); err != nil {
				return nil, err
			}
		}
		scoped := c.routes[dex]
		if scoped == nil {
			return nil, scopeError(dex, "DEX is not registered")
		}
		want, ok := scoped.Info().CoinToAsset(coin)
		if !ok {
			return nil, scopeError(dex, "coin "+coin+" not found in info metadata")
		}
		if got, ok := write.Info().CoinToAsset(coin); !ok || got != want {
			return nil, scopeError(dex, "coin "+coin+" has no consistent global asset id")
		}
	}
	return write, nil
}

// PartialBatchError marks a multi-request action that cannot be safely retried
// as a whole.
//
// Deprecated: since S32 every homogeneous Place / CancelByCloid / CancelByOid is
// ONE signed /exchange request, so this is no longer returned by the package.
// The type and its IsUncertain handling remain only so existing importers keep
// compiling.
type PartialBatchError struct{ Err error }

func (e *PartialBatchError) Error() string {
	return "hyperliquid: partial multi-scope action; reconcile before retry: " + e.Err.Error()
}
func (e *PartialBatchError) Unwrap() error { return e.Err }

// runAction performs ONE mutation attempt: it takes the context-aware account
// action gate, seeds the Exchange's SDK nonce counter from the account-wide
// last signed nonce (nonceSigner observes the real nonce, so a failed send
// still consumes it), and makes exactly one signed /exchange request for the
// whole input in its original order. It never resends.
func runAction[T, R any](ctx context.Context, c *Client, ex *hl.Exchange, input []T, call func(context.Context, *hl.Exchange, []T) ([]R, error)) ([]R, error) {
	if err := c.lockAction(ctx); err != nil {
		return nil, err
	}
	defer c.unlockAction()
	c.seed(ex)
	return call(ctx, ex, input)
}
