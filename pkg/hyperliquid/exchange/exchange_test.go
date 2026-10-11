package exchange

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

// TestNewFetchesMetaOnce proves the constructor fetches meta itself (so
// sonirico's NewInfo never has to, and never panics) and that Meta exposes that
// single snapshot.
func TestNewFetchesMetaOnce(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)

	calls := v.infoCalls()
	if len(calls) != 1 || calls[0] != "meta" {
		t.Fatalf("/info calls during New = %v, want exactly [meta]", calls)
	}

	m, err := c.Meta(context.Background())
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}
	if got := v.infoCalls(); len(got) != 1 {
		t.Fatalf("Meta hit the network (%v), want the startup snapshot", got)
	}
	if len(m.Universe) != 3 {
		t.Fatalf("universe = %d assets, want 3", len(m.Universe))
	}
	if btc := m.Universe[0]; btc.Name != "BTC" || btc.SzDecimals != 5 || btc.MaxLeverage != 40 || btc.OnlyIsolated || btc.IsDelisted {
		t.Errorf("bad BTC asset: %+v", btc)
	}
	if eth := m.Universe[1]; eth.Name != "ETH" || eth.SzDecimals != 4 || eth.MaxLeverage != 25 {
		t.Errorf("bad ETH asset: %+v", eth)
	}
	if old := m.Universe[2]; old.Name != "OLD" || !old.OnlyIsolated || !old.IsDelisted {
		t.Errorf("bad delisted asset: %+v", old)
	}

	if c.Account() != testAccount {
		t.Errorf("Account() = %q, want %q", c.Account(), testAccount)
	}
	if c.BaseURL() != strings.TrimRight(url, "/") {
		t.Errorf("BaseURL() = %q, want %q", c.BaseURL(), url)
	}
}

// TestNewMetaFetchFailureReturnsError proves New returns an error instead of
// panicking when the meta fetch fails: hl.NewExchange → NewInfo panics if it is
// left to fetch meta itself, so the wrapper must always fetch it first.
func TestNewMetaFetchFailureReturnsError(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(v *venue)
		uncertain bool
	}{
		{
			name:      "http 500",
			setup:     func(v *venue) { v.setInfo("meta", `{"error":"internal"}`, http.StatusInternalServerError) },
			uncertain: true,
		},
		{
			name:      "200 but not json",
			setup:     func(v *venue) { v.setInfo("meta", `<html>gateway</html>`, 0) },
			uncertain: true,
		},
		{
			name:      "empty universe",
			setup:     func(v *venue) { v.setInfo("meta", `{"universe":[],"marginTables":[]}`, 0) },
			uncertain: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("New panicked instead of returning an error: %v", r)
				}
			}()
			v, url := newVenue(t)
			tc.setup(v)

			c, err := New(context.Background(), Config{
				BaseURL:  url,
				AgentKey: testAgentKey,
				Account:  testAccount,
			})
			if err == nil {
				t.Fatal("New = nil error, want an error")
			}
			if c != nil {
				t.Error("New returned a client alongside an error")
			}
			if got := IsUncertain(err); got != tc.uncertain {
				t.Errorf("IsUncertain(%v) = %v, want %v", err, got, tc.uncertain)
			}
		})
	}
}

// TestNewUnreachableVenueIsUncertain covers a transport failure during the meta
// fetch: an error, no panic, classified uncertain.
func TestNewUnreachableVenueIsUncertain(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("New panicked: %v", r)
		}
	}()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening any more

	_, err := New(context.Background(), Config{BaseURL: url, AgentKey: testAgentKey, Account: testAccount})
	if err == nil {
		t.Fatal("New = nil error against a closed server")
	}
	if !IsUncertain(err) {
		t.Errorf("IsUncertain(%v) = false, want true", err)
	}
}

// TestNewConfigValidation pins the local validation paths (and that none of them
// reaches the network).
func TestNewConfigValidation(t *testing.T) {
	v, url := newVenue(t)
	cases := []struct {
		name   string
		cfg    Config
		substr string
	}{
		{"unknown network", Config{Network: "devnet", AgentKey: testAgentKey, Account: testAccount}, "unknown network"},
		{"empty key", Config{BaseURL: url, Account: testAccount}, "agent key is empty"},
		{"bad key hex", Config{BaseURL: url, AgentKey: "zz", Account: testAccount}, "not valid hex"},
		{"short key", Config{BaseURL: url, AgentKey: "abcdef", Account: testAccount}, "secp256k1"},
		{"empty account", Config{BaseURL: url, AgentKey: testAgentKey}, "account address is empty"},
		{"short account", Config{BaseURL: url, AgentKey: testAgentKey, Account: "0x1234"}, "40 hex"},
		{"bad account hex", Config{BaseURL: url, AgentKey: testAgentKey, Account: "0x" + strings.Repeat("z", 40)}, "not valid hex"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(context.Background(), tc.cfg)
			if err == nil {
				t.Fatalf("New = nil error, want %q", tc.substr)
			}
			if c != nil {
				t.Error("client returned with an error")
			}
			if !strings.Contains(err.Error(), tc.substr) {
				t.Errorf("error %q does not contain %q", err, tc.substr)
			}
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Errorf("error %T is not *ConfigError", err)
			}
		})
	}
	if got := v.infoCalls(); len(got) != 0 {
		t.Errorf("/info was called %v despite invalid config", got)
	}
}

// TestNewWiresMetaIntoExchange proves the pre-fetched meta reached sonirico's
// asset→id map: without it every Place would fail with "coin not found in info".
func TestNewWiresMetaIntoExchange(t *testing.T) {
	_, url := newVenue(t)
	c := newTestClient(t, url)

	info := c.ex.Info()
	if info == nil {
		t.Fatal("exchange has no info")
	}
	if id, ok := info.CoinToAsset("BTC"); !ok || id != 0 {
		t.Errorf("BTC asset id = %d, ok = %v, want 0/true", id, ok)
	}
	if id, ok := info.CoinToAsset("ETH"); !ok || id != 1 {
		t.Errorf("ETH asset id = %d, ok = %v, want 1/true", id, ok)
	}
}

// agentAddress derives the agent wallet's address from testAgentKey — it must
// never appear in a query (queries use the master account).
func agentAddress(t *testing.T) string {
	t.Helper()
	var key *ecdsa.PrivateKey
	key, err := crypto.HexToECDSA(testAgentKey)
	if err != nil {
		t.Fatalf("HexToECDSA: %v", err)
	}
	return crypto.PubkeyToAddress(key.PublicKey).Hex()
}
