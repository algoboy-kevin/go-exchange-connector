package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// TestNewWithOptionsOverridesURLs proves the Options endpoints reach the
// connection dial URL and the REST depth-snapshot base, and that empty fields
// fall back to the production defaults (so New is unchanged).
func TestNewWithOptionsOverridesURLs(t *testing.T) {
	const (
		mdMarket = "wss://demo-fstream.binance.com/market/ws"
		mdPublic = "wss://demo-fstream.binance.com/public/ws"
		mdREST   = "https://demo-fapi.binance.com"
	)

	b := NewWithOptions(connector.New(false, nil), Options{
		PerpMarketWS: mdMarket,
		PerpPublicWS: mdPublic,
		PerpREST:     mdREST,
	})
	if got := b.connFor(MarketPerp, classMarket).wsURL(); got != mdMarket {
		t.Errorf("perp market wsURL = %q, want %q", got, mdMarket)
	}
	if got := b.connFor(MarketPerp, classPublic).wsURL(); got != mdPublic {
		t.Errorf("perp public wsURL = %q, want %q", got, mdPublic)
	}
	if got := b.restURL(MarketPerp); got != mdREST {
		t.Errorf("perp restURL = %q, want %q", got, mdREST)
	}
	// Spot is untouched by the options.
	if got := b.connFor(MarketSpot, classSpot).wsURL(); got != spotWSSURL {
		t.Errorf("spot wsURL = %q, want %q", got, spotWSSURL)
	}
	if got := b.restURL(MarketSpot); got != spotRESTURL {
		t.Errorf("spot restURL = %q, want %q", got, spotRESTURL)
	}

	// New keeps the production endpoints.
	d := New(connector.New(false, nil))
	if got := d.connFor(MarketPerp, classMarket).wsURL(); got != perpMarketWSSURL {
		t.Errorf("default market wsURL = %q, want %q", got, perpMarketWSSURL)
	}
	if got := d.connFor(MarketPerp, classPublic).wsURL(); got != perpPublicWSSURL {
		t.Errorf("default public wsURL = %q, want %q", got, perpPublicWSSURL)
	}
	if got := d.restURL(MarketPerp); got != perpRESTURL {
		t.Errorf("default perp restURL = %q, want %q", got, perpRESTURL)
	}
	if got := d.connFor(MarketPerp, classMarket).wsURL(); got != d.opts.PerpMarketWS {
		t.Errorf("New did not resolve options")
	}

	// Partial options: only PerpREST set, the WS endpoints stay default.
	e := NewWithOptions(connector.New(false, nil), Options{PerpREST: mdREST})
	if got := e.connFor(MarketPerp, classMarket).wsURL(); got != perpMarketWSSURL {
		t.Errorf("partial: market wsURL = %q, want default", got)
	}
	if got := e.connFor(MarketPerp, classPublic).wsURL(); got != perpPublicWSSURL {
		t.Errorf("partial: public wsURL = %q, want default", got)
	}
	if got := e.restURL(MarketPerp); got != mdREST {
		t.Errorf("partial: restURL = %q, want %q", got, mdREST)
	}

	// Every resolved field is non-empty even from a zero Options.
	z := New(connector.New(false, nil)).opts
	if z.PerpMarketWS == "" || z.PerpPublicWS == "" || z.PerpREST == "" {
		t.Errorf("zero Options not fully resolved: %+v", z)
	}
}

// TestDepthSnapshotUsesOptionsREST proves the overridden REST base is actually
// used to fetch depth snapshots (the point of PerpREST).
func TestDepthSnapshotUsesOptionsREST(t *testing.T) {
	var (
		gotMethod string
		gotPath   string
		gotSymbol string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotSymbol = r.URL.Query().Get("symbol")
		_, _ = w.Write([]byte(`{"lastUpdateId":7,"bids":[["100.0","1.0"]],"asks":[["101.0","2.0"]]}`))
	}))
	defer srv.Close()

	base := connector.New(false, nil)
	b := NewWithOptions(base, Options{PerpREST: srv.URL})
	b.SubscribeDepth(context.Background(), MarketPerp, []string{"btcusdt"})

	if err := b.snapshotDepthBook(context.Background(), MarketPerp, "btcusdt"); err != nil {
		t.Fatalf("snapshotDepthBook: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/fapi/v1/depth" {
		t.Errorf("depth snapshot request = %s %s", gotMethod, gotPath)
	}
	if gotSymbol != "BTCUSDT" {
		t.Errorf("depth snapshot symbol = %q, want BTCUSDT", gotSymbol)
	}

	bk := b.bookFor(MarketPerp, "btcusdt")
	bk.mu.Lock()
	defer bk.mu.Unlock()
	if !bk.snapshotted || bk.lastUpdateID != 7 {
		t.Errorf("book not seeded from the overridden REST base: %+v", bk)
	}
}
