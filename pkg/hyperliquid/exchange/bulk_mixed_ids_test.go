package exchange

import (
	"testing"
)

// The merged write Exchange must compute the SAME global asset id as every
// per-DEX read Exchange, whatever order the DEXes were registered in, and match
// the golden ids for registry [null,xyz,io,para].
func TestMergedWriteExchangeIDsMatchPerDexExchanges(t *testing.T) {
	golden := map[string]int{
		"BTC": 0, "ETH": 1,
		"xyz:A": 110000, "xyz:B": 110001,
		"io:A": 120000, "io:B": 120001,
		"para:A": 130000, "para:B": 130001,
	}
	perms := [][]string{
		{"xyz", "io", "para"}, {"xyz", "para", "io"}, {"io", "xyz", "para"},
		{"io", "para", "xyz"}, {"para", "xyz", "io"}, {"para", "io", "xyz"},
	}
	for _, order := range perms {
		_, url := newMixedVenue(t)
		c := newMixedClient(t, url, nil, order...)
		merged := c.writeExchange()
		if merged == nil {
			t.Fatal("no merged write exchange")
		}
		for _, dex := range append([]string{""}, order...) {
			scoped, err := c.scope(dex)
			if err != nil {
				t.Fatal(err)
			}
			for coin := range golden {
				want, inScope := scoped.Info().CoinToAsset(coin)
				if !inScope {
					continue
				}
				got, ok := merged.Info().CoinToAsset(coin)
				if !ok || got != want || got != golden[coin] {
					t.Fatalf("order %v coin %s merged=%d(%v) scoped=%d golden=%d", order, coin, got, ok, want, golden[coin])
				}
			}
		}
		for coin, want := range golden {
			if got, ok := merged.Info().CoinToAsset(coin); !ok || got != want {
				t.Fatalf("order %v coin %s merged=%d(%v) want %d", order, coin, got, ok, want)
			}
		}
	}
}
