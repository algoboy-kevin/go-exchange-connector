package polymarket

import (
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func TestResolveReadLimit(t *testing.T) {
	cases := []struct {
		in   int64
		want int64
	}{
		{0, defaultReadLimitBytes}, // unset → default
		{1 << 20, 1 << 20},         // explicit 1 MiB
		{4 << 20, 4 << 20},         // explicit larger
		{-1, -1},                   // disable the limit
	}
	for _, tc := range cases {
		if got := resolveReadLimit(tc.in); got != tc.want {
			t.Errorf("resolveReadLimit(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The market channel delivers full-depth book snapshots that routinely exceed
// coder/websocket's 32 KiB default. If the limit is not raised, an oversized
// frame fails the read, closes the socket, and the reconnect re-pulls the same
// snapshot — a drop/reconnect loop that loses the deepest books.
func TestMarketWSOptionsReadLimit(t *testing.T) {
	base := connector.New(false, nil)

	m := NewWSPolymarketMarket(base, Config{})
	if got := m.wsOptions(0).ReadLimit; got != defaultReadLimitBytes {
		t.Errorf("default market ReadLimit = %d, want %d", got, defaultReadLimitBytes)
	}

	m = NewWSPolymarketMarket(base, Config{ReadLimitBytes: 4 << 20})
	if got := m.wsOptions(0).ReadLimit; got != 4<<20 {
		t.Errorf("market ReadLimit = %d, want %d", got, int64(4<<20))
	}

	m = NewWSPolymarketMarket(base, Config{ReadLimitBytes: -1})
	if got := m.wsOptions(0).ReadLimit; got != -1 {
		t.Errorf("market ReadLimit = %d, want -1", got)
	}
}

func TestUserWSOptionsReadLimit(t *testing.T) {
	base := connector.New(false, nil)

	u := NewWSPolymarketUserWS(base, UserAuth{}, UserHandlers{})
	if got := u.wsOptions().ReadLimit; got != defaultReadLimitBytes {
		t.Errorf("default user ReadLimit = %d, want %d", got, defaultReadLimitBytes)
	}

	u.SetReadLimitBytes(2 << 20)
	if got := u.wsOptions().ReadLimit; got != 2<<20 {
		t.Errorf("user ReadLimit = %d, want %d", got, int64(2<<20))
	}

	u.SetReadLimitBytes(0) // zero resets to the default, not to 32 KiB
	if got := u.wsOptions().ReadLimit; got != defaultReadLimitBytes {
		t.Errorf("user ReadLimit after reset = %d, want %d", got, defaultReadLimitBytes)
	}
}

func TestRTDSWSOptionsReadLimit(t *testing.T) {
	base := connector.New(false, nil)

	r := NewWSPolymarketRTDS(base)
	if got := r.wsOptions(0).ReadLimit; got != defaultReadLimitBytes {
		t.Errorf("default rtds ReadLimit = %d, want %d", got, defaultReadLimitBytes)
	}

	r.SetReadLimitBytes(8 << 20)
	if got := r.wsOptions(0).ReadLimit; got != 8<<20 {
		t.Errorf("rtds ReadLimit = %d, want %d", got, int64(8<<20))
	}
}
