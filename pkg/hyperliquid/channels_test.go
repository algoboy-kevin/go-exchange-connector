package hyperliquid

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateCoin(t *testing.T) {
	valid := []string{"BTC", "ETH", "kPEPE", "HYPE", "xyz:TSLA", "0G"}
	for _, coin := range valid {
		if err := ValidateCoin(coin); err != nil {
			t.Errorf("ValidateCoin(%q) = %v, want nil", coin, err)
		}
	}

	invalid := []string{
		"",
		" BTC",     // leading space
		"BTC ",     // trailing space
		"B TC",     // embedded space
		"BTC\tETH", // tab
		"B\"TC",    // quote — would break out of the JSON string
		`B\TC`,     // backslash
		"BTC\n",    // newline
	}
	for _, coin := range invalid {
		if err := ValidateCoin(coin); err == nil {
			t.Errorf("ValidateCoin(%q) = nil, want error", coin)
		}
	}
}

func TestNormalizeCoinTrims(t *testing.T) {
	got, err := NormalizeCoin("  BTC  ")
	if err != nil {
		t.Fatalf("NormalizeCoin: %v", err)
	}
	if got != "BTC" {
		t.Errorf("NormalizeCoin = %q, want BTC", got)
	}
	if _, err := NormalizeCoin("   "); err == nil {
		t.Error("NormalizeCoin(blank) = nil error, want error")
	}
}

func TestIsSubscribable(t *testing.T) {
	for _, ch := range SubscribableChannels() {
		if !IsSubscribable(ch) {
			t.Errorf("IsSubscribable(%q) = false, want true", ch)
		}
	}
	for _, ch := range []Channel{"", "trade", "l2book", "candle1m", ServerChannelPong, ServerChannelError} {
		if IsSubscribable(ch) {
			t.Errorf("IsSubscribable(%q) = true, want false", ch)
		}
	}
}

func TestBuildSubscriptionsValidation(t *testing.T) {
	tests := []struct {
		name   string
		ch     Channel
		coins  []string
		params SubParams
		ok     bool
	}{
		{"trades ok", ChannelTrades, []string{"BTC", "ETH"}, SubParams{}, true},
		{"trades trims", ChannelTrades, []string{" BTC "}, SubParams{}, true},
		{"trades no coins", ChannelTrades, nil, SubParams{}, false},
		{"trades duplicate", ChannelTrades, []string{"BTC", "BTC"}, SubParams{}, false},
		{"trades bad coin", ChannelTrades, []string{"B TC"}, SubParams{}, false},
		{"trades rejects params", ChannelTrades, []string{"BTC"}, SubParams{Fast: true}, false},
		{"unknown channel", "trade", []string{"BTC"}, SubParams{}, false},
		{"empty channel", "", []string{"BTC"}, SubParams{}, false},

		{"allMids ok", ChannelAllMids, nil, SubParams{}, true},
		{"allMids rejects coin", ChannelAllMids, []string{"BTC"}, SubParams{}, false},

		{"l2Book ok", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 5, Mantissa: 2, Fast: true}, true},
		{"l2Book nSigFigs 2", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 2}, true},
		{"l2Book nSigFigs 1 too low", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 1}, false},
		{"l2Book nSigFigs 6 too high", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 6}, false},
		{"l2Book mantissa without nSigFigs", ChannelL2Book, []string{"BTC"}, SubParams{Mantissa: 2}, false},
		{"l2Book mantissa with nSigFigs 4", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 4, Mantissa: 2}, false},
		{"l2Book mantissa 3 illegal", ChannelL2Book, []string{"BTC"}, SubParams{NSigFigs: 5, Mantissa: 3}, false},
		{"l2Book rejects interval", ChannelL2Book, []string{"BTC"}, SubParams{Interval: "1m"}, false},

		{"candle ok", ChannelCandle, []string{"BTC"}, SubParams{Interval: "1m"}, true},
		{"candle missing interval", ChannelCandle, []string{"BTC"}, SubParams{}, false},
		{"candle bad interval", ChannelCandle, []string{"BTC"}, SubParams{Interval: "2m"}, false},
		{"candle rejects fast", ChannelCandle, []string{"BTC"}, SubParams{Interval: "1m", Fast: true}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			subs, err := BuildSubscriptions(tc.ch, tc.coins, tc.params)
			if tc.ok && err != nil {
				t.Fatalf("BuildSubscriptions(%q, %v, %+v) = %v, want nil", tc.ch, tc.coins, tc.params, err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatalf("BuildSubscriptions(%q, %v, %+v) = %+v, want error", tc.ch, tc.coins, tc.params, subs)
				}
				return
			}
			// allMids is global: one subscription regardless of coin count.
			wantSubs := len(tc.coins)
			if tc.ch == ChannelAllMids {
				wantSubs = 1
			}
			if len(subs) != wantSubs {
				t.Fatalf("got %d subscriptions, want %d", len(subs), wantSubs)
			}
			for _, sub := range subs {
				if sub.Type != tc.ch {
					t.Errorf("subscription type = %q, want %q", sub.Type, tc.ch)
				}
			}
		})
	}
}

func TestBuildSubscriptionsAllMidsHasNoCoin(t *testing.T) {
	subs, err := BuildSubscriptions(ChannelAllMids, nil, SubParams{})
	if err != nil {
		t.Fatalf("BuildSubscriptions: %v", err)
	}
	if len(subs) != 1 || subs[0].Coin != "" {
		t.Fatalf("got %+v, want one coin-less subscription", subs)
	}
}

// TestSubscribeFrameShape pins the documented wire format. It is a regression
// guard: RECORDER_SPEC.md §1 shows a bare {"type":"l2Book","coin":"BTC"} frame,
// but the venue documents (and every published SDK sends) the method +
// subscription form below. A recorder built from the bare form would record
// nothing but error frames.
func TestSubscribeFrameShape(t *testing.T) {
	tests := []struct {
		name string
		sub  Subscription
		want string
	}{
		{
			name: "trades",
			sub:  Subscription{Type: ChannelTrades, Coin: "BTC"},
			want: `{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}`,
		},
		{
			name: "l2Book fast",
			sub:  Subscription{Type: ChannelL2Book, Coin: "BTC", Fast: true},
			want: `{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","fast":true}}`,
		},
		{
			name: "l2Book thinned",
			sub:  Subscription{Type: ChannelL2Book, Coin: "BTC", NSigFigs: 5, Mantissa: 2},
			want: `{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":5,"mantissa":2}}`,
		},
		{
			name: "candle",
			sub:  Subscription{Type: ChannelCandle, Coin: "BTC", Interval: "1m"},
			want: `{"method":"subscribe","subscription":{"type":"candle","coin":"BTC","interval":"1m"}}`,
		},
		{
			name: "allMids",
			sub:  Subscription{Type: ChannelAllMids},
			want: `{"method":"subscribe","subscription":{"type":"allMids"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SubscribeFrame(tc.sub)
			if err != nil {
				t.Fatalf("SubscribeFrame: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("frame mismatch:\n got  %s\n want %s", got, tc.want)
			}
		})
	}
}

func TestUnsubscribeFrameShape(t *testing.T) {
	got, err := UnsubscribeFrame(Subscription{Type: ChannelBBO, Coin: "ETH"})
	if err != nil {
		t.Fatalf("UnsubscribeFrame: %v", err)
	}
	want := `{"method":"unsubscribe","subscription":{"type":"bbo","coin":"ETH"}}`
	if string(got) != want {
		t.Errorf("frame mismatch:\n got  %s\n want %s", got, want)
	}
}

func TestPingFrame(t *testing.T) {
	if got := string(PingFrame()); got != `{"method":"ping"}` {
		t.Errorf("PingFrame = %s", got)
	}
	if !json.Valid(PingFrame()) {
		t.Error("PingFrame is not valid JSON")
	}
}

func TestSubscriptionKey(t *testing.T) {
	tests := []struct {
		sub  Subscription
		want string
	}{
		{Subscription{Type: ChannelTrades, Coin: "BTC"}, "trades:BTC"},
		{Subscription{Type: ChannelL2Book, Coin: "BTC", Fast: true}, "l2Book:BTC"},
		{Subscription{Type: ChannelAllMids}, "allMids"},
		{Subscription{Type: ChannelCandle, Coin: "BTC", Interval: "1m"}, "candle:BTC:1m"},
	}
	for _, tc := range tests {
		if got := tc.sub.Key(); got != tc.want {
			t.Errorf("Key() = %q, want %q", got, tc.want)
		}
	}
}

func TestCandleIntervals(t *testing.T) {
	if len(CandleIntervals()) != len(candleIntervals) {
		t.Fatalf("CandleIntervals length changed")
	}
	for _, iv := range []string{"1m", "5m", "1h", "1d", "1M"} {
		if !ValidCandleInterval(iv) {
			t.Errorf("ValidCandleInterval(%q) = false, want true", iv)
		}
	}
	for _, iv := range []string{"", "2m", "1m ", "1y"} {
		if ValidCandleInterval(iv) {
			t.Errorf("ValidCandleInterval(%q) = true, want false", iv)
		}
	}
	// The returned slice is a copy: mutating it must not affect the package.
	ivs := CandleIntervals()
	ivs[0] = "mutated"
	if !strings.HasPrefix(candleIntervals[0], "1") {
		t.Error("CandleIntervals returned the internal slice")
	}
}
