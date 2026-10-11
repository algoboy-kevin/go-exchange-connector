package hyperliquid

import (
	"context"
	"strings"
	"testing"
	"time"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// testUser is a well-formed 0x-prefixed account address.
const testUser = "0x8ba1f109551bd432803012645ac136ddd64dba72"

// TestValidateUser pins the account-address guard: a 0x-prefixed, 40-hex
// address is accepted (any case); a missing 0x, a wrong length or non-hex is
// rejected.
func TestValidateUser(t *testing.T) {
	valid := []string{
		testUser,
		"0x8BA1F109551BD432803012645AC136DDD64DBA72",
		"0x" + strings.Repeat("0", 40),
	}
	for _, u := range valid {
		if err := ValidateUser(u); err != nil {
			t.Errorf("ValidateUser(%q) = %v, want nil", u, err)
		}
	}

	invalid := []string{
		"",
		" 0x" + strings.Repeat("a", 40),      // leading space
		"0x" + strings.Repeat("a", 40) + " ", // trailing space
		strings.Repeat("a", 40),              // no 0x prefix
		"0x1234",                             // too short
		"0x" + strings.Repeat("a", 41),       // too long
		"0x" + strings.Repeat("z", 40),       // not hex
	}
	for _, u := range invalid {
		if err := ValidateUser(u); err == nil {
			t.Errorf("ValidateUser(%q) = nil, want error", u)
		}
	}
}

// TestNormalizeUserLowercases pins the canonical form used for subscription keys.
func TestNormalizeUser(t *testing.T) {
	got, err := NormalizeUser("  0x8BA1F109551BD432803012645AC136DDD64DBA72 ")
	if err != nil {
		t.Fatalf("NormalizeUser: %v", err)
	}
	if got != testUser {
		t.Errorf("NormalizeUser = %q, want %q", got, testUser)
	}
	if _, err := NormalizeUser("0xdeadbeef"); err == nil {
		t.Error("NormalizeUser(short) = nil error, want error")
	}
}

// TestUserSubscribeFrameShape pins the exact wire frame for both account
// channels — {"method":"subscribe","subscription":{"type":…,"user":…}}.
func TestUserSubscribeFrameShape(t *testing.T) {
	tests := []struct {
		name string
		sub  Subscription
		want string
	}{
		{
			name: "orderUpdates",
			sub:  Subscription{Type: ChannelOrderUpdates, User: testUser},
			want: `{"method":"subscribe","subscription":{"type":"orderUpdates","user":"` + testUser + `"}}`,
		},
		{
			name: "userFills",
			sub:  Subscription{Type: ChannelUserFills, User: testUser},
			want: `{"method":"subscribe","subscription":{"type":"userFills","user":"` + testUser + `"}}`,
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

// TestUserSubscriptionKey pins that the user is part of the dedupe key.
func TestUserSubscriptionKey(t *testing.T) {
	tests := []struct {
		sub  Subscription
		want string
	}{
		{Subscription{Type: ChannelOrderUpdates, User: testUser}, "orderUpdates:" + testUser},
		{Subscription{Type: ChannelUserFills, User: testUser}, "userFills:" + testUser},
	}
	for _, tc := range tests {
		if got := tc.sub.Key(); got != tc.want {
			t.Errorf("Key() = %q, want %q", got, tc.want)
		}
	}
}

// TestBuildSubscriptionsUserChannels covers the validation rules: a user is
// required for the account channels, a coin is rejected, and other parameters
// are rejected.
func TestBuildSubscriptionsUserChannels(t *testing.T) {
	tests := []struct {
		name   string
		ch     Channel
		coins  []string
		params SubParams
		ok     bool
	}{
		{"orderUpdates ok", ChannelOrderUpdates, nil, SubParams{User: testUser}, true},
		{"userFills ok", ChannelUserFills, nil, SubParams{User: testUser}, true},
		{"uppercase user normalized", ChannelOrderUpdates, nil, SubParams{User: "0x8BA1F109551BD432803012645AC136DDD64DBA72"}, true},
		{"orderUpdates missing user", ChannelOrderUpdates, nil, SubParams{}, false},
		{"userFills missing user", ChannelUserFills, nil, SubParams{}, false},
		{"orderUpdates bad address", ChannelOrderUpdates, nil, SubParams{User: "0x1234"}, false},
		{"orderUpdates rejects coin", ChannelOrderUpdates, []string{"BTC"}, SubParams{User: testUser}, false},
		{"userFills rejects coin", ChannelUserFills, []string{"BTC"}, SubParams{User: testUser}, false},
		{"orderUpdates rejects fast", ChannelOrderUpdates, nil, SubParams{User: testUser, Fast: true}, false},
		{"userFills rejects interval", ChannelUserFills, nil, SubParams{User: testUser, Interval: "1m"}, false},
		{"trades rejects user", ChannelTrades, []string{"BTC"}, SubParams{User: testUser}, false},
		{"l2Book rejects user", ChannelL2Book, []string{"BTC"}, SubParams{User: testUser}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			subs, err := BuildSubscriptions(tc.ch, tc.coins, tc.params)
			if tc.ok && err != nil {
				t.Fatalf("BuildSubscriptions = %v, want nil", err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatalf("BuildSubscriptions = %+v, want error", subs)
				}
				return
			}
			if len(subs) != 1 {
				t.Fatalf("subs = %d, want 1", len(subs))
			}
			if subs[0].Type != tc.ch || subs[0].Coin != "" {
				t.Errorf("built subscription = %+v", subs[0])
			}
			if got := subs[0].User; got != testUser {
				t.Errorf("user = %q, want normalized %q", got, testUser)
			}
		})
	}
}

// TestSubscribeUserChannels exercises the manager-level Subscribe path: the
// address is normalized, a duplicate is deduped, and a missing/coin-carrying
// subscription is rejected before anything is registered.
func TestSubscribeUserChannels(t *testing.T) {
	h, _ := newTestHyperliquid(t)
	ctx := context.Background()

	if err := h.SubscribeOrderUpdates(ctx, " 0x8BA1F109551BD432803012645AC136DDD64DBA72 "); err != nil {
		t.Fatalf("SubscribeOrderUpdates: %v", err)
	}
	if err := h.SubscribeUserFills(ctx, testUser); err != nil {
		t.Fatalf("SubscribeUserFills: %v", err)
	}
	// The same account again (different case) is a no-op, not a duplicate.
	if err := h.SubscribeOrderUpdates(ctx, strings.ToUpper(testUser)); err != nil {
		t.Fatalf("SubscribeOrderUpdates (repeat): %v", err)
	}
	// A direct Subscribe also normalizes the address, so it dedupes too.
	if err := h.Subscribe(ctx, Subscription{Type: ChannelOrderUpdates, User: strings.ToUpper(testUser)}); err != nil {
		t.Fatalf("Subscribe (direct, uppercase): %v", err)
	}

	subs := h.Subscriptions()
	if len(subs) != 2 {
		t.Fatalf("subscriptions = %d, want 2: %+v", len(subs), subs)
	}
	if subs[0].Key() != "orderUpdates:"+testUser || subs[1].Key() != "userFills:"+testUser {
		t.Errorf("registered keys = %q, %q", subs[0].Key(), subs[1].Key())
	}

	// Invalid requests must not register anything.
	if err := h.SubscribeOrderUpdates(ctx, "not-an-address"); err == nil {
		t.Error("SubscribeOrderUpdates with a malformed address = nil error, want error")
	}
	if err := h.SubscribeUserFills(ctx, ""); err == nil {
		t.Error("SubscribeUserFills with an empty address = nil error, want error")
	}
	if err := h.Subscribe(ctx, Subscription{Type: ChannelOrderUpdates, Coin: "BTC", User: testUser}); err == nil {
		t.Error("Subscribe with a coin on a user channel = nil error, want error")
	}
	if len(h.Subscriptions()) != 2 {
		t.Fatalf("invalid requests changed the registry: %+v", h.Subscriptions())
	}

	// Unsubscribing replays the stored subscription and empties that key.
	if err := h.UnsubscribeOrderUpdates(ctx, testUser); err != nil {
		t.Fatalf("UnsubscribeOrderUpdates: %v", err)
	}
	if len(h.Subscriptions()) != 1 || h.Subscriptions()[0].Type != ChannelUserFills {
		t.Fatalf("after unsubscribe: %+v", h.Subscriptions())
	}
}

// TestParseOrderUpdates decodes the four statuses the driver must act on
// (open, filled, canceled, badAloPxRejected), across cloid present (string) and
// absent (omitted and null).
func TestParseOrderUpdates(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeOrderUpdates(context.Background(), testUser); err != nil {
		t.Fatalf("SubscribeOrderUpdates: %v", err)
	}

	rx := time.Now()
	feed(h, rx, `{"channel":"orderUpdates","data":[`+
		`{"order":{"coin":"BTC","side":"B","limitPx":"121000.0","sz":"0.5","oid":7001,"timestamp":1787380860000,"origSz":"0.5","cloid":"0x00000000000000000000000000000001"},"status":"open","statusTimestamp":1787380860001},`+
		`{"order":{"coin":"BTC","side":"B","limitPx":"121000.0","sz":"0","oid":7001,"timestamp":1787380860000,"origSz":"0.5","cloid":"0x00000000000000000000000000000001"},"status":"filled","statusTimestamp":1787380862500},`+
		`{"order":{"coin":"ETH","side":"A","limitPx":"3000.25","sz":"0.25","oid":7002,"timestamp":1787380861000,"origSz":"0.25"},"status":"canceled","statusTimestamp":1787380863000},`+
		`{"order":{"coin":"ETH","side":"A","limitPx":"3000.25","sz":"0","oid":7003,"timestamp":1787380862000,"origSz":"0.25","cloid":null},"status":"badAloPxRejected","statusTimestamp":1787380864000}]}`)

	if len(*got) != 4 {
		t.Fatalf("events = %d, want 4: %+v", len(*got), *got)
	}
	want := []struct {
		coin, side, px, sz, origSz, cloid, status string
		oid, ts, statusTS                         int64
	}{
		{"BTC", "B", "121000.0", "0.5", "0.5", "0x00000000000000000000000000000001", "open", 7001, 1787380860000, 1787380860001},
		{"BTC", "B", "121000.0", "0", "0.5", "0x00000000000000000000000000000001", "filled", 7001, 1787380860000, 1787380862500},
		{"ETH", "A", "3000.25", "0.25", "0.25", "", "canceled", 7002, 1787380861000, 1787380863000},
		{"ETH", "A", "3000.25", "0", "0.25", "", "badAloPxRejected", 7003, 1787380862000, 1787380864000},
	}
	for i, w := range want {
		ev, ok := (*got)[i].(*connector.HyperliquidOrderUpdateEvent)
		if !ok {
			t.Fatalf("event %d is %T, want *HyperliquidOrderUpdateEvent", i, (*got)[i])
		}
		if ev.Coin != w.coin || ev.Side != w.side || ev.LimitPx != w.px || ev.Sz != w.sz ||
			ev.OrigSz != w.origSz || ev.Cloid != w.cloid || ev.Status != w.status ||
			ev.Oid != w.oid || ev.Timestamp != w.ts || ev.StatusTimestamp != w.statusTS {
			t.Errorf("event %d = %+v, want %+v", i, ev, w)
		}
		if !ev.ReceivedAt.Equal(rx) || ev.SeqID <= 0 {
			t.Errorf("event %d header = rx=%v seq=%d", i, ev.ReceivedAt, ev.SeqID)
		}
	}
	if (*got)[3].(*connector.HyperliquidOrderUpdateEvent).SeqID <= (*got)[0].(*connector.HyperliquidOrderUpdateEvent).SeqID {
		t.Error("SeqID is not monotonic across one frame's elements")
	}
}

// TestParseUserFills decodes the snapshot frame (IsSnapshot true) and an
// incremental frame, preserving the venue's decimal text.
func TestParseUserFills(t *testing.T) {
	h, got := newTestHyperliquid(t)
	if err := h.SubscribeUserFills(context.Background(), testUser); err != nil {
		t.Fatalf("SubscribeUserFills: %v", err)
	}

	rx := time.Now()
	feed(h, rx, `{"channel":"userFills","data":{"isSnapshot":true,"user":"`+testUser+`","fills":[`+
		`{"coin":"BTC","px":"121000.5","sz":"0.5","side":"B","time":1787380862500,"startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"0xabc","oid":7001,"crossed":true,"fee":"0.02","tid":123456789,"feeToken":"USDC"},`+
		`{"coin":"BTC","px":"121001.0","sz":"0.2","side":"A","time":1787380863000,"startPosition":"0.5","dir":"Close Long","closedPnl":"0.1","hash":"0xdef","oid":7002,"crossed":false,"fee":"-0.005","tid":123456790,"feeToken":"USDC"}]}}`)

	if len(*got) != 1 {
		t.Fatalf("events = %d, want 1 (one per frame)", len(*got))
	}
	snapshot, ok := (*got)[0].(*connector.HyperliquidUserFillsEvent)
	if !ok {
		t.Fatalf("event is %T, want *HyperliquidUserFillsEvent", (*got)[0])
	}
	if !snapshot.IsSnapshot || snapshot.User != testUser || !snapshot.ReceivedAt.Equal(rx) {
		t.Errorf("snapshot header = %+v", snapshot)
	}
	if len(snapshot.Fills) != 2 {
		t.Fatalf("fills = %d, want 2", len(snapshot.Fills))
	}
	f0 := snapshot.Fills[0]
	if f0.Coin != "BTC" || f0.Px != "121000.5" || f0.Sz != "0.5" || f0.Side != "B" ||
		f0.Dir != "Open Long" || f0.ClosedPnl != "0" || f0.Hash != "0xabc" || f0.Fee != "0.02" ||
		f0.FeeToken != "USDC" || f0.StartPosition != "0" || f0.Time != 1787380862500 ||
		f0.Oid != 7001 || f0.Tid != 123456789 || !f0.Crossed || f0.Cloid != "" {
		t.Errorf("fill[0] = %+v", f0)
	}
	f1 := snapshot.Fills[1]
	if f1.Fee != "-0.005" || f1.StartPosition != "0.5" || f1.Crossed || f1.Dir != "Close Long" {
		t.Errorf("fill[1] = %+v", f1)
	}

	// Incremental frame: no isSnapshot field.
	feed(h, rx, `{"channel":"userFills","data":{"user":"`+testUser+`","fills":[`+
		`{"coin":"ETH","px":"3000.25","sz":"1.0","side":"B","time":1787380864000,"startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"0x123","oid":8001,"crossed":true,"fee":"0.03","tid":223456789,"feeToken":"USDC"}]}}`)

	if len(*got) != 2 {
		t.Fatalf("events = %d, want 2", len(*got))
	}
	inc := (*got)[1].(*connector.HyperliquidUserFillsEvent)
	if inc.IsSnapshot || inc.User != testUser || len(inc.Fills) != 1 {
		t.Errorf("incremental event = %+v", inc)
	}
	if inc.Fills[0].Coin != "ETH" || inc.Fills[0].Px != "3000.25" || inc.Fills[0].Tid != 223456789 {
		t.Errorf("incremental fill = %+v", inc.Fills[0])
	}
}

// TestUserChannelFrameDecodedWithoutSubscription is the spec's decoding rule: a
// frame for a user channel we did not subscribe to is still decoded (the
// coin-channel "drop the unsubscribed" rule does not apply — the account
// channels carry no coin to key on).
func TestUserChannelFrameDecodedWithoutSubscription(t *testing.T) {
	h, got := newTestHyperliquid(t)
	// No Subscribe* call at all.

	feed(h, time.Now(), `{"channel":"orderUpdates","data":[`+
		`{"order":{"coin":"BTC","side":"B","limitPx":"121000.0","sz":"0.5","oid":1,"timestamp":1,"origSz":"0.5"},"status":"open","statusTimestamp":2}]}`)
	feed(h, time.Now(), `{"channel":"userFills","data":{"user":"`+testUser+`","fills":[`+
		`{"coin":"BTC","px":"1","sz":"1","side":"B","time":1,"startPosition":"0","dir":"Open Long","closedPnl":"0","oid":1,"crossed":true,"fee":"0","tid":1}]}}`)

	if len(*got) != 2 {
		t.Fatalf("events = %d, want 2 (both decoded)", len(*got))
	}
	if _, ok := (*got)[0].(*connector.HyperliquidOrderUpdateEvent); !ok {
		t.Errorf("event 0 is %T", (*got)[0])
	}
	if _, ok := (*got)[1].(*connector.HyperliquidUserFillsEvent); !ok {
		t.Errorf("event 1 is %T", (*got)[1])
	}
}

// TestUserChannelUndecodableFrameDoesNotPanic mirrors the market-channel guard: a
// wrong-shaped or non-JSON payload logs and drops.
func TestUserChannelUndecodableFrameDoesNotPanic(t *testing.T) {
	h, got := newTestHyperliquid(t)
	feed(h, time.Now(), `{"channel":"orderUpdates","data":{"not":"an array"}}`)
	feed(h, time.Now(), `{"channel":"orderUpdates","data":123}`)
	feed(h, time.Now(), `{"channel":"userFills","data":"not an object"}`)
	feed(h, time.Now(), `{"channel":"userFills","data":null}`)

	if len(*got) != 0 {
		t.Fatalf("events = %d, want 0: %+v", len(*got), *got)
	}
}
