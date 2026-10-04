package hyperliquid

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// ─────────────────────────────────────────────────────────────
// Channels
// ─────────────────────────────────────────────────────────────

// Channel identifies a Hyperliquid WebSocket channel.
//
// The subscription wire format (docs, /api/websocket/subscriptions) is one
// frame per subscription:
//
//	{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
//
// and the venue acknowledges with a frame whose `channel` is
// "subscriptionResponse" and whose `data` echoes the subscription. Every data
// frame is {"channel":"<type>","data":<payload>}.
//
// This is NOT the bare {"type":"l2Book","coin":"BTC"} form: that shape appears
// in some third-party write-ups but is not the documented protocol (verified
// 2026-10-04 against the venue docs and sonirico/go-hyperliquid, both of which
// send method+subscription).
type Channel string

const (
	// ChannelL2Book streams L2 order-book snapshots (L2BookSnapshot): a paced
	// feed, pushed at most every ~0.5s per coin.
	ChannelL2Book Channel = "l2Book"
	// ChannelTrades streams trade prints ([]Trade). Event-driven: a quiet coin
	// produces no frames.
	ChannelTrades Channel = "trades"
	// ChannelBBO streams best bid/offer updates (BBO), but only on a block
	// where the bbo changed — so it is event-driven, and either side may be
	// null.
	ChannelBBO Channel = "bbo"
	// ChannelActiveAssetCtx streams the perp asset context (funding, open
	// interest, oracle/mark/mid price) once per block.
	ChannelActiveAssetCtx Channel = "activeAssetCtx"
	// ChannelAllMids streams a mid price for every coin (AllMids). It takes no
	// coin argument: the subscription is global and one frame covers the whole
	// universe.
	ChannelAllMids Channel = "allMids"
	// ChannelCandle streams OHLCV candles (Candle) for a coin/interval pair.
	ChannelCandle Channel = "candle"
)

// Channels the venue sends that are not subscribable. They are named here so
// the parser, the recorder and any consumer can refer to them without string
// literals.
const (
	// ServerChannelSubscriptionResponse acknowledges a subscribe/unsubscribe
	// frame.
	ServerChannelSubscriptionResponse Channel = "subscriptionResponse"
	// ServerChannelPong answers an application-level {"method":"ping"} frame.
	ServerChannelPong Channel = "pong"
	// ServerChannelError reports a rejected subscription or a bad request
	// ({"channel":"error","data":"..."}). This is the only signal that a
	// subscription was refused — a subscription that never delivers otherwise
	// looks exactly like a quiet market.
	ServerChannelError Channel = "error"
)

// subscribableChannels is the closed set accepted by BuildSubscriptions and by
// every Subscribe/Unsubscribe method. A channel outside this set is a typo
// that would otherwise produce a silently empty feed.
var subscribableChannels = []Channel{
	ChannelL2Book,
	ChannelTrades,
	ChannelBBO,
	ChannelActiveAssetCtx,
	ChannelAllMids,
	ChannelCandle,
}

// SubscribableChannels returns the closed set of subscribable channels.
func SubscribableChannels() []Channel {
	out := make([]Channel, len(subscribableChannels))
	copy(out, subscribableChannels)
	return out
}

// IsSubscribable reports whether ch can be subscribed to.
func IsSubscribable(ch Channel) bool {
	for _, c := range subscribableChannels {
		if c == ch {
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────
// Candle intervals
// ─────────────────────────────────────────────────────────────

// candleIntervals is the venue's supported candle interval set, in ascending
// order (docs, /api/websocket/subscriptions §candle).
var candleIntervals = []string{
	"1m", "3m", "5m", "15m", "30m",
	"1h", "2h", "4h", "8h", "12h",
	"1d", "3d", "1w", "1M",
}

// CandleIntervals returns the supported candle intervals.
func CandleIntervals() []string {
	out := make([]string, len(candleIntervals))
	copy(out, candleIntervals)
	return out
}

// ValidCandleInterval reports whether interval is supported by the venue.
func ValidCandleInterval(interval string) bool {
	for _, v := range candleIntervals {
		if v == interval {
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────
// Coin validation
// ─────────────────────────────────────────────────────────────

// ValidateCoin checks a coin name before it is interpolated into a
// subscription frame.
//
// This is the package's one injection surface: the coin is embedded in a JSON
// document that is written raw to the socket. Rejecting whitespace, quotes,
// backslashes and control characters means a malformed coin can never break
// the frame — while still allowing the shapes the venue actually uses, e.g.
// "BTC", "kPEPE", "xyz:TSLA" (HIP-3 builder-deployed perps).
//
// Coin names are case-sensitive at the venue: "BTC" is valid, "btc" is not, so
// no case folding is applied.
func ValidateCoin(coin string) error {
	if coin == "" {
		return fmt.Errorf("hyperliquid: empty coin")
	}
	if coin != strings.TrimSpace(coin) {
		return fmt.Errorf("hyperliquid: coin %q has surrounding whitespace", coin)
	}
	for _, r := range coin {
		switch {
		case r == '"' || r == '\\':
			return fmt.Errorf("hyperliquid: coin %q contains %q", coin, string(r))
		case unicode.IsSpace(r):
			return fmt.Errorf("hyperliquid: coin %q contains whitespace", coin)
		case unicode.IsControl(r):
			return fmt.Errorf("hyperliquid: coin %q contains a control character", coin)
		}
	}
	return nil
}

// NormalizeCoin trims surrounding whitespace and validates the result. It
// returns the trimmed coin, ready to be put on the wire.
func NormalizeCoin(coin string) (string, error) {
	c := strings.TrimSpace(coin)
	if err := ValidateCoin(c); err != nil {
		return "", err
	}
	return c, nil
}

// ─────────────────────────────────────────────────────────────
// Subscription parameters
// ─────────────────────────────────────────────────────────────

// SubParams carries the optional, channel-specific subscription parameters.
//
// An unset numeric option is the zero value and is omitted from the frame:
// nSigFigs=0 and mantissa=0 are not legal venue values (nSigFigs is 2..5,
// mantissa is 1, 2 or 5), so "unset" and "zero" cannot be confused and no
// pointer types are needed. Fast=false is likewise identical on the wire to an
// absent field (both mean the 20-level book).
type SubParams struct {
	// Interval is the candle interval (ChannelCandle only). One of
	// CandleIntervals().
	Interval string

	// NSigFigs thins the l2Book snapshot to N significant figures
	// (ChannelL2Book only). Legal values are 2..5; zero means unset, i.e. the
	// venue default.
	//
	// Thinning aggregates *and* rounds prices, so a recorder that must keep
	// raw precision should leave it unset (the perp collector spec says
	// explicitly: do not set nSigFigs).
	NSigFigs int

	// Mantissa thins the book to the given mantissa (ChannelL2Book only):
	// legal values are 1, 2 and 5, and it is legal *only* together with
	// NSigFigs=5. Zero means unset.
	Mantissa int

	// Fast requests a 5-level book instead of the default 20 (ChannelL2Book
	// only). It roughly halves the l2Book byte rate.
	Fast bool
}

// validate checks the parameters against the channel they were passed for.
func (p SubParams) validate(ch Channel) error {
	switch ch {
	case ChannelCandle:
		if p.Interval == "" {
			return fmt.Errorf("hyperliquid: candle subscription requires an interval (%s)",
				strings.Join(candleIntervals, ", "))
		}
		if !ValidCandleInterval(p.Interval) {
			return fmt.Errorf("hyperliquid: unsupported candle interval %q (supported: %s)",
				p.Interval, strings.Join(candleIntervals, ", "))
		}
		if p.NSigFigs != 0 || p.Mantissa != 0 || p.Fast {
			return fmt.Errorf("hyperliquid: nSigFigs/mantissa/fast are l2Book-only (channel %s)", ch)
		}
	case ChannelL2Book:
		if p.Interval != "" {
			return fmt.Errorf("hyperliquid: interval is candle-only (channel %s)", ch)
		}
		if p.NSigFigs != 0 && (p.NSigFigs < 2 || p.NSigFigs > 5) {
			return fmt.Errorf("hyperliquid: nSigFigs must be 2..5, got %d", p.NSigFigs)
		}
		if p.Mantissa != 0 {
			if p.NSigFigs != 5 {
				return fmt.Errorf("hyperliquid: mantissa (%d) requires nSigFigs=5, got nSigFigs=%d",
					p.Mantissa, p.NSigFigs)
			}
			if p.Mantissa != 1 && p.Mantissa != 2 && p.Mantissa != 5 {
				return fmt.Errorf("hyperliquid: mantissa must be 1, 2 or 5, got %d", p.Mantissa)
			}
		}
	default:
		if p.Interval != "" || p.NSigFigs != 0 || p.Mantissa != 0 || p.Fast {
			return fmt.Errorf("hyperliquid: channel %s takes no subscription parameters", ch)
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────
// Subscriptions and frames
// ─────────────────────────────────────────────────────────────

// Subscription is one subscription object, i.e. the `subscription` field of a
// subscribe/unsubscribe frame. Fields not relevant to the channel are omitted
// from the JSON.
//
// Use BuildSubscriptions to construct validated subscriptions rather than
// filling this in by hand: it applies the channel/coin/parameter rules, and
// the venue rejects the whole frame if a coin is malformed.
type Subscription struct {
	Type     Channel `json:"type"`
	Coin     string  `json:"coin,omitempty"`
	Interval string  `json:"interval,omitempty"`
	NSigFigs int     `json:"nSigFigs,omitempty"`
	Mantissa int     `json:"mantissa,omitempty"`
	Fast     bool    `json:"fast,omitempty"`
}

// Key returns the identity of the subscription, e.g. "l2Book:BTC" or
// "candle:BTC:1m". Duplicate keys are what the manager deduplicates on.
func (s Subscription) Key() string {
	parts := []string{string(s.Type)}
	if s.Coin != "" {
		parts = append(parts, s.Coin)
	}
	if s.Interval != "" {
		parts = append(parts, s.Interval)
	}
	return strings.Join(parts, ":")
}

// BuildSubscriptions validates the request and builds one Subscription per
// coin.
//
// coins must be empty for ChannelAllMids (the subscription is global) and
// non-empty otherwise; duplicates are rejected rather than silently
// deduplicated, because a duplicate is a caller bug that would otherwise cost
// connection budget for nothing.
func BuildSubscriptions(ch Channel, coins []string, p SubParams) ([]Subscription, error) {
	if !IsSubscribable(ch) {
		return nil, fmt.Errorf("hyperliquid: unknown channel %q (subscribable: %s)",
			ch, channelList())
	}
	if err := p.validate(ch); err != nil {
		return nil, err
	}

	if ch == ChannelAllMids {
		if len(coins) != 0 {
			return nil, fmt.Errorf("hyperliquid: channel %s takes no coin (got %d)", ch, len(coins))
		}
		return []Subscription{{Type: ch}}, nil
	}

	if len(coins) == 0 {
		return nil, fmt.Errorf("hyperliquid: channel %s needs at least one coin", ch)
	}

	subs := make([]Subscription, 0, len(coins))
	seen := make(map[string]struct{}, len(coins))
	for _, raw := range coins {
		coin, err := NormalizeCoin(raw)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[coin]; dup {
			return nil, fmt.Errorf("hyperliquid: duplicate coin %q", coin)
		}
		seen[coin] = struct{}{}

		subs = append(subs, Subscription{
			Type:     ch,
			Coin:     coin,
			Interval: p.Interval,
			NSigFigs: p.NSigFigs,
			Mantissa: p.Mantissa,
			Fast:     p.Fast,
		})
	}
	return subs, nil
}

func channelList() string {
	names := make([]string, 0, len(subscribableChannels))
	for _, c := range subscribableChannels {
		names = append(names, string(c))
	}
	return strings.Join(names, ", ")
}

// request is the wire shape of a subscribe/unsubscribe frame. The venue sends
// one subscription per frame.
type request struct {
	Method       string       `json:"method"`
	Subscription Subscription `json:"subscription"`
}

// SubscribeFrame builds the {"method":"subscribe",...} frame for one
// subscription.
func SubscribeFrame(sub Subscription) ([]byte, error) {
	return json.Marshal(request{Method: "subscribe", Subscription: sub})
}

// UnsubscribeFrame builds the {"method":"unsubscribe",...} frame for one
// subscription. The venue matches it against the original subscribe message,
// so the subscription object must be identical to the one subscribed with.
func UnsubscribeFrame(sub Subscription) ([]byte, error) {
	return json.Marshal(request{Method: "unsubscribe", Subscription: sub})
}

// PingFrame is the application-level keepalive frame. The venue answers it
// with {"channel":"pong"}. This is not a WebSocket control ping — it is an
// ordinary text message.
func PingFrame() []byte { return []byte(`{"method":"ping"}`) }
