package hyperliquid

import "time"

// DecodeOutcome classifies one market-frame decode observation reported through
// Options.OnMarketFrameDecoded.
//
// It describes the DECODE, not the delivery: a frame whose payload decoded but
// that was then filtered out (unsubscribed coin) is DecodeSucceeded — the
// decoder did its job — while activeAssetCtx stops one step early (it checks the
// subscription before decoding the inner ctx) and therefore reports
// DecodeSkipped for that partial decode. Never treat skipped as success: the
// payload was not fully decoded.
type DecodeOutcome string

const (
	// DecodeSucceeded: envelope and payload decoded. Exactly one observation, at
	// the point the payload unmarshal returned (for trades: the whole batch,
	// before any print is converted or filtered).
	DecodeSucceeded DecodeOutcome = "ok"
	// DecodeFailed: envelope or payload unmarshal failed. Channel is empty when
	// the envelope itself could not be parsed (the channel is then unknown).
	DecodeFailed DecodeOutcome = "error"
	// DecodeSkipped: a partial decode stopped by the existing early subscription
	// check (activeAssetCtx); the inner ctx was never decoded.
	DecodeSkipped DecodeOutcome = "skipped"
)

// FrameDecodeTiming is one true decode-timing observation for one processed
// wire frame. It carries the frame's original identity and receive time, the
// local clock samples around the decode, and what the decode did.
//
// StartedAt is sampled immediately before the envelope json.Unmarshal and
// CompletedAt immediately after the relevant payload json.Unmarshal returns;
// the difference therefore covers envelope + payload decoding and excludes
// queue waiting before the frame and the typed subscriber work after it. On
// activeAssetCtx it includes the existing subscription lookup between the outer
// and inner unmarshals; this is elapsed decoder-path time, not pure JSON CPU time.
// The callback itself runs on the dispatcher decode goroutine, synchronously, so
// the time it spends is NOT part of CompletedAt - StartedAt.
//
// There is one observation per processed wire frame, never one per typed event
// or per trade print.
type FrameDecodeTiming struct {
	FrameID     uint64
	ConnID      uint64
	ReceivedAt  time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	// Channel is the native channel of the frame; empty when the envelope could
	// not be decoded.
	Channel Channel
	Outcome DecodeOutcome
}

// observeMarketDecode reports one decode observation. It returns immediately —
// no clock sample, no allocation, no callback — when the option is nil, which
// is what keeps the default path free of added time.Now calls.
func (h *WSHyperliquid) observeMarketDecode(f frame, channel Channel, started time.Time, outcome DecodeOutcome) {
	cb := h.opts.OnMarketFrameDecoded
	if cb == nil {
		return
	}
	cb(FrameDecodeTiming{
		FrameID:     f.id,
		ConnID:      f.connID,
		ReceivedAt:  f.rx,
		StartedAt:   started,
		CompletedAt: h.decodeNow(),
		Channel:     channel,
		Outcome:     outcome,
	})
}
