# Changelog

All notable changes to this module. Format follows Keep a Changelog. Unreleased and untagged; no
entry claims a date.

## [0.10.0] — unreleased (branch feat/hardening)

Baseline v0.9.0. `connector.Version` = `0.10.0`. Additive API: 174 new exported symbols, 0
removed. One signature change: `hyperliquid.NewInfoClient` now takes
`(baseURL string, opts ...InfoOption)` — variadic, source-compatible. No new dependency.
Verification is offline against loopback fake servers only; no live venue.

### Fixed

- websocket: `Close()` no longer hangs when the first dial failed, and no longer holds the manager
  mutex across the close handshake.
- websocket: unsynchronized `cancel`/`done` race removed.
- websocket: reconnect latency after a stable drop: median drop→next-accept 970.33ms before,
  685µs after (max 974.07ms → 11.67ms; loopback server, `ReconnectInterval` 1000ms; H1
  report §4).

### Added

**websocket** — all optional, zero value keeps v0.9.0 behavior:

- `WSOptions.DialGate func(ctx context.Context) error` — consulted before every dial attempt;
  a supervisor can veto or pace dials.
- `WSOptions.SendGate func(ctx context.Context, n int) error` — consulted once per outbound
  frame the websocket package writes (`n` = payload bytes); an error skips that frame only.
- `WSOptions.NoReconnect bool` — after a drop the status stays disconnected and the supervisor
  owns the redial.
- `WSOptions.Logger`.

**Frame provenance and loss accounting** — same shared types for every venue, so one hook serves
all of them:

- every market event carries `FrameID` and `ConnID` (JSON `frame_id`, `conn_id`). `FrameID` is a
  per-connection frame counter stamped before the decode queue and never reset across reconnects.
  It counts frames, not events: acks, pongs, unsubscribed symbols and dropped frames consume an ID
  with no event. A `FrameID` jump inside one `ConnID` is local loss or a frame that produced no event (confirm with
  the drop hook or `Stats()`); a `ConnID` change is a gap of
  unknown size. `FrameID` 0 marks an event synthesized by the connector (Binance depth book seeded
  from REST).
- shared types in the root package: `FrameStats` (`FramesRead`, `FramesDropped`, `FramesDrained`,
  `QueueLen`, `QueueCap`, `QueueHighWater`, `ConnID`, `Disconnects`), `DroppedFrame` (`FrameID`,
  `ConnID`, `At`, `Cause`) and `DropCause` with `DropQueueFull` and `DropDrainedOnDisconnect`.
- per venue: `WSHyperliquid.Stats()`, `WSHyperliquid.SetOnFrameDropped`,
  `WSHyperliquid.ConnID`; `WSBinance.Stats(mkt)` (returns `FrameStats, bool`),
  `WSBinance.SetOnFrameDropped`; `WSPolymarketMarket` and `WSPolymarketRTDS` each `Stats()` and
  `SetOnFrameDropped`.
- the drop hook runs on the goroutine that discarded the frame (read loop or disconnect path): it
  must be cheap, with no blocking and no I/O.
- identity: `FramesRead == delivered + FramesDropped + FramesDrained` (+ at most one frame in
  flight).
- queue depth knobs, same values as before but now explicit: `hyperliquid.Options.EventQueueSize`
  (default 8192), `binance.Options.EventQueueSize` (default 8192),
  `WSPolymarketMarket.SetEventQueueSize` (16384), `WSPolymarketRTDS.SetEventQueueSize` (8192);
  RTDS uses 4 dispatcher workers.

**hyperliquid** — account channels: on an account-bearing connection a full queue no longer drops
the frame. The connection fails and reconnects, and a `HyperliquidGapEvent` (fields
`AccountChannels`, `Cause`, `LostFrames`, `PrevConnID`, `ReceivedAt`, `SeqID`; causes
`HyperliquidGapQueueOverflow`, `HyperliquidGapDisconnect`) is delivered before any frame of the
new generation. Market-only connections are unchanged: drops are counted, no gap event.
Also `TimestampSynthesized`, `Trade` `BatchIndex`/`BatchLen`, `SubscribeMany` + `SubscribeReport` +
`SubscriptionAck` + `SetOnSubscriptionAck` + `Options.SubscribePaceMs`.

**binance** — gap-triggered depth re-seeds are gated: one re-seed in flight per book,
`Options.DepthReseedMinInterval` (default 1s per symbol) and `Options.DepthReseedPerMin` (default
300 per minute, shared by all symbols and both markets of one `WSBinance`). HTTP 429/418 pauses all
gap re-seeds for `Retry-After` (default 60s). `WSBinance.DepthReseedStats()` returns
`DepthReseedStats` (`Triggers == Fetched + Coalesced + Deferred`, plus `Failed` and `Paused`).
Initial per-connect seeding is not gated.

**polymarket** — queued frames are still delivered after a disconnect (never drained); frames of an
asset that is mid-resubscribe are no longer dropped at the asset gate.
`WSPolymarketMarket.SetReconnectOnDrop(bool)`, default off: after queue-full drops it rebuilds the
book via `ReconnectMarket`, at most once per 10s per overload episode.

**logging** — `Options.Logger` (hyperliquid, binance), `WSOptions.Logger` (websocket), `SetLogger`
(Polymarket market, RTDS, user WS). Nil means `slog.Default()` resolved at call time, so messages
and behavior are unchanged; a venue logger is also given to its websocket layer.

**Also in this branch** (from the H0 merge of the info/funding branch): `InfoClient` additions
`FundingHistory`, `FundingHistoryAll`, `PredictedFundings`, `MetaAndAssetCtxsDex`, `MetaDexRaw`,
and `InfoOption` helpers `WithHTTPClient`, `WithRateLimit`, `WithRetry`, `HTTPError.RetryAfter`,
`IsRateLimited`.

### Changed

Behavior differs from v0.9.0, API-compatible. By design:

1. Every event carries `frame_id` and `conn_id` JSON keys.
2. `HyperliquidGapEvent` appears on account streams after a reconnect.
3. Account-connection queue overflow fails the connection, so a disconnect or flap is visible.
4. `hyperliquid.DefaultOptions()` returns `EventQueueSize: 8192`.
5. Binance re-seed gate: far fewer REST calls, book repair slightly later.
6. Polymarket mid-resubscribe frames are delivered.
7. RTDS delivery order is documented (order by venue `Timestamp`; `FrameID` only for loss
   detection).
8. `connector.Version` is `0.10.0`, so the outbound User-Agent changes.

### Known limits

- Polymarket market `Stop()` discards queued frames uncounted.
- `WSOptions.SendGate` does not cover writes made directly on `Conn()` by the venue packages.
- `snapshotDepthBook` holds the book mutex across its REST GET; the dispatcher is blocked during a
  fetch.
- User streams still apply inline backpressure: no queue, no counters.
- A quiet-market stale tail and a failed re-seed are logged only.
- After about 10 consecutive drops the reconnect backoff reaches 20-30s, so a drop-heavy network
  looks disconnected for that long.
- Hyperliquid frame counters are a separate implementation from `internal/framestat` (same
  semantics, same shared types).
- The default 300/min re-seed budget is a conservative guess.
- Everything was verified offline against loopback fake servers: no live venue, soak runs of 60s
  (not days), no resource-limit tests.

### Not verified

No live venue was used: all results are from loopback fake servers in this repo.

Earlier releases: see git tags (v0.8.1, v0.9.0).
