# Exchange connector audit (per exchange)

Audited 2026-11-05 against the `exchange-integration` skill. Evidence: code read in this repo, `go build`/`go vet` (clean), `go test -count=1 ./...` (all packages ok). Nothing was run against a live venue in this audit.

Target bar: **market-making speed, lightweight**. That means the order path does not add retries or sleeps on the hot path, the feed never stalls silently, and every order has a measured latency.

## Summary

| Exchange | Market WS | Orders | Order transport | Verdict for MM |
|---|---|---|---|---|
| Hyperliquid | ✅ | ✅ place/cancel/schedule/leverage (uncommitted) | REST (`/exchange`) | Closest to ready. Needs WS-post, latency metric, testnet live test |
| Binance USDS-M | ✅ | ✅ place/batch/cancel/countdown | REST only | Ready for REST MM after live test. No WS-order path |
| Polymarket | ✅ | ✅ place/cancel (live cmd exists) | REST (CLOB) | Live test is **ungated** and places real orders. Auth must be checked against V2 |
| Binance spot | ✅ (market WS in `pkg/binance`) | ✅ place/cancel/cancel-all/queries (L0 only) | REST; no batch on spot | `pkg/binance/spot` + `bnspot` adapter. WS API order path not built |
| Aster perps | ✅ (public streams probed live) | ✅ place/batch/cancel/countdown (L0 only) | REST (same client as Binance futures) | `pkg/aster/futures` + `asfutures` adapter. No live order test run; no testnet documented. Spot not built |

## Per-exchange audit

| Check | Hyperliquid | Binance USDS-M (`pkg/binance/futures`) | Polymarket (`pkg/polymarket`) |
|---|---|---|---|
| Market WS (subscribe / unsubscribe) | `pkg/hyperliquid/hyperliquid.go`, `channels.go`: channel registry, subscription frames | `pkg/binance/binance.go`: `writeSubscribe` (`SUBSCRIBE` method) | `market.go`: incremental `operation:subscribe/unsubscribe`; reconnect resends full set |
| Reconnect and ping | Shared `pkg/websocket`: reconnect + backoff + flap detection. App ping 50 s, WS ping 20 s | Shared base. WS ping 5 s | Shared base. Ping 5 s. Data-stale watchdog 30 s, but disabled for market WS (quiet markets); consumer runs its own silence watchdog |
| Read limit (per message) | 1 MiB (`defaultReadLimitBytes`) | 1 MiB | `Config.ReadLimitBytes`, default 1 MiB |
| Connection limits (24 h lifetime, 200 streams/conn) | ❌ not found in code | ❌ not found in code | not applicable / not found |
| Inbound message rate limit (10 msg/s on Binance) | ❌ not found | ❌ not found | ❌ not found |
| Place single | ✅ `exchange/orders.go` `Place([]Order)` | ✅ `orders.go` `NewOrder` | ✅ `PlaceLimitOrders` via `clob.go` (verify endpoint version) |
| Place batch | ✅ `Place` takes a slice | ✅ `BatchOrders` | ⚠️ signature takes a slice; wire batch (`POST /orders`) not confirmed |
| Cancel by id | ✅ `CancelByOid`, `CancelByCloid` | ✅ `CancelOrder` (clientOrderID) | ✅ `CancelOrders` |
| Cancel batch / all | ⚠️ batch via slice; no cancel-all helper | ✅ `CancelBatch`, `CancelAll`, `CountdownCancelAll` | ⚠️ no cancel-all helper found in connector |
| Open orders | ✅ `OpenOrders` / `OpenOrdersDex` | ✅ `OpenOrders` | ⚠️ not audited in detail |
| Fills | ✅ `userFills` user channel (commit `6f12792`) | ⚠️ user-data stream exists (`userstream.go`); REST `userTrades` not found in `orders.go` | ⚠️ user WS `/ws/user` exists in `user.go`; verify event shape |
| Orders over WebSocket | ❌ REST only (WS `post` documented, not used) | ❌ REST only | ❌ REST only (WS order placement not documented by Polymarket) |
| Quota / weight tracking | ⚠️ 429 handling on info reads only | ✅ records `X-MBX-USED-WEIGHT-1M`, `X-MBX-ORDER-COUNT-10S/1M`, exposed via `Usage()` | ❌ not found |
| 429 / 418 handling | Info reads: 3 retries, 0.5–2 s backoff (adds up to 3.5 s). Exchange writes: no retry (correct) | `IsRateLimited` detected; **no backoff or throttle**, caller decides | Not audited |
| Error classes | ✅ `StatusError`, `uncertainError`, rate-limit classifier | ✅ `APIError`, `IsUncertain`, `IsRateLimited`, `IsUnknownOrder` | ⚠️ not audited |
| Order latency measured | ❌ none in exchange package | ❌ none | ⚠️ market-data latency tracker only (`latencyTracker`), not order ack latency |
| Testnet / safety default | ⚠️ testnet host supported; check default | ✅ `Network` config, testnet `demo-fapi.binance.com` | ❌ no network switch; test cmd uses live CLOB |
| Live test cmd | ❌ none (`cmd/test_hyperliquid` is market-data only) | ❌ none (`cmd/test_binance` has no order calls) | ⚠️ `cmd/test_ws` places and cancels a **real** order. **No confirm flag and no dry-run** |
| Unit tests | ✅ 12 test files, all pass | ✅ `orders_test.go`, `client_test.go`, `userstream_test.go`, pass | ✅ `reconnect_test.go` etc., pass (race not run in this audit) |
| Working-tree state | ⚠️ 8 modified + 9 untracked files, not committed | ✅ committed (branch `feat/mm-exec`) | ✅ committed |
| Hot-path cost | Signing via sonirico per action; no retry on writes | Plain `net/http`, HMAC per request, no alloc audit | Not audited |

## Findings, ranked

1. **Polymarket live test places real orders with no gate.** `cmd/test_ws/main.go` calls `PlaceLimitOrders` on the live CLOB with flags only for side, price, size. Add `-live` plus an env confirmation, and a dry-run default. Do this before anyone runs it again.
2. **No order-latency metric on any exchange.** Add send→ack and ack→WS-event timestamps for every order, with p50/p95 in the live cmd. Nothing measures this today.
3. **No WebSocket order path anywhere.** Hyperliquid (`post`) and Binance spot (`order.place`, `order.cancel`) both support it. REST-only costs a full HTTPS round trip per order.
4. **Hyperliquid uncommitted work.** Eight modified and nine untracked files in `pkg/hyperliquid/exchange`, including `dex.go`, `unified.go`, `hip3*_test.go`. Tests pass, but nothing is tagged. Commit with explicit paths once reviewed (AGENTS.md rule).
5. **Read retries sit on the trading path.** Hyperliquid `postInfo` retries 429s for up to about 3.5 s. A market maker that reads `openOrders` or `clearinghouseState` before quoting will stall. Make the retry budget configurable and set it to zero on the quoting path.
6. **Binance futures has no throttle.** It records weight but never slows down. Add a token-bucket for weight and order count, sized from `exchangeInfo.rateLimits`, and back off on 429 and on 418.
7. **Polymarket auth may be on the V1 scheme.** Docs now describe V2 (pUSD, `positionId`, session keys). Check `auth.go` and `signing.go` against `docs.polymarket.com` before any live order.
8. **Binance user-data stream has no fill cross-check.** Confirm `ORDER_TRADE_UPDATE` fills match REST `userTrades` on testnet.
9. **Binance spot and Aster are absent.** Not a defect, but the build list below depends on it.

## What to build next (ordered)

1. **Shared order-latency and gated-live harness** in `internal/`: one timer type, one gate (`-live` + env var), testnet default. Used by every exchange cmd. Covers Finding 1 and 2.
2. **Hyperliquid: commit and harden.** Review the uncommitted diff, commit by explicit path, make read retries configurable (Finding 5), add `cmd/test_hl_orders` (testnet place → read → cancel, prints latency).
3. **Hyperliquid WS post for orders** (`method: "post"`, `request.type: "action"`). Keep REST as fallback. Compare latency in the harness.
4. **Binance USDS-M: throttle + live test.** Token bucket on weight and order count (Finding 6), and `cmd/test_binance_orders` on `demo-fapi` with place → cancel → latency.
5. **Polymarket: V2 auth check, then gate the test.** Verify against the current docs (Finding 7), then add the gate (Finding 1).
6. **Binance spot connector** (`pkg/binance/spot`): REST plus WS API (`order.place`, `order.cancel`, `order.test`). This is the candidate for the lowest-latency Binance order path.
7. **Aster connector** (`pkg/aster`): follows the Binance futures layout from `exchange-api-reference/references/aster.md`. Reuse `pkg/binance/futures` signing where the APIs match.

## Notes

- The audit used my own reading of the code, not a separate model. I did not run `gpt-6-astra` (listed in `~/.hermes/config.yaml` on Codex OAuth) because the delegation tool does not let me pin a model for one task. If you want an independent second pass, run it with that model and diff the table.
- This file is untracked. AGENTS.md says to commit only with explicit paths and not to stage untracked user files. Nothing was committed.

## Verification status (per feature, per network)

Levels: **L0** unit test (no network). **L1** read-only live. **L2** live on testnet.
**L3** live on mainnet, small size. **L4** production soak (fills and fees over time, monitored).
"Tested" below means the level named, on the commit named. Working-tree claims are
marked `wt` until the work is committed, then replaced with the SHA.

Commit basis: `853074b` (HEAD at the time of this run) plus working-tree changes (`wt`).
Date: 2026-11-05. Evidence is the run output recorded in this section.

| Feature | Commit | Level | Network | Evidence |
|---|---|---|---|---|
| HL REST place / cancel by oid / batch / cancel by cloid | wt | L3 | mainnet | `test_hl_orders -network mainnet -live -confirm-mainnet -samples 1`, all PASS, orders resting then cancelled |
| HL REST place / cancel by oid / batch / cancel by cloid | wt | L2 | testnet | `test_hl_orders -network testnet -live -samples 3`, 1 round trip before the request quota ran out, then 1 sample after reserve |
| HL WS orderUpdates correlation (open / canceled) | wt | L3 | mainnet | ack→ws 80 ms (open), 28 ms (canceled), matched by oid |
| HL IOC fill round trip (entry, reduce-only exit, userFills, flat) | wt | L3 | mainnet | `test_hl_fill -network mainnet -live -confirm-mainnet`, 9/9 PASS, fee 0.009721 USDC, closedPnl -0.000140 |
| HL IOC fill round trip | wt | L2 | testnet | `test_hl_fill -network testnet -live`, 9/9 PASS, fee 0.010092 USDC |
| HL IOC no-fill does not rest | wt | L3 | mainnet | PASS in the fill run (resting=false, openOrders=0) |
| HL reserveRequestWeight (12,000 weight, ~6 USDC) | wt | L2 | testnet | status ok; **not** run on mainnet (cost rejected by the user) |
| HL account mode / spot balance reads | wt | L1 | mainnet and testnet | unifiedAccount on mainnet with 101.76 USDC spot; default on testnet |
| HL market data WS (existing) | 853074b | L1 | mainnet | existing `pkg/hyperliquid` tests; no fresh live run this session |
| HL HIP-3 / DEX-scoped execution | wt | L0 | none | unit tests only |
| HL user stream (userFills, orderUpdates) | wt | L2 | testnet | fill test read userFills; the WS fill event itself was not checked |
| Aster perps execution (REST place/batch/cancel/countdown, user stream) | d6ab3d9 | L0 | none | unit tests with httptest only (signing, routes, caps, error classes, `/ws/<key>` dial) |
| Aster perps public market WS | d6ab3d9 | L1 | mainnet | 20 s read-only probe 2026-11-05: bookTicker 272, aggTrade 6, depth 190, kline 6 events for BTCUSDT/ETHUSDT |
| Binance futures cancel/get by order id | eee4f03 | L0 | none | httptest unit tests |
| Binance spot REST + user stream | (this commit) | L0 | none | httptest and fake WS tests; public dry run of `test_binance_spot_orders` only |
| Binance futures and spot live order tests | — | not run | — | tools written, never run with `-live` |
| Polymarket execution | — | not run | — | out of scope for this run |

**Not tested at any level**
- Fee tiers beyond the observed fees above. Only one fill per side was observed.
- Partial fills, cancel races with a fill, and a rejected reduce-only order.
- Reconnect during an order, and WS-only fill events (the fill test checks REST userFills).
- Batch size cap (50). Still unverified in the docs.
- L4 anything: no production soak has run.

**Test-state notes**
- Testnet: the account held an open BTC long (0.00047) and ETH long (0.0115) from earlier runs. Both were closed on request before the fill test, so the account is flat.
- Mainnet: the account is flat, with 101.76 USDC in spot and 0 perp equity.
- Mainnet request quota: each live run consumes address request weight. Reserve was not used on mainnet.

## Aster perps notes (d6ab3d9, 2026-11-05)

- **Design:** `pkg/aster/futures` is configuration over `pkg/binance/futures` (Aster serves the legacy Binance futures layout), not a fork. Differences: host `fapi.asterdex.com`, positions at `/fapi/v2/positionRisk`, user stream at `wss://fstream.asterdex.com/ws/<listenKey>` with no events filter. The Binance package gained two additive options for this (`Config.PositionRiskPath`, `UserStreamConfig.KeyInPath`).
- **Not tested live:** order place, batch, cancel, countdown, positions, user-stream events. `cmd/test_aster_orders` is written and its refusal paths and dry run ran, but it has never run with `-live`.
- **Assumptions to confirm on the first live run:** Aster echoes `clientOrderId` in batch place/cancel responses; the `/ws/<listenKey>` ORDER_TRADE_UPDATE payload matches the Binance shape (it matches the doc, not yet seen live); `countdownTime` 0 stops the timer.
- **No testnet** is documented, so every live run is mainnet and needs `-live -confirm-mainnet` plus `ASTER_API_KEY` / `ASTER_API_SECRET` in the environment.
- **Not built:** cancel by exchange order id (same gap as Binance futures), Aster spot (`sapi.asterdex.com`, v3 wallet signing), WS order placement (none in the docs checked).
- **Limits from the docs, not enforced in code beyond what the shared client does:** WS connection 24 h, 10 inbound msg/s, 200 streams per connection; 429 backs off, repeated 429 leads to a 418 IP ban.
