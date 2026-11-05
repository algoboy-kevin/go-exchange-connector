# Exchange feature matrix (venue × market)

## Binance spot "testnet" remap — 2026-11-05 (no live run)

- Project `testnet` for Binance spot now means Binance **Spot Demo Mode** (`demo-api.binance.com`, `demo-ws-api.binance.com`, `demo-stream.binance.com`; official doc cached line 132362, llms-full sha256 `7fe8634d231eacc5564bef1995fe45c1bbc52ea7b7072cc50e3b0d06ad05c68e`). The label is a project alias; legacy Spot Testnet is not used by this repo.
- The "Testnet authentication failed with -2015" row below targeted the legacy testnet and does NOT establish Demo Mode key invalidity. No new live run was performed.

## Latest Binance validation — November 5, 2026

- BN-perp: place single, place batch (2), cancel by client id and batch cancel, open-order reads, and matching NEW/CANCELED user-stream events are **P on testnet only (L2)**. Three order-status queries confirmed canceled with zero executed quantity. Mainnet signed reads and user-stream connection worked, but an order was rejected for insufficient margin (-2019); mainnet execution is not proven.
- BN-spot: signed mainnet open-order/account reads worked, but insufficient quote funds prevented placement. Testnet authentication failed with -2015. Neither spot environment has a proven order round trip.
- Futures checker uses the proven path-style private stream `/private/ws/<listenKey>` via `KeyInPath: true`; the query URL returned 404. Native default remains query-style for backward compatibility, so configure this explicitly in other consumers.
- All requested runs used verified EC2 egress. Full detail and the historical evidence boundary are in `EXCHANGE_AUDIT.md`.

Helicopter view of operations per venue and market, for the shared-contract design.
Built 2026-11-05 from the docs hub (`exchange-docs-hub/catalog.json`, 20 sources). Every
downloaded file matched its recorded `content_sha256`, or was baselined on this run.

## Columns

| Code | Venue | Market | Package (repo) | Primary doc in hub |
|---|---|---|---|---|
| **HL-perp** | Hyperliquid | perpetuals | `pkg/hyperliquid`, `pkg/hyperliquid/exchange` | `hyperliquid llms-full` (part 1 of 2) |
| **HL-spot** | Hyperliquid | spot (same API, asset id `10000 + index`) | none for execution (`unified.go` reads balance) | same as HL-perp |
| **BN-perp** | Binance | USDS-M futures | `pkg/binance`, `pkg/binance/futures` | `binance llms-full` |
| **BN-spot** | Binance | spot | `pkg/binance` (market data only) | `binance llms-full` |
| **AS-perp** | Aster | futures (Binance-style, `fapi`) | `pkg/aster/futures` (thin config over `pkg/binance/futures`), `pkg/contract/asfutures` | `aster markdown` (API doc) |
| **AS-spot** | Aster | spot (`sapi`, `/api/v3`) | none | `aster-finance-spot-api-v3.md` (GitHub, added 2026-11-05) |
| **PM-clob** | Polymarket | prediction market (CLOB, V2) | `pkg/polymarket` | `polymarket llms-full` |
| **PM-perp** | Polymarket | perpetuals (`api.perpetuals`, `ws.perpetuals`) | none | `perps/overview`, `perps-openapi.json`, `asyncapi-perps.json` (added 2026-11-05; not fetched in full) |

## Status codes

| Code | Meaning |
|---|---|
| **R** | implemented in this repo (not necessarily live-tested) |
| **P** | proven live (only HL info-over-WS `post`, median 135 ms) |
| **D** | documented in the hub text, not implemented here |
| **X** | not offered by this venue/market in the docs checked |
| **?** | not verified: docs silent, ambiguous, or not in the checked text |

## Coverage limits

- HL and AS `llms-full.txt` are part 1 only. Cells depending on later pages are `?`.
- AS-spot: the spot API v3 markdown lists the endpoints, but its order-type and TIF pages
  are not fetched. `X` below means "absent from the endpoint list", which is strong but not
  a full negative check.
- PM-perp: the perps overview and specs are in the hub, but the 102-page perps reference is not
  fetched. Most PM-perp cells are `D` (existence) with `?` for the details.
- PM `heartbeat` in the repo is the RTDS ping (`pkg/polymarket/rtds.go`), not the order heartbeat.

## Market data

| Operation | Contract | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Subscribe / unsubscribe (market WS) | `Subscribe`/`Unsubscribe` | R | D (`coin` `PURR/USDC`) | R | R | P `aster.MarketOptions` + `binance.NewWithOptions` | D `sstream` (`/ws/<stream>`) | R | D (`req: sub`/`unsub`, `chs`) |
| Book snapshot | `Book` | D `l2Book` | D | D depth | R (market WS) | P (local book, 51/52 levels live) | D `/api/v3/depth` | D | ? (`bbo`, depth not checked) |
| Trades stream | `Trades` | D `trades` | D | D | D | P `aggTrade` | D | R | D `trades::<id>` |
| Reconnect, ping, stale watchdog | internal | R | R | R | R | R (shared BaseWebSocket) | D (24 h) | R | D (ping ~25 s; stale after ~65 s) |

## Orders (REST)

| Operation | Contract | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Place single limit | `PlaceOrder` | R `Place` | D (same action) | R `NewOrder` | R `NewOrder` | R `NewOrder` | D `POST /api/v3/order` | R `PlaceLimitOrders` | ? (REST `api.perpetuals`) |
| Place batch | `PlaceOrders` | R (one action) | D | R `BatchOrders` | X (none; adapter loops, cap 5) | R `BatchOrders` (max 5) | X (not in endpoint list) | R (loop; CLOB has `PostOrders`) | ? |
| Place market | `PlaceMarketOrder` | ? | ? | ? | D | ? | D (`MARKET`, `quoteOrderQty`) | R | ? |
| Cancel by exchange id | `CancelOrder` | R `CancelByOid` | D | R `CancelOrderByID` | R `CancelOrder` | D | D `DELETE /api/v3/order` | R `CancelOrders` | ? |
| Cancel by client id | `CancelOrder` (ref) | R `CancelByCloid` | D | R `CancelOrder` | R `CancelOrder` | R `CancelOrder` | D (`newClientOrderId`) | ? | ? |
| Cancel batch | `CancelOrders` | R (slice) | D | R `CancelBatch` | D `orderList.cancel` | R `CancelBatch` (max 10) | X | R | ? |
| Cancel all on a symbol | `CancelAll` | ? | ? | R `CancelAll` | R `CancelAll` | R `CancelAll` | X | D `cancel-all` | ? |
| Amend / modify | `Amend` | D `modify`, `batchModify` | D | ? | D `order.amend.keepPriority` | ? | X | ? | ? |
| Cancel-replace (atomic) | `CancelReplace` | ? | ? | ? | D `order.cancelReplace` | ? | X | ? | ? |
| Open orders | `OpenOrders` | R | ? | R | R | R | D `GET /api/v3/openOrders` | D `list_open_orders` | ? |
| Order status | `GetOrder` | D `orderStatus` | D | R `GetOrder` | R `GetOrder` | D | D `GET /api/v3/order` | D `get_order` | ? |
| Fills (REST history) | `Fills` | D `userFills` | D | D `userTrades` | R `MyTrades` | D `userTrades` | D `GET /api/v3/userTrades` | D `list_account_trades` | D (`fills` channel; REST not checked) |

## Orders (WebSocket)

| Operation | Contract | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Place order over WS | `PlaceOrderWS` (optional) | D (`post` action, unproven) | D | D `ws-fapi` `order.place` | D `order.place` | X | X (not in spot endpoint list) | X | D (overview: "send trading actions over a persistent connection"; op types not checked) |
| Cancel over WS | `CancelOrderWS` (optional) | D (unproven) | D | D | D `order.cancel` | X | X | X | ? |
| Info over WS | `InfoWS` (optional) | P (`meta`, 135 ms median) | D | ? | ? | ? | ? | X | D (`req: post`, `op.type` ping/auth) |
| Auth over WS | `Authenticate` (optional) | X (none; signing per action) | X | D (listenKey) | D (listenKey) | D (listenKey) | D (listenKey) | D (L2 HMAC headers) | D (`op.type: auth`, proxy + secret) |
| User stream (orders, fills) | `SubscribeUser` | R `orderUpdates`, `userFills` | D | R `userstream.go` | R WS API `userDataStream.subscribe.signature` | R `aster.NewUserStream` (`/ws/<key>`) | D listenKey `wss://sstream.asterdex.com/ws/<key>` | R `user.go` | D (`orders`, `fills`, `portfolio` channels) |

## Order options

| Option | Contract field | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Post-only | `PostOnly` | R (`Alo`) | D | D (GTX) | R (LIMIT_MAKER) | R (GTX) | D (post-only page, not fetched) | D | D (`po` field; rejects `post_only_rejected`) |
| IOC | `TIF` IOC | R | D | D | R | R | ? | D (FAK) | ? |
| FOK | `TIF` FOK | ? | ? | D | D | D | ? | D | ? |
| GTD / expiry | `TIF` GTD + `Expiry` | ? | ? | X | X | ? | ? | D | ? |
| Reduce-only | `ReduceOnly` | D | X | D | X | R | ? | X | ? |
| Trigger / stop | `Trigger` | D | ? | D `STOP_MARKET` | D | D `STOP_MARKET` | D `STOP`, `STOP_MARKET`, `TAKE_PROFIT_MARKET` | X | ? |
| Trailing stop | `Trailing` | D | ? | ? | ? | D `TRAILING_STOP` | X | X | ? |
| TWAP / algo | `TWAP` (optional) | D `twapOrder` | D | ? | ? | X | X | X | ? |
| Builder code | `Builder` (optional) | D | D | ? | ? | ? | D (`builder`, `feeRate`, spot builder page) | D | ? |

## Dead-man switch and heartbeat

| Operation | Contract | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Dead-man switch (server cancels after timeout) | `DeadManSwitch` (optional) | R `ScheduleCancel` (global) | ? | R `CountdownCancelAll` (per symbol) | ? | R `CountdownCancelAll` (per symbol) | X | X | ? |
| Order heartbeat (client keepalive for orders) | `Heartbeat` (optional) | X | X | X | X | X | X | D `POST /v1/heartbeats` (10 s) | ? |

Note: AS `countdownCancelAll` is documented as repeated heartbeats: a dead-man switch the
client renews. PM `heartbeats` is a separate order-keepalive endpoint. Keep them as separate
contracts.

## Account and risk

| Operation | Contract | HL-perp | HL-spot | BN-perp | BN-spot | AS-perp | AS-spot | PM-clob | PM-perp |
|---|---|---|---|---|---|---|---|---|---|
| Positions | `Positions` | R | X | R `PositionRisk` | X | R `PositionRisk` (v2) | X | D (Data API) | D (`portfolio` channel) |
| Balances | `Balances` | R `AccountState` | R `SpotUSDCBalance` | D `fapi/v2/balance` | R `Account` | D | D `GET /api/v3/account` | D | D (`portfolio.margin`, `withdrawable`) |
| Set leverage | `SetLeverage` (optional) | R `UpdateLeverage` | X | R | X | R | X | X | ? |
| Margin mode | `SetMarginMode` (optional) | R (`isCross`) | X | R | X | R | X | X | D (`invalid_margin_mode` error exists) |
| Position mode | `SetPositionMode` (optional) | X | X | R | X | R | X | X | ? |
| Clock sync | `SyncTime` | X | X | R | R | R | D `GET /api/v3/time` | X | ? |
| Commission / fee tier | `Fees` | ? | ? | ? | ? | ? | D `GET /api/v3/commissionRate` | D (fees page) | D (`fee_tier` in portfolio) |
| Rate-limit state | `Usage` | R (info 429 retry) | ? | R `Usage()` | R `Usage()` | R `Usage()` | D | ? | ? |

## Venue-specific (no shared contract)

| Operation | Venue | Where it lives |
|---|---|---|
| Matching-engine restart / 2 min post-only window | PM-clob | PM native client |
| Closed-only mode | PM-clob | PM native client |
| Session keys | PM-clob | PM native client |
| CTF split / merge / redeem | PM-clob | `pkg/polymarket/ctf.go` (R) |
| HIP-3 / DEX-scoped execution | HL-perp | `pkg/hyperliquid/exchange/dex.go` (uncommitted) |
| Vault / sub-account signing | HL-perp | D `vaultAddress` (native) |
| Spot transfer `spotSend` | HL-spot | D (native) |
| Spot wallet transfer `asset/wallet/transfer` | AS-spot | D (native) |
| Builder fee attribution (`approveAgent`, `approveBuilder`) | AS-spot, AS-perp | D (native) |
| Funding and mark-price streams | BN-perp, AS-perp | native |
| Polymarket perps proxy-wallet auth | PM-perp | native |

## Not offered (docs checked)

- HL-perp: no order heartbeat; no GTD; no position mode.
- BN-perp: no order heartbeat; no GTD on the checked endpoints.
- BN-spot: no leverage, margin, reduce-only, or dead-man switch (not seen in the docs checked).
- AS-perp: no WS order API in the docs checked; no order heartbeat.
- AS-spot: no batch orders, no cancel-all, no amend, no cancel-replace, and no WS order API, all absent from the spot endpoint list. Dead-man switch is not in the spot list.
- PM-clob: no WS order placement, no reduce-only, no trigger orders, no leverage.

## Documented but not implemented in the repo

- All venues: WS order placement and WS cancel. Only HL info-over-WS is proven.
- HL-spot: all execution (docs: same API as perp with `10000 + index`).
- BN-perp: `Amend`, `TWAP`, `Trigger` builders (types exist in `userstream_types.go`).
- AS-perp: no live order test yet (`cmd/test_aster_orders` is written, never run with `-live`). Cancel by exchange id not wired (same as BN-perp). No testnet is documented.
- AS-spot: the whole venue (no connector; endpoints listed above).
- PM-clob: batch as one call (connector loops single posts), order heartbeat, `GetOrder`, `CancelAll`.
- PM-perp: the whole venue (no connector).
- HL-perp: `GetOrder` query, `Amend` / `batchModify`, `CancelReplace`, `TWAP` place/cancel.

## Hub sources added on 2026-11-05 (catalog commit `90b5e4d`)

- Aster spot API v3 markdown (GitHub `asterdex/api-docs`, `V3(Recommended)`).
- Aster spot general info (HTML, fallback).
- Polymarket perps: reference index, overview, OpenAPI, AsyncAPI.
- Polymarket RTDS v2 AsyncAPI (`ws-live-v2.polymarket.com`), which replaces the old RTDS host.

All five Polymarket perps and RTDS v2 sources are verified in the catalog (`94f93706…` for
the RTDS v2 AsyncAPI). The RTDS v2 spec is not yet read in full, so RTDS cells that depend
on it stay `?` until it is.

## Notes: Aster perps (2026-11-05, d6ab3d9)

- AS-perp `R` cells mean implemented and unit-tested only. `P` cells (market WS subscribe, book, trades) were seen live in a read-only 20 s probe. No AS-perp order operation has run against the venue.
- AS-perp reuses `pkg/binance/futures` through configuration; a Binance-side change can therefore change Aster behaviour. Both packages' tests must pass together.
- Still `D` or `?` for AS-perp: cancel by exchange id, balances, fees, order status query, fills REST. AS-spot is not started.

## Notes: Binance perps and spot (2026-11-05)

- BN-perp gained cancel/get by exchange order id (single and batch). The contract adapter cancels by order id when set and rejects a batch that mixes order ids and client ids.
- BN-spot `R` cells (`pkg/binance/spot`, `pkg/contract/bnspot`) are unit-tested only; nothing has run against the venue. Spot has no batch place or cancel, so the adapter loops sequentially with a local cap of 5, not a venue cap.
- Spot user stream uses the WS API `userDataStream.subscribe.signature` (works with HMAC keys; the REST listenKey is marked removed in the docs). Connection lifetime 24 h, handled by reconnect and re-subscribe.
- Spot `DELETE /api/v3/openOrders` with nothing open: docs silent; code treats -2011 as success (unconfirmed).
- Live tools: `cmd/test_binance_orders` (futures), `cmd/test_binance_spot_orders`. Both: `-network testnet|mainnet`, `-live`, mainnet also `-confirm-mainnet`; keys `BINANCE_{FUTURES,SPOT}_{TESTNET,MAINNET}_API_{KEY,SECRET}` (resolver `internal/bnnet`).
