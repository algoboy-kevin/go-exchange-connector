# AGENTS.md — go-exchange-connector (repo memory)

Read this before touching the repo. Maintained by the orchestrating agent after every
subagent session; keep it short and current. Module: `github.com/algoboy-kevin/go-exchange-connector`.
Consumers: `go-collector` (sibling repo, pins a tag in its go.mod).

## Layout
- `pkg/websocket` — `BaseWebSocket`: reconnLoop, ping/pong, optional data-stale watchdog.
  `Disconnect()` = CloseNow + keep reconnLoop alive + fire OnDisconnect (subscriptions re-queue);
  `Close()` = permanent shutdown. Flap backoff via `nextDelayAfterDrop` (v0.7.2).
- `pkg/polymarket` — `PolymarketConnector` (market WS, user WS, RTDS share one base connector;
  Gamma client; CTF ops). `market.go`: incremental `operation:subscribe/unsubscribe` frames on a
  live socket; on (re)connect `onConnect` sends ONE handshake with the full asset set (server
  replies with every book snapshot — size scales with subscriptions).
  `Config.ReadLimitBytes` (yaml `read_limit_bytes`, 0 → 1 MiB) — added v0.7.2; before that the
  coder/websocket 32 KiB default broke reconnects. Market WS sets `DisableDataStaleWatchdog=true`
  (quiet markets false-trigger it); consumers run their own whole-feed silence watchdog and call
  `ReconnectMarket()` (v0.7.3).
- `pkg/binance`, `pkg/hyperliquid` — market data streams (Binance read limit 1 MiB).

## Build / test / release
```
go build ./... && go vet ./... && go test -count=1 ./...
go test -race -count=1 ./pkg/polymarket/
```
Release = bump `version.go` `Version` to match the tag, commit, tag `vX.Y.Z`. Tags/branches are
pushed only when the user asks; until then go-collector uses a TEMP `replace` to `../go-exchange-connector`.

## Rules for agents
- The user keeps untracked docs under `docs/` (`CONNECTOR_CHANGES.md`, `PERP_COLLECTOR_SPEC.md`,
  `EXCHANGE_AUDIT.md`, `FEATURE_MATRIX.md`). Never stage/modify them without an explicit request;
  commit with explicit paths only.
- No push / tag push unless asked.

## Current state (update every session)
- 2026-10-11: branch `feat/mm-exec` (from feat/reconnect-market, local, NOT pushed) for go-mm-driver
  execution (specs live in go-mm-driver/docs/specs S11, S13): a4d2b64 `pkg/binance/futures` signed
  REST; 93445e2 Binance user-data stream (`/private/ws?listenKey=&events=`, rekey with retry),
  markPrice@1s, `binance.NewWithOptions` (testnet endpoints); 159c2ab `pkg/hyperliquid/exchange`
  over sonirico/go-hyperliquid v0.46.1 (go directive now 1.25.3). Pitfalls: encoding/json matches
  keys case-insensitively — tag every Binance/HL field exactly (E/e, T/t collisions bit us);
  sonirico NewInfo panics on nil meta (pass meta + empty SpotMeta); hl.Meta cannot decode real
  marginTables (decode via shadow struct); sonirico UpdateLeverage hides venue rejections.
  go-collector's TEMP replace points here, so it now builds against this branch.
- 2026-10-09: branch `feat/reconnect-market` (commit 91a1cbc, local tag v0.7.3, NOT pushed):
  `PolymarketConnector.ReconnectMarket()` + tests (`reconnect_test.go`: nil-safe; redial resends
  the full handshake). main = ff51b8a (v0.7.2).
