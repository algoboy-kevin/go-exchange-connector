# testdata sources

JSON cannot carry comments, so the provenance is recorded here. Source of every file:
exchange-docs-hub catalog entry `binance`, https://developers.binance.com/en/docs/llms-full.txt,
sha256 7fe8634d231eacc5564bef1995fe45c1bbc52ea7b7072cc50e3b0d06ad05c68e. Line numbers are lines of that file.
Where the doc prints JavaScript object syntax (unquoted keys, trailing commas, comments) the file is the same
object rewritten as strict JSON; no value was changed.

| file | doc section (line) |
| --- | --- |
| new_order_ack.json / new_order_result.json / new_order_full.json | spot testnet REST, "New order (TRADE)" Response - ACK / RESULT / FULL (119962-120056) |
| query_order.json | "Query order (USER_DATA)" (120185-120206) |
| cancel_order.json | "Cancel order (TRADE)" (120246-120262) |
| cancel_all.json | "Cancel All Open Orders on a Symbol (TRADE)" (120319-120418); two orders and one OCO order list |
| open_orders.json | "Current open orders (USER_DATA)" (122010-122033) |
| my_trades.json | "Account trade list (USER_DATA)" (122321-122337) |
| account.json | "Account information (USER_DATA)" (121949-121982) |
| book_ticker.json | "Symbol order book ticker" Response (119553-119565) |
| server_time.json | "Check server time" Response (118477-118482) |
| exchange_info.json | "Exchange information" symbol skeleton (118542-118600, fields reduced to those present) with the filter examples of the filters doc inserted: PRICE_FILTER 23929, PERCENT_PRICE 23956, PERCENT_PRICE_BY_SIDE 23991, LOT_SIZE 24019, MIN_NOTIONAL 24048, NOTIONAL 24077. The filter values are from the doc examples, not a real symbol. |
| ws_execution_report.json | user-data stream "Order Update" payload (117283-117327) |
| ws_outbound_account_position.json | "Account Update" payload (117234-117255) |
| ws_balance_update.json | "Balance Update" payload (117264-117276) |
| ws_event_stream_terminated.json | "eventStreamTerminated" payload (117552-117562) |
| ws_server_shutdown.json | WS API "Server Shutdown" payload (20121-20128) |
| ws_subscribe_ok.json | WS API userDataStream.subscribe.signature Response (testnet WS API, 117187-117200) |
| ws_error.json | WS API "Failed response" example (19938-19960) |

Signing vectors used by the tests (REST, testnet REST "HMAC Keys", 117953-118090; WS API, 20529-20600): the doc's
example key `vmPUZE...` and secret `NhqPtm...` are published example values in the docs, not real credentials.
