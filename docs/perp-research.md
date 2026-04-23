# Perp WebSocket research

Live-probed each of the 7 exchanges on 2026-04-22. All subscribe
without auth for public book-ticker data. Full probe scripts:
[`scripts/probe_perp/`](../scripts/probe_perp/).

## Summary table

| Exchange | WS URL | Subscribe | Native symbol for `BTCUSDT` | BBO frame shape |
|---|---|---|---|---|
| Binance USDM | `wss://fstream.binance.com/ws/btcusdt@bookTicker` | path-based, no sub msg | `BTCUSDT` in payload, lowercase in URL | `{e,s,b,B,a,A,E,T}` |
| Bybit linear | `wss://stream.bybit.com/v5/public/linear` | `{op:subscribe, args:["orderbook.1.BTCUSDT"]}` | `BTCUSDT` | `{topic, ts, type:snapshot\|delta, data:{s, b:[[p,q]], a:[[p,q]], u, seq}}` |
| Gate.io USDT-M | `wss://fx-ws.gateio.ws/v4/ws/usdt` | `{time, channel:"futures.book_ticker", event:"subscribe", payload:["BTC_USDT"]}` | `BTC_USDT` (underscore) | `{channel, event:update, result:{s, b:str, B:int, a:str, A:int, t, u}}` |
| Bitget USDT-FUTURES | `wss://ws.bitget.com/v2/ws/public` | `{op:subscribe, args:[{instType:"USDT-FUTURES", channel:"books1", instId:"BTCUSDT"}]}` | `BTCUSDT` | `{action:snapshot, arg, data:[{asks:[[p,q]], bids:[[p,q]], ts, checksum, seq}]}` |
| KuCoin futures | `wss://ws-api-futures.kucoin.com/?token=…&connectId=…` (token from `POST https://api-futures.kucoin.com/api/v1/bullet-public`) | `{id, type:subscribe, topic:"/contractMarket/tickerV2:XBTUSDTM"}` | **`XBTUSDTM`** (BTC → XBT, USDT → USDTM suffix) | `{topic, subject:tickerV2, data:{bestBidPrice, bestAskPrice, bestBidSize, bestAskSize, ts (ns), sequence}}` |
| BingX swap | `wss://open-api-swap.bingx.com/swap-market` | `{id, reqType:"sub", dataType:"BTC-USDT@bookTicker"}` | `BTC-USDT` (dash) | gzip'd `{code, dataType, data:{e, s, b, B, a, A, E, T, u}}` |
| MEXC contract | `wss://contract.mexc.com/edge` | `{method:"sub.ticker", param:{symbol:"BTC_USDT"}}` | `BTC_USDT` (underscore) | **plain JSON** `{symbol, data:{bid1, ask1, lastPrice, timestamp, …}}` |

## Implementation notes per exchange

### Binance (USDM perp)

Smallest possible port of the existing spot listener:

- Change host from `stream.binance.com` → `fstream.binance.com`.
- Everything else — combined streams path, frame shape — identical.
- 24h forced disconnects exist on perp too; reuse the same
  reconnect/backoff loop.

### Bybit (linear perp)

Identical logic to spot listener:

- Change path from `public/spot` → `public/linear`.
- `orderbook.1` uses **snapshot + delta**, so the per-symbol merged
  state from `bybit.py` ports over unchanged.
- Heartbeat still 18s `{"op":"ping"}`.

### Gate.io (USDT-margined futures)

- Host: `fx-ws.gateio.ws` (different from spot's `api.gateio.ws`).
- Channel: `futures.book_ticker` (vs spot `spot.book_ticker`).
- ⚠️ **`B` and `A` on perp are contract counts, not coin qty.** Fine
  for arb detection (we use prices, not quantity), but need to
  convert for paper-trading notional sizing later.
- Ping: same `{"channel":"futures.ping"}` every 10s.

### Bitget (USDT-FUTURES)

The easiest perp port of the seven:

- Same host, same path (`/v2/ws/public`), same channel (`books1`).
- Only change: `instType: "USDT-FUTURES"` (vs spot `SPOT`).
- 25s ping identical.

### KuCoin (futures) — symbol quirk

Most surgery of the seven, in three ways:

1. **Different bullet-public host.** Spot uses
   `api.kucoin.com/api/v1/bullet-public`; futures uses
   `api-futures.kucoin.com/api/v1/bullet-public`. Token and WS
   endpoint both come from this response.
2. **Different WS host returned:**
   `wss://ws-api-futures.kucoin.com/`.
3. **Non-trivial symbol mapping:** `BTCUSDT` in our canonical form →
   `XBTUSDTM` on KuCoin perp:
   - `BTC` → `XBT` (legacy Kraken-style naming)
   - add `M` suffix ("margined") to USDT-margined contracts.
   - Example: `ETHUSDT` → `ETHUSDTM`, `SOLUSDT` → `SOLUSDTM`,
     but `BTCUSDT` → **`XBTUSDTM`** (not `BTCUSDTM`).
4. **Topic prefix:** `/contractMarket/tickerV2:<native>`
   (vs spot's `/market/ticker:<native>`).
5. **Frame shape:** `data.bestBidPrice` / `bestAskPrice` strings,
   `bestBidSize` / `bestAskSize` are integers (contracts).

Encode the symbol mapping in
`arbitrage/exchanges/perp/_symbol.py::to_kucoin_perp(sym)` so the
special-case for `BTC ↔ XBT` lives in one place.

### BingX (swap)

Twins of the spot listener:

- Different host path: `/swap-market` (vs spot `/market`).
- Symbol format: `BTC-USDT` with a dash — same as spot.
- Same gzip-per-frame and `Ping` → `Pong` keepalive protocol.
- Ping frame in perp looks like `Ping` plain text (not JSON) and
  answer is `Pong`; verify vs the spot listener which handles
  `{"ping": uuid, "time": …}` JSON. If the spot ping-pong code path
  is JSON-only, the perp listener needs to *also* handle the plain
  `Ping` text frame.

### MEXC (contract/perp) — **much simpler than spot!**

Big win: **contract WS uses plain JSON**, no Protobuf.

- Host: `wss://contract.mexc.com/edge` (totally different from spot
  `wss://wbs-api.mexc.com/ws`).
- Channel: `sub.ticker` — subscribe via `{method, param:{symbol}}`.
- Frame: `{symbol:"BTC_USDT", data:{bid1, ask1, timestamp, …}}`.
  `bid1` and `ask1` are top-of-book prices (numbers).
- Quantities: not in this channel. If we need them later, subscribe
  to `sub.depth.full` or `sub.deal` — but for spread detection, top
  prices are enough.
- Keepalive: send `{"method":"ping"}` every 10s (contract server
  disconnects after 20s of silence).

No `mexc_proto` dependency for the perp path. `arbitrage/mexc_proto/`
stays scoped to spot.

## Symbol normalization summary

From canonical `BTCUSDT` / `ETHUSDT` / `SOLUSDT`:

| Exchange | Spot native | Perp native |
|---|---|---|
| Binance | `BTCUSDT` | `BTCUSDT` (lowercase in URL) |
| Bybit | `BTCUSDT` | `BTCUSDT` |
| Gate.io | `BTC_USDT` | `BTC_USDT` |
| Bitget | `BTCUSDT` | `BTCUSDT` |
| KuCoin | `BTC-USDT` | `XBTUSDTM` / `ETHUSDTM` / `SOLUSDTM` (⚠️ BTC→XBT) |
| BingX | `BTC-USDT` | `BTC-USDT` |
| MEXC | `BTCUSDT` | `BTC_USDT` |

## Open questions before implementation

1. **Tick struct — add `market_type` field?** Options:
   - (a) `Tick(..., market: Literal["spot","perp"])` — one struct,
     dual-purpose.
   - (b) keep current Tick struct; split state via separate dicts
     (`prices_spot`, `prices_perp`) and keep exchange names clean
     (`binance`, not `binance-perp`).
   - **Recommend (b):** zero changes to the Tick hot-path struct,
     listener knows its market, signals written to the right dict.
2. **Comparator — one function with market arg, or two functions?**
   - **Recommend one function** `find_arbitrage(book, symbol,
     now_ms, fees, min_pct)` — pass the right fee table and
     threshold; caller picks spot-fees-or-perp-fees.
3. **Listener file layout:**
   - **Recommend subfolder split:** move current 7 files into
     `arbitrage/exchanges/spot/`, add parallel
     `arbitrage/exchanges/perp/`. Cleaner than suffix.
4. **KuCoin perp — rate limit for `bullet-public` is identical to
   spot (3 rps).** Rate-limit middleware applies to both.
