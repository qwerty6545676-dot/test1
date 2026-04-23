# arbitrage-scanner

Low-latency cross-exchange price arbitrage scanner.

Built around three decisions that together keep the hot path as short as
possible:

1. **No `asyncio.Queue` between WS and comparator.** Each exchange
   handler writes `Tick` objects directly into a shared in-memory dict
   (`prices[symbol][exchange] = Tick`). There's one event loop, one
   thread — Python's GIL makes the writes effectively atomic, so no
   lock is needed.
2. **Push-style comparator.** Right after a handler updates the book,
   it calls `check_and_signal(prices, symbol)` inline. No polling
   loop, no `asyncio.sleep(0.1)` — signals fire on the same tick as
   the data.
3. **Fastest building blocks available in Python.** `picows` (Cython
   WebSocket client), `uvloop` (C event loop), `msgspec` (C struct
   codec + JSON parser).

## Status

| Exchange | Status |
| -------- | ------ |
| Binance spot | implemented (`bookTicker`) |
| Bybit spot | implemented (`orderbook.1` + snapshot/delta state) |
| Gate.io spot | implemented (`spot.book_ticker` + 10s ping) |
| Bitget spot | implemented (`books1` + 25s ping) |
| KuCoin spot | implemented (REST-token bootstrap + `/market/ticker`) |
| BingX spot | implemented (gzip + `@bookTicker` + ping/pong replies) |
| MEXC spot | implemented (Protobuf + `aggre.bookTicker@100ms`) |

## Layout

```
arbitrage/
├── main.py          # uvloop + tasks + SIGINT/SIGTERM shutdown
├── settings.py      # typed YAML config loader (msgspec Struct schema)
├── config.py        # backwards-compat flat constants, driven by settings.py
├── normalizer.py    # Tick struct (msgspec.Struct, gc=False) + validator
├── heartbeat.py     # evict stale per-exchange entries
├── comparator.py    # find_arbitrage + check_and_signal
├── mexc_proto/      # generated Protobuf stubs for MEXC
└── exchanges/
    ├── _common.py   # backoff helper (with jitter) + symbol normalization
    ├── binance.py   # bookTicker
    ├── bybit.py     # orderbook.1 + snapshot/delta merge + 18s ping
    ├── gateio.py    # spot.book_ticker + 10s ping
    ├── bitget.py    # books1 + 25s ping
    ├── kucoin.py    # REST bullet-public + /market/ticker
    ├── bingx.py     # gzip-framed @bookTicker + ping/pong reply
    └── mexc.py      # Protobuf aggre.bookTicker@100ms

settings.example.yaml  # committed template — copy to settings.yaml & edit
.env.example           # committed template — copy to .env & fill secrets
proto/mexc/            # source .proto files for MEXC (regenerate with protoc)
```

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp settings.example.yaml settings.yaml   # tweak locally (git-ignored)
cp .env.example .env                     # fill in secrets (git-ignored)
```

Requires Python 3.11 or 3.12. Python 3.13t (free-threaded) slows
`asyncio` down — don't use it.

## Run

```bash
python -m arbitrage.main
```

Logs arbitrage opportunities that clear `filters.spot.min_profit_pct`
net of taker fees on both legs, for example:

```
10:32:07.412 INFO arbitrage.comparator | ARB BTCUSDT: buy binance @ 64982.10000000 -> sell bybit @ 65041.50000000 | net 0.181%
```

## Configuration

Everything tunable lives in **`settings.yaml`** (see
[`settings.example.yaml`](settings.example.yaml) for the full schema).
The file is validated via `msgspec.Struct`, so typos or wrong types fail
at startup, not in production. Secrets (Telegram bot token) live in
**`.env`**, never in `settings.yaml`.

Top-level sections:

- `symbols.{spot,perp}` — universe per market type.
- `filters.{spot,perp}` — detection thresholds (`min_profit_pct`),
  dedup cooldowns, and tier bands that route signals to Telegram
  forum topics.
- `fees.{spot,perp}` — per-exchange taker fees (fractions, not
  percent). Used round-trip in the comparator.
- `limits` — `max_age_ms`, heartbeat interval/timeout, reconnect
  backoff cap and jitter.
- `telegram` — `enabled`, `spot_chat_id`, `perp_chat_id`. Bot token
  is read from `$TELEGRAM_BOT_TOKEN`.

Point the scanner at a different file by setting
`ARB_SETTINGS_PATH=/path/to/settings.yaml`. Useful for running paper
and live instances side by side from the same checkout.

The legacy flat constants (`SYMBOLS`, `FEES`, `MIN_PROFIT_PCT`, ...) in
[`arbitrage/config.py`](arbitrage/config.py) are still exported for
backwards compatibility with the existing listeners, but their values
are derived from `settings.yaml` at import time. New code should import
from `arbitrage.settings` directly.

## Notes on exchange quirks

- **Binance** forcibly drops spot connections every 24h — the
  connect-forever loop with exponential backoff + jitter handles
  this.
- **Bybit** public WS drops connections after ~20s of silence; we
  send `{"op":"ping"}` every 18s as a keepalive.
- **`orderbook.1` deltas** on Bybit can omit a side. The listener
  keeps a per-symbol merged state and only emits a tick once both
  sides are known.
- **Gate.io** closes idle connections silently (no error frame) —
  we send `{"channel":"spot.ping"}` every 10s to stay alive.
- **KuCoin** requires a REST `POST /api/v1/bullet-public` before
  every WS connect to obtain a one-shot token and the actual
  endpoint; the token is regenerated on each reconnect and a fresh
  `connectId` (`uuid4`) is used every time.
- **BingX** gzips every server frame, even the pings, and the ping
  frame is `{"ping":"<uuid>","time":...}` — the client has to reply
  with `{"pong":"<same-uuid>","time":...}` or the socket drops.
- **MEXC** uses Protobuf for market data. Subscribe/control frames
  are still JSON, but book ticker payloads are
  `PushDataV3ApiWrapper` messages. Generated stubs are checked in
  under `arbitrage/mexc_proto/`; `.proto` sources under
  `proto/mexc/`. Regenerate with `grpcio-tools`:
  ```
  python -m grpc_tools.protoc -I proto/mexc \
      --python_out=arbitrage/mexc_proto proto/mexc/*.proto
  ```
- **Jitter in reconnect backoff** prevents all handlers from
  reconnecting simultaneously after a network blip and tripping
  per-IP rate limits.
