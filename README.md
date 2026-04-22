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
| Gate.io / Bitget / KuCoin / BingX / MEXC | stubs — `NotImplementedError` |

## Layout

```
arbitrage/
├── main.py          # uvloop + tasks + SIGINT/SIGTERM shutdown
├── config.py        # SYMBOLS, MAX_AGE_MS, MIN_PROFIT_PCT, FEES, ...
├── normalizer.py    # Tick struct (msgspec.Struct, gc=False) + validator
├── heartbeat.py     # evict stale per-exchange entries
├── comparator.py    # find_arbitrage + check_and_signal
└── exchanges/
    ├── _common.py   # backoff helper (with jitter)
    ├── binance.py   # picows listener
    ├── bybit.py     # picows listener + snapshot/delta merge + 18s ping
    └── gateio.py / bitget.py / kucoin.py / bingx.py / mexc.py  (TODO)
```

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Requires Python 3.11 or 3.12. Python 3.13t (free-threaded) slows
`asyncio` down — don't use it.

## Run

```bash
python -m arbitrage.main
```

Logs arbitrage opportunities that clear `MIN_PROFIT_PCT` net of taker
fees on both legs, for example:

```
10:32:07.412 INFO arbitrage.comparator | ARB BTCUSDT: buy binance @ 64982.10000000 -> sell bybit @ 65041.50000000 | net 0.181%
```

## Tuning

All constants live in [`arbitrage/config.py`](arbitrage/config.py):

- `SYMBOLS` — universe to track. Add/remove freely.
- `MAX_AGE_MS` — ticks older than this are ignored by the comparator.
- `MIN_PROFIT_PCT` — threshold for an arb signal, applied **after**
  fees on both legs.
- `FEES[exchange]` — taker fee as a fraction (0.001 = 0.10%).
- `HEARTBEAT_TIMEOUT_MS` — evict a per-exchange entry if it hasn't
  updated in this long.

## Notes on exchange quirks

- **Binance** forcibly drops spot connections every 24h — the
  connect-forever loop with exponential backoff + jitter handles
  this.
- **Bybit** public WS drops connections after ~20s of silence; we
  send `{"op":"ping"}` every 18s as a keepalive.
- **`orderbook.1` deltas** on Bybit can omit a side. The listener
  keeps a per-symbol merged state and only emits a tick once both
  sides are known.
- **Jitter in reconnect backoff** prevents all handlers from
  reconnecting simultaneously after a network blip and tripping
  per-IP rate limits.
