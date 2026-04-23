# Perp WebSocket probes

One-off scripts that connect to each exchange's **perp** book-ticker
WebSocket, subscribe to `BTCUSDT`, print the first few frames, and
exit. Used to confirm URLs, subscribe formats, symbol conventions and
frame shapes before writing full listeners.

Run any of them directly:

```bash
python3 scripts/probe_perp/binance_perp.py
python3 scripts/probe_perp/bybit_perp.py
python3 scripts/probe_perp/gateio_perp.py
python3 scripts/probe_perp/bitget_perp.py
python3 scripts/probe_perp/kucoin_perp.py
python3 scripts/probe_perp/bingx_perp.py
python3 scripts/probe_perp/mexc_perp.py
```

Each script exits after ~4 frames or 10s, whichever comes first.

See [`docs/perp-research.md`](../../docs/perp-research.md) for the
consolidated findings table.
