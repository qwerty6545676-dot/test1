# arbitrage-scanner — reference knowledge

Low-latency cross-exchange price arbitrage scanner. Hot path: WS frame → `prices[symbol][exchange] = Tick` (atomic dict write under GIL, no asyncio.Queue) → `check_and_signal()` called inline → Telegram + signals.jsonl + Prometheus.

**Stack:** `picows` (Cython WS), `uvloop`, `msgspec` (Struct + JSON), `pytest`. Python 3.11/3.12 in CI.

**See also:** [`GUIDE.ru.md`](../../../GUIDE.ru.md) — full Russian-language guide for run/troubleshoot/roadmap.

---

## Layout

```
arbitrage/
  main.py              # uvloop entrypoint, spawns all listeners + watchdog
  settings.py          # typed YAML loader (msgspec.Struct schema)
  config.py            # backwards-compat flat constants from settings
  normalizer.py        # Tick struct (msgspec.Struct, gc=False)
  comparator.py        # find_arbitrage + check_and_signal (push-style)
  heartbeat.py         # evict stale entries; emits eviction WARNs
  cooldown.py          # 3-min suppress per (market, symbol, buy_ex, sell_ex)
  ratelimit.py         # AsyncTokenBucket (KuCoin bullet-public guard)
  watchdog.py          # supervises listener coroutines, restart budget
  signals.py           # SignalBus + InfoEvent (kinds: silence, recovery, error, watchdog)
  tier_router.py       # routes signals to low/mid/high topic by spread
  metrics.py           # prometheus-client; off by default (port 9090)
  replay.py            # CLI: python -m arbitrage.replay --root data/ticks ...
  paper/{spot,perp}.py # $50/leg paper trading
  persistence/         # signals_writer.py (signals.jsonl) + ticks.py (ticks.bin)
  telegram_notify/     # client + cooldown + tier routing
  exchanges/
    spot/{binance,bybit,gateio,bitget,kucoin,bingx,mexc}.py
    perp/{binance,bybit,gateio,bitget,kucoin,bingx,mexc}.py + _symbol.py
    _common.py         # backoff with jitter, symbol normalize
scripts/
  discover_universe.py # rebuild top-N universe from REST tickers
  probe_perp/          # one-shot WS probes per venue
tests/                 # pytest, full suite must be green pre-PR
settings.example.yaml  # source of truth for defaults; settings.yaml is gitignored
.env.example           # only TELEGRAM_BOT_TOKEN; everything else in YAML
monitoring/            # docker-compose Prometheus + Grafana + dashboard.json
docs/universe-100.{md,json}  # current universe + coverage matrix
```

---

## Run / test

```bash
# Install
pip install -r requirements.txt -r requirements-dev.txt

# Tests — must be green before opening a PR
pytest -q

# Run scanner (foreground; reads settings.yaml, falls back to settings.example.yaml)
cp settings.example.yaml settings.yaml
cp .env.example .env  # fill TELEGRAM_BOT_TOKEN if telegram.enabled=true
python -m arbitrage.main

# Override settings path
ARB_SETTINGS_PATH=/etc/arbitrage/settings.yaml python -m arbitrage.main

# Replay recorded ticks
python -m arbitrage.replay --root data/ticks --from 2025-04-20 --to 2025-04-22 --speed 0

# Rebuild universe (rate-limited, ~30s)
python -m scripts.discover_universe
```

Metrics endpoint: `127.0.0.1:9090` when `metrics.enabled: true`. Kill-switch: `ARB_METRICS_DISABLED=1`.

---

## Exchange WS quirks (CRITICAL — easy to miss, cost real reconnect loops)

* **KuCoin** (spot+perp): bootstrap REST `/bullet-public` to get token+endpoint before WS connect (rate-limited via `AsyncTokenBucket`). KuCoin **perp** uses `XBTUSDTM` instead of `BTCUSDTM` — converter in `arbitrage/exchanges/perp/_symbol.py`.
* **MEXC** spot+perp: WS limit ~30 streams/connection. Listener shards 100 symbols across **4 parallel WS connections** of 25 streams each. Spot uses Protobuf (`mexc_proto/`), perp uses JSON.
* **Bybit spot:** `op:subscribe` accepts max 10 args per frame — listener chunks subscriptions into 10×10 frames. Perp accepts higher.
* **BingX spot:** gzip + JSON. Keepalive is JSON `{"ping": "<uuid>"}` → reply `{"pong": "<uuid>", "time": ...}`.
* **BingX perp** (`open-api-swap.bingx.com/swap-market`): keepalive is **literal binary** — server sends gzipped `b"Ping"` every ~5 s, expects `b"Pong"` reply, drops connection after ~30 s otherwise. **Different protocol from spot.** Listener handles literal first, JSON as defensive fallback (PR #18). Probe: `wss://open-api-swap.bingx.com/swap-market` with any subscription, log raw decompressed frames.
* **Binance:** blocked from AWS US-east IPs (HTTP 451 on WS handshake). Works from non-AWS VPS.
* **Gate.io:** sends ping every 10 s; **Bitget** every 25 s — both JSON ping/pong, handled.

---

## Settings (`settings.example.yaml`)

Key sections:
* `symbols.{spot,perp}` — currently 100/100, ~67 overlap (see `docs/universe-100.md` for coverage matrix and how to rebuild).
* `exchanges` — per-venue fees.
* `min_profit_pct` — perp 0.5%, spot 3.0% (spot too high for current market; lowering to 1.5–2% surfaces real signals).
* `max_age_ms` — 1500.
* `cooldown_seconds` — 180.
* `telegram.{enabled,spot_chat_id,perp_chat_id,topics.{info,low,mid,high}}`.
* `paper_trading.{enabled,open_path,closed_path}` — $50/leg simulated trades.
* `persistence.{enabled,signals_path,tick_storage.{enabled,root,retention_days,compression}}`.
* `metrics.{enabled,port}`.

`settings.yaml` and `.env` are gitignored. Always edit `settings.example.yaml` for committed defaults.

---

## Telegram

Bot token goes into `.env` as `TELEGRAM_BOT_TOKEN`. Chat IDs and topic mapping (info/low/mid/high) live in `settings.yaml` under `telegram.spot_chat_id` / `perp_chat_id` / `topics.*`.

Cooldown is 3 min per `(market, symbol, buy_ex, sell_ex)` — prevents 429s on hot pairs. Network errors (DNS / TLS / connection-refused) record `telegram_retries_total{reason="network"}` + `telegram_dropped_total` (PR #16 fix).

---

## Persistence paths (all under `data/`, gitignored)

* `data/signals.jsonl` — append-only signals (grep/jq friendly).
  * **Known issue:** duplicates tick-by-tick when prices are stable (e.g. one symbol got 14k identical entries in a 30-min run). Dedup PR pending.
* `data/ticks/{venue}/{market}/ticks-YYYY-MM-DD.bin[.zst]` — length-prefixed msgpack, daily UTC rotation, zstd-compressed in background, 7-day retention.
  * `discover_files()` prefers `.bin` over `.zst` for the same date to avoid double-replay if compression was interrupted (PR #15 Devin Review fix).
* `data/paper_open.jsonl` / `paper_closed.jsonl` — paper trading legs.

---

## Known issues / open backlog (post-PR-#18)

* **Heartbeat-eviction WARN flood:** ~66 k WARN / 30 min from symbols listed in universe but not tradeable on some venues (e.g. `kucoin-perp/ANIMEUSDT`, `bingx/SNEKUSDT`). Fix path: per-exchange overrides in settings, or probe-validation inside `discover_universe.py`.
* **`signals.jsonl` dedup** when prices unchanged.
* **Spot signal rate:** default `min_profit_pct=3.0` is too high for current market.
* **Binance from AWS:** 451 blocked; non-issue on non-AWS VPS.
* **systemd unit / process separation:** deferred (need VPS / live executor first).
* **Auto-discovery (Phase 2):** dynamic re-subscribe on listing/delisting (current discover is one-shot).
* **Per-symbol fee overrides:** currently one fee per venue; promo listings / BNB-discount / zero-fee newcomers can yield false 0.5% arbs.

---

## Git / PR workflow conventions

* **Branches:** `devin/<unix_ts>-<short-desc>` (e.g. `devin/1777286736-bingx-perp-keepalive`).
* **PR titles:** conventional-commit-ish (`fix(bingx-perp): ...`, `feat(metrics): ...`).
* **PR bodies:** explain root cause + verification. Cite live probe output where the fix concerns a network protocol. Use the repo template (`git_pr(action="fetch_template")` first).
* **CI:** three checks — `test (3.11)`, `test (3.12)`, `Devin Review`. Wait for all three green before reporting done.
* **Devin Review:** typically surfaces 1–2 real bugs per PR — address in a follow-up commit on the same branch; do not dismiss.
* **Never push to `main` directly.** User merges PRs.

---

## Status: 12-task roadmap closed (18 PRs merged)

Spot 7 venues · perp 7 venues · settings/env · GUIDE.ru.md · Telegram integration · rate-limit · watchdog · persistence (signals.jsonl + ticks.bin/replay) · reconnect tests · paper trading · Prometheus + Grafana · universe-100 · BingX-perp keepalive (PR #18).

Further directions on hold pending user choice: heartbeat-WARN dedup, signals.jsonl dedup, per-exchange symbol overrides, fee overrides per symbol, auto-discovery, 8th/9th exchange (OKX/HTX/Coinbase), Grafana alerts, live executor (Phase 2), systemd unit.
