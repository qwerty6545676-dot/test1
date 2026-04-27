# Universe-100: which symbols the scanner watches and why

**TL;DR.** The scanner monitors **100 spot pairs** and **100 perp
pairs** across our seven venues (Binance, Bybit, Gate.io, Bitget,
KuCoin, BingX, MEXC). Every pair is listed on **at least 5 of the 7
venues** so cross-venue arbitrage is mechanically possible; the lists
were ranked by aggregate 24h USD volume across all listing venues. 67
symbols appear in **both** the spot and the perp top-100 (they let
paper trading and the comparator share telemetry across markets).

The full machine-readable lists live in [`universe-100.json`](universe-100.json)
and are embedded into [`settings.example.yaml`](../settings.example.yaml)
as the project default.

---

## Why we don't try to listen to "everything"

Across the 7 venues, the union of unique USDT pairs is roughly
~7000 spot + ~3000 perp. The intersection of all 7 venues — the only
pairs we could actually arbitrage on every venue — is a much smaller
set (around ~80 symbols). Adding pairs that only exist on one or two
venues just spends a WS slot and metric-cardinality budget on a
symbol we can never trade against itself.

We compromised at **≥5/7 venues** because requiring all 7 dropped
several legitimate, high-volume names (e.g. XMR, which is delisted on
Binance/Bybit but very much active on the other 5). 5/7 keeps the
universe broad enough to actually find arbs while still guaranteeing
that any signal involves at least 4 distinct counterparties.

## How the universe is generated

[`scripts/discover_universe.py`](../scripts/discover_universe.py)
fetches each venue's REST endpoint:

| venue   | spot endpoint | perp endpoint |
|---------|---------------|---------------|
| Binance | `/api/v3/ticker/24hr`           | `/fapi/v1/ticker/24hr`             |
| Bybit   | `/v5/market/tickers?category=spot` | `/v5/market/tickers?category=linear` |
| Gate.io | `/api/v4/spot/tickers`          | `/api/v4/futures/usdt/tickers`     |
| Bitget  | `/api/v2/spot/market/tickers`   | `/api/v2/mix/market/tickers?productType=USDT-FUTURES` |
| KuCoin  | `/api/v1/market/allTickers`     | `/api/v1/contracts/active` (perp host) |
| BingX   | `/openApi/spot/v1/ticker/24hr`  | `/openApi/swap/v2/quote/ticker`    |
| MEXC    | `/api/v3/ticker/24hr`           | `/api/v1/contract/ticker`          |

For each row it canonicalises the venue-specific symbol form
(`XBTUSDTM` → `BTCUSDT`, `BTC_USDT` → `BTCUSDT`, `BTC-USDT` →
`BTCUSDT`, etc.) and records the venue's reported 24h USD volume.
Pairs are then ranked by **aggregate 24h USD volume** across all
listing venues, filtered to ≥5 venues, and the top *N* (default 100)
is emitted as a Markdown coverage matrix plus a YAML drop-in.

To regenerate (and overwrite `universe-100.json`):

```bash
python scripts/discover_universe.py --top 100 --min-venues 5 \
  --out-json docs/universe-100.json
```

## Manual curation on top of the discovery output

A few discovered names were dropped before merging into
`settings.example.yaml`:

* **Stable-against-USDT** (`USDC`, `USD1`, etc.) — quote currency is
  also USDT so the cross-venue spread is permanently $0; wastes a
  symbol slot.
* **Wrapped derivatives of an asset already in the list**
  (`WBTC`, `ZBT`) — they are *not* fungible with the underlying so
  the comparator would surface phantom arbs.
* **Tokens with versioned splits** that collide on the canonical
  form (`LUNA` classic vs `LUNA2`) — venues disagree about which
  one ships under `LUNAUSDT`, so signals are unreliable.
* **Confirmed micro-caps** that crossed the discovery threshold via a
  single venue's wash-traded volume only.

The full reject list is documented in `scripts/discover_universe.py`'s
output and reproducible.

## WS-subscription limits

Three of the 14 listeners need extra plumbing to subscribe to ~100
pairs without hitting venue limits. The other eleven take
`len(symbols) ~= 100` on a single connection without modification:

| listener            | limit                              | strategy                    |
|---------------------|------------------------------------|-----------------------------|
| `spot/bybit`        | 10 args per `op:subscribe`         | Send 10 sub frames on connect |
| `spot/mexc`         | 30 streams / connection            | Shard symbols across 4 parallel WS, 25/conn |
| `perp/mexc`         | 30 streams / connection            | Shard symbols across 4 parallel WS, 25/conn |
| `spot/binance`      | 1024 streams / conn (200 recommended) | unchanged — fits in one WS |
| `perp/binance`      | 200 streams / conn                 | unchanged                   |
| `perp/bybit`        | 200 args per request               | unchanged                   |
| `spot/gateio`       | 100+ args/sub                      | unchanged                   |
| `perp/gateio`       | 100+ args/sub                      | unchanged                   |
| `spot/bitget`       | high                               | unchanged                   |
| `perp/bitget`       | high                               | unchanged                   |
| `spot/kucoin`       | comma-separated topic, large       | unchanged                   |
| `perp/kucoin`       | comma-separated topic, large       | unchanged                   |
| `spot/bingx`        | 1 stream / sub message             | already loops per-symbol    |
| `perp/bingx`        | 1 stream / sub message             | already loops per-symbol    |

Sharding/chunking is implemented in `arbitrage/exchanges/spot/mexc.py`,
`arbitrage/exchanges/perp/mexc.py`, and `arbitrage/exchanges/spot/bybit.py`
and covered by `tests/test_subscription_limits.py`.

## Cardinality and resource estimates

Assuming ~100 symbols on each market:

* **Prometheus series for `arb_signals_total{market,symbol,buy_ex,sell_ex}`** —
  100 × 7 × 7 × 2 ≈ 9 800. Well within Prometheus's comfort zone;
  Grafana panels stay responsive.
* **`listener_last_tick_age_seconds`** — 14 series (one per listener).
* **`reconnects_total`** — 14 series.
* **`paper_pnl_total{leg}`** — 4 series.
* **MEXC connections** — 4 spot + 4 perp (sharded), the other 12
  listeners stay at 1 connection each. Total 20 long-lived WS
  connections to manage.
* **ticks.bin volume** — at ~10 BBO updates/sec/symbol/venue average,
  the binary tick log grows to ~1.5–2 GB / day uncompressed and
  ~400 MB / day after the daily zstd rotation. The 7-day retention
  policy keeps the on-disk footprint at ~3 GB.

## Composition of the 100-symbol list

### Spot (top 30 by aggregate 24h USD volume)

| # | symbol      | venues |
|---|-------------|--------|
| 1 | BTCUSDT     | 7/7    |
| 2 | ETHUSDT     | 7/7    |
| 3 | XRPUSDT     | 7/7    |
| 4 | SOLUSDT     | 7/7    |
| 5 | DOGEUSDT    | 7/7    |
| 6 | TAOUSDT     | 6/7    |
| 7 | DOTUSDT     | 7/7    |
| 8 | AAVEUSDT    | 7/7    |
| 9 | HYPEUSDT    | 6/7    |
| 10 | NEARUSDT   | 7/7    |
| 11 | PENGUUSDT  | 6/7    |
| 12 | ASTERUSDT  | 5/7    |
| 13 | BNBUSDT    | 7/7    |
| 14 | PEPEUSDT   | 7/7    |
| 15 | ADAUSDT    | 7/7    |
| 16 | UNIUSDT    | 7/7    |
| 17 | TRXUSDT    | 7/7    |
| 18 | SUIUSDT    | 7/7    |
| 19 | AVAXUSDT   | 7/7    |
| 20 | LTCUSDT    | 7/7    |
| 21 | LDOUSDT    | 6/7    |
| 22 | FILUSDT    | 7/7    |
| 23 | LINKUSDT   | 7/7    |
| 24 | APEUSDT    | 6/7    |
| 25 | HBARUSDT   | 7/7    |
| 26 | ATOMUSDT   | 7/7    |
| 27 | CFXUSDT    | 6/7    |
| 28 | ONDOUSDT   | 7/7    |
| 29 | SHIBUSDT   | 7/7    |
| 30 | ENAUSDT    | 6/7    |

(See [`universe-100.json`](universe-100.json) for the full 100.)

### Perp (top 30)

| # | symbol      | venues |
|---|-------------|--------|
| 1 | BTCUSDT     | 7/7    |
| 2 | ETHUSDT     | 7/7    |
| 3 | SOLUSDT     | 7/7    |
| 4 | TAOUSDT     | 6/7    |
| 5 | XRPUSDT     | 7/7    |
| 6 | XAUTUSDT    | 5/7    |
| 7 | DOGEUSDT    | 7/7    |
| 8 | ENAUSDT     | 6/7    |
| 9 | ORCAUSDT    | 5/7    |
| 10 | XMRUSDT    | 5/7    |
| 11 | SUIUSDT    | 7/7    |
| 12 | PENGUUSDT  | 6/7    |
| 13 | HYPEUSDT   | 6/7    |
| 14 | ZECUSDT    | 5/7    |
| 15 | BCHUSDT    | 7/7    |
| 16 | LINKUSDT   | 7/7    |
| 17 | LDOUSDT    | 6/7    |
| 18 | ADAUSDT    | 7/7    |
| 19 | AAVEUSDT   | 7/7    |
| 20 | AVAXUSDT   | 7/7    |
| 21 | BNBUSDT    | 7/7    |
| 22 | HBARUSDT   | 7/7    |
| 23 | XLMUSDT    | 7/7    |
| 24 | ICPUSDT    | 6/7    |
| 25 | FARTCOINUSDT | 5/7  |
| 26 | ASTERUSDT  | 5/7    |
| 27 | UNIUSDT    | 7/7    |
| 28 | LTCUSDT    | 7/7    |
| 29 | ONDOUSDT   | 7/7    |
| 30 | MASKUSDT   | 6/7    |

(See [`universe-100.json`](universe-100.json) for the full 100.)

## Trimming the universe locally

If you want to run a smaller universe (faster startup, lower
metric cardinality, less log noise), copy `settings.example.yaml`
to `settings.yaml` and shrink the `symbols.spot` / `symbols.perp`
lists. The 3-pair starter set we used during development was just:

```yaml
symbols:
  spot: [BTCUSDT, ETHUSDT, SOLUSDT]
  perp: [BTCUSDT, ETHUSDT, SOLUSDT]
```

The scanner has no minimum-symbol-count requirement; even a single
pair works.
