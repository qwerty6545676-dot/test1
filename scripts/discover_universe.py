"""Pick the top-N USDT pairs we should track.

Pulls 24h ticker data from every supported exchange (spot + perp),
canonicalises symbols to ``BASEUSDT`` form, ranks by **aggregate USD
volume across all venues that list the pair**, and prints the
top-N entries together with a venue-coverage matrix.

Why this isn't a one-liner against a global aggregator like CoinGecko:

* CoinGecko's "top by volume" includes wash-traded pairs and CEX/DEX
  pairs we can't actually arbitrage with our seven listeners.
* We need the **intersection** with our specific seven venues —
  a pair that sums to $5B on aggregators but only lists on Binance
  is useless to us.
* We also need spot- and perp-side coverage *separately*, since
  some venues list a coin on spot but not perp (or vice versa).

Run:

    python scripts/discover_universe.py --top 100 --min-venues 5

Output is a Markdown table that gets pasted into
``docs/universe-100.md``, plus a Python list ready to drop into
``settings.example.yaml``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import aiohttp


# ---- Exchange spec ------------------------------------------------
#
# Each entry maps a venue label to its public REST URL plus the
# extractor that returns ``[(canonical_symbol, usd_volume), ...]``.
#
# Canonical = ``BASEUSDT``. KuCoin perp's ``XBTUSDTM`` quirk is
# unwound here so the cross-venue intersection works.
#
# The extractors are deliberately defensive: missing fields, malformed
# floats, and empty arrays all degrade to "this pair contributes
# zero volume" instead of crashing the discovery run.


def _f(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


# ---- Spot extractors ---------------------------------------------

def _binance_spot(payload: list[dict]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for row in payload:
        sym = row.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        out.append((sym, _f(row.get("quoteVolume"))))
    return out


def _bybit_spot(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("result", {}).get("list", [])
    return [
        (r["symbol"], _f(r.get("turnover24h")))
        for r in rows
        if r.get("symbol", "").endswith("USDT")
    ]


def _gateio_spot(payload: list[dict]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for row in payload:
        cur = row.get("currency_pair", "")
        if not cur.endswith("_USDT"):
            continue
        sym = cur.replace("_", "")
        out.append((sym, _f(row.get("quote_volume"))))
    return out


def _bitget_spot(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", [])
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        out.append((sym, _f(r.get("usdtVolume") or r.get("quoteVol"))))
    return out


def _kucoin_spot(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", {}).get("ticker", [])
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("-USDT"):
            continue
        out.append((sym.replace("-", ""), _f(r.get("volValue"))))
    return out


def _bingx_spot(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", []) or []
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("-USDT"):
            continue
        out.append((sym.replace("-", ""), _f(r.get("quoteVolume"))))
    return out


def _mexc_spot(payload: list[dict]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for r in payload:
        sym = r.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        out.append((sym, _f(r.get("quoteVolume"))))
    return out


# ---- Perp extractors ----------------------------------------------

def _binance_perp(payload: list[dict]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for row in payload:
        sym = row.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        out.append((sym, _f(row.get("quoteVolume"))))
    return out


def _bybit_perp(payload: dict) -> list[tuple[str, float]]:
    return _bybit_spot(payload)  # same shape, different category param


def _gateio_perp(payload: list[dict]) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for r in payload:
        contract = r.get("contract", "")
        if not contract.endswith("_USDT"):
            continue
        sym = contract.replace("_", "")
        # gate perp returns volume_24h_usd or volume_24h_settle
        out.append((sym, _f(r.get("volume_24h_settle") or r.get("volume_24h"))))
    return out


def _bitget_perp(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", [])
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        out.append((sym, _f(r.get("usdtVolume") or r.get("quoteVolume"))))
    return out


def _kucoin_perp(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", []) or []
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        # KuCoin perp uses ``XBTUSDTM`` for BTC and ``…USDTM`` for the rest.
        if not sym.endswith("USDTM"):
            continue
        base = sym[:-5]
        if base == "XBT":
            base = "BTC"
        out.append((f"{base}USDT", _f(r.get("turnoverOf24h"))))
    return out


def _bingx_perp(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", []) or []
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("-USDT"):
            continue
        out.append((sym.replace("-", ""), _f(r.get("quoteVolume"))))
    return out


def _mexc_perp(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", []) or []
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("_USDT"):
            continue
        out.append((sym.replace("_", ""), _f(r.get("amount24"))))
    return out


@dataclass
class Source:
    label: str
    market: str  # "spot" or "perp"
    url: str
    extractor: Any  # Callable[[Any], list[tuple[str, float]]]
    headers: dict[str, str] = field(default_factory=dict)


SOURCES: list[Source] = [
    Source("binance", "spot", "https://api.binance.com/api/v3/ticker/24hr", _binance_spot),
    Source("binance", "perp", "https://fapi.binance.com/fapi/v1/ticker/24hr", _binance_perp),
    Source("bybit", "spot", "https://api.bybit.com/v5/market/tickers?category=spot", _bybit_spot),
    Source("bybit", "perp", "https://api.bybit.com/v5/market/tickers?category=linear", _bybit_perp),
    Source("gateio", "spot", "https://api.gateio.ws/api/v4/spot/tickers", _gateio_spot),
    Source("gateio", "perp", "https://api.gateio.ws/api/v4/futures/usdt/tickers", _gateio_perp),
    Source("bitget", "spot", "https://api.bitget.com/api/v2/spot/market/tickers", _bitget_spot),
    Source(
        "bitget", "perp",
        "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        _bitget_perp,
    ),
    Source("kucoin", "spot", "https://api.kucoin.com/api/v1/market/allTickers", _kucoin_spot),
    Source("kucoin", "perp", "https://api-futures.kucoin.com/api/v1/contracts/active", _kucoin_perp),
    Source(
        "bingx", "spot",
        "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
        _bingx_spot,
    ),
    Source(
        "bingx", "perp",
        "https://open-api.bingx.com/openApi/swap/v2/quote/ticker",
        _bingx_perp,
    ),
    Source("mexc", "spot", "https://api.mexc.com/api/v3/ticker/24hr", _mexc_spot),
    Source(
        "mexc", "perp",
        "https://contract.mexc.com/api/v1/contract/ticker",
        _mexc_perp,
    ),
]


_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=20)


async def _fetch(session: aiohttp.ClientSession, src: Source) -> tuple[Source, list[tuple[str, float]]]:
    try:
        async with session.get(src.url, headers=src.headers, timeout=_FETCH_TIMEOUT) as resp:
            payload = await resp.json(content_type=None)
        rows = src.extractor(payload)
        # Filter out zero-volume entries: they distort the rank when
        # a venue lists a coin but nobody trades it.
        rows = [(s, v) for s, v in rows if v > 0]
        return src, rows
    except Exception as exc:  # noqa: BLE001
        print(f"{src.label}-{src.market}: {exc}", file=sys.stderr)
        return src, []


async def _gather() -> dict[tuple[str, str], dict[str, float]]:
    """Returns ``{(market, symbol): {venue: usd_volume}}``."""
    out: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    async with aiohttp.ClientSession() as s:
        results = await asyncio.gather(*(_fetch(s, src) for src in SOURCES))
    for src, rows in results:
        for sym, vol in rows:
            out[(src.market, sym)][src.label] = vol
    return out


def _rank(
    universe: dict[tuple[str, str], dict[str, float]],
    market: str,
    min_venues: int,
    top: int,
) -> list[tuple[str, float, int, dict[str, float]]]:
    rows: list[tuple[str, float, int, dict[str, float]]] = []
    for (m, sym), vols in universe.items():
        if m != market:
            continue
        if len(vols) < min_venues:
            continue
        agg = sum(vols.values())
        rows.append((sym, agg, len(vols), vols))
    rows.sort(key=lambda r: r[1], reverse=True)
    return rows[:top]


def _print_md_table(market: str, ranked: list[tuple[str, float, int, dict[str, float]]]) -> None:
    venues = ["binance", "bybit", "gateio", "bitget", "kucoin", "bingx", "mexc"]
    print(f"\n## {market.upper()} top-{len(ranked)}")
    header = ["#", "symbol", "agg_24h_usd", "venues"] + venues
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join(["---"] * len(header)) + "|")
    for i, (sym, agg, n, vols) in enumerate(ranked, start=1):
        cells = [
            str(i),
            sym,
            f"${agg/1e6:.1f}M",
            f"{n}/7",
        ]
        for v in venues:
            cells.append("✓" if v in vols else "·")
        print("| " + " | ".join(cells) + " |")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument("--min-venues", type=int, default=5,
                    help="A pair must list on at least this many venues to "
                         "be eligible — anything less can't be arbitraged.")
    ap.add_argument("--out-json", default="",
                    help="If set, also writes the final {spot:[], perp:[]} "
                         "list as JSON for downstream tools.")
    args = ap.parse_args()

    universe = asyncio.run(_gather())

    spot = _rank(universe, "spot", args.min_venues, args.top)
    perp = _rank(universe, "perp", args.min_venues, args.top)

    _print_md_table("spot", spot)
    _print_md_table("perp", perp)

    spot_syms = [s for s, *_ in spot]
    perp_syms = [s for s, *_ in perp]

    print("\n### settings.yaml drop-in")
    print("symbols:")
    print("  spot:")
    for s in spot_syms:
        print(f"    - {s}")
    print("  perp:")
    for s in perp_syms:
        print(f"    - {s}")

    print("\n### Intersection (in both spot AND perp top-N)")
    both = [s for s in spot_syms if s in perp_syms]
    print(f"{len(both)} symbols: {both}")

    if args.out_json:
        with open(args.out_json, "w") as f:
            json.dump({"spot": spot_syms, "perp": perp_syms}, f, indent=2)
        print(f"\nWrote JSON to {args.out_json}")


if __name__ == "__main__":
    main()
