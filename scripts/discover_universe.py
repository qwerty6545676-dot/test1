"""Pick the top-N USDT pairs we should track and emit per-venue coverage.

Hybrid discovery — V5 (anchor-cascade + status filter + Tier 3 WS verify):

1. **REST cascade with status filter**. Pull 24h ticker data from
   every supported venue. Only keep pairs whose venue-specific
   ``status`` field marks them as actively trading (``TRADING``,
   ``Open``, ``status==1``, ...). Pairs that are listed in REST but
   paused / delisted are dropped here, before they ever hit the
   coverage matrix.

2. **Aggregate by symbol → ``{symbol: {venue: usd_volume_24h}}``**.
   This matrix is the cascade — every venue that lists a pair
   contributes a row. Order does not matter, but we iterate venues
   in a fixed priority list so logs read top-down.

3. **min_venues >= 2** (was 5). Arbitrage requires comparing across
   venues, so 2 is the real arithmetic minimum. Anything below 2 is
   discarded.

4. **Rank by aggregate USD volume** and take the top-N (default 100).

5. **Tier 3 WS-verify**. For ``mexc`` / ``kucoin`` / ``bingx`` —
   the venues whose REST listings most often mismatch what their WS
   actually streams — open the production listener for ~``--ws-probe-secs``
   and drop ``(venue, symbol)`` pairs that pushed zero ``bookTicker``
   updates in that window. After the verify pass, re-apply
   min_venues >= 2 to drop pairs that lost their second venue.

6. **Emit settings drop-in**. Final output is a ``coverage:`` block
   for ``settings.yaml`` with one entry per symbol, listing the venues
   that genuinely stream it. ``arbitrage.main`` reads this block and
   subscribes each listener only to its allowed-set, which prevents
   the heartbeat-evict log spam (~66k WARNs / 30 min at universe-100)
   that motivated this rewrite.

Run:

    python scripts/discover_universe.py --top 100

Output written to stdout (Markdown table + YAML drop-in) and
optionally to ``--out-json`` and ``--out-yaml`` for downstream tools.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp

# When invoked as ``python scripts/discover_universe.py``, Python sets
# ``sys.path[0]`` to the script's directory (``scripts/``), not the
# repo root — so the Tier-3 WS-verify phase can't import
# ``arbitrage.exchanges``. Inject the repo root explicitly so the
# script works the same whether it's run as a module or a file.
_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)


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
# zero volume" instead of crashing the discovery run. Status fields
# are treated the same way: when the venue doesn't expose one (or it
# differs between endpoints), we skip the filter rather than over-
# rejecting valid pairs.


def _f(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


# ---- Spot extractors ---------------------------------------------

def _binance_spot(payload: list[dict]) -> list[tuple[str, float]]:
    # Binance /ticker/24hr does not expose status; we use the volume
    # filter (vol > 0) downstream as a proxy. ``count`` (number of
    # 24h trades) further down the pipeline catches dead pairs.
    # Binance returns a dict (``{"code": 0, "msg": "..."}``) when a
    # request comes from a restricted location (HTTP 451) — guard
    # against that so the extractor doesn't crash and zero out
    # everything else.
    if not isinstance(payload, list):
        return []
    out: list[tuple[str, float]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
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
        # Bitget v2 spot: ``status`` is "online" for tradable pairs.
        status = (r.get("status") or "").lower()
        if status and status not in {"online", "trading", "open"}:
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
    # See ``_binance_spot`` — same defensive shape check.
    if not isinstance(payload, list):
        return []
    out: list[tuple[str, float]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
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
        # Gate.io futures: ``in_delisting`` flips true once delisting
        # is announced; the contract still appears in /tickers for a
        # while afterwards. Drop those before they pollute coverage.
        if r.get("in_delisting"):
            continue
        out.append((sym, _f(r.get("volume_24h_settle") or r.get("volume_24h"))))
    return out


def _bitget_perp(payload: dict) -> list[tuple[str, float]]:
    rows = payload.get("data", [])
    out: list[tuple[str, float]] = []
    for r in rows:
        sym = r.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        # Bitget perp v2: ``symbolStatus`` is ``"normal"`` while live;
        # ``maintain``/``offline``/``halt`` mean don't subscribe.
        status = (r.get("symbolStatus") or "").lower()
        if status and status != "normal":
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
        # ``status`` is ``"Open"`` for actively trading contracts.
        # ``"Pause"``/``"BeingDelivered"``/``"Settled"`` show up here too.
        status = (r.get("status") or "").lower()
        if status and status != "open":
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
        # MEXC contract /ticker includes ``state`` (0=enabled,
        # 1=delisted, ...). Drop anything but enabled. Use ``_f`` so
        # an unexpected non-numeric value (string, None, ...) doesn't
        # crash the whole extractor and zero out MEXC perp coverage —
        # matches the defensive contract documented at the top of
        # this file.
        state = r.get("state")
        if state is not None and _f(state) != 0:
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


# Order matters for log readability (cascade is logged top-down) but
# is functionally irrelevant for the resulting coverage matrix.
SOURCES: list[Source] = [
    Source("binance", "spot", "https://api.binance.com/api/v3/ticker/24hr", _binance_spot),
    Source("binance", "perp", "https://fapi.binance.com/fapi/v1/ticker/24hr", _binance_perp),
    Source("bybit", "spot", "https://api.bybit.com/v5/market/tickers?category=spot", _bybit_spot),
    Source("bybit", "perp", "https://api.bybit.com/v5/market/tickers?category=linear", _bybit_perp),
    Source("mexc", "spot", "https://api.mexc.com/api/v3/ticker/24hr", _mexc_spot),
    Source(
        "mexc", "perp",
        "https://contract.mexc.com/api/v1/contract/ticker",
        _mexc_perp,
    ),
    Source("bitget", "spot", "https://api.bitget.com/api/v2/spot/market/tickers", _bitget_spot),
    Source(
        "bitget", "perp",
        "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES",
        _bitget_perp,
    ),
    Source("kucoin", "spot", "https://api.kucoin.com/api/v1/market/allTickers", _kucoin_spot),
    Source("kucoin", "perp", "https://api-futures.kucoin.com/api/v1/contracts/active", _kucoin_perp),
    Source("gateio", "spot", "https://api.gateio.ws/api/v4/spot/tickers", _gateio_spot),
    Source("gateio", "perp", "https://api.gateio.ws/api/v4/futures/usdt/tickers", _gateio_perp),
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
]


_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=20)

# Tier 3 = venues whose REST listings frequently overstate what WS
# actually pushes. Discovery runs a short live probe against these to
# drop dead (venue, symbol) pairs before they hit the production
# coverage map.
_TIER3 = ("mexc", "kucoin", "bingx")


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


# ---- Tier 3 WS verification ---------------------------------------

async def _ws_probe_venue(
    market: str,
    venue: str,
    candidates: list[str],
    *,
    duration_s: float,
) -> set[str]:
    """Open the production listener for ``venue`` and return the set
    of candidate symbols that pushed at least one ``bookTicker``
    within ``duration_s``.

    Reuses the listener code (gzip framing, KuCoin bullet bootstrap,
    BingX keepalive, MEXC sharding etc.) so the probe is bit-for-bit
    representative of what the live scanner sees.

    The listener writes into a throwaway ``prices`` dict whose keys
    are the symbols we're verifying; absence after ``duration_s``
    means the venue is silent on that pair and we should not include
    it in the coverage map.
    """
    # Imported lazily so ``--help`` / unit tests don't pull in picows.
    from arbitrage.exchanges import perp as perp_mod
    from arbitrage.exchanges import spot as spot_mod

    if market == "spot":
        runner = {
            "mexc": spot_mod.run_mexc,
            "kucoin": spot_mod.run_kucoin,
            "bingx": spot_mod.run_bingx,
        }[venue]
    else:
        runner = {
            "mexc": perp_mod.run_mexc,
            "kucoin": perp_mod.run_kucoin,
            "bingx": perp_mod.run_bingx,
        }[venue]

    prices: dict[str, dict[str, Any]] = {}
    task = asyncio.create_task(runner(prices, tuple(candidates)))
    try:
        await asyncio.sleep(duration_s)
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass

    venue_label = venue if market == "spot" else f"{venue}-perp"
    seen: set[str] = set()
    for sym, book in prices.items():
        if venue_label in book:
            seen.add(sym)
    return seen


async def _verify_tier3(
    ranked: dict[str, list[tuple[str, float, int, dict[str, float]]]],
    *,
    duration_s: float,
) -> dict[str, dict[str, set[str]]]:
    """Run the WS probe for every Tier-3 (venue, market) combination.

    Returns ``{market: {venue: set_of_pushed_symbols}}``.
    """
    by_market: dict[str, dict[str, set[str]]] = {"spot": {}, "perp": {}}

    coros = []
    keys: list[tuple[str, str, list[str]]] = []
    for market, rows in ranked.items():
        for venue in _TIER3:
            candidates = [sym for sym, _agg, _n, vols in rows if venue in vols]
            if not candidates:
                continue
            coros.append(
                _ws_probe_venue(market, venue, candidates, duration_s=duration_s)
            )
            keys.append((market, venue, candidates))

    print(
        f"\n>> Tier-3 WS verify: {len(coros)} probes × {duration_s:.0f}s "
        f"(total wall ≈ {duration_s:.0f}s, probes run concurrently)",
        file=sys.stderr,
    )
    results = await asyncio.gather(*coros, return_exceptions=True)

    for (market, venue, candidates), res in zip(keys, results):
        if isinstance(res, BaseException):
            print(
                f"   {market}/{venue}: probe failed ({res!r}); keeping all "
                f"{len(candidates)} candidates",
                file=sys.stderr,
            )
            by_market[market][venue] = set(candidates)
            continue
        dropped = len(candidates) - len(res)
        print(
            f"   {market}/{venue}: {len(res)}/{len(candidates)} pushed "
            f"(dropped {dropped})",
            file=sys.stderr,
        )
        by_market[market][venue] = res

    return by_market


def _apply_verify(
    ranked: list[tuple[str, float, int, dict[str, float]]],
    pushed_by_venue: dict[str, set[str]],
) -> list[tuple[str, float, int, dict[str, float]]]:
    """Drop ``(venue, symbol)`` entries the WS probe didn't see, then
    re-apply ``min_venues >= 2`` so pairs that lost their only second
    venue go away too.
    """
    cleaned: list[tuple[str, float, int, dict[str, float]]] = []
    for sym, _agg, _n, vols in ranked:
        new_vols = dict(vols)
        for venue in _TIER3:
            if venue in new_vols:
                pushed = pushed_by_venue.get(venue, set())
                if sym not in pushed:
                    del new_vols[venue]
        if len(new_vols) < 2:
            continue
        agg = sum(new_vols.values())
        cleaned.append((sym, agg, len(new_vols), new_vols))
    cleaned.sort(key=lambda r: r[1], reverse=True)
    return cleaned


# ---- Output ------------------------------------------------------

_VENUES = ["binance", "bybit", "gateio", "bitget", "kucoin", "bingx", "mexc"]


def _print_md_table(market: str, ranked: list[tuple[str, float, int, dict[str, float]]]) -> None:
    print(f"\n## {market.upper()} top-{len(ranked)}")
    header = ["#", "symbol", "agg_24h_usd", "venues"] + _VENUES
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join(["---"] * len(header)) + "|")
    for i, (sym, agg, n, vols) in enumerate(ranked, start=1):
        cells = [
            str(i),
            sym,
            f"${agg/1e6:.1f}M",
            f"{n}/7",
        ]
        for v in _VENUES:
            cells.append("✓" if v in vols else "·")
        print("| " + " | ".join(cells) + " |")


def _coverage_block(
    market: str,
    ranked: list[tuple[str, float, int, dict[str, float]]],
) -> dict[str, list[str]]:
    """Build ``{symbol: [venue, venue, ...]}`` for one market.

    For perp, the venue labels are ``-perp``-suffixed to match the
    ``Tick.exchange`` convention used by the listeners and the
    ``coverage`` schema in :mod:`arbitrage.settings`.
    """
    suffix = "-perp" if market == "perp" else ""
    return {
        sym: [v + suffix for v in _VENUES if v in vols]
        for sym, _agg, _n, vols in ranked
    }


def _print_yaml_dropin(
    spot: list[tuple[str, float, int, dict[str, float]]],
    perp: list[tuple[str, float, int, dict[str, float]]],
) -> None:
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

    print("coverage:")
    for market_name, ranked in (("spot", spot), ("perp", perp)):
        print(f"  {market_name}:")
        block = _coverage_block(market_name, ranked)
        for sym, venues in block.items():
            venue_csv = ", ".join(venues)
            print(f"    {sym}: [{venue_csv}]")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=100)
    ap.add_argument(
        "--min-venues", type=int, default=2,
        help="A pair must list on at least this many venues to be "
             "eligible. 2 is the arithmetic minimum for arbitrage; "
             "raise it to bias toward pairs with broader coverage.",
    )
    ap.add_argument(
        "--ws-probe-secs", type=float, default=15.0,
        help="How long to run the Tier-3 WS verification probe. Set "
             "to 0 to skip the probe entirely (REST-only discovery).",
    )
    ap.add_argument("--out-json", default="",
                    help="If set, also writes the final coverage map "
                         "as JSON for downstream tools.")
    args = ap.parse_args()

    universe = asyncio.run(_gather())

    spot = _rank(universe, "spot", args.min_venues, args.top)
    perp = _rank(universe, "perp", args.min_venues, args.top)

    if args.ws_probe_secs > 0:
        pushed = asyncio.run(
            _verify_tier3({"spot": spot, "perp": perp}, duration_s=args.ws_probe_secs)
        )
        spot = _apply_verify(spot, pushed["spot"])
        perp = _apply_verify(perp, pushed["perp"])

    _print_md_table("spot", spot)
    _print_md_table("perp", perp)
    _print_yaml_dropin(spot, perp)

    spot_syms = [s for s, *_ in spot]
    perp_syms = [s for s, *_ in perp]
    print("\n### Intersection (in both spot AND perp top-N)")
    both = [s for s in spot_syms if s in perp_syms]
    print(f"{len(both)} symbols: {both}")

    if args.out_json:
        with open(args.out_json, "w") as f:
            json.dump(
                {
                    "spot_symbols": spot_syms,
                    "perp_symbols": perp_syms,
                    "coverage": {
                        "spot": _coverage_block("spot", spot),
                        "perp": _coverage_block("perp", perp),
                    },
                },
                f,
                indent=2,
            )
        print(f"\nWrote JSON to {args.out_json}")


if __name__ == "__main__":
    main()
