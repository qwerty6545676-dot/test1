"""Tests for the REST extractors and coverage-block builder in
``scripts/discover_universe.py``.

We don't hit the network here — each test passes a hand-crafted REST
payload (matching the shape documented at the venue's API docs) and
checks that the extractor:

* keeps actively-trading pairs,
* drops paused / delisted ones via the venue-specific status field,
* canonicalises the symbol to ``BASEUSDT``,
* preserves the volume number for downstream ranking.

The Tier-3 WS verify path is *not* exercised here (it spawns real
listeners against live venues); that lives in an integration smoke
test the user runs by hand via ``python -m scripts.discover_universe``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    """Import scripts/discover_universe.py as a module without
    requiring a ``scripts/__init__.py``. Mirrors how pytest discovers
    other ad-hoc scripts in the tree."""
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "scripts" / "discover_universe.py"
    spec = importlib.util.spec_from_file_location("_discover_universe_test", src)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_discover_universe_test"] = mod
    spec.loader.exec_module(mod)
    return mod


_du = _load_module()


# ---- Status filter: spot ------------------------------------------


def test_bitget_spot_filters_offline_status():
    """Bitget v2 spot tags non-trading pairs as ``status: "offline"``
    or ``"halt"``; the extractor must drop them so they don't leak
    into discovery as live universe entries."""
    payload = {
        "data": [
            {"symbol": "BTCUSDT", "status": "online", "usdtVolume": "1000"},
            {"symbol": "DEADUSDT", "status": "offline", "usdtVolume": "500"},
            {"symbol": "PAUSUSDT", "status": "halt", "usdtVolume": "200"},
            # No status field (older API shape) — keep, defensive default.
            {"symbol": "OLDUSDT", "usdtVolume": "300"},
        ]
    }
    assert _du._bitget_spot(payload) == [
        ("BTCUSDT", 1000.0),
        ("OLDUSDT", 300.0),
    ]


# ---- Status filter: perp ------------------------------------------


def test_bitget_perp_filters_non_normal_symbol_status():
    payload = {
        "data": [
            {"symbol": "BTCUSDT", "symbolStatus": "normal", "usdtVolume": "10"},
            {"symbol": "PAUSUSDT", "symbolStatus": "maintain", "usdtVolume": "5"},
            {"symbol": "OFFUSDT", "symbolStatus": "offline", "usdtVolume": "2"},
            {"symbol": "OLDUSDT", "usdtVolume": "1"},  # missing → keep
        ]
    }
    assert _du._bitget_perp(payload) == [("BTCUSDT", 10.0), ("OLDUSDT", 1.0)]


def test_kucoin_perp_filters_non_open_status_and_unwraps_xbt():
    payload = {
        "data": [
            {"symbol": "XBTUSDTM", "status": "Open", "turnoverOf24h": "100"},
            {"symbol": "ETHUSDTM", "status": "Open", "turnoverOf24h": "50"},
            {"symbol": "DEADUSDTM", "status": "Pause", "turnoverOf24h": "1"},
            {"symbol": "DELUSDTM", "status": "BeingDelivered", "turnoverOf24h": "1"},
        ]
    }
    rows = _du._kucoin_perp(payload)
    # XBT -> BTC rewrite still happens, status filter drops Pause/BeingDelivered.
    assert rows == [("BTCUSDT", 100.0), ("ETHUSDT", 50.0)]


def test_mexc_perp_filters_non_zero_state():
    payload = {
        "data": [
            {"symbol": "BTC_USDT", "state": 0, "amount24": "100"},
            {"symbol": "PAUS_USDT", "state": 1, "amount24": "5"},   # delisted
            {"symbol": "DEL_USDT", "state": 2, "amount24": "5"},    # delisting
            {"symbol": "OLD_USDT", "amount24": "10"},               # missing → keep
        ]
    }
    assert _du._mexc_perp(payload) == [("BTC_USDT".replace("_", ""), 100.0),
                                       ("OLDUSDT", 10.0)]


def test_mexc_perp_tolerates_non_numeric_state():
    """Defensive: if MEXC ever returns a string ``state`` (or any
    other unexpected shape) the extractor must not blow up — the rest
    of MEXC's payload would otherwise be lost via the outer
    ``_fetch`` exception catch. The float coercion in ``_f`` returns
    0.0 for unparseable input, which evaluates as ``state == 0`` and
    keeps the row (treated as enabled). That's safer than dropping
    everything when the API shape mutates.
    """
    payload = {
        "data": [
            {"symbol": "BTC_USDT", "state": "weird", "amount24": "100"},
            {"symbol": "ETH_USDT", "state": "0", "amount24": "50"},  # numeric str
            {"symbol": "DEAD_USDT", "state": "1", "amount24": "5"},  # delisted as str
        ]
    }
    rows = _du._mexc_perp(payload)
    syms = [s for s, _ in rows]
    # Unparseable -> kept (defensive); "0" -> kept; "1" -> dropped.
    assert "BTCUSDT" in syms
    assert "ETHUSDT" in syms
    assert "DEADUSDT" not in syms


def test_gateio_perp_skips_in_delisting():
    payload = [
        {"contract": "BTC_USDT", "volume_24h_settle": "100"},
        {"contract": "DEAD_USDT", "in_delisting": True, "volume_24h_settle": "5"},
        {"contract": "ETH_USDT", "in_delisting": False, "volume_24h_settle": "50"},
    ]
    rows = _du._gateio_perp(payload)
    assert rows == [("BTCUSDT", 100.0), ("ETHUSDT", 50.0)]


# ---- min_venues=2 ranking -----------------------------------------


def test_rank_drops_pairs_below_min_venues():
    """A pair on a single venue can't be arbitraged — the ranker
    must drop it regardless of volume."""
    universe = {
        ("spot", "BTCUSDT"): {"binance": 1_000, "bybit": 500},
        ("spot", "LONELYUSDT"): {"binance": 10_000_000_000},  # huge but solo
        ("spot", "MIDUSDT"): {"binance": 100, "bybit": 100, "mexc": 100},
    }
    ranked = _du._rank(universe, "spot", min_venues=2, top=10)
    syms = [r[0] for r in ranked]
    assert "LONELYUSDT" not in syms
    assert syms == ["BTCUSDT", "MIDUSDT"]  # ranked by aggregate (1500 > 300)


# ---- Coverage-block builder ---------------------------------------


def test_coverage_block_spot_uses_bare_venue_labels():
    ranked = [
        ("BTCUSDT", 1500.0, 2, {"binance": 1000, "bybit": 500}),
        ("ETHUSDT", 800.0, 3, {"binance": 300, "kucoin": 250, "mexc": 250}),
    ]
    block = _du._coverage_block("spot", ranked)
    assert block == {
        # _VENUES order is preserved, so output is stable across runs.
        "BTCUSDT": ["binance", "bybit"],
        "ETHUSDT": ["binance", "kucoin", "mexc"],
    }


def test_coverage_block_perp_suffixes_venue_labels():
    """Perp listeners use ``Tick.exchange = "binance-perp"`` etc.;
    the coverage block must match that convention so settings.yaml
    validation (and the listener filter in main._run) accepts it.
    """
    ranked = [
        ("BTCUSDT", 1500.0, 2, {"binance": 1000, "bybit": 500}),
    ]
    block = _du._coverage_block("perp", ranked)
    assert block == {"BTCUSDT": ["binance-perp", "bybit-perp"]}


# ---- Apply WS verify ----------------------------------------------


def test_apply_verify_drops_unverified_tier3_entries_and_re_filters():
    """Pairs that lose every Tier-3 venue and end up below 2 total
    venues must be removed from the result entirely."""
    ranked = [
        # Survives: kucoin gets dropped, but binance+bybit remain.
        ("BTCUSDT", 1500.0, 3, {"binance": 1000, "bybit": 400, "kucoin": 100}),
        # Removed: the only second venue was bingx, which didn't push.
        ("DEADUSDT", 50.0, 2, {"binance": 30, "bingx": 20}),
        # Survives unchanged: no Tier-3 venues at all.
        ("ETHUSDT", 800.0, 2, {"binance": 500, "bybit": 300}),
    ]
    pushed_by_venue = {
        "mexc": set(),
        "kucoin": set(),     # didn't push BTC on kucoin
        "bingx": set(),      # didn't push DEAD on bingx
    }
    cleaned = _du._apply_verify(ranked, pushed_by_venue)
    syms = [r[0] for r in cleaned]
    assert "DEADUSDT" not in syms
    assert syms == ["BTCUSDT", "ETHUSDT"]
    # BTC's coverage no longer includes kucoin.
    btc = next(r for r in cleaned if r[0] == "BTCUSDT")
    assert "kucoin" not in btc[3]
    assert btc[3] == {"binance": 1000, "bybit": 400}


def test_apply_verify_keeps_tier3_entry_when_it_pushed():
    ranked = [
        ("ALTUSDT", 200.0, 2, {"binance": 100, "mexc": 100}),
    ]
    cleaned = _du._apply_verify(ranked, {"mexc": {"ALTUSDT"}, "kucoin": set(), "bingx": set()})
    assert cleaned == [("ALTUSDT", 200.0, 2, {"binance": 100, "mexc": 100})]
