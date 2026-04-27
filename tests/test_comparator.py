import logging
import time

import pytest

from arbitrage import comparator
from arbitrage.comparator import find_arbitrage
from arbitrage.normalizer import Tick


# Tests exercise sub-1% arbs; keep the threshold low here regardless of
# what settings.yaml says for prod.
@pytest.fixture(autouse=True)
def _low_threshold(monkeypatch):
    monkeypatch.setattr(comparator, "MIN_PROFIT_PCT", 0.15)


def _tick(exchange: str, bid: float, ask: float, ts_local: int | None = None) -> Tick:
    if ts_local is None:
        ts_local = int(time.time() * 1000)
    return Tick(
        exchange=exchange,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        ts_exchange=ts_local,
        ts_local=ts_local,
    )


def test_detects_profitable_arb():
    now = int(time.time() * 1000)
    book = {
        "binance": _tick("binance", 99.9, 100.0, now),
        "bybit":   _tick("bybit",   101.0, 101.1, now),
    }
    hit = find_arbitrage(book, "BTCUSDT", now)
    assert hit is not None
    buy_ex, sell_ex, buy_ask, sell_bid, net_pct = hit
    assert buy_ex == "binance"
    assert sell_ex == "bybit"
    assert buy_ask == 100.0
    assert sell_bid == 101.0
    # Fees are 0.10% both sides -> net ≈ (101*(1-0.001)) / (100*(1+0.001)) - 1
    # = 100.899 / 100.1 - 1 = 0.798%
    assert 0.79 < net_pct < 0.81


def test_no_arb_below_threshold():
    now = int(time.time() * 1000)
    book = {
        "binance": _tick("binance", 100.0, 100.05, now),
        "bybit":   _tick("bybit",   100.06, 100.10, now),
    }
    # Gross spread is tiny (~0.01%), fees (0.2%) eat it.
    assert find_arbitrage(book, "BTCUSDT", now) is None


def test_stale_ticks_ignored():
    now = int(time.time() * 1000)
    book = {
        "binance": _tick("binance", 99.9, 100.0, now - 2000),  # stale
        "bybit":   _tick("bybit",   101.0, 101.1, now),
    }
    assert find_arbitrage(book, "BTCUSDT", now) is None


def test_single_exchange_returns_none():
    now = int(time.time() * 1000)
    assert find_arbitrage({"binance": _tick("binance", 100.0, 100.1, now)}, "BTCUSDT", now) is None


def test_check_and_signal_logs_on_arb(caplog):
    now = int(time.time() * 1000)
    prices: dict[str, dict[str, Tick]] = {
        "BTCUSDT": {
            "binance": _tick("binance", 99.9, 100.0, now),
            "bybit":   _tick("bybit",   101.0, 101.1, now),
        }
    }
    with caplog.at_level(logging.INFO, logger="arbitrage.comparator"):
        comparator.check_and_signal(prices, "BTCUSDT")
    # Log line is "ARB [spot] BTCUSDT: ..." — just check both pieces appear.
    assert any("ARB" in r.message and "BTCUSDT" in r.message for r in caplog.records)


def test_check_and_signal_silent_without_arb(caplog):
    now = int(time.time() * 1000)
    prices: dict[str, dict[str, Tick]] = {
        "BTCUSDT": {
            "binance": _tick("binance", 100.0, 100.05, now),
            "bybit":   _tick("bybit",   100.06, 100.10, now),
        }
    }
    with caplog.at_level(logging.INFO, logger="arbitrage.comparator"):
        comparator.check_and_signal(prices, "BTCUSDT")
    assert not any("ARB " in r.message for r in caplog.records)


def test_check_and_signal_skips_unknown_symbol():
    comparator.check_and_signal({}, "DOESNOTEXIST")
    # just make sure it doesn't throw


def test_find_arbitrage_accepts_custom_fees_and_min_pct():
    """Same book, different fee tables -> different outcomes."""
    now = int(time.time() * 1000)
    book = {
        "a": _tick("a", 99.9, 100.0, now),
        "b": _tick("b", 101.0, 101.1, now),
    }
    # Zero fees, threshold well below the ~1% gross spread -> hit.
    hit = find_arbitrage(
        book, "BTCUSDT", now, fees={"a": 0.0, "b": 0.0}, min_pct=0.1
    )
    assert hit is not None
    # Fat fees eat the whole spread -> no hit, even with same threshold.
    assert (
        find_arbitrage(
            book, "BTCUSDT", now, fees={"a": 0.02, "b": 0.02}, min_pct=0.1
        )
        is None
    )


def test_check_and_signal_perp_labels_log(caplog):
    """Perp variant must emit `[perp]` in the prefix."""
    now = int(time.time() * 1000)
    prices: dict[str, dict[str, Tick]] = {
        "BTCUSDT": {
            "a": _tick("a", 99.9, 100.0, now),
            "b": _tick("b", 101.0, 101.1, now),
        }
    }
    # Ensure the threshold is low enough for the synthetic spread.
    old_fees, old_min = comparator.FEES_PERP, comparator.MIN_PROFIT_PCT_PERP
    comparator.FEES_PERP = {"a": 0.0, "b": 0.0}
    comparator.MIN_PROFIT_PCT_PERP = 0.1
    try:
        with caplog.at_level(logging.INFO, logger="arbitrage.comparator"):
            comparator.check_and_signal_perp(prices, "BTCUSDT")
    finally:
        comparator.FEES_PERP, comparator.MIN_PROFIT_PCT_PERP = old_fees, old_min

    assert any("[perp]" in r.message and "BTCUSDT" in r.message for r in caplog.records)


def test_find_arbitrage_strips_perp_suffix_from_fee_lookup():
    """Perp exchange labels carry a '-perp' suffix ("binance-perp")
    but settings.yaml fees use bare names. The comparator must
    strip the suffix before looking up the fee or net_pct will be
    over-reported (fees silently default to 0.0)."""
    now = int(time.time() * 1000)
    # Book keyed by suffixed labels, as the real perp listeners emit.
    book = {
        "binance-perp": _tick("binance-perp", bid=99.95, ask=100.0, ts_local=now),
        "bybit-perp":   _tick("bybit-perp",   bid=100.0,  ask=100.05, ts_local=now),
    }
    # Bare-name fee table as loaded from settings.yaml.
    fees = {"binance": 0.002, "bybit": 0.002}

    # Spread is tiny (~0.05%) and 0.4% round-trip fees eat it.
    # If the suffix is NOT stripped, fees.get("binance-perp", 0.0)
    # returns 0, effective prices collapse, and net_pct falsely
    # clears the 0.01% threshold.
    from arbitrage.comparator import find_arbitrage

    hit = find_arbitrage(book, "BTCUSDT", now, fees=fees, min_pct=0.01, max_age_ms=5_000)
    assert hit is None, f"suffix not stripped — false hit: {hit!r}"
