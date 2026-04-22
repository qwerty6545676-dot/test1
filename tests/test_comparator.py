import logging
import time

from arbitrage import comparator
from arbitrage.comparator import find_arbitrage
from arbitrage.normalizer import Tick


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
    assert any("ARB BTCUSDT" in r.message for r in caplog.records)


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
