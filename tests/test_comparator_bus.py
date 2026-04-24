"""The comparator should publish ArbSignal events on the bus."""

from __future__ import annotations

import time

from arbitrage import comparator
from arbitrage.normalizer import Tick
from arbitrage.signals import ArbSignal, SignalBus, reset_bus, get_bus


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


def test_check_and_signal_emits_arb_on_bus(monkeypatch):
    # Isolated bus per test.
    reset_bus()
    got: list[ArbSignal] = []
    get_bus().register_arb(got.append)

    # Loosen the spot threshold so the synthetic ~0.8% spread registers.
    monkeypatch.setattr(comparator, "MIN_PROFIT_PCT", 0.1)

    now = int(time.time() * 1000)
    prices = {
        "BTCUSDT": {
            "binance": _tick("binance", 99.9, 100.0, now),
            "bybit":   _tick("bybit",   101.0, 101.1, now),
        }
    }
    comparator.check_and_signal_spot(prices, "BTCUSDT")

    assert len(got) == 1
    sig = got[0]
    assert sig.market == "spot"
    assert sig.symbol == "BTCUSDT"
    assert sig.buy_ex == "binance"
    assert sig.sell_ex == "bybit"
    assert 0.7 < sig.net_pct < 0.9


def test_check_and_signal_perp_emits_with_perp_market_tag():
    reset_bus()
    got: list[ArbSignal] = []
    get_bus().register_arb(got.append)

    # Directly set perp fees/threshold so the synthetic spread passes.
    old_fees, old_min = comparator.FEES_PERP, comparator.MIN_PROFIT_PCT_PERP
    comparator.FEES_PERP = {"a": 0.0, "b": 0.0}
    comparator.MIN_PROFIT_PCT_PERP = 0.1
    try:
        now = int(time.time() * 1000)
        prices = {
            "BTCUSDT": {
                "a": _tick("a", 99.9, 100.0, now),
                "b": _tick("b", 101.0, 101.1, now),
            }
        }
        comparator.check_and_signal_perp(prices, "BTCUSDT")
    finally:
        comparator.FEES_PERP, comparator.MIN_PROFIT_PCT_PERP = old_fees, old_min

    assert len(got) == 1
    assert got[0].market == "perp"


def test_no_arb_emits_nothing():
    reset_bus()
    got: list[ArbSignal] = []
    get_bus().register_arb(got.append)

    now = int(time.time() * 1000)
    prices = {
        "BTCUSDT": {
            "binance": _tick("binance", 100.0, 100.05, now),
            "bybit":   _tick("bybit",   100.06, 100.10, now),
        }
    }
    comparator.check_and_signal_spot(prices, "BTCUSDT")
    assert got == []
