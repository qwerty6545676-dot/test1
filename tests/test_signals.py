"""SignalBus pub/sub semantics."""

from __future__ import annotations

import logging

from arbitrage import signals as signals_mod
from arbitrage.signals import (
    ArbSignal,
    InfoEvent,
    SignalBus,
    emit_info,
    get_bus,
    reset_bus,
)


def test_arb_signal_key_is_stable():
    s = ArbSignal(
        ts_ms=1,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="binance",
        sell_ex="bybit",
        buy_ask=100.0,
        sell_bid=101.0,
        net_pct=1.0,
    )
    assert s.key == ("spot", "BTCUSDT", "binance", "bybit")


def test_bus_invokes_registered_handlers_in_order():
    bus = SignalBus()
    seen: list[str] = []
    bus.register_arb(lambda s: seen.append(f"a:{s.symbol}"))
    bus.register_arb(lambda s: seen.append("b"))
    sig = ArbSignal(
        ts_ms=1,
        market="spot",
        symbol="ETHUSDT",
        buy_ex="a",
        sell_ex="b",
        buy_ask=1.0,
        sell_bid=1.1,
        net_pct=5.0,
    )
    bus.emit_arb(sig)
    assert seen == ["a:ETHUSDT", "b"]


def test_bus_handler_exception_does_not_break_other_handlers(caplog):
    bus = SignalBus()

    def broken(_s):
        raise RuntimeError("boom")

    seen: list[InfoEvent] = []
    bus.register_info(broken)
    bus.register_info(seen.append)

    ev = InfoEvent(ts_ms=0, kind="notice", message="x")
    with caplog.at_level(logging.ERROR, logger="arbitrage.signals"):
        bus.emit_info(ev)

    assert seen == [ev]
    assert any("info handler failed" in r.message for r in caplog.records)


def test_bus_reset_clears_handlers():
    bus = SignalBus()
    calls: list[int] = []
    bus.register_arb(lambda _: calls.append(1))
    bus.reset()
    bus.emit_arb(
        ArbSignal(
            ts_ms=0,
            market="spot",
            symbol="X",
            buy_ex="a",
            sell_ex="b",
            buy_ask=1.0,
            sell_bid=1.0,
            net_pct=0.0,
        )
    )
    assert calls == []


def test_get_bus_is_singleton_until_reset():
    a = get_bus()
    b = get_bus()
    assert a is b
    reset_bus()
    c = get_bus()
    assert c is not a


def test_emit_info_helper_fills_timestamp(monkeypatch):
    reset_bus()
    received: list[InfoEvent] = []
    get_bus().register_info(received.append)

    # Freeze time to make the ts deterministic.
    monkeypatch.setattr(signals_mod.time, "time", lambda: 1_700_000_000.123)
    emit_info("startup", "hi", market="spot", severity="info")

    assert len(received) == 1
    ev = received[0]
    assert ev.kind == "startup"
    assert ev.message == "hi"
    assert ev.market == "spot"
    assert ev.severity == "info"
    assert ev.ts_ms == 1_700_000_000_123
