"""BusErrorHandler: ERROR logs become InfoEvents on the bus."""

from __future__ import annotations

import logging

from arbitrage.signals import InfoEvent, SignalBus
from arbitrage.telegram_notify.log_handler import BusErrorHandler


def _capturing_bus() -> tuple[SignalBus, list[InfoEvent]]:
    bus = SignalBus()
    received: list[InfoEvent] = []
    bus.register_info(received.append)
    return bus, received


def _record(name: str, level: int, msg: str, exc_info=None) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="x",
        lineno=1,
        msg=msg,
        args=None,
        exc_info=exc_info,
    )


def test_error_record_becomes_info_event():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)

    handler.handle(_record("arbitrage.binance", logging.ERROR, "broken"))

    assert len(got) == 1
    ev = got[0]
    assert ev.kind == "error"
    assert ev.severity == "error"
    assert "broken" in ev.message
    assert "arbitrage.binance" in ev.message


def test_warning_is_ignored():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)
    handler.handle(_record("arbitrage.binance", logging.WARNING, "flapping"))
    assert got == []


def test_telegram_logs_are_dropped_to_avoid_recursion():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)
    handler.handle(_record("arbitrage.telegram.client", logging.ERROR, "failed"))
    handler.handle(_record("arbitrage.telegram.notifier", logging.ERROR, "failed"))
    assert got == []


def test_signals_logger_is_dropped():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)
    handler.handle(_record("arbitrage.signals", logging.ERROR, "handler raised"))
    assert got == []


def test_exception_type_prefixed_onto_message():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)
    try:
        raise RuntimeError("nope")
    except RuntimeError:
        import sys

        handler.handle(
            _record("arbitrage.kucoin", logging.ERROR, "bad", exc_info=sys.exc_info())
        )

    assert len(got) == 1
    assert "RuntimeError" in got[0].message


def test_message_is_truncated():
    bus, got = _capturing_bus()
    handler = BusErrorHandler(bus)
    long = "x" * 5_000
    handler.handle(_record("arbitrage.binance", logging.ERROR, long))
    assert len(got) == 1
    # Prefix ("arbitrage.binance: ") plus ≤500-char message.
    assert len(got[0].message) < 600


def test_handler_default_level_is_error():
    handler = BusErrorHandler(SignalBus())
    assert handler.level == logging.ERROR
