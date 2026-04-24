"""SignalsWriter: append-only JSON Lines subscriber."""

from __future__ import annotations

import json
import logging

import pytest

from arbitrage.persistence import SignalsWriter
from arbitrage.signals import ArbSignal, SignalBus


def _sig(**overrides) -> ArbSignal:
    base = dict(
        ts_ms=1_700_000_000_000,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="binance",
        sell_ex="bybit",
        buy_ask=100.0,
        sell_bid=101.5,
        net_pct=1.5,
    )
    base.update(overrides)
    return ArbSignal(**base)


def test_writes_one_line_per_signal(tmp_path):
    path = tmp_path / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()
    try:
        writer._on_arb(_sig())
        writer._on_arb(_sig(symbol="ETHUSDT", net_pct=2.7))
    finally:
        writer.close()

    lines = path.read_text().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first == {
        "ts_ms": 1_700_000_000_000,
        "market": "spot",
        "symbol": "BTCUSDT",
        "buy_ex": "binance",
        "sell_ex": "bybit",
        "buy_ask": 100.0,
        "sell_bid": 101.5,
        "net_pct": 1.5,
    }
    assert json.loads(lines[1])["symbol"] == "ETHUSDT"


def test_append_preserves_previous_contents(tmp_path):
    path = tmp_path / "signals.jsonl"
    path.write_text('{"prev":"line"}\n')

    writer = SignalsWriter(path)
    writer.open()
    try:
        writer._on_arb(_sig())
    finally:
        writer.close()

    lines = path.read_text().splitlines()
    assert lines[0] == '{"prev":"line"}'
    assert json.loads(lines[1])["symbol"] == "BTCUSDT"


def test_attach_wires_bus(tmp_path):
    path = tmp_path / "signals.jsonl"
    bus = SignalBus()
    writer = SignalsWriter(path)
    writer.open()
    writer.attach(bus)
    try:
        bus.emit_arb(_sig())
        bus.emit_arb(_sig(symbol="SOLUSDT"))
    finally:
        writer.close()

    lines = path.read_text().splitlines()
    assert len(lines) == 2


def test_attach_is_idempotent(tmp_path):
    path = tmp_path / "signals.jsonl"
    bus = SignalBus()
    writer = SignalsWriter(path)
    writer.open()
    writer.attach(bus)
    writer.attach(bus)  # second attach should NOT register a second handler
    try:
        bus.emit_arb(_sig())
    finally:
        writer.close()

    assert len(path.read_text().splitlines()) == 1


def test_creates_parent_directory(tmp_path):
    path = tmp_path / "nested" / "dir" / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()
    writer._on_arb(_sig())
    writer.close()
    assert path.exists()


def test_close_is_idempotent(tmp_path):
    path = tmp_path / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()
    writer.close()
    writer.close()   # must not raise


def test_write_before_open_is_warned_not_raised(tmp_path, caplog):
    writer = SignalsWriter(tmp_path / "signals.jsonl")
    with caplog.at_level(logging.WARNING, logger="arbitrage.persistence.signals"):
        writer._on_arb(_sig())
    assert any("file not open" in r.message for r in caplog.records)


def test_line_buffering_flushes_each_signal(tmp_path):
    """After each handler call the file should already reflect it."""
    path = tmp_path / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()
    try:
        writer._on_arb(_sig())
        # No explicit flush() call; line-buffered IO should have done it.
        assert path.read_text().count("\n") == 1
        writer._on_arb(_sig(symbol="ETHUSDT"))
        assert path.read_text().count("\n") == 2
    finally:
        writer.close()


def test_write_failure_is_logged_not_raised(tmp_path, caplog, monkeypatch):
    path = tmp_path / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()

    class _Boom:
        def write(self, _):
            raise OSError("disk full")

        def close(self):
            pass

    # Swap the file handle out for something that always fails.
    writer._file = _Boom()  # type: ignore[assignment]

    with caplog.at_level(logging.ERROR, logger="arbitrage.persistence.signals"):
        # Must not raise.
        writer._on_arb(_sig())

    assert any("write failed" in r.message for r in caplog.records)


def test_perp_market_serialized(tmp_path):
    path = tmp_path / "signals.jsonl"
    writer = SignalsWriter(path)
    writer.open()
    try:
        writer._on_arb(_sig(market="perp"))
    finally:
        writer.close()

    payload = json.loads(path.read_text().splitlines()[0])
    assert payload["market"] == "perp"
