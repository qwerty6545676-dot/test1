"""Heartbeat silence + recovery info events."""

from __future__ import annotations

import asyncio
import time

import pytest

from arbitrage import heartbeat
from arbitrage.normalizer import Tick
from arbitrage.signals import InfoEvent, SignalBus, reset_bus, get_bus


def _tick(exchange: str, ts_local: int) -> Tick:
    return Tick(
        exchange=exchange,
        symbol="BTCUSDT",
        bid=100.0,
        ask=100.1,
        ts_exchange=ts_local,
        ts_local=ts_local,
    )


async def _run_one_tick(monkeypatch, prices, **kwargs):
    """Run the monitor long enough to do exactly one iteration."""
    monkeypatch.setattr(heartbeat, "HEARTBEAT_INTERVAL_MS", 10)
    monkeypatch.setattr(heartbeat, "HEARTBEAT_TIMEOUT_MS", 50)

    received: list[InfoEvent] = []
    reset_bus()
    get_bus().register_info(received.append)

    task = asyncio.create_task(heartbeat.heartbeat_monitor(prices, **kwargs))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return received


@pytest.mark.asyncio
async def test_silence_emits_info_event(monkeypatch):
    now = int(time.time() * 1000)
    prices = {
        "BTCUSDT": {
            "binance": _tick("binance", now - 120_000),  # silent
            "bybit":   _tick("bybit",   now),
        }
    }
    # Start from the "silent" state by pre-setting last_seen via a
    # stale-first pass: `expected_exchanges` initializes last_seen to
    # startup_ms, so if 'binance' never writes a fresh tick within the
    # silence threshold we should see the silence event.
    received = await _run_one_tick(
        monkeypatch,
        prices,
        label="spot",
        expected_exchanges=("binance", "bybit"),
        silence_threshold_ms=1,  # trip immediately
    )
    kinds = [ev.kind for ev in received if ev.kind in ("silence", "recovery")]
    assert "silence" in kinds
    sil = next(ev for ev in received if ev.kind == "silence")
    assert sil.market == "spot"
    assert "binance" in sil.message


@pytest.mark.asyncio
async def test_recovery_emits_after_silence(monkeypatch):
    # Simulate silence then recovery by stepping the monitor twice
    # with changing data. We do this by patching `time.time` inside
    # the module instead of wall-clock sleeping.
    monkeypatch.setattr(heartbeat, "HEARTBEAT_INTERVAL_MS", 5)
    monkeypatch.setattr(heartbeat, "HEARTBEAT_TIMEOUT_MS", 50_000)

    received: list[InfoEvent] = []
    reset_bus()
    get_bus().register_info(received.append)

    now = int(time.time() * 1000)
    prices: dict[str, dict[str, Tick]] = {"BTCUSDT": {}}

    task = asyncio.create_task(
        heartbeat.heartbeat_monitor(
            prices,
            label="spot",
            expected_exchanges=("binance",),
            # 20ms threshold gives the recovery transition a real
            # margin: a tick injected at ``time.time()`` will satisfy
            # ``now_ms - tick.ts_local < 20`` for several iterations,
            # whereas threshold=1 only recovers if the iteration
            # happens within the same ms as the injection (race).
            silence_threshold_ms=20,
        )
    )
    # First tick: no data from binance at all -> silence.
    # Wait long enough that gap_ms >= 20.
    await asyncio.sleep(0.04)
    # Now inject a fresh tick -> the next iteration should recover.
    prices["BTCUSDT"]["binance"] = _tick("binance", int(time.time() * 1000))
    await asyncio.sleep(0.04)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    kinds = [ev.kind for ev in received]
    assert "silence" in kinds
    assert "recovery" in kinds


@pytest.mark.asyncio
async def test_no_silence_when_expected_exchanges_empty(monkeypatch):
    now = int(time.time() * 1000)
    prices = {"BTCUSDT": {"binance": _tick("binance", now - 120_000)}}
    received = await _run_one_tick(
        monkeypatch,
        prices,
        label="spot",
        expected_exchanges=(),
        silence_threshold_ms=1,
    )
    kinds = [ev.kind for ev in received]
    assert "silence" not in kinds
