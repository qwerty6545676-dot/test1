"""sleep_backoff emits reconnect info events when exchange + market are set."""

from __future__ import annotations

import pytest

from arbitrage.exchanges import _common
from arbitrage.signals import InfoEvent, get_bus, reset_bus


@pytest.mark.asyncio
async def test_sleep_backoff_without_exchange_emits_nothing(monkeypatch):
    reset_bus()
    got: list[InfoEvent] = []
    get_bus().register_info(got.append)

    async def _noop(_delay):
        return None

    monkeypatch.setattr(_common.asyncio, "sleep", _noop)
    monkeypatch.setattr(_common, "backoff_delay", lambda _a: 0.0)

    await _common.sleep_backoff(0)
    assert got == []


@pytest.mark.asyncio
async def test_sleep_backoff_emits_reconnect(monkeypatch):
    reset_bus()
    got: list[InfoEvent] = []
    get_bus().register_info(got.append)

    async def _noop(_delay):
        return None

    monkeypatch.setattr(_common.asyncio, "sleep", _noop)
    monkeypatch.setattr(_common, "backoff_delay", lambda _a: 1.5)

    await _common.sleep_backoff(2, exchange="binance", market="spot")
    assert len(got) == 1
    ev = got[0]
    assert ev.kind == "reconnect"
    assert ev.market == "spot"
    assert "binance" in ev.message
    assert "attempt 3" in ev.message


@pytest.mark.asyncio
async def test_sleep_backoff_perp_market(monkeypatch):
    reset_bus()
    got: list[InfoEvent] = []
    get_bus().register_info(got.append)

    async def _noop(_delay):
        return None

    monkeypatch.setattr(_common.asyncio, "sleep", _noop)
    monkeypatch.setattr(_common, "backoff_delay", lambda _a: 0.0)

    await _common.sleep_backoff(0, exchange="kucoin-perp", market="perp")
    assert len(got) == 1
    assert got[0].market == "perp"
    assert "kucoin-perp" in got[0].message
