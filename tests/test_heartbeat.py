import asyncio
import time

import pytest

from arbitrage import heartbeat
from arbitrage.normalizer import Tick


def _tick(exchange: str, ts_local: int) -> Tick:
    return Tick(
        exchange=exchange,
        symbol="BTCUSDT",
        bid=100.0,
        ask=100.1,
        ts_exchange=ts_local,
        ts_local=ts_local,
    )


@pytest.mark.asyncio
async def test_heartbeat_evicts_stale_entries(monkeypatch):
    # Force a fast heartbeat interval and short timeout for the test.
    monkeypatch.setattr(heartbeat, "HEARTBEAT_INTERVAL_MS", 10)
    monkeypatch.setattr(heartbeat, "HEARTBEAT_TIMEOUT_MS", 50)

    now = int(time.time() * 1000)
    prices = {
        "BTCUSDT": {
            "binance": _tick("binance", now - 5000),   # stale
            "bybit":   _tick("bybit",   now),          # fresh
        }
    }

    task = asyncio.create_task(heartbeat.heartbeat_monitor(prices))
    # Give the monitor one tick to run.
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert "binance" not in prices["BTCUSDT"]
    assert "bybit" in prices["BTCUSDT"]
