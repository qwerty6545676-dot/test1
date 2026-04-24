"""Supervisor semantics: restart on crash, emit events, give up eventually."""

from __future__ import annotations

import asyncio

import pytest

from arbitrage.signals import InfoEvent, get_bus, reset_bus
from arbitrage.watchdog import supervise


def _collect_info() -> list[InfoEvent]:
    reset_bus()
    got: list[InfoEvent] = []
    get_bus().register_info(got.append)
    return got


@pytest.mark.asyncio
async def test_returns_cleanly_on_normal_completion():
    got = _collect_info()
    calls = [0]

    async def factory_coro() -> None:
        calls[0] += 1

    await supervise("normal", factory_coro)
    assert calls[0] == 1
    assert [e.kind for e in got] == []


@pytest.mark.asyncio
async def test_restarts_on_crash():
    got = _collect_info()
    calls = [0]

    async def factory_coro() -> None:
        calls[0] += 1
        if calls[0] < 3:
            raise RuntimeError("boom")
        # Third call returns cleanly -> supervisor stops.

    await supervise("crasher", factory_coro, base_backoff_s=0, max_backoff_s=0)
    assert calls[0] == 3
    kinds = [e.kind for e in got]
    assert kinds == ["watchdog", "watchdog"]
    assert all("crasher" in e.message for e in got)
    assert all(e.severity == "error" for e in got)


@pytest.mark.asyncio
async def test_gives_up_after_max_restarts():
    got = _collect_info()

    async def factory_coro() -> None:
        raise RuntimeError("always fails")

    with pytest.raises(RuntimeError):
        await supervise(
            "flapper",
            factory_coro,
            max_restarts=3,
            restart_window_s=60,
            base_backoff_s=0,
            max_backoff_s=0,
        )

    kinds = [e.kind for e in got]
    # 4 crashes (the 4th trips max_restarts) + 1 "giving up" event.
    assert kinds.count("watchdog") == 5
    assert "giving up" in got[-1].message


@pytest.mark.asyncio
async def test_passes_through_cancellation():
    got = _collect_info()

    async def factory_coro() -> None:
        await asyncio.sleep(10)

    task = asyncio.create_task(supervise("sleeper", factory_coro))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # No watchdog event should have been emitted for cancellation.
    assert [e.kind for e in got] == []


@pytest.mark.asyncio
async def test_restart_window_forgets_old_failures(monkeypatch):
    """Old failures outside the window should not count toward the cap."""
    got = _collect_info()
    fake_time = [100.0]

    from arbitrage import watchdog

    def fake_mono() -> float:
        return fake_time[0]

    monkeypatch.setattr(watchdog.time, "monotonic", fake_mono)

    call_times: list[float] = []

    async def factory_coro() -> None:
        call_times.append(fake_time[0])
        # First call: register the failure, advance past the window.
        # Second call: window has rolled over, restart count resets to 1.
        # Third call: succeed (clean return).
        if len(call_times) == 1:
            fake_time[0] += 100  # age out before next restart
            raise RuntimeError("first")
        if len(call_times) == 2:
            fake_time[0] += 100
            raise RuntimeError("second")

    await supervise(
        "windowed",
        factory_coro,
        max_restarts=1,
        restart_window_s=10,
        base_backoff_s=0,
        max_backoff_s=0,
    )

    # Three calls succeeded because each failure aged out before the
    # next one was recorded, so the window always saw count=1.
    assert len(call_times) == 3


@pytest.mark.asyncio
async def test_event_includes_exception_type_and_message():
    got = _collect_info()

    async def factory_coro() -> None:
        raise ValueError("something specific")

    with pytest.raises(ValueError):
        await supervise(
            "typer",
            factory_coro,
            max_restarts=0,
            base_backoff_s=0,
        )

    # First event is the restart record, second is the give-up.
    assert any("ValueError" in e.message for e in got)
    assert any("something specific" in e.message for e in got)


@pytest.mark.asyncio
async def test_market_is_tagged():
    got = _collect_info()

    async def factory_coro() -> None:
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        await supervise(
            "tagger",
            factory_coro,
            market="perp",
            max_restarts=0,
            base_backoff_s=0,
        )

    assert all(e.market == "perp" for e in got)
