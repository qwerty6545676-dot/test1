"""Integration tests for listener reconnect behaviour.

The existing per-listener parser tests cover "malformed frame doesn't
crash the parser". What's harder to test is the **outer loop**: if
the WS connection drops (for any reason), does the listener come
back up?

We target a single representative listener (``run_binance`` from
``arbitrage.exchanges.spot.binance``) and stub out the two points
where the outer loop interacts with the network:

* ``ws_connect`` — can be made to raise (simulating refused /
  unreachable) or return a fake transport.
* the returned transport's ``wait_disconnected`` — can be made to
  return immediately (simulating a server-side close) or raise
  (simulating an in-flight error).

All 14 listeners have the same ``while True:`` + ``sleep_backoff``
structure, so if Binance's path is exercised, the others follow
by induction. (We also separately unit-tested ``sleep_backoff``
itself in ``test_reconnect_notify.py``.)
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from arbitrage.exchanges.spot import binance as binance_mod


class _FakeTransport:
    def __init__(self, disconnect_behaviour: str = "return") -> None:
        # "return"     -> wait_disconnected returns immediately (clean close)
        # "raise"      -> wait_disconnected raises (in-flight error)
        # "block"      -> wait_disconnected never completes
        self.behaviour = disconnect_behaviour

    async def wait_disconnected(self) -> None:
        if self.behaviour == "return":
            return
        if self.behaviour == "raise":
            raise ConnectionResetError("simulated in-flight error")
        # "block"
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_reconnects_after_connect_error(monkeypatch):
    """Simulate 3 failed connect attempts then a hang — outer loop must retry each time."""
    connect_attempts = 0

    async def fake_ws_connect(factory, url, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal connect_attempts
        connect_attempts += 1
        if connect_attempts < 4:
            raise ConnectionRefusedError("refused")
        return _FakeTransport("block"), factory()

    sleep_calls: list[int] = []

    async def fake_sleep_backoff(attempt: int, **_kw: Any) -> None:
        sleep_calls.append(attempt)

    monkeypatch.setattr(binance_mod, "ws_connect", fake_ws_connect)
    monkeypatch.setattr(binance_mod, "sleep_backoff", fake_sleep_backoff)

    task = asyncio.create_task(binance_mod.run_binance({}, ("BTCUSDT",)))
    # Give the loop time to retry several times.
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # First three attempts raised and each one got a sleep_backoff.
    assert connect_attempts >= 4
    assert len(sleep_calls) >= 3
    # Attempt counter should grow on each failure.
    assert sleep_calls[0] == 0
    assert sleep_calls[1] == 1
    assert sleep_calls[2] == 2


@pytest.mark.asyncio
async def test_reconnects_after_server_close(monkeypatch):
    """``wait_disconnected`` returning cleanly = server closed us.

    We should go around the loop and try to reconnect (attempt counter
    is reset because the previous handshake succeeded)."""
    connect_attempts = 0

    async def fake_ws_connect(factory, url, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal connect_attempts
        connect_attempts += 1
        # Keep feeding clean-close transports for the first couple of
        # rounds, then hang so the test can cancel.
        if connect_attempts < 4:
            return _FakeTransport("return"), factory()
        return _FakeTransport("block"), factory()

    sleep_calls: list[int] = []

    async def fake_sleep_backoff(attempt: int, **_kw: Any) -> None:
        sleep_calls.append(attempt)

    monkeypatch.setattr(binance_mod, "ws_connect", fake_ws_connect)
    monkeypatch.setattr(binance_mod, "sleep_backoff", fake_sleep_backoff)

    task = asyncio.create_task(binance_mod.run_binance({}, ("BTCUSDT",)))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert connect_attempts >= 4
    # Since every handshake succeeded, attempt is reset to 0 before
    # each backoff — all calls should see attempt=0.
    assert all(a == 0 for a in sleep_calls)


@pytest.mark.asyncio
async def test_attempt_counter_resets_on_successful_handshake(monkeypatch):
    """Fail 3x, succeed once, fail again — the "succeed once" must reset attempt."""
    # Build a script: fail, fail, fail, succeed+clean-close, fail, block.
    script: list[str] = ["fail", "fail", "fail", "ok", "fail", "block"]
    idx = 0

    async def fake_ws_connect(factory, url, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal idx
        step = script[idx] if idx < len(script) else "block"
        idx += 1
        if step == "fail":
            raise OSError("boom")
        if step == "block":
            return _FakeTransport("block"), factory()
        # "ok"
        return _FakeTransport("return"), factory()

    sleeps: list[int] = []

    async def fake_sleep_backoff(attempt: int, **_kw: Any) -> None:
        sleeps.append(attempt)

    monkeypatch.setattr(binance_mod, "ws_connect", fake_ws_connect)
    monkeypatch.setattr(binance_mod, "sleep_backoff", fake_sleep_backoff)

    task = asyncio.create_task(binance_mod.run_binance({}, ("BTCUSDT",)))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Expected sequence of sleep_backoff(attempt) calls: [0, 1, 2, 0, 1].
    # Fails 1..3 see the counter growing (0, 1, 2). After the clean
    # handshake on step 4, attempt is reset to 0 and sleep_backoff(0)
    # fires — then the counter is incremented to 1 before the next
    # loop iteration, so the step-5 fail sees attempt=1. The reset
    # to 0 after the non-zero run is the meaningful invariant.
    assert any(a > 0 for a in sleeps)
    assert 0 in sleeps[3:]


@pytest.mark.asyncio
async def test_exception_during_wait_disconnected_is_caught(monkeypatch):
    """A mid-stream exception should be treated like a disconnect."""
    connect_attempts = 0

    async def fake_ws_connect(factory, url, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal connect_attempts
        connect_attempts += 1
        if connect_attempts < 3:
            return _FakeTransport("raise"), factory()
        return _FakeTransport("block"), factory()

    sleep_calls = 0

    async def fake_sleep_backoff(*_args: Any, **_kw: Any) -> None:
        nonlocal sleep_calls
        sleep_calls += 1

    monkeypatch.setattr(binance_mod, "ws_connect", fake_ws_connect)
    monkeypatch.setattr(binance_mod, "sleep_backoff", fake_sleep_backoff)

    task = asyncio.create_task(binance_mod.run_binance({}, ("BTCUSDT",)))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # We reconnected at least twice after the in-flight errors.
    assert connect_attempts >= 3
    assert sleep_calls >= 2


@pytest.mark.asyncio
async def test_url_contains_configured_symbols():
    """Sanity check: the subscription URL names every symbol we asked for."""
    url = binance_mod._build_url(("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    assert "btcusdt@bookTicker" in url
    assert "ethusdt@bookTicker" in url
    assert "solusdt@bookTicker" in url
