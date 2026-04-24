"""AsyncTokenBucket semantics."""

from __future__ import annotations

import asyncio

import pytest

from arbitrage.ratelimit import AsyncTokenBucket


class _FakeClock:
    """Deterministic clock paired with a sleep that just advances it."""

    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


@pytest.mark.asyncio
async def test_full_bucket_is_instant():
    clock = _FakeClock()
    bucket = AsyncTokenBucket(5, tokens_per_s=10, clock=clock, sleep=clock.sleep)
    for _ in range(5):
        await bucket.acquire()
    assert clock.sleeps == []


@pytest.mark.asyncio
async def test_blocks_when_empty():
    clock = _FakeClock()
    bucket = AsyncTokenBucket(2, tokens_per_s=2, clock=clock, sleep=clock.sleep)
    await bucket.acquire()
    await bucket.acquire()
    # Third one needs to wait 0.5s (rate=2 tokens/s).
    await bucket.acquire()
    assert clock.sleeps == [0.5]


@pytest.mark.asyncio
async def test_fractional_tokens():
    clock = _FakeClock()
    bucket = AsyncTokenBucket(10, tokens_per_s=2, clock=clock, sleep=clock.sleep)
    # Acquire 3 tokens at once: need 3 >= 3, cap 10, start with 10.
    await bucket.acquire(3)
    assert bucket.tokens == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_refill_caps_at_capacity():
    clock = _FakeClock()
    bucket = AsyncTokenBucket(10, tokens_per_s=1, clock=clock, sleep=clock.sleep)
    await bucket.acquire(10)
    # Let a week pass — still capped.
    clock.t += 86_400 * 7
    assert bucket.tokens == 10.0


@pytest.mark.asyncio
async def test_fifo_ordering():
    clock = _FakeClock()
    bucket = AsyncTokenBucket(1, tokens_per_s=1, clock=clock, sleep=clock.sleep)

    order: list[int] = []

    async def task(i: int) -> None:
        await bucket.acquire()
        order.append(i)

    # Drain the starting token synchronously before launching waiters.
    await bucket.acquire()
    tasks = [asyncio.create_task(task(i)) for i in range(3)]
    await asyncio.gather(*tasks)
    assert order == [0, 1, 2]


@pytest.mark.asyncio
async def test_acquire_more_than_capacity_raises():
    bucket = AsyncTokenBucket(5, tokens_per_s=1)
    with pytest.raises(ValueError):
        await bucket.acquire(6)


@pytest.mark.asyncio
async def test_nonpositive_n_raises():
    bucket = AsyncTokenBucket(5, tokens_per_s=1)
    with pytest.raises(ValueError):
        await bucket.acquire(0)
    with pytest.raises(ValueError):
        await bucket.acquire(-1)


def test_invalid_capacity():
    with pytest.raises(ValueError):
        AsyncTokenBucket(0, tokens_per_s=1)


def test_invalid_rate():
    with pytest.raises(ValueError):
        AsyncTokenBucket(1, tokens_per_s=0)


@pytest.mark.asyncio
async def test_refill_is_proportional():
    clock = _FakeClock()
    # 5 tokens/s means 1 token every 200ms.
    bucket = AsyncTokenBucket(5, tokens_per_s=5, clock=clock, sleep=clock.sleep)
    await bucket.acquire(5)  # drain
    assert bucket.tokens == 0.0
    clock.t += 0.4   # 2 tokens refilled
    assert bucket.tokens == pytest.approx(2.0)
