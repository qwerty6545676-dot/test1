"""Async token-bucket rate limiter for outbound REST calls.

The canonical use case is KuCoin's bullet-public bootstrap (called
once per reconnect for both the spot and futures listeners). If many
of our WS connections get booted simultaneously — say, a KuCoin
gateway hiccup — each listener re-requests a new token from the same
IP. Without coordination we can exceed KuCoin's 30 req / 3 s public
REST quota and start getting throttled (HTTP 429), which in turn
delays reconnects further.

One ``AsyncTokenBucket`` instance is shared by everything hitting the
same REST endpoint family. ``acquire()`` blocks until a token is
available, preserving FIFO order because we hold an ``asyncio.Lock``
across the whole wait.

Design decisions
----------------
* **Monotonic clock** (``time.monotonic``) — robust to wall-clock
  jumps.  Injectable so tests can step time deterministically.
* **Injectable sleeper** — tests replace ``asyncio.sleep`` with a
  coroutine that schedules an immediate callback rather than waiting
  real seconds.
* **Sub-second refill** is supported (``tokens_per_s=0.5`` means one
  token every 2 seconds).
* ``acquire(n)`` is rejected if ``n > capacity`` — otherwise the call
  would deadlock forever.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

Clock = Callable[[], float]
Sleeper = Callable[[float], Awaitable[None]]


class AsyncTokenBucket:
    """Token bucket with fractional tokens and monotonic-clock refill."""

    __slots__ = (
        "_capacity",
        "_rate",
        "_tokens",
        "_last",
        "_clock",
        "_sleep",
        "_lock",
    )

    def __init__(
        self,
        capacity: float,
        tokens_per_s: float,
        *,
        clock: Clock | None = None,
        sleep: Sleeper | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if tokens_per_s <= 0:
            raise ValueError("tokens_per_s must be > 0")
        self._capacity = float(capacity)
        self._rate = float(tokens_per_s)
        self._tokens = float(capacity)
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._last = self._clock()
        # Lazily created on first acquire() so the bucket can be
        # constructed on a different loop than it's consumed on.
        self._lock: asyncio.Lock | None = None

    @property
    def tokens(self) -> float:
        """Currently available tokens (snapshot, not a reservation)."""
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now

    async def acquire(self, n: float = 1.0) -> None:
        """Block until ``n`` tokens are available, then deduct them.

        Raises ``ValueError`` if ``n`` exceeds the bucket's capacity
        (otherwise the call would wait forever).
        """
        if n <= 0:
            raise ValueError("n must be > 0")
        if n > self._capacity:
            raise ValueError(
                f"n={n} exceeds bucket capacity={self._capacity}"
            )

        if self._lock is None:
            self._lock = asyncio.Lock()

        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                # Wait for exactly the missing delta to be refilled.
                wait_s = (n - self._tokens) / self._rate
                await self._sleep(wait_s)


__all__ = ["AsyncTokenBucket"]
