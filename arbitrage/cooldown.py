"""Per-key cooldown tracker.

Used by the Telegram notifier to avoid spamming the same arb over and
over — the same ``(market, symbol, buy_ex, sell_ex)`` tuple will only
fire once every ``cooldown_seconds``.

The tracker is pure Python and keeps no async state, so it works the
same way from every thread / coroutine.
"""

from __future__ import annotations

import time
from typing import Any, Hashable


class CooldownTracker:
    """Dict-backed TTL cooldown.

    ``should_emit(key)`` returns True at most once per
    ``ttl_seconds`` per key; when it returns True it also updates the
    stored timestamp so the next call within the window returns False.
    """

    __slots__ = ("_ttl_s", "_last", "_now")

    def __init__(
        self,
        ttl_seconds: float,
        *,
        now: Any = None,
    ) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        self._ttl_s = float(ttl_seconds)
        self._last: dict[Hashable, float] = {}
        # Injectable clock for deterministic tests.
        self._now = now or time.time

    def should_emit(self, key: Hashable) -> bool:
        now = self._now()
        last = self._last.get(key, 0.0)
        if now - last < self._ttl_s:
            return False
        self._last[key] = now
        return True

    def reset(self, key: Hashable | None = None) -> None:
        if key is None:
            self._last.clear()
        else:
            self._last.pop(key, None)

    def __len__(self) -> int:
        return len(self._last)
