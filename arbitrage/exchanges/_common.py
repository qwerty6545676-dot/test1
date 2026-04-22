"""Small helpers shared by all exchange handlers."""

from __future__ import annotations

import asyncio
import random

from ..config import BACKOFF_JITTER_S, BACKOFF_MAX_S


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter.

    attempt is 0-based. The +jitter spreads simultaneous reconnects so
    we don't hammer the exchange (and trigger an IP ban) if all venues
    go down at once.
    """
    base = min(2.0 ** attempt, BACKOFF_MAX_S)
    return base + random.uniform(0.0, BACKOFF_JITTER_S)


async def sleep_backoff(attempt: int) -> None:
    await asyncio.sleep(backoff_delay(attempt))
