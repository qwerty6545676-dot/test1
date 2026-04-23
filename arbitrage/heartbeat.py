"""Background heartbeat monitor.

Evicts per-exchange entries whose `ts_local` is older than
`HEARTBEAT_TIMEOUT_MS`. Stale entries are collected into a list first
and deleted in a second pass, to avoid
`RuntimeError: dictionary changed size during iteration`.

One monitor task runs per prices-book (one for `prices_spot`, one for
`prices_perp`). The ``label`` kwarg is included in the log line so
operators can tell which market evicted which listener.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .comparator import PricesBook
from .config import HEARTBEAT_INTERVAL_MS, HEARTBEAT_TIMEOUT_MS

logger = logging.getLogger("arbitrage.heartbeat")


async def heartbeat_monitor(prices: PricesBook, *, label: str = "spot") -> None:
    interval_s = HEARTBEAT_INTERVAL_MS / 1000.0
    timeout_ms = HEARTBEAT_TIMEOUT_MS

    while True:
        await asyncio.sleep(interval_s)
        now_ms = int(time.time() * 1000)

        to_evict: list[tuple[str, str]] = []
        for symbol, book in prices.items():
            for exchange, tick in book.items():
                if now_ms - tick.ts_local > timeout_ms:
                    to_evict.append((symbol, exchange))

        if not to_evict:
            continue

        for symbol, exchange in to_evict:
            book = prices.get(symbol)
            if book is None:
                continue
            book.pop(exchange, None)
            logger.warning(
                "heartbeat[%s]: evicted stale %s/%s (>%dms)",
                label,
                exchange,
                symbol,
                timeout_ms,
            )
