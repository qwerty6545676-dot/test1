"""Background heartbeat + silence monitor.

Two jobs in one task so we only walk the prices-book once per tick:

1. **Heartbeat eviction** — drop per-exchange entries whose
   ``ts_local`` is older than ``HEARTBEAT_TIMEOUT_MS``. Keeps the
   comparator from seeing stale quotes if a listener wedges.

2. **Silence / recovery alerts** — when an exchange goes quiet for
   ``silence_threshold_ms`` (default 30 s) an info event fires; when
   ticks resume a recovery event fires. Unlike heartbeat eviction,
   silence tracks *the whole exchange across all symbols*, so a flap
   only produces one message per transition instead of one per pair.

Stale entries are collected into a list first and deleted in a
second pass to avoid ``RuntimeError: dictionary changed size during
iteration``.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .comparator import PricesBook
from .config import HEARTBEAT_INTERVAL_MS, HEARTBEAT_TIMEOUT_MS
from .signals import InfoEvent, get_bus

logger = logging.getLogger("arbitrage.heartbeat")

# An exchange that goes this long without publishing a single tick
# for *any* symbol is considered silent. 30 s is long enough that
# normal jitter / brief reconnects don't trigger it, short enough
# that a real outage is visible within a minute.
_DEFAULT_SILENCE_MS = 30_000


async def heartbeat_monitor(
    prices: PricesBook,
    *,
    label: str = "spot",
    expected_exchanges: tuple[str, ...] = (),
    silence_threshold_ms: int = _DEFAULT_SILENCE_MS,
) -> None:
    """Evict stale ticks and emit silence / recovery info events.

    ``expected_exchanges`` is the list of ``Tick.exchange`` strings
    this book is *supposed* to receive from. It's used only for
    silence tracking: if it's empty we skip the silence check and
    keep the plain eviction behaviour. The main entry point passes
    the seven per-market labels (e.g. ``"binance"`` / ``"bybit"``
    for spot, or the ``"-perp"`` variants for perp).
    """
    interval_s = HEARTBEAT_INTERVAL_MS / 1000.0
    timeout_ms = HEARTBEAT_TIMEOUT_MS
    bus = get_bus()

    # Treat every expected exchange as "seen at startup" so we don't
    # fire a silence alert the first time the loop runs before ticks
    # have had a chance to arrive.
    startup_ms = int(time.time() * 1000)
    last_seen: dict[str, int] = {ex: startup_ms for ex in expected_exchanges}
    silent: set[str] = set()

    while True:
        await asyncio.sleep(interval_s)
        now_ms = int(time.time() * 1000)

        # --- Walk the book once: collect evictions, refresh last_seen.
        to_evict: list[tuple[str, str]] = []
        for symbol, book in prices.items():
            for exchange, tick in book.items():
                if exchange in last_seen and tick.ts_local > last_seen[exchange]:
                    last_seen[exchange] = tick.ts_local
                if now_ms - tick.ts_local > timeout_ms:
                    to_evict.append((symbol, exchange))

        # --- Second pass: do the evictions.
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

        # --- Silence / recovery transitions.
        if not expected_exchanges:
            continue

        for exchange, last in last_seen.items():
            gap_ms = now_ms - last
            if gap_ms >= silence_threshold_ms and exchange not in silent:
                silent.add(exchange)
                bus.emit_info(
                    InfoEvent(
                        ts_ms=now_ms,
                        kind="silence",
                        message=(
                            f"{exchange}: no ticks for {gap_ms // 1000}s"
                        ),
                        market=_as_market(label),
                        severity="warn",
                    )
                )
                logger.warning(
                    "heartbeat[%s]: %s silent for %ds",
                    label,
                    exchange,
                    gap_ms // 1000,
                )
            elif gap_ms < silence_threshold_ms and exchange in silent:
                silent.discard(exchange)
                bus.emit_info(
                    InfoEvent(
                        ts_ms=now_ms,
                        kind="recovery",
                        message=f"{exchange}: ticks resumed",
                        market=_as_market(label),
                        severity="info",
                    )
                )
                logger.info(
                    "heartbeat[%s]: %s recovered",
                    label,
                    exchange,
                )


def _as_market(label: str):
    """Accept any label string but only forward the two we care about."""
    if label == "spot":
        return "spot"
    if label == "perp":
        return "perp"
    return None
