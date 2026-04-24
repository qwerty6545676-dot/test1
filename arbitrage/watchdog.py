"""Supervisor for long-running listener tasks.

Every WS listener in :mod:`arbitrage.exchanges` already has an
internal ``while True`` loop that catches connection errors,
sleeps with jittered backoff, and reconnects. That covers 99 % of
real-world failures. ``supervise()`` catches the remaining 1 %:

* A bug that crashes the inner loop (e.g. an ``AttributeError`` in
  the parser) — without a wrapper the task just silently dies and
  that venue disappears from our price book until restart.
* A third-party library raising something we forgot to catch.
* A sudden spike of exceptions that makes the self-healing loop
  spin too hot — after too many restarts in a window we give up
  so the process as a whole can be restarted by an external
  supervisor (systemd, Docker, etc.).

Each restart is reported to the :mod:`arbitrage.signals` bus as an
``InfoEvent(kind="watchdog")`` so the Telegram info-topic can
surface the problem.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Literal

from .signals import emit_info

logger = logging.getLogger("arbitrage.watchdog")

Factory = Callable[[], Awaitable[None]]


async def supervise(
    name: str,
    factory: Factory,
    *,
    market: Literal["spot", "perp"] | None = None,
    max_restarts: int = 20,
    restart_window_s: float = 300.0,
    base_backoff_s: float = 1.0,
    max_backoff_s: float = 30.0,
) -> None:
    """Run ``factory()`` in a loop, restarting on unhandled exceptions.

    Parameters
    ----------
    name
        Human-readable label used in logs and info events
        (``"binance-spot"``, ``"bybit-perp"``, ...).
    factory
        Zero-arg callable that returns the coroutine to run. Called
        *fresh* on every restart so per-run state (``WSListener``,
        tasks, etc.) never leaks across attempts.
    market
        Used to tag the emitted info events so the notifier can
        route them to the right Telegram group. ``None`` = fan out
        to every group.
    max_restarts / restart_window_s
        If the coroutine fails more than ``max_restarts`` times
        within the last ``restart_window_s`` seconds we give up
        and re-raise.  Defaults are tuned for the listeners' own
        hourly reconnect rate.
    base_backoff_s / max_backoff_s
        Exponential backoff with a cap applied between restarts.
    """
    restarts: deque[float] = deque()

    while True:
        try:
            await factory()
        except asyncio.CancelledError:
            # Propagate cancellation — this is the *only* clean
            # shutdown path.
            raise
        except Exception as exc:
            now = time.monotonic()
            restarts.append(now)
            while restarts and now - restarts[0] > restart_window_s:
                restarts.popleft()

            logger.exception("watchdog(%s): task crashed — restart #%d", name, len(restarts))
            emit_info(
                "watchdog",
                f"{name}: crashed ({type(exc).__name__}: {exc!s:.200}), "
                f"restart #{len(restarts)}",
                market=market,
                severity="error",
            )

            if len(restarts) > max_restarts:
                logger.error(
                    "watchdog(%s): %d restarts in %.0fs — giving up",
                    name,
                    len(restarts),
                    restart_window_s,
                )
                emit_info(
                    "watchdog",
                    f"{name}: {len(restarts)} restarts in "
                    f"{int(restart_window_s)}s — supervisor giving up",
                    market=market,
                    severity="error",
                )
                raise

            delay = min(base_backoff_s * (2 ** (len(restarts) - 1)), max_backoff_s)
            await asyncio.sleep(delay)
            continue

        # The coroutine returned without raising. Treat this as an
        # intentional stop — some run_*() helpers may return early
        # during tests. Log and exit.
        logger.info("watchdog(%s): coroutine returned cleanly — stopping supervisor", name)
        return


__all__ = ["supervise"]
