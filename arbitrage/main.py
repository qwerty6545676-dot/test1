"""Entry point: install uvloop, spin up per-exchange tasks, handle SIGINT.

Run: ``python -m arbitrage.main``.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import uvloop

from .comparator import PricesBook
from .config import SYMBOLS
from .exchanges.binance import run_binance
from .exchanges.bybit import run_bybit
from .heartbeat import heartbeat_monitor


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


async def _run() -> None:
    prices: PricesBook = {}

    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(run_binance(prices, SYMBOLS), name="binance"),
        asyncio.create_task(run_bybit(prices, SYMBOLS), name="bybit"),
        asyncio.create_task(heartbeat_monitor(prices), name="heartbeat"),
    ]

    stop_event = asyncio.Event()

    def _stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows / restricted environments — fall back to default handling.
            pass

    try:
        await stop_event.wait()
    finally:
        logging.getLogger("arbitrage.main").info("shutting down")
        for t in tasks:
            t.cancel()
        # Swallow CancelledError from the task group.
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    _setup_logging()
    uvloop.install()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
