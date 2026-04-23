"""Entry point: install uvloop, spin up per-exchange tasks, handle SIGINT.

Run: ``python -m arbitrage.main``.

With spot+perp enabled there are 14 listener tasks (7 spot + 7 perp),
each writing into its own prices book. The heartbeat monitor runs
once for each book so stale entries in either market get evicted on
the same schedule.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import uvloop

from .comparator import PricesBook
from .heartbeat import heartbeat_monitor
from .settings import get_settings


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


async def _run() -> None:
    settings = get_settings()
    spot_symbols = tuple(settings.symbols.spot)
    perp_symbols = tuple(settings.symbols.perp)

    prices_spot: PricesBook = {}
    prices_perp: PricesBook = {}

    tasks: list[asyncio.Task[None]] = []

    # ---- Spot listeners (7) --------------------------------------
    from .exchanges.spot import (
        run_binance as run_binance_spot,
        run_bingx as run_bingx_spot,
        run_bitget as run_bitget_spot,
        run_bybit as run_bybit_spot,
        run_gateio as run_gateio_spot,
        run_kucoin as run_kucoin_spot,
        run_mexc as run_mexc_spot,
    )

    tasks += [
        asyncio.create_task(run_binance_spot(prices_spot, spot_symbols), name="binance-spot"),
        asyncio.create_task(run_bybit_spot(prices_spot, spot_symbols), name="bybit-spot"),
        asyncio.create_task(run_gateio_spot(prices_spot, spot_symbols), name="gateio-spot"),
        asyncio.create_task(run_bitget_spot(prices_spot, spot_symbols), name="bitget-spot"),
        asyncio.create_task(run_kucoin_spot(prices_spot, spot_symbols), name="kucoin-spot"),
        asyncio.create_task(run_bingx_spot(prices_spot, spot_symbols), name="bingx-spot"),
        asyncio.create_task(run_mexc_spot(prices_spot, spot_symbols), name="mexc-spot"),
    ]

    # ---- Perp listeners (7) --------------------------------------
    from .exchanges.perp import (
        run_binance as run_binance_perp,
        run_bingx as run_bingx_perp,
        run_bitget as run_bitget_perp,
        run_bybit as run_bybit_perp,
        run_gateio as run_gateio_perp,
        run_kucoin as run_kucoin_perp,
        run_mexc as run_mexc_perp,
    )

    tasks += [
        asyncio.create_task(run_binance_perp(prices_perp, perp_symbols), name="binance-perp"),
        asyncio.create_task(run_bybit_perp(prices_perp, perp_symbols), name="bybit-perp"),
        asyncio.create_task(run_gateio_perp(prices_perp, perp_symbols), name="gateio-perp"),
        asyncio.create_task(run_bitget_perp(prices_perp, perp_symbols), name="bitget-perp"),
        asyncio.create_task(run_kucoin_perp(prices_perp, perp_symbols), name="kucoin-perp"),
        asyncio.create_task(run_bingx_perp(prices_perp, perp_symbols), name="bingx-perp"),
        asyncio.create_task(run_mexc_perp(prices_perp, perp_symbols), name="mexc-perp"),
    ]

    # ---- Heartbeat monitors (one per book) -----------------------
    tasks += [
        asyncio.create_task(heartbeat_monitor(prices_spot, label="spot"), name="heartbeat-spot"),
        asyncio.create_task(heartbeat_monitor(prices_perp, label="perp"), name="heartbeat-perp"),
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
