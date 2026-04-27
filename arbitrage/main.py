"""Entry point: install uvloop, spin up per-exchange tasks, handle SIGINT.

Run: ``python -m arbitrage.main``.

With spot+perp enabled there are 14 listener tasks (7 spot + 7 perp),
each writing into its own prices book. The heartbeat monitor runs
once for each book so stale entries in either market get evicted on
the same schedule.

On top of that — when ``settings.yaml`` has ``telegram.enabled: true``
and ``TELEGRAM_BOT_TOKEN`` is set in the environment — a background
Telegram notifier is started. Arb signals and info events flow
through ``arbitrage.signals.SignalBus``; the notifier turns them into
messages addressed to the spot/perp groups at the configured topics.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import uvloop

from .comparator import PricesBook
from .heartbeat import heartbeat_monitor
from .paper import PaperTradesWriter, PerpPaperTrader, SpotPaperTrader
from .settings import Settings, get_settings, get_telegram_bot_token
from .signals import InfoEvent, get_bus
from .telegram_notify import TelegramClient, TelegramNotifier
from .telegram_notify.log_handler import BusErrorHandler

# Exchange labels the listeners use for ``Tick.exchange``. Passed into
# the heartbeat monitor so it knows which venues to watch for silence.
_SPOT_EXCHANGES: tuple[str, ...] = (
    "binance",
    "bybit",
    "gateio",
    "bitget",
    "kucoin",
    "bingx",
    "mexc",
)
_PERP_EXCHANGES: tuple[str, ...] = tuple(f"{ex}-perp" for ex in _SPOT_EXCHANGES)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


async def _setup_telegram(
    settings: Settings,
) -> tuple[TelegramClient, TelegramNotifier] | None:
    """Start the Telegram client + notifier if enabled and configured."""
    tg = settings.telegram
    if not tg.enabled:
        return None
    token = get_telegram_bot_token()
    if token is None:
        logging.getLogger("arbitrage.main").warning(
            "telegram.enabled=true but TELEGRAM_BOT_TOKEN is unset — skipping"
        )
        return None

    client = TelegramClient(token)
    await client.start()

    bus = get_bus()
    notifier = TelegramNotifier(settings, client, bus)
    notifier.attach()

    # Forward ERROR log records from *anywhere* in the app into the bus
    # so they're sent to the info-topic(s).
    bus_handler = BusErrorHandler(bus)
    logging.getLogger().addHandler(bus_handler)

    return client, notifier


async def _run() -> None:
    settings = get_settings()
    spot_symbols = tuple(settings.symbols.spot)
    perp_symbols = tuple(settings.symbols.perp)

    prices_spot: PricesBook = {}
    prices_perp: PricesBook = {}

    tg = await _setup_telegram(settings)
    bus = get_bus()

    paper_open_writer: PaperTradesWriter | None = None
    paper_closed_writer: PaperTradesWriter | None = None
    paper_perp_trader: PerpPaperTrader | None = None
    if settings.paper_trading.enabled:
        paper_open_writer = PaperTradesWriter(settings.paper_trading.open_path)
        paper_closed_writer = PaperTradesWriter(settings.paper_trading.closed_path)
        paper_open_writer.open()
        paper_closed_writer.open()

        SpotPaperTrader(
            bus, settings.fees,
            closed_writer=paper_closed_writer,
            notional_per_leg_usd=settings.paper_trading.spot.notional_per_leg_usd,
            slippage_pct=settings.paper_trading.spot.slippage_pct,
        ).attach()

        paper_perp_trader = PerpPaperTrader(
            bus, settings.fees, prices_perp,
            open_writer=paper_open_writer,
            closed_writer=paper_closed_writer,
            notional_per_leg_usd=settings.paper_trading.perp.notional_per_leg_usd,
            close_threshold_pct=settings.paper_trading.perp.close_threshold_pct,
            max_hold_seconds=settings.paper_trading.perp.max_hold_seconds,
            slippage_pct=settings.paper_trading.perp.slippage_pct,
            poll_interval_s=settings.paper_trading.perp.poll_interval_s,
        )
        paper_perp_trader.attach()

    bus.emit_info(
        InfoEvent(
            ts_ms=_now_ms(),
            kind="startup",
            message=(
                f"scanner starting: "
                f"{len(spot_symbols)} spot symbols × {len(_SPOT_EXCHANGES)} venues, "
                f"{len(perp_symbols)} perp symbols × {len(_PERP_EXCHANGES)} venues"
            ),
            market=None,
            severity="info",
        )
    )

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
        asyncio.create_task(
            heartbeat_monitor(
                prices_spot,
                label="spot",
                expected_exchanges=_SPOT_EXCHANGES,
            ),
            name="heartbeat-spot",
        ),
        asyncio.create_task(
            heartbeat_monitor(
                prices_perp,
                label="perp",
                expected_exchanges=_PERP_EXCHANGES,
            ),
            name="heartbeat-perp",
        ),
    ]

    # ---- Paper-trading poll (perp convergence watcher) -----------
    if paper_perp_trader is not None:
        tasks.append(
            asyncio.create_task(paper_perp_trader.run(), name="paper-perp-poll")
        )

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
        bus.emit_info(
            InfoEvent(
                ts_ms=_now_ms(),
                kind="shutdown",
                message="scanner stopping",
                market=None,
                severity="info",
            )
        )
        for t in tasks:
            t.cancel()
        # Swallow CancelledError from the task group.
        await asyncio.gather(*tasks, return_exceptions=True)
        if tg is not None:
            # Give the worker a moment to flush the shutdown message,
            # then tear it down. 1s is plenty for a single HTTP POST.
            await asyncio.sleep(1.0)
            await tg[0].stop()
        if paper_open_writer is not None:
            paper_open_writer.close()
        if paper_closed_writer is not None:
            paper_closed_writer.close()


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def main() -> None:
    _setup_logging()
    uvloop.install()
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
