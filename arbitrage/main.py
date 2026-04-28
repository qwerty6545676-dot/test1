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
from . import metrics
from .persistence import SignalsWriter, TickWriter, attach_writer as attach_tick_writer
from .settings import Settings, get_settings, get_telegram_bot_token
from .signals import InfoEvent, get_bus
from .telegram_notify import TelegramClient, TelegramNotifier
from .telegram_notify.log_handler import BusErrorHandler
from .watchdog import supervise

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


def _venue_symbols(
    fallback_symbols: tuple[str, ...],
    coverage_map: dict[str, list[str]],
    venue: str,
) -> tuple[str, ...]:
    """Return the symbols to subscribe on ``venue`` for one market.

    If ``coverage_map`` is non-empty, treat it as the source of truth:
    only the symbols that explicitly list ``venue`` are returned.
    This is what stops MEXC / KuCoin / BingX from subscribing to
    pairs they list in REST but never push via WS — the root cause of
    heartbeat-evict log spam at universe-100.

    If ``coverage_map`` is empty (legacy behaviour), every venue
    gets the full ``fallback_symbols`` list.
    """
    if not coverage_map:
        return fallback_symbols
    return tuple(s for s, venues in coverage_map.items() if venue in venues)


async def _run() -> None:
    settings = get_settings()
    spot_symbols = tuple(settings.symbols.spot)
    perp_symbols = tuple(settings.symbols.perp)
    spot_coverage = dict(settings.coverage.spot)
    perp_coverage = dict(settings.coverage.perp)

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

    signals_writer: SignalsWriter | None = None
    tick_writer: TickWriter | None = None
    if settings.persistence.enabled:
        signals_writer = SignalsWriter(settings.persistence.signals_path)
        signals_writer.open()
        signals_writer.attach(bus)
        ts = settings.persistence.tick_storage
        if ts.enabled:
            tick_writer = TickWriter(
                ts.root,
                compress=ts.compress,
                retention_days=ts.retention_days,
            )
            tick_writer.open()
            attach_tick_writer(tick_writer)

    if settings.metrics.enabled:
        metrics.start_metrics_server(
            port=settings.metrics.port,
            addr=settings.metrics.bind_addr,
        )


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

    def _spot_task(name: str, runner):  # type: ignore[no-untyped-def]
        # ``name`` is e.g. "binance-spot"; the metrics convention for
        # spot is the bare exchange ("binance") so the dashboard can
        # correlate watchdog_restarts_total with listener_last_tick_age_seconds.
        exchange = name.removesuffix("-spot")
        venue_syms = _venue_symbols(spot_symbols, spot_coverage, exchange)
        if not venue_syms:
            # Coverage map declared this venue but with no symbols, or
            # the operator is intentionally turning a venue off.
            # Don't open a WS connection that would just sit idle.
            logging.getLogger("arbitrage.main").info(
                "spot: %s has 0 symbols in coverage; skipping listener",
                name,
            )
            return None
        return asyncio.create_task(
            supervise(
                name,
                lambda: runner(prices_spot, venue_syms),
                market="spot",
                exchange=exchange,
            ),
            name=name,
        )

    tasks += [
        t for t in (
            _spot_task("binance-spot", run_binance_spot),
            _spot_task("bybit-spot", run_bybit_spot),
            _spot_task("gateio-spot", run_gateio_spot),
            _spot_task("bitget-spot", run_bitget_spot),
            _spot_task("kucoin-spot", run_kucoin_spot),
            _spot_task("bingx-spot", run_bingx_spot),
            _spot_task("mexc-spot", run_mexc_spot),
        ) if t is not None
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

    def _perp_task(name: str, runner):  # type: ignore[no-untyped-def]
        # ``name`` is already "binance-perp" — matches the perp
        # heartbeat exchange-label convention as-is.
        venue_syms = _venue_symbols(perp_symbols, perp_coverage, name)
        if not venue_syms:
            logging.getLogger("arbitrage.main").info(
                "perp: %s has 0 symbols in coverage; skipping listener",
                name,
            )
            return None
        return asyncio.create_task(
            supervise(
                name,
                lambda: runner(prices_perp, venue_syms),
                market="perp",
                exchange=name,
            ),
            name=name,
        )

    tasks += [
        t for t in (
            _perp_task("binance-perp", run_binance_perp),
            _perp_task("bybit-perp", run_bybit_perp),
            _perp_task("gateio-perp", run_gateio_perp),
            _perp_task("bitget-perp", run_bitget_perp),
            _perp_task("kucoin-perp", run_kucoin_perp),
            _perp_task("bingx-perp", run_bingx_perp),
            _perp_task("mexc-perp", run_mexc_perp),
        ) if t is not None
    ]

    # ---- Heartbeat monitors (one per book) -----------------------
    # Only track silence for venues we actually spawned a listener for.
    # Without this, a venue that has 0 symbols in coverage would be
    # flagged silent forever (we didn't connect, no ticks ever arrive).
    spot_active = tuple(
        v for v in _SPOT_EXCHANGES
        if _venue_symbols(spot_symbols, spot_coverage, v)
    )
    perp_active = tuple(
        v for v in _PERP_EXCHANGES
        if _venue_symbols(perp_symbols, perp_coverage, v)
    )
    tasks += [
        asyncio.create_task(
            heartbeat_monitor(
                prices_spot,
                label="spot",
                expected_exchanges=spot_active,
            ),
            name="heartbeat-spot",
        ),
        asyncio.create_task(
            heartbeat_monitor(
                prices_perp,
                label="perp",
                expected_exchanges=perp_active,
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
        if signals_writer is not None:
            signals_writer.close()
        if tick_writer is not None:
            attach_tick_writer(None)
            tick_writer.close()


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
