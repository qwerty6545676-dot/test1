"""Prometheus metrics — one process-wide registry, off by default.

Why
---
We already log every interesting event to stdout and ship the
critical ones to Telegram, but neither view answers questions like:

* "Was the surge of signals at 14:30 organic, or did three venues
  just reconnect simultaneously?" (correlate ``arb_signals_total``
  with ``listener_reconnects_total``)
* "How does our spread distribution look this week — has the median
  net pct dropped?" (histogram quantiles on ``arb_signal_net_pct``)
* "Is Telegram backpressure becoming an issue?" (gauges on
  ``telegram_queue_size`` and ``telegram_retries_total``)
* "When did KuCoin go silent yesterday?" (heatmap on
  ``listener_last_tick_age_seconds``)

A scrape endpoint solves all of these without touching the hot path
beyond a constant-time counter increment.

Architecture
------------
Single module-level :class:`prometheus_client.CollectorRegistry`
holding all metrics. ``record_*`` functions are the only public
surface — never expose the metric objects directly so we can swap
the backend later without touching listeners.

When ``settings.metrics.enabled = False`` (the default) the
``record_*`` calls are still executed but write to a no-op
collector — listeners don't need to feature-check anything. Setting
the env var ``ARB_METRICS_DISABLED=1`` has the same effect for
emergency shutoff without re-deployment.

The HTTP server is a stock :func:`prometheus_client.start_http_server`
on a configurable port (default 9090). Bind on ``0.0.0.0`` only on
trusted networks — anyone reaching ``/metrics`` learns your symbol
universe and venue line-up.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = logging.getLogger("arbitrage.metrics")


# ---------------------------------------------------------------------------
# Registry / metric definitions
# ---------------------------------------------------------------------------

#: Process-wide registry. Tests can swap this via :func:`reset_registry`.
REGISTRY = CollectorRegistry(auto_describe=True)


# Per-signal counters and the spread histogram. Labels intentionally
# kept low-cardinality: market × symbol × buy_ex × sell_ex is bounded
# by the configured universe (3 symbols × 7 venues × 7 venues × 2
# markets ≈ 294 series, well under Prom's 1-million-series guideline).
arb_signals_total = Counter(
    "arb_signals_total",
    "Arbitrage opportunities emitted by the comparator (pre-cooldown).",
    ("market", "symbol", "buy_ex", "sell_ex"),
    registry=REGISTRY,
)

# Histogram buckets follow a coarse log-spaced scale so a single panel
# can show both the mass of "barely-profitable" arbs and the rare
# 10 %+ outliers. Buckets are in *percent* (not fraction).
arb_signal_net_pct = Histogram(
    "arb_signal_net_pct",
    "Distribution of arbitrage opportunities by net profit percentage.",
    ("market",),
    buckets=(0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0, 15.0, 25.0),
    registry=REGISTRY,
)

# Telegram-bound notifications. Tier comes from the routing layer
# (``low``/``mid``/``high``) — useful to see which topics fire most.
arb_signals_routed_total = Counter(
    "arb_signals_routed_total",
    "Signals after cooldown / tier routing — i.e. actually sent to Telegram.",
    ("market", "tier"),
    registry=REGISTRY,
)

arb_signals_suppressed_total = Counter(
    "arb_signals_suppressed_total",
    "Signals suppressed by the cooldown filter.",
    ("market",),
    registry=REGISTRY,
)


# Heartbeat / silence — high-cardinality enough that we use a single
# Gauge with (market, exchange) labels instead of one per venue.
listener_last_tick_age_seconds = Gauge(
    "listener_last_tick_age_seconds",
    "Seconds since the most recent tick from this venue/symbol pair.",
    ("market", "exchange"),
    registry=REGISTRY,
)

listener_silence_active = Gauge(
    "listener_silence_active",
    "1 if the silence-monitor has flagged this venue, 0 otherwise.",
    ("market", "exchange"),
    registry=REGISTRY,
)


# Watchdog / reconnect — ``reason`` separates clean retries
# (``server_close``) from real bugs (``crash``).
listener_reconnects_total = Counter(
    "listener_reconnects_total",
    "Listener WebSocket reconnections.",
    ("market", "exchange", "reason"),
    registry=REGISTRY,
)

watchdog_restarts_total = Counter(
    "watchdog_restarts_total",
    "Listener task restarts performed by the supervisor.",
    ("market", "exchange"),
    registry=REGISTRY,
)


# Telegram backpressure. Queue size is a Gauge because it goes both
# up and down; everything else is monotonic.
telegram_queue_size = Gauge(
    "telegram_queue_size",
    "Outbound notifications waiting to be sent to Telegram.",
    registry=REGISTRY,
)

telegram_retries_total = Counter(
    "telegram_retries_total",
    "Telegram sendMessage retries (typically due to 429 rate-limit).",
    ("reason",),  # 429, 5xx, network, ...
    registry=REGISTRY,
)

telegram_dropped_total = Counter(
    "telegram_dropped_total",
    "Telegram messages permanently dropped after the retry budget.",
    registry=REGISTRY,
)


# Paper-trading PnL panels. Cumulative-PnL is a Gauge so a Grafana
# rate() / delta() over a window gives realized return per period.
paper_open_positions = Gauge(
    "paper_open_positions",
    "Open paper-trading positions.",
    ("market",),
    registry=REGISTRY,
)

paper_closed_total = Counter(
    "paper_closed_total",
    "Paper-trading positions closed.",
    ("market", "outcome"),  # converged | timeout | manual
    registry=REGISTRY,
)

paper_realized_pnl_usd = Gauge(
    "paper_realized_pnl_usd",
    "Cumulative paper-trading PnL in USD since process start.",
    ("market",),
    registry=REGISTRY,
)


# Persistence / on-disk health. The ticks-writer one matters because
# a stuck compression would silently bloat the disk.
ticks_records_total = Counter(
    "ticks_records_total",
    "Ticks appended to the binary log.",
    ("market",),
    registry=REGISTRY,
)


# ---------------------------------------------------------------------------
# Public hooks. Callers never touch the metrics objects directly.
# ---------------------------------------------------------------------------


def _disabled() -> bool:
    """Check the emergency shutoff env var.

    The ``settings.metrics.enabled`` flag controls whether the HTTP
    server starts; this env var lets an operator disable *the whole
    instrumentation surface* without redeploying — useful when a
    cardinality bug is causing trouble.
    """
    return os.getenv("ARB_METRICS_DISABLED") == "1"


def record_arb_signal(
    market: str,
    symbol: str,
    buy_ex: str,
    sell_ex: str,
    net_pct: float,
) -> None:
    """Hot path. Called from :class:`SignalBus`."""
    if _disabled():
        return
    arb_signals_total.labels(market, symbol, buy_ex, sell_ex).inc()
    arb_signal_net_pct.labels(market).observe(net_pct)


def record_arb_routed(market: str, tier: str) -> None:
    if _disabled():
        return
    arb_signals_routed_total.labels(market, tier).inc()


def record_arb_suppressed(market: str) -> None:
    if _disabled():
        return
    arb_signals_suppressed_total.labels(market).inc()


def set_listener_age(market: str, exchange: str, age_seconds: float) -> None:
    if _disabled():
        return
    listener_last_tick_age_seconds.labels(market, exchange).set(age_seconds)


def set_listener_silence(market: str, exchange: str, silent: bool) -> None:
    if _disabled():
        return
    listener_silence_active.labels(market, exchange).set(1 if silent else 0)


def record_listener_reconnect(market: str, exchange: str, reason: str = "unknown") -> None:
    if _disabled():
        return
    listener_reconnects_total.labels(market, exchange, reason).inc()


def record_watchdog_restart(market: str, exchange: str) -> None:
    if _disabled():
        return
    watchdog_restarts_total.labels(market, exchange).inc()


def set_telegram_queue_size(size: int) -> None:
    if _disabled():
        return
    telegram_queue_size.set(size)


def record_telegram_retry(reason: str) -> None:
    if _disabled():
        return
    telegram_retries_total.labels(reason).inc()


def record_telegram_dropped() -> None:
    if _disabled():
        return
    telegram_dropped_total.inc()


def set_paper_open_positions(market: str, n: int) -> None:
    if _disabled():
        return
    paper_open_positions.labels(market).set(n)


def record_paper_closed(market: str, outcome: str) -> None:
    if _disabled():
        return
    paper_closed_total.labels(market, outcome).inc()


def add_paper_pnl(market: str, pnl_usd: float) -> None:
    """Cumulative — call with a *delta*, the gauge is incremented."""
    if _disabled():
        return
    paper_realized_pnl_usd.labels(market).inc(pnl_usd)


def record_tick_persisted(market: str) -> None:
    if _disabled():
        return
    ticks_records_total.labels(market).inc()


# ---------------------------------------------------------------------------
# HTTP server lifecycle
# ---------------------------------------------------------------------------


_server_started = False


def start_metrics_server(port: int = 9090, addr: str = "0.0.0.0") -> None:
    """Start the Prometheus scrape endpoint.

    Idempotent — calling twice with the same port is a no-op. The
    underlying ``start_http_server`` spawns a daemon thread, so this
    does not block the asyncio loop.

    Bind on ``0.0.0.0`` is the default because Prometheus typically
    runs on a separate host. On a single-VPS deployment, prefer
    ``addr="127.0.0.1"`` and put Prometheus on the same machine —
    anyone reaching ``/metrics`` learns the symbol universe and
    venue line-up.
    """
    global _server_started
    if _server_started:
        logger.info("metrics server already started, skipping")
        return
    start_http_server(port, addr=addr, registry=REGISTRY)
    _server_started = True
    logger.info("metrics: serving /metrics on http://%s:%d", addr, port)


def reset_registry() -> None:
    """Drop all collected samples. Tests use this between cases.

    The metric *objects* remain (they're module globals) but their
    series are cleared.
    """
    for collector in list(REGISTRY._names_to_collectors.values()):
        if hasattr(collector, "_metrics"):
            collector._metrics.clear()
        if hasattr(collector, "_value"):
            try:
                collector._value.set(0)
            except AttributeError:
                pass
