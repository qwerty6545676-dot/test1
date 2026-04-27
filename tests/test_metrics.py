"""Tests for the Prometheus metrics surface.

Goals:
* Every public ``record_*`` / ``set_*`` hook actually mutates the
  registry (no silent no-ops).
* The kill-switch env var ``ARB_METRICS_DISABLED=1`` disables every
  hook.
* The HTTP server is idempotent (start_metrics_server can be called
  twice without raising).
* The full SignalBus → metrics integration path increments the
  arb counter.
"""

from __future__ import annotations

import os

import pytest

from arbitrage import metrics
from arbitrage.signals import ArbSignal, SignalBus


@pytest.fixture(autouse=True)
def _reset_registry():
    """Each test starts from a clean slate."""
    metrics.reset_registry()
    os.environ.pop("ARB_METRICS_DISABLED", None)
    yield
    metrics.reset_registry()
    os.environ.pop("ARB_METRICS_DISABLED", None)


def _value_for(metric, **labels) -> float:
    """Read the current sample for a metric by exact label match.

    Returns 0.0 if the series is missing — the metric *exists* (we
    declared it at module load), it just hasn't been observed yet.
    """
    for sample in metric.collect()[0].samples:
        if sample.labels == labels:
            return sample.value
    return 0.0


def test_record_arb_signal_increments_counter_and_observes_histogram():
    metrics.record_arb_signal("spot", "BTCUSDT", "binance", "bybit", 4.2)

    counter_val = _value_for(
        metrics.arb_signals_total,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="binance",
        sell_ex="bybit",
    )
    assert counter_val == 1.0

    # Histogram exposes a ``_count`` sample; checking it's >= 1 keeps
    # the test independent of bucket boundaries.
    hist_count = sum(
        s.value
        for s in metrics.arb_signal_net_pct.collect()[0].samples
        if s.name.endswith("_count") and s.labels.get("market") == "spot"
    )
    assert hist_count == 1.0


def test_record_arb_signal_histogram_buckets_categorize_correctly():
    """A 4.2% signal lands in the (..3.0, 5.0] bucket. The cumulative
    count for ``le=5.0`` should be 1, but for ``le=3.0`` should be 0.
    """
    metrics.record_arb_signal("spot", "BTCUSDT", "binance", "bybit", 4.2)

    samples_by_le = {
        s.labels["le"]: s.value
        for s in metrics.arb_signal_net_pct.collect()[0].samples
        if s.name.endswith("_bucket") and s.labels.get("market") == "spot"
    }
    assert samples_by_le.get("3.0", 0.0) == 0.0
    assert samples_by_le.get("5.0", 0.0) == 1.0


def test_record_arb_routed_and_suppressed():
    metrics.record_arb_routed("perp", "low")
    metrics.record_arb_routed("perp", "low")
    metrics.record_arb_suppressed("perp")

    assert _value_for(
        metrics.arb_signals_routed_total, market="perp", tier="low"
    ) == 2.0
    assert _value_for(metrics.arb_signals_suppressed_total, market="perp") == 1.0


def test_listener_age_and_silence_gauges_track_state():
    metrics.set_listener_age("spot", "binance", 0.5)
    metrics.set_listener_silence("spot", "binance", True)

    assert (
        _value_for(metrics.listener_last_tick_age_seconds, market="spot", exchange="binance")
        == 0.5
    )
    assert (
        _value_for(metrics.listener_silence_active, market="spot", exchange="binance")
        == 1.0
    )

    metrics.set_listener_silence("spot", "binance", False)
    assert (
        _value_for(metrics.listener_silence_active, market="spot", exchange="binance")
        == 0.0
    )


def test_listener_reconnect_and_watchdog_counters():
    metrics.record_listener_reconnect("spot", "kucoin", "server_close")
    metrics.record_watchdog_restart("perp", "bingx-perp")

    assert _value_for(
        metrics.listener_reconnects_total,
        market="spot",
        exchange="kucoin",
        reason="server_close",
    ) == 1.0
    assert _value_for(
        metrics.watchdog_restarts_total, market="perp", exchange="bingx-perp"
    ) == 1.0


def test_telegram_metrics_track_queue_retries_and_drops():
    metrics.set_telegram_queue_size(7)
    metrics.record_telegram_retry("429")
    metrics.record_telegram_retry("5xx")
    metrics.record_telegram_dropped()

    assert metrics.telegram_queue_size._value.get() == 7.0
    assert _value_for(metrics.telegram_retries_total, reason="429") == 1.0
    assert _value_for(metrics.telegram_retries_total, reason="5xx") == 1.0
    assert metrics.telegram_dropped_total._value.get() == 1.0


def test_paper_metrics():
    metrics.set_paper_open_positions("perp", 3)
    metrics.record_paper_closed("perp", "converged")
    metrics.add_paper_pnl("perp", 12.34)
    metrics.add_paper_pnl("perp", -2.0)

    assert _value_for(metrics.paper_open_positions, market="perp") == 3.0
    assert _value_for(
        metrics.paper_closed_total, market="perp", outcome="converged"
    ) == 1.0
    assert _value_for(metrics.paper_realized_pnl_usd, market="perp") == pytest.approx(10.34)


def test_kill_switch_disables_every_hook():
    """Setting ARB_METRICS_DISABLED=1 makes every hook a no-op without
    raising — useful for emergency shutoff via env var.
    """
    os.environ["ARB_METRICS_DISABLED"] = "1"

    metrics.record_arb_signal("spot", "BTCUSDT", "binance", "bybit", 4.2)
    metrics.record_arb_routed("perp", "low")
    metrics.record_arb_suppressed("perp")
    metrics.set_listener_age("spot", "binance", 1.0)
    metrics.set_listener_silence("spot", "binance", True)
    metrics.record_listener_reconnect("spot", "kucoin")
    metrics.record_watchdog_restart("perp", "bingx-perp")
    metrics.set_telegram_queue_size(7)
    metrics.record_telegram_retry("429")
    metrics.record_telegram_dropped()
    metrics.set_paper_open_positions("perp", 5)
    metrics.record_paper_closed("perp", "manual")
    metrics.add_paper_pnl("perp", 1.0)
    metrics.record_tick_persisted("spot")

    assert _value_for(
        metrics.arb_signals_total,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="binance",
        sell_ex="bybit",
    ) == 0.0
    assert _value_for(metrics.paper_open_positions, market="perp") == 0.0
    assert metrics.telegram_dropped_total._value.get() == 0.0


def test_signal_bus_emit_arb_increments_metric():
    """End-to-end: emitting through the bus must update the counter
    even if no handlers are registered.
    """
    bus = SignalBus()
    sig = ArbSignal(
        ts_ms=1700000000000,
        market="perp",
        symbol="ETHUSDT",
        buy_ex="binance-perp",
        sell_ex="kucoin-perp",
        buy_ask=2000.0,
        sell_bid=2050.0,
        net_pct=2.5,
    )

    bus.emit_arb(sig)

    assert _value_for(
        metrics.arb_signals_total,
        market="perp",
        symbol="ETHUSDT",
        buy_ex="binance-perp",
        sell_ex="kucoin-perp",
    ) == 1.0


def test_start_metrics_server_is_idempotent():
    """Calling twice with the same port must not raise (the second
    call is a documented no-op).
    """
    # Use a high port unlikely to collide with anything in CI.
    metrics.start_metrics_server(port=39091, addr="127.0.0.1")
    try:
        # Second call is a no-op.
        metrics.start_metrics_server(port=39091, addr="127.0.0.1")
    finally:
        # The HTTP server is a daemon thread; we leave it running.
        # Tests that come after will reuse it via the idempotency.
        pass
