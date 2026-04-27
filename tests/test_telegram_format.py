"""Message formatter for Telegram."""

from __future__ import annotations

from arbitrage.signals import ArbSignal, InfoEvent
from arbitrage.telegram_notify.format import format_arb, format_info


def test_format_arb_contains_all_fields():
    sig = ArbSignal(
        ts_ms=0,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="binance",
        sell_ex="bybit",
        buy_ask=12345.6789,
        sell_bid=12400.0,
        net_pct=0.876,
    )
    text = format_arb(sig)
    assert "ARB SPOT" in text
    assert "BTCUSDT" in text
    assert "binance" in text
    assert "bybit" in text
    assert "12345.67890000" in text
    assert "12400.00000000" in text
    assert "0.876%" in text


def test_format_arb_perp_label():
    sig = ArbSignal(
        ts_ms=0,
        market="perp",
        symbol="ETHUSDT",
        buy_ex="a",
        sell_ex="b",
        buy_ask=1.0,
        sell_bid=1.1,
        net_pct=3.5,
    )
    assert "ARB PERP" in format_arb(sig)


def test_format_arb_html_escapes_symbol():
    sig = ArbSignal(
        ts_ms=0,
        market="spot",
        symbol="<script>",
        buy_ex="a",
        sell_ex="b",
        buy_ask=1.0,
        sell_bid=1.0,
        net_pct=1.0,
    )
    text = format_arb(sig)
    assert "<script>" not in text
    assert "&lt;script&gt;" in text


def test_format_info_has_emoji_and_kind():
    ev = InfoEvent(ts_ms=0, kind="startup", message="hi")
    text = format_info(ev)
    assert "🚀" in text
    assert "STARTUP" in text
    assert "hi" in text


def test_format_info_with_market_tag():
    ev = InfoEvent(ts_ms=0, kind="silence", message="bybit: quiet", market="spot")
    text = format_info(ev)
    assert "[spot]" in text
    assert "SILENCE" in text


def test_format_info_without_market_tag():
    ev = InfoEvent(ts_ms=0, kind="startup", message="hello", market=None)
    text = format_info(ev)
    assert "[spot]" not in text
    assert "[perp]" not in text


def test_format_info_escapes_message():
    ev = InfoEvent(ts_ms=0, kind="error", message="<b>boom</b>")
    text = format_info(ev)
    assert "&lt;b&gt;boom&lt;/b&gt;" in text


def test_format_info_watchdog_kind_renders_with_dedicated_emoji():
    """Regression: watchdog crash/restart events are operationally
    important and must not fall back to the generic bullet."""
    ev = InfoEvent(
        ts_ms=0,
        kind="watchdog",
        message="binance-spot: crashed (RuntimeError: boom), restart #1",
        market="spot",
        severity="error",
    )
    text = format_info(ev)
    assert "•" not in text  # no fallback bullet
    assert "🐕" in text
    assert "WATCHDOG" in text
    assert "[spot]" in text
