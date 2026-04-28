"""Unit tests for the per-venue coverage filter in ``arbitrage.main``.

Covers ``_venue_symbols``, the helper that decides which symbols a
listener actually subscribes to. The full ``_run`` coroutine wires up
listeners + Telegram + paper trading + heartbeat, so we don't drive
it end-to-end here; we just pin the filter behaviour that the rest
of the pipeline depends on.
"""

from __future__ import annotations

from arbitrage.main import _venue_symbols


def test_empty_coverage_returns_full_fallback_list():
    fallback = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    # Empty coverage -> legacy behaviour, every venue gets everything.
    assert _venue_symbols(fallback, {}, "binance") == fallback
    assert _venue_symbols(fallback, {}, "mexc") == fallback


def test_coverage_filters_symbols_per_venue():
    coverage = {
        "BTCUSDT": ["binance", "bybit", "mexc"],
        "ETHUSDT": ["binance", "bybit"],
        "SOLUSDT": ["bybit", "mexc"],
    }
    fallback = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    # Binance lists BTC and ETH but not SOL in this map.
    assert _venue_symbols(fallback, coverage, "binance") == ("BTCUSDT", "ETHUSDT")
    # Bybit lists all three.
    assert _venue_symbols(fallback, coverage, "bybit") == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    # MEXC lists only the two it actually streams.
    assert _venue_symbols(fallback, coverage, "mexc") == ("BTCUSDT", "SOLUSDT")


def test_venue_not_in_any_entry_returns_empty():
    """A venue absent from the coverage map for every symbol gets an
    empty tuple; ``main._run`` then skips spawning its listener."""
    coverage = {
        "BTCUSDT": ["binance", "bybit"],
        "ETHUSDT": ["binance", "bybit"],
    }
    assert _venue_symbols(("BTCUSDT", "ETHUSDT"), coverage, "kucoin") == ()


def test_coverage_keyset_is_source_of_truth():
    """When coverage is provided, it — not ``fallback`` — drives the
    universe. Symbols in fallback that aren't in coverage are dropped
    even on venues listed in coverage."""
    coverage = {"BTCUSDT": ["binance", "bybit"]}
    # Fallback has ETH/SOL too, but coverage doesn't list them so
    # they're not subscribed anywhere.
    fallback = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert _venue_symbols(fallback, coverage, "binance") == ("BTCUSDT",)
    assert _venue_symbols(fallback, coverage, "bybit") == ("BTCUSDT",)


def test_perp_venue_label_uses_suffix():
    """Perp coverage entries are ``-perp``-suffixed venue labels and
    must be matched as such (the perp side of ``main._run`` looks up
    by ``"binance-perp"`` etc.)."""
    coverage = {
        "BTCUSDT": ["binance-perp", "bybit-perp"],
        "ETHUSDT": ["binance-perp"],
    }
    fallback = ("BTCUSDT", "ETHUSDT")
    assert _venue_symbols(fallback, coverage, "binance-perp") == ("BTCUSDT", "ETHUSDT")
    assert _venue_symbols(fallback, coverage, "bybit-perp") == ("BTCUSDT",)
    assert _venue_symbols(fallback, coverage, "mexc-perp") == ()
    # Bare venue name without the perp suffix should NOT match perp
    # coverage entries — defensive check against fan-out bugs.
    assert _venue_symbols(fallback, coverage, "binance") == ()
