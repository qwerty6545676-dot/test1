"""Global configuration for the arbitrage scanner.

Edit the constants here to adjust the universe of symbols, freshness
thresholds and per-exchange taker fees.
"""

from __future__ import annotations

# Symbols tracked across every exchange. Use the canonical BASE+QUOTE
# (no separator) form; per-exchange modules translate it if needed.
SYMBOLS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
)

# Ticks older than this are considered stale and ignored by the
# comparator / wiped by the heartbeat monitor.
MAX_AGE_MS: int = 500

# Minimum net profit (in percent) to emit an arbitrage signal.
MIN_PROFIT_PCT: float = 0.15

# How long a per-exchange entry is allowed to go without an update
# before the heartbeat monitor evicts it.
HEARTBEAT_TIMEOUT_MS: int = 3000

# How often the heartbeat monitor scans the price book.
HEARTBEAT_INTERVAL_MS: int = 1000

# Taker fees as fractions (0.001 == 0.10%). Used round-trip in the
# comparator (buy leg + sell leg).
FEES: dict[str, float] = {
    "binance": 0.001,
    "bybit": 0.001,
    # Wired up later — left here so the comparator is ready:
    "gateio": 0.002,
    "bitget": 0.001,
    "kucoin": 0.001,
    "bingx": 0.002,
    "mexc": 0.0,
}

# Reconnect backoff. `wait = min(2 ** attempt, BACKOFF_MAX) + jitter`.
BACKOFF_MAX_S: float = 60.0
BACKOFF_JITTER_S: float = 1.0
