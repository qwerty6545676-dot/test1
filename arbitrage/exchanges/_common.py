"""Small helpers shared by all exchange handlers."""

from __future__ import annotations

import asyncio
import random
from typing import Literal

from ..config import BACKOFF_JITTER_S, BACKOFF_MAX_S
from ..signals import emit_info


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter.

    attempt is 0-based. The +jitter spreads simultaneous reconnects so
    we don't hammer the exchange (and trigger an IP ban) if all venues
    go down at once.
    """
    base = min(2.0 ** attempt, BACKOFF_MAX_S)
    return base + random.uniform(0.0, BACKOFF_JITTER_S)


async def sleep_backoff(
    attempt: int,
    *,
    exchange: str | None = None,
    market: Literal["spot", "perp"] | None = None,
) -> None:
    """Sleep for a backoff-appropriate interval before the next connect.

    Called from every listener's reconnect loop after a failure or
    disconnect. When ``exchange`` + ``market`` are supplied, emit a
    ``reconnect`` info event so the Telegram notifier (and any other
    consumer) can see that this venue is flapping. ``attempt`` is
    0-based — an ``attempt=0`` call means "the *first* attempt just
    failed / disconnected", which is still worth reporting.
    """
    delay = backoff_delay(attempt)
    if exchange is not None and market is not None:
        emit_info(
            "reconnect",
            f"{exchange}: reconnecting in {delay:.1f}s (attempt {attempt + 1})",
            market=market,
        )
    await asyncio.sleep(delay)


# -------- Symbol normalization --------
#
# Canonical form in the rest of the codebase is BASE+QUOTE with no
# separator (e.g. "BTCUSDT"). Some exchanges use "BTC_USDT" (Gate.io)
# or "BTC-USDT" (KuCoin). The helpers below convert both directions.
# Ordered by "most specific first" so that e.g. "USDC" is matched
# before "USD" and "BTC" is not split off ETHBTC as the quote.

_KNOWN_QUOTES: tuple[str, ...] = (
    "USDT", "USDC", "BUSD", "FDUSD", "TUSD",
    "USD", "EUR", "TRY", "BRL", "JPY", "GBP",
    "BTC", "ETH", "BNB",
)


def split_canonical(symbol: str) -> tuple[str, str]:
    """Split "BTCUSDT" into ("BTC", "USDT")."""
    for q in _KNOWN_QUOTES:
        if symbol.endswith(q) and len(symbol) > len(q):
            return symbol[: -len(q)], q
    raise ValueError(f"unrecognized canonical symbol: {symbol!r}")


def to_native(symbol: str, separator: str) -> str:
    """Canonical -> native. "BTCUSDT", "_" -> "BTC_USDT"."""
    if not separator:
        return symbol
    base, quote = split_canonical(symbol)
    return f"{base}{separator}{quote}"


def from_native(native: str, separator: str) -> str:
    """Native -> canonical. "BTC-USDT", "-" -> "BTCUSDT"."""
    if not separator:
        return native
    return native.replace(separator, "")


# -------- Exchange-label helpers --------
#
# Perp listeners label themselves with a "-perp" suffix so that spot
# and perp ticks can share one `PricesBook` keyed by exchange without
# colliding. The user-facing `settings.yaml` still uses bare names
# ("binance") under both `fees.spot` and `fees.perp` because that's
# how humans think about them. This helper bridges the two.


def base_exchange(ex: str) -> str:
    """Strip market suffix from an exchange label.

    >>> base_exchange("binance-perp")
    'binance'
    >>> base_exchange("binance")
    'binance'

    Used at every fee-table lookup: callers pass raw `Tick.exchange` /
    `ArbSignal.buy_ex` values, and the helper returns the bare name
    that matches `settings.fees.{spot,perp}` keys.
    """
    if ex.endswith("-perp"):
        return ex[:-5]
    return ex
