"""Small helpers shared by all exchange handlers."""

from __future__ import annotations

import asyncio
import random

from ..config import BACKOFF_JITTER_S, BACKOFF_MAX_S


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter.

    attempt is 0-based. The +jitter spreads simultaneous reconnects so
    we don't hammer the exchange (and trigger an IP ban) if all venues
    go down at once.
    """
    base = min(2.0 ** attempt, BACKOFF_MAX_S)
    return base + random.uniform(0.0, BACKOFF_JITTER_S)


async def sleep_backoff(attempt: int) -> None:
    await asyncio.sleep(backoff_delay(attempt))


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
