"""Perp-specific symbol helpers.

Most exchanges expose perp symbols in the same shape as spot (or with
a straightforward separator change), so the generic
:func:`arbitrage.exchanges._common.to_native` helper already does the
job. KuCoin futures are the one exception — they use legacy
Kraken-style naming (``XBT`` instead of ``BTC``) and always append a
trailing ``M`` to USDT-margined contracts:

    BTCUSDT -> XBTUSDTM
    ETHUSDT -> ETHUSDTM
    SOLUSDT -> SOLUSDTM

All conversion knowledge lives in this one module so the perp
listener itself only needs ``to_kucoin_perp(sym)``.
"""

from __future__ import annotations

from .._common import split_canonical

# Bases that KuCoin futures rewrites. If they ever add more legacy
# aliases this is the only place to update.
_KUCOIN_PERP_BASE_ALIASES: dict[str, str] = {
    "BTC": "XBT",
}
_KUCOIN_PERP_REVERSE_ALIASES: dict[str, str] = {
    v: k for k, v in _KUCOIN_PERP_BASE_ALIASES.items()
}


def to_kucoin_perp(symbol: str) -> str:
    """Canonical ``BTCUSDT`` -> KuCoin-futures ``XBTUSDTM`` (etc)."""
    base, quote = split_canonical(symbol)
    base = _KUCOIN_PERP_BASE_ALIASES.get(base, base)
    return f"{base}{quote}M"


def from_kucoin_perp(native: str) -> str:
    """KuCoin-futures ``XBTUSDTM`` -> canonical ``BTCUSDT`` (etc).

    Accepts anything ending in ``M`` and strips it before reversing
    the BTC↔XBT alias. Unknown shapes pass through unchanged so the
    caller can still log them.
    """
    if not native.endswith("M"):
        return native
    head = native[:-1]
    # Split off a known quote the same way split_canonical does.
    try:
        base, quote = split_canonical(head)
    except ValueError:
        return native
    base = _KUCOIN_PERP_REVERSE_ALIASES.get(base, base)
    return f"{base}{quote}"
