"""Normalized tick representation shared by every exchange handler.

`Tick` is a `msgspec.Struct(gc=False)` to avoid GC tracking of short
lived objects on the hot path. Every exchange handler produces Ticks
with the same shape so the comparator does not have to branch on the
source.
"""

from __future__ import annotations

import msgspec


class Tick(msgspec.Struct, gc=False):
    """Top-of-book snapshot from a single exchange for a single symbol."""

    exchange: str
    symbol: str
    bid: float
    ask: float
    ts_exchange: int  # milliseconds, as reported by the exchange
    ts_local: int     # milliseconds, monotonic-ish local receive time


def validate_tick(tick: Tick) -> bool:
    """Cheap sanity checks — drop anything clearly bogus.

    Runs on the hot path; keep it short and branchless-ish.
    """
    bid = tick.bid
    ask = tick.ask
    if bid <= 0.0 or ask <= 0.0:
        return False
    if ask < bid:
        return False
    # Spread greater than 5% is almost certainly a bad feed / thin book.
    if ask / bid > 1.05:
        return False
    return True
