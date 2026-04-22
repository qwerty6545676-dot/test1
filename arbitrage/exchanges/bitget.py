"""Bitget spot BBO listener.

TODO: implement. Endpoint wss://ws.bitget.com/v2/ws/public,
subscribe to ``{"instType":"SPOT","channel":"books1","instId":"BTCUSDT"}``.
Requires raw ``ping`` text frame every ~25s.
"""

from __future__ import annotations

from ..comparator import PricesBook


async def run_bitget(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    raise NotImplementedError("bitget exchange handler not implemented yet")
