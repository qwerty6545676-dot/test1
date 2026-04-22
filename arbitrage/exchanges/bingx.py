"""BingX spot BBO listener.

TODO: implement. Endpoint wss://open-api-ws.bingx.com/market,
channel ``<SYMBOL>@bookTicker``. Frames are gzip-compressed binary.
"""

from __future__ import annotations

from ..comparator import PricesBook


async def run_bingx(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    raise NotImplementedError("bingx exchange handler not implemented yet")
