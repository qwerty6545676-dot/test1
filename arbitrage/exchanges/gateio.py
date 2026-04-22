"""Gate.io spot BBO listener.

TODO: implement. Endpoint wss://api.gateio.ws/ws/v4/, channel
``spot.book_ticker`` with payload ``{"time_ms", "result":
{"s", "b", "B", "a", "A", "t"}}``. Needs its own ping frame
``{"time": <unix>, "channel": "spot.ping"}`` every ~15s.
"""

from __future__ import annotations

from ..comparator import PricesBook


async def run_gateio(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    raise NotImplementedError("gateio exchange handler not implemented yet")
