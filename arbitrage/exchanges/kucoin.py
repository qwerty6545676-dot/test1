"""KuCoin spot BBO listener.

TODO: implement. KuCoin requires a REST bootstrap to
``/api/v1/bullet-public`` to obtain a token and WS endpoint, then
subscribe to ``/market/ticker:BTC-USDT``. Note symbol format uses a
``-`` separator. Requires client ``ping`` every ~18s.
"""

from __future__ import annotations

from ..comparator import PricesBook


async def run_kucoin(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    raise NotImplementedError("kucoin exchange handler not implemented yet")
