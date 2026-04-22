"""MEXC spot BBO listener.

TODO: implement. MEXC spot v3 WS uses protobuf payloads on channel
``spot@public.aggre.bookTicker.v3.api.pb@100ms@<SYMBOL>``. Needs the
``MEXC`` .proto files and the ``protobuf`` package for decoding.
"""

from __future__ import annotations

from ..comparator import PricesBook


async def run_mexc(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    raise NotImplementedError("mexc exchange handler not implemented yet")
