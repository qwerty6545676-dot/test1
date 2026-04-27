"""Binance USDM (perp) bookTicker listener (picows).

Mirror of :mod:`arbitrage.exchanges.spot.binance`. Only the host
changes: spot uses ``stream.binance.com`` whereas USDM perp uses
``fstream.binance.com``. Frame shape (``{e,s,b,B,a,A,E,T}``),
combined-streams path, 24h forced disconnect behaviour and backoff
are all identical, so the listener reuses the same code flow with a
different URL template and a ``binance-perp`` exchange label.
"""

from __future__ import annotations

import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...persistence.ticks import record_tick
from ...comparator import PricesBook, check_and_signal_perp
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff

logger = logging.getLogger("arbitrage.binance-perp")

_WS_URL_TMPL = "wss://fstream.binance.com/stream?streams={streams}"
_EXCHANGE = "binance-perp"

_decoder = msgspec.json.Decoder()


def _build_url(symbols: tuple[str, ...]) -> str:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    return _WS_URL_TMPL.format(streams=streams)


class BinancePerpListener(WSListener):
    __slots__ = ("_prices", "_symbols")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._symbols = frozenset(symbols)

    def on_ws_connected(self, transport: WSTransport) -> None:
        logger.info("binance-perp: connected, streams=%d", len(self._symbols))

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("binance-perp: disconnected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return

        try:
            msg = _decoder.decode(frame.get_payload_as_bytes())
        except msgspec.DecodeError:
            logger.exception("binance-perp: bad JSON frame")
            return

        data = msg.get("data") if isinstance(msg, dict) else None
        if not data:
            return

        try:
            symbol = data["s"]
        except (KeyError, TypeError):
            return
        if symbol not in self._symbols:
            return

        try:
            bid = float(data["b"])
            ask = float(data["a"])
            ts_exchange = int(data.get("T") or data.get("E") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("binance-perp: malformed tick: %s", data)
            return

        tick = Tick(
            exchange=_EXCHANGE,
            symbol=symbol,
            bid=bid,
            ask=ask,
            ts_exchange=ts_exchange,
            ts_local=int(time.time() * 1000),
        )
        if not validate_tick(tick):
            return

        book = self._prices.get(symbol)
        if book is None:
            book = {}
            self._prices[symbol] = book
        book[_EXCHANGE] = tick
        record_tick(tick, "perp")

        check_and_signal_perp(self._prices, symbol)


async def run_binance(
    prices: PricesBook,
    symbols: tuple[str, ...],
) -> None:
    url = _build_url(symbols)
    attempt = 0

    while True:
        try:
            logger.info("binance-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: BinancePerpListener(prices, symbols),
                url,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("binance-perp: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1
