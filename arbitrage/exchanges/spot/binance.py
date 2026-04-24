"""Binance spot bookTicker listener (picows).

Stream: combined ``/stream?streams=<sym>@bookTicker/...``.
Each message has the shape::

    {
      "stream": "btcusdt@bookTicker",
      "data": {"u": 400900217, "s": "BTCUSDT",
               "b": "4.00000000", "B": "431.00000000",
               "a": "4.00000200", "A": "9.00000000",
               "T": 1589437530011, "E": 1589437530011}
    }

bookTicker updates on every change of the best bid/ask — exactly what
we need for arbitrage — and is not throttled. Binance forcibly closes
spot connections after 24h, so we wrap the whole thing in a reconnect
loop with exponential backoff + jitter.
"""

from __future__ import annotations

import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_spot
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff

logger = logging.getLogger("arbitrage.binance")

_WS_URL_TMPL = "wss://stream.binance.com:9443/stream?streams={streams}"
_EXCHANGE = "binance"

# msgspec decoder reused across frames — parsing is the hottest step
# in the pipeline, so we keep the decoder warm.
_decoder = msgspec.json.Decoder()


def _build_url(symbols: tuple[str, ...]) -> str:
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols)
    return _WS_URL_TMPL.format(streams=streams)


class BinanceListener(WSListener):
    """picows listener: decodes bookTicker frames straight into the book."""

    __slots__ = ("_prices", "_symbols")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        # Pre-materialize the set of symbols we care about, for O(1) filtering.
        self._symbols = frozenset(symbols)

    def on_ws_connected(self, transport: WSTransport) -> None:
        logger.info("binance: connected, streams=%d", len(self._symbols))

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("binance: disconnected")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return

        # Payload is only valid during this callback — decode now.
        try:
            msg = _decoder.decode(frame.get_payload_as_bytes())
        except msgspec.DecodeError:
            logger.exception("binance: bad JSON frame")
            return

        data = msg.get("data") if isinstance(msg, dict) else None
        if not data:
            # Subscription ack / pong / unknown envelope — skip.
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
            logger.debug("binance: malformed tick: %s", data)
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

        # Push-compare: fire as soon as the state changes.
        check_and_signal_spot(self._prices, symbol)


async def run_binance(
    prices: PricesBook,
    symbols: tuple[str, ...],
) -> None:
    """Connect-forever loop. Never returns under normal operation."""
    url = _build_url(symbols)
    attempt = 0

    while True:
        try:
            logger.info("binance: connecting")
            transport, _listener = await ws_connect(
                lambda: BinanceListener(prices, symbols),
                url,
                # Binance sends a server PING every ~3min, picows replies
                # automatically when enable_auto_pong=True (default).
                enable_auto_ping=False,
            )
            attempt = 0  # reset after a successful handshake
            await transport.wait_disconnected()
        except Exception:
            logger.exception("binance: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="spot")
        attempt += 1
