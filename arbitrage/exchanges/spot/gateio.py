"""Gate.io spot ``spot.book_ticker`` listener (picows).

Endpoint: ``wss://api.gateio.ws/ws/v4/``.

Subscribe::

    {"time": <unix>, "channel": "spot.book_ticker",
     "event": "subscribe", "payload": ["BTC_USDT", ...]}

Update frame::

    {"time": ..., "channel": "spot.book_ticker", "event": "update",
     "result": {"t": 1700000000123, "u": 1, "s": "BTC_USDT",
                "b": "65000.1", "a": "65000.5"}}

Gate.io closes idle connections silently (no error frame), so we send
a manual ``{"channel":"spot.ping"}`` every 10s.
"""

from __future__ import annotations

import asyncio
import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_spot
from ...normalizer import Tick, validate_tick
from .._common import from_native, sleep_backoff, to_native

logger = logging.getLogger("arbitrage.gateio")

_WS_URL = "wss://api.gateio.ws/ws/v4/"
_EXCHANGE = "gateio"
_PING_INTERVAL_S = 10.0
_SEP = "_"

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


class GateioListener(WSListener):
    __slots__ = (
        "_prices",
        "_symbols_canonical",
        "_native_to_canonical",
        "_transport",
        "_ping_task",
    )

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._symbols_canonical = frozenset(symbols)
        # Lookup table for incoming "BTC_USDT" -> "BTCUSDT".
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport

        sub = {
            "time": int(time.time()),
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "payload": [to_native(s, _SEP) for s in self._symbols_canonical],
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info("gateio: subscribed to %d symbols", len(self._symbols_canonical))
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("gateio: disconnected")
        if self._ping_task is not None and not self._ping_task.done():
            self._ping_task.cancel()
            self._ping_task = None
        self._transport = None

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return
        try:
            msg = _decoder.decode(frame.get_payload_as_bytes())
        except msgspec.DecodeError:
            logger.exception("gateio: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return

        if msg.get("channel") == "spot.pong":
            return
        if msg.get("event") != "update" or msg.get("channel") != "spot.book_ticker":
            return

        result = msg.get("result")
        if not isinstance(result, dict):
            return

        native = result.get("s")
        canonical = self._native_to_canonical.get(native) if native else None
        if canonical is None:
            return

        try:
            bid = float(result["b"])
            ask = float(result["a"])
            ts_exchange = int(result.get("t") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("gateio: malformed tick: %s", result)
            return

        tick = Tick(
            exchange=_EXCHANGE,
            symbol=canonical,
            bid=bid,
            ask=ask,
            ts_exchange=ts_exchange,
            ts_local=int(time.time() * 1000),
        )
        if not validate_tick(tick):
            return

        book = self._prices.get(canonical)
        if book is None:
            book = {}
            self._prices[canonical] = book
        book[_EXCHANGE] = tick

        check_and_signal_spot(self._prices, canonical)

    async def _ping_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(_PING_INTERVAL_S)
                transport = self._transport
                if transport is None:
                    return
                ping = {"time": int(time.time()), "channel": "spot.ping"}
                transport.send(WSMsgType.TEXT, _encoder.encode(ping))
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("gateio: ping loop crashed")


async def run_gateio(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("gateio: connecting")
            transport, _listener = await ws_connect(
                lambda: GateioListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("gateio: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="spot")
        attempt += 1


# Keep the helper exported so tests don't have to reach into the listener.
__all__ = ["GateioListener", "run_gateio", "from_native"]
