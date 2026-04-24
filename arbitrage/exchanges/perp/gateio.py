"""Gate.io USDT-M futures ``futures.book_ticker`` listener (picows).

Mirror of :mod:`arbitrage.exchanges.spot.gateio`. Differences from the
spot listener:

- Host: ``fx-ws.gateio.ws`` (futures endpoint).
- Channel: ``futures.book_ticker`` (vs spot ``spot.book_ticker``).
- Ping: ``futures.ping`` (vs ``spot.ping``).

Symbol format (``BTC_USDT``) and the ``result.{s,b,a,t,u}`` frame
layout are the same. ``B`` / ``A`` in the perp payload are contract
counts rather than coin quantities, but the scanner only reads the
prices so it doesn't affect arbitrage detection.
"""

from __future__ import annotations

import asyncio
import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_perp
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff, to_native

logger = logging.getLogger("arbitrage.gateio-perp")

_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
_EXCHANGE = "gateio-perp"
_PING_INTERVAL_S = 10.0
_SEP = "_"

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


class GateioPerpListener(WSListener):
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
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        sub = {
            "time": int(time.time()),
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": [to_native(s, _SEP) for s in self._symbols_canonical],
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info(
            "gateio-perp: subscribed to %d symbols",
            len(self._symbols_canonical),
        )
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("gateio-perp: disconnected")
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
            logger.exception("gateio-perp: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return
        if msg.get("channel") == "futures.pong":
            return
        if msg.get("event") != "update" or msg.get("channel") != "futures.book_ticker":
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
            logger.debug("gateio-perp: malformed tick: %s", result)
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

        check_and_signal_perp(self._prices, canonical)

    async def _ping_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(_PING_INTERVAL_S)
                transport = self._transport
                if transport is None:
                    return
                ping = {"time": int(time.time()), "channel": "futures.ping"}
                transport.send(WSMsgType.TEXT, _encoder.encode(ping))
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("gateio-perp: ping loop crashed")


async def run_gateio(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("gateio-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: GateioPerpListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("gateio-perp: connection error")
        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1
