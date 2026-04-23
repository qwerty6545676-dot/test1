"""MEXC contract (perp) ``sub.ticker`` listener (picows, plain JSON).

MEXC is the one exchange where the perp listener is **simpler** than
the spot listener: the contract WebSocket speaks plain JSON over a
different host (``contract.mexc.com/edge``), so no protobuf parsing
or ``mexc_proto`` dependency is needed here.

Subscribe::

    {"method":"sub.ticker", "param":{"symbol":"BTC_USDT"}}

First response is a sub-ack::

    {"channel":"rs.sub.ticker", "data":"success", "ts":...}

Then one ticker frame per top-of-book change::

    {"symbol":"BTC_USDT",
     "data":{"bid1":77305, "ask1":77305.1,
             "lastPrice":..., "timestamp":1776941413753, ...}}

Keepalive: client sends ``{"method":"ping"}`` every 10s; server drops
the connection after ~20s of silence.
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

logger = logging.getLogger("arbitrage.mexc-perp")

_WS_URL = "wss://contract.mexc.com/edge"
_EXCHANGE = "mexc-perp"
_SEP = "_"
_PING_INTERVAL_S = 10.0

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()
_PING_PAYLOAD = _encoder.encode({"method": "ping"})


class MexcPerpListener(WSListener):
    __slots__ = (
        "_prices",
        "_native_to_canonical",
        "_transport",
        "_ping_task",
    )

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        for native in self._native_to_canonical:
            sub = {"method": "sub.ticker", "param": {"symbol": native}}
            transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info(
            "mexc-perp: subscribed to %d ticker streams",
            len(self._native_to_canonical),
        )
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("mexc-perp: disconnected")
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
            logger.exception("mexc-perp: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return

        # Sub acks / pongs: `{"channel":"rs.sub.ticker","data":"success"}`
        # or `{"channel":"pong",...}`. Any frame without an outer `symbol`
        # is metadata we ignore.
        native = msg.get("symbol")
        if not native:
            return
        canonical = self._native_to_canonical.get(native)
        if canonical is None:
            return

        data = msg.get("data")
        if not isinstance(data, dict):
            return

        try:
            bid = float(data["bid1"])
            ask = float(data["ask1"])
            ts_exchange = int(data.get("timestamp") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("mexc-perp: malformed tick: %s", data)
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
                transport.send(WSMsgType.TEXT, _PING_PAYLOAD)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("mexc-perp: ping loop crashed")


async def run_mexc(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("mexc-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: MexcPerpListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("mexc-perp: connection error")
        await sleep_backoff(attempt)
        attempt += 1


__all__ = ["MexcPerpListener", "run_mexc"]
