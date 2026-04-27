"""Bitget USDT-FUTURES ``books1`` listener (picows).

Mirror of :mod:`arbitrage.exchanges.spot.bitget`. Protocol is identical
— same host, same path, same channel, same 25s ping/pong — the single
difference is ``instType: "USDT-FUTURES"`` (instead of ``"SPOT"``).
Exchange label is ``bitget-perp``.
"""

from __future__ import annotations

import asyncio
import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...persistence.ticks import record_tick
from ...comparator import PricesBook, check_and_signal_perp
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff

logger = logging.getLogger("arbitrage.bitget-perp")

_WS_URL = "wss://ws.bitget.com/v2/ws/public"
_EXCHANGE = "bitget-perp"
_INST_TYPE = "USDT-FUTURES"
_PING_INTERVAL_S = 25.0

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()
_PING_PAYLOAD = _encoder.encode({"op": "ping"})


class BitgetPerpListener(WSListener):
    __slots__ = ("_prices", "_symbols", "_transport", "_ping_task")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._symbols = frozenset(symbols)
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        sub = {
            "op": "subscribe",
            "args": [
                {"instType": _INST_TYPE, "channel": "books1", "instId": s}
                for s in self._symbols
            ],
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info(
            "bitget-perp: subscribed to %d books1 streams",
            len(self._symbols),
        )
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("bitget-perp: disconnected")
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
            logger.exception("bitget-perp: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return

        if msg.get("op") == "pong" or msg.get("event") in {"subscribe", "error"}:
            return

        arg = msg.get("arg")
        if not isinstance(arg, dict) or arg.get("channel") != "books1":
            return
        if arg.get("instType") != _INST_TYPE:
            return
        symbol = arg.get("instId")
        if symbol not in self._symbols:
            return

        data = msg.get("data")
        if not isinstance(data, list) or not data:
            return
        level = data[0]
        if not isinstance(level, dict):
            return

        try:
            bids = level.get("bids") or []
            asks = level.get("asks") or []
            if not bids or not asks:
                return
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            ts_exchange = int(level.get("ts") or 0)
        except (KeyError, TypeError, ValueError, IndexError):
            logger.debug("bitget-perp: malformed tick: %s", level)
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
            logger.exception("bitget-perp: ping loop crashed")


async def run_bitget(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("bitget-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: BitgetPerpListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("bitget-perp: connection error")
        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1
