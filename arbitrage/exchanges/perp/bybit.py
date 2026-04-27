"""Bybit linear (perp) ``orderbook.1`` listener (picows).

Mirror of :mod:`arbitrage.exchanges.spot.bybit`. The only protocol
differences from spot are:

- path is ``/v5/public/linear`` instead of ``/v5/public/spot``;
- exchange label is ``bybit-perp`` so spot and perp show up as
  separate keys inside the same per-symbol book.

Snapshot/delta merge logic, the 18s keepalive and the reconnect loop
are shared with the spot module via :func:`_merge_side`.
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
from ..spot.bybit import _merge_side  # reuse the tested helper

logger = logging.getLogger("arbitrage.bybit-perp")

_WS_URL = "wss://stream.bybit.com/v5/public/linear"
_EXCHANGE = "bybit-perp"
_PING_INTERVAL_S = 18.0

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()
_PING_PAYLOAD = _encoder.encode({"op": "ping"})


class BybitPerpListener(WSListener):
    __slots__ = (
        "_prices",
        "_symbols",
        "_state",
        "_transport",
        "_ping_task",
    )

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._symbols = tuple(symbols)
        self._state: dict[str, dict[str, tuple[float, float]]] = {}
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        logger.info(
            "bybit-perp: connected, subscribing to %d orderbook.1 streams",
            len(self._symbols),
        )
        sub = {"op": "subscribe", "args": [f"orderbook.1.{s}" for s in self._symbols]}
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("bybit-perp: disconnected")
        if self._ping_task is not None and not self._ping_task.done():
            self._ping_task.cancel()
            self._ping_task = None
        self._transport = None
        self._state.clear()

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return
        try:
            msg = _decoder.decode(frame.get_payload_as_bytes())
        except msgspec.DecodeError:
            logger.exception("bybit-perp: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return
        topic = msg.get("topic")
        if not topic or not topic.startswith("orderbook.1."):
            return
        self._handle_orderbook(msg)

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
            logger.exception("bybit-perp: ping loop crashed")

    def _handle_orderbook(self, msg: dict) -> None:
        data = msg.get("data")
        if not isinstance(data, dict):
            return
        symbol = data.get("s")
        if not symbol:
            return

        msg_type = msg.get("type")
        if msg_type == "snapshot":
            state: dict[str, tuple[float, float]] = {}
            self._state[symbol] = state
        else:
            state = self._state.get(symbol)
            if state is None:
                return

        if _merge_side(state, data.get("b"), "b") is False:
            return
        if _merge_side(state, data.get("a"), "a") is False:
            return

        bid_pair = state.get("b")
        ask_pair = state.get("a")
        if bid_pair is None or ask_pair is None:
            return

        bid_price, _ = bid_pair
        ask_price, _ = ask_pair
        ts_exchange = msg.get("ts") or data.get("ts") or 0
        tick = Tick(
            exchange=_EXCHANGE,
            symbol=symbol,
            bid=bid_price,
            ask=ask_price,
            ts_exchange=int(ts_exchange),
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


async def run_bybit(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("bybit-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: BybitPerpListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("bybit-perp: connection error")
        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1
