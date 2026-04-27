"""Bybit spot ``orderbook.1`` listener (picows).

We use ``orderbook.1.SYMBOL`` rather than ``tickers.SYMBOL``: the
orderbook stream is the raw BBO and pushes on every change of the top
level, while ``tickers`` mixes in 24h statistics and is effectively
throttled. The delta / snapshot payloads here have the shape::

    snapshot:
      {"topic": "orderbook.1.BTCUSDT", "type": "snapshot", "ts": 1234,
       "data": {"s": "BTCUSDT",
                "b": [["65000.00", "0.5"]],
                "a": [["65001.00", "0.3"]],
                "u": 1, "seq": 100}}

    delta (only sides that changed are present; qty "0" = delete):
      {"topic": "orderbook.1.BTCUSDT", "type": "delta", "ts": 1235,
       "data": {"s": "BTCUSDT",
                "b": [["65000.10", "0.4"]],
                "u": 2, "seq": 101}}

Because delta frames can omit a side we keep a per-symbol merged state
``_state[symbol] = {"b": (price, qty), "a": (price, qty)}`` and merge
each delta on top. After each merge we emit a `Tick` and run the
comparator.

Bybit's public WS kills the connection after ~20s of silence, so we
run a ping task that emits ``{"op":"ping"}`` every 18s.
"""

from __future__ import annotations

import asyncio
import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...persistence.ticks import record_tick
from ...comparator import PricesBook, check_and_signal_spot
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff

logger = logging.getLogger("arbitrage.bybit")

_WS_URL = "wss://stream.bybit.com/v5/public/spot"
_EXCHANGE = "bybit"
_PING_INTERVAL_S = 18.0

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()

# Pre-encoded ping frame — avoids allocating on every tick.
_PING_PAYLOAD = _encoder.encode({"op": "ping"})


def _chunked(seq: list, n: int) -> list[list]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


class BybitListener(WSListener):
    """picows listener with snapshot+delta state for orderbook.1."""

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
        # Per-symbol merged top-of-book state. Values are
        # {"b": (price, qty), "a": (price, qty)}.
        self._state: dict[str, dict[str, tuple[float, float]]] = {}
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    # ---------- picows callbacks ----------

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        logger.info("bybit: connected, subscribing to %d orderbook.1 streams",
                    len(self._symbols))

        # Subscribe. Bybit caps a single ``op:subscribe`` request at
        # 10 args; if you exceed it the server replies with
        # ``{"success":false,"ret_msg":"args size > 10"}`` and
        # silently drops the *whole* request. Chunk to be safe.
        args = [f"orderbook.1.{s}" for s in self._symbols]
        for chunk in _chunked(args, 10):
            sub = {"op": "subscribe", "args": chunk}
            transport.send(WSMsgType.TEXT, _encoder.encode(sub))

        # Start the 18s ping loop.
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("bybit: disconnected")
        if self._ping_task is not None and not self._ping_task.done():
            self._ping_task.cancel()
            self._ping_task = None
        self._transport = None
        # Drop merged state — we must re-snapshot after reconnect.
        self._state.clear()

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return

        try:
            msg = _decoder.decode(frame.get_payload_as_bytes())
        except msgspec.DecodeError:
            logger.exception("bybit: bad JSON frame")
            return

        if not isinstance(msg, dict):
            return

        topic = msg.get("topic")
        if not topic or not topic.startswith("orderbook.1."):
            # Subscription ack / pong / auth / ... — ignore.
            return

        self._handle_orderbook(msg)

    # ---------- internals ----------

    async def _ping_loop(self) -> None:
        """Keepalive: Bybit drops public connections after ~20s of silence."""
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
            logger.exception("bybit: ping loop crashed")

    def _handle_orderbook(self, msg: dict) -> None:
        data = msg.get("data")
        if not isinstance(data, dict):
            return

        symbol = data.get("s")
        if not symbol:
            return

        msg_type = msg.get("type")
        if msg_type == "snapshot":
            state = {}
            self._state[symbol] = state
        else:
            state = self._state.get(symbol)
            if state is None:
                # Delta before snapshot — wait for next snapshot.
                return

        # Merge side updates. Each entry is [[price, qty], ...]; for
        # orderbook.1 depth is 1 but we still defensively take [0].
        if _merge_side(state, data.get("b"), "b") is False:
            return
        if _merge_side(state, data.get("a"), "a") is False:
            return

        bid_pair = state.get("b")
        ask_pair = state.get("a")
        if bid_pair is None or ask_pair is None:
            return

        bid_price, _bid_qty = bid_pair
        ask_price, _ask_qty = ask_pair

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
        record_tick(tick, "spot")

        check_and_signal_spot(self._prices, symbol)


def _merge_side(
    state: dict[str, tuple[float, float]],
    levels: object,
    side: str,
) -> bool | None:
    """Apply a side update in-place. Returns False on parse error.

    - `None` / missing side = no update for this side (common in deltas).
    - qty "0" = the top level is gone; we pop it from state and the caller
      will skip emitting a tick until we get a replacement.
    """
    if levels is None:
        return None
    if not isinstance(levels, list) or not levels:
        # Present but empty — also "no update".
        return None

    top = levels[0]
    if not isinstance(top, (list, tuple)) or len(top) < 2:
        return False
    try:
        price = float(top[0])
        qty = float(top[1])
    except (TypeError, ValueError):
        return False

    if qty == 0.0:
        state.pop(side, None)
    else:
        state[side] = (price, qty)
    return None


async def run_bybit(
    prices: PricesBook,
    symbols: tuple[str, ...],
) -> None:
    """Connect-forever loop with exponential backoff + jitter."""
    attempt = 0
    while True:
        try:
            logger.info("bybit: connecting")
            transport, _listener = await ws_connect(
                lambda: BybitListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("bybit: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="spot")
        attempt += 1
