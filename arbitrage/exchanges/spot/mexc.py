"""MEXC spot aggre-bookTicker listener (picows + Protobuf).

Endpoint: ``wss://wbs-api.mexc.com/ws`` (the old ``wbs.mexc.com/ws``
was retired in Aug 2025).

MEXC is the only venue in this project that uses Protobuf for market
data. Subscribe / control frames are still JSON; only the BINARY
data frames are wire-format ``PushDataV3ApiWrapper``.

Channel choice — we use ``aggre.bookTicker@100ms`` instead of either
``bookTicker`` or ``bookTicker.batch``. A live probe showed that the
plain ``spot@public.bookTicker.v3.api.pb`` channel is "Blocked" for
new subscribers while the aggregated one accepts subscriptions and
pushes fresh ticks at a configurable interval (10ms / 100ms). 100ms
is plenty for cross-exchange arbitrage and uses a fraction of the
bandwidth of 10ms.

Each binary frame decodes to ``PushDataV3ApiWrapper``:

* ``channel`` — the subscription channel we originally asked for.
* ``symbol`` — canonical form, e.g. ``"BTCUSDT"`` (no separator).
* ``sendTime`` — exchange push time in ms.
* ``publicAggreBookTicker`` oneof body — has ``bidPrice`` / ``askPrice``
  as strings (see ``proto/mexc/PublicAggreBookTickerV3Api.proto``).
"""

from __future__ import annotations

import logging
import time

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_spot
from ...mexc_proto import PushDataV3ApiWrapper_pb2
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff

logger = logging.getLogger("arbitrage.mexc")

_WS_URL = "wss://wbs-api.mexc.com/ws"
_EXCHANGE = "mexc"
_AGGRE_INTERVAL_MS = 100  # supported: 10, 100

_encoder = msgspec.json.Encoder()


def _aggre_param(symbol: str) -> str:
    return f"spot@public.aggre.bookTicker.v3.api.pb@{_AGGRE_INTERVAL_MS}ms@{symbol}"


class MexcListener(WSListener):
    __slots__ = ("_prices", "_symbols", "_transport", "_wrapper")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        # MEXC uses canonical ``BTCUSDT`` natively — no translation.
        self._symbols = frozenset(symbols)
        self._transport: WSTransport | None = None
        # Reuse one wrapper instance to avoid per-tick allocation.
        self._wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        sub = {
            "method": "SUBSCRIPTION",
            "params": [_aggre_param(s) for s in self._symbols],
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info("mexc: subscribed to %d aggre-bookTicker streams", len(self._symbols))

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("mexc: disconnected")
        self._transport = None

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        raw = frame.get_payload_as_bytes()
        if not raw:
            return

        # JSON control frames (subscribe ack, errors, pong) start with "{".
        # Protobuf frames start with field-1 tag 0x0a. This is cheaper
        # than try/except-wrapping every ParseFromString call.
        if raw[0:1] == b"{":
            return

        wrapper = self._wrapper
        wrapper.Clear()
        try:
            wrapper.ParseFromString(raw)
        except Exception:
            logger.exception("mexc: protobuf decode failed")
            return

        if wrapper.WhichOneof("body") != "publicAggreBookTicker":
            return

        symbol = wrapper.symbol
        if symbol not in self._symbols:
            return

        body = wrapper.publicAggreBookTicker
        try:
            bid = float(body.bidPrice)
            ask = float(body.askPrice)
        except (TypeError, ValueError):
            logger.debug("mexc: malformed tick: bid=%r ask=%r", body.bidPrice, body.askPrice)
            return

        tick = Tick(
            exchange=_EXCHANGE,
            symbol=symbol,
            bid=bid,
            ask=ask,
            ts_exchange=wrapper.sendTime,
            ts_local=int(time.time() * 1000),
        )
        if not validate_tick(tick):
            return

        book = self._prices.get(symbol)
        if book is None:
            book = {}
            self._prices[symbol] = book
        book[_EXCHANGE] = tick

        check_and_signal_spot(self._prices, symbol)


async def run_mexc(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("mexc: connecting")
            transport, _listener = await ws_connect(
                lambda: MexcListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("mexc: connection error")

        await sleep_backoff(attempt)
        attempt += 1


__all__ = ["MexcListener", "run_mexc"]
