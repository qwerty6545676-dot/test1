"""BingX spot ``bookTicker`` listener (picows + gzip).

Endpoint: ``wss://open-api-ws.bingx.com/market``.

All frames from the server are **gzipped BINARY**, including the
subscribe ack and the keepalive ping. The listener therefore ignores
picows' ``msg_type`` text/binary distinction and just tries to
gunzip every payload.

Observed protocol (gleaned from a live probe, not trusting the AI
spec, which had several wrong field names):

* Subscribe request — plain TEXT JSON, per symbol::

      {"id":"<uuid>", "reqType":"sub", "dataType":"BTC-USDT@bookTicker"}

* Successful subscribe ack — gzipped::

      {"code":0, "id":"...", "msg":"SUCCESS", "timestamp":...}

* BookTicker update — gzipped; fields mirror Binance (``b``/``B``/
  ``a``/``A``/``E``/``s``) rather than the ``bidPrice``/``bidQty``
  shape the upstream spec claimed::

      {"code":0,
       "dataType":"BTC-USDT@bookTicker",
       "data":{"s":"BTC-USDT",
               "b":"78132.00","B":"0.000094",
               "a":"78132.01","A":"0.003385",
               "E":1700000000123,"e":"bookTicker"}}

* Server keepalive — gzipped::

      {"ping":"<uuid>", "time":"2026-04-23T15:14:35.515+0800"}

  **Must be answered with ``{"pong":"<same-uuid>","time":"..."}`` or
  the connection drops.** The AI spec suggested we reply with the
  literal string ``"Pong"`` — that is wrong for BingX spot WS.
"""

from __future__ import annotations

import gzip
import logging
import time
import uuid

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ..comparator import PricesBook, check_and_signal
from ..normalizer import Tick, validate_tick
from ._common import sleep_backoff, to_native

logger = logging.getLogger("arbitrage.bingx")

_WS_URL = "wss://open-api-ws.bingx.com/market"
_EXCHANGE = "bingx"
_SEP = "-"

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


class BingxListener(WSListener):
    __slots__ = ("_prices", "_native_to_canonical", "_transport")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        self._transport: WSTransport | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        # BingX requires one subscribe per data stream, so we fan out.
        for native in self._native_to_canonical:
            req = {
                "id": uuid.uuid4().hex,
                "reqType": "sub",
                "dataType": f"{native}@bookTicker",
            }
            transport.send(WSMsgType.TEXT, _encoder.encode(req))
        logger.info("bingx: subscribed to %d streams", len(self._native_to_canonical))

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("bingx: disconnected")
        self._transport = None

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        raw = frame.get_payload_as_bytes()
        try:
            decompressed = gzip.decompress(raw)
        except (OSError, EOFError):
            # Not a gzipped frame — BingX spot sends everything gzipped,
            # so anything else is garbage and must not derail the loop.
            return

        try:
            msg = _decoder.decode(decompressed)
        except msgspec.DecodeError:
            return
        if not isinstance(msg, dict):
            return

        # Server ping: {"ping": "<uuid>", "time": "..."} — must pong back
        # with the same uuid, otherwise BingX drops the connection.
        ping_id = msg.get("ping")
        if ping_id is not None:
            pong = {"pong": ping_id, "time": msg.get("time")}
            transport.send(WSMsgType.TEXT, _encoder.encode(pong))
            return

        if msg.get("code", 0) != 0:
            logger.warning("bingx: non-zero code frame: %s", msg)
            return

        data = msg.get("data")
        if not isinstance(data, dict):
            # Subscribe ack carries msg="SUCCESS" and no data field.
            return

        native = data.get("s")
        canonical = self._native_to_canonical.get(native) if native else None
        if canonical is None:
            return

        try:
            bid = float(data["b"])
            ask = float(data["a"])
            ts_exchange = int(data.get("E") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("bingx: malformed tick: %s", data)
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

        check_and_signal(self._prices, canonical)


async def run_bingx(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("bingx: connecting")
            transport, _listener = await ws_connect(
                lambda: BingxListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("bingx: connection error")

        await sleep_backoff(attempt)
        attempt += 1


__all__ = ["BingxListener", "run_bingx"]
