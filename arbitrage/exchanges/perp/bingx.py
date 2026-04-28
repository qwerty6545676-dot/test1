"""BingX swap (perp) ``bookTicker`` listener (picows + gzip).

Differences from :mod:`arbitrage.exchanges.spot.bingx`:

- URL: ``wss://open-api-swap.bingx.com/swap-market`` (spot uses
  ``/market`` under a different host).
- Exchange label: ``bingx-perp``.
- **Keepalive protocol is different from spot.** Verified live
  against ``open-api-swap.bingx.com``: the server pings with the
  literal 4-byte BINARY frame ``b"Ping"`` (gzipped) every 5 s and
  drops the connection after ~30 s without a matching ``b"Pong"``
  reply. The spot WS uses JSON ``{"ping": "<uuid>"}`` /
  ``{"pong": "<uuid>"}`` instead — same code path won't work for
  both. We handle the literal form here; the JSON form stays
  defensively as a fallback in case BingX flips it back.
"""

from __future__ import annotations

import gzip
import logging
import time
import uuid

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...persistence.ticks import record_tick
from ...comparator import PricesBook, check_and_signal_perp
from ...normalizer import Tick, validate_tick
from .._common import sleep_backoff, to_native

logger = logging.getLogger("arbitrage.bingx-perp")

_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
_EXCHANGE = "bingx-perp"
_SEP = "-"

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


class BingxPerpListener(WSListener):
    __slots__ = ("_prices", "_native_to_canonical", "_transport")

    def __init__(self, prices: PricesBook, symbols: tuple[str, ...]) -> None:
        super().__init__()
        self._prices = prices
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        self._transport: WSTransport | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        for native in self._native_to_canonical:
            req = {
                "id": uuid.uuid4().hex,
                "reqType": "sub",
                "dataType": f"{native}@bookTicker",
            }
            transport.send(WSMsgType.TEXT, _encoder.encode(req))
        logger.info(
            "bingx-perp: subscribed to %d streams",
            len(self._native_to_canonical),
        )

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("bingx-perp: disconnected")
        self._transport = None

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        raw = frame.get_payload_as_bytes()
        try:
            decompressed = gzip.decompress(raw)
        except (OSError, EOFError):
            return

        # Literal-text keepalive: BingX swap-market sends ``b"Ping"``
        # every ~5 s and tears the connection down after ~30 s without
        # a ``b"Pong"`` reply. The spot WS does *not* use this form
        # (it sends JSON ``{"ping": "<uuid>", ...}``); see the spot
        # listener for the JSON variant. Cheap byte-equality check
        # before JSON decode.
        if decompressed == b"Ping":
            transport.send(WSMsgType.TEXT, b"Pong")
            return

        try:
            msg = _decoder.decode(decompressed)
        except msgspec.DecodeError:
            return
        if not isinstance(msg, dict):
            return

        # Defensive fallback: if BingX ever ships the JSON-style
        # keepalive on the swap WS too, handle it the same way the
        # spot listener does.
        ping_id = msg.get("ping")
        if ping_id is not None:
            pong = {"pong": ping_id, "time": msg.get("time")}
            transport.send(WSMsgType.TEXT, _encoder.encode(pong))
            return

        if msg.get("code", 0) != 0:
            logger.warning("bingx-perp: non-zero code frame: %s", msg)
            return

        data = msg.get("data")
        if not isinstance(data, dict):
            return

        native = data.get("s")
        canonical = self._native_to_canonical.get(native) if native else None
        if canonical is None:
            return

        try:
            bid = float(data["b"])
            ask = float(data["a"])
            ts_exchange = int(data.get("E") or data.get("T") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("bingx-perp: malformed tick: %s", data)
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
        record_tick(tick, "perp")

        check_and_signal_perp(self._prices, canonical)


async def run_bingx(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("bingx-perp: connecting")
            transport, _listener = await ws_connect(
                lambda: BingxPerpListener(prices, symbols),
                _WS_URL,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("bingx-perp: connection error")
        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1


__all__ = ["BingxPerpListener", "run_bingx"]
