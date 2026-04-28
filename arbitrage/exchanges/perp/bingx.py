"""BingX swap (perp) ``bookTicker`` listener (picows + gzip).

Endpoint: ``wss://open-api-swap.bingx.com/swap-market``. Exchange
label: ``bingx-perp``.

The frame format mirrors :mod:`arbitrage.exchanges.spot.bingx` —
gzipped payloads, per-stream ``reqType:sub`` subscriptions,
``BTC-USDT`` symbol convention — but **the keepalive protocol is
different**:

* Spot WS sends a JSON ping (``{"ping":"<uuid>","time":"..."}``)
  and expects a JSON pong with the same uuid.
* Perp WS sends the **literal binary string** ``b"Ping"`` (gzipped)
  every ~5s and expects ``b"Pong"`` (plain bytes) in reply. If the
  client doesn't respond the connection is closed after ~30s,
  which manifested as a reconnect every ~32s in production.

The listener handles the literal-binary form first and keeps the
JSON-ping branch as a defensive fallback in case BingX ever unifies
the two endpoints.
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

        # Server keepalive — perp endpoint sends the literal bytes
        # ``b"Ping"`` (gzipped). Reply with ``b"Pong"`` immediately,
        # before attempting JSON decode, otherwise the connection is
        # dropped after ~30s.
        if decompressed == b"Ping":
            transport.send(WSMsgType.TEXT, b"Pong")
            return

        try:
            msg = _decoder.decode(decompressed)
        except msgspec.DecodeError:
            return
        if not isinstance(msg, dict):
            return

        # Defensive fallback: handle a JSON ping the same way the spot
        # endpoint does, in case BingX ever unifies the two protocols.
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
