"""BingX swap (perp) ``bookTicker`` listener (picows + gzip).

Mirror of :mod:`arbitrage.exchanges.spot.bingx`. Differences from the
spot listener are confined to the endpoint path and the exchange
label:

- URL: ``wss://open-api-swap.bingx.com/swap-market`` (spot uses
  ``/market`` under a different host).
- Exchange label: ``bingx-perp``.

Gzip framing, per-stream subscribe (``reqType: sub``), ``BTC-USDT``
symbol convention, ``{ping: <uuid>, time: ...}`` → ``{pong: uuid, …}``
keepalive and the reconnect loop are identical.
"""

from __future__ import annotations

import gzip
import logging
import time
import uuid

import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

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

        try:
            msg = _decoder.decode(decompressed)
        except msgspec.DecodeError:
            return
        if not isinstance(msg, dict):
            return

        # Server keepalive — echo pong with same id.
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
