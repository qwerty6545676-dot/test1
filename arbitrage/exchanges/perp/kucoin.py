"""KuCoin futures ``/contractMarket/tickerV2`` listener.

Derivation of :mod:`arbitrage.exchanges.spot.kucoin`. Three things
differ from the spot listener:

1. **Bullet-public host** — futures uses
   ``api-futures.kucoin.com`` rather than ``api.kucoin.com``.
2. **WS host** — the bullet response points at
   ``ws-api-futures.kucoin.com`` (still token + connectId auth).
3. **Symbol mapping** — futures keeps legacy Kraken-style naming, so
   ``BTCUSDT`` is called ``XBTUSDTM`` on the wire (``BTC → XBT``,
   plus a trailing ``M`` suffix on every USDT-margined contract).
   That translation lives in :mod:`._symbol`.

Frame shape::

    {"topic":"/contractMarket/tickerV2:XBTUSDTM",
     "type":"message", "subject":"tickerV2",
     "data":{"symbol":"XBTUSDTM",
             "bestBidPrice":"77358.1", "bestBidSize":68,
             "bestAskPrice":"77358.2", "bestAskSize":2286,
             "ts":1776941410555000000,  # nanoseconds
             "sequence":...}}
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

import aiohttp
import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_perp
from ...normalizer import Tick, validate_tick
from ...ratelimit import AsyncTokenBucket
from .._common import sleep_backoff
from ._symbol import from_kucoin_perp, to_kucoin_perp

logger = logging.getLogger("arbitrage.kucoin-perp")

_BULLET_URL = "https://api-futures.kucoin.com/api/v1/bullet-public"
_EXCHANGE = "kucoin-perp"
_DEFAULT_PING_INTERVAL_MS = 18000

# Futures REST lives on a different host (api-futures.kucoin.com),
# which has its own 30 req / 3 s quota — so it gets its own bucket.
_BULLET_LIMITER = AsyncTokenBucket(capacity=10, tokens_per_s=10 / 3)

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


async def _fetch_ws_params() -> tuple[str, int]:
    """REST bootstrap: returns (ws_url_with_token, ping_interval_ms)."""
    await _BULLET_LIMITER.acquire()
    async with aiohttp.ClientSession() as session:
        async with session.post(
            _BULLET_URL, timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            payload = await resp.json()

    data = payload.get("data") or {}
    token = data.get("token")
    servers = data.get("instanceServers") or []
    if not token or not servers:
        raise RuntimeError(
            f"kucoin-perp: unexpected bullet-public response: {payload}"
        )
    server = servers[0]
    endpoint = server.get("endpoint")
    if not endpoint:
        raise RuntimeError(
            f"kucoin-perp: missing endpoint in bullet-public response: {payload}"
        )
    ping_interval_ms = int(server.get("pingInterval") or _DEFAULT_PING_INTERVAL_MS)
    connect_id = uuid.uuid4().hex
    ws_url = f"{endpoint}?token={token}&connectId={connect_id}"
    return ws_url, ping_interval_ms


class KuCoinPerpListener(WSListener):
    __slots__ = (
        "_prices",
        "_native_to_canonical",
        "_subscribe_topics",
        "_ping_interval_s",
        "_transport",
        "_ping_task",
    )

    def __init__(
        self,
        prices: PricesBook,
        symbols: tuple[str, ...],
        ping_interval_ms: int,
    ) -> None:
        super().__init__()
        self._prices = prices
        # BTCUSDT -> XBTUSDTM, ETHUSDT -> ETHUSDTM, etc.
        self._native_to_canonical = {to_kucoin_perp(s): s for s in symbols}
        self._subscribe_topics = ",".join(self._native_to_canonical.keys())
        self._ping_interval_s = max(ping_interval_ms, 5000) / 1000.0
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        sub = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": f"/contractMarket/tickerV2:{self._subscribe_topics}",
            "response": True,
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info(
            "kucoin-perp: subscribed to %d tickerV2 topics",
            len(self._native_to_canonical),
        )
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("kucoin-perp: disconnected")
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
            logger.exception("kucoin-perp: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return

        if msg.get("type") != "message":
            # welcome / ack / pong / error — ignore.
            return
        topic = msg.get("topic") or ""
        if not topic.startswith("/contractMarket/tickerV2:"):
            return

        native = topic.split(":", 1)[1]
        canonical = self._native_to_canonical.get(native)
        if canonical is None:
            # Best-effort fallback: compute canonical from the native
            # symbol if the subscription map missed it for some reason.
            canonical = from_kucoin_perp(native)
            if canonical == native:
                return

        data = msg.get("data")
        if not isinstance(data, dict):
            return

        try:
            bid = float(data["bestBidPrice"])
            ask = float(data["bestAskPrice"])
            # ts is in nanoseconds on futures — divide to milliseconds.
            ts_ns = int(data.get("ts") or 0)
            ts_exchange = ts_ns // 1_000_000 if ts_ns else 0
        except (KeyError, TypeError, ValueError):
            logger.debug("kucoin-perp: malformed tick: %s", data)
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
                await asyncio.sleep(self._ping_interval_s)
                transport = self._transport
                if transport is None:
                    return
                ping = {"id": str(int(time.time() * 1000)), "type": "ping"}
                transport.send(WSMsgType.TEXT, _encoder.encode(ping))
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("kucoin-perp: ping loop crashed")


async def run_kucoin(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("kucoin-perp: fetching ws params")
            ws_url, ping_interval_ms = await _fetch_ws_params()
            logger.info("kucoin-perp: connecting (ping=%dms)", ping_interval_ms)

            transport, _listener = await ws_connect(
                lambda: KuCoinPerpListener(prices, symbols, ping_interval_ms),
                ws_url,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("kucoin-perp: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="perp")
        attempt += 1


__all__ = ["KuCoinPerpListener", "run_kucoin", "_fetch_ws_params"]
