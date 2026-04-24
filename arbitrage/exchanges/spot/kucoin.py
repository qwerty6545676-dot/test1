"""KuCoin spot ``/market/ticker`` listener (picows + aiohttp bootstrap).

Unlike every other venue we touch, KuCoin does not let us connect
straight to a static WS URL. We must first POST to
``/api/v1/bullet-public`` to obtain a short-lived token and the actual
WS endpoint. The handshake has to be repeated for **every** reconnect
— the token is single-use and expires.

Subscribe::

    {"id": "<millis>", "type": "subscribe",
     "topic": "/market/ticker:BTC-USDT,ETH-USDT,...",
     "response": true}

Message frame::

    {"type": "message",
     "topic": "/market/ticker:BTC-USDT",
     "subject": "trade.ticker",
     "data": {"bestBid": "...", "bestAsk": "...",
              "bestBidSize": "...", "bestAskSize": "...",
              "Time": 1700000000123, "sequence": "..."}}

Before any data the server sends a ``welcome`` frame, plus a ``pong``
every time we ping. Both must be filtered out.

Ping frequency is dictated by the server (``pingInterval`` in the
bullet response, typically ~18000ms). We send ``{"type":"ping"}`` at
that interval.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

import aiohttp
import msgspec
from picows import WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from ...comparator import PricesBook, check_and_signal_spot
from ...normalizer import Tick, validate_tick
from ...ratelimit import AsyncTokenBucket
from .._common import sleep_backoff, to_native

logger = logging.getLogger("arbitrage.kucoin")

_BULLET_URL = "https://api.kucoin.com/api/v1/bullet-public"
_EXCHANGE = "kucoin"
_SEP = "-"
_DEFAULT_PING_INTERVAL_MS = 18000

# KuCoin's documented public REST limit is 30 req / 3 s per IP. We
# only actually hit it from one place (this bootstrap), so a capacity
# of 10 with a generous 10 tokens / 3 s refill leaves plenty of room
# and still protects us against reconnect storms.
_BULLET_LIMITER = AsyncTokenBucket(capacity=10, tokens_per_s=10 / 3)

_decoder = msgspec.json.Decoder()
_encoder = msgspec.json.Encoder()


async def _fetch_ws_params() -> tuple[str, int]:
    """REST bootstrap: returns (ws_url_with_token, ping_interval_ms)."""
    await _BULLET_LIMITER.acquire()
    async with aiohttp.ClientSession() as session:
        async with session.post(_BULLET_URL, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            payload = await resp.json()

    data = payload.get("data") or {}
    token = data.get("token")
    servers = data.get("instanceServers") or []
    if not token or not servers:
        raise RuntimeError(f"kucoin: unexpected bullet-public response: {payload}")
    server = servers[0]
    endpoint = server.get("endpoint")
    if not endpoint:
        raise RuntimeError(f"kucoin: missing endpoint in bullet-public response: {payload}")
    ping_interval_ms = int(server.get("pingInterval") or _DEFAULT_PING_INTERVAL_MS)
    # `connectId` MUST be unique per connection.
    connect_id = uuid.uuid4().hex
    ws_url = f"{endpoint}?token={token}&connectId={connect_id}"
    return ws_url, ping_interval_ms


class KuCoinListener(WSListener):
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
        self._native_to_canonical = {to_native(s, _SEP): s for s in symbols}
        # KuCoin supports comma-separated topics; one subscribe message is enough.
        self._subscribe_topics = ",".join(self._native_to_canonical.keys())
        self._ping_interval_s = max(ping_interval_ms, 5000) / 1000.0
        self._transport: WSTransport | None = None
        self._ping_task: asyncio.Task[None] | None = None

    def on_ws_connected(self, transport: WSTransport) -> None:
        self._transport = transport
        sub = {
            "id": str(int(time.time() * 1000)),
            "type": "subscribe",
            "topic": f"/market/ticker:{self._subscribe_topics}",
            "response": True,
        }
        transport.send(WSMsgType.TEXT, _encoder.encode(sub))
        logger.info("kucoin: subscribed to %d tickers", len(self._native_to_canonical))
        self._ping_task = asyncio.create_task(self._ping_loop())

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        logger.warning("kucoin: disconnected")
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
            logger.exception("kucoin: bad JSON frame")
            return
        if not isinstance(msg, dict):
            return

        msg_type = msg.get("type")
        if msg_type != "message":
            # welcome / ack / pong / error — ignore.
            return
        topic = msg.get("topic") or ""
        if not topic.startswith("/market/ticker:"):
            return

        native = topic.split(":", 1)[1]
        canonical = self._native_to_canonical.get(native)
        if canonical is None:
            return

        data = msg.get("data")
        if not isinstance(data, dict):
            return

        try:
            bid = float(data["bestBid"])
            ask = float(data["bestAsk"])
            ts_exchange = int(data.get("Time") or data.get("time") or 0)
        except (KeyError, TypeError, ValueError):
            logger.debug("kucoin: malformed tick: %s", data)
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

        check_and_signal_spot(self._prices, canonical)

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
            logger.exception("kucoin: ping loop crashed")


async def run_kucoin(prices: PricesBook, symbols: tuple[str, ...]) -> None:
    attempt = 0
    while True:
        try:
            logger.info("kucoin: fetching ws params")
            ws_url, ping_interval_ms = await _fetch_ws_params()
            logger.info("kucoin: connecting (ping=%dms)", ping_interval_ms)

            transport, _listener = await ws_connect(
                lambda: KuCoinListener(prices, symbols, ping_interval_ms),
                ws_url,
                enable_auto_ping=False,
            )
            attempt = 0
            await transport.wait_disconnected()
        except Exception:
            logger.exception("kucoin: connection error")

        await sleep_backoff(attempt, exchange=_EXCHANGE, market="spot")
        attempt += 1


__all__ = ["KuCoinListener", "run_kucoin", "_fetch_ws_params"]
