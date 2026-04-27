"""Parser-level tests for BingxPerpListener (swap-market, gzipped JSON)."""

from __future__ import annotations

import gzip

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp.bingx import BingxPerpListener


_encoder = msgspec.json.Encoder()


class _FakeFrame:
    def __init__(self, payload: bytes, msg_type: int = 2) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


class _FakeTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []

    def send(self, msg_type: int, payload: bytes) -> None:
        self.sent.append((msg_type, payload))


def _listener(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> tuple[BingxPerpListener, PricesBook]:
    prices: PricesBook = {}
    return BingxPerpListener(prices, symbols), prices


def _gz(obj) -> _FakeFrame:
    return _FakeFrame(gzip.compress(_encoder.encode(obj)))


def test_update_populates_prices():
    listener, prices = _listener()
    msg = {
        "code": 0,
        "dataType": "BTC-USDT@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": 1,
            "E": 1776941439179,
            "T": 1776941439170,
            "s": "BTC-USDT",
            "b": "77328.7",
            "B": "4.5829",
            "a": "77328.9",
            "A": "5.5300",
        },
    }
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    tick = prices["BTCUSDT"]["bingx-perp"]
    assert tick.bid == 77328.7
    assert tick.ask == 77328.9
    assert tick.ts_exchange == 1776941439179


def test_literal_ping_triggers_literal_pong():
    """BingX swap-market sends gzipped ``b"Ping"`` and expects ``b"Pong"`` back.

    Verified against ``open-api-swap.bingx.com``: the server pings every
    ~5 s and tears the connection down after ~30 s without a matching
    Pong. This was the root cause of bingx-perp reconnecting every 30 s
    in production runs against the 100-symbol universe — see PR notes.
    """
    listener, prices = _listener()
    transport = _FakeTransport()
    listener.on_ws_frame(transport=transport, frame=_FakeFrame(gzip.compress(b"Ping")))
    assert prices == {}
    assert transport.sent == [(1, b"Pong")]


def test_json_ping_still_triggers_json_pong():
    """Defensive fallback: if BingX ever switches the swap WS to JSON
    keepalive (which is what spot uses today), we must still reply
    correctly — otherwise a silent regression goes undetected."""
    listener, prices = _listener()
    transport = _FakeTransport()
    msg = {"ping": "abc123", "time": "2026-04-23T15:14:35.515+0800"}
    listener.on_ws_frame(transport=transport, frame=_gz(msg))
    assert prices == {}
    assert len(transport.sent) == 1
    _msg_type, payload = transport.sent[0]
    pong = msgspec.json.decode(payload)
    assert pong == {"pong": "abc123", "time": "2026-04-23T15:14:35.515+0800"}


def test_non_zero_code_ignored():
    listener, prices = _listener()
    msg = {"code": 1, "msg": "bad subscribe", "id": "x"}
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    assert prices == {}


def test_subscribe_ack_ignored():
    listener, prices = _listener()
    msg = {"code": 0, "id": "probe", "msg": "", "dataType": "", "data": None}
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener()
    msg = {
        "code": 0,
        "data": {"s": "DOGE-USDT", "b": "0.1", "a": "0.11", "E": 1},
    }
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    assert prices == {}


def test_non_gzip_frame_dropped_silently():
    listener, prices = _listener()
    listener.on_ws_frame(transport=_FakeTransport(), frame=_FakeFrame(b"not gzipped"))
    assert prices == {}
