"""Parser-level tests for BingxListener — all frames are gzipped JSON."""

from __future__ import annotations

import gzip

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.bingx import BingxListener


_encoder = msgspec.json.Encoder()


class _FakeFrame:
    """Mimic picows WSFrame's payload method."""

    def __init__(self, payload: bytes, msg_type: int = 2) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


class _FakeTransport:
    """Records whatever the listener tries to send back (pong)."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, bytes]] = []

    def send(self, msg_type: int, payload: bytes) -> None:
        self.sent.append((msg_type, payload))


def _listener(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> tuple[BingxListener, PricesBook]:
    prices: PricesBook = {}
    return BingxListener(prices, symbols), prices


def _gz(obj) -> _FakeFrame:
    return _FakeFrame(gzip.compress(_encoder.encode(obj)))


def test_update_populates_prices():
    listener, prices = _listener()
    msg = {
        "code": 0,
        "dataType": "BTC-USDT@bookTicker",
        "data": {
            "s": "BTC-USDT",
            "b": "78132.00",
            "B": "0.000094",
            "a": "78132.01",
            "A": "0.003385",
            "E": 1700000000123,
            "e": "bookTicker",
        },
    }
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    tick = prices["BTCUSDT"]["bingx"]
    assert tick.bid == 78132.0
    assert tick.ask == 78132.01
    assert tick.ts_exchange == 1700000000123


def test_server_ping_triggers_pong():
    listener, prices = _listener()
    transport = _FakeTransport()
    msg = {"ping": "abc123", "time": "2026-04-23T15:14:35.515+0800"}
    listener.on_ws_frame(transport=transport, frame=_gz(msg))
    assert prices == {}
    assert len(transport.sent) == 1
    _msg_type, payload = transport.sent[0]
    pong = msgspec.json.decode(payload)
    assert pong == {"pong": "abc123", "time": "2026-04-23T15:14:35.515+0800"}


def test_subscribe_ack_ignored():
    listener, prices = _listener()
    msg = {"code": 0, "id": "x", "msg": "SUCCESS", "timestamp": 1}
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    assert prices == {}


def test_non_zero_code_ignored():
    listener, prices = _listener()
    msg = {"code": 1, "msg": "bad subscribe", "id": "x"}
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


def test_malformed_tick_dropped():
    listener, prices = _listener()
    msg = {
        "code": 0,
        "data": {"s": "BTC-USDT", "b": "nan?", "a": "1", "E": 1},
    }
    listener.on_ws_frame(transport=_FakeTransport(), frame=_gz(msg))
    assert prices == {}
