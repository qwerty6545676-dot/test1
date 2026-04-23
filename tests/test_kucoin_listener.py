"""Parser-level tests for KuCoinListener."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.spot.kucoin import KuCoinListener


class _FakeFrame:
    def __init__(self, payload: bytes, msg_type: int = 1) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


_TEXT = 1
_encoder = msgspec.json.Encoder()


def _listener(symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")) -> tuple[KuCoinListener, PricesBook]:
    prices: PricesBook = {}
    return KuCoinListener(prices, symbols, ping_interval_ms=18000), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_message_populates_prices():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/market/ticker:BTC-USDT",
        "subject": "trade.ticker",
        "data": {
            "bestBid": "65000.1",
            "bestAsk": "65000.5",
            "bestBidSize": "0.5",
            "bestAskSize": "0.3",
            "Time": 1700000000123,
            "sequence": "42",
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["kucoin"]
    assert tick.bid == 65000.1
    assert tick.ask == 65000.5
    assert tick.ts_exchange == 1700000000123


def test_welcome_is_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"type": "welcome", "id": "x"}))
    assert prices == {}


def test_pong_is_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"type": "pong", "id": "x"}))
    assert prices == {}


def test_ack_is_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"type": "ack", "id": "x"}))
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/market/ticker:DOGE-USDT",
        "data": {"bestBid": "0.1", "bestAsk": "0.11", "Time": 1},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_unknown_topic_ignored():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/some/other:BTC-USDT",
        "data": {"bestBid": "1", "bestAsk": "2"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_malformed_price_dropped():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/market/ticker:BTC-USDT",
        "data": {"bestBid": "nope", "bestAsk": "1", "Time": 1},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}
