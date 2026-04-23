"""Parser-level tests for BybitPerpListener (linear orderbook.1)."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp.bybit import BybitPerpListener


class _FakeFrame:
    def __init__(self, payload: bytes, msg_type: int = 1) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


_TEXT = 1
_encoder = msgspec.json.Encoder()


def _listener(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> tuple[BybitPerpListener, PricesBook]:
    prices: PricesBook = {}
    return BybitPerpListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_snapshot_populates_prices():
    listener, prices = _listener()
    msg = {
        "topic": "orderbook.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1700000000123,
        "data": {
            "s": "BTCUSDT",
            "b": [["65000.00", "0.5"]],
            "a": [["65001.00", "0.3"]],
            "u": 1,
            "seq": 100,
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["bybit-perp"]
    assert tick.bid == 65000.00
    assert tick.ask == 65001.00
    assert tick.ts_exchange == 1700000000123


def test_delta_merges_on_top_of_snapshot():
    listener, prices = _listener()
    snap = {
        "topic": "orderbook.1.BTCUSDT",
        "type": "snapshot",
        "ts": 1,
        "data": {"s": "BTCUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 1},
    }
    delta = {
        "topic": "orderbook.1.BTCUSDT",
        "type": "delta",
        "ts": 2,
        "data": {"s": "BTCUSDT", "b": [["100.5", "1"]], "u": 2},
    }
    listener.on_ws_frame(transport=None, frame=_frame(snap))
    listener.on_ws_frame(transport=None, frame=_frame(delta))
    tick = prices["BTCUSDT"]["bybit-perp"]
    assert tick.bid == 100.5
    assert tick.ask == 101.0  # untouched by the delta


def test_delta_before_snapshot_ignored():
    listener, prices = _listener()
    delta = {
        "topic": "orderbook.1.BTCUSDT",
        "type": "delta",
        "ts": 1,
        "data": {"s": "BTCUSDT", "b": [["100", "1"]], "a": [["101", "1"]]},
    }
    listener.on_ws_frame(transport=None, frame=_frame(delta))
    assert prices == {}


def test_non_orderbook_topic_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None,
        frame=_frame({"topic": "tickers.BTCUSDT", "data": {"s": "BTCUSDT"}}),
    )
    assert prices == {}


def test_pong_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"op": "pong"}))
    assert prices == {}
