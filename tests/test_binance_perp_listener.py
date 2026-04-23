"""Parser-level tests for BinancePerpListener (fstream bookTicker)."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp.binance import BinancePerpListener


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
) -> tuple[BinancePerpListener, PricesBook]:
    prices: PricesBook = {}
    return BinancePerpListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_combined_stream_frame_populates_prices():
    listener, prices = _listener()
    msg = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "e": "bookTicker",
            "u": 1,
            "s": "BTCUSDT",
            "b": "65000.10",
            "B": "0.5",
            "a": "65001.00",
            "A": "0.3",
            "T": 1700000000123,
            "E": 1700000000125,
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["binance-perp"]
    assert tick.bid == 65000.10
    assert tick.ask == 65001.00
    assert tick.ts_exchange == 1700000000123
    assert tick.exchange == "binance-perp"


def test_unknown_symbol_ignored():
    listener, prices = _listener(symbols=("BTCUSDT",))
    msg = {
        "data": {
            "s": "DOGEUSDT",
            "b": "0.1",
            "a": "0.11",
            "T": 1,
            "E": 1,
        }
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_missing_data_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"stream": "btcusdt@bookTicker"}))
    assert prices == {}


def test_malformed_price_dropped():
    listener, prices = _listener()
    msg = {"data": {"s": "BTCUSDT", "b": "nope", "a": "1", "T": 1}}
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_non_text_frame_ignored():
    listener, prices = _listener()
    binary_frame = _FakeFrame(b"\x00\x01", msg_type=2)
    listener.on_ws_frame(transport=None, frame=binary_frame)
    assert prices == {}
