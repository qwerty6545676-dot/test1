"""Parser-level tests for GateioListener: feed synthetic frames directly."""

from __future__ import annotations

from types import SimpleNamespace

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.spot.gateio import GateioListener


class _FakeFrame:
    """Just enough of picows.WSFrame for the parser tests."""

    def __init__(self, payload: bytes, msg_type: int = 1) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


# picows.WSMsgType.TEXT == 1 in the current binding; mirror it here so we
# don't have to import picows when running the parser-only tests.
_TEXT = 1
_encoder = msgspec.json.Encoder()


def _listener(symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")) -> tuple[GateioListener, PricesBook]:
    prices: PricesBook = {}
    return GateioListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_update_populates_prices():
    listener, prices = _listener()
    msg = {
        "time": 1700000000,
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {
            "t": 1700000000123,
            "u": 42,
            "s": "BTC_USDT",
            "b": "65000.1",
            "a": "65000.5",
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["gateio"]
    assert tick.bid == 65000.1
    assert tick.ask == 65000.5
    assert tick.ts_exchange == 1700000000123
    assert tick.exchange == "gateio"


def test_unknown_symbol_is_ignored():
    listener, prices = _listener()
    msg = {
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {"t": 1, "s": "DOGE_USDT", "b": "0.1", "a": "0.11"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_pong_is_ignored():
    listener, prices = _listener()
    msg = {"time": 1, "channel": "spot.pong", "result": {"status": "success"}}
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_malformed_tick_is_dropped():
    listener, prices = _listener()
    msg = {
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {"t": 1, "s": "BTC_USDT", "b": "not-a-number", "a": "1"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_non_text_frames_ignored():
    listener, prices = _listener()
    frame = _FakeFrame(b"\x00\x01", msg_type=2)  # BINARY
    listener.on_ws_frame(transport=None, frame=frame)
    assert prices == {}
