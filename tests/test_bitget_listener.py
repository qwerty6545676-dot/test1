"""Parser-level tests for BitgetListener."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.bitget import BitgetListener


class _FakeFrame:
    def __init__(self, payload: bytes, msg_type: int = 1) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


_TEXT = 1
_encoder = msgspec.json.Encoder()


def _listener(symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")) -> tuple[BitgetListener, PricesBook]:
    prices: PricesBook = {}
    return BitgetListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_snapshot_populates_prices():
    listener, prices = _listener()
    msg = {
        "action": "snapshot",
        "arg": {"instType": "SPOT", "channel": "books1", "instId": "BTCUSDT"},
        "data": [{
            "bids": [["65000.1", "0.5"]],
            "asks": [["65000.5", "0.3"]],
            "ts": "1700000000123",
        }],
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["bitget"]
    assert tick.bid == 65000.1
    assert tick.ask == 65000.5
    assert tick.ts_exchange == 1700000000123


def test_update_overwrites_top_level():
    listener, prices = _listener()
    snap = {
        "action": "snapshot",
        "arg": {"instType": "SPOT", "channel": "books1", "instId": "BTCUSDT"},
        "data": [{"bids": [["100", "1"]], "asks": [["101", "1"]], "ts": "1"}],
    }
    upd = {
        "action": "update",
        "arg": {"instType": "SPOT", "channel": "books1", "instId": "BTCUSDT"},
        "data": [{"bids": [["100.5", "1"]], "asks": [["101.5", "1"]], "ts": "2"}],
    }
    listener.on_ws_frame(transport=None, frame=_frame(snap))
    listener.on_ws_frame(transport=None, frame=_frame(upd))
    tick = prices["BTCUSDT"]["bitget"]
    assert tick.bid == 100.5
    assert tick.ask == 101.5


def test_pong_is_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_frame({"op": "pong"}))
    assert prices == {}


def test_subscribe_ack_is_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None,
        frame=_frame({"event": "subscribe", "arg": {"channel": "books1", "instId": "BTCUSDT"}}),
    )
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener()
    msg = {
        "action": "snapshot",
        "arg": {"instType": "SPOT", "channel": "books1", "instId": "DOGEUSDT"},
        "data": [{"bids": [["0.1", "1"]], "asks": [["0.11", "1"]], "ts": "1"}],
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_empty_data_is_safe():
    listener, prices = _listener()
    msg = {
        "action": "snapshot",
        "arg": {"instType": "SPOT", "channel": "books1", "instId": "BTCUSDT"},
        "data": [],
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}
