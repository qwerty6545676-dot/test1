"""Parser-level tests for GateioPerpListener (futures.book_ticker)."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp.gateio import GateioPerpListener


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
) -> tuple[GateioPerpListener, PricesBook]:
    prices: PricesBook = {}
    return GateioPerpListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_update_populates_prices_and_normalizes_symbol():
    listener, prices = _listener()
    msg = {
        "time": 1700000000,
        "channel": "futures.book_ticker",
        "event": "update",
        "result": {
            "t": 1700000000123,
            "u": 42,
            "s": "BTC_USDT",
            "b": "65000.10",
            "B": 50,
            "a": "65000.50",
            "A": 40,
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["gateio-perp"]
    assert tick.bid == 65000.10
    assert tick.ask == 65000.50
    assert tick.ts_exchange == 1700000000123


def test_pong_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None, frame=_frame({"channel": "futures.pong"})
    )
    assert prices == {}


def test_subscribe_ack_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None,
        frame=_frame(
            {
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "result": {"status": "success"},
            }
        ),
    )
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener()
    msg = {
        "channel": "futures.book_ticker",
        "event": "update",
        "result": {"t": 1, "s": "DOGE_USDT", "b": "0.1", "a": "0.11"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_wrong_channel_ignored():
    """Spot frame accidentally delivered on the perp connection — ignore."""
    listener, prices = _listener()
    msg = {
        "channel": "spot.book_ticker",
        "event": "update",
        "result": {"t": 1, "s": "BTC_USDT", "b": "1", "a": "2"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}
