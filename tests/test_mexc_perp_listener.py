"""Parser-level tests for MexcPerpListener (plain JSON, contract.mexc.com)."""

from __future__ import annotations

import msgspec

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp.mexc import MexcPerpListener


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
) -> tuple[MexcPerpListener, PricesBook]:
    prices: PricesBook = {}
    return MexcPerpListener(prices, symbols), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


def test_ticker_update_populates_prices():
    listener, prices = _listener()
    msg = {
        "symbol": "BTC_USDT",
        "data": {
            "bid1": 77305.0,
            "ask1": 77305.1,
            "lastPrice": 77305.05,
            "timestamp": 1776941413753,
        },
        "channel": "push.ticker",
        "ts": 1776941413753,
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["mexc-perp"]
    assert tick.bid == 77305.0
    assert tick.ask == 77305.1
    assert tick.ts_exchange == 1776941413753


def test_sub_ack_ignored():
    listener, prices = _listener()
    # Ack frame has no outer `symbol`, so it should be dropped silently.
    msg = {"channel": "rs.sub.ticker", "data": "success", "ts": 1776941413000}
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_pong_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None, frame=_frame({"channel": "pong", "data": "pong", "ts": 1})
    )
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener(symbols=("BTCUSDT",))
    msg = {
        "symbol": "DOGE_USDT",
        "data": {"bid1": 0.1, "ask1": 0.11, "timestamp": 1},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_malformed_price_dropped():
    listener, prices = _listener()
    msg = {
        "symbol": "BTC_USDT",
        "data": {"bid1": "not-a-number", "ask1": 1, "timestamp": 1},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_missing_data_ignored():
    listener, prices = _listener()
    msg = {"symbol": "BTC_USDT"}
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}
