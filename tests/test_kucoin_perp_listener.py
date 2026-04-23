"""Parser-level tests for KuCoinPerpListener + the XBT/M symbol helper."""

from __future__ import annotations

import msgspec
import pytest

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.perp._symbol import from_kucoin_perp, to_kucoin_perp
from arbitrage.exchanges.perp.kucoin import KuCoinPerpListener


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
) -> tuple[KuCoinPerpListener, PricesBook]:
    prices: PricesBook = {}
    return KuCoinPerpListener(prices, symbols, ping_interval_ms=18000), prices


def _frame(obj) -> _FakeFrame:
    return _FakeFrame(_encoder.encode(obj), _TEXT)


# ---- symbol helper ---------------------------------------------------


@pytest.mark.parametrize(
    "canonical,native",
    [
        ("BTCUSDT", "XBTUSDTM"),
        ("ETHUSDT", "ETHUSDTM"),
        ("SOLUSDT", "SOLUSDTM"),
        ("DOGEUSDT", "DOGEUSDTM"),
    ],
)
def test_to_kucoin_perp_maps_btc_to_xbt_and_appends_m(canonical: str, native: str):
    assert to_kucoin_perp(canonical) == native


@pytest.mark.parametrize(
    "native,canonical",
    [
        ("XBTUSDTM", "BTCUSDT"),
        ("ETHUSDTM", "ETHUSDT"),
        ("SOLUSDTM", "SOLUSDT"),
    ],
)
def test_from_kucoin_perp_reverses_mapping(native: str, canonical: str):
    assert from_kucoin_perp(native) == canonical


def test_from_kucoin_perp_unknown_shape_passes_through():
    # No trailing M — treat as an unknown wire symbol, do not mangle.
    assert from_kucoin_perp("BTCUSDT") == "BTCUSDT"


# ---- listener --------------------------------------------------------


def test_ticker_v2_populates_prices_and_maps_xbt_back():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/contractMarket/tickerV2:XBTUSDTM",
        "subject": "tickerV2",
        "data": {
            "symbol": "XBTUSDTM",
            "bestBidPrice": "77358.1",
            "bestBidSize": 68,
            "bestAskPrice": "77358.2",
            "bestAskSize": 2286,
            # KuCoin futures ts is NANOseconds — listener must divide.
            "ts": 1776941410555000000,
            "sequence": 1,
        },
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    tick = prices["BTCUSDT"]["kucoin-perp"]
    assert tick.bid == 77358.1
    assert tick.ask == 77358.2
    # Nanoseconds -> milliseconds.
    assert tick.ts_exchange == 1776941410555


def test_welcome_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None, frame=_frame({"type": "welcome", "id": "x"})
    )
    assert prices == {}


def test_pong_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(
        transport=None, frame=_frame({"type": "pong", "id": "x"})
    )
    assert prices == {}


def test_unknown_topic_ignored():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/contractMarket/level2:XBTUSDTM",
        "data": {"symbol": "XBTUSDTM"},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener(symbols=("BTCUSDT",))
    msg = {
        "type": "message",
        "topic": "/contractMarket/tickerV2:DOGEUSDTM",
        "data": {"symbol": "DOGEUSDTM", "bestBidPrice": "0.1", "bestAskPrice": "0.11", "ts": 0},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}


def test_malformed_price_dropped():
    listener, prices = _listener()
    msg = {
        "type": "message",
        "topic": "/contractMarket/tickerV2:XBTUSDTM",
        "data": {"symbol": "XBTUSDTM", "bestBidPrice": "nope", "bestAskPrice": "1", "ts": 0},
    }
    listener.on_ws_frame(transport=None, frame=_frame(msg))
    assert prices == {}
