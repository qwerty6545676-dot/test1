"""Parser-level tests for MexcListener — serialize fixtures via Protobuf."""

from __future__ import annotations

from arbitrage.comparator import PricesBook
from arbitrage.exchanges.mexc import MexcListener
from arbitrage.mexc_proto import PushDataV3ApiWrapper_pb2


class _FakeFrame:
    def __init__(self, payload: bytes, msg_type: int = 2) -> None:
        self._payload = payload
        self.msg_type = msg_type

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


def _listener(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> tuple[MexcListener, PricesBook]:
    prices: PricesBook = {}
    return MexcListener(prices, symbols), prices


def _wrap_tick(symbol: str, bid: str, ask: str, send_time: int) -> bytes:
    w = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
    w.channel = f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{symbol}"
    w.symbol = symbol
    w.sendTime = send_time
    w.publicAggreBookTicker.bidPrice = bid
    w.publicAggreBookTicker.askPrice = ask
    w.publicAggreBookTicker.bidQuantity = "0.001"
    w.publicAggreBookTicker.askQuantity = "0.001"
    return w.SerializeToString()


def test_protobuf_tick_populates_prices():
    listener, prices = _listener()
    raw = _wrap_tick("BTCUSDT", "78132.00", "78132.01", 1700000000123)
    listener.on_ws_frame(transport=None, frame=_FakeFrame(raw))
    tick = prices["BTCUSDT"]["mexc"]
    assert tick.bid == 78132.0
    assert tick.ask == 78132.01
    assert tick.ts_exchange == 1700000000123


def test_json_ack_ignored_without_parse_attempt():
    listener, prices = _listener()
    # ack frame starts with '{' — must not attempt protobuf parsing
    raw = b'{"id":0,"code":0,"msg":"Subscribed successful!"}'
    listener.on_ws_frame(transport=None, frame=_FakeFrame(raw))
    assert prices == {}


def test_malformed_binary_dropped():
    listener, prices = _listener()
    # random bytes that aren't valid protobuf nor start with '{'
    listener.on_ws_frame(transport=None, frame=_FakeFrame(b"\xff\xff\xff\xff\x00"))
    assert prices == {}


def test_unknown_symbol_ignored():
    listener, prices = _listener(symbols=("BTCUSDT",))
    raw = _wrap_tick("DOGEUSDT", "0.1", "0.11", 1)
    listener.on_ws_frame(transport=None, frame=_FakeFrame(raw))
    assert prices == {}


def test_empty_frame_ignored():
    listener, prices = _listener()
    listener.on_ws_frame(transport=None, frame=_FakeFrame(b""))
    assert prices == {}


def test_wrong_body_oneof_ignored():
    """A wrapper with a different body (e.g. PublicDeals) must not crash."""
    listener, prices = _listener()
    w = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
    w.channel = "spot@public.deals.v3.api.pb@BTCUSDT"
    w.symbol = "BTCUSDT"
    # populate publicDeals (a different oneof body) so WhichOneof != publicAggreBookTicker
    deal = w.publicDeals.deals.add()
    deal.price = "100"
    deal.quantity = "1"
    deal.tradeType = 1
    deal.time = 1
    listener.on_ws_frame(transport=None, frame=_FakeFrame(w.SerializeToString()))
    assert prices == {}
