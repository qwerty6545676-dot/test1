"""SpotPaperTrader: instant-close cross-exchange arb simulator."""

from __future__ import annotations

import json

from arbitrage.paper.spot import SpotPaperTrader
from arbitrage.paper.writer import PaperTradesWriter
from arbitrage.settings import Fees
from arbitrage.signals import ArbSignal, InfoEvent, SignalBus


def _fees(**spot_fees: float) -> Fees:
    return Fees(spot=spot_fees or {"a": 0.001, "b": 0.001}, perp={})


def _sig(**kw) -> ArbSignal:
    base = dict(
        ts_ms=1_700_000_000_000,
        market="spot",
        symbol="BTCUSDT",
        buy_ex="a",
        sell_ex="b",
        buy_ask=100.0,
        sell_bid=105.0,
        net_pct=5.0,
    )
    base.update(kw)
    return ArbSignal(**base)


def _make(tmp_path, notional=50.0, slippage=0.0):
    bus = SignalBus()
    writer = PaperTradesWriter(tmp_path / "paper_closed.jsonl")
    writer.open()
    trader = SpotPaperTrader(
        bus,
        _fees(),
        closed_writer=writer,
        notional_per_leg_usd=notional,
        slippage_pct=slippage,
    )
    trader.attach()
    return bus, writer, tmp_path / "paper_closed.jsonl"


def test_emits_one_closed_trade_per_signal(tmp_path):
    bus, writer, path = _make(tmp_path)
    bus.emit_arb(_sig())
    writer.close()
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["market"] == "spot"
    assert data["symbol"] == "BTCUSDT"
    assert data["reason"] == "instant"
    assert data["hold_seconds"] == 0


def test_positive_pnl_on_wide_spread(tmp_path):
    bus, writer, path = _make(tmp_path, notional=100.0, slippage=0.0)
    # 5% raw spread, 0.1% + 0.1% fees -> net should be clearly positive.
    bus.emit_arb(_sig(buy_ask=100.0, sell_bid=105.0, net_pct=5.0))
    writer.close()
    data = json.loads(path.read_text().splitlines()[0])
    # qty = 1.0, gross = 1 * (105 - 100) = 5.0
    # fees = 100*0.001 + 1*105*0.001 = 0.1 + 0.105 = 0.205
    # slippage = 0 (slippage_pct=0)
    assert data["gross_pnl_usd"] == 5.0
    assert abs(data["fee_usd"] - 0.205) < 1e-9
    assert abs(data["net_pnl_usd"] - (5.0 - 0.205)) < 1e-9


def test_negative_pnl_when_fees_eat_spread(tmp_path):
    """Tiny spread -> fees + slippage make it unprofitable."""
    bus, writer, path = _make(tmp_path, notional=100.0, slippage=0.5)
    bus.emit_arb(_sig(buy_ask=100.0, sell_bid=100.05, net_pct=0.05))
    writer.close()
    data = json.loads(path.read_text().splitlines()[0])
    assert data["net_pnl_usd"] < 0


def test_emits_info_event(tmp_path):
    bus, writer, path = _make(tmp_path)
    received: list[InfoEvent] = []
    bus.register_info(received.append)
    bus.emit_arb(_sig())
    writer.close()
    notices = [e for e in received if e.kind == "notice"]
    assert len(notices) == 1
    assert "SPOT ARB" in notices[0].message
    assert notices[0].market == "spot"


def test_ignores_perp_signals(tmp_path):
    bus, writer, path = _make(tmp_path)
    bus.emit_arb(_sig(market="perp"))
    writer.close()
    assert path.read_text() == ""


def test_ignores_malformed_prices(tmp_path, caplog):
    bus, writer, path = _make(tmp_path)
    bus.emit_arb(_sig(buy_ask=0.0))
    bus.emit_arb(_sig(sell_bid=-1.0))
    writer.close()
    assert path.read_text() == ""


def test_attach_is_idempotent(tmp_path):
    """Calling attach() twice on one trader must only register one handler."""
    bus = SignalBus()
    writer = PaperTradesWriter(tmp_path / "paper_closed.jsonl")
    writer.open()
    trader = SpotPaperTrader(
        bus,
        _fees(),
        closed_writer=writer,
        notional_per_leg_usd=50,
        slippage_pct=0,
    )
    trader.attach()
    trader.attach()  # second call should be a no-op
    bus.emit_arb(_sig())
    writer.close()
    assert len((tmp_path / "paper_closed.jsonl").read_text().splitlines()) == 1


def test_missing_fee_defaults_to_zero(tmp_path):
    bus = SignalBus()
    writer = PaperTradesWriter(tmp_path / "paper_closed.jsonl")
    writer.open()
    fees = Fees(spot={}, perp={})
    trader = SpotPaperTrader(
        bus, fees, closed_writer=writer,
        notional_per_leg_usd=100, slippage_pct=0,
    )
    trader.attach()
    bus.emit_arb(_sig(buy_ask=100.0, sell_bid=105.0))
    writer.close()
    data = json.loads((tmp_path / "paper_closed.jsonl").read_text().splitlines()[0])
    # Zero fees, zero slippage -> net_pnl == gross
    assert data["net_pnl_usd"] == data["gross_pnl_usd"] == 5.0
