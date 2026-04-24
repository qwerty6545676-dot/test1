"""PerpPaperTrader: open on signal, close on convergence or expiry."""

from __future__ import annotations

import json

from arbitrage.normalizer import Tick
from arbitrage.paper.perp import PerpPaperTrader
from arbitrage.paper.writer import PaperTradesWriter
from arbitrage.settings import Fees
from arbitrage.signals import ArbSignal, InfoEvent, SignalBus


class _Clock:
    def __init__(self, t: float = 1_700_000_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _fees() -> Fees:
    return Fees(
        spot={},
        perp={"a": 0.0004, "b": 0.0004},
    )


def _sig(ts_ms: int = 1_700_000_000_000, **kw) -> ArbSignal:
    base = dict(
        ts_ms=ts_ms,
        market="perp",
        symbol="BTCUSDT",
        buy_ex="a",
        sell_ex="b",
        buy_ask=100.0,
        sell_bid=103.0,
        net_pct=3.0,
    )
    base.update(kw)
    return ArbSignal(**base)


def _tick(exchange: str, bid: float, ask: float, ts_ms: int = 1_700_000_000_000) -> Tick:
    return Tick(
        exchange=exchange,
        symbol="BTCUSDT",
        bid=bid,
        ask=ask,
        ts_exchange=ts_ms,
        ts_local=ts_ms,
    )


def _make(tmp_path, *, close_threshold=0.5, max_hold=86400):
    bus = SignalBus()
    prices = {}
    open_w = PaperTradesWriter(tmp_path / "paper_open.jsonl")
    open_w.open()
    closed_w = PaperTradesWriter(tmp_path / "paper_closed.jsonl")
    closed_w.open()
    clock = _Clock()
    trader = PerpPaperTrader(
        bus, _fees(), prices,
        open_writer=open_w, closed_writer=closed_w,
        notional_per_leg_usd=100.0,
        close_threshold_pct=close_threshold,
        max_hold_seconds=max_hold,
        poll_interval_s=0.01,
        now=clock,
    )
    trader.attach()
    return bus, prices, trader, clock, open_w, closed_w, tmp_path


def test_open_on_signal_writes_open_file(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(tmp_path)
    bus.emit_arb(_sig())
    open_w.close(); closed_w.close()
    data = json.loads((root / "paper_open.jsonl").read_text().splitlines()[0])
    assert data["symbol"] == "BTCUSDT"
    assert data["buy_ex"] == "a"
    assert data["entry_spread_pct"] == 3.0
    # Nothing closed yet.
    assert (root / "paper_closed.jsonl").read_text() == ""


def test_dedup_same_signal(tmp_path):
    bus, prices, trader, *_ = _make(tmp_path)
    bus.emit_arb(_sig())
    bus.emit_arb(_sig())   # identical — should be ignored
    bus.emit_arb(_sig(symbol="ETHUSDT"))  # different symbol -> another open
    assert len(trader._open_trades) == 2


def test_close_on_convergence(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(tmp_path, close_threshold=0.5)
    bus.emit_arb(_sig(buy_ask=100.0, sell_bid=103.0))   # 3%
    # Spread shrinks to < 0.5% — should close.
    prices["BTCUSDT"] = {
        "a": _tick("a", bid=101.9, ask=102.0),
        "b": _tick("b", bid=102.1, ask=102.2),
    }
    clock.t += 30  # held 30 seconds
    trader.poll_once()
    open_w.close(); closed_w.close()
    closed = (root / "paper_closed.jsonl").read_text().splitlines()
    assert len(closed) == 1
    data = json.loads(closed[0])
    assert data["reason"] == "converged"
    assert data["hold_seconds"] == 30
    # exit_spread_pct computed from poll quotes.
    assert data["exit_spread_pct"] < 0.5


def test_no_close_if_spread_still_wide(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(tmp_path, close_threshold=0.5)
    bus.emit_arb(_sig())
    prices["BTCUSDT"] = {
        "a": _tick("a", bid=99.0, ask=100.0),
        "b": _tick("b", bid=103.0, ask=104.0),   # still 3% spread
    }
    trader.poll_once()
    assert len(trader._open_trades) == 1
    assert (tmp_path / "paper_closed.jsonl").read_text() == ""


def test_force_close_on_expiry(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(
        tmp_path, close_threshold=0.5, max_hold=60
    )
    bus.emit_arb(_sig())
    # Still wide, but hold timed out.
    prices["BTCUSDT"] = {
        "a": _tick("a", bid=99.0, ask=100.0),
        "b": _tick("b", bid=103.0, ask=104.0),
    }
    clock.t += 61
    trader.poll_once()
    open_w.close(); closed_w.close()
    closed = (root / "paper_closed.jsonl").read_text().splitlines()
    assert len(closed) == 1
    data = json.loads(closed[0])
    assert data["reason"] == "expired"


def test_expiry_without_quote_uses_entry_prices(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(
        tmp_path, close_threshold=0.5, max_hold=60
    )
    bus.emit_arb(_sig())
    # No new quotes in prices_perp — simulate silent exchanges.
    clock.t += 61
    trader.poll_once()
    open_w.close(); closed_w.close()
    closed = (root / "paper_closed.jsonl").read_text().splitlines()
    assert len(closed) == 1
    data = json.loads(closed[0])
    assert data["reason"] == "expired"
    # No quote available, so exit_* equals entry_*.
    assert data["exit_buy"] == 100.0
    assert data["exit_sell"] == 103.0


def test_closed_pnl_sign(tmp_path):
    """When spread collapses ~symmetrically, PnL should be positive."""
    bus, prices, trader, clock, open_w, closed_w, root = _make(tmp_path, close_threshold=0.5)
    # Open at 100 / 103 (3% spread).
    bus.emit_arb(_sig(buy_ask=100.0, sell_bid=103.0))
    # Both converge to ~101.5.
    prices["BTCUSDT"] = {
        "a": _tick("a", bid=101.5, ask=101.5),
        "b": _tick("b", bid=101.5, ask=101.5),
    }
    trader.poll_once()
    open_w.close(); closed_w.close()
    data = json.loads((root / "paper_closed.jsonl").read_text().splitlines()[0])
    # long leg: bought @100, now @101.5 -> +1.5% * $100 = +$1.5
    # short leg: sold @103, now @101.5 -> +1.5% * $100 = +$1.45... approx
    # fees: 4 * $100 * 0.0004 = $0.16
    assert data["gross_pnl_usd"] > 0
    assert data["net_pnl_usd"] > 0


def test_emits_close_info_event(tmp_path):
    bus, prices, trader, clock, open_w, closed_w, root = _make(tmp_path, close_threshold=0.5)
    received: list[InfoEvent] = []
    bus.register_info(received.append)
    bus.emit_arb(_sig())
    prices["BTCUSDT"] = {
        "a": _tick("a", bid=101.0, ask=101.0),
        "b": _tick("b", bid=101.0, ask=101.0),
    }
    trader.poll_once()
    notices = [e for e in received if e.kind == "notice" and "PERP CLOSED" in e.message]
    assert len(notices) == 1
    assert notices[0].market == "perp"


def test_ignores_spot_signals(tmp_path):
    bus, prices, trader, *_ = _make(tmp_path)
    bus.emit_arb(_sig(market="spot"))
    assert len(trader._open_trades) == 0


def test_poll_noop_when_no_open_trades(tmp_path):
    bus, prices, trader, *_ = _make(tmp_path)
    # Should not raise.
    trader.poll_once()
