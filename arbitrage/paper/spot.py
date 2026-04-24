"""Spot paper trader: instant cross-exchange execution model.

Spot arb is *spatial*, not cash-and-carry — you buy on A, sell on
B at the same instant. There's no spread-convergence to wait for,
so we don't keep any open-trade state. One signal → one closed
trade.

PnL model
---------
Given a signal ``(buy_ex @ buy_ask, sell_ex @ sell_bid)`` and a
notional-per-leg of N USD::

    qty                = N / buy_ask              # base units of the asset
    gross_pnl          = qty * (sell_bid - buy_ask)
    slippage_loss      = (buy_ask + sell_bid) * qty * slippage_pct / 100
    entry_fees         = N * fee_buy
    exit_fees          = (qty * sell_bid) * fee_sell
    net_pnl            = gross_pnl - entry_fees - exit_fees - slippage_loss
"""

from __future__ import annotations

import logging
import time
import uuid

from ..settings import Fees
from ..signals import ArbSignal, InfoEvent, SignalBus
from .models import ClosedPaperTrade
from .writer import PaperTradesWriter

logger = logging.getLogger("arbitrage.paper.spot")


class SpotPaperTrader:
    """Subscribes to spot ArbSignals and records instant-close trades."""

    __slots__ = (
        "_bus",
        "_fees",
        "_closed_writer",
        "_notional",
        "_slippage_pct",
        "_registered",
    )

    def __init__(
        self,
        bus: SignalBus,
        fees: Fees,
        *,
        closed_writer: PaperTradesWriter,
        notional_per_leg_usd: float,
        slippage_pct: float,
    ) -> None:
        self._bus = bus
        self._fees = fees
        self._closed_writer = closed_writer
        self._notional = notional_per_leg_usd
        self._slippage_pct = slippage_pct
        self._registered = False

    def attach(self) -> None:
        if self._registered:
            return
        self._bus.register_arb(self._on_arb)
        self._registered = True

    def _on_arb(self, signal: ArbSignal) -> None:
        if signal.market != "spot":
            return
        if signal.buy_ask <= 0 or signal.sell_bid <= 0:
            logger.warning("spot paper: skipping bad signal %r", signal)
            return

        fee_buy = self._fees.spot.get(signal.buy_ex, 0.0)
        fee_sell = self._fees.spot.get(signal.sell_ex, 0.0)

        qty = self._notional / signal.buy_ask
        gross_pnl = qty * (signal.sell_bid - signal.buy_ask)
        slip = (signal.buy_ask + signal.sell_bid) * qty * (self._slippage_pct / 100.0)
        fees_usd = self._notional * fee_buy + (qty * signal.sell_bid) * fee_sell
        net_pnl = gross_pnl - fees_usd - slip

        now_ms = int(time.time() * 1000)
        trade = ClosedPaperTrade(
            id=uuid.uuid4().hex,
            market="spot",
            symbol=signal.symbol,
            buy_ex=signal.buy_ex,
            sell_ex=signal.sell_ex,
            entry_ts_ms=signal.ts_ms,
            exit_ts_ms=now_ms,
            entry_buy=signal.buy_ask,
            entry_sell=signal.sell_bid,
            exit_buy=signal.buy_ask,
            exit_sell=signal.sell_bid,
            entry_spread_pct=signal.net_pct,
            exit_spread_pct=signal.net_pct,
            notional_per_leg=self._notional,
            gross_pnl_usd=round(gross_pnl, 4),
            fee_usd=round(fees_usd + slip, 4),
            net_pnl_usd=round(net_pnl, 4),
            hold_seconds=0,
            reason="instant",
        )
        self._closed_writer.write(trade)
        self._bus.emit_info(
            InfoEvent(
                ts_ms=now_ms,
                kind="notice",
                message=(
                    f"SPOT ARB {signal.symbol}: "
                    f"buy {signal.buy_ex} @ {signal.buy_ask:g} -> "
                    f"sell {signal.sell_ex} @ {signal.sell_bid:g}, "
                    f"net ${trade.net_pnl_usd:+.2f}"
                ),
                market="spot",
                severity="info",
            )
        )


__all__ = ["SpotPaperTrader"]
