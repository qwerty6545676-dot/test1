"""Perp paper trader: open long+short legs, wait for spread to close.

On each ``ArbSignal(market="perp")`` we open a paired position:

* LONG ``notional_per_leg`` USD on the cheap exchange (``buy_ex``)
* SHORT ``notional_per_leg`` USD on the expensive exchange
  (``sell_ex``)

A background task polls the shared ``prices_perp`` book every
``poll_interval_s`` seconds and checks each open position:

* If the current spread (``sell_bid_here - buy_ask_there``) has
  collapsed below ``close_threshold_pct`` — we close at current
  mids.
* If the position has been held longer than
  ``max_hold_seconds`` — we force-close at current mids with
  ``reason="expired"``. This keeps memory bounded when a spread
  never reverts (wide-moat listings, one-off halts, etc.).

Same-key dedup: we don't open two positions on the same
``(symbol, buy_ex, sell_ex)`` tuple. The cooldown tracker already
drops re-emissions of the same key, but we also guard here so
manual ``bus.emit_arb`` calls during tests don't accidentally
double up.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Callable

from ..comparator import PricesBook
from ..settings import Fees
from ..signals import ArbSignal, InfoEvent, SignalBus
from .models import ClosedPaperTrade, OpenPaperTrade
from .writer import PaperTradesWriter

logger = logging.getLogger("arbitrage.paper.perp")


class PerpPaperTrader:
    """Open-on-signal, close-on-convergence simulator for perp arb."""

    __slots__ = (
        "_bus",
        "_fees",
        "_prices",
        "_open_writer",
        "_closed_writer",
        "_notional",
        "_close_threshold_pct",
        "_max_hold_s",
        "_poll_interval_s",
        "_now",
        "_open_trades",
        "_registered",
    )

    def __init__(
        self,
        bus: SignalBus,
        fees: Fees,
        prices_perp: PricesBook,
        *,
        open_writer: PaperTradesWriter,
        closed_writer: PaperTradesWriter,
        notional_per_leg_usd: float,
        close_threshold_pct: float,
        max_hold_seconds: int,
        poll_interval_s: float = 1.0,
        now: Callable[[], float] | None = None,
    ) -> None:
        self._bus = bus
        self._fees = fees
        self._prices = prices_perp
        self._open_writer = open_writer
        self._closed_writer = closed_writer
        self._notional = notional_per_leg_usd
        self._close_threshold_pct = close_threshold_pct
        self._max_hold_s = max_hold_seconds
        self._poll_interval_s = poll_interval_s
        self._now = now or time.time
        # Keyed by (symbol, buy_ex, sell_ex) to dedup.
        self._open_trades: dict[tuple[str, str, str], OpenPaperTrade] = {}
        self._registered = False

    # -- wiring --------------------------------------------------------

    def attach(self) -> None:
        if self._registered:
            return
        self._bus.register_arb(self._on_arb)
        self._registered = True

    async def run(self) -> None:
        """Background task that polls for convergence / expiration.

        Swallow CancelledError: treat cancellation as clean shutdown
        (main.py cancels all tasks on SIGINT).
        """
        try:
            while True:
                try:
                    self.poll_once()
                except Exception:
                    logger.exception("perp paper: poll loop error")
                await asyncio.sleep(self._poll_interval_s)
        except asyncio.CancelledError:
            return

    # -- signal handler ------------------------------------------------

    def _on_arb(self, signal: ArbSignal) -> None:
        if signal.market != "perp":
            return
        key = (signal.symbol, signal.buy_ex, signal.sell_ex)
        if key in self._open_trades:
            return  # already tracking this leg
        if signal.buy_ask <= 0 or signal.sell_bid <= 0:
            return

        trade = OpenPaperTrade(
            id=uuid.uuid4().hex,
            market="perp",
            symbol=signal.symbol,
            buy_ex=signal.buy_ex,
            sell_ex=signal.sell_ex,
            entry_ts_ms=signal.ts_ms,
            entry_buy=signal.buy_ask,
            entry_sell=signal.sell_bid,
            entry_spread_pct=signal.net_pct,
            notional_per_leg=self._notional,
        )
        self._open_trades[key] = trade
        self._open_writer.write(trade)
        logger.info(
            "perp paper: opened %s long=%s@%.6f short=%s@%.6f spread=%.3f%%",
            signal.symbol, signal.buy_ex, signal.buy_ask,
            signal.sell_ex, signal.sell_bid, signal.net_pct,
        )

    # -- polling -------------------------------------------------------

    def poll_once(self) -> None:
        """Walk open trades, close any that converged / expired."""
        if not self._open_trades:
            return

        now = self._now()
        # Materialize the list of keys first — _close() mutates the dict.
        for key in list(self._open_trades):
            trade = self._open_trades[key]
            hold_s = now - trade.entry_ts_ms / 1000.0

            current = self._current_quote(trade)
            if current is None:
                # One or both legs have no fresh quote — keep waiting.
                if hold_s >= self._max_hold_s:
                    # Close with entry prices as a best-effort if we
                    # truly can't get a quote. Better than leaking the
                    # position for ever.
                    self._close(
                        trade,
                        exit_buy=trade.entry_buy,
                        exit_sell=trade.entry_sell,
                        reason="expired",
                    )
                continue

            exit_buy, exit_sell = current
            exit_spread_pct = self._spread_pct(exit_buy, exit_sell)

            if exit_spread_pct <= self._close_threshold_pct:
                self._close(trade, exit_buy=exit_buy, exit_sell=exit_sell, reason="converged")
            elif hold_s >= self._max_hold_s:
                self._close(trade, exit_buy=exit_buy, exit_sell=exit_sell, reason="expired")

    def _current_quote(
        self, trade: OpenPaperTrade
    ) -> tuple[float, float] | None:
        """Return current (buy-leg-ask, sell-leg-bid). None if either missing."""
        book = self._prices.get(trade.symbol)
        if not book:
            return None
        buy_tick = book.get(trade.buy_ex)
        sell_tick = book.get(trade.sell_ex)
        if buy_tick is None or sell_tick is None:
            return None
        return buy_tick.ask, sell_tick.bid

    @staticmethod
    def _spread_pct(buy_ask: float, sell_bid: float) -> float:
        if buy_ask <= 0:
            return 0.0
        return (sell_bid - buy_ask) / buy_ask * 100.0

    # -- closing -------------------------------------------------------

    def _close(
        self,
        trade: OpenPaperTrade,
        *,
        exit_buy: float,
        exit_sell: float,
        reason: str,
    ) -> None:
        # Long leg: bought at entry_buy, "selling" now at exit_buy (mid).
        qty_long = self._notional / trade.entry_buy
        long_pnl = qty_long * (exit_buy - trade.entry_buy)

        # Short leg: sold at entry_sell, "buying back" at exit_sell.
        qty_short = self._notional / trade.entry_sell
        short_pnl = qty_short * (trade.entry_sell - exit_sell)

        gross_pnl = long_pnl + short_pnl

        fee_buy = self._fees.perp.get(trade.buy_ex, 0.0)
        fee_sell = self._fees.perp.get(trade.sell_ex, 0.0)
        # Entry + exit taker fees on both legs, valued at notional.
        fees_usd = (
            self._notional * fee_buy * 2
            + self._notional * fee_sell * 2
        )
        net_pnl = gross_pnl - fees_usd

        now_ms = int(self._now() * 1000)
        hold_seconds = (now_ms - trade.entry_ts_ms) // 1000
        exit_spread_pct = self._spread_pct(exit_buy, exit_sell)

        closed = ClosedPaperTrade(
            id=trade.id,
            market="perp",
            symbol=trade.symbol,
            buy_ex=trade.buy_ex,
            sell_ex=trade.sell_ex,
            entry_ts_ms=trade.entry_ts_ms,
            exit_ts_ms=now_ms,
            entry_buy=trade.entry_buy,
            entry_sell=trade.entry_sell,
            exit_buy=exit_buy,
            exit_sell=exit_sell,
            entry_spread_pct=trade.entry_spread_pct,
            exit_spread_pct=round(exit_spread_pct, 4),
            notional_per_leg=self._notional,
            gross_pnl_usd=round(gross_pnl, 4),
            fee_usd=round(fees_usd, 4),
            net_pnl_usd=round(net_pnl, 4),
            hold_seconds=int(hold_seconds),
            reason=reason,  # type: ignore[arg-type]
        )
        self._closed_writer.write(closed)
        self._open_trades.pop((trade.symbol, trade.buy_ex, trade.sell_ex), None)

        self._bus.emit_info(
            InfoEvent(
                ts_ms=now_ms,
                kind="notice",
                message=(
                    f"PERP CLOSED {trade.symbol} ({reason}): "
                    f"entry {trade.entry_spread_pct:.3f}% -> exit {exit_spread_pct:.3f}%, "
                    f"net ${closed.net_pnl_usd:+.2f}, held {int(hold_seconds)}s"
                ),
                market="perp",
                severity="info",
            )
        )


__all__ = ["PerpPaperTrader"]
