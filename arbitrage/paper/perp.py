"""Perp paper trader: open long+short legs, wait for spread to close.

On each ``ArbSignal(market="perp")`` we open a paired position:

* LONG ``notional_per_leg`` USD on the cheap exchange (``buy_ex``)
* SHORT ``notional_per_leg`` USD on the expensive exchange
  (``sell_ex``)

A background task polls the shared ``prices_perp`` book every
``poll_interval_s`` seconds and checks each open position:

* If the current spread (``sell_bid_here - buy_ask_there``) has
  collapsed below ``close_threshold_pct`` — we close.
* If the position has been held longer than
  ``max_hold_seconds`` — we force-close with ``reason="expired"``.
  This keeps memory bounded when a spread never reverts (wide-moat
  listings, one-off halts, etc.).

Exit-PnL realism
----------------
Convergence is checked against the *new arb spread* (``sell.bid -
buy.ask``), but actual close fills happen on the *opposite* side of
each book:

* Closing the LONG leg is a SELL → we get filled at the buy-venue's
  ``bid``.
* Closing the SHORT leg is a BUY → we pay the sell-venue's ``ask``.

Using the favorable side on both legs would silently inflate paper
PnL by roughly half a spread per leg per close, which is exactly the
class of error paper-trading is supposed to surface, not hide. We
also apply a ``slippage_pct`` haircut on each exit leg to model
imperfect fill price on top of the bid/ask haircut.

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
from ..exchanges._common import base_exchange
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
        "_slippage_pct",
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
        slippage_pct: float = 0.0,
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
        self._slippage_pct = slippage_pct
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

            quote = self._current_quote(trade)
            if quote is None:
                # One or both legs have no fresh quote — keep waiting.
                if hold_s >= self._max_hold_s:
                    # Best-effort fallback: use entry prices when no
                    # current quote is available. Better than leaking
                    # the position for ever.
                    self._close(
                        trade,
                        exit_long_fill=trade.entry_buy,
                        exit_short_fill=trade.entry_sell,
                        exit_spread_pct=trade.entry_spread_pct,
                        reason="expired",
                    )
                continue

            buy_bid, buy_ask, sell_bid, sell_ask = quote
            # Convergence check uses the *new arb* spread so we close
            # only when a fresh entry would no longer beat threshold.
            exit_spread_pct = self._spread_pct(buy_ask, sell_bid)

            # Realistic exit fills: sell long at buy-venue bid, buy back
            # short at sell-venue ask.
            exit_long_fill = buy_bid
            exit_short_fill = sell_ask

            if exit_spread_pct <= self._close_threshold_pct:
                self._close(
                    trade,
                    exit_long_fill=exit_long_fill,
                    exit_short_fill=exit_short_fill,
                    exit_spread_pct=exit_spread_pct,
                    reason="converged",
                )
            elif hold_s >= self._max_hold_s:
                self._close(
                    trade,
                    exit_long_fill=exit_long_fill,
                    exit_short_fill=exit_short_fill,
                    exit_spread_pct=exit_spread_pct,
                    reason="expired",
                )

    def _current_quote(
        self, trade: OpenPaperTrade
    ) -> tuple[float, float, float, float] | None:
        """Return ``(buy_bid, buy_ask, sell_bid, sell_ask)`` or None.

        We need both sides of both books: the *ask* side for the
        convergence check ("would a fresh arb still pay?"), and the
        *bid* side of the buy-venue plus the *ask* side of the
        sell-venue for realistic exit fills.
        """
        book = self._prices.get(trade.symbol)
        if not book:
            return None
        buy_tick = book.get(trade.buy_ex)
        sell_tick = book.get(trade.sell_ex)
        if buy_tick is None or sell_tick is None:
            return None
        return buy_tick.bid, buy_tick.ask, sell_tick.bid, sell_tick.ask

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
        exit_long_fill: float,
        exit_short_fill: float,
        exit_spread_pct: float,
        reason: str,
    ) -> None:
        # Long leg: opened at entry_buy (the buy-venue *ask*), now
        # selling into the buy-venue *bid* (= exit_long_fill).
        qty_long = self._notional / trade.entry_buy
        long_pnl = qty_long * (exit_long_fill - trade.entry_buy)

        # Short leg: opened at entry_sell (the sell-venue *bid*), now
        # buying back at the sell-venue *ask* (= exit_short_fill).
        qty_short = self._notional / trade.entry_sell
        short_pnl = qty_short * (trade.entry_sell - exit_short_fill)

        gross_pnl = long_pnl + short_pnl

        # Fee table keys are bare names ("binance"); ArbSignal carries
        # market-suffixed labels ("binance-perp") — strip before lookup.
        fee_buy = self._fees.perp.get(base_exchange(trade.buy_ex), 0.0)
        fee_sell = self._fees.perp.get(base_exchange(trade.sell_ex), 0.0)
        # Entry + exit taker fees on both legs, valued at notional.
        fees_usd = (
            self._notional * fee_buy * 2
            + self._notional * fee_sell * 2
        )
        # Slippage haircut on each exit leg — models imperfect fill on
        # top of the bid/ask haircut already baked into exit_*_fill.
        slippage_usd = self._notional * (self._slippage_pct / 100.0) * 2
        net_pnl = gross_pnl - fees_usd - slippage_usd

        now_ms = int(self._now() * 1000)
        hold_seconds = (now_ms - trade.entry_ts_ms) // 1000

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
            exit_buy=exit_long_fill,
            exit_sell=exit_short_fill,
            entry_spread_pct=trade.entry_spread_pct,
            exit_spread_pct=round(exit_spread_pct, 4),
            notional_per_leg=self._notional,
            gross_pnl_usd=round(gross_pnl, 4),
            fee_usd=round(fees_usd + slippage_usd, 4),
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
