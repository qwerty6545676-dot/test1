"""Data models for paper trades."""

from __future__ import annotations

from typing import Literal

import msgspec

Market = Literal["spot", "perp"]


class OpenPaperTrade(msgspec.Struct, frozen=True, gc=False):
    """Perp position opened on signal, waiting for convergence."""

    id: str
    market: Market
    symbol: str
    buy_ex: str                       # cheap leg — we went LONG here
    sell_ex: str                      # expensive leg — we went SHORT here
    entry_ts_ms: int
    entry_buy: float                  # long-leg entry price
    entry_sell: float                 # short-leg entry price
    entry_spread_pct: float
    notional_per_leg: float           # USD notional on each leg


class ClosedPaperTrade(msgspec.Struct, frozen=True, gc=False):
    """One finished paper trade; written to paper_closed.jsonl."""

    id: str
    market: Market
    symbol: str
    buy_ex: str
    sell_ex: str
    entry_ts_ms: int
    exit_ts_ms: int
    entry_buy: float
    entry_sell: float
    exit_buy: float
    exit_sell: float
    entry_spread_pct: float
    exit_spread_pct: float
    notional_per_leg: float
    gross_pnl_usd: float              # pre-fee PnL, both legs combined
    fee_usd: float                    # entry + exit fees on both legs
    net_pnl_usd: float
    hold_seconds: int
    reason: Literal["converged", "expired", "instant"]


__all__ = ["ClosedPaperTrade", "OpenPaperTrade"]
