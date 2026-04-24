"""Paper-trading simulator for arbitrage signals.

We don't place real orders — we compute theoretical PnL assuming
the signal had been executed at the observed prices. Two models,
per user spec:

* **Spot** (:class:`SpotPaperTrader`): instant execution.  Spot
  cross-exchange arb isn't a cash-and-carry, it's a spatial trade;
  you buy on A, sell on B simultaneously, and your PnL is just the
  price gap minus two taker fees and a small slippage haircut. No
  tracking over time — one signal produces exactly one closed
  trade record.
* **Perp** (:class:`PerpPaperTrader`): open a long on the cheap
  venue and an equal short on the expensive venue. Wait for the
  spread to close (or hit a timeout), then compute PnL on both
  legs. Positions live in memory and are serialized to
  ``paper_open.jsonl`` on open, and to ``paper_closed.jsonl``
  when they close.

Every closed trade also emits an ``InfoEvent(kind="notice",
severity="info")`` so the Telegram info-topic stays the single
pane of glass for the operator.

All accounting is in USD-equivalent; we assume USDT-margined
stables, which matches every venue/symbol we currently support.
"""

from .models import ClosedPaperTrade, OpenPaperTrade
from .perp import PerpPaperTrader
from .spot import SpotPaperTrader
from .writer import PaperTradesWriter

__all__ = [
    "ClosedPaperTrade",
    "OpenPaperTrade",
    "PaperTradesWriter",
    "PerpPaperTrader",
    "SpotPaperTrader",
]
