"""Push-style arbitrage comparator.

`check_and_signal` is called directly from the exchange listeners right
after they write a fresh Tick into `prices`. There is no polling loop
and no queue — the signal fires on the same event-loop tick as the
update, so arbitrage windows aren't missed to scheduling latency.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

from .config import FEES, MAX_AGE_MS, MIN_PROFIT_PCT
from .normalizer import Tick

logger = logging.getLogger("arbitrage.comparator")

# Shared order book: prices[symbol][exchange] = Tick.
# Exchange handlers write to this dict directly; the comparator reads it.
# Single event loop == single thread, so no lock is required.
PricesBook = dict[str, dict[str, Tick]]


def find_arbitrage(
    book: dict[str, Tick],
    symbol: str,
    now_ms: int,
) -> tuple[str, str, float, float, float] | None:
    """Return the best (buy_ex, sell_ex, buy_ask, sell_bid, net_pct) if any.

    - Skips stale ticks (`now_ms - ts_local > MAX_AGE_MS`).
    - Fees are applied round-trip: buy at `ask * (1 + fee_buy)`, sell at
      `bid * (1 - fee_sell)`.
    - Returns None if no pair beats `MIN_PROFIT_PCT`.
    """
    best: tuple[str, str, float, float, float] | None = None
    best_pct = MIN_PROFIT_PCT

    # Local aliases — tiny, but this is the hot path.
    max_age = MAX_AGE_MS
    fees = FEES

    # Two passes is fine — N is tiny (<= number of exchanges, currently 2).
    for sell_ex, sell in book.items():
        if now_ms - sell.ts_local > max_age:
            continue
        sell_bid = sell.bid
        if sell_bid <= 0.0:
            continue
        fee_sell = fees.get(sell_ex, 0.0)
        effective_sell = sell_bid * (1.0 - fee_sell)

        for buy_ex, buy in book.items():
            if buy_ex == sell_ex:
                continue
            if now_ms - buy.ts_local > max_age:
                continue
            buy_ask = buy.ask
            if buy_ask <= 0.0:
                continue
            fee_buy = fees.get(buy_ex, 0.0)
            effective_buy = buy_ask * (1.0 + fee_buy)
            if effective_buy <= 0.0:
                continue

            net_pct = (effective_sell / effective_buy - 1.0) * 100.0
            if net_pct > best_pct:
                best_pct = net_pct
                best = (buy_ex, sell_ex, buy_ask, sell_bid, net_pct)

    return best


def check_and_signal(prices: PricesBook, symbol: str) -> None:
    """Called from the WS listener after every price update.

    Looks up the per-symbol book and, if an opportunity clears the
    configured threshold, emits a log line. Swap `logger.info` for your
    own sink (queue, webhook, redis pub/sub, ...) without touching the
    hot path upstream.
    """
    book = prices.get(symbol)
    if book is None or len(book) < 2:
        return

    now_ms = int(time.time() * 1000)
    hit = find_arbitrage(book, symbol, now_ms)
    if hit is None:
        return

    buy_ex, sell_ex, buy_ask, sell_bid, net_pct = hit
    logger.info(
        "ARB %s: buy %s @ %.8f -> sell %s @ %.8f | net %.3f%%",
        symbol,
        buy_ex,
        buy_ask,
        sell_ex,
        sell_bid,
        net_pct,
    )


def iter_symbols(prices: PricesBook) -> Iterable[str]:
    """Helper for external tools (CLI dumps, tests) — not on the hot path."""
    return prices.keys()
