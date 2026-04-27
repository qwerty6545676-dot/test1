"""Push-style arbitrage comparator.

`check_and_signal_spot` / `check_and_signal_perp` are called directly
from the exchange listeners right after they write a fresh Tick into
the respective prices-book. There is no polling loop and no queue —
the signal fires on the same event-loop tick as the update, so
arbitrage windows aren't missed to scheduling latency.

The underlying `find_arbitrage(...)` accepts the fees table and
minimum profit percent as kwargs, so it can be driven from either the
spot or the perp config without duplicating logic.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable, Literal

from .config import FEES, MAX_AGE_MS, MIN_PROFIT_PCT
from .exchanges._common import base_exchange
from .normalizer import Tick
from .settings import get_settings
from .signals import ArbSignal, get_bus

logger = logging.getLogger("arbitrage.comparator")

# Shared order book: prices[symbol][exchange] = Tick.
# Exchange handlers write to this dict directly; the comparator reads it.
# Single event loop == single thread, so no lock is required.
PricesBook = dict[str, dict[str, Tick]]


# -------- Perp constants --------
# Spot values (FEES / MIN_PROFIT_PCT / MAX_AGE_MS) come from `config.py`
# for backwards compatibility with pre-perp imports. The perp values
# are loaded from the same settings object at import time and cached
# here. Tests that need different values can monkeypatch these.
def _load_perp_constants() -> None:
    s = get_settings()
    g = globals()
    g["FEES_PERP"] = dict(s.fees.perp)
    g["MIN_PROFIT_PCT_PERP"] = s.filters.perp.min_profit_pct


FEES_PERP: dict[str, float] = {}
MIN_PROFIT_PCT_PERP: float = 0.0
_load_perp_constants()


def find_arbitrage(
    book: dict[str, Tick],
    symbol: str,
    now_ms: int,
    *,
    fees: dict[str, float] | None = None,
    min_pct: float | None = None,
    max_age_ms: int | None = None,
) -> tuple[str, str, float, float, float] | None:
    """Return the best (buy_ex, sell_ex, buy_ask, sell_bid, net_pct) if any.

    - Skips stale ticks (`now_ms - ts_local > max_age_ms`).
    - Fees are applied round-trip: buy at `ask * (1 + fee_buy)`, sell at
      `bid * (1 - fee_sell)`.
    - Returns None if no pair beats `min_pct`.

    The ``fees`` / ``min_pct`` / ``max_age_ms`` kwargs default to the
    spot values (`FEES` / `MIN_PROFIT_PCT` / `MAX_AGE_MS`) so existing
    call sites and tests keep working unchanged.
    """
    # Local aliases — tiny, but this is the hot path.
    if fees is None:
        fees = FEES
    if min_pct is None:
        min_pct = MIN_PROFIT_PCT
    if max_age_ms is None:
        max_age_ms = MAX_AGE_MS

    best: tuple[str, str, float, float, float] | None = None
    best_pct = min_pct

    # Two passes is fine — N is tiny (<= number of exchanges, currently 7).
    for sell_ex, sell in book.items():
        if now_ms - sell.ts_local > max_age_ms:
            continue
        sell_bid = sell.bid
        if sell_bid <= 0.0:
            continue
        # Strip any "-perp" suffix — fee tables in settings.yaml use
        # bare names, while exchange labels carry the market suffix.
        fee_sell = fees.get(base_exchange(sell_ex), 0.0)
        effective_sell = sell_bid * (1.0 - fee_sell)

        for buy_ex, buy in book.items():
            if buy_ex == sell_ex:
                continue
            if now_ms - buy.ts_local > max_age_ms:
                continue
            buy_ask = buy.ask
            if buy_ask <= 0.0:
                continue
            fee_buy = fees.get(base_exchange(buy_ex), 0.0)
            effective_buy = buy_ask * (1.0 + fee_buy)
            if effective_buy <= 0.0:
                continue

            net_pct = (effective_sell / effective_buy - 1.0) * 100.0
            if net_pct > best_pct:
                best_pct = net_pct
                best = (buy_ex, sell_ex, buy_ask, sell_bid, net_pct)

    return best


def check_and_signal(
    prices: PricesBook,
    symbol: str,
    *,
    fees: dict[str, float] | None = None,
    min_pct: float | None = None,
    max_age_ms: int | None = None,
    market: str = "spot",
    now_ms: int | None = None,
) -> None:
    """Called from the WS listener after every price update.

    Looks up the per-symbol book and, if an opportunity clears the
    configured threshold, emits a log line. The ``market`` label only
    appears in the log prefix (``ARB [spot]`` vs ``ARB [perp]``) so
    operators can tail each stream separately.

    ``now_ms`` lets the replay harness inject the recorded timestamp
    so staleness checks operate against the historical clock instead
    of wall time.
    """
    book = prices.get(symbol)
    if book is None or len(book) < 2:
        return

    if now_ms is None:
        now_ms = int(time.time() * 1000)
    hit = find_arbitrage(
        book, symbol, now_ms, fees=fees, min_pct=min_pct, max_age_ms=max_age_ms
    )
    if hit is None:
        return

    buy_ex, sell_ex, buy_ask, sell_bid, net_pct = hit
    logger.info(
        "ARB [%s] %s: buy %s @ %.8f -> sell %s @ %.8f | net %.3f%%",
        market,
        symbol,
        buy_ex,
        buy_ask,
        sell_ex,
        sell_bid,
        net_pct,
    )
    # Publish to the in-process bus. Consumers (Telegram notifier,
    # persistence layer once it lands) handle fan-out. The bus is
    # synchronous; registered handlers must not block — see `signals.py`.
    market_lit: Literal["spot", "perp"] = "perp" if market == "perp" else "spot"
    get_bus().emit_arb(
        ArbSignal(
            ts_ms=now_ms,
            market=market_lit,
            symbol=symbol,
            buy_ex=buy_ex,
            sell_ex=sell_ex,
            buy_ask=buy_ask,
            sell_bid=sell_bid,
            net_pct=net_pct,
        )
    )


def check_and_signal_spot(
    prices: PricesBook, symbol: str, *, now_ms: int | None = None
) -> None:
    """Spot-market variant: uses `FEES` / `MIN_PROFIT_PCT` from config."""
    check_and_signal(
        prices,
        symbol,
        fees=FEES,
        min_pct=MIN_PROFIT_PCT,
        max_age_ms=MAX_AGE_MS,
        market="spot",
        now_ms=now_ms,
    )


def check_and_signal_perp(
    prices: PricesBook, symbol: str, *, now_ms: int | None = None
) -> None:
    """Perp-market variant: uses `FEES_PERP` / `MIN_PROFIT_PCT_PERP`."""
    check_and_signal(
        prices,
        symbol,
        fees=FEES_PERP,
        min_pct=MIN_PROFIT_PCT_PERP,
        max_age_ms=MAX_AGE_MS,
        market="perp",
        now_ms=now_ms,
    )


def iter_symbols(prices: PricesBook) -> Iterable[str]:
    """Helper for external tools (CLI dumps, tests) — not on the hot path."""
    return prices.keys()
