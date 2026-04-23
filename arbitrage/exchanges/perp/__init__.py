"""Perp (USDT-margined) listeners for all 7 exchanges.

Each submodule exposes a ``run_<exchange>()`` coroutine that writes
into the shared ``prices_perp`` book and calls
:func:`arbitrage.comparator.check_and_signal_perp` after every
update. Exchange labels are suffixed with ``-perp`` (e.g.
``binance-perp``) so spot and perp venues can coexist without clashing
in logs or prices-books.
"""

from .binance import run_binance
from .bingx import run_bingx
from .bitget import run_bitget
from .bybit import run_bybit
from .gateio import run_gateio
from .kucoin import run_kucoin
from .mexc import run_mexc

__all__ = [
    "run_binance",
    "run_bybit",
    "run_gateio",
    "run_bitget",
    "run_kucoin",
    "run_bingx",
    "run_mexc",
]
