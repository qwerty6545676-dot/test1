"""Spot-market listeners for all 7 exchanges.

Each submodule exposes a single ``run_<exchange>()`` coroutine that
opens one WebSocket, normalizes incoming frames into Ticks and writes
them into the shared ``prices_spot`` book. All listeners are in this
package so the ``from .exchanges.spot import …`` imports in
``main.py`` stay short.
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
