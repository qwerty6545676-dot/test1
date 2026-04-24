"""File-based persistence for signals and ticks.

Currently implemented:

* :class:`SignalsWriter` — append-only JSON-Lines log of every
  :class:`~arbitrage.signals.ArbSignal` emitted on the bus. Used for
  later analysis (grep / jq) and as the input corpus for the
  paper-trading simulator.

Planned (separate PR, coupled with the backtest / replay module):

* ``TicksWriter`` — binary msgspec-encoded raw ticks with daily
  rotation and zstd compression.
"""

from .signals_writer import SignalsWriter

__all__ = ["SignalsWriter"]
