"""File-based persistence for signals and ticks.

* :class:`SignalsWriter` — append-only JSON-Lines log of every
  :class:`~arbitrage.signals.ArbSignal` emitted on the bus. Used for
  later analysis (grep / jq) and as the input corpus for the
  paper-trading simulator.
* :class:`TickWriter` — append-only binary log of raw
  :class:`~arbitrage.normalizer.Tick` records, daily-rotated and
  zstd-compressed. Drives :mod:`arbitrage.replay` for backtest and
  parameter tuning.
"""

from .signals_writer import SignalsWriter
from .ticks import (
    TickRecord,
    TickWriter,
    attach_writer,
    encode_record,
    iter_records,
    record_tick,
)

__all__ = [
    "SignalsWriter",
    "TickRecord",
    "TickWriter",
    "attach_writer",
    "encode_record",
    "iter_records",
    "record_tick",
]
