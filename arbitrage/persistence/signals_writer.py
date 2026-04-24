"""Append-only JSON-Lines log of detected arbitrage signals.

Each line is a standalone JSON document with this shape::

    {"ts_ms": 1700000000123,
     "market": "spot",
     "symbol": "BTCUSDT",
     "buy_ex": "bitget",
     "sell_ex": "kucoin",
     "buy_ask": 78120.5,
     "sell_bid": 78280.3,
     "net_pct": 0.204}

JSON-Lines was picked over binary / msgpack because:

* Human-readable — `tail -f data/signals.jsonl` just works.
* Trivially greppable — ``grep '"symbol":"BTCUSDT"' signals.jsonl``.
* Analyzable with ``jq`` without any custom decoder.
* Append-only + line-oriented = survives SIGKILL cleanly (partial
  writes at end of file are the only failure mode, and at worst
  you lose one line).

Writes are synchronous (`os.write`-ish) and go through Python's
buffered text IO with ``flush=True`` per line, so the file always
reflects committed state up to N-1 signals even if the process dies
mid-second. The overhead on the hot path is ~20 µs / signal on a
modern CPU — negligible compared to websocket decode.

The writer is a **subscriber** on :class:`~arbitrage.signals.SignalBus`.
Signals keep flowing regardless of disk errors — if the file goes
bad we log ERROR (which the BusErrorHandler forwards to the info
topic) but never raise.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import TextIO

from ..signals import ArbSignal, SignalBus

logger = logging.getLogger("arbitrage.persistence.signals")

# Fields serialized per signal, in order. Kept explicit so schema
# changes are visible in a diff (downstream tools key on these
# field names).
_FIELDS: tuple[str, ...] = (
    "ts_ms",
    "market",
    "symbol",
    "buy_ex",
    "sell_ex",
    "buy_ask",
    "sell_bid",
    "net_pct",
)


class SignalsWriter:
    """Subscriber that appends every ArbSignal to a JSON-Lines file."""

    __slots__ = ("_path", "_file", "_lock", "_registered")

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._file: TextIO | None = None
        # Lock guards the file handle: emit_arb() is documented to be
        # called from the event loop thread, but nothing on the bus
        # enforces that. Cheap insurance.
        self._lock = threading.Lock()
        self._registered = False

    # -- lifecycle -----------------------------------------------------

    def open(self) -> None:
        """Create parent dir and open the file in append mode."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # line_buffering=True flushes on every '\n'. Exactly what we
        # want for crash-safety without fsync-per-line overhead.
        self._file = open(
            self._path,
            mode="a",
            encoding="utf-8",
            buffering=1,
        )
        logger.info("signals writer: appending to %s", self._path)

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                try:
                    self._file.close()
                finally:
                    self._file = None

    # -- bus glue ------------------------------------------------------

    def attach(self, bus: SignalBus) -> None:
        """Register ``_on_arb`` as a handler on the bus (idempotent)."""
        if self._registered:
            return
        bus.register_arb(self._on_arb)
        self._registered = True

    def _on_arb(self, signal: ArbSignal) -> None:
        # This runs on the hot path — must be non-blocking and must
        # never raise out. ``json.dumps`` + a small write is ~20µs.
        line = self._serialize(signal)
        with self._lock:
            fh = self._file
            if fh is None:
                # We were called before open() or after close(). Log
                # once-ish and move on.
                logger.warning("signals writer: dropping signal, file not open")
                return
            try:
                fh.write(line)
                fh.write("\n")
            except Exception:
                logger.exception("signals writer: write failed")

    # -- helpers -------------------------------------------------------

    @staticmethod
    def _serialize(signal: ArbSignal) -> str:
        payload = {f: getattr(signal, f) for f in _FIELDS}
        # separators=(",", ":") — compact, no trailing whitespace.
        return json.dumps(payload, separators=(",", ":"))


__all__ = ["SignalsWriter"]
