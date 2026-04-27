"""JSONL writers for open and closed paper trades.

Two separate files so you can `wc -l paper_closed.jsonl` to see
how many paper trades settled, and tail `paper_open.jsonl` to see
every opening without re-parsing closures out.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import TextIO

from .models import ClosedPaperTrade, OpenPaperTrade

logger = logging.getLogger("arbitrage.paper.writer")


class PaperTradesWriter:
    """Append-only JSONL writer paired with its file handle.

    Each ``PaperTradesWriter`` is single-file. Callers create two —
    one for opens, one for closes — and hand them to the
    simulators.
    """

    __slots__ = ("_path", "_file", "_lock")

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._file: TextIO | None = None
        self._lock = threading.Lock()

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, mode="a", encoding="utf-8", buffering=1)
        logger.info("paper writer: appending to %s", self._path)

    def close(self) -> None:
        with self._lock:
            if self._file is not None:
                try:
                    self._file.close()
                finally:
                    self._file = None

    def write(self, record: OpenPaperTrade | ClosedPaperTrade) -> None:
        with self._lock:
            fh = self._file
            if fh is None:
                logger.warning("paper writer: dropping record, file not open")
                return
            try:
                fh.write(json.dumps(_asdict(record), separators=(",", ":")))
                fh.write("\n")
            except Exception:
                logger.exception("paper writer: write failed")


def _asdict(s: OpenPaperTrade | ClosedPaperTrade) -> dict:
    # msgspec structs: pull values via `__struct_fields__` (stable).
    return {f: getattr(s, f) for f in s.__struct_fields__}


__all__ = ["PaperTradesWriter"]
