"""Append-only binary log of every Tick observed across all venues.

Why a separate format from ``signals.jsonl``?

* Volume is two orders of magnitude higher (~1000 ticks/s/venue × 14
  venues vs ~1 signal/min). JSON-Lines + UTF-8 encoding overhead would
  cost a meaningful slice of CPU; msgpack via msgspec is ~5× faster
  to encode and ~3× smaller on disk.
* Daily rotation + zstd compression gives ~4× further reduction so a
  full day fits in ~250 MB instead of ~1 GB.
* The corpus is the input to backtest / replay (``arbitrage.replay``),
  which only needs structured access to ``Tick`` records — humans
  rarely tail this file directly.

File layout
-----------
``data/ticks-YYYY-MM-DD.bin`` — current day, length-prefixed records::

    [4-byte big-endian uint32 length] [msgpack(TickRecord)] ...

Length-prefixing makes the format self-delimiting without parsing the
msgpack body, which keeps the streaming reader (``iter_records``)
trivial. After UTC-midnight rotation, the previous file is compressed
in a background task to ``ticks-YYYY-MM-DD.bin.zst`` and the
uncompressed ``.bin`` is deleted.

Crash safety
------------
Writes are synchronous on a buffered binary file (default OS-block
buffering). A partial trailing record at EOF is the only failure mode;
``iter_records`` skips it cleanly via the length-prefix sentinel.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterator, Literal

import msgspec

from ..normalizer import Tick

logger = logging.getLogger("arbitrage.persistence.ticks")

Market = Literal["spot", "perp"]


class TickRecord(msgspec.Struct, frozen=True, gc=False):
    """One persisted tick. Fields and order are part of the on-disk
    schema — never reorder or rename without a migration story."""

    market: str
    exchange: str
    symbol: str
    bid: float
    ask: float
    ts_exchange: int
    ts_local: int


# Module-level decoder/encoder reused across all writes — cheap to
# allocate but allocation-free hot path matters at 7000 ticks/s.
_encoder: msgspec.msgpack.Encoder = msgspec.msgpack.Encoder()
_decoder: msgspec.msgpack.Decoder[TickRecord] = msgspec.msgpack.Decoder(TickRecord)


def encode_record(rec: TickRecord) -> bytes:
    """Serialize one record as ``[4-byte BE length][msgpack body]``."""
    body = _encoder.encode(rec)
    return len(body).to_bytes(4, "big") + body


def iter_records(fh: BinaryIO) -> Iterator[TickRecord]:
    """Yield every ``TickRecord`` from a length-prefixed binary stream.

    Stops cleanly on EOF; a truncated trailing record (partial length
    or partial body) is silently dropped — that's the documented
    failure mode for SIGKILL mid-write.
    """
    while True:
        prefix = fh.read(4)
        if not prefix:
            return
        if len(prefix) < 4:
            logger.warning("ticks: truncated length prefix at EOF, skipping tail")
            return
        n = int.from_bytes(prefix, "big")
        body = fh.read(n)
        if len(body) < n:
            logger.warning("ticks: truncated record (got %d/%d bytes), skipping tail", len(body), n)
            return
        try:
            yield _decoder.decode(body)
        except msgspec.DecodeError:
            logger.exception("ticks: corrupt record, skipping")
            continue


# -- Process-wide writer registration -------------------------------
#
# Listeners call :func:`record_tick` after writing the tick into the
# prices book. The function is a near-no-op when no writer is attached,
# which is the default in unit tests.

_writer: "TickWriter | None" = None


def attach_writer(writer: "TickWriter | None") -> None:
    """Install the process-wide writer (or ``None`` to detach)."""
    global _writer
    _writer = writer


def record_tick(tick: Tick, market: Market) -> None:
    """Hot-path hook called from each listener after book update.

    No-op when the writer is not attached. Never raises — disk
    pressure must not stop the scanner.
    """
    w = _writer
    if w is None:
        return
    try:
        w.write(tick, market)
    except Exception:  # pragma: no cover — defensive
        logger.exception("ticks: record_tick failed")


# -- Writer ---------------------------------------------------------


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _daily_path(root: Path, date_str: str) -> Path:
    return root / f"ticks-{date_str}.bin"


class TickWriter:
    """Daily-rotated, optionally zstd-compressed tick log.

    Lifecycle::

        w = TickWriter(root="data/ticks", compress=True, retention_days=7)
        w.open()
        # ... record_tick(tick, market) called from listeners ...
        w.close()  # also rotates current day into .zst on shutdown

    Rotation is triggered lazily inside :meth:`write` whenever the
    UTC date advances. There's no separate timer task — under steady
    listener traffic ticks arrive every few milliseconds, so the
    rotation latency at midnight is well under a second.

    Compression happens off the event loop via
    :class:`concurrent.futures.ThreadPoolExecutor`. The hot path
    blocks only on ``open(..., 'ab')`` of the new day's file (~50µs).
    """

    __slots__ = (
        "_root",
        "_compress",
        "_retention_days",
        "_file",
        "_current_date",
        "_lock",
        "_compress_loop",
    )

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        compress: bool = True,
        retention_days: int = 7,
    ) -> None:
        self._root = Path(root)
        self._compress = compress
        self._retention_days = retention_days
        self._file: BinaryIO | None = None
        self._current_date: str = ""
        # Guards file handle and rotation. Bus + listener may call into
        # ``write`` from the event loop thread; we still want
        # process-shared state.
        self._lock = threading.Lock()
        self._compress_loop: asyncio.AbstractEventLoop | None = None

    # -- lifecycle -----------------------------------------------------

    def open(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        self._current_date = _today_utc()
        path = _daily_path(self._root, self._current_date)
        self._file = open(path, mode="ab")
        # Capture the current event loop, if any, so background
        # compression can be scheduled on it without thread-safety
        # surprises.
        try:
            self._compress_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._compress_loop = None
        logger.info("ticks writer: appending to %s", path)
        # Best-effort retention sweep on startup.
        if self._retention_days > 0:
            self._sweep_old_files()

    def close(self) -> None:
        with self._lock:
            if self._file is None:
                return
            try:
                self._file.close()
            finally:
                self._file = None
            stale_date = self._current_date
        # Compress what was the active file synchronously on shutdown
        # so the user gets a consistent zst on disk after Ctrl+C.
        if self._compress and stale_date:
            self._compress_file_sync(stale_date)

    # -- write hot path -----------------------------------------------

    def write(self, tick: Tick, market: Market) -> None:
        """Append one record. Triggers rotation on UTC date change."""
        rec = TickRecord(
            market=market,
            exchange=tick.exchange,
            symbol=tick.symbol,
            bid=tick.bid,
            ask=tick.ask,
            ts_exchange=tick.ts_exchange,
            ts_local=tick.ts_local,
        )
        payload = encode_record(rec)
        with self._lock:
            today = _today_utc()
            if today != self._current_date and self._file is not None:
                self._rotate_locked(today)
            fh = self._file
            if fh is None:
                logger.warning("ticks writer: dropping record, file not open")
                return
            try:
                fh.write(payload)
            except Exception:
                logger.exception("ticks writer: write failed")

    # -- rotation ------------------------------------------------------

    def _rotate_locked(self, new_date: str) -> None:
        """Close the current file and open a new one for ``new_date``.

        Caller MUST hold ``self._lock``.
        """
        old_date = self._current_date
        try:
            assert self._file is not None
            self._file.close()
        except Exception:
            logger.exception("ticks writer: error closing rotated file")
        self._file = None

        self._current_date = new_date
        path = _daily_path(self._root, new_date)
        try:
            self._file = open(path, mode="ab")
        except Exception:
            logger.exception("ticks writer: cannot open new day file %s", path)
            return
        logger.info("ticks writer: rotated -> %s", path)

        # Schedule background compression of the just-closed day.
        if self._compress and old_date:
            self._schedule_compress(old_date)

        # Apply retention to drop ancient files.
        if self._retention_days > 0:
            self._sweep_old_files()

    def _schedule_compress(self, date_str: str) -> None:
        loop = self._compress_loop
        if loop is not None and loop.is_running():
            # Off the event loop in a worker thread.
            loop.run_in_executor(None, self._compress_file_sync, date_str)
        else:
            # No loop (CLI / tests) — just do it inline.
            self._compress_file_sync(date_str)

    def _compress_file_sync(self, date_str: str) -> None:
        src = _daily_path(self._root, date_str)
        if not src.exists():
            return
        dst = src.with_suffix(src.suffix + ".zst")
        try:
            import zstandard as zstd
        except ImportError:
            logger.warning("ticks writer: zstandard not installed; leaving %s uncompressed", src)
            return
        try:
            cctx = zstd.ZstdCompressor(level=10)
            with open(src, "rb") as fin, open(dst, "wb") as fout:
                cctx.copy_stream(fin, fout)
            src.unlink()
            logger.info("ticks writer: compressed %s -> %s", src.name, dst.name)
        except Exception:
            logger.exception("ticks writer: compression failed for %s", src)

    def _sweep_old_files(self) -> None:
        """Delete daily files older than ``retention_days``."""
        try:
            now = datetime.now(timezone.utc).date()
            for entry in self._root.glob("ticks-*.bin*"):
                # Filename pattern: ticks-YYYY-MM-DD.bin[.zst]
                stem = entry.name
                try:
                    date_part = stem[len("ticks-"):][:10]
                    file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                except ValueError:
                    continue
                age_days = (now - file_date).days
                if age_days > self._retention_days:
                    try:
                        entry.unlink()
                        logger.info("ticks writer: retention swept %s (%d days old)", entry.name, age_days)
                    except OSError:
                        logger.exception("ticks writer: cannot remove %s", entry)
        except Exception:
            logger.exception("ticks writer: retention sweep failed")


__all__ = [
    "TickRecord",
    "TickWriter",
    "attach_writer",
    "record_tick",
    "encode_record",
    "iter_records",
]
