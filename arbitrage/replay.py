"""Replay recorded ticks through the live comparator.

The persistence layer (:mod:`arbitrage.persistence.ticks`) writes a
length-prefixed binary log of every tick observed in production. This
module reads that log back, reconstructs the in-memory price book, and
fires the *real* comparator at every record — exactly as if the data
were arriving live.

Why
---
* **Parameter tuning.** Want to see how many signals you'd have caught
  yesterday with ``filters.spot.min_profit_pct = 2.0`` instead of
  ``3.0``? Edit ``settings.yaml``, run replay, count lines in
  ``data/signals.jsonl``. No need to wait 24 hours of real time.
* **Regression debugging.** A weird signal at 3:42 — replay the
  surrounding window and ``logger.debug`` your way through the parser.
* **Paper-trading backtest.** Plug :class:`SpotPaperTrader` /
  :class:`PerpPaperTrader` into the bus, replay a week of ticks,
  inspect ``paper_closed.jsonl`` for the realized PnL distribution.

CLI
---
::

    python -m arbitrage.replay --root data/ticks
    python -m arbitrage.replay --from 2025-04-20 --to 2025-04-22
    python -m arbitrage.replay --speed 0  # default: as fast as possible
    python -m arbitrage.replay --speed 1  # honor recorded ts_local

The CLI uses the live ``settings.yaml`` for fees / thresholds / tier
filters. To replay against alternate parameters, just point
``ARB_SETTINGS_PATH`` at a different config — no special replay knobs.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Iterator

from .comparator import (
    PricesBook,
    check_and_signal_perp,
    check_and_signal_spot,
)
from .normalizer import Tick
from .persistence.ticks import TickRecord, iter_records

logger = logging.getLogger("arbitrage.replay")


def _open_records(path: Path) -> Iterator[TickRecord]:
    """Open ``ticks-*.bin`` (plain) or ``ticks-*.bin.zst`` (compressed)
    and yield records. Handles either transparently.
    """
    if path.suffix == ".zst":
        try:
            import zstandard as zstd
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "zstandard required to read .zst tick logs"
            ) from exc
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as fin:
            with dctx.stream_reader(fin) as reader:
                # Wrap the streaming reader so iter_records can do
                # blocking read(n) calls of any length.
                yield from iter_records(_StreamReader(reader))
    else:
        with open(path, "rb") as fin:
            yield from iter_records(fin)


class _StreamReader:
    """Adapter making ``zstd.stream_reader`` compatible with our
    fixed-size :func:`iter_records` ``read(n)`` contract.

    The raw stream reader can short-read; we keep calling until ``n``
    bytes are accumulated or EOF.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: IO[bytes]) -> None:
        self._inner = inner

    def read(self, n: int) -> bytes:
        out = bytearray()
        remaining = n
        while remaining > 0:
            chunk = self._inner.read(remaining)
            if not chunk:
                break
            out.extend(chunk)
            remaining -= len(chunk)
        return bytes(out)


def discover_files(
    root: Path,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[Path]:
    """Return ``ticks-*.bin[.zst]`` files in chronological order, one
    per date.

    ``start`` and ``end`` are inclusive UTC dates; ``None`` means
    unbounded.

    If both ``ticks-D.bin`` and ``ticks-D.bin.zst`` exist for the same
    date ``D`` (the transient state during background compression, or
    the persistent state after a crash mid-compress) the ``.bin`` is
    preferred — it's the source of truth and is guaranteed complete,
    while the ``.zst`` may be partially-written. Replaying both would
    double-count every tick of that day.
    """
    by_date: dict[date, Path] = {}
    for entry in root.iterdir():
        name = entry.name
        if not name.startswith("ticks-"):
            continue
        if not (name.endswith(".bin") or name.endswith(".bin.zst")):
            continue
        try:
            d = datetime.strptime(name[len("ticks-"):][:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if start is not None and d < start:
            continue
        if end is not None and d > end:
            continue

        existing = by_date.get(d)
        if existing is None:
            by_date[d] = entry
            continue
        # Tie-break: prefer the .bin (source of truth) over the .zst
        # (possibly partial after an interrupted compression).
        if entry.suffix == ".bin" and existing.suffix == ".zst":
            by_date[d] = entry
        # else keep `existing` (already .bin, or both .zst — second .zst
        # is a duplicate rename which doesn't happen in normal flow)

    return [by_date[d] for d in sorted(by_date)]


def _record_to_tick(rec: TickRecord) -> Tick:
    return Tick(
        exchange=rec.exchange,
        symbol=rec.symbol,
        bid=rec.bid,
        ask=rec.ask,
        ts_exchange=rec.ts_exchange,
        ts_local=rec.ts_local,
    )


async def replay(
    paths: list[Path],
    *,
    speed: float = 0.0,
) -> dict[str, int]:
    """Stream every record through the live comparator.

    Parameters
    ----------
    paths
        Files to read, already in chronological order. ``.bin.zst``
        and ``.bin`` are both fine.
    speed
        ``0.0`` = playback as fast as possible (default — useful for
        backtest runs of days of data in seconds). ``1.0`` = honor
        the recorded ``ts_local`` so events fire at their original
        wall-clock pace (useful for live-feel paper trading
        simulations).

    Returns
    -------
    dict
        ``{"records": N, "spot_ticks": A, "perp_ticks": B}``.
    """
    prices_spot: PricesBook = {}
    prices_perp: PricesBook = {}
    counts = {"records": 0, "spot_ticks": 0, "perp_ticks": 0}

    first_ts: int | None = None
    started_at: float | None = None

    for path in paths:
        logger.info("replay: reading %s", path)
        for rec in _open_records(path):
            counts["records"] += 1
            if speed > 0 and rec.ts_local:
                if first_ts is None:
                    first_ts = rec.ts_local
                    started_at = asyncio.get_running_loop().time()
                else:
                    elapsed_real = asyncio.get_running_loop().time() - (started_at or 0.0)
                    elapsed_recorded_s = (rec.ts_local - first_ts) / 1000.0
                    delay = (elapsed_recorded_s / speed) - elapsed_real
                    if delay > 0:
                        await asyncio.sleep(delay)

            tick = _record_to_tick(rec)
            # Pass the recorded ts as the "current" wall clock so the
            # comparator's staleness check (`now_ms - ts_local > MAX_AGE_MS`)
            # operates against the historical clock — otherwise every
            # historical tick would be rejected as stale.
            if rec.market == "spot":
                book = prices_spot.get(rec.symbol)
                if book is None:
                    book = {}
                    prices_spot[rec.symbol] = book
                book[rec.exchange] = tick
                check_and_signal_spot(prices_spot, rec.symbol, now_ms=rec.ts_local)
                counts["spot_ticks"] += 1
            elif rec.market == "perp":
                book = prices_perp.get(rec.symbol)
                if book is None:
                    book = {}
                    prices_perp[rec.symbol] = book
                book[rec.exchange] = tick
                check_and_signal_perp(prices_perp, rec.symbol, now_ms=rec.ts_local)
                counts["perp_ticks"] += 1

    return counts


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m arbitrage.replay",
        description="Replay recorded ticks through the live comparator.",
    )
    p.add_argument(
        "--root",
        default="data/ticks",
        type=Path,
        help="directory containing ticks-YYYY-MM-DD.bin[.zst] files (default: data/ticks)",
    )
    p.add_argument(
        "--from",
        dest="from_date",
        type=_parse_date,
        default=None,
        help="inclusive UTC start date YYYY-MM-DD (default: earliest)",
    )
    p.add_argument(
        "--to",
        dest="to_date",
        type=_parse_date,
        default=None,
        help="inclusive UTC end date YYYY-MM-DD (default: latest)",
    )
    p.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="playback speed: 0=as-fast-as-possible (default), 1=realtime, "
        "10=10x realtime, ...",
    )
    return p


async def _amain(argv: list[str] | None = None) -> int:
    args = _build_argparser().parse_args(argv)
    root: Path = args.root
    if not root.exists():
        logger.error("replay: --root %s does not exist", root)
        return 2

    paths = discover_files(root, start=args.from_date, end=args.to_date)
    if not paths:
        logger.error("replay: no tick files matched in %s", root)
        return 1

    logger.info(
        "replay: %d file(s), speed=%s, from=%s to=%s",
        len(paths),
        "max" if args.speed == 0 else f"{args.speed}x",
        args.from_date or "earliest",
        args.to_date or "latest",
    )
    counts = await replay(paths, speed=args.speed)
    logger.info(
        "replay: done — %d records (%d spot, %d perp)",
        counts["records"],
        counts["spot_ticks"],
        counts["perp_ticks"],
    )
    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":  # pragma: no cover
    main()
