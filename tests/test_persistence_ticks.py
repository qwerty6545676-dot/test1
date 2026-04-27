"""Tests for ``arbitrage.persistence.ticks``: writer, rotation,
zstd compression, retention sweep, and reader round-trips."""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from arbitrage.normalizer import Tick
from arbitrage.persistence import ticks as ticks_mod
from arbitrage.persistence.ticks import (
    TickRecord,
    TickWriter,
    attach_writer,
    encode_record,
    iter_records,
    record_tick,
)


def _tick(exchange: str = "binance", symbol: str = "BTCUSDT", bid: float = 100.0, ask: float = 100.5) -> Tick:
    return Tick(
        exchange=exchange,
        symbol=symbol,
        bid=bid,
        ask=ask,
        ts_exchange=int(time.time() * 1000),
        ts_local=int(time.time() * 1000),
    )


# -- encode/decode round-trip ----------------------------------------


def test_encode_record_roundtrip():
    rec = TickRecord(
        market="spot",
        exchange="binance",
        symbol="BTCUSDT",
        bid=99.5,
        ask=100.5,
        ts_exchange=1700000000000,
        ts_local=1700000000123,
    )
    blob = encode_record(rec)
    assert len(blob) > 4
    # First 4 bytes are the BE length prefix.
    payload_len = int.from_bytes(blob[:4], "big")
    assert len(blob) == 4 + payload_len

    # Round-trip through iter_records via an in-memory file.
    import io
    decoded = list(iter_records(io.BytesIO(blob)))
    assert decoded == [rec]


def test_iter_records_skips_truncated_tail(tmp_path):
    """A SIGKILL mid-write produces a partial record. iter_records must
    skip cleanly rather than raise."""
    rec = TickRecord(
        market="spot", exchange="binance", symbol="BTCUSDT",
        bid=1.0, ask=2.0, ts_exchange=1, ts_local=2,
    )
    good = encode_record(rec)
    # Truncate the second record to a partial length prefix.
    blob = good + good[:2]
    import io
    decoded = list(iter_records(io.BytesIO(blob)))
    assert decoded == [rec]


def test_iter_records_skips_truncated_body():
    """Truncate inside the body, not the prefix."""
    rec = TickRecord(
        market="spot", exchange="binance", symbol="BTCUSDT",
        bid=1.0, ask=2.0, ts_exchange=1, ts_local=2,
    )
    good = encode_record(rec)
    # First record full, second has full prefix but only half body.
    half_body = good[: 4 + (len(good) - 4) // 2]
    blob = good + half_body
    import io
    decoded = list(iter_records(io.BytesIO(blob)))
    assert decoded == [rec]


# -- TickWriter basic write ------------------------------------------


def test_tick_writer_writes_and_reads_back(tmp_path):
    w = TickWriter(tmp_path, compress=False, retention_days=0)
    w.open()
    try:
        w.write(_tick("binance", "BTCUSDT", 100.0, 100.5), "spot")
        w.write(_tick("bybit-perp", "ETHUSDT", 2000.0, 2000.5), "perp")
    finally:
        w.close()

    daily_files = list(tmp_path.glob("ticks-*.bin"))
    assert len(daily_files) == 1, daily_files
    with open(daily_files[0], "rb") as fh:
        records = list(iter_records(fh))
    assert len(records) == 2
    assert records[0].market == "spot"
    assert records[0].exchange == "binance"
    assert records[0].symbol == "BTCUSDT"
    assert records[0].bid == 100.0
    assert records[1].market == "perp"
    assert records[1].symbol == "ETHUSDT"


def test_tick_writer_open_creates_parent_dir(tmp_path):
    target = tmp_path / "nested" / "deeper" / "ticks"
    w = TickWriter(target, compress=False, retention_days=0)
    w.open()
    try:
        w.write(_tick(), "spot")
    finally:
        w.close()
    assert target.exists()
    assert any(target.glob("ticks-*.bin"))


def test_record_tick_no_op_without_writer():
    """``record_tick`` is the listener-side hook; with no writer
    attached it must be silent and never raise."""
    attach_writer(None)
    record_tick(_tick(), "spot")  # would raise on a bug
    record_tick(_tick(), "perp")


def test_record_tick_writes_when_attached(tmp_path):
    w = TickWriter(tmp_path, compress=False, retention_days=0)
    w.open()
    attach_writer(w)
    try:
        record_tick(_tick("binance", "SOLUSDT", 50.0, 50.1), "spot")
        record_tick(_tick("kucoin-perp", "BTCUSDT", 70000.0, 70010.0), "perp")
    finally:
        attach_writer(None)
        w.close()
    daily_files = list(tmp_path.glob("ticks-*.bin"))
    with open(daily_files[0], "rb") as fh:
        records = list(iter_records(fh))
    assert {r.symbol for r in records} == {"SOLUSDT", "BTCUSDT"}
    assert {r.market for r in records} == {"spot", "perp"}


# -- Rotation --------------------------------------------------------


def test_rotation_on_utc_date_change(tmp_path, monkeypatch):
    """Forcing the date forward triggers rotation: previous file is
    closed (and compressed when ``compress=True``), new daily file is
    opened, write is appended there."""
    fake_date = ["2025-04-22"]

    def _today():
        return fake_date[0]

    monkeypatch.setattr(ticks_mod, "_today_utc", _today)

    w = TickWriter(tmp_path, compress=False, retention_days=0)
    w.open()
    try:
        w.write(_tick("binance", "BTCUSDT", 1.0, 2.0), "spot")
        # advance the wall clock
        fake_date[0] = "2025-04-23"
        w.write(_tick("bybit", "ETHUSDT", 3.0, 4.0), "spot")
    finally:
        w.close()

    files = sorted(p.name for p in tmp_path.glob("ticks-*.bin"))
    assert files == ["ticks-2025-04-22.bin", "ticks-2025-04-23.bin"]

    with open(tmp_path / "ticks-2025-04-22.bin", "rb") as fh:
        recs1 = list(iter_records(fh))
    with open(tmp_path / "ticks-2025-04-23.bin", "rb") as fh:
        recs2 = list(iter_records(fh))
    assert len(recs1) == 1 and recs1[0].symbol == "BTCUSDT"
    assert len(recs2) == 1 and recs2[0].symbol == "ETHUSDT"


def test_rotation_compresses_old_day(tmp_path, monkeypatch):
    fake_date = ["2025-04-22"]
    monkeypatch.setattr(ticks_mod, "_today_utc", lambda: fake_date[0])

    w = TickWriter(tmp_path, compress=True, retention_days=0)
    w.open()
    try:
        w.write(_tick("binance", "BTCUSDT", 1.0, 2.0), "spot")
        fake_date[0] = "2025-04-23"
        w.write(_tick("bybit", "ETHUSDT", 3.0, 4.0), "spot")
    finally:
        w.close()

    # Old day must end up zstd-compressed; the .bin gone.
    assert not (tmp_path / "ticks-2025-04-22.bin").exists()
    assert (tmp_path / "ticks-2025-04-22.bin.zst").exists()
    # The just-closed-on-shutdown current day is also compressed.
    assert (tmp_path / "ticks-2025-04-23.bin.zst").exists()
    assert not (tmp_path / "ticks-2025-04-23.bin").exists()


def test_close_compresses_current_day(tmp_path, monkeypatch):
    """Even without rotation, ``close()`` must zstd-compress the
    current day so the user always finds a ``.zst`` after shutdown."""
    monkeypatch.setattr(ticks_mod, "_today_utc", lambda: "2025-04-22")
    w = TickWriter(tmp_path, compress=True, retention_days=0)
    w.open()
    w.write(_tick(), "spot")
    w.close()
    assert (tmp_path / "ticks-2025-04-22.bin.zst").exists()
    assert not (tmp_path / "ticks-2025-04-22.bin").exists()


# -- Retention -------------------------------------------------------


def test_retention_sweep_drops_old_files(tmp_path, monkeypatch):
    """``retention_days=7`` deletes anything older than 7 days at
    open(). Files within the window stay."""
    today = datetime.now(timezone.utc).date()

    # Old file (10 days old) — must be deleted.
    old = tmp_path / f"ticks-{(today - timedelta(days=10)).isoformat()}.bin.zst"
    old.write_bytes(b"\x00")

    # Within-window file (2 days old) — must survive.
    fresh = tmp_path / f"ticks-{(today - timedelta(days=2)).isoformat()}.bin.zst"
    fresh.write_bytes(b"\x00")

    # Today — must survive.
    todays = tmp_path / f"ticks-{today.isoformat()}.bin"
    todays.write_bytes(b"")

    w = TickWriter(tmp_path, compress=False, retention_days=7)
    w.open()
    w.close()

    survivors = sorted(p.name for p in tmp_path.glob("ticks-*"))
    assert old.name not in survivors
    assert fresh.name in survivors
    # The current day file may or may not have been compressed depending
    # on whether close() saw a writer state — assert at least one of
    # today's variants remains.
    assert any(today.isoformat() in name for name in survivors)


def test_retention_zero_disables_sweep(tmp_path):
    """``retention_days=0`` keeps every file forever."""
    today = datetime.now(timezone.utc).date()
    ancient = tmp_path / f"ticks-{(today - timedelta(days=365)).isoformat()}.bin"
    ancient.write_bytes(b"")

    w = TickWriter(tmp_path, compress=False, retention_days=0)
    w.open()
    w.close()

    assert ancient.exists()
