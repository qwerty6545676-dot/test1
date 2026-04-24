"""Cooldown TTL semantics."""

from __future__ import annotations

import pytest

from arbitrage.cooldown import CooldownTracker


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1_000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_first_emit_allowed():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    assert cd.should_emit(("spot", "BTCUSDT"))


def test_second_emit_within_window_blocked():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    assert cd.should_emit("k")
    clock.advance(5.0)
    assert not cd.should_emit("k")


def test_emit_allowed_after_ttl_expires():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    assert cd.should_emit("k")
    clock.advance(10.0)
    assert cd.should_emit("k")


def test_different_keys_do_not_interfere():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    assert cd.should_emit("a")
    assert cd.should_emit("b")
    clock.advance(1.0)
    assert not cd.should_emit("a")
    assert not cd.should_emit("b")


def test_reset_individual_key():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    cd.should_emit("a")
    cd.should_emit("b")
    cd.reset("a")
    assert cd.should_emit("a")
    assert not cd.should_emit("b")


def test_reset_all_keys():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=10.0, now=clock)
    cd.should_emit("a")
    cd.should_emit("b")
    cd.reset()
    assert cd.should_emit("a")
    assert cd.should_emit("b")


def test_ttl_zero_always_allows():
    clock = _FakeClock()
    cd = CooldownTracker(ttl_seconds=0.0, now=clock)
    assert cd.should_emit("k")
    assert cd.should_emit("k")


def test_negative_ttl_rejected():
    with pytest.raises(ValueError):
        CooldownTracker(ttl_seconds=-1.0)
