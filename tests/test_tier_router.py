"""Tier routing: which topic does a given net_pct go to?"""

from __future__ import annotations

import pytest

from arbitrage.settings import (
    FilterSet,
    MarketFilters,
    Settings,
    Symbols,
    Fees,
    Limits,
    TelegramConfig,
    TierFilter,
)
from arbitrage.tier_router import route_tier


def _make_settings() -> Settings:
    spot = MarketFilters(
        min_profit_pct=3.0,
        cooldown_seconds=180,
        info_topic_id=100,
        tiers={
            "low": TierFilter(from_pct=3.0, to_pct=5.0, topic_id=101),
            "mid": TierFilter(from_pct=5.0, to_pct=10.0, topic_id=102),
            "high": TierFilter(from_pct=10.0, to_pct=999.0, topic_id=103),
        },
    )
    perp = MarketFilters(
        min_profit_pct=0.5,
        cooldown_seconds=180,
        info_topic_id=200,
        tiers={
            "low": TierFilter(from_pct=0.5, to_pct=1.0, topic_id=201),
            "mid": TierFilter(from_pct=1.0, to_pct=3.0, topic_id=202),
            "high": TierFilter(from_pct=3.0, to_pct=999.0, topic_id=203),
        },
    )
    return Settings(
        symbols=Symbols(spot=["BTCUSDT"], perp=["BTCUSDT"]),
        filters=FilterSet(spot=spot, perp=perp),
        fees=Fees(spot={"a": 0.001}, perp={"a": 0.0004}),
        limits=Limits(),
        telegram=TelegramConfig(),
    )


def test_spot_low_tier():
    s = _make_settings()
    assert route_tier(s, "spot", 3.5) == ("low", 101)


def test_spot_mid_tier():
    s = _make_settings()
    assert route_tier(s, "spot", 7.0) == ("mid", 102)


def test_spot_high_tier():
    s = _make_settings()
    assert route_tier(s, "spot", 50.0) == ("high", 103)


def test_perp_low_tier():
    s = _make_settings()
    assert route_tier(s, "perp", 0.7) == ("low", 201)


def test_below_min_returns_none():
    s = _make_settings()
    # 2.0% is below spot.min_profit_pct and below any tier.from_pct.
    assert route_tier(s, "spot", 2.0) is None


def test_boundaries_are_inclusive_on_lower():
    s = _make_settings()
    # 5.0% is exactly the lower bound of mid; should land in mid, not low.
    assert route_tier(s, "spot", 5.0) == ("mid", 102)


def test_unknown_market_raises():
    s = _make_settings()
    with pytest.raises(ValueError):
        route_tier(s, "options", 5.0)
